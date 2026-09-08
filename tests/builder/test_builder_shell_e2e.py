"""Batch 12b -- end-to-end tests against a REAL server and a REAL browser.

This is the batch's actual gate, and it DOES run in CI: `playwright` is in
pyproject.toml's dev extras and the workflow runs `playwright install
--with-deps chromium`, because the package alone still skips. The skip-guard
below is kept for a developer who has not installed the browser locally, not
as an excuse for CI -- a green run here means these tests actually executed.

No indicator id, NIS code, or commune figure is hand-typed here beyond what
`tests/fixtures/pages/builders.py` already licenses as ordinary UI-copy test
fixtures (claude.md rule 36's own carve-out, restated in that module's
docstring) -- block content strings like "Hero heading EN" are hand-written
placeholder text, not indicator values.
"""

from __future__ import annotations

import http.client
import json
import secrets
import socket
import threading

import pytest

from src.builder import paths as builder_paths
from src.builder import store as builder_store
from src.builder.service import BuilderConfig, make_server
from tests.fixtures.pages import builders, real_data

# ---------------------------------------------------------------------------
# harness
# ---------------------------------------------------------------------------


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class ServerHarness:
    def __init__(self):
        self.token = secrets.token_urlsafe(32)
        self.config = BuilderConfig(host="127.0.0.1", port=_free_loopback_port(), token=self.token)
        self.server = make_server(self.config)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def url(self) -> str:
        return f"{self.base_url}/?token={self.token}"

    def request(self, method, path, *, body=None, token="__default__"):
        headers = {}
        if token == "__default__":
            headers["X-BelPulse-Token"] = self.token
        elif token is not None:
            headers["X-BelPulse-Token"] = token
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
            headers["Origin"] = self.base_url
        # Timeout well ABOVE the server's own 10s per-socket read timeout
        # (BuilderHandler.timeout): this harness opens a fresh connection per
        # call, and the single-threaded server cannot accept it while a
        # browser page open in the same test still holds its own kept-alive
        # connection idle. See the known-risks.md row on this; a shorter
        # timeout here would make the TEST flaky for a server property that
        # is real and already documented, not a bug in what is under test.
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=20)
        try:
            conn.request(method, path, body=data, headers=headers)
            resp = conn.getresponse()
            raw = resp.read()
        finally:
            conn.close()
        return resp, raw

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def wait_until(predicate, *, timeout_s=15.0, interval_s=0.1, message="condition"):
    """Poll `predicate` (a zero-arg callable returning truthy/falsy) instead
    of a fixed sleep. A fixed short sleep is the wrong tool here: the single-
    threaded builder service can legitimately take up to ~10s to accept a new
    connection while a browser page's own kept-alive connection is still open
    (see known-risks.md) -- a short, fixed wait makes a real, already-known
    server property look like a flaky test instead."""
    import time

    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval_s)
    raise AssertionError(f"timed out waiting for: {message} (last value: {last!r})")


@pytest.fixture()
def pages_root(tmp_path, monkeypatch):
    root = tmp_path / "pages"
    root.mkdir()
    monkeypatch.setattr(builder_paths, "PAGES_ROOT", root)
    monkeypatch.setattr(builder_store, "PAGES_ROOT", root)
    return root


@pytest.fixture()
def server(pages_root):
    harness = ServerHarness()
    yield harness
    harness.close()


def _save_fixture_page(server: ServerHarness, page_id: str, document: dict):
    resp, raw = server.request("POST", "/api/save", body={"page_id": page_id, "document": document})
    assert resp.status == 200, raw
    return json.loads(raw)


def _draft_path(pages_root, page_id: str):
    return pages_root / page_id / "draft.json"


# ---------------------------------------------------------------------------
# Playwright skip-guard -- copied in spirit from test_builder_shell_service.py
# so this file's gate does not depend on a sibling test module's internals.
# ---------------------------------------------------------------------------


def _chromium():
    try:
        from playwright import sync_api
    except ImportError as exc:
        pytest.skip(f"playwright is not a declared dependency ({exc})")
    try:
        manager = sync_api.sync_playwright().start()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"playwright could not start: {exc}")
    try:
        return manager, manager.chromium.launch()
    except Exception as exc:  # pragma: no cover - environment dependent
        manager.stop()
        pytest.skip(f"no chromium available: {exc}")


class Browser:
    """One browser, one context, a fresh page per call to `.new_page()`, all
    torn down together. A real route (e.g. an actual network dependency)
    would defeat the point of testing offline; this shell makes none."""

    # Chromium logs every non-2xx fetch as a console error, including the 422
    # the service is SUPPOSED to answer when live-validation fires mid-word on
    # a form the operator has not finished filling in. That is the designed
    # behaviour -- the shell catches it and renders the findings -- so counting
    # it as a page error makes these tests fail on a slow machine and pass on a
    # fast one, purely on where the 400ms debounce lands. Every other status
    # still counts: only 422, only the browser's own resource-load line.
    _EXPECTED_RESOURCE_LOG = "Failed to load resource: the server responded with a status of 422"

    def __init__(self, server: ServerHarness):
        self.manager, self.browser = _chromium()
        self.context = self.browser.new_context(viewport={"width": 1440, "height": 900})
        self.server = server
        self.console_errors = []
        self.all_console_errors = []
        self.context.on("console", self._on_console)

    def _on_console(self, msg):
        if msg.type != "error":
            return
        self.all_console_errors.append(msg.text)
        if msg.text.startswith(self._EXPECTED_RESOURCE_LOG):
            return
        self.console_errors.append(msg.text)

    def open(self, wait_until="networkidle"):
        page = self.context.new_page()
        page.goto(self.server.url, wait_until=wait_until)
        return page

    def close(self):
        self.context.close()
        self.browser.close()
        self.manager.stop()


@pytest.fixture()
def browser(server):
    b = Browser(server)
    yield b
    b.close()


# ---------------------------------------------------------------------------
# required state 1: no pages exist yet
# ---------------------------------------------------------------------------


def test_no_pages_yet_is_a_labelled_empty_state(browser):
    page = browser.open()
    sidebar_text = page.locator("#shell-sidebar").inner_text()
    assert "No pages exist yet" in sidebar_text
    assert not browser.console_errors


# ---------------------------------------------------------------------------
# new page: a real form, driven by /api/validate, never a hard-coded
# route/page_type/theme allowlist
# ---------------------------------------------------------------------------


def test_new_page_form_shows_the_servers_own_route_message(browser):
    page = browser.open()
    page.click("text=+ New page")
    page.fill("#npf-page_id", "e2e-page")
    page.fill("#npf-route", "/this-is-not-an-allowed-route/")
    page.fill("#npf-theme", "light-institutional")
    title = page.locator("fieldset:has(legend:has-text('SEO title')) input")
    title.nth(0).fill("E2E EN")
    title.nth(1).fill("E2E FR")
    title.nth(2).fill("E2E NL")
    page.wait_for_timeout(600)
    feedback = page.locator(".bp-validate-feedback").inner_text()
    assert "not an allowlisted route" in feedback or "route" in feedback.lower()
    # Never a locally hard-coded allowlist: the message is the server's own.
    assert "Allowed:" in feedback


def test_creating_a_page_with_a_valid_route_succeeds_and_never_prefills_languages(browser):
    page = browser.open()
    page.click("text=+ New page")
    page.fill("#npf-page_id", "e2e-page")
    page.fill("#npf-route", "/about.html")
    page.fill("#npf-theme", "light-institutional")
    title = page.locator("fieldset:has(legend:has-text('SEO title')) input")
    title.nth(0).fill("E2E title EN")
    # rule 7: fr/nl must NEVER be auto-filled from en.
    assert title.nth(1).input_value() == ""
    assert title.nth(2).input_value() == ""
    title.nth(1).fill("E2E title FR")
    title.nth(2).fill("E2E title NL")
    page.wait_for_timeout(600)
    page.click("text=Create page")
    page.wait_for_timeout(600)
    assert "e2e-page" in page.locator("#shell-sidebar").inner_text()
    assert not browser.console_errors


def test_a_rejected_create_leaves_no_directory_behind(browser, pages_root):
    page = browser.open()
    page.click("text=+ New page")
    page.fill("#npf-page_id", "should-not-exist")
    page.fill("#npf-route", "/not-a-real-route/")
    page.fill("#npf-theme", "light-institutional")
    title = page.locator("fieldset:has(legend:has-text('SEO title')) input")
    title.nth(0).fill("X")
    title.nth(1).fill("X")
    title.nth(2).fill("X")
    page.click("text=Create page")
    page.wait_for_timeout(500)
    assert not (pages_root / "should-not-exist").exists()


# ---------------------------------------------------------------------------
# block library from the registry, structure tree, trilingual editing
# (this exercises the trilingual/CTA/binding "shared snapshot" fix)
# ---------------------------------------------------------------------------


def _create_page_with_all_block_types(page):
    page.click("text=+ New page")
    page.fill("#npf-page_id", "all-blocks")
    page.fill("#npf-route", "/about.html")
    page.fill("#npf-theme", "light-institutional")
    title = page.locator("fieldset:has(legend:has-text('SEO title')) input")
    title.nth(0).fill("All blocks EN")
    title.nth(1).fill("All blocks FR")
    title.nth(2).fill("All blocks NL")
    page.wait_for_timeout(600)
    page.click("text=Create page")
    page.wait_for_timeout(600)
    for name in ["Hero", "Kpi Card", "Chart", "Comparison Table", "Map", "Rich Text"]:
        page.locator(
            f".bp-block-library-item:has(.bp-block-type-name:has-text('{name}')) button:has-text('Add')"
        ).click()
        page.wait_for_timeout(120)


def test_block_library_lists_exactly_the_six_registry_types_with_no_fabricated_figure(browser):
    page = browser.open()
    _create_page_with_all_block_types(page)
    library_text = page.locator(".bp-block-library").inner_text()
    for name in ["Hero", "Kpi Card", "Chart", "Comparison Table", "Map", "Rich Text"]:
        assert name in library_text
    # invariant 3: no thumbnail-style figure in the palette.
    import re

    assert not re.search(r"€|\d\s*%|%\s*\d", library_text)


def test_trilingual_field_keeps_all_three_languages_across_edits(browser):
    """Regression test for a real bug found while building this batch: typing
    into one language input was clobbering the other two back to empty,
    because the control rebuilt the triple from a stale pre-edit snapshot
    instead of a shared, live one."""
    page = browser.open()
    _create_page_with_all_block_types(page)
    page.click("button:has-text('hero (hero-1)')")
    page.wait_for_timeout(150)
    heading = page.locator("#shell-inspector fieldset:has(legend:has-text('Heading')) input")
    heading.nth(0).fill("Hero heading EN")
    heading.nth(1).fill("Hero heading FR")
    heading.nth(2).fill("Hero heading NL")
    page.wait_for_timeout(150)
    assert heading.nth(0).input_value() == "Hero heading EN"
    assert heading.nth(1).input_value() == "Hero heading FR"
    assert heading.nth(2).input_value() == "Hero heading NL"
    page.click("text=Validate")
    page.wait_for_timeout(400)
    banners = page.locator("#shell-banners").inner_text()
    assert "props/heading" not in banners


def test_cta_label_and_href_survive_independent_edits(browser):
    page = browser.open()
    _create_page_with_all_block_types(page)
    page.click("button:has-text('hero (hero-1)')")
    page.wait_for_timeout(120)
    page.select_option("#tabpanel-content select[aria-label='Add an optional field']", "cta")
    page.wait_for_timeout(120)
    label = page.locator("fieldset.bp-cta fieldset:has(legend:has-text('Label')) input")
    href = page.locator("fieldset.bp-cta input[type=text]").last
    label.nth(0).fill("CTA EN")
    href.fill("/somewhere/")
    label.nth(1).fill("CTA FR")
    label.nth(2).fill("CTA NL")
    page.wait_for_timeout(150)
    assert [label.nth(i).input_value() for i in range(3)] == ["CTA EN", "CTA FR", "CTA NL"]
    assert href.input_value() == "/somewhere/"


def test_structure_tree_add_delete_duplicate_move(browser):
    page = browser.open()
    _create_page_with_all_block_types(page)
    tree_text = lambda: page.locator("#shell-sidebar .bp-tree").inner_text()  # noqa: E731
    assert "hero (hero-1)" in tree_text()

    page.click("button[aria-label='Duplicate hero-1']")
    page.wait_for_timeout(150)
    assert "hero-2" in tree_text()

    page.click("button[aria-label='Move hero-2 up']")
    page.wait_for_timeout(150)
    order_before = tree_text().index("hero-2")
    assert order_before < tree_text().index("kpi_card")

    page.on("dialog", lambda d: d.accept())
    page.click("button[aria-label='Delete hero-2']")
    page.wait_for_timeout(200)
    # our own confirmation modal, not a native dialog
    if page.locator(".bp-modal").count():
        page.click(".bp-modal-buttons button:has-text('Delete block')")
    page.wait_for_timeout(200)
    assert "hero-2" not in tree_text()


# ---------------------------------------------------------------------------
# round-trip fidelity -- the headline property
# ---------------------------------------------------------------------------


def test_canonical_draft_round_trips_byte_identical_on_unedited_save(browser, server, pages_root):
    doc = builders.realistic_multi_section_document()
    doc["page_id"] = "roundtrip-page"
    saved = _save_fixture_page(server, "roundtrip-page", doc)
    before = _draft_path(pages_root, "roundtrip-page").read_bytes()
    assert saved["sha256"]

    page = browser.open()
    page.click("text=Open draft")
    page.wait_for_timeout(500)
    page.click("text=Save")
    page.wait_for_timeout(500)
    after = _draft_path(pages_root, "roundtrip-page").read_bytes()
    assert after == before
    assert not browser.console_errors


def test_a_document_that_migrates_on_load_warns_before_save_never_silently(
    browser, server, pages_root
):
    """kpi_card v1 -> v2 renames show_sparkline -> sparkline and adds
    show_provenance (src/pages/migrations.py). Saving the OLD shape directly
    to disk (bypassing validation, as a hand-authored draft would arrive) and
    then opening it in the shell must show the migration banner BEFORE any
    save, and the save itself must be a deliberate, confirmed action."""
    doc = builders.minimal_valid_document()
    doc["page_id"] = "migrating-page"
    kpi_props = builders.props_for("kpi_card", version=1)
    kpi_props["show_sparkline"] = True
    # This document has to be VALID apart from its old kpi_card version, or the
    # save is refused and the migration never gets exercised. Three things that
    # requires: kpi_card declares requires_binding; a `context` geography needs
    # the page to declare context.nis, which this one doesn't, so the NIS is
    # fixed; and the default layout would sit exactly on top of the hero block.
    kpi = builders.make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        version=1,
        props=kpi_props,
        binding=builders.municipal_binding(
            real_data.an_additive_municipal_indicator_id(),
            nis=real_data.a_municipal_nis_code(),
        ),
        block_layout=builders.layout(
            desktop=(0, 2, 4, 2), tablet=(0, 2, 4, 2), mobile=(0, 2, 4, 2)
        ),
    )
    doc["sections"][0]["blocks"].append(kpi)
    text = json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    page_dir = pages_root / "migrating-page"
    page_dir.mkdir(parents=True)
    (page_dir / "draft.json").write_text(text, encoding="utf-8")

    page = browser.open()
    page.click("text=Open draft")
    wait_until(
        lambda: "migrated" in page.locator("#shell-banners").inner_text().lower()
        or "upgraded" in page.locator("#shell-banners").inner_text().lower(),
        message="migration banner",
    )

    page.click("text=Save")
    wait_until(lambda: page.locator(".bp-modal").count() == 1, message="save-upgrade modal")
    modal_text = page.locator(".bp-modal").inner_text()
    assert "upgrade" in modal_text.lower()
    page.click(".bp-modal-buttons button:has-text('Save upgraded')")

    def _kpi_after_save():
        try:
            saved = json.loads((page_dir / "draft.json").read_text())
        except (json.JSONDecodeError, OSError):
            return None
        return next(
            (b for s in saved["sections"] for b in s["blocks"] if b["type"] == "kpi_card"), None
        )

    kpi_saved = wait_until(
        lambda: (_kpi_after_save() or {}).get("version") == 2 and _kpi_after_save(),
        message="draft.json rewritten with kpi_card upgraded to v2",
    )
    assert "sparkline" in kpi_saved["props"]
    assert "show_sparkline" not in kpi_saved["props"]


def test_browser_numeric_fidelity_hazard_blocks_editing_rather_than_silently_rewriting(
    browser, pages_root
):
    """1.0 -> 1, 1e3 -> 1000 and integers past 2**53 are real JSON.parse
    hazards with no existing defence anywhere in the pipeline. The shell must
    refuse to silently re-represent any of them."""
    doc = builders.minimal_valid_document()
    doc["page_id"] = "numeric-hazard"
    text = json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    # Hand-introduce a risky literal a canonical writer would never produce.
    text = text.replace('"revision": 1,', '"revision": 1.0,', 1)
    page_dir = pages_root / "numeric-hazard"
    page_dir.mkdir(parents=True)
    (page_dir / "draft.json").write_text(text, encoding="utf-8")

    page = browser.open()
    page.click("text=Open draft")
    page.wait_for_timeout(500)
    banner = page.locator("#shell-banners").inner_text()
    assert "cannot edit" in banner or "cannot" in banner.lower()
    assert page.locator(".bp-block-library").count() == 0


# ---------------------------------------------------------------------------
# validation errors mapped to the right block, not a raw dump; 413
# ---------------------------------------------------------------------------


def test_invalid_document_errors_land_in_a_structured_list_not_a_raw_dump(browser):
    page = browser.open()
    _create_page_with_all_block_types(page)
    page.click("text=Validate")
    # Wait for the findings themselves, not for the container: #shell-banners is
    # created at startup, so counting it returns 1 before the validate response
    # has arrived and the text would be read empty.
    wait_until(
        lambda: page.locator("#shell-banners li").count() >= 1,
        message="validation findings listed",
    )
    text = page.locator("#shell-banners").inner_text()
    # Not a raw JSON envelope dump: no literal `"code":`/`"errors":` key, and
    # it is rendered as a bulleted list (one <li> per finding), not one blob.
    # (A `{nis}` route-template placeholder legitimately contains a brace, so
    # this checks for JSON-*shaped* text, not the bare character.)
    assert '"code"' not in text and '"errors"' not in text and '"path"' not in text
    assert page.locator("#shell-banners li").count() >= 1
    assert "sections/" in text  # a real document path, per block


def test_413_payload_too_large_is_stated_plainly(browser):
    page = browser.open()
    _create_page_with_all_block_types(page)
    page.click("button:has-text('rich_text (rich-text-1)')")
    page.wait_for_timeout(150)
    para = page.locator("#tabpanel-content .bp-richtext-item fieldset input").nth(0)
    huge = "x" * 1_200_000
    para.fill(huge)
    page.wait_for_timeout(200)
    page.click("text=Save")
    page.wait_for_timeout(700)
    banner_text = page.locator("#shell-banners").inner_text()
    assert "larger than this service will store" in banner_text or "resource limit" in banner_text
    assert "Traceback" not in banner_text


# ---------------------------------------------------------------------------
# publish, restore
# ---------------------------------------------------------------------------


def test_publish_requires_its_own_confirmation_and_is_never_a_save_side_effect(
    browser, server, pages_root
):
    doc = builders.realistic_multi_section_document()
    doc["page_id"] = "publish-page"
    _save_fixture_page(server, "publish-page", doc)

    page = browser.open()
    page.click("text=Open draft")
    page.wait_for_timeout(400)
    page.click("text=Save")
    page.wait_for_timeout(400)
    assert not (pages_root / "publish-page" / "published.json").exists()

    page.click("text=Publish…")
    page.wait_for_timeout(200)
    assert page.locator(".bp-modal").count() == 1
    page.click(".bp-modal-buttons button:has-text('Cancel')")
    page.wait_for_timeout(200)
    assert not (pages_root / "publish-page" / "published.json").exists()

    page.click("text=Publish…")
    page.wait_for_timeout(200)
    page.click(".bp-modal-buttons button:has-text('Publish')")
    page.wait_for_timeout(500)
    assert (pages_root / "publish-page" / "published.json").exists()


def test_publish_is_refused_when_the_draft_is_invalid(browser, server, pages_root):
    doc = builders.minimal_valid_document()
    doc["page_id"] = "invalid-publish"
    doc["sections"][0]["blocks"][0]["props"]["heading"]["en"] = ""
    text = json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    page_dir = pages_root / "invalid-publish"
    page_dir.mkdir(parents=True)
    (page_dir / "draft.json").write_text(text, encoding="utf-8")

    page = browser.open()
    page.click("text=Open draft")
    page.wait_for_timeout(400)
    page.click("text=Publish…")
    page.wait_for_timeout(150)
    page.click(".bp-modal-buttons button:has-text('Publish')")
    page.wait_for_timeout(400)
    assert not (page_dir / "published.json").exists()
    assert "not valid" in page.locator("#shell-banners").inner_text()


def test_restore_shows_empty_state_then_a_confirmation_that_states_it_overwrites_the_draft(
    browser, server, pages_root
):
    doc = builders.realistic_multi_section_document()
    doc["page_id"] = "restore-page"
    _save_fixture_page(server, "restore-page", doc)

    page = browser.open()
    page.click("text=Open draft")
    wait_until(
        lambda: "restore-page" in page.locator("#shell-sidebar").inner_text(), message="draft open"
    )

    page.click("text=Restore…")
    wait_until(lambda: page.locator(".bp-modal").count() == 1, message="restore modal (empty)")
    assert "no published versions" in page.locator(".bp-modal").inner_text()
    page.click(".bp-modal-buttons button:has-text('Close')")
    wait_until(lambda: page.locator(".bp-modal").count() == 0, message="modal closed")

    page.click("text=Publish…")
    wait_until(lambda: page.locator(".bp-modal").count() == 1, message="publish confirm modal")
    page.click(".bp-modal-buttons button:has-text('Publish')")
    wait_until(
        lambda: (pages_root / "restore-page" / "published.json").exists(), message="first publish"
    )

    doc2 = builders.deep_clone(doc)
    doc2["seo"]["title"]["en"] = "Edited after publish"
    resp, _raw = server.request(
        "POST", "/api/save", body={"page_id": "restore-page", "document": doc2}
    )
    assert resp.status == 200

    page.click("text=Publish…")
    wait_until(
        lambda: page.locator(".bp-modal").count() == 1, message="second publish confirm modal"
    )
    page.click(".bp-modal-buttons button:has-text('Publish')")
    wait_until(
        lambda: len(builder_store.list_versions("restore-page")) >= 1, message="a snapshot exists"
    )

    page.click("text=Restore…")
    wait_until(lambda: page.locator(".bp-modal").count() == 1, message="restore modal (populated)")
    modal_text = page.locator(".bp-modal").inner_text()
    assert "overwrites the CURRENT DRAFT" in modal_text or "overwrite" in modal_text.lower()
    page.click(".bp-modal-buttons button:has-text('Cancel')")


# ---------------------------------------------------------------------------
# 401, server unreachable
# ---------------------------------------------------------------------------


def test_401_after_a_server_restart_says_restart_make_builder(browser, server):
    page = browser.open()
    page.click("text=+ New page")
    page.wait_for_timeout(200)
    # Simulate "the server restarted, minting a new token" without actually
    # tearing the process down: the shell's own closure still has the OLD
    # token, so the very next request is indistinguishable from a real
    # restart from the browser's point of view.
    server.config.token = secrets.token_urlsafe(32)
    page.fill("#npf-page_id", "wont-be-created")
    page.fill("#npf-route", "/about.html")
    page.fill("#npf-theme", "light-institutional")
    title = page.locator("fieldset:has(legend:has-text('SEO title')) input")
    title.nth(0).fill("X")
    title.nth(1).fill("X")
    title.nth(2).fill("X")
    page.wait_for_timeout(700)
    feedback = page.locator(".bp-validate-feedback").inner_text()
    assert "restart" in feedback.lower() and "make builder" in feedback.lower()


def test_server_unreachable_is_stated_plainly_not_as_a_blank_failure(browser, server):
    page = browser.open()
    server.close()
    page.click("text=+ New page")
    page.fill("#npf-page_id", "x")
    page.fill("#npf-route", "/about.html")
    page.fill("#npf-theme", "light-institutional")
    title = page.locator("fieldset:has(legend:has-text('SEO title')) input")
    title.nth(0).fill("X")
    title.nth(1).fill("X")
    title.nth(2).fill("X")
    page.wait_for_timeout(700)
    feedback = page.locator(".bp-validate-feedback").inner_text()
    assert "make builder" in feedback.lower() or "reach the builder" in feedback.lower()


# ---------------------------------------------------------------------------
# undo/redo, viewports, unsaved-changes guard, keyboard-only path
# ---------------------------------------------------------------------------


def test_undo_redo_across_at_least_ten_operations(browser):
    page = browser.open()
    _create_page_with_all_block_types(page)
    tree = lambda: page.locator("#shell-sidebar .bp-tree").inner_text()  # noqa: E731
    baseline = tree()
    for _ in range(10):
        page.click(
            ".bp-block-library-item:has(.bp-block-type-name:has-text('Rich Text')) button:has-text('Add')"
        )
        page.wait_for_timeout(60)
    assert tree() != baseline
    for _ in range(10):
        page.click("text=Undo")
        page.wait_for_timeout(40)
    assert tree() == baseline
    for _ in range(10):
        page.click("text=Redo")
        page.wait_for_timeout(40)
    assert tree() != baseline


def test_all_three_reference_viewports_size_the_preview_frame(browser):
    page = browser.open()
    _create_page_with_all_block_types(page)
    expectations = {"Desktop": "1280px", "Tablet": "834px", "Mobile": "390px"}
    for label, width in expectations.items():
        page.click(f"button:has-text('{label}')")
        page.wait_for_timeout(150)
        actual = page.eval_on_selector("#shell-preview", "el => el.style.width")
        assert actual == width, (label, actual)


def test_unsaved_changes_trigger_a_native_before_unload_prompt(browser):
    """A real cross-browser navigation-away dialog is what rule-required
    behaviour looks like to a user, but relying on Chromium's headless
    dialog plumbing (`page.close(run_before_unload=True)` + a `dialog`
    listener) proved unreliable in this sandbox regardless of the handler's
    own correctness. Dispatching a real, cancelable `beforeunload` event and
    reading `defaultPrevented` tests the SAME contract the browser's own
    unload algorithm checks (per the HTML spec, a browser shows its prompt
    exactly when a `beforeunload` listener calls `preventDefault()` or sets
    `returnValue`), without depending on that automation-specific plumbing."""
    page = browser.open()
    _create_page_with_all_block_types(page)
    page.click("button:has-text('hero (hero-1)')")
    page.wait_for_timeout(100)
    heading = page.locator("#shell-inspector fieldset:has(legend:has-text('Heading')) input").nth(0)
    heading.fill("Unsaved edit")
    page.wait_for_timeout(150)
    assert "Unsaved changes" in page.locator(".bp-dirty-indicator").inner_text()

    default_prevented = page.evaluate("""() => {
            const evt = new Event('beforeunload', {cancelable: true});
            window.dispatchEvent(evt);
            return evt.defaultPrevented || evt.returnValue === '';
        }""")
    assert default_prevented


def test_keyboard_only_create_edit_save_publish(browser):
    """No pointer events anywhere in this test -- every action goes through
    Tab/Enter/Space and typing, per the batch's accessibility requirement."""
    page = browser.open()
    page.keyboard.press("Tab")  # focus "+ New page" (first/only control initially)
    page.keyboard.press("Enter")
    page.wait_for_timeout(150)
    page.locator("#npf-page_id").focus()
    page.keyboard.type("kb-page")
    page.keyboard.press("Tab")
    page.keyboard.type("/about.html")
    page.keyboard.press("Tab")
    page.keyboard.type("blank")
    page.keyboard.press("Tab")
    page.keyboard.type("light-institutional")
    page.keyboard.press("Tab")
    page.keyboard.type("KB title EN")
    page.keyboard.press("Tab")
    page.keyboard.type("KB title FR")
    page.keyboard.press("Tab")
    page.keyboard.type("KB title NL")
    page.wait_for_timeout(600)
    # Tab to "Create page" and activate it with the keyboard.
    for _ in range(8):
        page.keyboard.press("Tab")
        focused = page.evaluate("document.activeElement && document.activeElement.textContent")
        if focused == "Create page":
            break
    page.keyboard.press("Enter")
    page.wait_for_timeout(600)
    assert "kb-page" in page.locator("#shell-sidebar").inner_text()
    assert not browser.console_errors


def test_closing_a_modal_returns_focus_to_the_control_that_opened_it(browser):
    """Closing a dialog re-renders the whole top bar, destroying the button
    that opened it. Without an explicit restore, focus falls to <body> and a
    keyboard operator has to Tab through the entire shell again to get back --
    after every cancelled publish, restore or confirmation.

    axe cannot see this: the markup is correct at every instant, and only the
    transition loses the focus.
    """
    page = browser.open()
    _create_page_with_all_block_types(page)

    page.locator("#bp-action-publish").focus()
    assert (
        page.evaluate("document.activeElement && document.activeElement.id") == "bp-action-publish"
    )

    page.keyboard.press("Enter")
    wait_until(lambda: page.locator(".bp-modal").count() == 1, message="publish modal")
    # Focus moved into the dialog, onto its safe default.
    assert page.evaluate("document.activeElement && document.activeElement.textContent") == "Cancel"

    page.keyboard.press("Escape")
    wait_until(lambda: page.locator(".bp-modal").count() == 0, message="modal closed")
    assert (
        page.evaluate("document.activeElement && document.activeElement.id") == "bp-action-publish"
    )
    assert not browser.console_errors


# ---------------------------------------------------------------------------
# accessibility: axe, best-effort and local-only (see the module docstring)
# ---------------------------------------------------------------------------


def _find_local_axe_source():
    import glob

    candidates = glob.glob("/home/*/.npm/_npx/*/node_modules/axe-core/axe.min.js")
    candidates += glob.glob("/root/.npm/_npx/*/node_modules/axe-core/axe.min.js")
    return candidates[0] if candidates else None


def test_axe_scan_reports_zero_serious_or_critical_violations(browser):
    """Best-effort: this needs a LOCAL copy of axe-core, which is not a
    declared dependency of this repository (same category of gap as
    Playwright itself). Injected via CDP evaluation, which runs in the page
    but is not subject to the page's own script-src CSP -- unlike a `<script
    src>` tag, which the CSP would (correctly) block. Skips cleanly when no
    local axe-core build is found."""
    axe_path = _find_local_axe_source()
    if not axe_path:
        pytest.skip("no local axe-core build found; not a declared dependency")
    axe_source = open(axe_path, encoding="utf-8").read()

    page = browser.open()
    _create_page_with_all_block_types(page)
    page.evaluate(axe_source)
    results = page.evaluate("async () => await axe.run()")
    serious = [v for v in results["violations"] if v["impact"] in ("serious", "critical")]
    assert not serious, json.dumps(serious, indent=2)[:4000]


# ---------------------------------------------------------------------------
# the layout editor: drag, resize, collision, lock, autosave (Batch 13b)
# ---------------------------------------------------------------------------


def _two_block_page(pages_root, server, page_id="layout-page"):
    """A page with two blocks side by side on desktop, so a move can be both
    legal (down) and refused (right, into the neighbour)."""
    doc = builders.minimal_valid_document()
    doc["page_id"] = page_id
    doc["sections"][0]["blocks"][0]["layout"] = builders.layout(
        desktop=(0, 0, 4, 2), tablet=(0, 0, 4, 2), mobile=(0, 0, 4, 2)
    )
    doc["sections"][0]["blocks"].append(
        builders.make_block(
            "hero",
            block_id="blk-hero-2",
            block_layout=builders.layout(
                desktop=(4, 0, 4, 2), tablet=(4, 0, 4, 2), mobile=(0, 2, 4, 2)
            ),
        )
    )
    _save_fixture_page(server, page_id, doc)
    return doc


def _open_layout_page(browser, server, pages_root, page_id="layout-page"):
    _two_block_page(pages_root, server, page_id)
    page = browser.open()
    page.click("text=Open draft")
    wait_until(
        lambda: page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]').count() == 1,
        message="layout editor drawn",
    )
    return page


def _cell_on_disk(pages_root, page_id, block_id, breakpoint):
    saved = json.loads(_draft_path(pages_root, page_id).read_text())
    for section in saved["sections"]:
        for block in section["blocks"]:
            if block["id"] == block_id:
                return block["layout"][breakpoint]
    raise AssertionError(f"no block {block_id!r} on disk")


def test_the_layout_editor_draws_one_tile_per_block(browser, server, pages_root):
    page = _open_layout_page(browser, server, pages_root)
    assert page.locator(".bp-grid-tile").count() == 2
    assert not browser.console_errors


def test_an_arrow_key_moves_a_block_on_the_grid(browser, server, pages_root):
    """The keyboard equivalent is not a lesser path: it goes through the same
    attempt() every pointer drag does."""
    page = _open_layout_page(browser, server, pages_root)
    tile = page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]')
    tile.focus()
    page.keyboard.press("ArrowDown")
    wait_until(
        lambda: "row 2" in tile.get_attribute("aria-label"),
        message="the tile reports its new row",
    )
    assert not browser.console_errors


def test_shift_arrow_resizes_rather_than_moves(browser, server, pages_root):
    page = _open_layout_page(browser, server, pages_root)
    tile = page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]')
    tile.focus()
    before = tile.get_attribute("aria-label")
    page.keyboard.press("Shift+ArrowDown")
    wait_until(lambda: tile.get_attribute("aria-label") != before, message="the tile resized")
    label = tile.get_attribute("aria-label")
    # Taller, and still starting on the same row: a resize, not a move.
    assert "row 1 to 3" in label, label


def test_a_move_that_would_overlap_a_neighbour_is_refused_and_says_why(browser, server, pages_root):
    """A silent snap-back is indistinguishable from a broken builder."""
    page = _open_layout_page(browser, server, pages_root)
    tile = page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]')
    tile.focus()
    before = tile.get_attribute("aria-label")
    page.keyboard.press("ArrowRight")
    wait_until(
        lambda: "overlap" in page.locator("#shell-status").inner_text().lower(),
        message="the refusal is announced",
    )
    status = page.locator("#shell-status").inner_text()
    assert "blk-hero-2" in status, status
    assert tile.get_attribute("aria-label") == before, "the block must not have moved"


def test_a_move_past_the_edge_of_the_grid_is_refused_and_says_why(browser, server, pages_root):
    page = _open_layout_page(browser, server, pages_root)
    tile = page.locator('.bp-grid-tile[data-block-id="blk-hero-2"]')
    tile.focus()
    for _ in range(5):
        page.keyboard.press("ArrowRight")
    status = page.locator("#shell-status").inner_text().lower()
    assert "right edge" in status, status
    assert "12 columns" in status, status


def test_each_breakpoint_is_arranged_separately(browser, server, pages_root):
    """The document has no reflow engine -- every breakpoint's cell is declared
    -- so moving on desktop must not quietly move the block on mobile, where
    the operator may have arranged something different."""
    page = _open_layout_page(browser, server, pages_root)
    tile = page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]')
    tile.focus()
    page.keyboard.press("ArrowDown")
    page.click("text=Save")
    wait_until(
        lambda: _cell_on_disk(pages_root, "layout-page", "blk-hero-1", "desktop")["y"] == 1,
        message="desktop moved on disk",
    )
    assert _cell_on_disk(pages_root, "layout-page", "blk-hero-1", "mobile")["y"] == 0


def test_a_locked_block_refuses_to_move_and_says_so(browser, server, pages_root):
    page = _open_layout_page(browser, server, pages_root)
    tile = page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]')
    tile.focus()
    page.keyboard.press("l")
    wait_until(
        lambda: page.locator('.bp-grid-tile[data-block-id="blk-hero-1"].is-locked').count() == 1,
        message="the tile shows as locked",
    )
    before = tile.get_attribute("aria-label")
    page.keyboard.press("ArrowDown")
    status = page.locator("#shell-status").inner_text().lower()
    assert "locked" in status, status
    assert tile.get_attribute("aria-label") == before, "a locked block must not move"


def test_the_lock_state_is_written_to_the_document(browser, server, pages_root):
    page = _open_layout_page(browser, server, pages_root)
    page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]').focus()
    page.keyboard.press("l")
    page.click("text=Save")
    wait_until(
        lambda: json.loads(_draft_path(pages_root, "layout-page").read_text())["sections"][0][
            "blocks"
        ][0]["locked"]
        is True,
        message="locked persisted",
    )


def test_a_pointer_drag_moves_a_block(browser, server, pages_root):
    page = _open_layout_page(browser, server, pages_root)
    tile = page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]')
    box = tile.bounding_box()
    before = tile.get_attribute("aria-label")

    page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    page.mouse.down()
    # Well past DRAG_THRESHOLD_PX, and down by more than one row height.
    page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2 + 60, steps=8)
    page.mouse.up()

    wait_until(
        lambda: tile.get_attribute("aria-label") != before, message="the drag moved the block"
    )
    assert not browser.console_errors


def test_a_drag_is_one_undo_step_not_one_per_pixel(browser, server, pages_root):
    """history.js refuses a push while a transaction is open, so the many
    intermediate cells a drag passes through cannot each become an undo step.
    The operator undoes the drag, not the last pixel of it."""
    page = _open_layout_page(browser, server, pages_root)
    tile = page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]')
    before = tile.get_attribute("aria-label")
    box = tile.bounding_box()

    page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    page.mouse.down()
    page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2 + 60, steps=12)
    page.mouse.up()
    wait_until(lambda: tile.get_attribute("aria-label") != before, message="drag applied")

    page.click("text=Undo")
    wait_until(
        lambda: page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]').get_attribute(
            "aria-label"
        )
        == before,
        message="one undo returns the block to where it started",
    )


def test_autosave_writes_the_draft_without_anyone_pressing_save(browser, server, pages_root):
    """Safe only because /api/save refuses a stale write (Batch 13a): before
    that, autosaving from two tabs would have destroyed one of them without
    anyone clicking anything."""
    page = _open_layout_page(browser, server, pages_root)
    page.locator('.bp-grid-tile[data-block-id="blk-hero-1"]').focus()
    page.keyboard.press("ArrowDown")
    wait_until(
        lambda: _cell_on_disk(pages_root, "layout-page", "blk-hero-1", "desktop")["y"] == 1,
        timeout_s=20.0,
        message="autosave reached the disk with no Save click",
    )
    assert not browser.console_errors


def test_the_canvas_previews_unsaved_edits(browser, server, pages_root):
    """The preview renders the document in the browser, not the file: a move
    has to show before it is saved, or dragging shows nothing until you save
    and the builder has to autosave on every pointer move to compensate."""
    page = _open_layout_page(browser, server, pages_root)
    notices = page.locator(".bp-canvas-notices").inner_text()
    assert "last saved draft" not in notices.lower(), notices
