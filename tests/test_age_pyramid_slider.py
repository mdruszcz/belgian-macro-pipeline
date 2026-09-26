"""commune.html's age-pyramid year slider (docs/features -- PR #267 shipped the
data, this batch only wires the page to it).

Follows the shared-server + Playwright pattern tests/test_a4_finishing_fixes.py
and tests/test_sources_page.py already use: one local HTTP server over the
repo, one browser context per test.
"""

from __future__ import annotations

import functools
import http.server
import json
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
COMMUNE_HTML = REPO / "commune.html"
I18N_JS = REPO / "assets" / "i18n.js"
HISTORY_92094_JSON = REPO / "public" / "data" / "demography_history" / "92094.json"

NEW_I18N_KEYS = (
    "cpPyramidPlay",
    "cpPyramidPause",
    "cpPyramidYearLabel",
    "cpPyramidHistorySub",
    "cpPyramidIncomplete",
)


class Quiet(socketserver.TCPServer):
    allow_reuse_address = True

    def log_message(self, *args):  # pragma: no cover - silence the server
        pass

    def handle_error(self, *args):  # pragma: no cover - a closed socket is not a failure
        pass


@pytest.fixture(scope="module")
def site():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO))
    server = Quiet(("127.0.0.1", 0), handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _context(chromium, width=1122, height=900):
    return chromium.new_context(viewport={"width": width, "height": height}, locale="en-US")


# --- static wiring (no browser needed) --------------------------------------


def test_page_fetches_the_history_payload():
    page = COMMUNE_HTML.read_text(encoding="utf-8")
    assert "demography_history/" in page
    assert "AGE_SEX_HISTORY_URL" in page


def test_new_i18n_keys_exist_in_all_three_languages():
    strings_text = I18N_JS.read_text(encoding="utf-8")
    import re

    blocks = {}
    for lang in ("en", "fr", "nl"):
        m = re.search(rf"\n  {lang}: \{{(.*?)\n  \}},\n", strings_text, re.DOTALL)
        assert m, f"no {lang} table found in assets/i18n.js"
        blocks[lang] = m.group(1)

    for key in NEW_I18N_KEYS:
        for lang, block in blocks.items():
            assert f"{key}:" in block, f"{lang} is missing new key {key}"


def test_fixture_92094_history_has_multiple_years():
    """Sanity check on the fixture the browser test below drives: Namur's
    history payload really does span more than one year, so moving the
    slider to 2010 is testing a real frame change, not a no-op."""
    data = json.loads(HISTORY_92094_JSON.read_text(encoding="utf-8"))
    periods = [y["period"] for y in data["years"]]
    assert "2010" in periods
    assert len(periods) > 10


# --- browser behaviour -------------------------------------------------------


def test_slider_moves_to_2010_and_updates_the_displayed_year_with_no_console_errors(
    chromium, site
):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.on("pageerror", lambda exc: errors.append(str(exc)))

        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#pyramidPanel[data-state="ready"]')
        page.wait_for_selector("#pyramidHistory.is-active")

        year_out = page.locator("#pyramidYearOut")
        before = year_out.inner_text()

        history = json.loads(HISTORY_92094_JSON.read_text(encoding="utf-8"))
        index_2010 = next(i for i, y in enumerate(history["years"]) if y["period"] == "2010")

        range_input = page.locator("#pyramidYear")
        range_input.fill(str(index_2010))
        range_input.dispatch_event("input")

        page.wait_for_function(
            "() => document.getElementById('pyramidYearOut').textContent === '2010'"
        )
        after = year_out.inner_text()

        assert after == "2010"
        assert after != before or before == "2010"

        # A missing buyer-origin-flows payload for this commune is expected
        # (renderFlows() degrades to a hidden section, not an error), and
        # Chromium logs every 404'd fetch as a console error regardless of
        # who consumes the response -- that pre-existing, unrelated noise is
        # filtered out here rather than asserting on the whole console.
        real_errors = [e for e in errors if "404" not in e]
        assert not real_errors, f"console/page errors during slider use: {real_errors}"
    finally:
        ctx.close()


def test_play_button_is_keyboard_reachable_and_toggles_aria_pressed(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#pyramidPanel[data-state="ready"]')
        page.wait_for_selector("#pyramidHistory.is-active")

        play = page.locator("#pyramidPlay")
        assert play.get_attribute("aria-pressed") == "false"
        play.click()
        assert play.get_attribute("aria-pressed") == "true"
        play.click()
        assert play.get_attribute("aria-pressed") == "false"
    finally:
        ctx.close()


def test_no_horizontal_overflow_at_mobile_width(chromium, site):
    ctx = _context(chromium, width=390, height=844)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#pyramidPanel[data-state="ready"]')
        page.wait_for_selector("#pyramidHistory.is-active")

        scroll_width = page.evaluate("document.documentElement.scrollWidth")
        client_width = page.evaluate("document.documentElement.clientWidth")
        assert scroll_width <= client_width + 1, (
            f"horizontal overflow at 390px: scrollWidth={scroll_width} "
            f"clientWidth={client_width}"
        )
    finally:
        ctx.close()


def test_commune_without_history_payload_shows_default_view_unchanged(chromium, site):
    """Every current commune happens to have a demography_history payload in
    this checkout (PR #267 covered all 565), so the "history absent" degrade
    path is exercised by blocking that one request rather than hunting for a
    commune that has none -- an existing visitor on a day the history export
    failed must still see exactly today's single-year pyramid, slider
    hidden, per the brief's "optional, like the others" rule."""
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.route(
            "**/demography_history/92094.json",
            lambda route: route.fulfill(status=404, body="not found"),
        )
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#pyramidPanel[data-state="ready"]')
        history_box = page.locator("#pyramidHistory")
        assert "is-active" not in (history_box.get_attribute("class") or "")
        # The default single-year chart still renders -- rows present, exactly
        # as it did before this batch.
        assert page.locator(".pyramid-row").count() > 0
    finally:
        ctx.close()
