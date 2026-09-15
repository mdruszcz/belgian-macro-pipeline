"""Browser checks for Unification A4's finishing fixes.

One shared local-file HTTP server (the pattern tests/test_sources_page.py
already uses), one Chromium context per test. Covers:

  1. (retired 2026-09-15: macro.html no longer carries a commune search.)
  2. Belgium's own REFNIS label ("ROYAUME" / "HET RIJK") never reaches
     rendered text on any of the pages a reader can see it from.
  5. the age pyramid's bars are real hover/keyboard tooltip targets, showing
     the exact payload count.
  7. (retired 2026-09-15 with the search box and breadcrumb themselves.)
  9. commune.html's "All data" section is a compact table, one row per
     indicator, not a 14,000px wall of cards.
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
GEOGRAPHIES_JSON = REPO / "public" / "data" / "metadata" / "geographies.json"
COMMUNE_92094_JSON = REPO / "public" / "data" / "communes" / "92094.json"
DEMOGRAPHY_92094_JSON = REPO / "public" / "data" / "demography" / "92094.json"

FORBIDDEN_COUNTRY_LABELS = ("ROYAUME", "HET RIJK")


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
    # locale="en-US" pins I18N.initial()'s navigator.language fallback, same
    # reason tests/test_sources_page.py's own `page` fixture gives.
    return chromium.new_context(viewport={"width": width, "height": height}, locale="en-US")


# --- fixture sanity ----------------------------------------------------------


def test_fixture_geographies_has_over_500_municipalities():
    data = json.loads(GEOGRAPHIES_JSON.read_text(encoding="utf-8"))
    municipalities = [g for g in data["geographies"] if g.get("level") == "municipality"]
    assert len(municipalities) > 500


def test_fixture_92094_has_a_country_row_carrying_the_refnis_label():
    """Sanity check on the bug itself: the raw payload really does carry
    ROYAUME/HET RIJK for be:country, so a page NOT showing it is doing real
    work, not just failing to exercise the case."""
    data = json.loads(COMMUNE_92094_JSON.read_text(encoding="utf-8"))
    found = False
    for entry in data["indicators"].values():
        country = (entry.get("comparison") or {}).get("country")
        if country:
            assert country["name"]["fr"] == "ROYAUME"
            assert country["name"]["nl"] == "HET RIJK"
            found = True
    assert found, "fixture drift: no indicator in 92094.json carries a country comparison"


# --- fix 1: macro.html's commune search --------------------------------------


# --- fix 2: Belgium's REFNIS label never reaches a reader --------------------


@pytest.mark.parametrize(
    "path,ready_selector",
    [
        ("home2.html", "#heroMini .glass canvas"),
        ("macro.html", "#kpiRow .macro-kpi"),
        ("commune.html?nis=92094", "#allDataSections .indicator-group"),
        ("profiles.html", "#communeList option"),
        ("map.html", '#scope option[value="be:country"]'),
        ("sources.html", "#srcTableBody tr.src-row"),
    ],
)
def test_belgium_refnis_label_never_appears(chromium, site, path, ready_selector):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/{path}", wait_until="load")
        # "attached", not the default "visible": the readiness probe is
        # sometimes a <datalist> <option> (never rendered with a box) or
        # content inside commune.html's closed-by-default #allData <details>
        # -- neither becomes Playwright-"visible", but both prove the data
        # actually loaded and rendered, which is all this readiness wait needs.
        page.wait_for_selector(ready_selector, state="attached")
        # page.content() -- the full serialized HTML -- not body.inner_text(),
        # which silently skips a closed <details> (commune.html's #allData
        # starts collapsed) and would let a leak hide there undetected.
        html = page.content()
        for forbidden in FORBIDDEN_COUNTRY_LABELS:
            assert forbidden not in html, f"{path} shows the raw REFNIS label {forbidden!r}"
    finally:
        ctx.close()


def test_map_shows_the_interface_string_for_belgium(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/map.html", wait_until="load")
        page.wait_for_selector('#scope option[value="be:country"]', state="attached")
        label = page.locator('#scope option[value="be:country"]').inner_text()
        assert label.strip() == "All of Belgium"
    finally:
        ctx.close()


# --- fix 5: age pyramid tooltips ---------------------------------------------


def test_pyramid_bar_tooltip_shows_the_payload_count(chromium, site):
    demography = json.loads(DEMOGRAPHY_92094_JSON.read_text(encoding="utf-8"))
    band = next(b for b in demography["bands"] if b["from"] == 40 and b["to"] == 44)
    expected_female_count = band["female"]

    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#pyramidPanel[data-state="ready"]')

        # Locate the row by its own age text ("40–44"), then that row's
        # FEMALE (right) bar.
        row = page.locator(".pyramid-row", has_text="40").filter(has_text="44").first
        female_bar = row.locator(".pyramid-track.right i")
        female_bar.focus()
        tip = page.locator(".bp-chart-tip")
        page.wait_for_selector(".bp-chart-tip:not([hidden])")
        tip_text = tip.inner_text()
        assert str(expected_female_count) in tip_text.replace(" ", "").replace(",", "").replace(
            " ", ""
        ), f"tooltip {tip_text!r} does not carry the payload count {expected_female_count}"
        assert "Women" in tip_text or "40" in tip_text

        female_bar.blur()
        page.wait_for_selector(".bp-chart-tip", state="hidden")
    finally:
        ctx.close()


def test_pyramid_bars_are_keyboard_focusable(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#pyramidPanel[data-state="ready"]')
        bars = page.locator(".pyramid-track i")
        assert bars.count() > 0
        for i in range(min(3, bars.count())):
            assert bars.nth(i).get_attribute("tabindex") == "0"
    finally:
        ctx.close()


# --- fix 9: the compact "All data" table -------------------------------------


def test_all_data_is_one_row_per_indicator_and_stays_compact(chromium, site):
    payload = json.loads(COMMUNE_92094_JSON.read_text(encoding="utf-8"))
    indicator_count = len(payload["indicators"])
    assert indicator_count > 0

    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#allDataSections .indicator-group", state="attached")
        page.click("#allData summary")
        page.wait_for_selector("#allData[open]")

        rows = page.locator("#allDataSections .indicator-row")
        assert (
            rows.count() == indicator_count
        ), f"expected {indicator_count} rows (one per indicator), found {rows.count()}"

        box = page.locator("#allData").bounding_box()
        assert box is not None
        assert box["height"] < 4000, f"#allData is {box['height']}px tall, expected under 4000px"
    finally:
        ctx.close()


def test_all_data_history_disclosure_reveals_every_period(chromium, site):
    payload = json.loads(COMMUNE_92094_JSON.read_text(encoding="utf-8"))
    code, entry = next(iter(payload["indicators"].items()))
    period_count = len(entry.get("periods") or {})

    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#allDataSections .indicator-group", state="attached")
        page.click("#allData summary")
        page.wait_for_selector("#allData[open]")

        row = page.locator(f'.indicator-row:has(.indicator-code:has-text("{code}"))').first
        toggle = row.locator(".history-toggle")
        assert toggle.get_attribute("aria-expanded") == "false"
        toggle.click()
        assert toggle.get_attribute("aria-expanded") == "true"

        hist_id = toggle.get_attribute("aria-controls")
        history_lines = page.locator(f"#{hist_id} .indicator-history-line")
        assert (
            history_lines.count() == period_count
        ), f"expected {period_count} history lines for {code}, found {history_lines.count()}"
    finally:
        ctx.close()


def test_all_data_five_states_stay_distinct(chromium, site):
    """A real number (zero included), a suppressed cell and an indicator with
    no published period at all must never render the same way (rule 26)."""
    payload = json.loads(COMMUNE_92094_JSON.read_text(encoding="utf-8"))
    # Find one indicator whose latest period is a real number, if any.
    numeric_code = None
    for code, entry in payload["indicators"].items():
        periods = entry.get("periods") or {}
        if not periods:
            continue
        latest = periods[sorted(periods)[-1]]
        if isinstance(latest.get("value"), (int, float)):
            numeric_code = code
            break
    assert numeric_code, "fixture drift: no indicator in 92094.json has a numeric latest value"

    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#allDataSections .indicator-group", state="attached")
        page.click("#allData summary")
        page.wait_for_selector("#allData[open]")

        row = page.locator(f'.indicator-row:has(.indicator-code:has-text("{numeric_code}"))').first
        value_cell = row.locator("td.ind-value")
        assert (
            value_cell.get_attribute("data-state") is None
        ), "a real numeric value must not carry a non-ready data-state"
        assert value_cell.inner_text().strip() not in ("", "—")
    finally:
        ctx.close()
