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


def test_slider_moves_to_2010_and_updates_the_displayed_year_with_no_console_errors(chromium, site):
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


#: Must match the ::-webkit-slider-thumb/::-moz-range-thumb width in
#: commune.html's CSS (and PYRAMID_THUMB_PX in its JS) -- this is the
#: independently hand-computed expectation the test checks the page's own
#: rendered pixels against, not a value read back from the page.
THUMB_PX = 16


def _thumb_centre_x(page, input_selector, value, minimum, maximum):
    """The x-coordinate (viewport, matching Playwright's bounding_box) of a
    native range input's thumb CENTRE for `value` -- computed independently
    of the page's own renderPyramidTicks(), from the input's own bounding
    box: a thumb's centre travels from half-its-width in from the left edge
    to half-its-width in from the right edge, never edge to edge."""
    box = page.locator(input_selector).bounding_box()
    assert box, f"{input_selector} has no bounding box (not laid out/visible)"
    fraction = (value - minimum) / (maximum - minimum) if maximum > minimum else 0
    usable = box["width"] - THUMB_PX
    return box["x"] + THUMB_PX / 2 + fraction * usable


def _tick_centre_x(page, period):
    """The x-coordinate of the tick labelled `period`'s own centre."""
    tick = page.locator("#pyramidTicks span", has_text=period).first
    box = tick.bounding_box()
    assert box, f"no #pyramidTicks span found for period {period!r}"
    return box["x"] + box["width"] / 2


@pytest.mark.parametrize("width,height", [(1440, 900), (390, 844)])
def test_tick_centres_line_up_with_the_thumb_centre_for_that_year(chromium, site, width, height):
    """The maintainer's own complaint: the slider (~130px in the reported
    screenshot) reads far narrower than the year-tick row it sits above
    (~630px) -- the input must fill its wrapper, #pyramidTicks must live
    inside that same wrapper (not span Play+range+year-readout too), and
    each tick must be placed at the THUMB's centre for that year, not a
    plain linear 0-100% (which ignores the thumb-width inset and misplaces
    every tick, worst at the first/last years)."""
    ctx = _context(chromium, width=width, height=height)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#pyramidPanel[data-state="ready"]')
        page.wait_for_selector("#pyramidHistory.is-active")

        history = json.loads(HISTORY_92094_JSON.read_text(encoding="utf-8"))
        periods = [y["period"] for y in history["years"]]
        minimum, maximum = 0, len(periods) - 1

        range_input = page.locator("#pyramidYear")
        # The input itself must fill its wrapper -- a ~130px input beside
        # ~630px of ticks is exactly the regression being fixed here.
        wrapper_box = page.locator(".pyramid-history-range").bounding_box()
        input_box = range_input.bounding_box()
        assert input_box["width"] >= wrapper_box["width"] - 1, (
            f"#pyramidYear ({input_box['width']:.0f}px) does not fill its "
            f".pyramid-history-range wrapper ({wrapper_box['width']:.0f}px)"
        )

        for period in ("2010", "2018", "2026"):
            assert period in periods, f"fixture drift: 92094 has no {period} year"
            index = periods.index(period)
            range_input.fill(str(index))
            range_input.dispatch_event("input")
            page.wait_for_function(
                "(p) => document.getElementById('pyramidYearOut').textContent === p",
                arg=period,
            )
            thumb_x = _thumb_centre_x(page, "#pyramidYear", index, minimum, maximum)
            tick_x = _tick_centre_x(page, period)
            assert abs(thumb_x - tick_x) <= 4, (
                f"at {width}px, year {period}: thumb centre at {thumb_x:.1f}px, "
                f"tick centre at {tick_x:.1f}px (off by {abs(thumb_x - tick_x):.1f}px)"
            )
    finally:
        ctx.close()


def test_gridlines_show_a_fixed_scale_with_hand_computed_widest_bar_widths(chromium, site):
    """The scale (h.max) was already correct and already fixed across every
    year -- 4416 for 92094 (males 25-29, 2026), the largest male-or-female
    band count across ALL years -- but nothing on screen showed a NUMBER,
    so the maintainer read the (actually identical) bar lengths as if the
    scale might be rescaling under the animation frame to frame. Adds 2-3
    gridlines with count labels, derived only from h.max (BPCharts.
    niceSteps, never hand-typed -- rule 36).

    Every expected number here is hand-computed from the real payload, in
    the docstring's own arithmetic, not copied from the page's own output:
    global max across all years = 4416 (confirmed against the payload);
    2026's widest band (males 25-29) = 4416, so 4416/4416 = 100.000%;
    2010's widest band (males 25-29) = 4201, so 4201/4416 = 95.1314%,
    i.e. 95.13% to 2 d.p., which is within the 0.2 point tolerance below."""
    history = json.loads(HISTORY_92094_JSON.read_text(encoding="utf-8"))
    all_values = []
    for year in history["years"]:
        all_values.extend(year["male"])
        all_values.extend(year["female"])
    global_max = max(all_values)
    assert global_max == 4416, f"fixture drift: 92094's global max is {global_max}, expected 4416"

    def widest_bar_pct(period):
        year = next(y for y in history["years"] if y["period"] == period)
        widest = max(max(year["male"]), max(year["female"]))
        return widest / global_max * 100

    pct_2026 = widest_bar_pct("2026")
    pct_2010 = widest_bar_pct("2010")
    assert pct_2026 == 100.0
    assert abs(pct_2010 - 95.1314) < 0.01, f"hand-computed 2010 pct drifted: {pct_2010}"

    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#pyramidPanel[data-state="ready"]')
        page.wait_for_selector("#pyramidHistory.is-active")

        history_index = {y["period"]: i for i, y in enumerate(history["years"])}
        range_input = page.locator("#pyramidYear")

        def widest_rendered_bar_pct():
            widths = page.evaluate(
                """() => Array.from(document.querySelectorAll('.pyramid-track i'))
                    .map(el => parseFloat(el.style.width) || 0)"""
            )
            assert widths, "no .pyramid-track i bars found"
            return max(widths)

        range_input.fill(str(history_index["2026"]))
        range_input.dispatch_event("input")
        page.wait_for_function(
            "() => document.getElementById('pyramidYearOut').textContent === '2026'"
        )
        rendered_2026 = widest_rendered_bar_pct()
        assert (
            abs(rendered_2026 - pct_2026) < 0.2
        ), f"2026 widest rendered bar {rendered_2026}% != hand-computed {pct_2026}%"
        gridline_labels_2026 = page.evaluate(
            "() => Array.from(document.querySelectorAll('.pyramid-gridline-label')).map(e => e.textContent)"
        )
        assert gridline_labels_2026, "no .pyramid-gridline-label elements rendered"

        range_input.fill(str(history_index["2010"]))
        range_input.dispatch_event("input")
        page.wait_for_function(
            "() => document.getElementById('pyramidYearOut').textContent === '2010'"
        )
        rendered_2010 = widest_rendered_bar_pct()
        assert (
            abs(rendered_2010 - pct_2010) < 0.2
        ), f"2010 widest rendered bar {rendered_2010}% != hand-computed {pct_2010}%"
        gridline_labels_2010 = page.evaluate(
            "() => Array.from(document.querySelectorAll('.pyramid-gridline-label')).map(e => e.textContent)"
        )

        range_input.fill(str(history_index["2018"]))
        range_input.dispatch_event("input")
        page.wait_for_function(
            "() => document.getElementById('pyramidYearOut').textContent === '2018'"
        )
        gridline_labels_2018 = page.evaluate(
            "() => Array.from(document.querySelectorAll('.pyramid-gridline-label')).map(e => e.textContent)"
        )

        # The scale is FIXED across the whole timeline -- the gridline
        # labels must be byte-identical in every frame, proving to the
        # reader (and to this test) that the axis never rescales.
        assert gridline_labels_2026 == gridline_labels_2010 == gridline_labels_2018, (
            f"gridline labels changed between frames: 2026={gridline_labels_2026} "
            f"2010={gridline_labels_2010} 2018={gridline_labels_2018}"
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
