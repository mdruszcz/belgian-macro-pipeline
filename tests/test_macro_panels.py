"""Browser contract for macro.html's panel switcher -- Batch A1.4
(docs/features/site_unification.md, "Macro/Micro : panneaux thématiques").

Served over real HTTP (not file://) for the same reason
tests/site/test_iframe_contract.py is: `history.pushState`/`replaceState`
and `location.hash` behave the same either way here, but a relative fetch of
public/data/*.json -- which this page needs for its layout and its
localStorage-backed "last panel" behaviour -- is friendlier to reason about
against a real origin, and it matches every other browser test in this repo.
"""

from __future__ import annotations

import functools
import http.server
import json
import socketserver
import threading
from pathlib import Path

import pytest
import yaml
from playwright.sync_api import expect

REPO_ROOT = Path(__file__).resolve().parents[1]

DESKTOP_VIEWPORT = {"width": 1280, "height": 900}
PHONE_VIEWPORT = {"width": 390, "height": 844}

#: Every panel this batch adds, in sidebar order.
PANEL_IDS = [
    "apercu",
    "croissance",
    "prix",
    "emploi",
    "conjoncture",
    "finances-publiques",
    "europe",
]


@pytest.fixture(scope="module")
def site():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO_ROOT))

    class Quiet(socketserver.TCPServer):
        allow_reuse_address = True

        def log_message(self, *args):  # pragma: no cover - silence the server
            pass

        def handle_error(self, *args):  # pragma: no cover - a closed socket is not a failure
            pass

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


@pytest.fixture(scope="module")
def browser(chromium):
    """The session's Chromium (tests/conftest.py); each test opens its own
    context so nothing but the process is shared between tests."""
    return chromium


def _national() -> dict:
    return json.loads(
        (REPO_ROOT / "public" / "data" / "national.json").read_text(encoding="utf-8")
    )["indicators"]


def _panel_chart(chart_id: str) -> dict:
    layout = yaml.safe_load(
        (REPO_ROOT / "config" / "national_sections.yaml").read_text(encoding="utf-8")
    )
    charts = {item["id"]: item for item in layout.get("panel_charts") or []}
    return charts[chart_id]


def test_a_legacy_anchor_resolves_to_its_panel_and_rewrites_the_url(browser, site):
    """#growth was the GDP-history section's own anchor before this batch;
    it is now inside the "croissance" panel, and CLAUDE.md rule 31 says the
    old link must still work."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#growth", wait_until="load")
        page.wait_for_selector("#croissance:not([hidden])")
        assert page.evaluate("location.hash") == "#croissance"
        assert not page.is_visible("#apercu")
    finally:
        context.close()


def test_clicking_a_panel_then_going_back_shows_the_previous_one(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click('.bp-sidebar-nav a[href="#prix"]')
        page.wait_for_selector("#prix:not([hidden])")
        page.go_back()
        page.wait_for_selector("#apercu:not([hidden])")
    finally:
        context.close()


def test_a_shown_panels_first_canvas_has_real_width(browser, site):
    """The regression this whole switching mechanism exists to avoid: a
    canvas measured while its panel is `hidden` is 0px wide -- or, worse,
    PERMANENTLY pinned to assets/belpulse/charts.js's fallback (300px):
    that fallback gets written back as the canvas's own inline style, so a
    later, correctly-timed redraw still measures the stale value rather
    than the container. `width > 0` alone would not have caught 300px
    sitting inside a ~500px card, which is exactly what the lead's review
    of PR #170's screenshots found -- so this also checks the canvas fills
    (at least 90% of) the wrapper that gives it its size."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click('.bp-sidebar-nav a[href="#croissance"]')
        page.wait_for_selector("#croissance:not([hidden])")
        page.wait_for_function("document.getElementById('historyCanvas').width > 0")
        # The onShow redraw is deferred one animation frame (panels.js) on
        # top of that -- wait for the canvas's OWN rendered width to reach
        # something close to its wrapper's, not just for the first non-zero
        # value, which the stale-300px bug would also produce immediately.
        page.wait_for_function(
            "(function(){"
            "var c = document.getElementById('historyCanvas');"
            "var wrap = c.parentElement;"
            "return c.getBoundingClientRect().width >= wrap.clientWidth * 0.9;"
            "})()"
        )
        canvas_width = page.evaluate(
            "document.getElementById('historyCanvas').getBoundingClientRect().width"
        )
        wrap_width = page.evaluate(
            "document.getElementById('historyCanvas').parentElement.clientWidth"
        )
        assert canvas_width >= wrap_width * 0.9, (
            f"the GDP history canvas is {canvas_width}px wide inside a {wrap_width}px card "
            "-- looks like the stale-300px-fallback bug, not a real fit"
        )
    finally:
        context.close()


def test_no_growth_driver_label_is_truncated(browser, site):
    """The lead's review of PR #170's screenshots: "Dépenses de
    consommation des APU" was rendered as "Dépen...". A statistical label
    losing its ending is not the same label -- CLAUDE.md's own rule that a
    reader must never be shown a number (or a name) that isn't real.
    Checks BOTH the CSS (no `text-overflow: ellipsis` computed on the
    element) and the actual layout (nothing is wider than its own box),
    since either one alone could pass while the other still clips text."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click('.bp-sidebar-nav a[href="#croissance"]')
        page.wait_for_selector("#croissance:not([hidden])")
        page.wait_for_selector("#contribList li .name")
        names = page.locator("#contribList li .name")
        count = names.count()
        assert count > 0, "the growth-drivers list rendered no rows"
        for i in range(count):
            el = names.nth(i)
            overflow = el.evaluate("e => getComputedStyle(e).textOverflow")
            assert overflow != "ellipsis", f"row {i} ({el.text_content()!r}) is set to ellipsis"
            clipped = el.evaluate("e => e.scrollWidth > e.clientWidth + 1")
            assert not clipped, f"row {i} ({el.text_content()!r}) overflows its own box"
    finally:
        context.close()


def test_reload_keeps_the_panel_the_hash_names(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#prix", wait_until="load")
        page.wait_for_selector("#prix:not([hidden])")
        page.reload(wait_until="load")
        page.wait_for_selector("#prix:not([hidden])")
        assert page.evaluate("location.hash") == "#prix"
    finally:
        context.close()


def test_at_phone_width_the_select_replaces_the_sidebar(browser, site):
    context = browser.new_context(viewport=PHONE_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        assert page.is_visible("#panelPick")
        assert not page.is_visible(".bp-sidebar")
        page.select_option("#panelPick", "emploi")
        page.wait_for_selector("#emploi:not([hidden])")
        assert page.evaluate("location.hash") == "#emploi"
    finally:
        context.close()


@pytest.mark.parametrize("panel_id", PANEL_IDS)
def test_exactly_one_panel_is_visible_after_choosing_each_one(browser, site, panel_id):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click(f'.bp-sidebar-nav a[href="#{panel_id}"]')
        page.wait_for_selector(f"#{panel_id}:not([hidden])")
        visible = page.eval_on_selector_all(
            "[data-panel]", "els => els.filter(e => !e.hidden).map(e => e.id)"
        )
        assert visible == [panel_id], visible
        # The sidebar agrees with what is actually shown.
        current = page.eval_on_selector_all(
            '.bp-sidebar-nav a[aria-current="page"]', "els => els.map(e => e.getAttribute('href'))"
        )
        assert current == [f"#{panel_id}"], current
    finally:
        context.close()


def test_choosing_a_panel_moves_focus_to_its_heading(browser, site):
    """Keyboard/screen-reader users need to land somewhere after a panel
    switch, not stay wherever the sidebar link was."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click('.bp-sidebar-nav a[href="#conjoncture"]')
        page.wait_for_selector("#conjoncture:not([hidden])")
        page.wait_for_function(
            "document.activeElement && document.activeElement.closest('#conjoncture') !== null"
        )
        focused_tag = page.evaluate("document.activeElement.tagName")
        assert focused_tag == "H2", focused_tag
    finally:
        context.close()


def test_loading_the_page_fresh_does_not_move_focus(browser, site):
    """`onShow` fires for the default panel too (macro.html's chart redraw
    needs it), but a first load must not steal focus from the address bar --
    only a user CHOICE does."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        focused_tag = page.evaluate("document.activeElement && document.activeElement.tagName")
        assert focused_tag != "H2", "the default panel's heading stole focus on load"
    finally:
        context.close()


# --- BATCH A1.4b: history charts inside Prix/Emploi/Conjoncture ---------------

#: {panel id: (chart card id, canvas id)}, matching macro.html's camelId() of
#: each config/national_sections.yaml `panel_charts[].id`.
PANEL_CHARTS = {
    "prix": ("prices-chart", "pricesChartCanvas"),
    "emploi": ("employment-chart", "employmentChartCanvas"),
    "conjoncture": ("business-cycle-chart", "businessCycleChartCanvas"),
}


def _open_panel_chart(browser, site, panel_id, canvas_id, lang=None):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    if lang:
        # Pinned rather than left to the browser's own locale, the same way
        # tests/test_charts_tooltip.py pins home2.html's -- the value this
        # test independently recomputes below has to be in the SAME language
        # the page actually rendered, on every machine this runs on.
        context.add_init_script(
            f"try{{localStorage.setItem('belpulse-lang', '{lang}');}}catch(e){{}}"
        )
    page = context.new_page()
    page.goto(f"{site}/macro.html", wait_until="load")
    page.wait_for_selector("#apercu:not([hidden])")
    page.click(f'.bp-sidebar-nav a[href="#{panel_id}"]')
    page.wait_for_selector(f"#{panel_id}:not([hidden])")
    page.wait_for_function(f"document.getElementById('{canvas_id}').width > 0")
    # Same "wait for the real rendered width, not just a non-zero one" as the
    # GDP-history canvas test above -- the stale-300px-fallback bug would
    # also produce an early non-zero width.
    page.wait_for_function(
        "(function(){"
        f"var c = document.getElementById('{canvas_id}');"
        "var wrap = c.parentElement;"
        "return c.getBoundingClientRect().width >= wrap.clientWidth * 0.9;"
        "})()"
    )
    return context, page


@pytest.mark.parametrize("panel_id", ["prix", "emploi", "conjoncture"])
def test_a_panel_charts_canvas_fills_its_cards_inner_width(browser, site, panel_id):
    chart_id, canvas_id = PANEL_CHARTS[panel_id]
    context, page = _open_panel_chart(browser, site, panel_id, canvas_id)
    try:
        canvas_width = page.evaluate(
            f"document.getElementById('{canvas_id}').getBoundingClientRect().width"
        )
        wrap_width = page.evaluate(
            f"document.getElementById('{canvas_id}').parentElement.clientWidth"
        )
        assert (
            canvas_width >= wrap_width * 0.9
        ), f"{chart_id}'s canvas is {canvas_width}px wide inside a {wrap_width}px wrapper"
    finally:
        context.close()


def _hover_until_tip(page, x, y, budget_ms=5000, step_ms=200):
    """Hover (x, y) and wait for the shared tooltip to show, re-nudging the
    pointer on every poll. A single mouse.move fires one mousemove; if the
    chart's hover layer attaches after that (data arriving late on a slow
    runner), nothing re-triggers it. Moving by one pixel on each poll gives a
    late-attached handler an event to answer. Waits on the real condition --
    the tooltip element visible -- never on a fixed sleep, and fails loudly
    with the same message as before once the budget is spent."""
    tip_visible = (
        "document.querySelector('.bp-chart-tip') && "
        "!document.querySelector('.bp-chart-tip').hidden"
    )
    waited = 0
    nudge = 0
    while True:
        page.mouse.move(x + nudge, y)
        if page.evaluate(f"() => !!({tip_visible})"):
            return
        if waited >= budget_ms:
            raise AssertionError(
                f"tooltip did not appear within {budget_ms} ms of hovering ({x}, {y})"
            )
        page.wait_for_timeout(step_ms)
        waited += step_ms
        nudge = 1 - nudge  # 0, 1, 0, 1 ... a one-pixel wiggle, never leaving the point


def test_hovering_the_last_hicp_point_shows_the_real_value_and_period(browser, site):
    chart_id, canvas_id = PANEL_CHARTS["prix"]
    series = _panel_chart(chart_id)["series"]
    assert series == ["HICP"], series
    entry = _national()["HICP"]
    periods = sorted(entry["periods"].keys())
    last_period = periods[-1]
    last_value = entry["periods"][last_period]["value"]
    assert last_value is not None, "fixture assumption broken: HICP's last period has no value"

    context, page = _open_panel_chart(browser, site, "prix", canvas_id, lang="en")
    try:
        canvas = page.locator(f"#{canvas_id}")
        box = canvas.bounding_box()
        assert box, "HICP canvas has no layout box"
        # Failed twice on 2026-09-16 in CI (develop push after #209, then the
        # bot PR #216) with "Timeout 5000ms exceeded", 186 others passing,
        # and passed on rerun both times -- a race, not a regression.
        # _open_panel_chart waits for the canvas to have its LAYOUT (width
        # >= 90% of its wrapper), not for the chart's DATA to be drawn or its
        # hover layer to be attached: macro.html draws the series and calls
        # BPCharts.attachTooltip only once national.json has arrived, and on
        # a loaded runner that lands AFTER a single mouse.move. One mousemove
        # fires once; nothing re-fires it when the hover layer appears later,
        # so the wait expires. Two fixes, both on real conditions rather than
        # a longer sleep: (1) wait for the hover layer to be bound -- macro.html
        # sets canvas.dataset.bpBound = '1' in the same synchronous block that
        # calls attachTooltip after the first draw; (2) re-nudge the mouse
        # while polling, so a redraw that lands after the first move (the
        # ResizeObserver redraw is rAF-coalesced, i.e. asynchronous) still gets
        # a mousemove to answer. Same 5 s budget as before.
        page.wait_for_function(
            f"document.getElementById('{canvas_id}').dataset.bpBound === '1'",
            timeout=15000,
        )
        _hover_until_tip(page, box["x"] + box["width"] - 3, box["y"] + box["height"] / 2)
        tip_text = page.locator(".bp-chart-tip").inner_text()
        assert last_period in tip_text, f"tooltip missing period: {tip_text!r}"

        # Independently formatted in the SAME runtime/locale the tooltip
        # uses (pinned to 'en' above), the same way MapUI.formatValue does
        # it (assets/commune_map.js): a DECLARED decimals count is a
        # minimum as well as a maximum, and HICP's unit is "percent_yy" --
        # a trailing "%" MapUI adds, not a raw unit code (rule 7).
        decimals = entry.get("decimals", 1)
        expected_value_text = (
            page.evaluate(
                "([v, d]) => v.toLocaleString('en', {minimumFractionDigits: d, maximumFractionDigits: d})",
                [last_value, decimals],
            )
            + "%"
        )
        assert (
            expected_value_text in tip_text
        ), f"tooltip missing value {expected_value_text!r}: {tip_text!r}"
    finally:
        context.close()


@pytest.mark.parametrize("panel_id", ["prix", "emploi", "conjoncture"])
def test_a_panel_charts_data_table_lists_every_drawn_period(browser, site, panel_id):
    chart_id, canvas_id = PANEL_CHARTS[panel_id]
    series = _panel_chart(chart_id)["series"]
    national = _national()
    # All the periods any drawn series carries -- BPCharts aligns every
    # series on the UNION of their periods, so a period only one of the two
    # Conjoncture series has still gets a row (the other becomes a gap).
    all_periods: set[str] = set()
    for code in series:
        all_periods |= set(national[code]["periods"].keys())
    assert all_periods, f"{chart_id}: no periods to check against"

    context, page = _open_panel_chart(browser, site, panel_id, canvas_id)
    try:
        card = page.locator(f"#{chart_id}")
        details = card.locator("details.bp-chart-data")
        assert details.count() == 1, f"no data table for {chart_id}"
        rows = details.locator("tbody tr")
        expected_rows = len(all_periods) * len(series)
        # The table is rebuilt whenever BPCharts redraws the chart (resize,
        # theme, language, panel shown), so a single count() can land in the
        # instant between clearing and refilling it: CI once read 0 rows and
        # then 192 while formatting the failure message. Wait for the settled
        # count instead of sampling it once.
        try:
            expect(rows).to_have_count(expected_rows, timeout=10_000)
        except AssertionError as exc:
            raise AssertionError(
                f"{chart_id}: table has {rows.count()} rows, expected {expected_rows} "
                f"({len(series)} series x {len(all_periods)} aligned periods)"
            ) from exc
        table_text = details.locator("table").text_content()
        for period in all_periods:
            assert period in table_text, f"{chart_id}: period {period} missing from its data table"
    finally:
        context.close()


def test_a_compact_empty_cards_message_only_renders_for_the_state_that_owns_it(browser, site):
    """A2b audit, Finding 4: item 7's compact empty-state rule used
    `!important` (`.card-compact-empty .bp-state-message{display:flex
    !important; ...!important}`), which overrides components.css's state
    machine UNCONDITIONALLY -- not just for the `unavailable` state the card
    ships in today. `#map` and `#news` are hard-set to `unavailable` right
    now (renderUnavailable()), so this was latent, but the very first time
    either slot carries real data and moves to another state (`loading` while
    it fetches, `ready` once drawn), "Not available yet" would render right
    next to it -- collapsing distinct states into one, which CLAUDE.md rule
    26 forbids. The fix scopes the override to
    `.card-compact-empty[data-state="unavailable"]` instead of `!important`,
    so this drives the card through the OTHER real state names the page
    itself uses (assets/belpulse/components.css) and checks the message
    actually disappears, the way it does for every other card on the page."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#map .bp-state-message")

        # Sanity check: as shipped, #map is `unavailable` and the message
        # does render -- otherwise the states below would pass vacuously.
        assert page.get_attribute("#map", "data-state") == "unavailable"
        shown = page.eval_on_selector(
            "#map .bp-state-message", "el => getComputedStyle(el).display"
        )
        assert shown == "flex", "message should render while #map is unavailable"

        for other_state in ("loading", "ready", "suppressed"):
            page.evaluate(
                "(s) => { document.getElementById('map').dataset.state = s; }", other_state
            )
            hidden = page.eval_on_selector(
                "#map .bp-state-message", "el => getComputedStyle(el).display"
            )
            assert hidden == "none", (
                f"#map still renders 'Not available yet' with data-state={other_state!r} "
                "-- the !important override is back"
            )
    finally:
        context.close()
