"""Browser contract for micro.html's panel switcher -- Batch A3.1c
(docs/features/site_unification.md, "Macro/Micro : panneaux thématiques").

A clone of tests/test_macro_panels.py, adapted to micro.html's own panels and
card ids. Served over real HTTP (not file://) for the same reason
tests/site/test_iframe_contract.py is: `history.pushState`/`replaceState` and
`location.hash` behave the same either way here, but a relative fetch of
public/data/*.json -- which this page needs for its layout, its aggregates
and its localStorage-backed "last panel" behaviour -- is friendlier to reason
about against a real origin, and it matches every other browser test in this
repo.
"""

from __future__ import annotations

import functools
import http.server
import socketserver
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

DESKTOP_VIEWPORT = {"width": 1280, "height": 900}
PHONE_VIEWPORT = {"width": 390, "height": 844}

#: Every panel this batch adds, in sidebar order.
PANEL_IDS = [
    "apercu",
    "population",
    "revenus",
    "emploi",
    "logement",
    "entreprises",
    "comparaisons",
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


def _wait_ready(page):
    """Wait until micro.html's own `boot()` has actually finished, not just
    until the DOM has loaded. `#apercu` is never `hidden` in the static
    markup (a no-JS reader still gets it), so `#apercu:not([hidden])` is
    true from the very first paint -- long before `boot()`'s own fetches
    (national.json, aggregates.json, the commune geojson, ...) resolve and
    `BPPanels.init()` actually attaches its click/hash listeners. A click
    fired in that window falls through to the browser's own default
    same-page anchor jump, and BPPanels later picks up that hash through its
    OWN boot-time resolution (`show(initial, false)` -- `focusHeading`
    false), which shows the right panel but never moves focus: same visible
    outcome, no focus, a flaky test rather than a real bug. `#kpiRow` is
    only ever populated by `renderAll()`, called synchronously right before
    `BPPanels.init()` with no `await` in between -- so waiting for it is
    waiting for panels.js to be live."""
    page.wait_for_selector("#apercu:not([hidden])")
    page.wait_for_function("document.getElementById('kpiRow').children.length > 0")


def test_a_legacy_anchor_resolves_to_its_panel_and_rewrites_the_url(browser, site):
    """#housing was the housing-market section's own anchor before this
    batch; it is now inside the "logement" panel, and CLAUDE.md rule 31 says
    the old link must still work."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html#housing", wait_until="load")
        page.wait_for_selector("#logement:not([hidden])")
        assert page.evaluate("location.hash") == "#logement"
        assert not page.is_visible("#apercu")
    finally:
        context.close()


def test_clicking_a_panel_then_going_back_shows_the_previous_one(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        _wait_ready(page)
        page.click('.bp-sidebar-nav a[href="#revenus"]')
        page.wait_for_selector("#revenus:not([hidden])")
        page.go_back()
        _wait_ready(page)
    finally:
        context.close()


def test_a_shown_panels_first_canvas_has_real_width(browser, site):
    """The regression this whole switching mechanism exists to avoid: a
    canvas measured while its panel is `hidden` is 0px wide -- or, worse,
    PERMANENTLY pinned to assets/belpulse/charts.js's fallback (300px):
    that fallback gets written back as the canvas's own inline style, so a
    later, correctly-timed redraw still measures the stale value rather than
    the container. `width > 0` alone would not have caught 300px sitting
    inside a wider card, so this also checks the canvas fills (at least 90%
    of) the wrapper that gives it its size -- the housing bar chart, moved
    into the logement panel by this batch."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        _wait_ready(page)
        page.click('.bp-sidebar-nav a[href="#logement"]')
        page.wait_for_selector("#logement:not([hidden])")
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
            f"the housing-sales canvas is {canvas_width}px wide inside a {wrap_width}px card "
            "-- looks like the stale-300px-fallback bug, not a real fit"
        )
    finally:
        context.close()


def test_the_comparaisons_panels_ranking_canvas_has_real_width(browser, site):
    """Same regression, for the comparison ranking chart moved into the
    comparaisons panel."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        _wait_ready(page)
        page.click('.bp-sidebar-nav a[href="#comparaisons"]')
        page.wait_for_selector("#comparaisons:not([hidden])")
        page.wait_for_function("document.getElementById('compareCanvas').width > 0")
        page.wait_for_function(
            "(function(){"
            "var c = document.getElementById('compareCanvas');"
            "var wrap = c.parentElement;"
            "return c.getBoundingClientRect().width >= wrap.clientWidth * 0.9;"
            "})()"
        )
        canvas_width = page.evaluate(
            "document.getElementById('compareCanvas').getBoundingClientRect().width"
        )
        wrap_width = page.evaluate(
            "document.getElementById('compareCanvas').parentElement.clientWidth"
        )
        assert canvas_width >= wrap_width * 0.9, (
            f"the comparison ranking canvas is {canvas_width}px wide inside a {wrap_width}px "
            "card -- looks like the stale-300px-fallback bug, not a real fit"
        )
    finally:
        context.close()


def test_no_extra_list_label_is_truncated(browser, site):
    """The same lead's-review lesson macro.html's own test protects: a
    statistical label losing its ending is not the same label. Checks BOTH
    the CSS (no `text-overflow: ellipsis` computed on the element) and the
    actual layout (nothing is wider than its own box), on the Revenus
    panel's income-and-taxation list -- the longest labels this batch adds."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        _wait_ready(page)
        page.click('.bp-sidebar-nav a[href="#revenus"]')
        page.wait_for_selector("#revenus:not([hidden])")
        page.wait_for_selector("#incomeList li .name")
        names = page.locator("#incomeList li .name")
        count = names.count()
        assert count > 0, "the income list rendered no rows"
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
        page.goto(f"{site}/micro.html#emploi", wait_until="load")
        page.wait_for_selector("#emploi:not([hidden])")
        page.reload(wait_until="load")
        page.wait_for_selector("#emploi:not([hidden])")
        assert page.evaluate("location.hash") == "#emploi"
    finally:
        context.close()


def test_at_phone_width_the_select_replaces_the_sidebar(browser, site):
    context = browser.new_context(viewport=PHONE_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        _wait_ready(page)
        assert page.is_visible("#panelPick")
        assert not page.is_visible(".bp-sidebar")
        page.select_option("#panelPick", "logement")
        page.wait_for_selector("#logement:not([hidden])")
        assert page.evaluate("location.hash") == "#logement"
        no_horizontal_scroll = page.evaluate(
            "document.documentElement.scrollWidth <= document.documentElement.clientWidth + 1"
        )
        assert no_horizontal_scroll, (
            page.evaluate("document.documentElement.scrollWidth"),
            page.evaluate("document.documentElement.clientWidth"),
        )
    finally:
        context.close()


@pytest.mark.parametrize("panel_id", PANEL_IDS)
def test_exactly_one_panel_is_visible_after_choosing_each_one(browser, site, panel_id):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        _wait_ready(page)
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
        page.goto(f"{site}/micro.html", wait_until="load")
        _wait_ready(page)
        page.click('.bp-sidebar-nav a[href="#entreprises"]')
        page.wait_for_selector("#entreprises:not([hidden])")
        page.wait_for_function(
            "document.activeElement && document.activeElement.closest('#entreprises') !== null"
        )
        focused_tag = page.evaluate("document.activeElement.tagName")
        assert focused_tag == "H2", focused_tag
    finally:
        context.close()


def test_loading_the_page_fresh_does_not_move_focus(browser, site):
    """`onShow` fires for the default panel too (this page's chart/map
    redraw needs it), but a first load must not steal focus from the address
    bar -- only a user CHOICE does."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        _wait_ready(page)
        focused_tag = page.evaluate("document.activeElement && document.activeElement.tagName")
        assert focused_tag != "H2", "the default panel's heading stole focus on load"
    finally:
        context.close()


def test_the_region_table_shows_no_median_price_column(browser, site):
    """The Logement panel's region table (Belgium + its three regions) never
    grows a median-price column -- a median cannot be aggregated from
    commune medians (CLAUDE.md's aggregation rule); `regional_housing.
    price_note` explains this in the reader's own language instead."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        _wait_ready(page)
        page.click('.bp-sidebar-nav a[href="#logement"]')
        page.wait_for_selector("#logement:not([hidden])")
        page.wait_for_selector("#regionalHousingBody tr")
        headers = page.locator("#housing-region thead th")
        assert headers.count() == 2, headers.count()
        header_texts = [headers.nth(i).text_content() for i in range(headers.count())]
        assert not any("price" in (t or "").lower() for t in header_texts), header_texts
        note = page.locator("#regionalHousingPriceNote").text_content()
        assert note and note.strip(), "no price_note explaining the missing column"
        rows = page.locator("#regionalHousingBody tr")
        assert rows.count() >= 2, "expected Belgium plus at least one region"
        for i in range(rows.count()):
            cells = rows.nth(i).locator("td")
            assert cells.count() == 2, "a region row grew a third (price) column"
    finally:
        context.close()
