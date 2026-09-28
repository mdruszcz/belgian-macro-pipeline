"""Browser contract for micro.html's chapter navigation -- Micro Portrait
(docs/features/micro_portrait.md), copying macro.html's own Portrait
redesign (docs/features/macro_portrait.md, tests/test_macro_panels.py)
verbatim in structure and adapted to micro.html's own chapters, KPI band,
household tiles, region table and map/comparison charts.

Served over real HTTP (not file://) for the same reason
tests/site/test_iframe_contract.py is: `location.hash`/scroll behave the
same either way here, but a relative fetch of public/data/*.json -- which
this page needs for its layout, its aggregates and the commune geojson -- is
friendlier to reason about against a real origin, and it matches every other
browser test in this repo.

Portrait redesign, why this file changed so much: micro.html used to show
exactly one `[data-panel]` section at a time (panels.js toggling `hidden`,
rewriting the URL to the panel's own id, moving focus to the panel's heading
on every switch) with a `<select id="panelPick">` standing in for the
sidebar on phones. The redesign drops all of that in favour of macro.html's
own shipped Portrait pattern, verbatim: every chapter scrolls on ONE page, a
sticky horizontal pill strip (`.bp-sidebar`, `.bp-sidebar-nav`, still that
class name) is plain anchor links, a scroll listener keeps `aria-current` on
the pill for whichever chapter's heading is closest to the line just below
the sticky strip (micro.html's own `updateCurrentChapter`, copied from
macro.html -- a direct claim-point comparison, not an IntersectionObserver
band, which has a dead zone for a short trailing chapter native anchor-scroll
cannot fully bring up to the line), and there is no separate phone picker --
the pill strip is the nav at every width, exactly like macro.html and
commune.html's own chapter strip. panels.js is no longer loaded by
micro.html at all (see tests/test_micro.py::
test_panels_js_is_no_longer_loaded_by_micro_html).

What stayed CANVAS, unlike macro.html: the comparison ranking chart
(#compareCanvas, BPCharts.drawRanking) and the choropleth map
(assets/commune_map.js, rule 29) -- neither has an SVG Portrait equivalent
to reuse (macro_portrait_charts.js's factory only draws a line/spark/bar
time series), so this batch did not build a third chart renderer for them.
The GDP-shaped time-series charts (#historyPlot, every KPI/list tile) moved
to the SVG renderer, same as macro.html.

Every invariant this file protected still holds and is still checked here --
old anchors resolve, a chart fills its card at real width (not a stale
canvas-fallback size), no list label truncates, a reload keeps the hash, the
region table still has no price column -- only the MECHANICS of navigating
to a chapter changed (native anchor scroll instead of a JS panel switch),
and the housing/KPI/list charts' measurements became SVG measurements.
"""

from __future__ import annotations

import functools
import http.server
import socketserver
import threading
from pathlib import Path

import pytest
from playwright.sync_api import expect

REPO_ROOT = Path(__file__).resolve().parents[1]

DESKTOP_VIEWPORT = {"width": 1280, "height": 900}
PHONE_VIEWPORT = {"width": 390, "height": 844}

#: Every chapter this page has, in pill-strip order.
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


def test_every_chapter_is_present_and_unhidden_from_the_start(browser, site):
    """Portrait redesign: nothing is ever `hidden` any more -- every chapter
    section exists, unhidden, on the one scrolling page (replaces the old
    "exactly one panel visible" contract)."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        page.wait_for_function("document.getElementById('kpiRow').children.length > 0")
        hidden = page.eval_on_selector_all(
            "[data-panel]", "els => els.filter(e => e.hidden).map(e => e.id)"
        )
        assert hidden == [], f"chapter(s) unexpectedly hidden: {hidden}"
        ids = page.eval_on_selector_all("[data-panel]", "els => els.map(e => e.id)")
        assert ids == PANEL_IDS, ids
    finally:
        context.close()


def test_a_legacy_anchor_still_lands_on_its_chapter(browser, site):
    """#housing was the housing-market section's own anchor before this
    batch; it is now inside the "logement" chapter's `#housing` sub-element,
    and CLAUDE.md rule 31 says the old link must still work. Every chapter
    scrolls on one page now, so "still works" means the browser's native
    anchor navigation lands inside view of the right chapter -- there is no
    separate panel to reveal or URL to rewrite any more."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html#housing", wait_until="load")
        page.wait_for_timeout(400)
        assert page.evaluate("location.hash") == "#housing"
        assert page.is_visible("#housing")
        # The whole page scrolls together now -- #logement (the chapter
        # #housing lives in) is not hidden the way an old, unselected panel
        # used to be.
        assert page.get_attribute("#logement", "hidden") is None
        box = page.locator("#housing").bounding_box()
        assert (
            box and box["y"] < DESKTOP_VIEWPORT["height"]
        ), f"#housing is not in the initial viewport after a #housing deep link: {box}"
    finally:
        context.close()


def test_a_chapters_lead_chart_svg_fills_its_cards_inner_width(browser, site):
    """The regression this used to guard against for a canvas: measuring a
    chart's plot host while its ancestor chapter was `hidden` produced a
    permanently-stale width. There is no `hidden` chapter to race any more,
    but the housing-market SVG plot must still actually fill its card
    rather than falling back to some default size."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        page.wait_for_function("document.getElementById('kpiRow').children.length > 0")
        page.eval_on_selector("#logement", "el => el.scrollIntoView()")
        svg = page.locator("#historyPlot svg")
        expect(svg).to_have_count(1)
        svg_width = page.eval_on_selector(
            "#historyPlot svg", "el => el.getBoundingClientRect().width"
        )
        wrap_width = page.eval_on_selector("#historyPlot", "el => el.clientWidth")
        assert svg_width >= wrap_width * 0.9, (
            f"the housing-market history chart is {svg_width}px wide inside a {wrap_width}px "
            "card -- looks like a stale fallback size, not a real fit"
        )
    finally:
        context.close()


def test_a_shown_chapters_comparison_canvas_has_real_width(browser, site):
    """The regression the old panel-switching mechanism existed to avoid: a
    canvas measured while its ancestor panel is `hidden` is 0px wide -- or,
    worse, PERMANENTLY pinned to assets/belpulse/charts.js's fallback
    (300px), because that fallback is written back as the canvas's own
    inline style. There is no `hidden` chapter any more, but the comparison
    ranking canvas (still BPCharts.drawRanking -- no SVG ranking-bar
    equivalent exists to reuse) must still fill its real card width once
    drawComparison()'s own visibility guard (`canvas.offsetParent !== null`)
    has had a chance to run for real, not just render into a zero-size box
    on first paint before the browser has laid the page out."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        page.wait_for_function("document.getElementById('kpiRow').children.length > 0")
        page.eval_on_selector("#comparaisons", "el => el.scrollIntoView()")
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
    chapter's income-and-taxation list -- the longest labels this page has."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        page.wait_for_function("document.getElementById('kpiRow').children.length > 0")
        page.eval_on_selector("#revenus", "el => el.scrollIntoView()")
        page.wait_for_selector("#incomeList .leadchart-eyebrow")
        names = page.locator("#incomeList .leadchart-eyebrow")
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


def test_reload_keeps_the_hash_and_the_chapter_in_view(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html#emploi", wait_until="load")
        page.wait_for_timeout(400)
        page.reload(wait_until="load")
        page.wait_for_timeout(400)
        assert page.evaluate("location.hash") == "#emploi"
        box = page.locator("#emploi").bounding_box()
        assert (
            box and box["y"] < DESKTOP_VIEWPORT["height"]
        ), f"#emploi is not in view after a reload with #emploi in the URL: {box}"
    finally:
        context.close()


def test_the_pill_strip_is_the_nav_at_every_width_and_scroll_spy_tracks_it(browser, site):
    """Portrait redesign: there is no separate phone `<select>` any more --
    macro.html and commune.html's own pattern has no such picker either, and
    one page that scrolls does not need a mode-switch to "jump to a
    section" that a plain anchor link already does. The pill strip
    (`.bp-sidebar`) is visible and is the nav at both desktop and phone
    width; scrolling a chapter into view marks its own pill `aria-current`."""
    for viewport in (DESKTOP_VIEWPORT, PHONE_VIEWPORT):
        context = browser.new_context(viewport=viewport)
        page = context.new_page()
        try:
            page.goto(f"{site}/micro.html", wait_until="load")
            page.wait_for_function("document.getElementById('kpiRow').children.length > 0")
            assert page.is_visible(".bp-sidebar"), f"pill strip hidden at {viewport}"
            assert page.is_visible(
                '.bp-sidebar-nav a[href="#emploi"]'
            ), f"no #emploi pill link at {viewport}"
            page.eval_on_selector("#emploi", "el => el.scrollIntoView()")
            page.wait_for_function(
                "document.querySelector('.bp-sidebar-nav a[aria-current=\"page\"]')"
                "?.getAttribute('href') === '#emploi'"
            )
        finally:
            context.close()


@pytest.mark.parametrize("panel_id", PANEL_IDS)
def test_every_chapter_is_reachable_and_becomes_the_current_pill(browser, site, panel_id):
    """Replaces the old one-panel-visible-at-a-time check: every chapter now
    coexists on the page (nothing is ever `hidden`), so what a reader
    actually needs is that clicking its pill scrolls it into view and the
    pill strip agrees on which chapter that is.

    Parametrized over every chapter INCLUDING the last two ("entreprises",
    "comparaisons"): both are the same shape as macro.html's own short
    trailing chapters ("finances-publiques", "europe") that an
    IntersectionObserver-band scroll-spy could never mark current, because
    native anchor-scroll cannot bring either chapter's heading far enough
    up the page for the band to overlap it. micro.html's
    `updateCurrentChapter` is copied from macro.html's own fix (a
    precomputed, clamped claim-point comparison, never a band) -- this
    parametrization is what would catch a regression back to a
    band-shaped rule."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        page.wait_for_function("document.getElementById('kpiRow').children.length > 0")
        page.click(f'.bp-sidebar-nav a[href="#{panel_id}"]')
        page.wait_for_function(
            "document.querySelector('.bp-sidebar-nav a[aria-current=\"page\"]')"
            f"?.getAttribute('href') === '#{panel_id}'"
        )
        hidden = page.eval_on_selector_all(
            "[data-panel]", "els => els.filter(e => e.hidden).map(e => e.id)"
        )
        assert hidden == [], f"chapter(s) unexpectedly hidden: {hidden}"
        box = page.locator(f"#{panel_id}").bounding_box()
        assert (
            box and box["y"] < DESKTOP_VIEWPORT["height"] and box["y"] > -50
        ), f"#{panel_id} did not scroll into view: {box}"
    finally:
        context.close()


def test_the_region_table_shows_no_median_price_column(browser, site):
    """The Logement chapter's region table (Belgium + its three regions)
    never grows a median-price column -- a median cannot be aggregated from
    commune medians (CLAUDE.md's aggregation rule); `regional_housing.
    price_note` explains this in the reader's own language instead."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        page.wait_for_function("document.getElementById('kpiRow').children.length > 0")
        page.eval_on_selector("#logement", "el => el.scrollIntoView()")
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


def test_the_household_tile_grid_renders_four_real_tiles(browser, site):
    """The Population chapter's 2x2 household-dynamics grid -- unlike every
    other card, tiles.indicators are single-period Census snapshots with no
    delta, so this checks the tiles render real, dated values rather than
    the placeholder dash a missing entry would leave."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        page.wait_for_function("document.getElementById('kpiRow').children.length > 0")
        page.eval_on_selector("#population", "el => el.scrollIntoView()")
        page.wait_for_selector("#tileGrid .bp-stat-tile")
        tiles = page.locator("#tileGrid .bp-stat-tile")
        assert tiles.count() == 4, tiles.count()
        for i in range(tiles.count()):
            value = tiles.nth(i).locator(".bp-value .num").text_content()
            assert (
                value and value.strip() and value.strip() != "—"
            ), f"household tile {i} has no real value: {value!r}"
    finally:
        context.close()


def test_a_compact_empty_cards_message_only_renders_for_the_state_that_owns_it(browser, site):
    """The same audit finding macro.html's own test protects against
    (docs/features/macro_portrait.md; tests/test_macro_panels.py's
    identically-named test): the Portrait CSS's `.bp-state-message` override
    must be scoped to `[data-state="unavailable"]` specifically, never a
    blanket `:not([data-state="ready"])` -- which would put "Not available
    yet" back on every non-ready card, including one still `loading` (a
    flash of that message before data even arrives) or `suppressed` (which
    components.css already renders as a dash + reason, not this message).
    Checked here against #apartment_price, one of this page's own
    `data-slot` unavailable cards."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/micro.html", wait_until="load")
        page.wait_for_selector("#apartment_price .bp-state-message")

        assert page.get_attribute("#apartment_price", "data-state") == "unavailable"
        shown = page.eval_on_selector(
            "#apartment_price .bp-state-message", "el => getComputedStyle(el).display"
        )
        assert shown == "block", "message should render while #apartment_price is unavailable"

        for other_state in ("loading", "ready", "suppressed"):
            page.evaluate(
                "(s) => { document.getElementById('apartment_price').dataset.state = s; }",
                other_state,
            )
            hidden = page.eval_on_selector(
                "#apartment_price .bp-state-message", "el => getComputedStyle(el).display"
            )
            assert hidden == "none", (
                f"#apartment_price still renders 'Not available yet' with "
                f"data-state={other_state!r} -- a state is collapsing into 'unavailable' again"
            )
    finally:
        context.close()
