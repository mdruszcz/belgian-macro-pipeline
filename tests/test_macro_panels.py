"""Browser contract for macro.html's chapter navigation -- originally
Batch A1.4 (docs/features/site_unification.md, "Macro/Micro : panneaux
thematiques"), rewritten for the Portrait redesign
(docs/features/macro_portrait.md).

Served over real HTTP (not file://) for the same reason
tests/site/test_iframe_contract.py is: `location.hash`/scroll behave the
same either way here, but a relative fetch of public/data/*.json -- which
this page needs for its layout -- is friendlier to reason about against a
real origin, and it matches every other browser test in this repo.

Portrait redesign, why this file changed so much: macro.html used to show
exactly one `[data-panel]` section at a time (panels.js toggling `hidden`,
rewriting the URL to the panel's own id, moving focus to the panel's
heading on every switch) with a `<select id="panelPick">` standing in for
the sidebar on phones. The redesign drops all of that in favour of
commune.html's own shipped pattern, verbatim: every chapter scrolls on ONE
page, a sticky horizontal pill strip (`.bp-sidebar`, `.bp-sidebar-nav`,
still that class name) is plain anchor links, a scroll listener keeps
`aria-current` on the pill for whichever chapter's heading is closest to
the line just below the sticky strip (macro.html's own
`updateCurrentChapter`, a direct getBoundingClientRect comparison, not an
IntersectionObserver band -- a band has a dead zone for a short trailing
chapter that native anchor-scroll cannot fully bring up to the line), and
there is no separate phone picker -- the pill strip is the nav at every
width, exactly like commune.html's own chapter strip. panels.js is no
longer loaded by
macro.html at all (see tests/test_macro.py::
test_panels_js_is_no_longer_loaded_by_macro_html). Charts moved from
`<canvas>` (assets/belpulse/charts.js) to the SVG renderer copied from
commune.html (assets/belpulse/macro_portrait_charts.js) -- `<canvas
id="...Canvas">` is gone; `.portrait-plot` SVG hosts replace it.

Every invariant this file protected still holds and is still checked here
-- old anchors resolve, a chart fills its card at real width (not a stale
canvas-fallback size), no growth-driver label truncates, a reload keeps
the hash, a chart's hover tooltip shows the real value, its data table
lists every period, and the compact-empty state machine still collapses
correctly -- only the MECHANICS of navigating to a chapter changed (native
anchor scroll instead of a JS panel switch), and canvas measurements
became SVG measurements.
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

#: Every chapter this page has, in pill-strip order.
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


def test_a_legacy_anchor_still_lands_on_its_chapter(browser, site):
    """#growth was the GDP-history section's own anchor before this batch;
    it is now inside the "croissance" chapter's `#growth` sub-element, and
    CLAUDE.md rule 31 says the old link must still work. Every chapter
    scrolls on one page now, so "still works" means the browser's native
    anchor navigation lands inside view of the right chapter -- there is
    no separate panel to reveal or URL to rewrite any more."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#growth", wait_until="load")
        page.wait_for_timeout(400)
        assert page.evaluate("location.hash") == "#growth"
        assert page.is_visible("#growth")
        # The whole page scrolls together now -- #croissance (the chapter
        # #growth lives in) is not hidden the way an old, unselected panel
        # used to be.
        assert page.get_attribute("#croissance", "hidden") is None
        # #growth is actually the element the browser scrolled to, not just
        # present somewhere off-screen.
        box = page.locator("#growth").bounding_box()
        assert (
            box and box["y"] < DESKTOP_VIEWPORT["height"]
        ), f"#growth is not in the initial viewport after a #growth deep link: {box}"
    finally:
        context.close()


def test_a_chapter_charts_svg_fills_its_cards_inner_width(browser, site):
    """The regression this used to guard against a canvas for: measuring a
    chart's plot host while its ancestor chapter was `hidden` produced a
    permanently-stale width. There is no `hidden` chapter to race any more
    (every chapter is always in the DOM and unhidden -- scrolling is the
    only thing that changes), but the SVG plot must still actually fill
    its card rather than falling back to some default size."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.eval_on_selector("#croissance", "el => el.scrollIntoView()")
        svg = page.locator("#historyPlot svg")
        expect(svg).to_have_count(1)
        svg_width = page.eval_on_selector(
            "#historyPlot svg", "el => el.getBoundingClientRect().width"
        )
        wrap_width = page.eval_on_selector("#historyPlot", "el => el.clientWidth")
        assert svg_width >= wrap_width * 0.9, (
            f"the GDP history chart is {svg_width}px wide inside a {wrap_width}px card "
            "-- looks like a stale fallback size, not a real fit"
        )
    finally:
        context.close()


def test_no_growth_driver_label_is_truncated(browser, site):
    """The lead's review of PR #170's screenshots: "Depenses de
    consommation des APU" was rendered as "Depen...". A statistical label
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
        page.eval_on_selector("#croissance", "el => el.scrollIntoView()")
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


def test_reload_keeps_the_hash_and_the_chapter_in_view(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#prix", wait_until="load")
        page.wait_for_timeout(400)
        page.reload(wait_until="load")
        page.wait_for_timeout(400)
        assert page.evaluate("location.hash") == "#prix"
        box = page.locator("#prix").bounding_box()
        assert (
            box and box["y"] < DESKTOP_VIEWPORT["height"]
        ), f"#prix is not in view after a reload with #prix in the URL: {box}"
    finally:
        context.close()


def test_the_pill_strip_is_the_nav_at_every_width_and_scroll_spy_tracks_it(browser, site):
    """Portrait redesign: there is no separate phone `<select>` any more --
    commune.html's own pattern has no such picker either, and one page
    that scrolls does not need a mode-switch to "jump to a section" that a
    plain anchor link already does. The pill strip (`.bp-sidebar`) is
    visible and is the nav at both desktop and phone width; scrolling a
    chapter into view marks its own pill `aria-current`."""
    for viewport in (DESKTOP_VIEWPORT, PHONE_VIEWPORT):
        context = browser.new_context(viewport=viewport)
        page = context.new_page()
        try:
            page.goto(f"{site}/macro.html", wait_until="load")
            page.wait_for_selector("#apercu:not([hidden])")
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
    """Replaces the old one-panel-visible-at-a-time check: every chapter
    now coexists on the page (nothing is ever `hidden`), so what a reader
    actually needs is that clicking its pill scrolls it into view and the
    pill strip agrees on which chapter that is.

    Parametrized over every chapter INCLUDING the last two ("finances-
    publiques", "europe"): both are short enough, this close to the end of
    the page, that native anchor-scroll cannot bring their heading all the
    way up to the sticky strip's own line -- real bug found writing this
    batch, where an IntersectionObserver-band scroll-spy left "europe"
    permanently stuck on "finances-publiques"'s pill (and that in turn
    stuck on "conjoncture"'s) no matter how far a reader scrolled, because
    the band's trigger zone could never overlap either heading. Fixed in
    macro.html's `updateCurrentChapter` (closest-heading-to-the-line, not
    a band) -- this parametrization is what would catch a regression back
    to a band-shaped rule."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click(f'.bp-sidebar-nav a[href="#{panel_id}"]')
        page.wait_for_function(
            "document.querySelector('.bp-sidebar-nav a[aria-current=\"page\"]')"
            f"?.getAttribute('href') === '#{panel_id}'"
        )
        # Nothing is ever hidden any more -- every chapter section exists,
        # unhidden, on the one scrolling page.
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


# --- BATCH A1.4b: history charts inside Prix/Emploi/Conjoncture ---------------

#: {panel id: (chart card id, plot host id)}, matching macro.html's
#: camelId() of each config/national_sections.yaml `panel_charts[].id`.
#: The plot host used to be a `<canvas id="...Canvas">`; the Portrait
#: redesign's SVG renderer draws into a `<div id="...Plot">` instead
#: (macro.html's renderPanelCharts(), assets/belpulse/
#: macro_portrait_charts.js).
PANEL_CHARTS = {
    "prix": ("prices-chart", "pricesChartPlot"),
    "emploi": ("employment-chart", "employmentChartPlot"),
    "conjoncture": ("business-cycle-chart", "businessCycleChartPlot"),
}


def _open_panel_chart(browser, site, panel_id, plot_id, lang=None):
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
    page.wait_for_function(
        "document.querySelector('.bp-sidebar-nav a[aria-current=\"page\"]')"
        f"?.getAttribute('href') === '#{panel_id}'"
    )
    page.wait_for_selector(f"#{plot_id} svg")
    # Same "wait for the real rendered width, not just present" caution the
    # old canvas version had -- the SVG's viewBox is fixed at build time
    # (buildLineChartSVG), so what actually needs to settle is its CSS
    # width filling the host.
    page.wait_for_function(
        "(function(){"
        f"var svg = document.querySelector('#{plot_id} svg');"
        f"var wrap = document.getElementById('{plot_id}');"
        "return svg && svg.getBoundingClientRect().width >= wrap.clientWidth * 0.9;"
        "})()"
    )
    return context, page


@pytest.mark.parametrize("panel_id", ["prix", "emploi", "conjoncture"])
def test_a_panel_charts_svg_fills_its_cards_inner_width(browser, site, panel_id):
    chart_id, plot_id = PANEL_CHARTS[panel_id]
    context, page = _open_panel_chart(browser, site, panel_id, plot_id)
    try:
        svg_width = page.eval_on_selector(
            f"#{plot_id} svg", "el => el.getBoundingClientRect().width"
        )
        wrap_width = page.eval_on_selector(f"#{plot_id}", "el => el.clientWidth")
        assert (
            svg_width >= wrap_width * 0.9
        ), f"{chart_id}'s chart is {svg_width}px wide inside a {wrap_width}px wrapper"
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
    chart_id, plot_id = PANEL_CHARTS["prix"]
    series = _panel_chart(chart_id)["series"]
    assert series == ["HICP"], series
    entry = _national()["HICP"]
    periods = sorted(entry["periods"].keys())
    last_period = periods[-1]
    last_value = entry["periods"][last_period]["value"]
    assert last_value is not None, "fixture assumption broken: HICP's last period has no value"

    context, page = _open_panel_chart(browser, site, "prix", plot_id, lang="en")
    try:
        svg = page.locator(f"#{plot_id} svg")
        box = svg.bounding_box()
        assert box, "HICP chart has no layout box"
        # wireLine (assets/belpulse/macro_portrait_charts.js) attaches its
        # hover layer once the SVG exists; the SVG is already confirmed
        # present and real-width by _open_panel_chart's wait_for_function
        # above, so a nudge-and-poll hover is enough here (same rationale
        # as the old canvas version's comment: a mousemove that lands
        # before the layer attaches gets nothing to answer, so poll rather
        # than sleep once).
        _hover_until_tip(page, box["x"] + box["width"] - 3, box["y"] + box["height"] / 2)
        tip_text = page.locator(".bp-chart-tip").inner_text()
        assert last_period in tip_text, f"tooltip missing period: {tip_text!r}"

        # Independently formatted in the SAME runtime/locale the tooltip
        # uses (pinned to 'en' above), the same way MapUI.formatValue does
        # it (assets/commune_map.js): a DECLARED decimals count is a
        # minimum as well as a maximum, and HICP's unit is "percent_yy" --
        # a trailing " %" (WITH the leading space) the renderer adds, not
        # a raw unit code (rule 7). The space matches commune.html's own
        # displayUnit()/pctText convention verbatim (commune.html has a
        # comment noting a past batch caught exactly this space going
        # missing) -- the Portrait SVG renderer's `displayUnit()` in
        # macro.html was copied from the same convention, so "2.2 %" is
        # the correct, intentional format, not a regression from the old
        # canvas tooltip's "2.2%".
        decimals = entry.get("decimals", 1)
        expected_value_text = (
            page.evaluate(
                "([v, d]) => v.toLocaleString('en', {minimumFractionDigits: d, maximumFractionDigits: d})",
                [last_value, decimals],
            )
            + " %"
        )
        assert (
            expected_value_text in tip_text
        ), f"tooltip missing value {expected_value_text!r}: {tip_text!r}"
    finally:
        context.close()


@pytest.mark.parametrize("panel_id", ["prix", "emploi", "conjoncture"])
def test_a_panel_charts_data_table_lists_every_drawn_period(browser, site, panel_id):
    chart_id, plot_id = PANEL_CHARTS[panel_id]
    series = _panel_chart(chart_id)["series"]
    national = _national()
    # All the periods any drawn series carries. Unlike the old canvas
    # renderer (BPCharts, which aligned every series on the union of their
    # periods in one chart), the Portrait SVG renderer draws each series in
    # `panel_charts[].series` as its OWN separate chart+table inside the
    # card (macro.html's renderPanelCharts() creates one `.portrait-series`
    # per code) -- so the expected row count is per-series periods, summed
    # across the series sharing this card, not a period union times series
    # count.
    context, page = _open_panel_chart(browser, site, panel_id, plot_id)
    try:
        # The table renders each row's period through macro.html's own
        # fmtPeriod() -- a localized "MMM YYYY" (or "Qn YYYY"), never the
        # raw ISO string ("2026-02" becomes "févr. 2026" in French,
        # the language this context defaults to) -- so the check below
        # asks the PAGE to format the same way it would, in whatever
        # language it actually booted in, rather than assuming the raw
        # period string appears verbatim (it never does; this was the
        # test's own bug, not the page's -- caught because the real
        # UNEMPLOYMENT_RATE/BUSINESS_CONFIDENCE fixtures happened to
        # include a period the naive check couldn't find).
        current_lang = page.evaluate("document.documentElement.lang")
        card = page.locator(f"#{chart_id}")
        details = card.locator("details.portrait-data")
        expect(details).to_have_count(len(series), timeout=10_000)
        for i, code in enumerate(series):
            code_periods = sorted(national[code]["periods"].keys())
            assert code_periods, f"{chart_id}: no periods for {code} to check against"
            table = details.nth(i).locator("table")
            rows = details.nth(i).locator("tbody tr")
            expect(rows).to_have_count(len(code_periods), timeout=10_000)
            table_text = table.text_content()
            for period in code_periods:
                expected = page.evaluate(
                    """([p, lang]) => {
                        var m = /^(\\d{4})-(\\d{2})$/.exec(p);
                        if (m) return new Date(Number(m[1]), Number(m[2]) - 1, 1)
                            .toLocaleDateString(lang, {month: 'short', year: 'numeric'});
                        var q = /^(\\d{4})-Q([1-4])$/.exec(p);
                        if (q) return ({en: 'Q', fr: 'T', nl: 'K'}[lang] || 'Q') + q[2] + ' ' + q[1];
                        return p;
                    }""",
                    [period, current_lang],
                )
                assert expected in table_text, (
                    f"{chart_id}/{code}: period {period} (rendered {expected!r}) "
                    f"missing from its data table: {table_text!r}"
                )
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
    26 forbids. The original fix scoped the override to
    `.card-compact-empty[data-state="unavailable"]` instead of `!important`.

    Portrait redesign found the SAME bug reintroduced in a new form: the
    whole `.card-compact-empty` rule moved out of macro.html's inline
    <style> to assets/belpulse/macro_portrait.css (all layout did, same
    as commune.html), and on the way its narrow, state-specific override
    was rewritten as a blanket `.bp-state:not([data-state="ready"])`
    rule -- which put "Not available yet" back on EVERY non-ready state,
    on EVERY `.bp-state` card on the page, not just #map/#news: a flash
    of that message on every card while it is still `loading` (present in
    the markup as the initial state, before data arrives), and it also
    overrode `suppressed`'s own dash-and-reason rendering
    (components.css). Fixed by scoping macro_portrait.css's override to
    `[data-state="unavailable"]` specifically (the only state macro.html's
    own JS ever needs this message for) rather than `:not([data-state=
    "ready"])`, so `loading` and `suppressed` fall back to
    components.css's own correct, more granular rules. The message is
    `display:block` now, not `flex` -- unrelated to this bug, just how
    the ported card CSS lays it out (icon above text, not beside it)."""
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
        assert shown == "block", "message should render while #map is unavailable"

        for other_state in ("loading", "ready", "suppressed"):
            page.evaluate(
                "(s) => { document.getElementById('map').dataset.state = s; }", other_state
            )
            hidden = page.eval_on_selector(
                "#map .bp-state-message", "el => getComputedStyle(el).display"
            )
            assert hidden == "none", (
                f"#map still renders 'Not available yet' with data-state={other_state!r} "
                "-- a state is collapsing into 'unavailable' again"
            )
    finally:
        context.close()
