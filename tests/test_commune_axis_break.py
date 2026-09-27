"""Commune Portrait polish (stage 2), A5: axis breaks on lead line charts.

The maintainer drew "BREAK" over the empty 2000-2017 stretch of the
Securite/burglary chart (HOUSE_BURGLARIES_PER_10K, headline of the "safety"
section, config/local_sections.yaml) -- the source only has one figure for
2000 and then resumes annual reporting from 2017, a 17-year gap against a
1-year median step elsewhere in the series. buildLineChartSVG() in
commune.html compresses any gap over 3x the median step to exactly 2x and
draws a break marker (two slanted strokes + a dotted vertical line + a
<title>) instead of a dashed bridge; see the A5 doc comment directly above
isCompressedBreak() in commune.html.

92094's AVG_NET_TAXABLE_INCOME is annual with no such gap (2005-2023,
one-year steps throughout) and must render with zero break markers -- the
regression this test also covers is a break marker appearing where the data
has no real gap.
"""

from __future__ import annotations

import functools
import http.server
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


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


def _load(chromium, site, nis):
    ctx = chromium.new_context(viewport={"width": 1200, "height": 900}, locale="fr-FR")
    page = ctx.new_page()
    page.goto(f"{site}/commune.html?nis={nis}", wait_until="load")
    page.wait_for_selector("#chapters .chapter", state="attached")
    page.wait_for_timeout(500)
    return ctx, page


#: JS run in-page against the "safety" chapter's lead chart -- the one whose
#: headline is HOUSE_BURGLARIES_PER_10K (config/local_sections.yaml), so its
#: own indicator ID never needs to appear in this test (rule 24 extended:
#: the test finds the chart by chapter id, the same way the page's own
#: buildLeadRow()/renderChart() do, not by an indicator code).
_AXIS_BREAK_JS = """
() => {
    const section = document.getElementById('chapter-safety');
    if (!section) return { error: 'no #chapter-safety section' };
    const svg = section.querySelector('.leadchart-col svg');
    if (!svg) return { error: 'no lead chart svg in #chapter-safety' };
    const breaks = Array.from(svg.querySelectorAll('g.bp-axis-break'));
    const breakInfo = breaks.map(g => {
        const line = g.querySelector('line[stroke-dasharray="1 3"]');
        const title = g.querySelector('title');
        return {
            x: line ? parseFloat(line.getAttribute('x1')) : null,
            title: title ? title.textContent : null,
        };
    });
    // Every x-axis tick <text> (first/last/segment starts/intermediate
    // picks) -- see the A5 tick-order doc comment in buildLineChartSVG.
    const ticks = Array.from(svg.querySelectorAll('text')).filter(t => {
        const y = parseFloat(t.getAttribute('y'));
        return !isNaN(y) && y > 100; // axis labels sit near the bottom; gridline value labels are near x=0/y small
    }).map(t => ({ x: parseFloat(t.getAttribute('x')), text: t.textContent }));
    return { breakCount: breaks.length, breaks: breakInfo, ticks };
}
"""


def test_burglary_chart_has_exactly_one_break_at_2x_normal_step(chromium, site):
    # 21004 and 92094 both have HOUSE_BURGLARIES_PER_10K = [2000, 2017..2025]
    # (a 17-year gap, then annual) -- either is a valid fixture; 92094 is the
    # commune already used throughout this page's other browser tests.
    ctx, page = _load(chromium, site, "92094")
    try:
        result = page.evaluate(_AXIS_BREAK_JS)
        assert "error" not in result, result.get("error")
        assert result["breakCount"] == 1, (
            f"expected exactly one break marker on the burglary chart's "
            f"2000-2017 gap, found {result['breakCount']}: {result['breaks']}"
        )
        brk = result["breaks"][0]
        assert brk["title"], "the break marker has no <title>"
        assert (
            "2000" in brk["title"] and "2017" in brk["title"]
        ), f"break <title> does not name the real skipped years 2000/2017: {brk['title']!r}"

        # The x-distance the break itself spans (from the last point before
        # the gap to the first point after it) must equal 2x a normal
        # (post-2017, annual) step, within 1px -- the A5 spec's own
        # "compress to exactly 2x the median step" rule.
        tick_by_text = {}
        for t in result["ticks"]:
            tick_by_text.setdefault(t["text"], t["x"])
        assert "2000" in tick_by_text, f"no 2000 tick kept: {result['ticks']}"
        assert "2017" in tick_by_text, f"no 2017 tick kept: {result['ticks']}"
        assert (
            "2018" in tick_by_text or "2019" in tick_by_text
        ), f"no post-break annual tick kept to measure a normal step against: {result['ticks']}"
        # A normal annual step's x-width, measured from whichever consecutive
        # post-break tick pair is available among the kept ticks.
        post_break_years = sorted(
            int(t["text"])
            for t in result["ticks"]
            if t["text"].isdigit() and int(t["text"]) >= 2017
        )
        assert (
            len(post_break_years) >= 2
        ), f"not enough post-break ticks to measure a step: {post_break_years}"
        y0, y1 = post_break_years[0], post_break_years[1]
        step_years = y1 - y0
        x0, x1 = tick_by_text[str(y0)], tick_by_text[str(y1)]
        normal_step_px = abs(x1 - x0) / step_years

        gap_x = brk["x"]
        # The gap's own x-span is from the last pre-break tick (2000) to the
        # first post-break tick (2017), centred on the break marker -- so
        # the FULL gap width is twice the distance from 2000's tick to the
        # break marker's own x (the marker sits at the gap's midpoint).
        gap_half_width = abs(gap_x - tick_by_text["2000"])
        gap_full_width = gap_half_width * 2
        expected = normal_step_px * 2
        assert abs(gap_full_width - expected) <= 1.0, (
            f"compressed gap width {gap_full_width:.2f}px != 2x normal step "
            f"{expected:.2f}px (normal step {normal_step_px:.2f}px)"
        )
    finally:
        ctx.close()


def test_income_chart_has_no_break_marker(chromium, site):
    # 92094's AVG_NET_TAXABLE_INCOME is annual 2005-2023 with no gap -- the
    # regression check for a break marker appearing on a series that has
    # none. It is the "income" chapter's own headline (config/
    # local_sections.yaml), so commune.html already renders it on load with
    # no interaction needed; every OTHER rendered chapter's lead chart is
    # checked the same way, generically by chapter id, as a second line of
    # defence (no indicator ID appears in this test -- rule 24 extended).
    ctx, page = _load(chromium, site, "92094")
    try:
        result = page.evaluate("""
            () => {
                const out = {};
                document.querySelectorAll('.chapter').forEach(section => {
                    const svg = section.querySelector('.leadchart-col svg');
                    if (!svg) return;
                    out[section.id] = svg.querySelectorAll('g.bp-axis-break').length;
                });
                return out;
            }
            """)
        offenders = {k: v for k, v in result.items() if v > 0}
        # The safety chapter (burglary) is the one EXPECTED break; every
        # other rendered lead chart on the page -- including finances,
        # whose headline is the annual, gapless municipal tax-rate series --
        # must have none.
        offenders.pop("chapter-safety", None)
        assert not offenders, f"unexpected break marker(s) on non-burglary lead charts: {offenders}"
    finally:
        ctx.close()
