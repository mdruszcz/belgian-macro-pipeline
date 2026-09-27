"""Commune Portrait polish (stage 2) -- the regression net for A4/A5/A7/B2/
B3/B5/C3: text must stay inside its own layout box, at any of the three
widths the maintainer's annotated screenshots covered (1440/1024/390px) and
in all three languages, on four real communes (a small one, Brussels, a
Flemish one, and 92094 -- the fixture already used throughout this page's
other browser tests).

One shared local-file HTTP server (the pattern every other browser test in
this repo already uses) and one Chromium context per (nis, width, lang)
combination, sharing the one session-scoped `chromium` browser (tests/
conftest.py) so the run stays a single Chromium process. 4 communes x 3
widths x 3 langs = 36 page loads; kept to one assertion pass per load rather
than one test per assertion so the suite does not multiply further.
"""

from __future__ import annotations

import functools
import http.server
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

NIS_CODES = ["21002", "21004", "46030", "92094"]
WIDTHS = [1440, 1024, 390]
LANGS = ["fr", "nl", "en"]
LANG_STORAGE_KEY = "belpulse-lang"

#: Every container this batch's spec names as a box text must stay inside.
CONTAINER_SELECTORS = [
    ".leadchart-col",
    ".leadmap-card",
    ".multiple-tile",
    ".contextrow > div",
    ".hero-namebox",
    ".hero-locator",
    ".essentiel-item",
    "#pyramidPanel",
    "#schoolsPanel",
    ".wrap",
]

#: 1px slack for sub-pixel layout rounding across engines/zoom levels.
TOLERANCE_PX = 1


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


def _set_lang(ctx, lang):
    ctx.add_init_script(
        f"try {{ window.localStorage.setItem('{LANG_STORAGE_KEY}', '{lang}'); }} catch(_){{}}"
    )


def _load(chromium, site, nis, width, lang):
    ctx = chromium.new_context(viewport={"width": width, "height": 900}, locale="en-US")
    _set_lang(ctx, lang)
    page = ctx.new_page()
    page.goto(f"{site}/commune.html?nis={nis}", wait_until="load")
    page.wait_for_selector("#chapters .chapter", state="attached")
    page.wait_for_timeout(500)
    # Scroll to the bottom in steps so every lazily-mounted (IntersectionObserver
    # -gated) section -- charts, the neighbours map, the pyramid -- has actually
    # rendered its real content, not a still-empty placeholder, before anything
    # is measured.
    height = page.evaluate("document.documentElement.scrollHeight")
    y = 0
    step = 800
    while y < height:
        page.evaluate(f"window.scrollTo(0, {y})")
        page.wait_for_timeout(60)
        y += step
        height = page.evaluate("document.documentElement.scrollHeight")
    page.wait_for_timeout(400)
    return ctx, page


#: JS run in-page: for every visible text node (element or SVG <text>/<tspan>)
#: and every visible ellipsis/box target, checks containment against its
#: nearest ancestor matching one of CONTAINER_SELECTORS, plus the specific
#: ellipsis-clipping and neighbours-map-label-overlap checks the spec calls
#: out by name. Runs entirely in the page so per-node getBoundingClientRect()
#: calls do not round-trip one at a time.
_CHECK_JS = """
({ containerSelectors, tolerance }) => {
    const out = { overflow: [], notContained: [], ellipsisClipped: [], labelOverlaps: [] };

    // 1. No page overflow.
    const doc = document.documentElement;
    if (doc.scrollWidth > doc.clientWidth + tolerance) {
        out.overflow.push(`scrollWidth ${doc.scrollWidth} > clientWidth ${doc.clientWidth}`);
    }

    function nearestContainer(el) {
        let n = el;
        while (n && n !== document.body) {
            if (n instanceof Element && containerSelectors.some(sel => n.matches(sel))) return n;
            n = n.parentElement || (n.getRootNode && n.getRootNode().host) || null;
        }
        return null;
    }

    // A container the page deliberately made horizontally (or vertically)
    // scrollable -- e.g. .chapternav .wrap, the sticky chip strip, which
    // auto-scrolls its active chip toward the middle as the reader scrolls
    // the page -- legitimately has children outside its OWN visible
    // clientWidth/clientHeight at any given scroll position. That is the
    // scroll container's own job, not the text-overflow bug this test
    // hunts for, so any such ancestor between the text and the named
    // CONTAINER_SELECTORS box is skipped instead of flagged.
    function hasScrollableAncestor(el, stopAt) {
        // Inclusive of stopAt itself -- a CONTAINER_SELECTORS box (e.g.
        // plain .wrap) can itself BE the scrollable element, as
        // .chapternav .wrap is.
        let n = el;
        while (n && n !== document.body) {
            if (n instanceof Element) {
                const style = window.getComputedStyle(n);
                const scrollsX = (style.overflowX === 'auto' || style.overflowX === 'scroll') && n.scrollWidth > n.clientWidth + 1;
                const scrollsY = (style.overflowY === 'auto' || style.overflowY === 'scroll') && n.scrollHeight > n.clientHeight + 1;
                if (scrollsX || scrollsY) return true;
            }
            if (n === stopAt) break;
            n = n.parentElement;
        }
        return false;
    }

    function isVisible(el) {
        const r = el.getBoundingClientRect ? el.getBoundingClientRect() : null;
        if (!r || r.width === 0 || r.height === 0) return false;
        const style = window.getComputedStyle(el instanceof Element ? el : el.parentElement);
        if (style && (style.visibility === 'hidden' || style.display === 'none')) return false;
        return true;
    }

    function checkBox(label, box, containerBox, vTolerance) {
        if (!containerBox) return;
        const left = box.left >= containerBox.left - tolerance;
        const right = box.right <= containerBox.right + tolerance;
        const top = box.top >= containerBox.top - vTolerance;
        const bottom = box.bottom <= containerBox.bottom + vTolerance;
        if (!(left && right && top && bottom)) {
            out.notContained.push(
                `${label}: box=${JSON.stringify(box)} container=${JSON.stringify(containerBox)}`
            );
        }
    }

    // 2. Every visible text node (HTML) sits inside its nearest container.
    // Horizontal overflow (the maintainer's actual complaint: a label or
    // value running past the chart/card edge) is checked against the exact
    // Range box, at the 1px tolerance. Vertical containment is checked
    // against the Range's OWN box too, but with a wider tolerance (a font's
    // ascent/descent-based line-box, which Range.getClientRects() reports,
    // routinely overshoots a CSS line-height by a few px at font-sizes in
    // the page's 30-38px range -- a rendering-engine artifact, not a layout
    // bug the maintainer's screenshots ever flagged) -- confirmed against
    // the real page: a plain <div class="leadchart-lastval"> at font-size
    // 38px/line-height 38px still gets a Range box 46px tall, evenly
    // straddling its own element box by ~4px on each side.
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    let node;
    while ((node = walker.nextNode())) {
        const text = (node.textContent || '').trim();
        if (!text) continue;
        const parent = node.parentElement;
        if (!parent || !isVisible(parent)) continue;
        if (parent.closest('svg')) continue; // SVG text handled separately below
        const container = nearestContainer(parent);
        if (!container) continue;
        if (hasScrollableAncestor(parent, container)) continue;
        const containerBox = container.getBoundingClientRect();
        const fontPx = parseFloat(window.getComputedStyle(parent).fontSize) || 12;
        const vTolerance = Math.max(tolerance, fontPx * 0.25);
        // A CSS ellipsis (overflow:hidden + text-overflow:ellipsis) is a
        // PAINT-time truncation, not a layout one -- Range.getClientRects()
        // on the underlying text node still reports the full, un-clipped
        // text width even though nothing past the element's own clipped box
        // is actually drawn (confirmed against .essentiel-rank: computed
        // width shrank to 51px and the "…" painted correctly, but the same
        // text node's Range box still measured 76px). An element that
        // clips its own overflow this way is checked by its OWN
        // (already-clipped) box instead of the Range's -- exactly the
        // "ellipsis clipping" case section 4 below is for, so a visually
        // fine ellipsis never gets flagged as an overflow here.
        const parentStyle = window.getComputedStyle(parent);
        const clips = parentStyle.overflow === 'hidden' || parentStyle.overflowX === 'hidden';
        const rects = clips ? [parent.getBoundingClientRect()] : Array.from((() => {
            const range = document.createRange();
            range.selectNodeContents(node);
            return range.getClientRects();
        })());
        rects.forEach((r, i) => {
            if (r.width === 0 && r.height === 0) return;
            checkBox(`text "${text.slice(0, 40)}" in ${container.className || container.id}`, r, containerBox, vTolerance);
        });
    }

    // 3. Every visible SVG <text>/<tspan> sits inside its nearest container.
    document.querySelectorAll('svg text, svg tspan').forEach(el => {
        if (!isVisible(el)) return;
        const svg = el.closest('svg');
        if (!svg) return;
        const container = nearestContainer(svg);
        if (!container) return;
        if (hasScrollableAncestor(svg, container)) return;
        const containerBox = container.getBoundingClientRect();
        let box;
        try { box = el.getBoundingClientRect(); } catch (e) { return; }
        if (box.width === 0 && box.height === 0) return;
        checkBox(`svg text "${(el.textContent||'').slice(0,40)}" in ${container.className||container.id}`, box, containerBox, tolerance);
    });

    // 4. No ellipsis clipping in .mtile-bars, .mapbtns, .nbbar-name: the
    // element's own scrollWidth must not exceed its clientWidth, UNLESS the
    // element's own <title> carries the untouched full text (the batch's
    // approved way to shorten a label -- an ellipsis backed by a <title> is
    // the intended fix, not clipping).
    document.querySelectorAll('.mtile-bars, .mtile-bars *, .mapbtns button, .nbbar-name').forEach(el => {
        if (!isVisible(el)) return;
        const hasTitle = !!(el.querySelector && el.querySelector('title')) || !!el.title;
        if (hasTitle) return;
        if (el.scrollWidth > el.clientWidth + tolerance && el.scrollHeight <= el.clientHeight + tolerance) {
            out.ellipsisClipped.push(`${el.className}: scrollWidth ${el.scrollWidth} > clientWidth ${el.clientWidth}`);
        }
    });

    // 5. No intersecting neighbours-map labels (svg text inside the
    // neighbours map specifically -- the greedy placement's own job).
    document.querySelectorAll('.nbmap-box svg.map').forEach(svg => {
        const texts = Array.from(svg.querySelectorAll('text'))
            .filter(isVisible)
            .map(el => el.getBoundingClientRect());
        for (let i = 0; i < texts.length; i++) {
            for (let j = i + 1; j < texts.length; j++) {
                const a = texts[i], b = texts[j];
                const ix = a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top;
                if (ix) out.labelOverlaps.push(`labels ${i} and ${j} overlap`);
            }
        }
    });

    return out;
}
"""


@pytest.mark.parametrize("nis", NIS_CODES)
@pytest.mark.parametrize("width", WIDTHS)
@pytest.mark.parametrize("lang", LANGS)
def test_no_text_overflows_its_layout_box(chromium, site, nis, width, lang):
    ctx, page = _load(chromium, site, nis, width, lang)
    try:
        result = page.evaluate(
            _CHECK_JS, {"containerSelectors": CONTAINER_SELECTORS, "tolerance": TOLERANCE_PX}
        )
        assert not result[
            "overflow"
        ], f"nis={nis} w={width} lang={lang}: page overflow: {result['overflow']}"
        assert not result["notContained"], (
            f"nis={nis} w={width} lang={lang}: text outside its layout box "
            f"(first 8): {result['notContained'][:8]}"
        )
        assert not result["ellipsisClipped"], (
            f"nis={nis} w={width} lang={lang}: ellipsis-clipped text with no <title> "
            f"backup: {result['ellipsisClipped'][:8]}"
        )
        assert not result["labelOverlaps"], (
            f"nis={nis} w={width} lang={lang}: intersecting neighbours-map labels: "
            f"{result['labelOverlaps'][:8]}"
        )
    finally:
        ctx.close()


#: Live-site finding (lead, 2026-09-27): on Farciennes (52021, desktop) the
#: chips row ("Logement / Entreprises / Emploi / Securite") drew ON TOP of
#: the locator card's own legend ("Province - Province du Hainaut",
#: "Communes voisines - Commune - Farciennes"). #285's syncLocatorHeight()
#: set a FIXED height on .hero-locator equal to the photo's height, but the
#: locator's own content (mapbox min-height 210px + a legend that wraps to
#: two lines once region+province+neighbours+commune names are long enough)
#: can need more room than the photo does -- fixed height doesn't clip (no
#: overflow:hidden on .hero-locator) but overflow past a fixed-height box
#: is still drawn OVER whatever comes next in the page, here the chips row.
LOCATOR_NIS_CODES = ["52021", "21002", "92094"]
LOCATOR_WIDTHS = [1440, 1280, 1180, 1024]


@pytest.mark.parametrize("nis", LOCATOR_NIS_CODES)
@pytest.mark.parametrize("width", LOCATOR_WIDTHS)
def test_chips_row_never_overlaps_the_locator_card(chromium, site, nis, width):
    ctx, page = _load(chromium, site, nis, width, "fr")
    try:
        result = page.evaluate("""
            () => {
                const chips = document.getElementById('heroQuick');
                const locator = document.querySelector('.hero-locator');
                const legend = document.getElementById('locLegend');
                if (!chips || !locator) return { skip: true };
                const chipsBox = chips.getBoundingClientRect();
                const locatorBox = locator.getBoundingClientRect();
                const legendBox = legend ? legend.getBoundingClientRect() : null;
                const overlapsLocator = chipsBox.top < locatorBox.bottom && chipsBox.bottom > locatorBox.top
                    && chipsBox.left < locatorBox.right && chipsBox.right > locatorBox.left;
                // The legend must sit INSIDE the card (its own bottom edge
                // at or above the card's bottom edge) -- the direct
                // regression check for the fixed-height overflow.
                let legendOutsideCard = false;
                if (legendBox && legendBox.width > 0 && legendBox.height > 0) {
                    legendOutsideCard = legendBox.bottom > locatorBox.bottom + 1;
                }
                return {
                    skip: false,
                    overlapsLocator,
                    legendOutsideCard,
                    chipsBox, locatorBox, legendBox,
                };
            }
            """)
        if result.get("skip"):
            pytest.skip("heroQuick or .hero-locator not present on this build")
        assert not result["overlapsLocator"], (
            f"nis={nis} w={width}: chips row overlaps the locator card: "
            f"chips={result['chipsBox']} locator={result['locatorBox']}"
        )
        assert not result["legendOutsideCard"], (
            f"nis={nis} w={width}: locator legend spills past its own card's bottom edge: "
            f"legend={result['legendBox']} locator={result['locatorBox']}"
        )
    finally:
        ctx.close()
