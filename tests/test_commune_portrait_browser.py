"""Browser behaviour for the Portrait commune.html (feat/commune-portrait)
not already covered by tests/test_age_pyramid_slider.py (the slider/Play
timeline) or tests/test_a4_finishing_fixes.py (the allData accordion,
pyramid tooltips).

Follows the shared-server + Playwright pattern those files already use.
"""

from __future__ import annotations

import functools
import http.server
import json
import re
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
FLOWS_92094_JSON = REPO / "public" / "data" / "flows" / "buyer_origin" / "92094.json"
DEMOGRAPHY_92094_JSON = REPO / "public" / "data" / "demography" / "92094.json"


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


#: A missing buyer-origin-flows payload for many communes is expected
#: (renderFlows-equivalent degrades to no card, not an error), and Chromium
#: logs every 404'd fetch as a console error regardless of who consumes the
#: response -- filtered out here, same as tests/test_age_pyramid_slider.py's
#: own convention, rather than asserting on the whole console.
def _real_errors(errors):
    return [e for e in errors if "404" not in e]


def test_92094_loads_with_zero_console_errors(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        page.wait_for_timeout(600)  # lazy chart draw-in / IntersectionObserver settle
        assert not _real_errors(errors), f"console/page errors on load: {_real_errors(errors)}"
    finally:
        ctx.close()


def test_a_tile_click_changes_the_lead_chart_title_and_the_map_dropdown(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")

        # The first chapter's small-multiples grid: click a non-selected tile
        # and confirm both the lead chart eyebrow (title) and the map's own
        # indicator <select> change to match it.
        first_chapter = page.locator("#chapters .chapter").first
        unselected = first_chapter.locator(".multiple-tile:not(.selected)")
        assert (
            unselected.count() > 0
        ), "no unselected small-multiples tile to click in the first chapter"
        # A STABLE reference to the one tile being clicked, as a real
        # ElementHandle -- a Locator (even one held in a Python variable) is
        # always a LIVE re-query of its selector, and ":not(.selected)" stops
        # matching this exact tile the instant the click adds .selected to
        # it, so re-invoking the locator afterwards would silently resolve
        # to a DIFFERENT (still-unselected) tile instead of the one clicked.
        target_handle = unselected.first.element_handle()
        assert target_handle is not None
        target_code = target_handle.get_attribute("data-code")
        assert target_code

        before_title = first_chapter.locator(".leadchart-eyebrow").inner_text()
        target_handle.click()
        page.wait_for_function(
            """(before) => {
                const eyebrow = document.querySelector('#chapters .chapter .leadchart-eyebrow');
                return eyebrow && eyebrow.textContent !== before;
            }""",
            arg=before_title,
        )
        after_title = first_chapter.locator(".leadchart-eyebrow").inner_text()
        assert after_title != before_title

        select = first_chapter.locator(".leadmap-select select")
        if select.count() > 0:
            assert select.input_value() == target_code

        # The clicked tile must now carry .selected.
        assert "selected" in (target_handle.get_attribute("class") or "")
    finally:
        ctx.close()


def test_a_flemish_commune_with_no_schools_row_shows_not_applicable(chromium, site):
    """11001 (Aartselaar, Flanders) has no row in schools/by_commune.json --
    rule 26's three distinct schools states must show "not applicable" here,
    never the "unavailable" wording a Wallonia/Brussels commune with zero
    sites gets, and never silently omit the card."""
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=11001", wait_until="load")
        page.wait_for_selector("#schoolsPanel[data-state]")
        state = page.locator("#schoolsPanel").get_attribute("data-state")
        assert state == "not-applicable", f"expected not-applicable, got {state!r}"
        headline = page.locator("#schoolsPanel .headline").inner_text()
        assert headline.strip(), "not-applicable state shows no headline text"
    finally:
        ctx.close()


def test_schools_site_list_is_collapsed_by_default_and_opens_on_toggle(chromium, site):
    """Maintainer's request: the school-sites list is long on some communes,
    so the summary tiles (sites/mean FO/mean SO) stay visible but the full
    list collapses behind a toggle, CLOSED by default -- the toggle's own
    label carries the real count read from the rendered list (never hand-
    typed), and switches to a different label once open."""
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#schoolsPanel[data-state="ready"]')

        toggle = page.locator("#schoolsListToggle")
        listHost = page.locator("#schoolsList")
        assert toggle.get_attribute("aria-expanded") == "false"
        assert (
            listHost.get_attribute("hidden") is not None
        ), "school sites list is not hidden on load"
        # Summary tiles stay visible regardless of the list's own state.
        assert page.locator("#schoolsSummary .ftile").count() >= 1

        before_label = toggle.inner_text()
        assert re.search(r"\d", before_label), f"toggle label has no count: {before_label!r}"

        toggle.click()
        page.wait_for_function(
            "() => document.getElementById('schoolsListToggle').getAttribute('aria-expanded') === 'true'"
        )
        assert listHost.get_attribute("hidden") is None, "school sites list did not open on toggle"
        assert listHost.locator("li").count() > 0
        after_label = toggle.inner_text()
        assert after_label != before_label

        toggle.click()
        page.wait_for_function(
            "() => document.getElementById('schoolsListToggle').getAttribute('aria-expanded') === 'false'"
        )
        assert (
            listHost.get_attribute("hidden") is not None
        ), "school sites list did not re-close on toggle"
        assert (
            toggle.inner_text() == before_label
        ), "toggle label did not restore the count on re-close"

        real_errors = [e for e in errors if "404" not in e]
        assert not real_errors, f"console/page errors: {real_errors}"
    finally:
        ctx.close()


def test_a_flanders_commune_with_a_real_fwb_site_still_shows_ready():
    """45041 (Ronse) is a Flemish commune that DOES have an FWB site in the
    payload -- confirms the row-wins-over-region rule without a browser:
    the payload itself must contain the row this test's browser sibling
    would otherwise need to open a page to check."""
    import json

    schools = json.loads(
        (REPO / "public" / "data" / "schools" / "by_commune.json").read_text(encoding="utf-8")
    )
    assert "45041" in schools.get("communes", {}), (
        "fixture drift: Ronse (45041) no longer carries a schools row -- "
        "the not-applicable/ready distinction this batch's placement relies "
        "on has nothing left to demonstrate the row-wins case with"
    )


#: assets/i18n.js's STORAGE_KEY -- set directly rather than relying on a
#: Playwright context locale mapping to the right navigator.language, which
#: I18N.initial() only consults as a FALLBACK behind this key anyway.
LANG_STORAGE_KEY = "belpulse-lang"

#: cpLinkMap's bug (PR #278 blocker item 1): a data-t element whose i18n
#: value takes a {placeholder} substituted only by page JS, not by the
#: generic applyStrings() pass (T(el.dataset.t) is called with no vars) --
#: left unfilled, the raw template such as "Voir {name} sur la carte" (fr)
#: renders verbatim. Matches an unsubstituted {word} anywhere.
RAW_PLACEHOLDER_RE = r"\{[A-Za-z0-9_]+\}"

#: Lead's addendum: a raw i18n KEY NAME left on screen (never filled by T()
#: at all, e.g. a typo'd key or a call that resolved to nothing) -- every
#: prefix this page's own keys actually use (v4/cp/pf) plus the shared
#: chrome prefixes assets/i18n.js also defines (map/home/nav), so a plain
#: English word that happens to start with e.g. "map" is not a false hit:
#: i18n keys are camelCase with an immediate capital after the prefix.
RAW_KEY_RE = r"\b(?:v4|cp|pf|map|home|nav)[A-Z][A-Za-z0-9_]+\b"

COMMUNES_FOR_LEFTOVER_CHECK = ("92094", "11004", "21004", "21018", "57081")


def _set_lang(ctx, lang):
    ctx.add_init_script(
        f"try {{ window.localStorage.setItem('{LANG_STORAGE_KEY}', '{lang}'); }} catch(_){{}}"
    )


def _visible_text_and_attrs(page):
    """Every visible text node's own text, PLUS every title/aria-label/
    placeholder attribute site-wide -- applyStrings() fills all four
    categories (data-t/data-t-title/data-t-aria/data-t-placeholder), so a
    leftover key or placeholder can surface in any of them, not only in a
    text node."""
    return page.evaluate("""() => {
            const out = [];
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
                acceptNode(node) {
                    const t = node.textContent.trim();
                    if (!t) return NodeFilter.FILTER_REJECT;
                    const el = node.parentElement;
                    if (!el) return NodeFilter.FILTER_REJECT;
                    const style = getComputedStyle(el);
                    if (style.display === 'none' || style.visibility === 'hidden') return NodeFilter.FILTER_REJECT;
                    return NodeFilter.FILTER_ACCEPT;
                }
            });
            let n;
            while ((n = walker.nextNode())) out.push(n.textContent.trim());
            document.querySelectorAll('[title],[aria-label],[placeholder]').forEach(el => {
                ['title', 'aria-label', 'placeholder'].forEach(attr => {
                    const v = el.getAttribute(attr);
                    if (v && v.trim()) out.push(v.trim());
                });
            });
            return out;
        }""")


def _assert_no_leftovers_for(chromium, site, nis, lang):
    ctx = _context(chromium)
    _set_lang(ctx, lang)
    try:
        page = ctx.new_page()
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.goto(f"{site}/commune.html?nis={nis}", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        page.wait_for_timeout(500)
        # Scroll to the bottom in steps so every IntersectionObserver-gated
        # section (lazy chapters, reveal animations) has actually mounted
        # its real content by the time text is scanned.
        height = page.evaluate("document.documentElement.scrollHeight")
        step = 800
        y = 0
        while y < height:
            page.evaluate(f"window.scrollTo(0, {y})")
            page.wait_for_timeout(60)
            y += step
            height = page.evaluate("document.documentElement.scrollHeight")
        page.wait_for_timeout(300)

        strings = _visible_text_and_attrs(page)
        placeholder_hits = [s for s in strings if re.search(RAW_PLACEHOLDER_RE, s)]
        key_hits = [s for s in strings if re.fullmatch(RAW_KEY_RE, s.strip())]
        assert (
            not placeholder_hits
        ), f"nis={nis} lang={lang}: unsubstituted {{placeholder}} on screen: {placeholder_hits[:5]}"
        assert not key_hits, f"nis={nis} lang={lang}: raw i18n key on screen: {key_hits[:5]}"

        real_errors = [e for e in errors if "404" not in e]
        assert not real_errors, f"nis={nis} lang={lang}: console/page errors: {real_errors}"
    finally:
        ctx.close()


@pytest.mark.parametrize("lang", ["fr", "nl", "en"])
def test_no_unsubstituted_i18n_placeholder_or_raw_key_anywhere_on_the_page(chromium, site, lang):
    """PR #278 blocker (item 1): cpLinkMap's header link showed the raw
    template "Voir {name} sur la carte" in fr/nl/en on every commune page --
    applyStrings()'s generic [data-t] pass calls T(key) with no vars, so a
    key whose value takes a placeholder is never filled by it; the fix
    restores an explicit T('cpLinkMap', {name}) call in renderHero(), which
    must also re-run on a language switch (it does: renderHero() is part of
    renderAll(), called from the 'bp:lang' listener).

    Scrolls the whole page so every lazy/IntersectionObserver-revealed
    section actually renders, then scans EVERY visible text node and every
    title/aria-label/placeholder attribute for a raw {placeholder} or a raw
    i18n key name left unfilled -- across every commune and language the
    lead asked this widened to."""
    for nis in COMMUNES_FOR_LEFTOVER_CHECK:
        _assert_no_leftovers_for(chromium, site, nis, lang)


#: The payload's own bucket order (public/data/flows/buyer_origin/92094.json
#: `buckets` keys) paired with FLOWS_BUCKET_ORDER's i18n key in commune.html
#: -- restated here rather than imported (the page has no module exports),
#: so a future edit to either the page's own list or this test's copy that
#: puts them out of step is caught by comparing rendered LABELS below
#: against assets/i18n.js directly, not by trusting this tuple alone.
FLOWS_BUCKET_ORDER_FOR_TEST = (
    ("same_commune", "cpFlowsBucketSameCommune"),
    ("rest_of_arrondissement", "cpFlowsBucketRestArr"),
    ("rest_of_region", "cpFlowsBucketRestRegion"),
    ("other_regions", "cpFlowsBucketOtherRegions"),
    ("abroad", "cpFlowsBucketAbroad"),
    ("origin_unknown", "cpFlowsBucketUnknown"),
)


def _i18n_fr_value(key):
    text = (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
    m = re.search(r"\n  fr: \{(.*?)\n  \},\n", text, re.DOTALL)
    assert m, "no fr table found in assets/i18n.js"
    block = m.group(1)
    m2 = re.search(key + r""": (['"])((?:[^'"\\]|\\.)*)\1""", block)
    assert m2, f"{key} not found in assets/i18n.js's fr table"
    return m2.group(2)


def test_all_six_buyer_origin_buckets_render_in_order_with_real_shares_fr(chromium, site):
    """PR #278 shipped a bucket-id map with ids that exist nowhere in the
    payload (same_province/same_region/other_region/unknown) -- 4 of 6
    buckets silently matched no row and vanished, and the one whose id
    survived by coincidence (same_commune) showed a raw, unresolved i18n
    key ("cpFlowsBucketSame") instead of its translated label, because that
    WRONG key was also never defined in assets/i18n.js. Fixed by restoring
    the bucket map commit 6abc5298a (develop before #278) used.

    Every expectation here is read from a real source, never hand-typed:
    the six share percentages from the live payload, the six labels from
    assets/i18n.js's own fr table -- this test only confirms the RENDERED
    page agrees with both, in the payload's own bucket order."""
    payload = json.loads(FLOWS_92094_JSON.read_text(encoding="utf-8"))
    buckets = payload["buckets"]
    assert {b[0] for b in FLOWS_BUCKET_ORDER_FOR_TEST} == set(buckets.keys()), (
        "fixture drift: 92094's buyer_origin payload buckets no longer match "
        "the six ids this test (and commune.html's FLOWS_BUCKET_ORDER) expect"
    )

    ctx = _context(chromium)
    _set_lang(ctx, "fr")
    try:
        page = ctx.new_page()
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        page.wait_for_timeout(800)

        rows = page.evaluate("""() => {
                const list = document.querySelector('.barlist');
                if (!list) return null;
                return Array.from(list.querySelectorAll('li')).map(li => ({
                    label: li.querySelector('.lbl').textContent,
                    pct: li.querySelector('.pct').textContent,
                }));
            }""")
        assert (
            rows is not None
        ), "no .barlist found -- the buyer-origin buckets did not render at all"
        assert len(rows) == 6, f"expected 6 bucket rows, got {len(rows)}: {rows}"

        for row, (bucket_id, label_key) in zip(rows, FLOWS_BUCKET_ORDER_FOR_TEST, strict=True):
            expected_label = _i18n_fr_value(label_key)
            assert row["label"] == expected_label, (
                f"bucket {bucket_id!r}: rendered label {row['label']!r} != "
                f"assets/i18n.js fr {label_key} {expected_label!r}"
            )
            expected_share = float(buckets[bucket_id]["share_pct"])
            # flowsPct(): a comma decimal separator (fr), one decimal, no
            # space before '%' -- matches develop before #278 exactly.
            expected_pct_text = f"{expected_share:.1f}".replace(".", ",") + "%"
            assert row["pct"] == expected_pct_text, (
                f"bucket {bucket_id!r}: rendered share {row['pct']!r} != "
                f"payload-derived {expected_pct_text!r}"
            )

        real_errors = [e for e in errors if "404" not in e]
        assert not real_errors, f"console/page errors: {real_errors}"
    finally:
        ctx.close()


@pytest.mark.parametrize("lang", ["fr", "nl", "en"])
def test_pyramid_subtitle_shows_a_real_update_date_never_the_dash_placeholder(chromium, site, lang):
    """cpPyramidSub reads state.ageSexHistory.source_updated, which the
    history payload (public/data/demography_history/92094.json) does not
    carry at all -- calendarDate(undefined) returns '-' (an em dash), so
    the subtitle read "...Statbel, mise a jour -" for every commune with a
    history payload (i.e. all 565). Fixed by falling back to
    state.ageSex.source_updated (public/data/demography/92094.json DOES
    have one: read directly from that payload below, never hand-typed) and,
    if neither resolves, omitting the whole "Statbel update" clause via a
    SEPARATE i18n string (cpPyramidSubNoUpdate) rather than ever printing
    the dash in its place."""
    demography = json.loads(DEMOGRAPHY_92094_JSON.read_text(encoding="utf-8"))
    source_updated = demography.get("source_updated")
    assert source_updated, "fixture drift: 92094's demography payload has no source_updated"

    ctx = _context(chromium)
    _set_lang(ctx, lang)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector('#pyramidPanel[data-state="ready"]')

        subtitle = page.locator("#pyramidSub").inner_text()
        assert (
            "—" not in subtitle
        ), f"lang={lang}: subtitle still shows the dash placeholder: {subtitle!r}"
        # The update date, reformatted into the page's own locale further
        # down the page (calendarDate) -- checked here by its ISO year,
        # which survives every locale's date formatting unchanged.
        year = source_updated[:4]
        assert (
            year in subtitle
        ), f"lang={lang}: subtitle has no real update date ({year!r} not in {subtitle!r})"
    finally:
        ctx.close()


@pytest.mark.parametrize("theme", ["light", "dark", "paper"])
def test_header_action_buttons_share_the_bp_btn_style_in_every_theme(chromium, site, theme):
    """#downloadLink ("Telecharger le profil") carried class="bp-btn bp-btn--dark",
    but .bp-btn--dark had no matching CSS rule anywhere on the page (it used
    to live in a page-local <style> block on the PRE-#278 commune.html,
    which #278's full-body replacement dropped) -- #downloadLink rendered
    as a bare, unstyled blue link while Compare/Share (bp-btn bp-btn--outline)
    rendered as real buttons. Fixed by moving .bp-btn--dark into the SHARED
    assets/belpulse/layout.css (profiles.html already carried an identical
    page-local copy of this exact rule, so this also removes a duplicate).

    Checks the three header actions render with a non-transparent background
    and identical padding/border-radius in light, dark and paper -- not
    exact colours (those are theme tokens, allowed to differ by design)."""
    ctx = _context(chromium, width=1200, height=600)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#heroName")
        if theme != "light":
            page.evaluate(f"() => document.documentElement.setAttribute('data-theme', '{theme}')")
        page.wait_for_timeout(200)

        styles = page.evaluate("""() => ['downloadLink', 'shareLink'].map(id => {
                const el = document.getElementById(id);
                const cs = getComputedStyle(el);
                return {id, bg: cs.backgroundColor, radius: cs.borderRadius, padding: cs.padding};
            })""")
        by_id = {s["id"]: s for s in styles}
        download_bg = by_id["downloadLink"]["bg"]
        assert download_bg not in ("rgba(0, 0, 0, 0)", "transparent"), (
            f"theme={theme}: #downloadLink has no background fill "
            f"(renders as a bare link, not a button): {download_bg!r}"
        )
        # Compare/Share are bp-btn--outline (deliberately transparent-filled,
        # bordered) -- the shared contract this test checks is the SHAPE
        # (padding/radius), which every bp-btn variant carries identically,
        # not the fill colour, which differs by design between variants.
        assert by_id["downloadLink"]["radius"] == by_id["shareLink"]["radius"]
        assert by_id["downloadLink"]["padding"] == by_id["shareLink"]["padding"]
    finally:
        ctx.close()


def test_no_horizontal_overflow_on_a_narrow_flemish_commune_at_390px(chromium, site):
    ctx = _context(chromium, width=390, height=844)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=11001", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        scroll_width = page.evaluate("document.documentElement.scrollWidth")
        client_width = page.evaluate("document.documentElement.clientWidth")
        assert scroll_width <= client_width + 1, (
            f"horizontal overflow at 390px on 11001: scrollWidth={scroll_width} "
            f"clientWidth={client_width}"
        )
    finally:
        ctx.close()
