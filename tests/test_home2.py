"""Static contract for the public home2 homepage."""

import re
from pathlib import Path

from src.site.routes import is_indexable, sitemap_routes

REPO = Path(__file__).resolve().parents[1]
PAGE = REPO / "home2.html"
# home2-hero-brussels.png (the old static hero background, from before the
# brand film existed at all) is NOT in this list: the film-first redesign
# replaced the dark `.hero` section it decorated with `.filmhero`, so
# home2.html no longer references it. The file on disk is left alone here --
# removing an asset unrelated to this batch's own scope is a separate
# cleanup, flagged in the PR rather than done inline.
ASSETS = (
    "assets/belpulse/home2/home2-namur-card.png",
    "assets/belpulse/home2/home2-namur-cta.png",
)


def _html() -> str:
    return PAGE.read_text(encoding="utf-8")


def test_home2_has_the_reference_sections_and_generated_assets():
    """The film-first redesign (home-v5) replaced the single dark `.hero`
    section with a two-phase hero: `.filmhero` (phase 1, the brand film
    alone) and `.datahero` (phase 2, the commune-Portrait-styled data hero,
    holding the map/chart cards the old `.hero` used to carry directly).
    Every other reference section/asset below the hero is unchanged."""
    html = _html()
    for marker in (
        'class="filmhero"',
        'id="dataHero"',
        'id="financeStrip"',
        'id="featuredCard"',
        'id="themeThumbs"',
        'id="nationalGrid"',
        'class="features"',
        'class="cta"',
        'class="foot"',
    ):
        assert marker in html
    for relative in ASSETS:
        assert relative in html
        assert (REPO / relative).is_file()


def test_home2_reuses_the_shared_map_and_has_page_scoped_seven_band_palettes():
    """The three map-theme thumbnails still carry their own page-scoped
    7-band palette (unchanged from before). The hero map card itself no
    longer overrides the palette (the old dark `.home2 .hero-map` navy-tuned
    ramp went away with the dark hero) -- it now draws with the shared
    component's own default ramp, the same as every other light-card map on
    the site."""
    html = _html()
    assert '<script src="assets/commune_map.js"></script>' in html
    assert "new MapUI.CommuneMap" in html
    assert "class CommuneMap" not in html
    for palette_owner in (
        ".thumb:nth-child(1)",
        ".thumb:nth-child(2)",
        ".thumb:nth-child(3)",
    ):
        rule = re.search(re.escape(palette_owner) + r"\s*\{([^}]+)\}", html)
        assert rule, palette_owner
        assert all(f"--ramp-{band}:" in rule.group(1) for band in range(7))


def test_home2_uses_published_payloads_without_reference_statistics():
    html = _html()
    rendered_markup = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    assert "FEATURED_CONFIG_URL" in html
    assert "public/data/national.json" in html
    assert "public/data/manifest.json" in html
    assert "public/data/communes/" in html
    assert '<b id="statCommunes">—</b>' in html
    assert '<b id="statIndicators">—</b>' in html
    for copied_reference_value in ("172,4", "159,8", "133,2", "42,8", "11,7 M", "200+"):
        assert copied_reference_value not in rendered_markup


def test_home2_images_are_local_and_ai_disclosure_is_translated():
    html = _html()
    assert not re.search(r"<img[^>]+src=[\"']https?://", html, re.IGNORECASE)
    assert not re.search(r"url\([\"']?https?://", html, re.IGNORECASE)
    for key in ("homeAiNamurAlt", "homeAiImageNote", "homeFeaturedConfigured"):
        assert key in html
        strings = (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
        assert strings.count(f"{key}:") == 3


def test_home2_is_the_indexable_canonical_homepage():
    html = _html()
    assert '<meta name="robots" content="noindex">' not in html
    assert is_indexable("/home2.html")
    assert "/home2.html" in sitemap_routes()
    assert "/home2.html" in (REPO / "sitemap-pages.xml").read_text(encoding="utf-8")


def test_home2_hero_map_has_three_material_zones():
    """Batch A1.3 (docs/features/site_unification.md, "Home2 : corriger la
    composition"): the hero map card is materially three zones -- title +
    indicator picker, map + zoom controls, legend + source -- not one box
    with the zoom buttons floating over whatever happens to be underneath.
    Structural: the three zone classes exist inside the map card, in that
    order, and the zoom buttons live inside the map zone (mapbox), never as
    a sibling of maphead where they could land on top of the picker."""
    html = _html()
    card_match = re.search(r'id="mapCard"[^>]*>(.*?)<div class="hero-mini"', html, re.DOTALL)
    assert card_match, "could not find the hero map card markup"
    card = card_match.group(1)

    head_at = card.index('class="maphead"')
    zone_at = card.index('class="mapzone"')
    foot_at = card.index('class="mapfoot"')
    assert head_at < zone_at < foot_at, "the three zones must appear in this order"

    # The zoom buttons must be inside mapzone (in fact inside mapbox, which
    # is itself inside mapzone) -- not a sibling of maphead, or an absolute
    # position anchored to the whole card could still land on the picker.
    zoombtns_at = card.index('class="zoombtns"')
    mapfoot_open_at = card.index('<div class="mapfoot"')
    assert zone_at < zoombtns_at < mapfoot_open_at


def test_home2_hero_series_come_from_national_sections_config_not_the_page():
    """The hero's two mini-charts must be the ids config/national_sections.yaml
    declares under `hero:`, read at runtime from
    public/data/metadata/national_sections.json -- never a literal id typed
    into home2.html (claude.md rule 2/24, the same guarantee
    tests/test_macro.py's test_macro_names_no_indicator_anywhere gives
    macro.html)."""
    import yaml

    html = _html()
    layout = yaml.safe_load(
        (REPO / "config" / "national_sections.yaml").read_text(encoding="utf-8")
    )
    hero = layout.get("hero") or []
    assert hero, "config/national_sections.yaml declares no hero series"

    named = sorted(code for code in hero if code in html)
    assert not named, f"home2.html names hero indicator(s) directly: {named}"

    # And it must actually be wired to read the declared list, not just
    # happen to avoid the literal ids.
    assert "national_sections.json" in html
    assert "heroCodes" in html
    assert "heroOrder" in html


def test_home2_has_a_commune_search_wired_to_the_real_commune_page():
    """The film-first redesign (home-v5) added a commune search to the data
    hero, matching commune.html's own switcher pattern: every commune in
    view-src/geojson goes into the <datalist> as 'Name (nis)', and picking one
    (or typing the name and pressing enter, which fires `change`) navigates to
    the REAL commune.html?nis=... profile -- not a dead link, since this is
    the real site page, not the self-contained mockup that first proved the
    interaction. This replaces the old 'no search box' assertion: the old
    dark-hero layout deliberately had none, the new commune-Portrait-styled
    hero deliberately does."""
    html = _html()
    assert 'id="communeSearch"' in html
    assert 'id="communeList"' in html
    assert "wireSearch" in html
    assert "commune.html?nis=" in html


def test_home2_nav_uses_the_shared_header_typography():
    # No page-local font-size on the nav: the shared .bp-nav size applies,
    # the same as on every other page.
    assert not re.search(r"\.home2 \.bp-nav\{[^}]*font-size", _html())


# --- Batch A2b: visual polish -------------------------------------------------


def test_home2_headline_wraps_with_a_balanced_measure_not_a_tight_character_cap():
    """Item 1: 'mises à jour automatiquement' used to leave 'jour' alone on
    its own line in every theme, because the h1 was capped too tight for the
    column it sits in. text-wrap:balance lets the browser choose break points
    that avoid an orphan. The film-first redesign moved the headline from
    `.home2 .hero h1` into the phase-2 data hero's own `.datahero h1`; the
    same balanced-wrap contract still applies there."""
    html = _html()
    rule = re.search(r"\.datahero h1\{([^}]+)\}", html)
    assert rule, "no .datahero h1 rule"
    body = rule.group(1).replace(" ", "")
    assert "text-wrap:balance" in body
    assert "max-width:17ch" not in body, "still capped to the width that produced the orphan"


def test_home2_headline_translations_have_no_new_orphan_risk():
    """The fix is CSS, not wording (item 1 says so explicitly) -- so the same
    three strings from before must still be there, unedited, in all three
    languages."""
    strings = (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
    assert "homeTitle: 'Belgium’s economic data, updated automatically.'," in strings
    assert (
        "homeTitle: 'Les données économiques de la Belgique, mises à jour automatiquement.',"
        in strings
    )
    assert "homeTitle: 'De economische gegevens van België, automatisch bijgewerkt.'," in strings


def test_home2_finance_strip_is_simulated_with_a_link_to_the_chapter():
    """Item 2/3 built one compact line instead of four cards, each of which
    used to read 'Not published by this pipeline yet'. Issue #309 PR 2
    supersedes that honest-but-empty line: the strip now mounts five live
    simulated counters (the same shared engine as the Macro page's own
    chapter) with a link to the full chapter -- still one line/section, the
    old four-card markup still gone, but no longer a bare sentence."""
    html = _html()
    assert "ftile" not in html, "the old four-card markup/CSS is still here"
    assert '<section class="finance" id="financeStrip" data-simulated="true"' in html, (
        "the strip is not wired as a simulated region -- BPLiveCounters.mount() "
        'requires data-simulated="true" on its container'
    )
    strip = re.search(
        r'<section class="finance" id="financeStrip"[^>]*>(.*?)</section>', html, re.DOTALL
    )
    assert strip, "no #financeStrip section"
    body = strip.group(1)
    assert 'id="financeCompactText"' in body
    assert (
        'class="bp-simulated-badge' in body
    ), "no simulated badge -- mount() would throw without one"
    assert 'id="financePause"' in body
    assert 'class="fin-link"' in body
    assert (
        'href="macro.html#finances-publiques"' in body
    ), "the link no longer points at the full chapter"
    assert 'data-t="homeFinanceSeeDetails"' in body
    assert 'data-t="homeFinanceTitle"' in body


def test_home2_finance_fallback_is_honest_about_what_is_actually_missing():
    """Issue #309 PR 2 (round-2 fix, NEW P1-1): the OLD fallback sentence
    (FINANCE_KEYS + homeFinanceUnavailable: 'Not published by this pipeline
    yet') stopped being true the moment the Macro page's Official figures
    block shipped reading real Eurostat levels -- this pipeline does hold
    them now. FINANCE_KEYS/renderFinance() are gone; HomeFinance.
    renderFallback() says the narrower, still-true thing instead (only the
    LIVE trend simulation can be unavailable here), with a link to the
    chapter where the real figures already are. claude.md rule 26 in
    spirit still holds: the absence stays visible, just accurately named."""
    html = _html()
    assert "FINANCE_KEYS" not in html, "the old, now-false fixed editorial group is gone"
    assert "function renderFinance(" not in html

    fallback = re.search(r"function renderFallback\(reason\)\{(.*?)\n    \}", html, re.DOTALL)
    assert fallback, "HomeFinance.renderFallback() not found"
    body = fallback.group(1)
    for key in (
        "homeFinanceUnavailable",
        "homeFinanceWhy",
        "homeFinanceWhyLink",
        "homeFinanceExpired",
        "homeFinanceExpiredWhy",
    ):
        assert key in body, f"renderFallback() no longer reads {key}"
    assert "macro.html#finances-publiques" in body, "no link to where the real figures are"

    # The static, no-JS default says the same honest thing before any
    # script runs, never the old, now-false "not published" claim.
    static_default = re.search(r'id="financeCompactText">(.*?)</p>', html, re.DOTALL)
    assert static_default and "macro.html#finances-publiques" in static_default.group(1)

    strings = (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
    for key in (
        "homeFinanceUnavailable",
        "homeFinanceWhy",
        "homeFinanceWhyLink",
        "homeFinanceExpired",
        "homeFinanceExpiredWhy",
        "homeFinanceSeeDetails",
    ):
        assert strings.count(f"{key}:") == 3, f"{key} is not defined in all three languages"


def test_home2_heading_structure_exposes_one_h1_and_a_named_h2_per_section():
    """A2b audit, Finding 3: the compact finance strip (item 2/3) swapped the
    section's <h2> for a <strong>, so a screen-reader user navigating by
    heading no longer found 'Finances publiques' in the page outline, and
    <section class="finance"> lost the heading every sibling section still
    has. The visual result (small, inline with the compact sentence) is
    fine -- what regressed was the element, not the styling -- so this pins
    the element, not the look."""
    html = _html()
    body = re.search(r"<body[^>]*>(.*)</body>", html, re.DOTALL).group(1)

    # Exactly one <h1> on the page -- the hero title.
    assert len(re.findall(r"<h1\b", body)) == 1

    # The finance strip's title is a real heading element, not a <strong>
    # or a bare paragraph, so it shows up when navigating by heading.
    assert re.search(
        r'<h[1-6][^>]*\bdata-t="homeFinanceTitle"', body
    ), "finance strip title is not a real heading element"
    assert '<strong data-t="homeFinanceTitle"' not in body

    # Every top-level content section keeps its own named <h2>, matching
    # the site's existing pattern (commune profiles, maps, key indicators,
    # cta) -- five sections in the outline, not four.
    for key in (
        "homeFinanceTitle",
        "homeCommunesTitle",
        "homeMapsTitle",
        "homeIndicatorsTitle",
        "homeCtaTitle",
    ):
        assert re.search(rf'<h2\b[^>]*\bdata-t="{key}"', body), key


def test_home2_map_legend_caption_is_built_from_the_payload_not_hardcoded():
    """Item 4, and claude.md rules 2/24: the caption naming what the legend's
    ticks count must come from state.byCode's own name and
    MapUI.unitLabel(meta.unit, LANG) -- the same shared unit vocabulary the
    KPI cards use -- never a literal indicator name or unit string typed into
    this page."""
    html = _html()
    assert 'id="legendCaption"' in html
    draw = re.search(r"async function drawIndicator\(code\)\{(.*?)\n  \}", html, re.DOTALL)
    assert draw, "drawIndicator() not found"
    body = draw.group(1)
    assert "getElementById('legendCaption')" in body
    assert "MapUI.unitLabel(meta.unit, LANG)" in body
    assert "nameOf(code)" in body
    # Never assigned a literal string -- always the name/unit variables above.
    assert not re.search(r"legendCaption'\)\.textContent\s*=\s*['\"]", body)


def test_home2_thumbnail_captions_share_one_explicit_background_not_an_accidental_one():
    """Item 5: pixel-sampling the reviewer's own screenshots showed the three
    captions were already byte-identical in background colour -- the
    apparent 'first is white, the others are tinted' was each ramp's own
    lightest band meeting the caption with no real boundary between them.
    An explicit, uniform background + divider removes the ambiguity instead
    of leaving it to whatever the adjacent map colour happens to be."""
    html = _html()
    rule = re.search(r"\.thumb \.tcap\{([^}]+)\}", html)
    assert rule, "no .thumb .tcap rule"
    body = rule.group(1).replace(" ", "")
    assert "background:var(--bp-surface-alt)" in body
    assert "border-top:1pxsolidvar(--bp-border)" in body


def test_home2_licence_notice_is_a_native_disclosure_reachable_without_js():
    """Item 6: presentation changed (collapsed by default, one summary line,
    source/licence links visible without opening it) -- the notice itself did
    not. tests/test_statbel_attribution.py reads the full text out of this
    exact markup and must keep passing unweakened; this test pins the
    STRUCTURE that makes it reachable with no script running: a native
    <details>/<summary>, never `hidden`, and the full text still inside the
    same #attribution block."""
    html = _html()
    block = re.search(
        r'<div class="attribution" id="attribution">(.*?)</div>\s*\n<script',
        html,
        re.DOTALL,
    )
    assert block, "no #attribution block found before the first <script>"
    body = block.group(1)
    assert "<details" in body and "<summary" in body
    assert 'id="attrBoundaries"' in body
    assert 'id="attrValues"' in body
    assert 'id="attrUpdated"' in body

    details_tag = re.search(r"<details[^>]*>", body).group(0)
    assert "hidden" not in details_tag, "a hidden <details> defeats native no-JS toggling"

    summary = re.search(r"<summary[^>]*>(.*?)</summary>", body, re.DOTALL).group(1)
    for href in (
        "statbel.fgov.be",
        "onem.be",
        "police.be",
        "creativecommons.org/licenses/by/4.0",
    ):
        assert href in summary, f"{href} is not visible in the always-shown summary"
