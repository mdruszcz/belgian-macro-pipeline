"""Static contract for the public home2 homepage."""

import re
from pathlib import Path

from src.site.routes import is_indexable, sitemap_routes

REPO = Path(__file__).resolve().parents[1]
PAGE = REPO / "home2.html"
ASSETS = (
    "assets/belpulse/home2/home2-hero-brussels.png",
    "assets/belpulse/home2/home2-namur-card.png",
    "assets/belpulse/home2/home2-namur-cta.png",
)


def _html() -> str:
    return PAGE.read_text(encoding="utf-8")


def test_home2_has_the_reference_sections_and_generated_assets():
    html = _html()
    for marker in (
        'class="hero"',
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
    html = _html()
    assert '<script src="assets/commune_map.js"></script>' in html
    assert "new MapUI.CommuneMap" in html
    assert "class CommuneMap" not in html
    for palette_owner in (
        ".home2 .hero-map",
        ".home2 .thumb:nth-child(1)",
        ".home2 .thumb:nth-child(2)",
        ".home2 .thumb:nth-child(3)",
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


def test_home2_has_no_search_box_and_uses_the_wide_layout():
    html = _html()
    assert 'id="communeSearch"' not in html
    assert "wireSearch" not in html
    assert "searchbox" not in html
    assert ".home2 .wrap{max-width:1400px;" in html


def test_home2_nav_uses_the_shared_header_typography():
    # No page-local font-size on the nav: the shared .bp-nav size applies,
    # the same as on every other page.
    assert not re.search(r"\.home2 \.bp-nav\{[^}]*font-size", _html())


def test_home2_hero_map_tooltip_is_opaque_navy():
    rule = re.search(r"\.hero-map \.map-tip\{([^}]+)\}", _html())
    assert rule, "no hero-map tooltip rule"
    assert "background:var(--bp-navy-surface)" in rule.group(1)
    assert "color:var(--bp-navy-text)" in rule.group(1)


# --- Batch A2b: visual polish -------------------------------------------------


def test_home2_headline_wraps_with_a_balanced_measure_not_a_tight_character_cap():
    """Item 1: 'mises à jour automatiquement' used to leave 'jour' alone on
    its own line in every theme, because the h1 was capped to 17ch -- tighter
    than the column it sits in actually allows. text-wrap:balance lets the
    browser choose break points that avoid an orphan; the widened cap gives
    it room to do that instead of being forced back to the same four lines."""
    html = _html()
    rule = re.search(r"\.home2 \.hero h1\{([^}]+)\}", html)
    assert rule, "no .home2 .hero h1 rule"
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


def test_home2_finance_strip_is_one_compact_line_not_four_cards():
    """Item 2/3: was four cards, each reading 'Not published by this pipeline
    yet' -- the widest band on the page, right under the hero. Same honest
    state (FINANCE_KEYS + homeFinanceUnavailable), built into one line with
    the 'see national data' link inline instead."""
    html = _html()
    assert "ftile" not in html, "the old four-card markup/CSS is still here"
    strip = re.search(
        r'<section class="finance" id="financeStrip">(.*?)</section>', html, re.DOTALL
    )
    assert strip, "no #financeStrip section"
    body = strip.group(1)
    assert 'id="financeCompactText"' in body
    assert 'class="fin-link"' in body
    assert 'data-t="homeAllIndicators"' in body
    assert 'data-t="homeFinanceTitle"' in body


def test_home2_finance_compact_sentence_reuses_the_existing_honest_strings():
    """The absence must remain visible (claude.md rule 26 in spirit -- an
    honest 'unavailable' must not quietly disappear): the compact line is
    still built from FINANCE_KEYS and the same homeFinanceUnavailable/
    homeFinanceWhy strings the four cards used, not a new invented one."""
    html = _html()
    render = re.search(r"function renderFinance\(\)\{(.*?)\n  \}", html, re.DOTALL)
    assert render, "renderFinance() not found"
    body = render.group(1)
    assert "FINANCE_KEYS.map" in body
    assert "listJoinerAnd" in body
    assert "homeFinanceUnavailable" in body
    assert "homeFinanceWhy" in body
    # And the joiner itself exists in all three languages.
    strings = (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
    assert strings.count("listJoinerAnd:") == 3


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
