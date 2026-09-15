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
