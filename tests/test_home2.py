"""Static contract for the intentionally unreleased home2 visual test."""

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


def test_home2_is_exact_route_noindex_and_absent_from_sitemaps():
    html = _html()
    assert '<meta name="robots" content="noindex">' in html
    assert not is_indexable("/home2.html")
    assert "/home2.html" not in sitemap_routes()
    for sitemap in ("sitemap.xml", "sitemap-pages.xml", "local/sitemap.xml"):
        assert "/home2.html" not in (REPO / sitemap).read_text(encoding="utf-8")
