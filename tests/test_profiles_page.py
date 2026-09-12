"""Contracts for the data-complete commune view in profiles.html."""

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PROFILES_HTML = REPO / "profiles.html"
GEOGRAPHIES = REPO / "public" / "data" / "metadata" / "geographies.json"
COMMUNES = REPO / "public" / "data" / "communes"


def test_every_published_commune_can_open_a_full_profile():
    """Every municipality offered by the directory needs its one-fetch payload."""
    geographies = json.loads(GEOGRAPHIES.read_text(encoding="utf-8"))["geographies"]
    expected = {row["nis_code"] for row in geographies if row["level"] == "municipality"}
    published = {path.stem for path in COMMUNES.glob("*.json")}

    assert expected
    assert expected <= published, f"missing commune payloads: {sorted(expected - published)}"


def test_full_profile_renders_the_payload_not_a_fixed_indicator_list():
    """New indicators must appear after a data build without editing the page."""
    page = PROFILES_HTML.read_text(encoding="utf-8")

    assert "'public/data/communes/' + nis + '.json'" in page
    assert "var codes = Object.keys(indicators);" in page
    assert "state.sections = sectionsPayload.sections || [];" in page
    assert "Object.keys(remaining)" in page, "unsectioned indicators would disappear"
    assert "entry.periods" in page
    assert "entry.comparison" in page
    assert "entry.percentile" in page
    assert "profileIndicatorCard(code, indicators[code]" in page


def test_directory_routes_to_complete_profiles_and_keeps_visual_profile():
    page = PROFILES_HTML.read_text(encoding="utf-8")

    assert "return 'profiles.html?nis=' + nis" in page
    assert "return 'commune.html?nis=' + nis" in page
    assert 'id="profileInteractive"' in page
    assert 'id="profileJson"' in page
    assert 'id="indicatorSearch"' in page


def test_directory_keeps_its_overview_cards_compact_and_explains_the_map():
    """The landing directory summarizes types; cluster detail belongs downstream."""
    page = PROFILES_HTML.read_text(encoding="utf-8")

    assert "regionEmblem(code)" in page
    assert "FAMILY_ICON_INDEX" in page
    assert "fam.clusters.map(clusterLabel).join(' · ')" in page
    family_renderer = page.split("function renderFamilies(){", 1)[1].split(
        "/* ---- directory: the pickers", 1
    )[0]
    assert "box.appendChild(ul)" not in family_renderer
    assert 'class="province-map-block"' in page
    assert 'id="provLegend"' in page
    assert 'id="provZoomIn"' in page
    assert 'id="provZoomOut"' in page
    assert 'class="typology-explainer"' in page
