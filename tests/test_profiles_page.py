"""Contracts for the commune directory (profiles.html) and its redirect to
the data-complete commune view, which A3.2 moved to commune.html.

Before A3.2, profiles.html?nis= rendered every indicator and every published
period itself. That view now lives in commune.html's <details id="allData">,
ported outright rather than duplicated, and profiles.html?nis= redirects
there as early as a script can run in <head>, before anything renders."""

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PROFILES_HTML = REPO / "profiles.html"
COMMUNE_HTML = REPO / "commune.html"
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
    """New indicators must appear after a data build without editing the page.

    Re-pointed at commune.html by A3.2: the every-indicator-every-period
    renderer moved there outright, so this is now commune.html's contract,
    not profiles.html's."""
    page = COMMUNE_HTML.read_text(encoding="utf-8")

    assert "'public/data/communes/' + nis + '.json'" in page
    assert "var codes = Object.keys(indicators);" in page
    assert "(state.sections.sections || []).forEach(function(section){" in page
    assert "Object.keys(remaining)" in page, "unsectioned indicators would disappear"
    assert "entry.periods" in page
    assert "entry.comparison" in page
    assert "entry.percentile" in page
    assert "allDataIndicatorCard(code, indicators[code])" in page
    assert 'id="allData"' in page
    assert 'id="allDataSections"' in page


def test_profiles_nis_redirects_to_commune_html_before_anything_renders():
    """The legacy full-profile URL (rule 31: it must keep working) now leaves
    immediately for commune.html, ahead of any render -- and ahead of the
    theme-flash script, which this batch does not touch."""
    page = PROFILES_HTML.read_text(encoding="utf-8")

    head, _, rest = page.partition("</head>")
    assert "location.replace('commune.html'" in head, "the redirect must run in <head>"
    assert "nis=(\\d{5})" in head, "nis must be validated as a five-digit code"

    theme_script_pos = head.index("belpulse-theme")
    redirect_pos = head.index("location.replace('commune.html'")
    assert redirect_pos < theme_script_pos, "the redirect must run before the theme script"

    assert "<noscript>" in rest, "a no-JS reader needs a fallback link"

    # The retired view and its renderer are gone outright, not just hidden.
    assert 'id="viewProfile"' not in page
    assert "profileIndicatorCard" not in page
    assert "paintProfile" not in page
    assert (
        "return 'profiles.html?nis=' + nis" not in page
    ), "no commune link may still point back at the retired ?nis= view"


def test_directory_links_every_commune_to_the_interactive_profile():
    page = PROFILES_HTML.read_text(encoding="utf-8")

    assert "return 'commune.html?nis=' + nis" in page
    # Carries the directory's own current filter query back through the round
    # trip, so commune.html's breadcrumb can return to the filtered view.
    assert "'&from=' + encodeURIComponent(window.location.search.slice(1))" in page


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
