"""The map panel on communes.html.

The panel draws the SAME table it sits above -- same indicator, same year, same
filters -- so what matters here is that it cannot become a second, disagreeing
copy of the data, and that adding it did not put the table itself at risk. The
map is an addition to that page; the table is its job.

Behaviour that needs a browser (drawing, panning, the year following the
selector) is covered by the headless check recorded in docs/steps.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
COMMUNES_HTML = REPO / "communes.html"


@pytest.fixture(scope="module")
def page() -> str:
    return COMMUNES_HTML.read_text(encoding="utf-8")


def test_the_panel_is_collapsed_until_asked_for(page):
    """The boundary file is 1.2 MB. Most visitors to a data TABLE came for the
    table, and making every one of them download a map they never opened would
    be a tax on the page's main job."""
    panel = re.search(r'<div class="map-panel([^"]*)" id="mapPanel">', page)
    assert panel, "no map panel found"
    assert "map-hidden" in panel.group(1), "the map panel is not collapsed by default"


def test_boundaries_are_fetched_only_when_the_map_opens(page):
    """The fetch must sit inside openMap(), not at page load."""
    open_map = re.search(r"async function openMap\(\)\{(.*?)\n\}", page, re.DOTALL)
    assert open_map, "openMap() not found"
    assert "communes.geojson" in open_map.group(1)
    # ...and nowhere else, which is what makes it lazy.
    assert page.count("communes.geojson") == 1, "boundaries referenced outside openMap()"


def test_a_suppressed_cell_is_never_drawn_as_a_value(page):
    """ONEM withholds counts under 10: those arrive with status 'S' and an
    EMPTY value. Painting one as 0 would put the commune at the bottom of the
    colour scale and state, in colour, a figure the source refused to publish.
    """
    body = re.search(r"function valuesForMap\(.*?\n\}", page, re.DOTALL).group(0)
    assert "isNaN(num)" in body, "non-numeric cells are not separated out"
    # A withheld cell must never acquire a number on the way through.
    assert "value: null" in body, "a withheld cell is not passed through as null"
    assert "parseFloat" in body and "|| 0" not in body, "a blank must not become a zero"


def test_a_withheld_cell_is_passed_through_so_the_map_can_say_why(page):
    """It is still never COLOURED -- the component only paints a numeric value
    -- but dropping it left the map unable to tell a figure the source withheld
    from one that never existed, a distinction the table beside it has always
    made with its Suppressed pill."""
    body = re.search(r"function valuesForMap\(.*?\n\}", page, re.DOTALL).group(0)
    assert "'suppressed'" in body, "suppressed cells are not recognised"
    # And only suppressed: a merely blank cell is "no row", a different fact.
    assert re.search(
        r"status === 'suppressed'\s*\)\s*values\[", body
    ), "every non-numeric cell is passed through, not only the withheld ones"


def test_the_map_component_distinguishes_withheld_from_never_collected(page):
    """The tooltip must have three branches, not two. Asserted on the shared
    component, which is where both maps get this behaviour."""
    component = (REPO / "assets" / "commune_map.js").read_text(encoding="utf-8")
    tip = re.search(r"_tipHtml\(f\) \{(.*?)\n  \}", component, re.DOTALL).group(1)
    assert "suppressed" in tip, "no withheld branch in the tooltip"
    # The wording itself now lives in assets/i18n.js, in three languages, so
    # the component is asserted to reach for the right KEYS rather than to
    # contain the English sentences it used to hold.
    assert "mapNoValueHere" in tip, "no never-collected branch in the tooltip"
    assert "mapWithheld" in tip, "no withheld wording in the tooltip"
    strings = (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
    assert "not zero" in strings, "the withheld wording does not rule out a zero reading"


def test_the_map_reads_the_table_rather_than_fetching_its_own_data(page):
    """One source of truth per figure. If the map fetched its own copy, the two
    halves of this page could show different numbers for the same commune."""
    body = re.search(r"function valuesForMap\(.*?\n\}", page, re.DOTALL).group(0)
    assert "COMMUNES" in body, "the map does not read the table's own rows"
    assert "fetch(" not in body


def test_the_map_follows_the_year_the_table_is_showing(page):
    """The whole reason this map exists alongside map.html: map.html reads
    latest-value payloads and can only ever draw the most recent figure, while
    this page holds every year."""
    body = re.search(r"function refreshMap\(\)\{(.*?)\n\}", page, re.DOTALL).group(1)
    assert "selectedYear" in body, "the map ignores the year selector"
    assert "visibleNisSet()" in body, "the map ignores the region filter and search"


def test_render_refreshes_the_map_but_never_depends_on_it(page):
    """render() draws the table. If the map script failed to load, the table
    must still render -- so the hook is guarded, not assumed."""
    render = re.search(r"\nfunction render\(\)\{(.*?)\n\}\n", page, re.DOTALL).group(1)
    assert "refreshMap()" in render, "the map does not follow the table's filters"
    assert (
        "typeof MapView !== 'undefined'" in render
    ), "render() would throw if the map script were missing, taking the table with it"


def test_the_panel_ids_the_script_wires_all_exist_in_the_markup(page):
    """A typo in one of these is invisible: the component would simply skip
    that element and, for the legend, silently draw nothing."""
    wired = set(re.findall(r"getElementById\('(map[A-Za-z]+)'\)", page))
    assert wired, "no map elements are wired"
    for element_id in wired:
        assert f'id="{element_id}"' in page, f"{element_id} is wired but not in the markup"


def test_the_page_still_carries_its_licence_attribution(page):
    """Adding a map must not have disturbed the attribution block -- it is a
    licence condition, and test_statbel_attribution.py covers its contents."""
    assert '<div class="attribution" id="attribution">' in page
