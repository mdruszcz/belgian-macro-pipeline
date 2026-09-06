"""Logic tests for local.html's pure-JS helpers (LocalUI), run under Node
since no browser is available in this environment -- the same method used
for Block G/I's end-to-end verification.

Only the DOM-free logic (search matching, ancestor walk, headline
availability) is covered here; canvas rendering and DOM wiring are the part
that genuinely needs a browser and is left to the manual phone check in
docs/steps.
"""

import json
import re
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LOCAL_HTML = REPO / "local.html"


def _extract_local_ui_js() -> str:
    """Pull out the first <script> block (the DOM-free LocalUI module) from
    local.html, so the test exercises the exact code the page ships, not a
    copy that could drift from it."""
    text = LOCAL_HTML.read_text(encoding="utf-8")
    scripts = re.findall(r"<script>(.*?)</script>", text, re.DOTALL)
    for script in scripts:
        if "LocalUI" in script and "module.exports" in script:
            return script
    raise AssertionError("Could not find the LocalUI <script> block in local.html")


def _run_node(js_body: str):
    harness = _extract_local_ui_js() + "\n" + js_body
    result = subprocess.run(["node", "-e", harness], capture_output=True, text=True, timeout=10)
    if result.returncode != 0:
        raise AssertionError(f"node failed:\n{result.stderr}")
    return json.loads(result.stdout)


GEOGRAPHIES = [
    {
        "geo_id": "be:country",
        "nis_code": "01000",
        "level": "country",
        "name": {"en": "Belgium", "fr": "Belgique", "nl": "Belgie"},
        "parent_geo_id": None,
    },
    {
        "geo_id": "be:reg:02000",
        "nis_code": "02000",
        "level": "region",
        "name": {"en": "Flanders", "fr": "Flandre", "nl": "Vlaanderen"},
        "parent_geo_id": "be:country",
    },
    {
        "geo_id": "be:prov:10000",
        "nis_code": "10000",
        "level": "province",
        "name": {"en": "Antwerp", "fr": "Anvers", "nl": "Antwerpen"},
        "parent_geo_id": "be:reg:02000",
    },
    {
        "geo_id": "be:mun:11001",
        "nis_code": "11001",
        "level": "municipality",
        "name": {"en": "Aartselaar", "fr": "Aartselaar", "nl": "Aartselaar"},
        "parent_geo_id": "be:prov:10000",
    },
    {
        "geo_id": "be:mun:11002",
        "nis_code": "11002",
        "level": "municipality",
        "name": {"en": "Antwerp", "fr": "Anvers", "nl": "Antwerpen"},
        "parent_geo_id": "be:prov:10000",
    },
]


def test_search_matches_any_of_the_three_languages_or_nis_code():
    results = _run_node(f"""
        const geos = {json.dumps(GEOGRAPHIES)};
        console.log(JSON.stringify({{
            byFrenchName: LocalUI.searchCommunes(geos, 'Anvers').map(g => g.geo_id),
            byNis: LocalUI.searchCommunes(geos, '11001').map(g => g.geo_id),
            excludesNonMunicipality: LocalUI.searchCommunes(geos, 'Flanders').map(g => g.geo_id),
        }}));
    """)
    assert results["byFrenchName"] == ["be:mun:11002"]
    assert results["byNis"] == ["be:mun:11001"]
    # "Flanders" matches a region's name, but searchCommunes only ever
    # returns municipality-level rows -- a region is not a commune page.
    assert results["excludesNonMunicipality"] == []


def test_ancestor_chain_is_ordered_root_to_leaf():
    results = _run_node(f"""
        const geos = {json.dumps(GEOGRAPHIES)};
        console.log(JSON.stringify(
            LocalUI.ancestorChain(geos, 'be:mun:11001').map(g => g.geo_id)
        ));
    """)
    assert results == ["be:country", "be:reg:02000", "be:prov:10000", "be:mun:11001"]


def test_children_of_returns_only_direct_children():
    results = _run_node(f"""
        const geos = {json.dumps(GEOGRAPHIES)};
        console.log(JSON.stringify(
            LocalUI.childrenOf(geos, 'be:prov:10000').map(g => g.geo_id)
        ));
    """)
    assert set(results) == {"be:mun:11001", "be:mun:11002"}


COMMUNE_WITH_PARTIAL_DATA = {
    "nis_code": "11001",
    "geo_id": "be:mun:11001",
    "name": {"en": "Aartselaar", "fr": "Aartselaar", "nl": "Aartselaar"},
    "region": "Flanders",
    "province": "Antwerp",
    "arrondissement": "Arrondissement Antwerpen",
    "indicators": {
        "POPULATION_BY_COMMUNE": {
            "name": "Population",
            "unit": "count",
            "periods": {
                "2020": {"value": 100, "status": "final"},
                "2021": {"value": 110, "status": "final"},
            },
        },
        # No LOCAL_UNITS_BY_COMMUNE, no AVG_NET_TAXABLE_INCOME, no POPULATION_CHANGE_5Y --
        # this commune only has population, the way a 2025-merger successor might.
    },
}


def test_headlines_report_unavailable_honestly_not_as_a_blank_or_zero():
    results = _run_node(f"""
        const commune = {json.dumps(COMMUNE_WITH_PARTIAL_DATA)};
        console.log(JSON.stringify(LocalUI.buildHeadlines(commune)));
    """)
    by_label = {r["label"]: r for r in results}

    assert by_label["Population"]["available"] is True
    assert by_label["Population"]["value"] == 110
    assert by_label["Population"]["period"] == "2021"

    # Missing indicator on this commune -> unavailable with a reason, not a
    # zero or a null value rendered as "€0".
    assert by_label["Avg. net taxable income"]["available"] is False
    assert "why" in by_label["Avg. net taxable income"]

    # Structurally absent from the whole pipeline (roadmap wants it, no
    # dataset provides it) -- a different reason string than "missing for
    # this commune", which is the point of local_ui.md's table.
    assert by_label["Unemployment"]["available"] is False
    assert "municipal level" in by_label["Unemployment"]["why"]
    assert by_label["Housing price"]["available"] is False
    assert "deferred" in by_label["Housing price"]["why"]


def test_latest_of_picks_the_most_recent_period_not_insertion_order():
    results = _run_node("""
        const entry = {name: 'Population', unit: 'count', periods: {
            '2021': {value: 110}, '2016': {value: 90}, '2020': {value: 100}
        }};
        console.log(JSON.stringify(LocalUI.latestOf(entry)));
    """)
    assert results["period"] == "2021"
    assert results["value"] == 110
