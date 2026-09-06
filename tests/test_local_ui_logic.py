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

import pytest

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


# ── the comparison column (Block L) ────────────────────────────────────────

_COMMUNE_WITH_COMPARISON = {
    "nis_code": "11002",
    "geo_id": "be:mun:11002",
    "name": {"en": "Antwerp", "fr": "Anvers", "nl": "Antwerpen"},
    "indicators": {
        "POPULATION_BY_COMMUNE": {
            "name": "Population",
            "unit": "count",
            "additive": True,
            "periods": {"2025": {"value": 100.0}, "2026": {"value": 200.0}},
            "comparison": {
                "province": {
                    "name": {"en": "Antwerp"},
                    "value": 1000.0,
                    "period": "2026",
                    "coverage": {"n": 69, "of": 69, "pct": 100.0},
                },
                "country": {
                    "name": {"en": "Belgium"},
                    "value": 4000.0,
                    "period": "2026",
                    "coverage": {"n": 565, "of": 565, "pct": 100.0},
                },
            },
        },
        "AVG_INCOME": {
            "name": "Average income",
            "unit": "eur",
            "additive": False,
            "periods": {"2023": {"value": 80.0}},
            "comparison": {
                "region": {
                    "name": {"en": "Flanders"},
                    "value": 100.0,
                    "period": "2023",
                    "coverage": {"n": 300, "of": 300, "pct": 100.0},
                }
            },
        },
    },
}


def _rows(indicator):
    return _run_node(f"""
        const commune = {json.dumps(_COMMUNE_WITH_COMPARISON)};
        console.log(JSON.stringify(
            LocalUI.comparisonRows(commune.indicators[{json.dumps(indicator)}])
        ));
    """)


def test_a_count_is_compared_as_a_share_of_its_reference():
    """A commune is PART of its province, so a difference would be
    arithmetically true and useless -- every commune is "below" its province,
    and a tiny one reads -100% against everything. 200 of 1000 is 20%."""
    rows = _rows("POPULATION_BY_COMMUNE")
    by_scope = {r["scope"]: r for r in rows}

    assert by_scope["commune"]["value"] == 200.0  # the latest period, not the first
    assert by_scope["province"]["relation"] == {"kind": "share", "pct": 20.0}
    assert by_scope["country"]["relation"] == {"kind": "share", "pct": 5.0}


def test_a_ratio_is_compared_as_a_difference_from_its_reference():
    """An average sits on the same scale as its reference, so a share would be
    meaningless. 80 against 100 is -20%."""
    rows = _rows("AVG_INCOME")
    region = next(r for r in rows if r["scope"] == "region")
    assert region["relation"]["kind"] == "diff"
    # approx, not exact: 80/100 - 1 is -0.19999999999999996 in IEEE754, and
    # pinning the exact float would be testing the arithmetic of the machine
    # rather than the behaviour of the code.
    assert region["relation"]["pct"] == pytest.approx(-20.0)


def test_the_commune_is_always_the_first_row_and_has_no_relation_to_itself():
    rows = _rows("AVG_INCOME")
    assert rows[0]["scope"] == "commune"
    assert "relation" not in rows[0]


def test_a_missing_province_simply_yields_one_row_fewer():
    """A Brussels commune has no province (geography.md Q3). The component
    must render what resolves rather than assume a fixed depth."""
    rows = _rows("POPULATION_BY_COMMUNE")
    assert [r["scope"] for r in rows] == ["commune", "province", "country"]

    rows = _rows("AVG_INCOME")
    assert [r["scope"] for r in rows] == ["commune", "region"]


def test_coverage_travels_with_every_reference_row():
    """An aggregate built from fewer communes than exist is a materially
    different number, so the count must reach the page, not stay in the CSV."""
    rows = _rows("POPULATION_BY_COMMUNE")
    province = next(r for r in rows if r["scope"] == "province")
    assert province["coverage"] == {"n": 69, "of": 69, "pct": 100.0}


def test_no_comparison_rows_when_there_is_nothing_to_compare_against():
    results = _run_node("""
        const entry = {unit: 'count', additive: true, periods: {'2026': {value: 5}}};
        console.log(JSON.stringify(LocalUI.comparisonRows(entry).length));
    """)
    # Only the commune's own row, so the caller renders no table at all.
    assert results == 1


def test_a_zero_reference_yields_no_relation_rather_than_infinity():
    results = _run_node("""
        const entry = {unit: 'count', additive: true, periods: {'2026': {value: 5}},
          comparison: {country: {name: {en: 'Belgium'}, value: 0, period: '2026'}}};
        const rows = LocalUI.comparisonRows(entry);
        console.log(JSON.stringify(rows[1].relation));
    """)
    assert results is None
