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
            # Trilingual names travel in the payload, so the page can label a
            # figure in the reader's language without holding any indicator
            # metadata itself (CLAUDE.md rule 7).
            "names": {"en": "Population", "fr": "Population", "nl": "Bevolking"},
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

    # Missing indicator on this commune -> the label falls back to the id,
    # and the entry is unavailable with a reason rather than a zero or a
    # null rendered as "€0". Applies uniformly across every headline key --
    # unemployment and house price used to be structurally absent from the
    # whole pipeline (before the Census 2021 employment-status and
    # real-estate loads) and were tested with a different reason string;
    # now that both are real municipal indicators, a commune missing THEM
    # is exactly the same situation as a commune missing any other figure.
    for indicator_id in (
        "AVG_NET_TAXABLE_INCOME",
        "UNEMPLOYMENT_RATE_COM",
        "MEDIAN_HOUSE_PRICE",
    ):
        assert by_label[indicator_id]["available"] is False, indicator_id
        assert "why" in by_label[indicator_id]


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


# ── the percentile component (Block L) ─────────────────────────────────────


def _pct_lines(percentile_block):
    return _run_node(f"""
        const entry = {{periods: {{'2026': {{value: 1}}}},
                        percentile: {json.dumps(percentile_block)}}};
        console.log(JSON.stringify(LocalUI.percentileLines(entry)));
    """)


def test_a_percentile_always_states_its_universe_and_period():
    """The roadmap's requirement: "a percentile without a stated universe is
    unfalsifiable, and the first sceptical directeur financier will ask"."""
    lines = _pct_lines(
        {
            "national": {
                "scope_name": "Belgium",
                "pct": 14.1997,
                "rank": 499,
                "peers": 581,
                "period": "2023",
            }
        }
    )
    assert len(lines) == 1
    assert lines[0]["text"] == "14th percentile in Belgium"
    assert lines[0]["note"] == "499th of 581 communes"
    assert lines[0]["period"] == "2023"


def test_a_withheld_percentile_states_the_rank_instead():
    """Brussels-Capital's 19 communes. The exporter sends pct=None and the
    page must say "13th of 19", never invent a percentile."""
    lines = _pct_lines(
        {
            "regional": {
                "scope_name": "Brussels-Capital Region",
                "pct": None,
                "rank": 13,
                "peers": 19,
                "period": "2023",
            }
        }
    )
    assert lines[0]["text"] == "13th of 19 in Brussels-Capital Region"
    assert "too few communes" in lines[0]["note"]
    assert "percentile" not in lines[0]["text"]


def test_the_top_and_bottom_are_not_stated_as_a_percentile():
    """Under the rank definition the highest of 565 communes scores 99.91 and
    the lowest 0.09, which round to "100th" and "0th" -- both impossible
    positions, and both a claim the data does not support."""
    top = _pct_lines(
        {
            "national": {
                "scope_name": "Belgium",
                "pct": 99.9115,
                "rank": 1,
                "peers": 565,
                "period": "2026",
            }
        }
    )
    assert top[0]["text"] == "Highest of 565 communes in Belgium"
    assert "100th" not in top[0]["text"]

    bottom = _pct_lines(
        {
            "national": {
                "scope_name": "Belgium",
                "pct": 0.0885,
                "rank": 565,
                "peers": 565,
                "period": "2026",
            }
        }
    )
    assert bottom[0]["text"] == "Lowest of 565 communes in Belgium"
    assert "0th" not in bottom[0]["text"]


def test_a_near_extreme_percentile_is_clamped_rather_than_rounded_past_the_limit():
    """99.6 must not become "100th percentile" -- a position nobody can
    occupy -- just because it is not literally the top rank."""
    lines = _pct_lines(
        {
            "national": {
                "scope_name": "Belgium",
                "pct": 99.6,
                "rank": 3,
                "peers": 565,
                "period": "2026",
            }
        }
    )
    assert lines[0]["text"] == "99th percentile in Belgium"


def test_both_scopes_appear_national_first():
    lines = _pct_lines(
        {
            "regional": {
                "scope_name": "Flanders",
                "pct": 50.0,
                "rank": 150,
                "peers": 300,
                "period": "2026",
            },
            "national": {
                "scope_name": "Belgium",
                "pct": 40.0,
                "rank": 340,
                "peers": 565,
                "period": "2026",
            },
        }
    )
    assert [line["text"].split(" in ")[-1] for line in lines] == ["Belgium", "Flanders"]


def test_no_percentile_block_yields_no_lines():
    results = _run_node("""
        console.log(JSON.stringify(LocalUI.percentileLines({periods: {'2026': {value: 1}}})));
    """)
    assert results == []


def test_ordinals_handle_the_teens_correctly():
    results = _run_node("""
        console.log(JSON.stringify([1,2,3,4,11,12,13,21,22,23,101,111]
          .map(n => LocalUI.ordinal(n))));
    """)
    assert results == [
        "1st",
        "2nd",
        "3rd",
        "4th",
        "11th",
        "12th",
        "13th",
        "21st",
        "22nd",
        "23rd",
        "101st",
        "111th",
    ]


# ── the free four-commune picker (Block L) ─────────────────────────────────


def test_parse_vs_param_caps_at_three_peers():
    results = _run_node("""
        console.log(JSON.stringify(
            LocalUI.parseVsParam('11002,21004,52011,73028', '11001')
        ));
    """)
    assert results == ["11002", "21004", "52011"]


def test_parse_vs_param_excludes_the_primary_commune():
    """Comparing a commune to itself is not a peer comparison -- if the URL
    somehow names the commune it is already on, that entry is dropped."""
    results = _run_node("""
        console.log(JSON.stringify(LocalUI.parseVsParam('11001,21004', '11001')));
    """)
    assert results == ["21004"]


def test_parse_vs_param_deduplicates():
    results = _run_node("""
        console.log(JSON.stringify(LocalUI.parseVsParam('21004,21004,52011', '11001')));
    """)
    assert results == ["21004", "52011"]


def test_parse_vs_param_handles_absence():
    results = _run_node("""
        console.log(JSON.stringify([
            LocalUI.parseVsParam('', '11001'),
            LocalUI.parseVsParam(null, '11001'),
        ]));
    """)
    assert results == [[], []]


_PEER_INDICATOR = "POPULATION_BY_COMMUNE"
_PEERS = [
    {
        "nis_code": "21004",
        "name": {"en": "Brussels", "fr": "Bruxelles", "nl": "Brussel"},
        "indicators": {_PEER_INDICATOR: {"unit": "count", "periods": {"2026": {"value": 200.0}}}},
    },
    {
        "nis_code": "52011",
        "name": {"en": "Charleroi", "fr": "Charleroi", "nl": "Charleroi"},
        "indicators": {_PEER_INDICATOR: {"unit": "count", "periods": {"2026": {"value": 50.0}}}},
    },
]


def test_peer_rows_are_a_difference_from_the_primary_commune():
    """100 (own) vs 200 (peer) is +100%; 100 vs 50 is -50%. A peer commune is
    not a container the primary is part of, so this is always a DIFFERENCE,
    never a share -- unlike the additive/non-additive branch aggregate rows
    use."""
    results = _run_node(f"""
        console.log(JSON.stringify(
            LocalUI.peerRows(null, 100.0, 'en', {json.dumps(_PEERS)}, {json.dumps(_PEER_INDICATOR)})
        ));
    """)
    assert len(results) == 2
    assert results[0]["label"] == "Brussels"
    assert results[0]["value"] == 200.0
    assert results[0]["relation"] == {"kind": "diff", "pct": pytest.approx(100.0)}
    assert results[1]["relation"] == {"kind": "diff", "pct": pytest.approx(-50.0)}


def test_peer_rows_skip_a_peer_with_no_value_for_this_indicator():
    peers = [{"nis_code": "99999", "name": {"en": "Nowhere"}, "indicators": {}}]
    results = _run_node(f"""
        console.log(JSON.stringify(
            LocalUI.peerRows(null, 100.0, 'en', {json.dumps(peers)}, {json.dumps(_PEER_INDICATOR)})
        ));
    """)
    assert results == []


def test_peer_rows_empty_when_no_peers_or_no_indicator_id():
    results = _run_node(f"""
        console.log(JSON.stringify([
            LocalUI.peerRows(null, 100.0, 'en', [], {json.dumps(_PEER_INDICATOR)}),
            LocalUI.peerRows(null, 100.0, 'en', {json.dumps(_PEERS)}, null),
        ]));
    """)
    assert results == [[], []]


def test_comparison_rows_places_peers_between_the_commune_and_the_aggregates():
    """The order a reader sees: this commune, then the peers they chose to
    add, then the province/region/Belgium aggregates -- peers are the
    closest, most directly comparable rows and belong right after "this
    commune"."""
    entry = {
        "additive": True,
        "periods": {"2026": {"value": 100.0}},
        "comparison": {
            "province": {"name": {"en": "Some Province"}, "value": 1000.0, "period": "2026"}
        },
    }
    results = _run_node(f"""
        console.log(JSON.stringify(
            LocalUI.comparisonRows({json.dumps(entry)}, 'en',
                {{peers: {json.dumps(_PEERS)}, indicatorId: {json.dumps(_PEER_INDICATOR)}}})
        ));
    """)
    assert [r["scope"] for r in results] == ["commune", "peer", "peer", "province"]
