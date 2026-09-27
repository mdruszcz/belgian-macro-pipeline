"""Logic tests for BPSimilar's pure functions (assets/belpulse/similar.js),
run under Node -- the same method test_charts_logic.py uses for BPCharts.

Every function under test takes plain data: no DOM, no fetch, no indicator
id. The fixtures for `benchmarkState` below are copied from the real
public/data/peers/92094.json (Namur) and public/data/peers/11002.json
(Antwerp, its 5-year population change fixed to 4.6478 by PR A's
history-row-precedence fix) payloads, per the build spec's section 5.
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SIMILAR_JS = REPO / "assets" / "belpulse" / "similar.js"


def _run_node(js_body: str):
    """Run the harness through a temp FILE -- Windows caps a whole command
    line well under what `node -e <harness>` can need, the same reasoning
    tests/test_charts_logic.py documents."""
    harness = SIMILAR_JS.read_text(encoding="utf-8") + "\n" + js_body
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as handle:
        handle.write(harness)
        script = handle.name
    try:
        result = subprocess.run(
            ["node", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=10,
        )
    finally:
        os.unlink(script)
    if result.returncode != 0:
        raise AssertionError(f"node failed:\n{result.stderr}")
    return json.loads(result.stdout)


# ---------------------------------------------------------------------------
# deviationParts
# ---------------------------------------------------------------------------


def test_deviation_parts_rounds_and_directs_from_the_published_sign():
    cases = [
        2.5937,
        -7.6746,
        48.4383,
        48.6911,
        15.1515,
        21.9251,
        19.299,
        -1.4314,
        -100,
        99.94,
        99.96,
        100.4,
        8768.2,
        0.04,
        -0.04,
        0.05,
        0,
    ]
    result = _run_node(
        "console.log(JSON.stringify(" + json.dumps(cases) + ".map(BPSimilar.deviationParts)));"
    )
    expected = [
        {"direction": "above", "tiny": False, "magnitude": 2.6, "decimals": 1},
        {"direction": "below", "tiny": False, "magnitude": 7.7, "decimals": 1},
        {"direction": "above", "tiny": False, "magnitude": 48.4, "decimals": 1},
        {"direction": "above", "tiny": False, "magnitude": 48.7, "decimals": 1},
        {"direction": "above", "tiny": False, "magnitude": 15.2, "decimals": 1},
        {"direction": "above", "tiny": False, "magnitude": 21.9, "decimals": 1},
        {"direction": "above", "tiny": False, "magnitude": 19.3, "decimals": 1},
        {"direction": "below", "tiny": False, "magnitude": 1.4, "decimals": 1},
        {"direction": "below", "tiny": False, "magnitude": 100, "decimals": 0},
        {"direction": "above", "tiny": False, "magnitude": 99.9, "decimals": 1},
        {"direction": "above", "tiny": False, "magnitude": 100, "decimals": 0},
        {"direction": "above", "tiny": False, "magnitude": 100, "decimals": 0},
        {"direction": "above", "tiny": False, "magnitude": 8768, "decimals": 0},
        {"direction": "above", "tiny": True, "magnitude": 0.1, "decimals": 1},
        {"direction": "below", "tiny": True, "magnitude": 0.1, "decimals": 1},
        {"direction": "above", "tiny": False, "magnitude": 0.1, "decimals": 1},
        {"direction": "equal", "tiny": False, "magnitude": 0, "decimals": 1},
    ]
    assert result == expected


# ---------------------------------------------------------------------------
# ordinal
# ---------------------------------------------------------------------------


def test_ordinal_english_suffixes_with_11_13_exception():
    ns = [2, 3, 4, 8, 11, 12, 13, 21, 22]
    result = _run_node(
        "console.log(JSON.stringify(" + json.dumps(ns) + ".map(function(n){ "
        "return BPSimilar.ordinal(n, 'en'); })));"
    )
    assert result == ["2nd", "3rd", "4th", "8th", "11th", "12th", "13th", "21st", "22nd"]


def test_ordinal_french_and_dutch_always_append_e():
    result = _run_node(
        "console.log(JSON.stringify({"
        "fr2: BPSimilar.ordinal(2, 'fr'), fr5: BPSimilar.ordinal(5, 'fr'),"
        "nl2: BPSimilar.ordinal(2, 'nl'), nl5: BPSimilar.ordinal(5, 'nl')"
        "}));"
    )
    assert result == {"fr2": "2e", "fr5": "5e", "nl2": "2e", "nl5": "5e"}


# ---------------------------------------------------------------------------
# positionParts
# ---------------------------------------------------------------------------


def test_position_parts_highest_lowest_and_nth():
    result = _run_node(
        "console.log(JSON.stringify({"
        "highest: BPSimilar.positionParts({position: 1, of: 11}, 'en'),"
        "lowest: BPSimilar.positionParts({position: 11, of: 11}, 'en'),"
        "nth5: BPSimilar.positionParts({position: 5, of: 11}, 'en'),"
        "nth3: BPSimilar.positionParts({position: 3, of: 8}, 'en')"
        "}));"
    )
    assert result["highest"] == {"key": "simPosHighest", "ord": None}
    assert result["lowest"] == {"key": "simPosLowest", "ord": None}
    assert result["nth5"] == {"key": "simPosNth", "ord": "5th"}
    assert result["nth3"] == {"key": "simPosNth", "ord": "3rd"}


# ---------------------------------------------------------------------------
# benchmarkState -- fixtures copied from the real payloads
# ---------------------------------------------------------------------------

NAMUR_INCOME_REGION_ENTRY = {
    "value": 37748.548378031905,
    "peer_median": 36794.23148739447,
    "deviation_pct": 2.5937,
    "position": 5,
    "of": 11,
    "peers_with_value": 10,
    "period": "2023",
    "selection_variable": True,
}


def _sim_fixture(entry, list_name="region", n_members=10, extra_withheld=None):
    return {
        "min_peers_with_value": 7,
        "peers": {
            list_name: [{"nis": str(i), "rank": i + 1} for i in range(n_members)],
        },
        "lists": {list_name: ({"INCOME": entry} if entry else {})},
        "withheld": {list_name: (extra_withheld or {})},
    }


def test_benchmark_state_ok_for_a_consistent_entry():
    sim = _sim_fixture(NAMUR_INCOME_REGION_ENTRY)
    latest = {"period": "2023", "value": 37748.548378031905}
    result = _run_node(
        "console.log(JSON.stringify(BPSimilar.benchmarkState("
        + json.dumps(sim)
        + ", 'region', 'INCOME', "
        + json.dumps(latest)
        + ", {additive: false}).state));"
    )
    assert result == "ok"


def test_benchmark_state_mismatch_when_antwerp_value_disagrees_with_the_page():
    """Antwerp's POPULATION_CHANGE_5Y entry (pre-fix 6.837332386379735) does
    not match the page's own value (post-fix 4.6477851743035075) -- the
    consistency check must withhold the comparison rather than show two
    different numbers for the same figure."""
    entry = dict(NAMUR_INCOME_REGION_ENTRY, value=6.837332386379735, period="2026")
    sim = _sim_fixture(entry, list_name="national")
    latest = {"period": "2026", "value": 4.6477851743035075}
    result = _run_node(
        "console.log(JSON.stringify(BPSimilar.benchmarkState("
        + json.dumps(sim)
        + ", 'national', 'INCOME', "
        + json.dumps(latest)
        + ", {additive: false}).state));"
    )
    assert result == "mismatch"


def test_benchmark_state_mismatch_cases():
    latest = {"period": "2023", "value": 37748.548378031905}

    def state_for(entry, list_name="region", n_members=10):
        sim = _sim_fixture(entry, list_name=list_name, n_members=n_members)
        return _run_node(
            "console.log(JSON.stringify(BPSimilar.benchmarkState("
            + json.dumps(sim)
            + f", '{list_name}', 'INCOME', "
            + json.dumps(latest)
            + ", {additive: false}).state));"
        )

    # period disagrees with the page's own latest period.
    assert state_for(dict(NAMUR_INCOME_REGION_ENTRY, period="2025")) == "mismatch"
    # of != peers_with_value + 1 (10 != 10 + 1).
    assert state_for(dict(NAMUR_INCOME_REGION_ENTRY, of=10)) == "mismatch"
    # position out of [1, of] range.
    assert state_for(dict(NAMUR_INCOME_REGION_ENTRY, position=12)) == "mismatch"
    # peers_with_value below the published minimum (7).
    assert state_for(dict(NAMUR_INCOME_REGION_ENTRY, peers_with_value=6, of=7)) == "mismatch"


def test_benchmark_state_null_deviation_reasons():
    latest = {"period": "2023", "value": 37748.548378031905}

    def state_for(deviation_withheld):
        entry = dict(NAMUR_INCOME_REGION_ENTRY, deviation_pct=None)
        if deviation_withheld is not None:
            entry["deviation_withheld"] = deviation_withheld
        sim = _sim_fixture(entry)
        return _run_node(
            "console.log(JSON.stringify(BPSimilar.benchmarkState("
            + json.dumps(sim)
            + ", 'region', 'INCOME', "
            + json.dumps(latest)
            + ", {additive: false}).state));"
        )

    assert state_for("median_zero") == "no_pct_zero"
    assert state_for("median_negative") == "no_pct_negative"
    assert state_for(None) == "no_pct_other"


def test_benchmark_state_peer_deviation_none_wins_over_a_numeric_or_null_deviation():
    latest = {"period": "2023", "value": 37748.548378031905}
    sim_numeric = _sim_fixture(NAMUR_INCOME_REGION_ENTRY)
    sim_null = _sim_fixture(dict(NAMUR_INCOME_REGION_ENTRY, deviation_pct=None))

    for sim in (sim_numeric, sim_null):
        result = _run_node(
            "console.log(JSON.stringify(BPSimilar.benchmarkState("
            + json.dumps(sim)
            + ", 'region', 'INCOME', "
            + json.dumps(latest)
            + ", {additive: false, peer_deviation: 'none'}).state));"
        )
        assert result == "no_pct_config"


def test_benchmark_state_by_withheld_reason_when_no_entry_exists():
    latest = {"period": "2024", "value": 1}

    def state_for(withheld):
        sim = _sim_fixture(None, extra_withheld={"X": withheld} if withheld else {})
        return _run_node(
            "console.log(JSON.stringify(BPSimilar.benchmarkState("
            + json.dumps(sim)
            + ", 'region', 'X', "
            + json.dumps(latest)
            + ", {additive: false}).state));"
        )

    assert state_for({"reason": "few_peers", "period": "2024", "peers_with_value": 6}) == (
        "few_peers"
    )
    assert (
        state_for({"reason": "no_current_value", "period": "2025", "own_period": "2024"}) == "stale"
    )
    assert (
        state_for({"reason": "no_current_value", "period": "2025", "own_period": None}) == "generic"
    )
    assert state_for({"reason": "excluded"}) == "excluded"
    assert state_for(None) == "generic"


def test_benchmark_state_suppressed_and_na_reasons_from_pr_287():
    """PR #287 (peer model, not yet merged as of this writing) adds two more
    specific reasons for the commune's OWN row existing at the current
    period but not being usable: 'suppressed' (status suppressed -- the
    source withholds it, e.g. statistical confidentiality) and 'na' (status
    na -- no figure at all). Both are distinct from 'stale' (an older period
    IS available, no_current_value with an own_period) and from 'generic'
    (nothing else fits) -- rule 26, missing/suppressed/na never collapse.
    similar.js must keep working whether or not #287 has merged: an
    unrecognised reason string still falls back to 'generic' (covered by
    test_benchmark_state_by_withheld_reason_when_no_entry_exists' state_for
    (None) case and the 'generic' no_current_value case above)."""
    latest = {"period": "2024", "value": 1}

    def state_for(withheld):
        sim = _sim_fixture(None, extra_withheld={"X": withheld} if withheld else {})
        return _run_node(
            "console.log(JSON.stringify(BPSimilar.benchmarkState("
            + json.dumps(sim)
            + ", 'region', 'X', "
            + json.dumps(latest)
            + ", {additive: false}).state));"
        )

    assert (
        state_for({"reason": "suppressed", "period": "2026", "own_period": "2024"}) == "suppressed"
    )
    assert state_for({"reason": "na", "period": "2026", "own_period": "2024"}) == "na"
    # An unrecognised reason (e.g. a future addition this module doesn't
    # know about yet) still falls back to 'generic', never crashes.
    assert state_for({"reason": "some_future_reason", "period": "2026"}) == "generic"


def test_benchmark_state_none_when_additive_or_list_off_or_no_sim():
    latest = {"period": "2023", "value": 37748.548378031905}
    sim = _sim_fixture(NAMUR_INCOME_REGION_ENTRY)

    def state_for(sim_arg, list_arg, meta_extra):
        meta = dict({"additive": False}, **meta_extra)
        return _run_node(
            "console.log(JSON.stringify(BPSimilar.benchmarkState("
            + json.dumps(sim_arg)
            + f", '{list_arg}', 'INCOME', "
            + json.dumps(latest)
            + ", "
            + json.dumps(meta)
            + ").state));"
        )

    assert state_for(sim, "region", {"additive": True}) == "none"
    assert state_for(sim, "region", {"additive": None}) == "none"
    assert state_for(sim, "off", {}) == "none"
    assert state_for(None, "region", {}) == "none"


def test_benchmark_state_offers_the_other_list_when_it_is_ok():
    """Namur's municipal debt: the national list is few_peers, but the
    region list has a valid entry -- `other` must be true so the page can
    offer the "compare within the same region instead" button."""
    sim = {
        "min_peers_with_value": 7,
        "peers": {
            "region": [{"nis": str(i), "rank": i + 1} for i in range(10)],
            "national": [{"nis": str(i), "rank": i + 1} for i in range(10)],
        },
        "lists": {
            "region": {
                "DEBT": {
                    "value": 2965.5,
                    "peer_median": 1997.8,
                    "deviation_pct": 48.4383,
                    "position": 3,
                    "of": 11,
                    "peers_with_value": 10,
                    "period": "2024",
                    "selection_variable": False,
                }
            },
            "national": {},
        },
        "withheld": {
            "national": {"DEBT": {"reason": "few_peers", "period": "2024", "peers_with_value": 6}},
            "region": {},
        },
    }
    latest = {"period": "2024", "value": 2965.5}
    result = _run_node(
        "console.log(JSON.stringify(BPSimilar.benchmarkState("
        + json.dumps(sim)
        + ", 'national', 'DEBT', "
        + json.dumps(latest)
        + ", {additive: false})));"
    )
    assert result["state"] == "few_peers"
    assert result["other"] is True


# ---------------------------------------------------------------------------
# peerRuns
# ---------------------------------------------------------------------------


def test_peer_runs_splits_on_a_null_value_and_on_a_long_step():
    result = _run_node(
        "var pts = [{period:2019,value:1},{period:2020,value:null},"
        "{period:2021,value:2},{period:2022,value:3}];\n"
        "var xId = function(t){ return t; };\n"
        "console.log(JSON.stringify(BPSimilar.peerRuns(pts, xId, 1)"
        ".map(function(r){ return r.map(function(p){ return p.period; }); })));"
    )
    assert result == [[2019], [2021, 2022]]


def test_peer_runs_axis_break_style_gap_also_splits():
    result = _run_node(
        "var pts = [{period:2000,value:1},{period:2017,value:2},{period:2018,value:3}];\n"
        "var xId = function(t){ return t; };\n"
        "console.log(JSON.stringify(BPSimilar.peerRuns(pts, xId, 1)"
        ".map(function(r){ return r.map(function(p){ return p.period; }); })));"
    )
    assert result == [[2000], [2017, 2018]]


def test_peer_runs_a_real_zero_stays_inside_its_run():
    """A value of 0 is a real measurement, never treated as a gap."""
    result = _run_node(
        "var pts = [{period:1,value:0},{period:2,value:1}];\n"
        "var xId = function(t){ return t; };\n"
        "console.log(JSON.stringify(BPSimilar.peerRuns(pts, xId, 1)"
        ".map(function(r){ return r.map(function(p){ return p.period; }); })));"
    )
    assert result == [[1, 2]]


def test_peer_runs_drops_points_xoftime_maps_to_null_and_splits_there():
    result = _run_node(
        "var pts = [{period:1,value:1},{period:2,value:2},{period:3,value:3},{period:4,value:4}];\n"
        "var xNullAt3 = function(t){ return t === 3 ? null : t; };\n"
        "console.log(JSON.stringify(BPSimilar.peerRuns(pts, xNullAt3, 1)"
        ".map(function(r){ return r.map(function(p){ return p.period; }); })));"
    )
    assert result == [[1, 2], [4]]


# ---------------------------------------------------------------------------
# nearestPeer
# ---------------------------------------------------------------------------


def test_nearest_peer_respects_tolerance_and_the_commune_point():
    result = _run_node(
        "var A = {nis:'A', y:100, rank:1};\n"
        "var B = {nis:'B', y:130, rank:2};\n"
        "console.log(JSON.stringify({"
        "tooFar: BPSimilar.nearestPeer([A,B], 112, 8, 999),"
        "hitsA: (BPSimilar.nearestPeer([A,B], 104, 8, 999) || {}).nis,"
        "communeCloser: BPSimilar.nearestPeer([A,B], 104, 8, 105)"
        "}));"
    )
    assert result["tooFar"] is None
    assert result["hitsA"] == "A"
    assert result["communeCloser"] is None


def test_nearest_peer_tie_breaks_to_the_lower_rank():
    result = _run_node(
        "var A = {nis:'A', y:100, rank:1};\n"
        "var B = {nis:'B', y:108, rank:2};\n"
        "console.log(JSON.stringify((BPSimilar.nearestPeer([A,B], 104, 8, 999) || {}).nis));"
    )
    assert result == "A"


# ---------------------------------------------------------------------------
# resolveList
# ---------------------------------------------------------------------------


def test_resolve_list_defaults_and_validates_the_stored_value():
    result = _run_node(
        "console.log(JSON.stringify({"
        "nullVal: BPSimilar.resolveList(null),"
        "national: BPSimilar.resolveList('national'),"
        "off: BPSimilar.resolveList('off'),"
        "junk: BPSimilar.resolveList('xyz'),"
        "nullSaveData: BPSimilar.resolveList(null, true),"
        "regionSaveData: BPSimilar.resolveList('region', true)"
        "}));"
    )
    assert result == {
        "nullVal": "region",
        "national": "national",
        "off": "off",
        "junk": "region",
        "nullSaveData": "off",
        "regionSaveData": "region",
    }


# ---------------------------------------------------------------------------
# isProvisional
# ---------------------------------------------------------------------------


def test_is_provisional_matches_a_hyphenated_prerelease_suffix():
    result = _run_node(
        "console.log(JSON.stringify({"
        "rc: BPSimilar.isProvisional('1.0.0-rc.1'),"
        "final: BPSimilar.isProvisional('1.0.0')"
        "}));"
    )
    assert result == {"rc": True, "final": False}
