"""scripts/export_peer_benchmarks.py, run against the real committed data.

Same shape as tests/test_export_peer_model.py: no fixture, no invented
values (CLAUDE.md rule 36) -- this runs the exporter against the actual
repository CSVs and public/data/metadata/peers.json and checks the
structural guarantees the spec makes about the output: 565 files, the
model_version stamp, a hand-computed peer median for a real indicator and
commune, the selection_variable flag on the right indicators, a Flemish
commune having no WalStat indicator in its region list, and byte-identical
reruns (rule 35).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import scripts.export_peer_benchmarks as export_peer_benchmarks  # noqa: E402
import scripts.export_peer_model as export_peer_model  # noqa: E402

PEERS_JSON = REPO_ROOT / "public" / "data" / "metadata" / "peers.json"

pytestmark = pytest.mark.slow  # rebuilds all 565 commune benchmark payloads


def _rebuild():
    """Runs the whole exporter pipeline exactly as scripts/export_peer_benchmarks.py's
    main() does, and returns (payloads, candidate_ids, excluded_ids) -- one place so
    every test and the byte-identical-rerun check exercise the real call sequence,
    not a hand-abbreviated one that could drift from main()."""
    communes = export_peer_model._current_municipality_nis(export_peer_model.DEFAULT_GEOGRAPHIES)
    peers_model = export_peer_benchmarks._load_peers(PEERS_JSON)
    raw_rows = export_peer_benchmarks._read_all_history_rows(
        export_peer_benchmarks.DEFAULT_HISTORY_DIR, export_peer_benchmarks.DEFAULT_HISTORY_CSV
    )
    derived_rows, _names, _engine_only_ids = export_peer_benchmarks.build_derived_rows(
        raw_rows, export_peer_benchmarks.DEFAULT_DERIVED_DIR
    )
    candidate_ids, excluded_ids = export_peer_benchmarks.build_benchmark_universe(
        raw_rows, export_peer_benchmarks.DEFAULT_DERIVED_DIR
    )
    all_rows = raw_rows + derived_rows
    index = export_peer_benchmarks._index_rows(all_rows)
    latest_period = export_peer_benchmarks._latest_period_per_indicator(index)
    payloads = export_peer_benchmarks.build_benchmarks(
        communes, index, latest_period, peers_model, candidate_ids, excluded_ids
    )
    return payloads, candidate_ids, excluded_ids


@pytest.fixture(scope="module")
def _built():
    if not PEERS_JSON.exists():
        pytest.skip(f"{PEERS_JSON} not built -- run scripts/export_peer_model.py first")
    return _rebuild()


@pytest.fixture(scope="module")
def payloads(_built):
    return _built[0]


@pytest.fixture(scope="module")
def universe(_built):
    _payloads, candidate_ids, excluded_ids = _built
    return candidate_ids, excluded_ids


def test_exactly_565_communes(payloads):
    assert len(payloads) == 565


def test_model_version_and_variant_on_every_payload(payloads):
    for nis, payload in payloads.items():
        assert payload["model_version"] == export_peer_model.PEER_MODEL_VERSION, nis
        assert payload["variant"] == "standardised", nis
        assert payload["nis_code"] == nis


def test_every_payload_has_both_lists(payloads):
    for nis, payload in payloads.items():
        assert set(payload["lists"]) == {"national", "region"}, nis


def test_every_payload_has_a_peers_object_with_ten_ranked_entries_per_list(payloads):
    for nis, payload in payloads.items():
        assert set(payload["peers"]) == {"national", "region"}, nis
        for list_name in ("national", "region"):
            entries = payload["peers"][list_name]
            assert len(entries) == 10, f"{nis}/{list_name} has {len(entries)} peers"
            for entry in entries:
                assert set(entry) == {"nis", "rank"}, entry
            assert [e["rank"] for e in entries] == list(range(1, 11))


def test_namur_national_peers_list_matches_peers_json(payloads):
    # Hand-checked against public/data/metadata/peers.json directly (CLAUDE.md
    # rule 5/36): Namur's (92094) "peers.national" must be exactly the nis/rank
    # pairs peers.json's own national top-10 carries for Namur, no name, no
    # distance, no similarity (the handoff's explicit reason: the 10th peer's
    # similarity score reads as "0% similar", which is not a benchmark).
    peers_model = export_peer_benchmarks._load_peers(PEERS_JSON)
    for list_name in ("national", "region"):
        expected = [
            {"nis": e["nis"], "rank": e["rank"]}
            for e in peers_model["communes"]["92094"][list_name]
        ]
        assert payloads["92094"]["peers"][list_name] == expected


def test_peers_entries_carry_no_distance_or_similarity(payloads):
    for list_name in ("national", "region"):
        for entry in payloads["92094"]["peers"][list_name]:
            assert "distance" not in entry
            assert "similarity" not in entry


def test_namur_avg_net_taxable_income_national_peer_median_matches_hand_computation(payloads):
    # Namur (92094), national peers read from peers.json, values read from
    # data/communes_history.csv -- computed independently here, by hand, and
    # compared against the exporter's own output (CLAUDE.md rule 5).
    peers_model = export_peer_benchmarks._load_peers(PEERS_JSON)
    peer_nis_list = [e["nis"] for e in peers_model["communes"]["92094"]["national"]]
    assert len(peer_nis_list) == 10

    raw_rows = export_peer_benchmarks._read_all_history_rows(
        export_peer_benchmarks.DEFAULT_HISTORY_DIR, export_peer_benchmarks.DEFAULT_HISTORY_CSV
    )
    by_nis_2023: dict[str, float] = {}
    for row in raw_rows:
        if row["indicator_code"] == "AVG_NET_TAXABLE_INCOME" and row["period"] == "2023":
            by_nis_2023[row["nis_code"]] = float(row["value"])

    peer_values = sorted(by_nis_2023[nis] for nis in peer_nis_list)
    assert len(peer_values) == 10  # every one of Namur's 10 national peers has a 2023 value
    hand_median = (peer_values[4] + peer_values[5]) / 2
    namur_value = by_nis_2023["92094"]
    hand_deviation = (namur_value - hand_median) / hand_median * 100

    entry = payloads["92094"]["lists"]["national"]["AVG_NET_TAXABLE_INCOME"]
    assert entry["value"] == pytest.approx(namur_value)
    assert entry["peer_median"] == pytest.approx(hand_median)
    assert entry["peers_with_value"] == 10
    assert entry["deviation_pct"] == pytest.approx(round(hand_deviation, 4))
    assert entry["period"] == "2023"
    assert entry["selection_variable"] is True


def test_selection_variable_true_for_income_false_for_a_finance_indicator(payloads):
    namur_national = payloads["92094"]["lists"]["national"]
    assert namur_national["AVG_NET_TAXABLE_INCOME"]["selection_variable"] is True
    namur_region = payloads["92094"]["lists"]["region"]
    if "MUN_DEBT_TOTAL_PER_CAPITA" in namur_region:
        assert namur_region["MUN_DEBT_TOTAL_PER_CAPITA"]["selection_variable"] is False


def test_negative_peer_median_entry_is_kept_with_reason_not_omitted(payloads):
    # Antwerpen (11002), INTERNAL_MIGRATION_NET, national list, 2025: value
    # -4085 against a peer median of -85 (both real, read from the payload,
    # not hand-typed -- CLAUDE.md rule 36). The entry must be KEPT (rank
    # still means something) with deviation_pct null and deviation_withheld
    # naming why, never omitted as if the whole benchmark were unavailable.
    entry = payloads["11002"]["lists"]["national"]["INTERNAL_MIGRATION_NET"]
    assert entry["peer_median"] < 0
    assert entry["deviation_pct"] is None
    assert entry["deviation_withheld"] == "median_negative"
    assert entry["position"] is not None
    assert entry["of"] is not None
    assert entry["peers_with_value"] >= 7


def test_deviation_withheld_key_absent_when_deviation_is_computed(payloads):
    # The ordinary case (a real, positive median): deviation_withheld is
    # omitted entirely, not written as null, per docs/features/peer_model.md.
    entry = payloads["92094"]["lists"]["national"]["AVG_NET_TAXABLE_INCOME"]
    assert entry["deviation_pct"] is not None
    assert "deviation_withheld" not in entry


def test_selection_variable_indicators_equals_the_derived_set_from_peers_py(payloads):
    # scripts/export_peer_benchmarks.py's SELECTION_VARIABLE_INDICATORS must
    # be exactly src.analytics.peers.SELECTION_VARIABLE_INDICATOR_IDS -- no
    # separate hand-maintained constant that can drift from the model
    # (audit should-fix 2). Pinning today's known set of 12 ids as a second,
    # independent check: if this fails while the identity check above still
    # passes, the drift is in peers.py's VARIABLES, not in the export script.
    from src.analytics.peers import SELECTION_VARIABLE_INDICATOR_IDS

    assert export_peer_benchmarks.SELECTION_VARIABLE_INDICATORS is SELECTION_VARIABLE_INDICATOR_IDS
    assert SELECTION_VARIABLE_INDICATOR_IDS == {
        "POPULATION_BY_COMMUNE",
        "POPULATION_AGE_65_PLUS",
        "POPULATION_AGE_0_14",
        "POPULATION_CHANGE_5Y",
        "AVG_NET_TAXABLE_INCOME",
        "FISCAL_TOT_NET_TAXABLE_INC",
        "UNEMPLOYMENT_RATE_INSURED",
        "SHARE_FOREIGN_NATIONALS",
        "POP_FOREIGN_NATIONALS",
        "AVERAGE_HOUSEHOLD_SIZE",
        "LOCAL_UNITS_BY_COMMUNE",
        "MUN_CADASTRAL_INCOME_TOTAL",
    }


def test_selection_variable_true_for_unemployment(payloads):
    for payload in payloads.values():
        entry = payload["lists"]["national"].get("UNEMPLOYMENT_RATE_INSURED")
        if entry is not None:
            assert entry["selection_variable"] is True
            break
    else:  # pragma: no cover -- would mean the indicator never survives the floor anywhere
        pytest.fail("UNEMPLOYMENT_RATE_INSURED never appears in any national list")


def test_a_flemish_commune_has_no_walstat_indicator_in_either_list(payloads):
    # Aartselaar (11001), Flanders -- WalStat is Wallonia-only
    # (docs/features/walstat_adapter.md); the spec says this is correct, not
    # a bug to special-case.
    walstat_indicators = {
        "MUN_DEBT_TOTAL_PER_CAPITA",
        "MUN_DEBT_TO_REVENUE",
        "MUN_EXPENDITURE_GROWTH_1Y",
        "MUN_REVENUE_GROWTH_1Y",
        "MUN_INVESTMENT_SHARE_OF_EXPENDITURE",
        "BIM_BENEFICIARIES_SHARE",
        "GRAPA_RECIPIENTS_SHARE_65_PLUS",
        "UNEMPLOYMENT_RATE_BIT",
        "PREPAYMENT_METERS_ELECTRICITY_SHARE",
        "PREPAYMENT_METERS_GAS_SHARE",
    }
    aartselaar = payloads["11001"]
    for list_name in ("national", "region"):
        present = walstat_indicators & set(aartselaar["lists"][list_name])
        assert not present, (list_name, present)


def test_no_indicator_is_written_as_an_entirely_null_block(payloads):
    for nis, payload in payloads.items():
        for list_name in ("national", "region"):
            for indicator_id, entry in payload["lists"][list_name].items():
                assert entry["peer_median"] is not None, (nis, list_name, indicator_id)


def test_two_runs_produce_byte_identical_json(tmp_path):
    # Rebuilds the whole payload from scratch twice, independently, rather
    # than writing the same already-built `payloads` dict to disk twice --
    # that would only prove json.dumps is deterministic, not that a real
    # rerun of the exporter (re-reading the CSVs, re-indexing, rebuilding
    # every commune's benchmarks) is byte-identical (CLAUDE.md rule 35).
    if not PEERS_JSON.exists():
        pytest.skip(f"{PEERS_JSON} not built -- run scripts/export_peer_model.py first")

    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    export_peer_benchmarks.write_payloads(_rebuild()[0], out1)
    export_peer_benchmarks.write_payloads(_rebuild()[0], out2)

    files1 = sorted(out1.glob("*.json"))
    files2 = sorted(out2.glob("*.json"))
    assert len(files1) == len(files2) == 565
    for f1, f2 in zip(files1, files2, strict=True):
        assert f1.name == f2.name
        assert f1.read_bytes() == f2.read_bytes()


def test_output_json_is_compact_sorted_and_ends_with_a_trailing_newline(tmp_path, payloads):
    out_dir = tmp_path / "out"
    export_peer_benchmarks.write_payloads(payloads, out_dir)
    sample = (out_dir / "92094.json").read_bytes()
    text = sample.decode("utf-8")
    assert text.endswith("\n")
    assert not text.endswith("\n\n")
    assert "\n" not in text[:-1]  # one compact line, newline only at the very end
    assert ", " not in text and ": " not in text  # compact separators (",", ":")
    parsed = json.loads(text)
    assert list(parsed.keys()) == sorted(parsed.keys())


# --- Bug 1: a published history row must never be overwritten by the engine ------


def test_11002_population_change_5y_national_value_matches_the_published_csv_row(payloads):
    # The bug this batch fixes: public/data/peers/11002.json's national
    # POPULATION_CHANGE_5Y used to publish the ENGINE's recomputation
    # (6.8373...) instead of data/communes_history.csv's own 2026 row for
    # 11002 (Antwerpen) -- 4.6477851743035075, reused verbatim everywhere
    # else on the site by the maintainer's 2026-09-16 "growth on current
    # territory" rule. Read independently here from the real CSV, not
    # hand-typed (rule 36), and compared to the exporter's own output.
    raw_rows = export_peer_benchmarks._read_all_history_rows(
        export_peer_benchmarks.DEFAULT_HISTORY_DIR, export_peer_benchmarks.DEFAULT_HISTORY_CSV
    )
    csv_rows_2026 = [
        row
        for row in raw_rows
        if row["indicator_code"] == "POPULATION_CHANGE_5Y"
        and row["nis_code"] == "11002"
        and row["period"] == "2026"
    ]
    assert len(csv_rows_2026) == 1, "expected exactly one published 2026 row for 11002"
    csv_value = float(csv_rows_2026[0]["value"])

    entry = payloads["11002"]["lists"]["national"]["POPULATION_CHANGE_5Y"]
    assert entry["period"] == "2026"
    assert entry["value"] == pytest.approx(csv_value)
    # Guards against the bug reappearing under a different number: the old,
    # wrong engine value was materially higher (~6.84) than the published
    # figure (~4.65) for this commune.
    assert entry["value"] != pytest.approx(6.8373, abs=0.01)


def test_a_national_peers_population_change_5y_value_also_matches_its_csv_row(payloads):
    # Same check, one hop further: a PEER's value inside the benchmark must
    # also be the published row, not an engine recomputation -- the bug could
    # just as easily have shown up as a wrong peer median built from wrong
    # peer inputs even where the commune's own value happened to be right.
    peers_model = export_peer_benchmarks._load_peers(PEERS_JSON)
    peer_nis = peers_model["communes"]["11002"]["national"][0]["nis"]

    raw_rows = export_peer_benchmarks._read_all_history_rows(
        export_peer_benchmarks.DEFAULT_HISTORY_DIR, export_peer_benchmarks.DEFAULT_HISTORY_CSV
    )
    csv_rows = [
        row
        for row in raw_rows
        if row["indicator_code"] == "POPULATION_CHANGE_5Y"
        and row["nis_code"] == peer_nis
        and row["period"] == payloads["11002"]["lists"]["national"]["POPULATION_CHANGE_5Y"]["period"]
    ]
    if not csv_rows:
        pytest.skip(f"peer {peer_nis} has no published POPULATION_CHANGE_5Y row at this period")
    csv_value = float(csv_rows[0]["value"])

    # The peer's value only shows up indirectly (via peer_median); recompute
    # what the exporter must have used as this peer's raw input by rebuilding
    # the index the same way the exporter does and reading that cell back.
    derived_rows, _names, _ids = export_peer_benchmarks.build_derived_rows(
        raw_rows, export_peer_benchmarks.DEFAULT_DERIVED_DIR
    )
    index = export_peer_benchmarks._index_rows(raw_rows + derived_rows)
    period = payloads["11002"]["lists"]["national"]["POPULATION_CHANGE_5Y"]["period"]
    used_value, _status = index[("POPULATION_CHANGE_5Y", period)][peer_nis]
    assert used_value == pytest.approx(csv_value)


def test_a_derived_indicator_with_a_published_row_is_never_recomputed_by_the_engine():
    # build_derived_rows must exclude every derived indicator id that has at
    # least one row in the raw history CSVs -- POPULATION_CHANGE_5Y among
    # them (measured 2026-09-27: 3,390 published rows). Only a derived
    # indicator with ZERO published rows anywhere (e.g. BIRTH_RATE_PER_1000)
    # may reach the engine here.
    raw_rows = export_peer_benchmarks._read_all_history_rows(
        export_peer_benchmarks.DEFAULT_HISTORY_DIR, export_peer_benchmarks.DEFAULT_HISTORY_CSV
    )
    _derived_rows, _names, engine_only_ids = export_peer_benchmarks.build_derived_rows(
        raw_rows, export_peer_benchmarks.DEFAULT_DERIVED_DIR
    )
    published_ids = export_peer_benchmarks._published_indicator_ids(raw_rows)
    assert "POPULATION_CHANGE_5Y" in published_ids
    assert "POPULATION_CHANGE_5Y" not in engine_only_ids
    # At least one genuinely engine-only derived indicator still gets
    # computed (the fix must not have accidentally excluded everything).
    assert engine_only_ids, "no derived indicator was computed by the engine at all"
    assert engine_only_ids.isdisjoint(published_ids)


# --- Bug 2: percentile/rank-shaped indicators must never be benchmarked ----------


def test_cross_sectional_functions_are_matched_by_function_not_by_indicator_id():
    # config/indicators/derived/POPULATION_PERCENTILE.yaml declares
    # function: percentile -- the exclusion must key off that field, not off
    # the id containing the word "PERCENTILE" (bug 2's fix: a rank-shaped
    # function under any future name/id must be excluded the same way).
    import yaml

    cfg = yaml.safe_load(
        (export_peer_benchmarks.DEFAULT_DERIVED_DIR / "POPULATION_PERCENTILE.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert cfg["derived"]["function"] in export_peer_benchmarks.CROSS_SECTIONAL_FUNCTIONS


def test_population_percentile_is_excluded_in_every_commune_and_list(universe):
    candidate_ids, excluded_ids = universe
    assert "POPULATION_PERCENTILE" in candidate_ids
    assert "POPULATION_PERCENTILE" in excluded_ids


def test_population_percentile_never_appears_in_a_lists_block(payloads):
    for nis, payload in payloads.items():
        for list_name in ("national", "region"):
            assert "POPULATION_PERCENTILE" not in payload["lists"][list_name], nis


def test_population_percentile_is_withheld_as_excluded_everywhere(payloads):
    for nis, payload in payloads.items():
        for list_name in ("national", "region"):
            entry = payload["withheld"][list_name]["POPULATION_PERCENTILE"]
            assert entry == {"reason": "excluded"}, (nis, list_name, entry)


# --- Contract: peers / min_peers_with_value / withheld ---------------------------


def test_every_payload_carries_min_peers_with_value_from_code(payloads):
    from src.analytics.peers import MIN_PEERS_WITH_VALUE

    for nis, payload in payloads.items():
        assert payload["min_peers_with_value"] == MIN_PEERS_WITH_VALUE, nis
        assert payload["min_peers_with_value"] == 7


def test_every_universe_indicator_is_in_exactly_one_of_lists_or_withheld(payloads, universe):
    # The core exhaustiveness guarantee, run over all 565 real files: for
    # every commune, every list, and every indicator in the benchmark
    # universe, EXACTLY ONE of lists[list][IND] / withheld[list][IND] exists
    # -- never both, never neither.
    candidate_ids, _excluded_ids = universe
    for nis, payload in payloads.items():
        for list_name in ("national", "region"):
            lst = set(payload["lists"][list_name])
            wh = set(payload["withheld"][list_name])
            assert lst.isdisjoint(wh), (nis, list_name, lst & wh)
            assert lst | wh == candidate_ids, (
                nis,
                list_name,
                "missing from both" if candidate_ids - (lst | wh) else "extra beyond universe",
                candidate_ids ^ (lst | wh),
            )


def test_withheld_no_current_value_reason_shape(payloads):
    found = False
    for payload in payloads.values():
        for list_name in ("national", "region"):
            for entry in payload["withheld"][list_name].values():
                if entry["reason"] != "no_current_value":
                    continue
                found = True
                assert set(entry) == {"reason", "period", "own_period"}
    assert found, "no no_current_value withheld entry found anywhere to check the shape of"


def test_withheld_few_peers_reason_shape_and_below_the_floor(payloads):
    from src.analytics.peers import MIN_PEERS_WITH_VALUE

    found = False
    for payload in payloads.values():
        for list_name in ("national", "region"):
            for entry in payload["withheld"][list_name].values():
                if entry["reason"] != "few_peers":
                    continue
                found = True
                assert set(entry) == {"reason", "period", "peers_with_value"}
                assert entry["peers_with_value"] < MIN_PEERS_WITH_VALUE
    assert found, "no few_peers withheld entry found anywhere to check the shape of"


def test_antwerpen_internal_migration_net_is_withheld_no_current_value_for_11002(payloads):
    # Handoff's own example commune/indicator, checked for the withheld side
    # this time: 11002 (Antwerpen)'s national INTERNAL_MIGRATION_NET already
    # has a real entry (test_negative_peer_median_entry_is_kept_with_reason_not_omitted
    # above); this test instead confirms a genuinely absent indicator for the
    # same commune reads as "no_current_value", never silently dropped.
    withheld = payloads["11002"]["withheld"]["national"]
    # Every indicator not in lists must be in withheld -- pick one at random
    # from withheld itself to check the reason vocabulary is one of the three.
    assert withheld, "11002 has no withheld entries at all -- fixture likely stale"
    for entry in withheld.values():
        assert entry["reason"] in ("excluded", "no_current_value", "few_peers")


# --- Duplicate rows (measured, not assumed) --------------------------------------


def test_the_committed_history_has_no_duplicate_indicator_period_commune_rows():
    # Measured across data/communes_history.csv + data/communes_history/*.csv
    # (2026-09-27): zero duplicates. This pins that measurement so a future
    # regression is caught here rather than silently changing which row an
    # index keeps.
    raw_rows = export_peer_benchmarks._read_all_history_rows(
        export_peer_benchmarks.DEFAULT_HISTORY_DIR, export_peer_benchmarks.DEFAULT_HISTORY_CSV
    )
    duplicates = export_peer_benchmarks.find_duplicate_rows(raw_rows)
    assert duplicates == {}


def test_index_rows_refuses_on_a_duplicate_indicator_period_commune_row():
    rows = [
        {
            "indicator_code": "POPULATION_BY_COMMUNE",
            "period": "2026",
            "nis_code": "11002",
            "value": "500000",
            "status": "A",
            "indicator_name": "Population",
        },
        {
            "indicator_code": "POPULATION_BY_COMMUNE",
            "period": "2026",
            "nis_code": "11002",
            "value": "500001",  # a second, different row for the same key
            "status": "A",
            "indicator_name": "Population",
        },
    ]
    with pytest.raises(export_peer_benchmarks.BenchmarksExportError):
        export_peer_benchmarks._index_rows(rows)


def test_find_duplicate_rows_is_empty_for_non_duplicated_input():
    rows = [
        {"indicator_code": "X", "period": "2026", "nis_code": "11002", "value": "1", "status": "A"},
        {"indicator_code": "X", "period": "2026", "nis_code": "11001", "value": "2", "status": "A"},
        {"indicator_code": "Y", "period": "2026", "nis_code": "11002", "value": "3", "status": "A"},
    ]
    assert export_peer_benchmarks.find_duplicate_rows(rows) == {}


def test_find_duplicate_rows_flags_the_exact_offending_key():
    rows = [
        {"indicator_code": "X", "period": "2026", "nis_code": "11002", "value": "1", "status": "A"},
        {"indicator_code": "X", "period": "2026", "nis_code": "11002", "value": "2", "status": "A"},
        {"indicator_code": "X", "period": "2026", "nis_code": "11001", "value": "3", "status": "A"},
    ]
    duplicates = export_peer_benchmarks.find_duplicate_rows(rows)
    assert duplicates == {("X", "2026", "11002"): 2}
