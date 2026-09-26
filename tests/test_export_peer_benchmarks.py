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


@pytest.fixture(scope="module")
def payloads():
    if not PEERS_JSON.exists():
        pytest.skip(f"{PEERS_JSON} not built -- run scripts/export_peer_model.py first")
    communes = export_peer_model._current_municipality_nis(export_peer_model.DEFAULT_GEOGRAPHIES)
    peers_model = export_peer_benchmarks._load_peers(PEERS_JSON)
    raw_rows = export_peer_benchmarks._read_all_history_rows(
        export_peer_benchmarks.DEFAULT_HISTORY_DIR, export_peer_benchmarks.DEFAULT_HISTORY_CSV
    )
    derived_rows, _names = export_peer_benchmarks.build_derived_rows(
        raw_rows, export_peer_benchmarks.DEFAULT_DERIVED_DIR
    )
    all_rows = raw_rows + derived_rows
    index = export_peer_benchmarks._index_rows(all_rows)
    latest_period = export_peer_benchmarks._latest_period_per_indicator(index)
    return export_peer_benchmarks.build_benchmarks(communes, index, latest_period, peers_model)


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

    def rebuild():
        communes = export_peer_model._current_municipality_nis(
            export_peer_model.DEFAULT_GEOGRAPHIES
        )
        peers_model = export_peer_benchmarks._load_peers(PEERS_JSON)
        raw_rows = export_peer_benchmarks._read_all_history_rows(
            export_peer_benchmarks.DEFAULT_HISTORY_DIR, export_peer_benchmarks.DEFAULT_HISTORY_CSV
        )
        derived_rows, _names = export_peer_benchmarks.build_derived_rows(
            raw_rows, export_peer_benchmarks.DEFAULT_DERIVED_DIR
        )
        all_rows = raw_rows + derived_rows
        index = export_peer_benchmarks._index_rows(all_rows)
        latest_period = export_peer_benchmarks._latest_period_per_indicator(index)
        return export_peer_benchmarks.build_benchmarks(communes, index, latest_period, peers_model)

    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    export_peer_benchmarks.write_payloads(rebuild(), out1)
    export_peer_benchmarks.write_payloads(rebuild(), out2)

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
