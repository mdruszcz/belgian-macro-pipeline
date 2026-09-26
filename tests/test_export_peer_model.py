"""scripts/export_peer_model.py, run against the real committed data.

These tests run the exporter against the actual repository files (no
fixture, no invented values -- CLAUDE.md rule 36) and check the structural
guarantees the spec makes about the output: exactly 565 communes, exactly 10
peers per list, self-exclusion, region-pool restriction, the version stamp
everywhere it must appear, and byte-identical reruns (rule 35).
"""

from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import scripts.export_peer_model as export_peer_model  # noqa: E402
import src.analytics.peers as peers_module  # noqa: E402

AREAS_CSV = REPO_ROOT / "config" / "geography" / "commune_area_km2.csv"

pytestmark = pytest.mark.slow  # rebuilds the real 565-commune model from committed CSVs


@pytest.fixture(scope="module")
def model():
    if not AREAS_CSV.exists():
        pytest.skip(f"{AREAS_CSV} not built -- run scripts/derive_commune_area.py first")
    communes = export_peer_model._current_municipality_nis(export_peer_model.DEFAULT_GEOGRAPHIES)
    rows = export_peer_model._read_all_history_rows(
        export_peer_model.DEFAULT_HISTORY_DIR, export_peer_model.DEFAULT_HISTORY_CSV
    )
    meta = export_peer_model._commune_metadata(rows, communes)
    values = export_peer_model.build_raw_values(
        export_peer_model.DEFAULT_HISTORY_DIR,
        export_peer_model.DEFAULT_HISTORY_CSV,
        export_peer_model.DEFAULT_AREAS,
        communes,
    )
    return export_peer_model.build_model(values, meta, variant="standardised"), meta


def test_exactly_565_communes(model):
    payload, _meta = model
    assert len(payload["communes"]) == 565


def test_every_commune_has_exactly_10_national_and_10_region_peers(model):
    payload, _meta = model
    for nis, commune in payload["communes"].items():
        assert len(commune["national"]) == 10, nis
        assert len(commune["region"]) == 10, nis


def test_a_commune_is_never_its_own_peer(model):
    payload, _meta = model
    for nis, commune in payload["communes"].items():
        assert nis not in {e["nis"] for e in commune["national"]}
        assert nis not in {e["nis"] for e in commune["region"]}


def test_region_peers_are_all_in_the_same_region(model):
    payload, meta = model
    for nis, commune in payload["communes"].items():
        own_region = meta[nis]["region"]
        for entry in commune["region"]:
            assert meta[entry["nis"]]["region"] == own_region, (nis, entry["nis"])


def test_brussels_communes_region_peers_all_brussels(model):
    payload, meta = model
    brussels = [nis for nis, m in meta.items() if m["region"] == "BXL"]
    assert len(brussels) == 19
    for nis in brussels:
        for entry in payload["communes"][nis]["region"]:
            assert meta[entry["nis"]]["region"] == "BXL"


def test_model_version_on_the_payload(model):
    payload, _meta = model
    assert payload["model_version"] == peers_module.PEER_MODEL_VERSION
    assert payload["variant"] == "standardised"


def test_ranks_are_1_through_10_in_order(model):
    payload, _meta = model
    for _nis, commune in payload["communes"].items():
        for list_name in ("national", "region"):
            ranks = [e["rank"] for e in commune[list_name]]
            assert ranks == list(range(1, 11))


def test_two_runs_produce_byte_identical_json(tmp_path):
    if not AREAS_CSV.exists():
        pytest.skip(f"{AREAS_CSV} not built -- run scripts/derive_commune_area.py first")
    out1 = tmp_path / "peers1.json"
    out2 = tmp_path / "peers2.json"
    csv1 = tmp_path / "peers1.csv"
    csv2 = tmp_path / "peers2.csv"

    communes = export_peer_model._current_municipality_nis(export_peer_model.DEFAULT_GEOGRAPHIES)
    rows = export_peer_model._read_all_history_rows(
        export_peer_model.DEFAULT_HISTORY_DIR, export_peer_model.DEFAULT_HISTORY_CSV
    )
    meta = export_peer_model._commune_metadata(rows, communes)
    values = export_peer_model.build_raw_values(
        export_peer_model.DEFAULT_HISTORY_DIR,
        export_peer_model.DEFAULT_HISTORY_CSV,
        export_peer_model.DEFAULT_AREAS,
        communes,
    )
    model1 = export_peer_model.build_model(values, meta, variant="standardised")
    export_peer_model.write_json(model1, out1)
    export_peer_model.write_csv(model1, meta, csv1)

    model2 = export_peer_model.build_model(values, meta, variant="standardised")
    export_peer_model.write_json(model2, out2)
    export_peer_model.write_csv(model2, meta, csv2)

    assert out1.read_bytes() == out2.read_bytes()
    assert csv1.read_bytes() == csv2.read_bytes()


def test_csv_rows_carry_the_model_version(tmp_path):
    if not AREAS_CSV.exists():
        pytest.skip(f"{AREAS_CSV} not built -- run scripts/derive_commune_area.py first")
    out_csv = tmp_path / "peers.csv"
    communes = export_peer_model._current_municipality_nis(export_peer_model.DEFAULT_GEOGRAPHIES)
    rows = export_peer_model._read_all_history_rows(
        export_peer_model.DEFAULT_HISTORY_DIR, export_peer_model.DEFAULT_HISTORY_CSV
    )
    meta = export_peer_model._commune_metadata(rows, communes)
    values = export_peer_model.build_raw_values(
        export_peer_model.DEFAULT_HISTORY_DIR,
        export_peer_model.DEFAULT_HISTORY_CSV,
        export_peer_model.DEFAULT_AREAS,
        communes,
    )
    model = export_peer_model.build_model(values, meta, variant="standardised")
    export_peer_model.write_csv(model, meta, out_csv)

    with out_csv.open(encoding="utf-8", newline="") as fh:
        csv_rows = list(csv.DictReader(fh))
    assert len(csv_rows) == 565 * 10 * 2  # 565 communes x 10 peers x 2 lists
    assert all(r["model_version"] == peers_module.PEER_MODEL_VERSION for r in csv_rows)


def test_json_is_sorted_keys_compact_no_trailing_whitespace_issues(tmp_path):
    if not AREAS_CSV.exists():
        pytest.skip(f"{AREAS_CSV} not built -- run scripts/derive_commune_area.py first")
    out = tmp_path / "peers.json"
    communes = export_peer_model._current_municipality_nis(export_peer_model.DEFAULT_GEOGRAPHIES)
    rows = export_peer_model._read_all_history_rows(
        export_peer_model.DEFAULT_HISTORY_DIR, export_peer_model.DEFAULT_HISTORY_CSV
    )
    meta = export_peer_model._commune_metadata(rows, communes)
    values = export_peer_model.build_raw_values(
        export_peer_model.DEFAULT_HISTORY_DIR,
        export_peer_model.DEFAULT_HISTORY_CSV,
        export_peer_model.DEFAULT_AREAS,
        communes,
    )
    model = export_peer_model.build_model(values, meta, variant="standardised")
    export_peer_model.write_json(model, out)
    text = out.read_text(encoding="utf-8")
    assert text.endswith("\n")
    assert "\r" not in text
    parsed = json.loads(text)
    assert parsed == model


def test_pca_variant_is_a_different_variant_field_and_structure(model):
    """--pca writes a SEPARATE payload with variant "pca"; the default job
    (no --pca) never builds it. This test builds both in-process (never
    touching public/data/metadata/peers_pca.json, so a `pytest` run leaves
    no on-demand file behind) and checks the variant field and that the
    two payloads' national top-10 lists are not required to be identical
    (they may legitimately differ -- PCA reduces dimensionality first)."""
    payload, meta = model
    communes = export_peer_model._current_municipality_nis(export_peer_model.DEFAULT_GEOGRAPHIES)
    values = export_peer_model.build_raw_values(
        export_peer_model.DEFAULT_HISTORY_DIR,
        export_peer_model.DEFAULT_HISTORY_CSV,
        export_peer_model.DEFAULT_AREAS,
        communes,
    )
    pca_payload = export_peer_model.build_model(values, meta, variant="pca")
    assert pca_payload["variant"] == "pca"
    assert payload["variant"] == "standardised"
    assert len(pca_payload["communes"]) == 565
    for nis, commune in pca_payload["communes"].items():
        assert len(commune["national"]) == 10, nis
        assert len(commune["region"]) == 10, nis
        assert nis not in {e["nis"] for e in commune["national"]}


def test_every_similarity_in_both_lists_is_within_0_100(model):
    """Audit finding S2 (2026-09-26): similarity must be computed per list
    (national d_max for the national list, region d_max for the region
    list) -- reusing the national d_max for the region list let a region
    peer farther than the farthest national peer score below 0. This test
    would have caught it: 461/565 region lists had a negative entry under
    the bug."""
    payload, _meta = model
    for nis, commune in payload["communes"].items():
        for list_name in ("national", "region"):
            for entry in commune[list_name]:
                assert 0.0 <= entry["similarity"] <= 100.0, (nis, list_name, entry)


def _read_value(path: Path, indicator: str, period: str, nis: str) -> float:
    """One value read directly from a committed history CSV -- used only by
    the data-binding test below, never by the exporter itself (which reads
    the whole file once via _index_by_indicator_period; this helper is
    intentionally a second, independent, much slower path so the two do not
    share a bug)."""
    with path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if (
                row["indicator_code"] == indicator
                and row["period"] == period
                and row["nis_code"] == nis
            ):
                return float(row["value"])
    raise AssertionError(f"no row for {indicator} {period} {nis} in {path}")


def test_namur_every_variable_recomputed_from_committed_csvs_matches_features(model):
    """Audit finding B1's missing test: for Namur (92094), recompute EVERY
    one of the eleven raw feature values directly from the committed CSV
    rows at the spec's own literal periods (docs/features/peer_model.md
    "The variable list") -- independently of build_raw_values -- and assert
    each equals payload["communes"]["92094"]["features"][var_id]. This is
    the binding test that would have caught B1 (enterprise_density using
    population 2026 instead of the spec's 2023): every variable gets its
    own independent recomputation, not just the one that was wrong.

    features stores the value AFTER any log transform (population,
    population_density, enterprise_density, property_tax_base_per_resident
    are all logged per the spec's variable table) -- this test applies
    exactly that same transform to its own independently-read raw value
    before comparing, mirroring build_feature_matrix's own log step rather
    than re-deriving a different formula.
    """
    payload, _meta = model
    nis = "92094"
    features = payload["communes"][nis]["features"]

    history_dir = export_peer_model.DEFAULT_HISTORY_DIR
    history_csv = export_peer_model.DEFAULT_HISTORY_CSV
    population_path = history_dir / "population.csv"
    onem_rates_path = history_dir / "onem_rates.csv"
    patrimony_path = history_dir / "spf_agdp_patrimony.csv"

    pop_2026 = _read_value(population_path, "POPULATION_BY_COMMUNE", "2026", nis)
    age_65_2026 = _read_value(population_path, "POPULATION_AGE_65_PLUS", "2026", nis)
    age_0_14_2026 = _read_value(population_path, "POPULATION_AGE_0_14", "2026", nis)
    unemployment_2026 = _read_value(onem_rates_path, "UNEMPLOYMENT_RATE_INSURED", "2026", nis)
    cadastral_2026 = _read_value(patrimony_path, "MUN_CADASTRAL_INCOME_TOTAL", "2026", nis)
    pop_change_5y = _read_value(history_csv, "POPULATION_CHANGE_5Y", "2026", nis)
    avg_income_2023 = _read_value(history_csv, "AVG_NET_TAXABLE_INCOME", "2023", nis)
    share_foreign_2021 = _read_value(history_csv, "SHARE_FOREIGN_NATIONALS", "2021", nis)
    household_size_2021 = _read_value(history_csv, "AVERAGE_HOUSEHOLD_SIZE", "2021", nis)
    local_units_2023q4 = _read_value(history_csv, "LOCAL_UNITS_BY_COMMUNE", "2023-Q4", nis)
    # Spec variable 10's own denominator: POPULATION_BY_COMMUNE at 2023, the
    # same year as the 2023-Q4 enterprise snapshot -- NOT 2026 (B1).
    pop_2023 = _read_value(population_path, "POPULATION_BY_COMMUNE", "2023", nis)

    area_km2 = None
    with export_peer_model.DEFAULT_AREAS.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["nis"] == nis:
                area_km2 = float(row["area_km2"])
                break
    assert area_km2 is not None

    expected = {
        "population": math.log(pop_2026),
        "population_density": math.log(pop_2026 / area_km2),
        "share_65_plus": (age_65_2026 / pop_2026) * 100.0,
        "share_0_14": (age_0_14_2026 / pop_2026) * 100.0,
        "population_change_5y": pop_change_5y,
        "avg_net_taxable_income": avg_income_2023,
        "unemployment_rate_insured": unemployment_2026,
        "share_foreign_nationals": share_foreign_2021,
        "average_household_size": household_size_2021,
        "enterprise_density": math.log((local_units_2023q4 / pop_2023) * 1000.0),
        "property_tax_base_per_resident": math.log(cadastral_2026 / pop_2026),
    }

    assert set(expected) == set(features)
    for var_id, expected_value in expected.items():
        assert features[var_id] == pytest.approx(expected_value, rel=1e-9), var_id
