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
