"""Contracts for the FWB site-class snapshot."""

import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from export_schools_ise import SchoolExportError, normalize, resolve_commune_names  # noqa: E402


def row(**changes):
    record = {
        "type_d_enseignement": "Fondamental ordinaire",
        "acp_resultat_calcul_acp": "FO",
        "numero_fase_de_l_etablissement": 1.0,
        "nom_de_l_etablissement": "École du Centre",
        "numero_fase_de_l_implantation": 42.0,
        "adresse_voie": "Rue Haute",
        "numero_de_porte": "2",
        "code_postal": 5000.0,
        "numero_de_classe": "3a",
        "numero_de_classe_hed": 3.0,
        "annee_de_reference_de_la_donnee": 2025.0,
    }
    record.update(changes)
    return record


def register_row(**changes):
    record = {
        "ndeg_fase_de_l_implantation": 42.0,
        "commune_de_l_implantation": "Namur",
        "reseau": "WBE",
        "niveau": "Fondamental",
        "latitude": 50.46,
        "longitude": 4.86,
    }
    record.update(changes)
    return record


def _write_geographies(tmp_path: Path, rows: list[dict]) -> Path:
    path = tmp_path / "geographies.csv"
    fieldnames = [
        "geo_id",
        "nis_code",
        "level",
        "name_nl",
        "name_fr",
        "name_en",
        "parent_geo_id",
        "valid_from",
        "valid_to",
        "successor_geo_id",
        "nuts",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return path


def _write_crosswalk(tmp_path: Path, rows: list[dict]) -> Path:
    path = tmp_path / "municipality_crosswalk.csv"
    fieldnames = [
        "old_nis",
        "old_name_nl",
        "old_name_fr",
        "new_nis",
        "relationship",
        "has_partial_transfer",
        "valid_from",
        "valid_to",
        "evidence",
        "verified",
        "verified_source",
        "note",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return path


def _mun(nis, name_nl, name_fr, name_en=None, valid_from="1977-01-01", valid_to=""):
    return {
        "geo_id": f"be:mun:{nis}",
        "nis_code": nis,
        "level": "municipality",
        "name_nl": name_nl,
        "name_fr": name_fr,
        "name_en": name_en or name_fr,
        "parent_geo_id": "be:prov:00000",
        "valid_from": valid_from,
        "valid_to": valid_to,
        "successor_geo_id": "",
        "nuts": "",
    }


FIXTURE_MUNICIPALITIES = [
    _mun("92094", "Namen", "Namur"),
    _mun("62093", "Saint-Nicolas", "Saint-Nicolas"),
    _mun("46021", "Sint-Niklaas", "Saint-Nicolas (Sint-Niklaas)", name_en="Sint-Niklaas"),
    _mun("25017", "Braine-l'Alleud", "Braine-l'Alleud"),
    _mun("82039", "Bastenaken", "Bastogne"),
]

FIXTURE_CROSSWALK = [
    {
        "old_nis": "82005",
        "old_name_nl": "Bertogne",
        "old_name_fr": "Bertogne",
        "new_nis": "82039",
        "relationship": "merged",
        "has_partial_transfer": "true",
        "valid_from": "1977-01-01",
        "valid_to": "2024-12-02",
        "evidence": "test fixture",
        "verified": "true",
        "verified_source": "",
        "note": "",
    },
]


def test_preserves_qualified_class_and_does_not_invent_commune():
    result = normalize([row()])["records"][0]
    assert result["ed"] == "3a"
    assert result["postcode"] == 5000
    assert "nis" not in result


def test_rejects_duplicate_site_and_formula():
    with pytest.raises(ValueError, match="duplicate"):
        normalize([row(), row(nom_de_l_etablissement="Other")])


def test_rejects_schema_or_year_change():
    with pytest.raises(ValueError, match="schema changed"):
        normalize([row(extra="new column")])
    with pytest.raises(ValueError, match="reference year"):
        normalize([row(annee_de_reference_de_la_donnee=2026.0)])


# ── commune assignment ───────────────────────────────────────────────────────


def test_joins_register_and_assigns_nis(tmp_path):
    geographies = _write_geographies(tmp_path, FIXTURE_MUNICIPALITIES)
    crosswalk = _write_crosswalk(tmp_path, [])
    result = normalize([row()], [register_row()], geographies, crosswalk)
    record = result["records"][0]
    assert record["nis"] == "92094"
    assert record["commune"] == {"fr": "Namur", "nl": "Namen", "en": "Namur"}
    assert record["network"] == "WBE"
    assert record["lat"] == 50.46
    assert record["lon"] == 4.86
    assert result["unassigned_sites"] == 0
    assert result["register_year"] == 2026


def test_unmatched_site_keeps_nis_null_and_is_counted():
    geographies_rows = FIXTURE_MUNICIPALITIES
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        tmp_path = Path(td)
        geographies = _write_geographies(tmp_path, geographies_rows)
        crosswalk = _write_crosswalk(tmp_path, [])
        # 199 ISE rows, only 1 has a register row -- unmatched share 198/199 =
        # 99.5%, far over the 2% threshold, so this also exercises the refusal
        # path via a distinct assertion below; here we test the near-boundary
        # "still under 2%" case with a single unmatched site among many matched.
        many_rows = [
            row(numero_fase_de_l_implantation=float(i), numero_de_classe=None)
            for i in range(1, 100)
        ]
        many_rows[0] = row(numero_fase_de_l_implantation=1.0, numero_de_classe=None)
        # Give every site except site 1 a register row.
        registers = [register_row(ndeg_fase_de_l_implantation=float(i)) for i in range(2, 100)]
        result = normalize(many_rows, registers, geographies, crosswalk)
        unmatched = [r for r in result["records"] if r["nis"] is None]
        assert len(unmatched) == 1
        assert unmatched[0]["site"] == 1
        assert unmatched[0]["commune"] is None
        assert result["unassigned_sites"] == 1


def test_refuses_when_unmatched_share_exceeds_2_percent(tmp_path):
    geographies = _write_geographies(tmp_path, FIXTURE_MUNICIPALITIES)
    crosswalk = _write_crosswalk(tmp_path, [])
    # 100 ISE rows, only 1 register row -- 99% unmatched.
    many_rows = [row(numero_fase_de_l_implantation=float(i)) for i in range(1, 101)]
    registers = [register_row(ndeg_fase_de_l_implantation=1.0)]
    with pytest.raises(SchoolExportError, match="exceeds"):
        normalize(many_rows, registers, geographies, crosswalk)


def test_saint_nicolas_overrides_to_liege(tmp_path):
    geographies = _write_geographies(tmp_path, FIXTURE_MUNICIPALITIES)
    crosswalk = _write_crosswalk(tmp_path, [])
    resolved = resolve_commune_names({"Saint-Nicolas"}, geographies, crosswalk)
    assert resolved == {"Saint-Nicolas": "62093"}


def test_braine_l_alleud_with_curly_apostrophe_resolves(tmp_path):
    geographies = _write_geographies(tmp_path, FIXTURE_MUNICIPALITIES)
    crosswalk = _write_crosswalk(tmp_path, [])
    resolved = resolve_commune_names({"Braine-l’Alleud"}, geographies, crosswalk)
    assert resolved == {"Braine-l’Alleud": "25017"}


def test_bertogne_resolves_via_crosswalk_successor_to_bastogne(tmp_path):
    geographies = _write_geographies(tmp_path, FIXTURE_MUNICIPALITIES)
    crosswalk = _write_crosswalk(tmp_path, FIXTURE_CROSSWALK)
    resolved = resolve_commune_names({"Bertogne"}, geographies, crosswalk)
    assert resolved == {"Bertogne": "82039"}


def test_unresolved_commune_name_refuses_and_names_it(tmp_path):
    geographies = _write_geographies(tmp_path, FIXTURE_MUNICIPALITIES)
    crosswalk = _write_crosswalk(tmp_path, [])
    with pytest.raises(SchoolExportError, match="Nowhereville"):
        resolve_commune_names({"Nowhereville"}, geographies, crosswalk)


def test_two_runs_produce_identical_bytes(tmp_path):
    geographies = _write_geographies(tmp_path, FIXTURE_MUNICIPALITIES)
    crosswalk = _write_crosswalk(tmp_path, [])
    import json

    first = json.dumps(normalize([row()], [register_row()], geographies, crosswalk), sort_keys=True)
    second = json.dumps(
        normalize([row()], [register_row()], geographies, crosswalk), sort_keys=True
    )
    assert first == second
