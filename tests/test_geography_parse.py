import csv

import openpyxl
import pytest

from src.geography.refnis import (
    build_hierarchy,
    geo_id_for,
    parse_nis9_xlsx,
    parse_refnis_csv,
)

# Statbel's own column names. The fixture mirrors the real workbook's layout so
# that a rename upstream fails the parser test rather than silently shifting
# which column is read.
NIS9_HEADER = [
    "CS01012026",
    "T_SEC_NL",
    "C_NIS6",
    "T_NIS6_NL",
    "CNIS5_2026",
    "T_MUN_NL",
    "T_MUN_FR",
    "T_MUN_DE",
    "CNIS_ARRD_2026",
    "T_ARRD_NL",
    "T_ARRD_FR",
    "T_ARRD_DE",
    "CNIS_PROVI_2026",
    "T_PROVI_NL",
    "T_PROVI_FR",
    "T_PROVI_DE",
    "CNIS_REGIO_2026",
    "T_REGIO_NL",
    "T_REGIO_FR",
    "T_REGIO_DE",
    "NUTS1_2024",
    "NUTS2_2024",
    "NUTS3_2024",
]


def _nis9_row(
    *, nis6, commune, mun_nl, mun_fr, arr, arr_nl, prov, prov_nl, reg, reg_nl, nuts3="BE211"
):
    return [
        f"{nis6}00-",
        "SECTOR",
        nis6,
        "SUBMUN",
        commune,
        mun_nl,
        mun_fr,
        mun_nl,
        arr,
        arr_nl,
        arr_nl,
        arr_nl,
        prov,
        prov_nl,
        prov_nl,
        prov_nl,
        reg,
        reg_nl,
        reg_nl,
        reg_nl,
        "BE2",
        "BE21",
        nuts3,
    ]


def _write_nis9(path, rows):
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(NIS9_HEADER)
    for row in rows:
        sheet.append(row)
    workbook.save(str(path))
    return path


def _write_refnis(path, rows, delimiter=";"):
    lines = [delimiter.join(["Code INS", "Entités", "Langue", "Code NIS", "Eenheden", "Taal"])]
    lines.extend(delimiter.join(row) for row in rows)
    path.write_text("﻿" + "\r\n".join(lines), encoding="utf-8")
    return path


def test_parse_refnis_detects_pipe_and_semicolon_delimiters(tmp_path):
    rows = [
        ["01000", "ROYAUME", "", "01000", "HET RIJK", ""],
        ["11001", "Aartselaar", "N", "11001", "Aartselaar", "N"],
    ]
    semi = parse_refnis_csv(_write_refnis(tmp_path / "semi.csv", rows, ";"))
    pipe = parse_refnis_csv(_write_refnis(tmp_path / "pipe.csv", rows, "|"))
    assert semi == pipe
    assert [r["nis_code"] for r in semi] == ["01000", "11001"]


def test_parse_refnis_marks_communes_by_language_regime(tmp_path):
    path = _write_refnis(
        tmp_path / "r.csv",
        [
            ["01000", "ROYAUME", "", "01000", "HET RIJK", ""],
            ["11001", "Aartselaar", "N", "11001", "Aartselaar", "N"],
        ],
    )
    rows = parse_refnis_csv(path)
    assert rows[0]["level_hint"] == "aggregate"
    assert rows[1]["level_hint"] == "commune"
    assert rows[1]["language_regime"] == "N"


def test_parse_nis9_rejects_renamed_columns(tmp_path):
    workbook = openpyxl.Workbook()
    workbook.active.append(["SOMETHING_ELSE"])
    path = tmp_path / "bad.xlsx"
    workbook.save(str(path))
    with pytest.raises(ValueError, match="missing expected column"):
        parse_nis9_xlsx(path)


def test_build_hierarchy_assembles_every_level(tmp_path):
    path = _write_nis9(
        tmp_path / "n.xlsx",
        [
            _nis9_row(
                nis6="11001A",
                commune="11001",
                mun_nl="Aartselaar",
                mun_fr="Aartselaar",
                arr=11000,
                arr_nl="Arr Antwerpen",
                prov=10000,
                prov_nl="Prov Antwerpen",
                reg="02000",
                reg_nl="Vlaams Gewest",
            )
        ],
    )
    refnis = [
        {
            "nis_code": "11001",
            "name_nl": "Aartselaar",
            "name_fr": "Aartselaar",
            "language_regime": "N",
            "level_hint": "commune",
        }
    ]
    entities = {e["geo_id"]: e for e in build_hierarchy(parse_nis9_xlsx(path), refnis, {})}

    assert set(entities) == {
        "be:country",
        "be:reg:02000",
        "be:prov:10000",
        "be:arr:11000",
        "be:mun:11001",
    }
    assert entities["be:mun:11001"]["parent_geo_id"] == "be:arr:11000"
    assert entities["be:arr:11000"]["parent_geo_id"] == "be:prov:10000"
    assert entities["be:prov:10000"]["parent_geo_id"] == "be:reg:02000"
    assert entities["be:reg:02000"]["parent_geo_id"] == "be:country"
    assert entities["be:country"]["parent_geo_id"] is None


def test_brussels_commune_skips_the_province_level(tmp_path):
    """Brussels-Capital has no province; its arrondissement parents to the
    region directly. A null-coalescing accident here would silently reparent
    19 communes."""
    path = _write_nis9(
        tmp_path / "n.xlsx",
        [
            _nis9_row(
                nis6="21004A",
                commune="21004",
                mun_nl="Brussel",
                mun_fr="Bruxelles",
                arr=21000,
                arr_nl="Arr Brussel",
                prov=None,
                prov_nl=None,
                reg="04000",
                reg_nl="Brussels Hoofdstedelijk Gewest",
            )
        ],
    )
    entities = {e["geo_id"]: e for e in build_hierarchy(parse_nis9_xlsx(path), [], {})}
    assert not any(e["level"] == "province" for e in entities.values())
    assert entities["be:arr:21000"]["parent_geo_id"] == "be:reg:04000"


def test_name_en_uses_official_language_then_exonym(tmp_path):
    path = _write_nis9(
        tmp_path / "n.xlsx",
        [
            _nis9_row(
                nis6="52011A",
                commune="52011",
                mun_nl="Charleroi",
                mun_fr="Charleroi",
                arr=52000,
                arr_nl="Arr Charleroi",
                prov=50000,
                prov_nl="Henegouwen",
                reg="03000",
                reg_nl="Waals Gewest",
            ),
            _nis9_row(
                nis6="44021A",
                commune="44021",
                mun_nl="Gent",
                mun_fr="Gand",
                arr=44000,
                arr_nl="Arr Gent",
                prov=40000,
                prov_nl="Oost-Vlaanderen",
                reg="02000",
                reg_nl="Vlaams Gewest",
            ),
        ],
    )
    refnis = [
        {
            "nis_code": "52011",
            "name_nl": "Charleroi",
            "name_fr": "Charleroi",
            "language_regime": "F",
            "level_hint": "commune",
        },
        {
            "nis_code": "44021",
            "name_nl": "Gent",
            "name_fr": "Gand",
            "language_regime": "N",
            "level_hint": "commune",
        },
    ]
    entities = {
        e["geo_id"]: e for e in build_hierarchy(parse_nis9_xlsx(path), refnis, {"44021": "Ghent"})
    }
    # French-regime commune falls back to its French name, not the Dutch one.
    assert entities["be:mun:52011"]["name_en"] == "Charleroi"
    # Exonym overrides the official-language default.
    assert entities["be:mun:44021"]["name_en"] == "Ghent"


def test_geo_id_convention():
    assert geo_id_for("country", None) == "be:country"
    assert geo_id_for("municipality", "11002") == "be:mun:11002"
    assert geo_id_for("region", "02000") == "be:reg:02000"
    with pytest.raises(ValueError, match="Unknown geography level"):
        geo_id_for("galaxy", "1")
    with pytest.raises(ValueError, match="requires a NIS code"):
        geo_id_for("municipality", None)


def test_committed_geographies_csv_matches_expected_counts():
    """Hardcoded counts, per docs/steps: a fixed expected count is the fastest
    way to detect a loader that silently dropped a province."""
    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / "config" / "geography" / "geographies.csv"
    with path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["level"]] = counts.get(row["level"], 0) + 1
    current = {}
    for row in rows:
        if not row["valid_to"]:
            current[row["level"]] = current.get(row["level"], 0) + 1
    assert current == {
        "country": 1,
        "region": 3,
        "province": 10,
        "arrondissement": 43,
        "municipality": 565,
    }
    # Historical windows: the 55 merged communes plus the aggregates whose
    # territory changed in 2019 or 2025.
    assert sum(1 for r in rows if r["valid_to"]) == 66
