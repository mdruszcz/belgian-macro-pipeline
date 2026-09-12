"""Contracts for the Vlaamse Arbeidsrekening municipal-rate loader."""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_var  # noqa: E402

from src.db import migrate  # noqa: E402

REPO = Path(__file__).resolve().parents[1]


def _write_export(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-16", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=sync_var.EXPECTED_COLUMNS, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def _row(label="Antwerpen", period="2024", value="6,4%") -> dict:
    return {
        "Geo": label,
        "Geslacht": "Totaal",
        "Leeftijdsklasse": "15-64",
        "Jaar": period,
        "Geslacht_titel": "Mannen en vrouwen",
        sync_var.MEASURE: value,
    }


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "var.db"
    migrate.run(path, migrations_dir=REPO / "migrations")
    load_geography.load(path, REPO / "config" / "geography", allow_unverified=True)
    return path


def test_parser_reads_decimal_comma_percent_and_suppression(tmp_path):
    path = tmp_path / "var.csv"
    _write_export(path, [_row(value="6,4%"), _row(label="Herstappe", value="")])
    rows = sync_var.read_export(path, expected_communes=2)
    assert [(row["label"], row["value"]) for row in rows] == [
        ("Antwerpen", 6.4),
        ("Herstappe", None),
    ]


def test_parser_refuses_the_wrong_age_filter(tmp_path):
    path = tmp_path / "var.csv"
    row = _row()
    row["Leeftijdsklasse"] = "20-64"
    _write_export(path, [row])
    with pytest.raises(ValueError, match="not filtered to ages 15-64"):
        sync_var.read_export(path, expected_communes=1)


def test_parser_refuses_partial_coverage(tmp_path):
    path = tmp_path / "var.csv"
    _write_export(path, [_row()])
    with pytest.raises(ValueError, match="exactly 565"):
        sync_var.read_export(path)


def test_parser_refuses_a_value_without_percent_unit(tmp_path):
    path = tmp_path / "var.csv"
    _write_export(path, [_row(value="6,4")])
    with pytest.raises(ValueError, match="no percent sign"):
        sync_var.read_export(path, expected_communes=1)


def test_current_dutch_labels_resolve_strictly_and_parentheses_are_only_disambiguation(db):
    conn = sqlite3.connect(db)
    index = sync_var.CommuneIndex(conn)
    assert index.count == 565
    assert index.resolve("Aalst (Aalst)") == "be:mun:41002"
    assert index.resolve("Beveren-Kruibeke-Zwijndrecht") == "be:mun:46030"
    assert index.resolve("Aalst with a typo") is None
    conn.close()


def test_reference_row_is_a_non_additive_direct_rate(db):
    sync_var.sync(db, reference_rows_only=True)
    conn = sqlite3.connect(db)
    row = conn.execute(
        "SELECT source_id, unit, is_additive, aggregation_method, decimals "
        "FROM indicators WHERE indicator_id = ?",
        (sync_var.INDICATOR_ID,),
    ).fetchone()
    conn.close()
    assert row == ("steunpunt_werk", "percent", 0, "not_applicable", 1)


def test_real_export_covers_every_current_commune_when_available(db):
    source = sync_var.DEFAULT_SOURCE_FILE
    if not source.is_file():
        pytest.skip("manual Tableau export is not present")
    read, written = sync_var.sync(db, source)
    assert read % 565 == 0
    assert written == read
    conn = sqlite3.connect(db)
    assert (
        conn.execute(
            "SELECT COUNT(DISTINCT geo_id) FROM observations WHERE indicator_id = ?",
            (sync_var.INDICATOR_ID,),
        ).fetchone()[0]
        == 565
    )
    assert conn.execute(
        "SELECT status FROM observations WHERE indicator_id = ? AND geo_id = 'be:mun:73028'",
        (sync_var.INDICATOR_ID,),
    ).fetchone() == ("suppressed",)
    conn.close()
