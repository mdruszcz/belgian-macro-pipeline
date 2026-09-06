"""Tests for scripts/sync_police.py.

The real source file is one hand-fetched JSON object from police.be, 54 KB,
committed at data/raw/police/ -- faked here as a small in-memory equivalent
rather than depending on that exact file, so these tests do not silently
start passing or failing if it is ever refreshed or renamed.
"""

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_police  # noqa: E402

from src.db import migrate  # noqa: E402
from src.geography.resolve import UnknownGeographyError  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"


def _write_fixture(path: Path, rows: list[dict]) -> None:
    path.write_text(json.dumps({"error": False, "data": rows}), encoding="utf-8")


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    conn = sqlite3.connect(str(db_path))
    yield db_path, conn
    conn.close()


# --- reading the file -----------------------------------------------------


def test_reads_geo_code_and_z_into_a_flat_rate_map(tmp_path):
    path = tmp_path / "f.json"
    _write_fixture(path, [{"geo_code": "11001", "z": 76.77}, {"geo_code": "11002", "z": 63.14}])
    rates = sync_police._read_rates(path)
    assert rates == {"11001": 76.77, "11002": 63.14}


def test_an_error_true_payload_is_refused_rather_than_read(tmp_path):
    path = tmp_path / "f.json"
    path.write_text(json.dumps({"error": True, "data": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="error=true"):
        sync_police._read_rates(path)


def test_a_missing_data_list_is_refused_rather_than_guessed(tmp_path):
    path = tmp_path / "f.json"
    path.write_text(json.dumps({"error": False, "something_else": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="no usable 'data' list"):
        sync_police._read_rates(path)


# --- geography resolution --------------------------------------------------


def test_a_current_commune_resolves_directly_under_2025(db):
    db_path, conn = db
    assert sync_police._resolve(conn, "11001") == "be:mun:11001"


def test_a_pre_merger_only_commune_falls_back_to_2024(db):
    """Borsbeek (11007) merged into Antwerp on 2025-01-01 and does not exist
    under period 2025 -- it must resolve to ITS OWN geo_id via the 2024
    fallback, not silently attach to whatever absorbed it."""
    db_path, conn = db
    assert sync_police._resolve(conn, "11007") == "be:mun:11007"


def test_a_code_unknown_under_either_period_raises(db):
    db_path, conn = db
    with pytest.raises(UnknownGeographyError):
        sync_police._resolve(conn, "99999")


# --- the zero-valued placeholder rule --------------------------------------


def test_an_unresolvable_zero_valued_code_is_skipped_not_fatal(db, tmp_path, monkeypatch):
    """The three negative placeholder codes and three positive-but-unknown
    ones the real file carries are all always paired with z=0. This is a
    property of ANY unresolvable code with value 0, not a hardcoded list --
    tested here with a made-up code, not one of the six seen in the real
    file, to prove the rule is general."""
    db_path, conn = db
    path = tmp_path / "f.json"
    _write_fixture(
        path,
        [
            {"geo_code": "11001", "z": 76.77},
            {"geo_code": "-1", "z": 0},
            {"geo_code": "99999999", "z": 0},  # not one of the real file's codes
        ],
    )
    read, written = sync_police.sync(db_path, path)
    assert read == 3
    assert written == 1  # only the one real commune


def test_an_unresolvable_nonzero_code_is_fatal(db, tmp_path):
    """The one case NOT forgiven: an unresolvable code carrying a real
    number. Guessing what it means would be exactly the mistake CLAUDE.md
    rule 13 exists to prevent."""
    db_path, conn = db
    path = tmp_path / "f.json"
    _write_fixture(path, [{"geo_code": "-1", "z": 12.5}])
    with pytest.raises(SystemExit, match="did not resolve"):
        sync_police.sync(db_path, path)


# --- end-to-end load --------------------------------------------------------


def test_first_sync_writes_provisional_observations(db, tmp_path):
    db_path, conn = db
    path = tmp_path / "f.json"
    _write_fixture(
        path,
        [
            {"geo_code": "11001", "z": 76.77},
            {"geo_code": "11007", "z": 61.47},  # Borsbeek, 2024 fallback
        ],
    )
    read, written = sync_police.sync(db_path, path)
    assert (read, written) == (2, 2)

    rows = conn.execute(
        "SELECT geo_id, value, status, period FROM observations "
        "WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K' ORDER BY geo_id"
    ).fetchall()
    assert rows == [
        ("be:mun:11001", 76.77, "provisional", "2025"),
        ("be:mun:11007", 61.47, "provisional", "2025"),
    ]


def test_resync_with_unchanged_values_writes_no_new_vintage(db, tmp_path):
    db_path, conn = db
    path = tmp_path / "f.json"
    _write_fixture(path, [{"geo_code": "11001", "z": 76.77}])
    sync_police.sync(db_path, path)
    read, written = sync_police.sync(db_path, path)
    assert (read, written) == (1, 0)


def test_reference_rows_mark_the_indicator_not_aggregatable(db):
    db_path, conn = db
    sync_police.sync(db_path, reference_rows_only=True)
    row = conn.execute(
        "SELECT is_additive, aggregation_method FROM indicators "
        "WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K'"
    ).fetchone()
    assert row == (0, "not_applicable")


def test_reference_rows_only_needs_no_source_file(db, tmp_path):
    db_path, conn = db
    read, written = sync_police.sync(
        db_path, tmp_path / "does_not_exist.json", reference_rows_only=True
    )
    assert (read, written) == (0, 0)


def test_a_missing_source_file_raises_a_clear_error(db, tmp_path):
    db_path, conn = db
    with pytest.raises(FileNotFoundError, match="not found"):
        sync_police.sync(db_path, tmp_path / "does_not_exist.json")
