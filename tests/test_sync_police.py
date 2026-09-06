"""Tests for scripts/sync_police.py.

The real source files are hand-fetched JSON from police.be, committed under
data/raw/police/ (gitignored). Faked here as small in-memory equivalents
rather than depending on the exact real files, so these tests do not
silently start passing or failing if they are ever refreshed or renamed.
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


def _write_year_file(directory: Path, year: str, rows: list[dict]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / year).write_text(json.dumps({"error": False, "data": rows}), encoding="utf-8")


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    conn = sqlite3.connect(str(db_path))
    yield db_path, conn
    conn.close()


def _one_category_dir(tmp_path, dirname: str = "cambriolage") -> Path:
    """A raw_dir containing only ONE of the four real categories, so a test
    does not need to fake all four just to exercise one behaviour. sync()
    still requires every configured category to have at least one file, so
    callers that use this must monkeypatch DATASETS down to that one entry."""
    return tmp_path / dirname


# --- reading a file ---------------------------------------------------------


def test_reads_geo_code_and_z_into_a_flat_rate_map(tmp_path):
    path = tmp_path / "f.json"
    path.write_text(
        json.dumps({"error": False, "data": [{"geo_code": "11001", "z": 76.77}]}),
        encoding="utf-8",
    )
    assert sync_police._read_rates(path) == {"11001": 76.77}


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


# --- year-file discovery -----------------------------------------------------


def test_years_available_reads_purely_numeric_filenames(tmp_path):
    d = tmp_path / "cambriolage"
    _write_year_file(d, "2017", [])
    _write_year_file(d, "2024", [])
    (d / "notes.txt").write_text("not a year")
    assert sync_police._years_available(d) == ["2017", "2024"]


def test_years_available_is_empty_for_a_missing_directory(tmp_path):
    assert sync_police._years_available(tmp_path / "does_not_exist") == []


# --- geography: the pinned-period rule --------------------------------------


def test_a_current_commune_resolves_at_the_pinned_period(db):
    db_path, conn = db
    assert sync_police._resolve(conn, "11001") == "be:mun:11001"


def test_a_pre_merger_only_commune_resolves_to_its_own_geo_id(db):
    """Borsbeek (11007) merged into Antwerp on 2025-01-01. The pinned period
    (2024) is BEFORE that merger, so it must resolve to Borsbeek itself, not
    silently attach to whatever absorbed it."""
    db_path, conn = db
    assert sync_police._resolve(conn, "11007") == "be:mun:11007"


def test_a_2019_merger_created_commune_resolves_even_for_an_older_filename(db):
    """Kruisem (45068) did not exist before 2019-01-01. The pinned period
    (2024) is AFTER that merger, so it resolves fine regardless of which
    year's FILE the row came from -- proving the rule is 'resolve at the
    pinned period', not 'resolve at the file's own year', which would raise
    for exactly this commune on a pre-2019 file."""
    db_path, conn = db
    assert sync_police._resolve(conn, "45068") == "be:mun:45068"


def test_a_code_unknown_at_the_pinned_period_raises(db):
    db_path, conn = db
    with pytest.raises(UnknownGeographyError):
        sync_police._resolve(conn, "99999")


# --- the zero-valued placeholder rule ---------------------------------------


def test_an_unresolvable_zero_valued_code_is_skipped_not_fatal(db, tmp_path, monkeypatch):
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "cambriolage",
        "2024",
        [
            {"geo_code": "11001", "z": 76.77},
            {"geo_code": "-1", "z": 0},
            {"geo_code": "99999999", "z": 0},  # not one of the real file's codes
        ],
    )
    read, written = sync_police.sync(db_path, raw_dir)
    assert read == 3
    assert written == 1  # only the one real commune


def test_an_unresolvable_nonzero_code_is_fatal(db, tmp_path, monkeypatch):
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2024", [{"geo_code": "-1", "z": 12.5}])
    with pytest.raises(SystemExit, match="did not resolve"):
        sync_police.sync(db_path, raw_dir)


# --- status: latest year provisional, earlier years final -------------------


def test_only_the_latest_year_in_each_category_is_provisional(db, tmp_path, monkeypatch):
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2017", [{"geo_code": "11001", "z": 100.0}])
    _write_year_file(raw_dir / "cambriolage", "2024", [{"geo_code": "11001", "z": 76.77}])
    sync_police.sync(db_path, raw_dir)

    rows = dict(
        conn.execute(
            "SELECT period, status FROM observations "
            "WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K' AND geo_id = 'be:mun:11001'"
        ).fetchall()
    )
    assert rows == {"2017": "final", "2024": "provisional"}


def test_a_single_year_category_has_that_one_year_provisional(db, tmp_path, monkeypatch):
    """The three categories with only a 2025 file each -- their one year is
    automatically 'the latest', so it is provisional, same as the multi-year
    category's own latest year."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "vol de voiture", "2025", [{"geo_code": "11001", "z": 5.0}])
    sync_police.sync(db_path, raw_dir)
    status = conn.execute(
        "SELECT status FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K'"
    ).fetchone()
    assert status == ("provisional",)


# --- resolution is pinned regardless of the file's own year -----------------


def test_a_2019_merger_created_commune_loads_from_a_pre_2019_filename(db, tmp_path, monkeypatch):
    """The real-world case this whole design exists for: Kruisem carries a
    value in police.be's own '2000' file despite not existing until 2019.
    That must load, resolved to Kruisem's real geo_id, not raise."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2000", [{"geo_code": "45068", "z": 131.86}])
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (1, 1)
    row = conn.execute(
        "SELECT geo_id, value FROM observations WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K'"
    ).fetchone()
    assert row == ("be:mun:45068", 131.86)


# --- end-to-end load, all four categories -----------------------------------


def test_first_sync_loads_every_configured_category(db, tmp_path):
    db_path, conn = db
    raw_dir = tmp_path / "raw"
    for dirname in sync_police.DATASETS.values():
        _write_year_file(raw_dir / dirname, "2025", [{"geo_code": "11001", "z": 1.0}])
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (4, 4)
    loaded = {
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT indicator_id FROM observations WHERE geo_id = 'be:mun:11001'"
        )
    }
    assert loaded == set(sync_police.DATASETS)


def test_a_category_with_no_files_at_all_raises(db, tmp_path):
    db_path, conn = db
    raw_dir = tmp_path / "raw"  # empty -- no category directories exist
    with pytest.raises(FileNotFoundError, match="no year-named file"):
        sync_police.sync(db_path, raw_dir)


def test_resync_with_unchanged_values_writes_no_new_vintage(db, tmp_path, monkeypatch):
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2024", [{"geo_code": "11001", "z": 76.77}])
    sync_police.sync(db_path, raw_dir)
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (1, 0)


def test_reference_rows_mark_every_indicator_not_aggregatable(db):
    db_path, conn = db
    sync_police.sync(db_path, reference_rows_only=True)
    placeholders = ",".join("?" * len(sync_police.DATASETS))
    rows = conn.execute(
        f"SELECT indicator_id, is_additive, aggregation_method FROM indicators "
        f"WHERE indicator_id IN ({placeholders})",
        list(sync_police.DATASETS),
    ).fetchall()
    assert len(rows) == len(sync_police.DATASETS)
    for _indicator_id, is_additive, aggregation_method in rows:
        assert (is_additive, aggregation_method) == (0, "not_applicable")


def test_reference_rows_only_needs_no_source_files(db, tmp_path):
    db_path, conn = db
    read, written = sync_police.sync(db_path, tmp_path / "does_not_exist", reference_rows_only=True)
    assert (read, written) == (0, 0)
