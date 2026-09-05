import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import sync_to_canonical as sync_mod  # noqa: E402

import belgian_macro_db as bmdb  # noqa: E402
from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


@pytest.fixture
def db_with_legacy(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE legacy_indicators (code TEXT PRIMARY KEY);
        CREATE TABLE legacy_observations (indicator_code TEXT, period TEXT, value REAL,
                                           obs_status TEXT, fetched_at TEXT);
        """)
    conn.commit()
    conn.close()

    fake_sources = {
        "GDP_QUARTERLY_YY": {
            "name": "GDP",
            "url": "https://example.test/nbb",
            "frequency": "Q",
            "unit": "percent_yy",
            "source_agency": "NBB",
            "type": "nbb",
        }
    }
    monkeypatch.setattr(bmdb, "SOURCES", fake_sources)
    monkeypatch.setattr(sync_mod, "SOURCES", fake_sources)
    monkeypatch.setattr(sync_mod, "INCLUDED", {"GDP_QUARTERLY_YY"})
    monkeypatch.setattr(sync_mod, "PREFERRED_DIRECTION", {"GDP_QUARTERLY_YY": "higher_is_better"})
    return db_path


def _set_legacy_row(db_path, period, value, obs_status="A"):
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "DELETE FROM legacy_observations WHERE indicator_code='GDP_QUARTERLY_YY' AND period=?",
        (period,),
    )
    conn.execute(
        "INSERT INTO legacy_observations VALUES ('GDP_QUARTERLY_YY', ?, ?, ?, '2026-01-01T00:00:00+00:00')",
        (period, value, obs_status),
    )
    conn.commit()
    conn.close()


def test_first_sync_inserts_new_vintage(db_with_legacy):
    _set_legacy_row(db_with_legacy, "2024-Q1", 1.5)
    checked, changed = sync_mod.sync(db_with_legacy, vintage="v1")
    assert checked == 1
    assert changed == 1
    conn = sqlite3.connect(str(db_with_legacy))
    row = conn.execute(
        "SELECT value, status, is_latest FROM observations WHERE period='2024-Q1'"
    ).fetchone()
    assert row == (1.5, "final", 1)


def test_unchanged_value_creates_no_new_vintage(db_with_legacy):
    _set_legacy_row(db_with_legacy, "2024-Q1", 1.5)
    sync_mod.sync(db_with_legacy, vintage="v1")
    checked, changed = sync_mod.sync(db_with_legacy, vintage="v2")
    assert checked == 1
    assert changed == 0
    conn = sqlite3.connect(str(db_with_legacy))
    count = conn.execute("SELECT COUNT(*) FROM observations WHERE period='2024-Q1'").fetchone()[0]
    assert count == 1


def test_changed_value_inserts_new_vintage_and_flips_is_latest(db_with_legacy):
    _set_legacy_row(db_with_legacy, "2024-Q1", 1.5)
    sync_mod.sync(db_with_legacy, vintage="v1")
    _set_legacy_row(db_with_legacy, "2024-Q1", 2.0)
    checked, changed = sync_mod.sync(db_with_legacy, vintage="v2")
    assert changed == 1

    conn = sqlite3.connect(str(db_with_legacy))
    rows = conn.execute(
        "SELECT vintage, value, is_latest FROM observations WHERE period='2024-Q1' ORDER BY vintage"
    ).fetchall()
    assert rows == [("v1", 1.5, 0), ("v2", 2.0, 1)]


def test_genuine_vintage_collision_raises_instead_of_dropping_data(db_with_legacy):
    _set_legacy_row(db_with_legacy, "2024-Q1", 1.5)
    sync_mod.sync(db_with_legacy, vintage="same-vintage")
    _set_legacy_row(db_with_legacy, "2024-Q1", 999.0)
    with pytest.raises(RuntimeError, match="Vintage collision"):
        sync_mod.sync(db_with_legacy, vintage="same-vintage")
