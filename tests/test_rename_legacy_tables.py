import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from rename_legacy_tables import rename_legacy_tables  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def test_noop_on_fresh_db_with_no_legacy_tables(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db_path))
    before = _tables(conn)

    rename_legacy_tables(conn)

    assert _tables(conn) == before
    conn.close()


def test_renames_old_shaped_tables(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE indicators (code TEXT PRIMARY KEY, name TEXT, frequency TEXT,
                                  unit TEXT, source_agency TEXT, description TEXT, api_url TEXT);
        CREATE TABLE observations (indicator_code TEXT, period TEXT, value REAL,
                                    obs_status TEXT, fetched_at TEXT);
        CREATE TABLE fetch_log (id INTEGER PRIMARY KEY, indicator_code TEXT, fetched_at TEXT,
                                 rows_upserted INTEGER, status TEXT, message TEXT);
        """)
    conn.execute("INSERT INTO indicators VALUES ('GDP', 'GDP', 'Q', 'pct', 'NBB', '', '')")
    conn.commit()

    rename_legacy_tables(conn)

    tables = _tables(conn)
    assert "legacy_indicators" in tables
    assert "legacy_observations" in tables
    assert "legacy_fetch_log" in tables
    assert "indicators" not in tables
    row = conn.execute("SELECT code FROM legacy_indicators").fetchone()
    assert row == ("GDP",)
    conn.close()


def test_idempotent_second_call_is_noop(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE indicators (code TEXT PRIMARY KEY);
        CREATE TABLE observations (indicator_code TEXT);
        CREATE TABLE fetch_log (id INTEGER PRIMARY KEY, indicator_code TEXT);
        """)
    conn.commit()

    rename_legacy_tables(conn)
    after_first = _tables(conn)
    rename_legacy_tables(conn)  # must not raise, must not change anything further
    assert _tables(conn) == after_first
    conn.close()


def test_does_not_touch_already_canonical_indicators_table(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db_path))
    before = _tables(conn)

    rename_legacy_tables(conn)  # canonical `indicators` has indicator_id, not code -- must skip

    assert _tables(conn) == before
    assert "legacy_indicators" not in _tables(conn)
    conn.close()
