import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.db import migrate

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


@pytest.fixture
def migrated_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    return db_path


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def test_fresh_db_applies_all_migrations(migrated_db):
    conn = sqlite3.connect(str(migrated_db))
    tables = _tables(conn)
    for expected in (
        "sources",
        "geographies",
        "indicators",
        "observations",
        "fetch_runs",
        "schema_migrations",
    ):
        assert expected in tables
    rows = conn.execute(
        "SELECT version, filename FROM schema_migrations ORDER BY version"
    ).fetchall()
    assert rows == [(1, "001_core_schema.sql"), (2, "002_indexes.sql")]
    conn.close()


def test_round_trip_insert_and_read(migrated_db):
    conn = migrate.connect(migrated_db)
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) VALUES (?,?,?,?,?)",
        ("nbb", "NBB SDMX", "NBB", "nbb", "docs/data_catalog.md#nbb"),
    )
    conn.execute(
        """INSERT INTO geographies
           (geo_id, level, name_nl, name_fr, name_en, valid_from)
           VALUES (?,?,?,?,?,?)""",
        ("be:country", "country", "België", "Belgique", "Belgium", "1830-01-01"),
    )
    conn.execute(
        """INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            "gdp",
            "nbb",
            "BBP",
            "PIB",
            "GDP",
            "Q",
            "percent_yy",
            "higher_is_better",
            0,
            "config/indicators/gdp.yaml",
        ),
    )
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?,?,?,?)",
        ("nbb", "nbb", now, "ok"),
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            "gdp",
            "be:country",
            "2024-Q1",
            "2024-05-01",
            1.5,
            "final",
            "2024-01-01",
            "2024-03-31",
            1,
            run_id,
            now,
        ),
    )
    conn.commit()

    row = conn.execute(
        "SELECT indicator_id, geo_id, period, vintage, value, status FROM observations"
    ).fetchone()
    assert row == ("gdp", "be:country", "2024-Q1", "2024-05-01", 1.5, "final")
    conn.close()


def _insert_one_observation(conn, value=1.0, vintage="2024-05-01"):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO sources (source_id, name, agency, adapter, catalog_ref) VALUES (?,?,?,?,?)",
        ("nbb", "NBB SDMX", "NBB", "nbb", "docs/data_catalog.md#nbb"),
    )
    conn.execute(
        """INSERT OR IGNORE INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
           VALUES (?,?,?,?,?,?)""",
        ("be:country", "country", "België", "Belgique", "Belgium", "1830-01-01"),
    )
    conn.execute(
        """INSERT OR IGNORE INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            "gdp",
            "nbb",
            "BBP",
            "PIB",
            "GDP",
            "Q",
            "percent_yy",
            "higher_is_better",
            0,
            "config/indicators/gdp.yaml",
        ),
    )
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?,?,?,?)",
        ("nbb", "nbb", now, "ok"),
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            "gdp",
            "be:country",
            "2024-Q1",
            vintage,
            value,
            "final",
            "2024-01-01",
            "2024-03-31",
            1,
            run_id,
            now,
        ),
    )
    conn.commit()
    return run_id


def test_pk_collision_insert_or_ignore_keeps_first_row(migrated_db):
    conn = migrate.connect(migrated_db)
    _insert_one_observation(conn, value=1.0)
    conn.execute("""INSERT OR IGNORE INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('gdp', 'be:country', '2024-Q1', '2024-05-01', 999.0, 'final',
                   '2024-01-01', '2024-03-31', 1,
                   (SELECT fetch_run_id FROM fetch_runs LIMIT 1), '2024-05-01T00:00:00+00:00')""")
    rows = conn.execute("SELECT value FROM observations").fetchall()
    assert rows == [(1.0,)]
    conn.close()


def test_pk_collision_plain_insert_raises(migrated_db):
    conn = migrate.connect(migrated_db)
    _insert_one_observation(conn, value=1.0)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO observations
               (indicator_id, geo_id, period, vintage, value, status,
                period_start, period_end, is_latest, fetch_run_id, created_at)
               VALUES ('gdp', 'be:country', '2024-Q1', '2024-05-01', 999.0, 'final',
                       '2024-01-01', '2024-03-31', 1,
                       (SELECT fetch_run_id FROM fetch_runs LIMIT 1), '2024-05-01T00:00:00+00:00')"""
        )
    conn.close()


def test_fk_violation_raises_with_pragma_on(migrated_db):
    conn = migrate.connect(migrated_db)  # sets PRAGMA foreign_keys=ON
    now = datetime.now(timezone.utc).isoformat()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO observations
               (indicator_id, geo_id, period, vintage, value, status,
                period_start, period_end, is_latest, fetch_run_id, created_at)
               VALUES ('does_not_exist', 'be:country', '2024-Q1', '2024-05-01', 1.0, 'final',
                       '2024-01-01', '2024-03-31', 1, 1, ?)""",
            (now,),
        )
    conn.close()


def test_fk_violation_silently_allowed_with_pragma_default_off(migrated_db):
    # Same insert, but on a connection where foreign_keys was never turned on
    # (SQLite's own default). This directly demonstrates the risk the spec
    # names: integrity constraints you believe exist do not, unless the
    # pragma is set on every connection.
    conn = sqlite3.connect(str(migrated_db))  # no PRAGMA foreign_keys=ON
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('does_not_exist', 'geo_does_not_exist', '2024-Q1', '2024-05-01', 1.0, 'final',
                   '2024-01-01', '2024-03-31', 1, 999, ?)""",
        (now,),
    )
    conn.commit()
    row = conn.execute("SELECT indicator_id, geo_id FROM observations").fetchone()
    assert row == ("does_not_exist", "geo_does_not_exist")
    conn.close()


def test_migration_idempotency(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db_path))
    schema_before = sorted(
        r[0] for r in conn.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL")
    )
    conn.close()

    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)  # second run

    conn = sqlite3.connect(str(db_path))
    assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 2
    schema_after = sorted(
        r[0] for r in conn.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL")
    )
    assert schema_before == schema_after
    conn.close()


def test_migration_checksum_mismatch_detected(tmp_path):
    db_path = tmp_path / "test.db"
    migdir = tmp_path / "migrations"
    migdir.mkdir()
    migration_file = migdir / "001_x.sql"
    migration_file.write_text("CREATE TABLE t (id INTEGER PRIMARY KEY);")

    migrate.run(db_path, migrations_dir=migdir)

    migration_file.write_text("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        migrate.run(db_path, migrations_dir=migdir)


def test_failed_migration_rolls_back_ddl(tmp_path):
    db_path = tmp_path / "test.db"
    migdir = tmp_path / "migrations"
    migdir.mkdir()
    (migdir / "001_ok.sql").write_text("CREATE TABLE t (id INTEGER PRIMARY KEY);")
    (migdir / "002_bad.sql").write_text("CREATE TABLE t2 (id INTEGER PRIMARY KEY); GARBAGE SQL;")

    with pytest.raises(sqlite3.OperationalError):
        migrate.run(db_path, migrations_dir=migdir)

    conn = sqlite3.connect(str(db_path))
    tables = _tables(conn)
    assert "t" in tables
    assert "t2" not in tables  # rolled back, not left half-applied
    assert conn.execute("SELECT version FROM schema_migrations").fetchall() == [(1,)]
    conn.close()
