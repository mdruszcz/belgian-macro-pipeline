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
        "indicator_volume",
        "schema_migrations",
    ):
        assert expected in tables
    rows = conn.execute(
        "SELECT version, filename FROM schema_migrations ORDER BY version"
    ).fetchall()
    # Derived from the directory rather than hardcoded, so adding a migration
    # does not require editing this assertion -- which is how a test that is
    # meant to guard ordering turns into one people edit reflexively.
    expected_rows = [
        (version, path.name) for version, path in migrate.discover_migrations(REAL_MIGRATIONS_DIR)
    ]
    assert rows == expected_rows
    assert [v for v, _ in expected_rows] == sorted(v for v, _ in expected_rows)
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
    assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == len(
        migrate.discover_migrations(REAL_MIGRATIONS_DIR)
    )
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


# ---------------------------------------------------------------------------
# "recreate-with-foreign-keys-off" mode (PR #174 audit, SHOULD-FIX 1).
#
# A migration that recreates a table other tables' foreign keys target
# (SQLite has no ALTER TABLE ... ALTER CONSTRAINT for an inline CHECK) used
# to manage its own PRAGMA foreign_keys / BEGIN / COMMIT. The audit proved,
# on temp-database probes, three ways trusting the file instead of the
# runner could go wrong:
#   (a) a file that sets PRAGMA foreign_keys=OFF and never restores it
#       leaves FK enforcement off for every migration and connection after it.
#   (b) a file with no BEGIN of its own is not atomic: a statement failing
#       partway through leaves earlier DDL in that file applied.
#   (c) checking PRAGMA foreign_key_check after the file's own COMMIT means
#       a real violation is found too late to roll back -- the schema is
#       changed but the migration is not recorded, an inconsistent state.
#
# The fix moves PRAGMA foreign_keys, BEGIN/COMMIT and the FK check entirely
# into src/db/migrate.py's runner (_apply_recreate_mode); a migration file
# opts in with a fixed first-line marker and must not touch any of those
# itself (_refuse_forbidden_statements). These tests reproduce each of (a),
# (b), (c) against synthetic two-table schemas built the same way this
# file's other synthetic-migdir tests already do, plus a run of the real
# migration 004 against a representative geographies table to prove it is
# row-preserving.
# ---------------------------------------------------------------------------

_RECREATE_MARKER = migrate.RECREATE_MODE_MARKER

_PARENT_CHILD_SCHEMA = """
CREATE TABLE parent (
    id    TEXT PRIMARY KEY,
    level TEXT NOT NULL CHECK (level IN ('a', 'b'))
);
CREATE TABLE child (
    id        INTEGER PRIMARY KEY,
    parent_id TEXT NOT NULL REFERENCES parent(id)
);
"""


def _seed_parent_child(migdir, tmp_path):
    (migdir / "001_schema.sql").write_text(_PARENT_CHILD_SCHEMA)
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=migdir)
    conn = migrate.connect(db_path)
    conn.execute("INSERT INTO parent (id, level) VALUES ('p1', 'a')")
    conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 'p1')")
    conn.commit()
    conn.close()
    return db_path


def test_recreate_mode_widens_a_check_constraint_and_restores_foreign_keys_on(tmp_path):
    """The happy path: recreate `parent` with a wider CHECK, keep `child`'s
    FK intact and enforced afterwards. Proves (a) on the connection that
    actually matters: migrate.run() applies migrations 002 (recreate-mode)
    and 003 (normal-mode, on the SAME shared connection, in the SAME run()
    call) back to back -- if 002 left foreign_keys OFF and never restored
    it, 003's bad INSERT below would succeed silently instead of raising.
    A fresh connection opened afterwards would hide that bug entirely
    (migrate.connect() always sets foreign_keys=ON itself), so this test
    deliberately does not use one to make its assertion."""
    migdir = tmp_path / "migrations"
    migdir.mkdir()
    db_path = _seed_parent_child(migdir, tmp_path)

    (migdir / "002_widen.sql").write_text(
        f"{_RECREATE_MARKER}\n"
        "CREATE TABLE parent_new (\n"
        "    id    TEXT PRIMARY KEY,\n"
        "    level TEXT NOT NULL CHECK (level IN ('a', 'b', 'c'))\n"
        ");\n"
        "INSERT INTO parent_new (id, level) SELECT id, level FROM parent;\n"
        "DROP TABLE parent;\n"
        "ALTER TABLE parent_new RENAME TO parent;\n"
    )
    # Runs immediately after 002, on the same connection, within the same
    # migrate.run() call: a normal-mode migration whose own data-fixing
    # INSERT violates the FK 002 must have already restored.
    (migdir / "003_bad_insert.sql").write_text(
        "INSERT INTO child (id, parent_id) VALUES (2, 'does-not-exist');"
    )

    with pytest.raises(sqlite3.IntegrityError):
        migrate.run(db_path, migrations_dir=migdir)

    conn = sqlite3.connect(str(db_path))
    # 002 itself still applied and is recorded (only 003 failed and rolled back).
    assert conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 2").fetchone() == (
        1,
    )
    assert conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 3").fetchone() == (
        0,
    )
    assert conn.execute("SELECT COUNT(*) FROM child").fetchone() == (1,)  # the bad row never landed
    # And the widened CHECK genuinely took effect.
    conn.execute("INSERT INTO parent (id, level) VALUES ('p2', 'c')")
    conn.close()


def test_recreate_mode_refuses_a_file_that_manages_its_own_pragma_or_transaction(tmp_path):
    """A file that tries to own PRAGMA foreign_keys/BEGIN/COMMIT itself is
    refused outright -- exactly the trust the audit proved unsafe -- rather
    than silently allowed to race the runner's own control of the same
    connection."""
    migdir = tmp_path / "migrations"
    migdir.mkdir()
    db_path = _seed_parent_child(migdir, tmp_path)

    (migdir / "002_bad.sql").write_text(
        f"{_RECREATE_MARKER}\n"
        "PRAGMA foreign_keys = OFF;\n"
        "CREATE TABLE parent_new (id TEXT PRIMARY KEY, level TEXT NOT NULL CHECK (level IN ('a','b','c')));\n"
    )

    with pytest.raises(migrate.MigrationError, match="PRAGMA"):
        migrate.run(db_path, migrations_dir=migdir)

    conn = sqlite3.connect(str(db_path))
    assert conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 2").fetchone() == (
        0,
    )
    assert "parent_new" not in _tables(conn)
    conn.close()


def test_recreate_mode_failure_mid_file_leaves_nothing_applied(tmp_path):
    """(b): a statement failing partway through a recreate-mode file must
    not leave the earlier statements in that same file applied -- the
    runner's own BEGIN covers the whole file, not just the parts a
    (hypothetical, forbidden) file-level BEGIN would have covered."""
    migdir = tmp_path / "migrations"
    migdir.mkdir()
    db_path = _seed_parent_child(migdir, tmp_path)

    (migdir / "002_bad.sql").write_text(
        f"{_RECREATE_MARKER}\n"
        "CREATE TABLE parent_new (id TEXT PRIMARY KEY, level TEXT NOT NULL CHECK (level IN ('a','b','c')));\n"
        "INSERT INTO parent_new (id, level) SELECT id, level FROM parent;\n"
        "DROP TABLE parent;\n"
        "GARBAGE SQL THAT DOES NOT PARSE;\n"
    )

    with pytest.raises(sqlite3.OperationalError):
        migrate.run(db_path, migrations_dir=migdir)

    conn = sqlite3.connect(str(db_path))
    tables = _tables(conn)
    # Nothing left half-applied: the original `parent` table is still there
    # (the DROP was rolled back too), and the new one never survives.
    assert "parent" in tables
    assert "parent_new" not in tables
    assert conn.execute("SELECT COUNT(*) FROM parent").fetchone() == (1,)
    assert conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 2").fetchone() == (
        0,
    )
    assert conn.execute("PRAGMA foreign_keys").fetchone() == (0,)  # fresh connection default
    conn.close()

    # And the restore genuinely happened on migrate.run()'s own connection,
    # not just visible on a later fresh one by coincidence: run a THIRD
    # migration that would misbehave under foreign_keys=OFF and confirm it
    # is enforced.
    (migdir / "003_probe.sql").write_text("CREATE TABLE probe (id INTEGER PRIMARY KEY);")
    with pytest.raises(sqlite3.OperationalError):
        migrate.run(db_path, migrations_dir=migdir)  # 002 still fails, re-raises before 003
    conn = sqlite3.connect(str(db_path))
    assert "probe" not in _tables(conn)
    conn.close()


def test_recreate_mode_fk_violation_is_caught_before_commit_and_not_recorded(tmp_path):
    """(c): a recreate-mode migration whose own INSERT...SELECT drops a row
    a child table depends on must be caught by the runner's
    PRAGMA foreign_key_check BEFORE commit -- rolled back, not applied, not
    recorded -- never silently left as a broken-but-recorded schema."""
    migdir = tmp_path / "migrations"
    migdir.mkdir()
    db_path = _seed_parent_child(migdir, tmp_path)

    # Deliberately buggy: WHERE level='b' drops 'p1' (level='a'), which
    # `child` still references -- the exact shape of bug this check exists
    # to catch.
    (migdir / "002_orphans.sql").write_text(
        f"{_RECREATE_MARKER}\n"
        "CREATE TABLE parent_new (id TEXT PRIMARY KEY, level TEXT NOT NULL CHECK (level IN ('a','b','c')));\n"
        "INSERT INTO parent_new (id, level) SELECT id, level FROM parent WHERE level = 'b';\n"
        "DROP TABLE parent;\n"
        "ALTER TABLE parent_new RENAME TO parent;\n"
    )

    with pytest.raises(migrate.MigrationError, match="foreign_key_check"):
        migrate.run(db_path, migrations_dir=migdir)

    conn = sqlite3.connect(str(db_path))
    # Rolled back: the original row is still there, untouched.
    assert conn.execute("SELECT id, level FROM parent").fetchall() == [("p1", "a")]
    assert conn.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 2").fetchone() == (
        0,
    )
    conn.close()


def test_004_preserves_every_existing_geographies_row():
    """Migration 004 (the real file) widens geographies.level's CHECK to
    add 'nuts2' by recreating the table -- this proves that recreation is
    row-preserving, using a representative row per existing level plus the
    nullable columns and self-references a real Belgian hierarchy actually
    uses, run through the REAL migrations/001-004 files (not a synthetic
    stand-in). A literal copy of the committed data/belgian_macro.db was
    deliberately not used as a test fixture -- every other test in this file
    builds its own minimal schema/data rather than depending on a ~2 MB
    binary, and a representative synthetic table exercises exactly the
    columns and constraints the real migration touches.
    """
    import tempfile

    # REAL_MIGRATIONS_DIR already contains 004; to insert representative
    # rows BEFORE it runs, build a filtered copy with every file except it,
    # apply that, seed the data, then add the real 004 file and re-run.
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        migdir = tmp_path / "migrations"
        migdir.mkdir()
        for version, path in migrate.discover_migrations(REAL_MIGRATIONS_DIR):
            if version == 4:
                continue
            (migdir / path.name).write_text(path.read_text())
        db_path = tmp_path / "test.db"
        migrate.run(db_path, migrations_dir=migdir)

        conn = migrate.connect(db_path)
        rows = [
            # geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id,
            # valid_from, valid_to, successor_geo_id, population, area_km2
            (
                "be:country",
                "01000",
                "country",
                "HET RIJK",
                "ROYAUME",
                "Belgium",
                None,
                "1830-01-01",
                None,
                None,
                11500000,
                30528.0,
            ),
            (
                "be:reg:02000",
                "02000",
                "region",
                "Vlaams Gewest",
                "Région flamande",
                "Flanders",
                "be:country",
                "1977-01-01",
                None,
                None,
                None,
                None,
            ),
            (
                "be:prov:10000",
                "10000",
                "province",
                "Provincie Antwerpen",
                "Province d'Anvers",
                "Antwerp",
                "be:reg:02000",
                "2025-01-01",
                None,
                None,
                None,
                None,
            ),
            (
                "be:arr:11000",
                "11000",
                "arrondissement",
                "Arrondissement Antwerpen",
                "Arrondissement d'Anvers",
                "Arrondissement Antwerpen",
                "be:prov:10000",
                "2025-01-01",
                None,
                None,
                None,
                None,
            ),
            # The successor FIRST -- a merger's "old" row's successor_geo_id
            # FK needs its target to already exist.
            (
                "be:mun:11057",
                "11057",
                "municipality",
                "Nieuw",
                "Nouveau",
                "New",
                "be:arr:11000",
                "2019-01-01",
                None,
                None,
                None,
                None,
            ),
            (
                "be:mun:11057-old",
                "11057-old",
                "municipality",
                "Oud",
                "Ancien",
                "Old",
                "be:arr:11000",
                "1977-01-01",
                "2019-01-01",
                "be:mun:11057",
                None,
                None,
            ),
        ]
        for row in rows:
            conn.execute(
                """INSERT INTO geographies
                   (geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id,
                    valid_from, valid_to, successor_geo_id, population, area_km2)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                row,
            )
        conn.commit()

        before = {
            r[0]: r
            for r in conn.execute(
                "SELECT geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id, "
                "valid_from, valid_to, successor_geo_id, population, area_km2 "
                "FROM geographies ORDER BY geo_id"
            )
        }
        conn.close()

        # Now apply 004 (copy the real file into this migdir and run again).
        real_004 = next(p for v, p in migrate.discover_migrations(REAL_MIGRATIONS_DIR) if v == 4)
        (migdir / real_004.name).write_text(real_004.read_text())
        migrate.run(db_path, migrations_dir=migdir)

        conn = migrate.connect(db_path)
        after = {
            r[0]: r
            for r in conn.execute(
                "SELECT geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id, "
                "valid_from, valid_to, successor_geo_id, population, area_km2 "
                "FROM geographies ORDER BY geo_id"
            )
        }
        assert after == before  # every row, every column, byte-for-byte identical
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        # And the whole point: 'nuts2' now works where it used to be refused.
        conn.execute("""INSERT INTO geographies
               (geo_id, level, name_nl, name_fr, name_en, valid_from)
               VALUES ('be21:nuts2', 'nuts2', 'x', 'x', 'x', '2024-01-01')""")
        conn.commit()
        conn.close()
