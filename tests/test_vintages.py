"""Tests for src.db.vintages.upsert_observation -- Block I, docs/features/vintages.md.

Not duplicating what each writer's own test suite already covers (they all
now call this same function), this file tests the function itself, plus the
specific defect the spec names by name: a status change at an unchanged
value must write a new vintage, which a value-only comparison would miss.
"""

import sqlite3
from pathlib import Path

import pytest

from src.db import migrate
from src.db.vintages import VintageCollisionError, upsert_observation

REPO = Path(__file__).resolve().parents[1]
REAL_MIGRATIONS_DIR = REPO / "migrations"


@pytest.fixture
def conn(tmp_path):
    db = tmp_path / "test.db"
    migrate.run(db, migrations_dir=REAL_MIGRATIONS_DIR)
    c = sqlite3.connect(str(db))
    c.execute("PRAGMA foreign_keys=ON")
    c.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) "
        "VALUES ('s','S','S','s','x')"
    )
    c.execute(
        "INSERT INTO indicators (indicator_id, source_id, name_nl, name_fr, name_en, "
        "frequency, unit, preferred_direction, is_additive, config_path) "
        "VALUES ('IND','s','a','b','c','A','count','contextual',1,'x')"
    )
    c.execute(
        "INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from) "
        "VALUES ('be:mun:A','municipality','A','A','A','1830-01-01')"
    )
    c.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('s','s','2026-01-01','ok')"
    )
    c.commit()
    return c


def _write(conn, value, status, vintage, **kw):
    return upsert_observation(
        conn,
        indicator_id="IND",
        geo_id="be:mun:A",
        period="2023",
        period_start="2023-01-01",
        period_end="2023-12-31",
        value=value,
        status=status,
        vintage=vintage,
        fetch_run_id=1,
        **kw,
    )


def test_a_brand_new_cell_writes_one_row(conn):
    assert _write(conn, 100.0, "final", "v1") == 1
    rows = conn.execute("SELECT value, status, is_latest FROM observations").fetchall()
    assert rows == [(100.0, "final", 1)]


def test_an_unchanged_value_and_status_writes_nothing(conn):
    _write(conn, 100.0, "final", "v1")
    assert _write(conn, 100.0, "final", "v2") == 0
    assert conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1


def test_a_changed_value_flips_is_latest_and_writes_a_new_row(conn):
    _write(conn, 100.0, "final", "v1")
    assert _write(conn, 105.0, "final", "v2") == 1
    rows = conn.execute("SELECT value, is_latest FROM observations ORDER BY vintage").fetchall()
    assert rows == [(100.0, 0), (105.0, 1)]


def test_a_status_change_at_an_unchanged_value_writes_a_new_vintage(conn):
    """The exact defect the spec names: a value-only comparison would treat
    a cell moving final -> suppressed at an unchanged number as unchanged,
    and the suppression would be silently lost."""
    _write(conn, 100.0, "final", "v1")
    assert _write(conn, None, "suppressed", "v2") == 1
    rows = conn.execute(
        "SELECT value, status, is_latest FROM observations ORDER BY vintage"
    ).fetchall()
    assert rows == [(100.0, "final", 0), (None, "suppressed", 1)]


def test_a_genuine_pk_collision_raises_rather_than_silently_drops(conn):
    _write(conn, 100.0, "final", "v1")
    with pytest.raises(VintageCollisionError, match="Vintage collision"):
        # Same vintage, different value: the UPDATE flips the old row, but
        # inserting a second row with an identical PK cannot succeed --
        # exactly the trap a bare-date vintage created before it was fixed.
        _write(conn, 999.0, "final", "v1")


def test_created_at_defaults_to_vintage(conn):
    _write(conn, 100.0, "final", "2026-01-01T00:00:00+00:00")
    row = conn.execute("SELECT vintage, created_at FROM observations").fetchone()
    assert row == ("2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00")


def test_created_at_can_be_given_separately_from_vintage(conn):
    """sync_to_canonical.py's own tests pass a synthetic vintage ('v1', 'v2')
    to control the primary key deterministically, while created_at stays a
    real timestamp -- this is what keeps that distinction working."""
    _write(conn, 100.0, "final", "v1", created_at="2026-03-01T00:00:00+00:00")
    row = conn.execute("SELECT vintage, created_at FROM observations").fetchone()
    assert row == ("v1", "2026-03-01T00:00:00+00:00")
