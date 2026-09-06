"""Tests for scripts/revisions_report.py -- Block I, docs/features/vintages.md."""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from revisions_report import _parse_since, find_revisions  # noqa: E402

from src.db import migrate  # noqa: E402
from src.db.vintages import upsert_observation  # noqa: E402

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
    for ind in ("GDP", "POP"):
        c.execute(
            "INSERT INTO indicators (indicator_id, source_id, name_nl, name_fr, name_en, "
            "frequency, unit, preferred_direction, is_additive, config_path) "
            f"VALUES ('{ind}','s','a','b','c','A','count','contextual',1,'x')"
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


def _write(conn, indicator_id, value, vintage, status="final"):
    upsert_observation(
        conn,
        indicator_id=indicator_id,
        geo_id="be:mun:A",
        period="2026",
        period_start="2026-01-01",
        period_end="2026-12-31",
        value=value,
        status=status,
        vintage=vintage,
        fetch_run_id=1,
    )


def test_a_single_write_is_not_a_revision(conn):
    """New data, not a revision -- there is no prior vintage to differ from."""
    _write(conn, "GDP", 100.0, "2026-01-01T00:00:00+00:00")
    assert find_revisions(conn) == []


def test_a_changed_value_is_reported(conn):
    _write(conn, "GDP", 100.0, "2026-01-01T00:00:00+00:00")
    _write(conn, "GDP", 105.0, "2026-06-01T00:00:00+00:00")
    revisions = find_revisions(conn)
    assert len(revisions) == 1
    r = revisions[0]
    assert r["old_value"] == 100.0
    assert r["new_value"] == 105.0
    assert r["old_vintage"] == "2026-01-01T00:00:00+00:00"
    assert r["new_vintage"] == "2026-06-01T00:00:00+00:00"


def test_since_excludes_revisions_before_the_cutoff(conn):
    _write(conn, "GDP", 100.0, "2026-01-01T00:00:00+00:00")
    _write(conn, "GDP", 105.0, "2026-06-01T00:00:00+00:00")
    assert find_revisions(conn, since=_parse_since("2026-07-01")) == []
    assert len(find_revisions(conn, since=_parse_since("2026-05-01"))) == 1


def test_a_bare_since_date_includes_a_revision_written_that_same_day(conn):
    """The opposite rule from get_observations()'s as_of: --since must
    include what happened ON that day, so a bare date means its START, not
    its end."""
    _write(conn, "GDP", 100.0, "2026-01-01T00:00:00+00:00")
    _write(conn, "GDP", 105.0, "2026-06-01T09:00:00+00:00")
    assert len(find_revisions(conn, since=_parse_since("2026-06-01"))) == 1


def test_ordered_by_absolute_relative_change_descending(conn):
    """A 0.1% GDP revision and a 40% population revision are not equally
    interesting -- the bigger one must lead."""
    _write(conn, "GDP", 1000.0, "2026-01-01T00:00:00+00:00")
    _write(conn, "GDP", 1001.0, "2026-06-01T00:00:00+00:00")  # +0.1%
    _write(conn, "POP", 100.0, "2026-01-01T00:00:00+00:00")
    _write(conn, "POP", 140.0, "2026-06-01T00:00:00+00:00")  # +40%
    revisions = find_revisions(conn)
    assert [r["indicator_id"] for r in revisions] == ["POP", "GDP"]


def test_a_transition_with_no_computable_percentage_still_appears(conn):
    """A suppression (old_value None) has no percentage to rank by, but it
    must not disappear from the report -- it goes after the ranked ones."""
    _write(conn, "GDP", 100.0, "2026-01-01T00:00:00+00:00")
    _write(conn, "GDP", 105.0, "2026-06-01T00:00:00+00:00")
    _write(conn, "POP", 100.0, "2026-01-01T00:00:00+00:00")
    _write(conn, "POP", None, "2026-06-01T00:00:00+00:00", status="suppressed")
    revisions = find_revisions(conn)
    assert {r["indicator_id"] for r in revisions} == {"GDP", "POP"}
    assert revisions[-1]["indicator_id"] == "POP"
