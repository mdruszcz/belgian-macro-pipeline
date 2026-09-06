"""Tests for get_observations(as_of=...) -- Block I, docs/features/vintages.md.

A fixture with one cell revised twice, asserted from BOTH time perspectives
(before and after each revision), plus the two traps the spec calls out by
name: as_of compared as a parsed timestamp rather than a string, and a bare
date meaning the end of that day rather than midnight.
"""

import sqlite3
from pathlib import Path

import pytest

from src.db import migrate
from src.db.observations import get_observations
from src.db.vintages import upsert_observation

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
        "VALUES ('UNEMPLOYMENT_RATE','s','a','b','c','M','percent','lower_is_better',0,'x')"
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


def _write(conn, value, vintage):
    upsert_observation(
        conn,
        indicator_id="UNEMPLOYMENT_RATE",
        geo_id="be:mun:A",
        period="2026-03",
        period_start="2026-03-01",
        period_end="2026-03-31",
        value=value,
        status="final",
        vintage=vintage,
        fetch_run_id=1,
    )


@pytest.fixture
def revised(conn):
    """A council report quoted 7.2% on 2026-03-10. Statbel revised it to
    7.4% on 2026-12-01. Both facts must remain queryable forever."""
    _write(conn, 7.2, "2026-03-10T09:00:00+00:00")
    _write(conn, 7.4, "2026-12-01T09:00:00+00:00")
    return conn


# ── The core promise: reproduce a past report ───────────────────────────────


def test_as_of_before_the_revision_returns_the_march_figure(revised):
    obs = get_observations(revised, indicator_id="UNEMPLOYMENT_RATE", as_of="2026-06-01")
    assert len(obs) == 1
    assert obs[0].value == 7.2


def test_as_of_after_the_revision_returns_the_december_figure(revised):
    obs = get_observations(revised, indicator_id="UNEMPLOYMENT_RATE", as_of="2027-01-01")
    assert len(obs) == 1
    assert obs[0].value == 7.4


def test_is_latest_returns_the_revised_figure_regardless_of_when_asked(revised):
    """is_latest is never a function of when you ask -- only as_of is."""
    obs = get_observations(revised, indicator_id="UNEMPLOYMENT_RATE")
    assert len(obs) == 1
    assert obs[0].value == 7.4
    assert obs[0].is_latest is True


# ── The cell-did-not-exist-yet case ──────────────────────────────────────────


def test_as_of_earlier_than_every_vintage_returns_nothing(revised):
    """Not the earliest row -- nothing. The cell did not exist on this date,
    and inventing a first-known value would be worse than reporting nothing."""
    obs = get_observations(revised, indicator_id="UNEMPLOYMENT_RATE", as_of="2026-01-01")
    assert obs == []


# ── The two traps the spec names ────────────────────────────────────────────


def test_a_bare_date_is_the_end_of_that_day(revised):
    """as_of='2026-03-10' must include the 09:00 write made THAT DAY --
    parsing it as midnight would exclude it, which is exactly the day a
    client's own report was generated on."""
    obs = get_observations(revised, indicator_id="UNEMPLOYMENT_RATE", as_of="2026-03-10")
    assert len(obs) == 1
    assert obs[0].value == 7.2


def test_a_non_utc_offset_still_orders_correctly(conn):
    """Every vintage in the real store ends '+00:00', so a naive string
    comparison happens to work. A row written with a different offset (here,
    +02:00, i.e. the same instant as 07:00 UTC) must still compare correctly
    against a plain UTC as_of -- proven by picking values where a LEXICAL
    comparison of the two strings would give the wrong order."""
    _write(conn, 7.2, "2026-03-10T09:00:00+02:00")  # == 2026-03-10T07:00:00 UTC
    _write(conn, 7.4, "2026-03-10T08:00:00+00:00")  # a later instant, lexically "smaller"
    obs = get_observations(
        conn, indicator_id="UNEMPLOYMENT_RATE", as_of="2026-03-10T23:59:59+00:00"
    )
    assert len(obs) == 1
    assert obs[0].value == 7.4, "the +00:00 row is chronologically later and must win"
