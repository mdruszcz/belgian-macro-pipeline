from pathlib import Path

import pytest

import belgian_macro_db as bmdb
from belgian_macro_db import MacroDatabase


@pytest.fixture
def db(tmp_path: Path) -> MacroDatabase:
    database = MacroDatabase(tmp_path / "test.db")
    yield database
    database.close()


@pytest.fixture(autouse=True)
def small_sources(monkeypatch):
    monkeypatch.setattr(
        bmdb,
        "SOURCES",
        {
            "OK_IND": {
                "name": "OK",
                "frequency": "A",
                "unit": "percent_yy",
                "source_agency": "TEST",
                "type": "nbb",
                "url": "https://example.test/ok",
            },
            "FAIL_IND": {
                "name": "FAIL",
                "frequency": "A",
                "unit": "percent_yy",
                "source_agency": "TEST",
                "type": "nbb",
                "url": "https://example.test/fail",
            },
        },
    )


def test_fetch_all_returns_false_when_a_source_fails(db, monkeypatch):
    def fake_fetch(self, url, *, cache_key, conn=None, **kwargs):
        if "fail" in url:
            raise ValueError("simulated source failure")
        return [{"period": "2020", "value": 1.0, "obs_status": "A"}]

    monkeypatch.setattr(bmdb.NBBSource, "fetch", fake_fetch)
    monkeypatch.setattr(bmdb.FPBSource, "fetch", lambda self, url, *, cache_key, conn=None: [])

    assert bmdb.fetch_all(db) is False
    history = {e["code"]: e["status"] for e in db.get_fetch_history()}
    assert history["OK_IND"] == "OK"
    assert history["FAIL_IND"] == "ERROR"


def test_fetch_all_returns_true_when_everything_succeeds(db, monkeypatch):
    monkeypatch.setattr(
        bmdb.NBBSource,
        "fetch",
        lambda self, url, *, cache_key, conn=None: [
            {"period": "2020", "value": 1.0, "obs_status": "A"}
        ],
    )
    monkeypatch.setattr(bmdb.FPBSource, "fetch", lambda self, url, *, cache_key, conn=None: [])

    assert bmdb.fetch_all(db) is True


def test_fetch_all_returns_false_when_forecasts_fail(db, monkeypatch):
    monkeypatch.setattr(
        bmdb.NBBSource,
        "fetch",
        lambda self, url, *, cache_key, conn=None: [
            {"period": "2020", "value": 1.0, "obs_status": "A"}
        ],
    )

    def fake_fpb_fetch(self, url, *, cache_key, conn=None):
        raise ValueError("simulated FPB failure")

    monkeypatch.setattr(bmdb.FPBSource, "fetch", fake_fpb_fetch)

    assert bmdb.fetch_all(db) is False
    history = {e["code"]: e["status"] for e in db.get_fetch_history()}
    assert history["FPB_FORECASTS"] == "ERROR"


def test_fetch_all_does_not_orphan_the_fpb_fetch_runs_row(db, monkeypatch):
    """Regression test for a real production FK violation found 2026-09-05:
    FPBSource().fetch(conn=db.conn) always logs a fetch_runs row keyed
    source_id='fpb' via DataSource's own finally-block, win or lose, but
    'fpb' is deliberately excluded from the canonical `sources` table
    (adapter not in "nbb"/"dbnomics"). MacroDatabase never enables
    PRAGMA foreign_keys, so this had silently orphaned a row for a long time
    without ever raising -- exactly what pragma foreign_key_check is for.

    Deliberately does NOT monkeypatch FPBSource.fetch itself (unlike the
    other tests in this file) -- only its hook methods, _get_with_retry and
    _parse -- so the real DataSource.fetch() template method runs and its
    finally-block actually performs the fetch_runs INSERT this test exists
    to check. A monkeypatched no-op fetch would never touch fetch_runs and
    this check would be tautological."""
    monkeypatch.setattr(
        bmdb.NBBSource,
        "fetch",
        lambda self, url, *, cache_key, conn=None: [
            {"period": "2020", "value": 1.0, "obs_status": "A"}
        ],
    )
    monkeypatch.setattr(bmdb.FPBSource, "_get_with_retry", lambda self, url: (b"", 200))

    def _parse(self, raw, **kwargs):
        raise ValueError("simulated FPB parse failure")

    monkeypatch.setattr(bmdb.FPBSource, "_parse", _parse)

    assert bmdb.fetch_all(db) is False
    history = {e["code"]: e["status"] for e in db.get_fetch_history()}
    assert history["FPB_FORECASTS"] == "ERROR"

    fpb_row = db.conn.execute("SELECT status FROM fetch_runs WHERE source_id = 'fpb'").fetchone()
    assert fpb_row is not None, "the fetch_runs logging itself must have run"

    db.conn.execute("PRAGMA foreign_keys = ON")
    violations = db.conn.execute("PRAGMA foreign_key_check").fetchall()
    assert violations == [], f"fetch_runs.source_id='fpb' is an orphan: {violations}"

    source_row = db.conn.execute(
        "SELECT source_id, adapter FROM sources WHERE source_id = 'fpb'"
    ).fetchone()
    assert source_row == ("fpb", "fpb")


def test_ensure_fpb_source_row_is_idempotent(db):
    bmdb._ensure_fpb_source_row(db.conn)
    bmdb._ensure_fpb_source_row(db.conn)
    count = db.conn.execute("SELECT COUNT(*) FROM sources WHERE source_id = 'fpb'").fetchone()[0]
    assert count == 1
