"""End-to-end regression test for BLOCKER 1 (audit): belgian_macro_db.fetch_all's
eurostat branch used to write CANONICAL status words ("final", "provisional", ...)
into legacy_observations.obs_status, but scripts/sync_to_canonical.py reads that
column back through port_existing_indicators.map_obs_status(), which only
accepts SDMX letters -- so it raised `Unrecognized SDMX OBS_STATUS code 'final'`
and aborted the whole canonical sync for every fetch after the first eurostat
row, on every single daily run.

tests/fixtures/eurostat/ei_bssi_m_r2_BE.json is EC_CONS_CONF_BE's own real
dataset+filters (ei_bssi_m_r2, indic=BS-CSMCI-BAL, s_adj=SA, geo=BE), reduced
to Belgium only from the real recorded multi-country response
(tests/fixtures/sync_international/CONSUMER_CONFIDENCE_EUROPE.json) -- exactly
the shape a `geo=BE`-filtered live call returns, not a synthetic fixture.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import sync_to_canonical as sync_mod  # noqa: E402

import belgian_macro_db as bmdb  # noqa: E402
from belgian_macro_db import MacroDatabase  # noqa: E402

FIXTURE = REPO / "tests" / "fixtures" / "eurostat" / "ei_bssi_m_r2_BE.json"


class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        pass


@pytest.fixture
def db(tmp_path: Path) -> MacroDatabase:
    database = MacroDatabase(tmp_path / "test.db")
    yield database
    database.close()


@pytest.fixture(autouse=True)
def one_real_eurostat_indicator(monkeypatch):
    """EC_CONS_CONF_BE's own real SOURCES entry (config-derived, not
    hand-typed), alone -- keeps the test fast without fetching every national
    indicator."""
    fake_sources = {"EC_CONS_CONF_BE": bmdb.SOURCES["EC_CONS_CONF_BE"]}
    monkeypatch.setattr(bmdb, "SOURCES", fake_sources)
    monkeypatch.setattr(sync_mod, "SOURCES", fake_sources)
    monkeypatch.setattr(bmdb.FPBSource, "fetch", lambda self, url, *, cache_key, conn=None: [])


def test_fetch_all_writes_sdmx_lettered_status_to_the_legacy_table(db, monkeypatch):
    raw = FIXTURE.read_bytes()
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(raw))

    assert bmdb.fetch_all(db) is True

    row = db.conn.execute(
        "SELECT obs_status FROM legacy_observations WHERE indicator_code = 'EC_CONS_CONF_BE' "
        "LIMIT 1"
    ).fetchone()
    assert row is not None
    assert row[0] in {
        "A",
        "P",
        "E",
        "B",
        "M",
        "S",
    }, f"legacy_observations.obs_status must be SDMX-lettered, got {row[0]!r}"


def test_fetch_all_then_sync_to_canonical_does_not_raise_and_writes_final(
    db, monkeypatch, tmp_path
):
    raw = FIXTURE.read_bytes()
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(raw))
    assert bmdb.fetch_all(db) is True
    db.close()

    # sync_to_canonical.sync() opens its own connection on the same file.
    checked, changed = sync_mod.sync(db.db_path, vintage="v1")

    assert checked > 0
    assert changed == checked

    conn = sqlite3.connect(str(db.db_path))
    try:
        rows = conn.execute(
            "SELECT period, value, status FROM observations "
            "WHERE indicator_id = 'EC_CONS_CONF_BE' AND geo_id = 'be:country' "
            "ORDER BY period"
        ).fetchall()
    finally:
        conn.close()
    assert rows, "the canonical sync must have written EC_CONS_CONF_BE's rows"
    assert all(status == "final" for _period, _value, status in rows)
