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

#: Belgian general-government finance (PR 1/2, feat/public-finance-data).
#: GOV_REVENUE_BE's own real Belgium-only response -- gov_10a_main,
#: na_item=TR, sector=S13, unit=MIO_EUR, geo=BE -- fetched live 2026-10-03,
#: same geo=BE-filtered shape as FIXTURE above. 2025's own cell carries
#: OBS_FLAG 'p' and value 314736.4, matching the maintainer-approved
#: catalogue entry (docs/data_catalog.md) byte for byte.
GOV_FIXTURE = REPO / "tests" / "fixtures" / "eurostat" / "gov_10a_main_TR_BE.json"

#: Captured at import time, before `one_real_eurostat_indicator` (autouse,
#: below) replaces `bmdb.SOURCES` for every test in this file -- by the time
#: a test function body runs, `bmdb.SOURCES` is already scoped down to
#: EC_CONS_CONF_BE alone, so GOV_REVENUE_BE's own entry has to be grabbed
#: here first.
_GOV_REVENUE_SOURCE = bmdb.SOURCES["GOV_REVENUE_BE"]

#: ADR 0018 (docs/decisions/0018-stopped-inflation-series.md, ACCEPTED
#: 2026-10-04): HICP_EUROSTAT_BE's own real response, prc_hicp_minr filtered
#: to unit=RCH_A, coicop18=TOTAL, geo=BE -- fetched live 2026-10-04 (same
#: real-fetch run this PR's own data regeneration used, not a synthetic
#: fixture). 225 rows, 2008-01 to 2026-09; the last cell carries OBS_FLAG
#: 'e' (2026-09 = 4.6, a flash estimate), matching the catalogue exactly.
_HICP_EUROSTAT_BE_SOURCE = bmdb.SOURCES["HICP_EUROSTAT_BE"]
HICP_FIXTURE = REPO / "tests" / "fixtures" / "eurostat" / "prc_hicp_minr_BE.json"


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


def test_a_public_finance_series_p_flag_lands_as_provisional_end_to_end(db, monkeypatch):
    """Audit P2-4: the 25 new Belgium-only public-finance configs (PR 1/2)
    went through this PR's own fixtures-only exporter tests, never through
    this real fetch_all -> sync_to_canonical path a live Eurostat 'p' flag
    actually takes. GOV_REVENUE_BE here stands in for all 25 -- same
    dataset family (gov_10a_main), same single-country fetch shape, same
    adapter code path as every other one of them."""
    fake_sources = {"GOV_REVENUE_BE": _GOV_REVENUE_SOURCE}
    monkeypatch.setattr(bmdb, "SOURCES", fake_sources)
    monkeypatch.setattr(sync_mod, "SOURCES", fake_sources)

    raw = GOV_FIXTURE.read_bytes()
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(raw))

    assert bmdb.fetch_all(db) is True

    row = db.conn.execute(
        "SELECT value, obs_status FROM legacy_observations "
        "WHERE indicator_code = 'GOV_REVENUE_BE' AND period = '2025'"
    ).fetchone()
    assert row is not None
    assert row[0] == 314736.4, "must match the live-verified catalogue value exactly, never coerced"
    assert row[1] == "P", f"legacy_observations.obs_status must be SDMX-lettered, got {row[1]!r}"
    db.close()

    checked, changed = sync_mod.sync(db.db_path, vintage="v1")
    assert checked > 0
    assert changed == checked

    conn = sqlite3.connect(str(db.db_path))
    try:
        canonical = conn.execute(
            "SELECT value, status FROM observations WHERE indicator_id = 'GOV_REVENUE_BE' "
            "AND geo_id = 'be:country' AND period = '2025'"
        ).fetchone()
    finally:
        conn.close()
    assert canonical is not None, "the canonical sync must have written GOV_REVENUE_BE's 2025 row"
    assert canonical == (314736.4, "provisional")


def test_hicp_eurostat_be_flash_estimate_lands_as_estimate_end_to_end(db, monkeypatch):
    """ADR 0018: HICP_EUROSTAT_BE's real prc_hicp_minr response, through the
    same national fetch_all -> sync_to_canonical path as every other
    single-country Eurostat indicator (unlike HICP_ANNUAL_RATE_EUROPE,
    which is multi-geo and goes through scripts/sync_international.py
    instead). The 'e' flag on 2026-09 (4.6, Eurostat's own flash estimate)
    must land as 'estimate' at both layers, and 2026-08 (a settled 'A' cell,
    4.2) as 'final' -- the exact two values the catalogue and ADR both name."""
    fake_sources = {"HICP_EUROSTAT_BE": _HICP_EUROSTAT_BE_SOURCE}
    monkeypatch.setattr(bmdb, "SOURCES", fake_sources)
    monkeypatch.setattr(sync_mod, "SOURCES", fake_sources)

    raw = HICP_FIXTURE.read_bytes()
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(raw))

    assert bmdb.fetch_all(db) is True

    legacy_rows = dict(
        db.conn.execute(
            "SELECT period, obs_status FROM legacy_observations "
            "WHERE indicator_code = 'HICP_EUROSTAT_BE' AND period IN ('2026-08', '2026-09')"
        )
    )
    assert legacy_rows == {"2026-08": "A", "2026-09": "E"}
    db.close()

    checked, changed = sync_mod.sync(db.db_path, vintage="v1")
    assert checked > 0
    assert changed == checked

    conn = sqlite3.connect(str(db.db_path))
    try:
        canonical = dict(
            conn.execute(
                "SELECT period, value || '|' || status FROM observations "
                "WHERE indicator_id = 'HICP_EUROSTAT_BE' AND geo_id = 'be:country' "
                "AND period IN ('2025-12', '2026-08', '2026-09')"
            )
        )
    finally:
        conn.close()
    assert canonical == {
        "2025-12": "2.2|final",
        "2026-08": "4.2|final",
        "2026-09": "4.6|estimate",
    }
