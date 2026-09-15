"""Tests for scripts/sync_international.py -- the international pilot's
direct-Eurostat, every-country-at-once indicators (international pilot PR 1;
extended to seven by the Europe countries batch, docs/features/europe_countries.md;
extended again to 25 by the Eurostat additional domains batch,
docs/data_catalog.md, 2026-09-15).

tests/fixtures/sync_international/*.json are REAL recorded Eurostat responses
(the original five fetched 2026-09-13, GDP_PC_PPS_COUNTRY/POPULATION_COUNTRY
and the 18 additional-domains indicators fetched 2026-09-15, one file per
pilot indicator's exact dataset+filters) -- replayed with --from-dir so these
tests need no network and exercise the real geography resolution against real
codes (EA/EA12/EA19/EA20/EU/EU28/EEA/UK/US/JP/XK/TR/FX/EEA31/EEA30_2007/EFTA/
AM/AZ/BY/RU/SM/AD/MC/CN_X_HK/KR/EA18 all appear in at least one of these 25
real responses; GDP_PC_PPS_COUNTRY/POPULATION_COUNTRY additionally exercise
is_country_level_code() against real NUTS 1/2/3 regional codes mixed into the
same responses).
"""

import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import sync_international as si  # noqa: E402

from src.db import migrate  # noqa: E402

FIXTURES = REPO / "tests" / "fixtures" / "sync_international"


@pytest.fixture
def db(tmp_path):
    import load_geography

    path = tmp_path / "test.db"
    migrate.run(path, migrations_dir=REPO / "migrations")
    load_geography.load(path, REPO / "config" / "geography", allow_unverified=True)
    return path


def test_reference_rows_only_needs_no_network_and_corrects_the_source_row(tmp_path):
    """The committed `sources` row for 'eurostat' predates this pilot and
    says adapter='dbnomics' -- this must correct it, not leave it stale."""
    path = tmp_path / "test.db"
    migrate.run(path, migrations_dir=REPO / "migrations")
    conn = sqlite3.connect(str(path))
    conn.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) VALUES "
        "('eurostat', 'Eurostat (old)', 'Eurostat', 'dbnomics', 'docs/data_catalog.md (pending)')"
    )
    conn.commit()
    conn.close()

    fetched, changed = si.sync(path, reference_rows_only=True)

    assert (fetched, changed) == (0, 0)
    conn = sqlite3.connect(str(path))
    try:
        adapter, catalog_ref = conn.execute(
            "SELECT adapter, catalog_ref FROM sources WHERE source_id = 'eurostat'"
        ).fetchone()
        assert adapter == "eurostat"
        assert catalog_ref != "docs/data_catalog.md (pending)"
        n_indicators = conn.execute(
            "SELECT COUNT(*) FROM indicators WHERE source_id = 'eurostat'"
        ).fetchone()[0]
        n_geo = conn.execute("SELECT COUNT(*) FROM geographies").fetchone()[0]
    finally:
        conn.close()
    assert n_indicators == 25
    assert n_geo == 43  # every row in config/geography/international.csv


def test_replaying_the_real_fixtures_writes_every_allowlisted_geography(db):
    fetched, changed = si.sync(db, from_dir=FIXTURES)

    assert fetched > 0 and changed == fetched
    conn = sqlite3.connect(str(db))
    try:
        geo_ids = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT geo_id FROM observations WHERE indicator_id = 'GDP_VOLUME_EUROPE'"
            )
        }
        indicator_ids = {
            row[0] for row in conn.execute("SELECT DISTINCT indicator_id FROM observations")
        }
    finally:
        conn.close()
    assert indicator_ids == {
        "GDP_VOLUME_EUROPE",
        "HICP_ANNUAL_RATE_EUROPE",
        "UNEMPLOYMENT_RATE_EUROPE",
        "GOV_DEBT_EUROPE",
        "CONSUMER_CONFIDENCE_EUROPE",
        "GDP_PC_PPS_COUNTRY",
        "POPULATION_COUNTRY",
        # Eurostat additional domains batch (docs/data_catalog.md, 2026-09-15).
        "VALUE_ADDED_TOTAL_EUROPE",
        "EMPLOYMENT_LFS_EUROPE",
        "GOV_BALANCE_EUROPE",
        "TAX_RECEIPTS_EUROPE",
        "GOV_EXPENDITURE_HEALTH_EUROPE",
        "GOV_DEBT_QUARTERLY_EUROPE",
        "EXPORTS_GOODS_SERVICES_EUROPE",
        "IMPORTS_GOODS_SERVICES_EUROPE",
        "GINI_COEFFICIENT_EUROPE",
        "POVERTY_RATE_EUROPE",
        "POVERTY_SOCIAL_EXCLUSION_EUROPE",
        "RENEWABLE_ENERGY_SHARE_EUROPE",
        "GHG_EMISSIONS_EUROPE",
        "POPULATION_EUROPE",
        "LIFE_EXPECTANCY_EUROPE",
        "POPULATION_GROWTH_RATE_EUROPE",
        "RD_EXPENDITURE_EUROPE",
        "RD_PERSONNEL_EUROPE",
    }
    # Belgium and Germany, both allowlisted and both present in namq_10_gdp.
    assert {"be:country", "de:country"} <= geo_ids
    # Neither pilot aggregate variant Eurostat also returns leaks in: EA
    # (legacy scope) and EA12/EA19/EA20/EU/EU28/EEA/UK/US/JP/XK/TR (excluded
    # or unrelated) must never appear as a written geo_id.
    assert "ea:aggregate" not in geo_ids


def test_a_second_replay_writes_no_new_vintage(db):
    si.sync(db, from_dir=FIXTURES)
    _fetched, changed = si.sync(db, from_dir=FIXTURES)
    assert changed == 0


def test_an_unknown_geography_fails_the_fetch_and_writes_nothing_for_it(db, tmp_path):
    """A code Eurostat returns that is neither allowlisted nor excluded must
    stop that indicator's fetch entirely -- no partial write, no silent
    drop -- and the exception must name the code."""
    import json
    import shutil

    baddir = tmp_path / "bad_fixtures"
    baddir.mkdir()
    for f in FIXTURES.glob("*.json"):
        shutil.copy2(f, baddir / f.name)

    cube = {
        "id": ["unit", "geo", "time"],
        "size": [1, 2, 1],
        "dimension": {
            "unit": {"category": {"index": {"PC_GDP": 0}}},
            "geo": {"category": {"index": {"BE": 0, "ZZ": 1}}},
            "time": {"category": {"index": {"2023": 0}}},
        },
        "value": {"0": 1.0, "1": 2.0},
        "status": {},
    }
    (baddir / "GOV_DEBT_EUROPE.json").write_text(json.dumps(cube), encoding="utf-8")

    with pytest.raises(si.SyncInternationalError, match="ZZ"):
        si.sync(db, from_dir=baddir)

    conn = sqlite3.connect(str(db))
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM observations WHERE indicator_id = 'GOV_DEBT_EUROPE'"
        ).fetchone()[0]
        status = conn.execute(
            "SELECT status FROM fetch_runs WHERE source_id = 'eurostat' ORDER BY fetch_run_id DESC"
        ).fetchall()
    finally:
        conn.close()
    assert n == 0
    assert status[0] == ("error",)


def test_ea_is_recognized_but_never_written_by_the_pilot(db):
    """namq_10_gdp and prc_hicp_manr both return a bare 'EA' row in the real
    fixtures -- allowlisted (scope legacy, for EUROSTAT_GDP_Q_MEUR_EA's
    national fetch) but the pilot must not write a second, ambiguous
    euro-area total beside EA21."""
    si.sync(db, from_dir=FIXTURES)
    conn = sqlite3.connect(str(db))
    try:
        geo_ids = {row[0] for row in conn.execute("SELECT DISTINCT geo_id FROM observations")}
    finally:
        conn.close()
    assert "ea:aggregate" not in geo_ids
    assert "ea21:aggregate" in geo_ids
    assert "eu27_2020:aggregate" in geo_ids


class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        pass


def _one_real_pilot_indicator(monkeypatch, code: str) -> None:
    """Restrict sync()'s loop to a single real pilot indicator config, so a
    test only needs to fake one dataset's response."""
    from src.validation.config_schema import load_and_validate_all

    indicators, _sources = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    monkeypatch.setattr(si, "pilot_indicators", lambda _cfgs: {code: indicators[code]})


def test_exactly_one_fetch_runs_row_per_indicator_via_base_fetch(db, monkeypatch):
    """Audit SHOULD-FIX 7: sync()'s own loop opens one fetch_runs row per
    indicator, but used to pass `conn=` into EurostatSource.fetch() too --
    DataSource.fetch()'s own `finally` block then logged a SECOND row for
    the same fetch. Exercised through the REAL base.fetch() path
    (requests.get monkeypatched), not --from-dir, which calls `_parse`
    directly and would never have seen this bug (the committed db had ten
    eurostat runs for five real fetches)."""
    _one_real_pilot_indicator(monkeypatch, "GOV_DEBT_EUROPE")
    raw = (FIXTURES / "GOV_DEBT_EUROPE.json").read_bytes()
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(raw))

    si.sync(db, from_dir=None)

    conn = sqlite3.connect(str(db))
    try:
        runs = conn.execute(
            "SELECT status FROM fetch_runs WHERE source_id = 'eurostat' AND adapter = 'eurostat'"
        ).fetchall()
    finally:
        conn.close()
    assert runs == [("ok",)], f"expected exactly one 'ok' fetch_runs row, got {runs}"


def test_exactly_one_fetch_runs_row_is_marked_error_on_failure_via_base_fetch(db, monkeypatch):
    """The failure-path twin: an unknown geography raises AFTER
    EurostatSource.fetch() has already returned successfully and (before the
    fix) already logged its own 'ok' row with a higher fetch_run_id than
    sync()'s own row -- so fetch_error, which reads the highest
    fetch_run_id per source, saw 'ok' even though the fetch genuinely
    failed. With the fix there is only ever sync()'s own row to read."""
    _one_real_pilot_indicator(monkeypatch, "GOV_DEBT_EUROPE")
    cube = json.dumps(
        {
            "id": ["unit", "geo", "time"],
            "size": [1, 2, 1],
            "dimension": {
                "unit": {"category": {"index": {"PC_GDP": 0}}},
                "geo": {"category": {"index": {"BE": 0, "ZZ": 1}}},
                "time": {"category": {"index": {"2023": 0}}},
            },
            "value": {"0": 1.0, "1": 2.0},
            "status": {},
        }
    ).encode()
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(cube))

    with pytest.raises(si.SyncInternationalError, match="ZZ"):
        si.sync(db, from_dir=None)

    conn = sqlite3.connect(str(db))
    try:
        runs = conn.execute(
            "SELECT status FROM fetch_runs WHERE source_id = 'eurostat' AND adapter = 'eurostat'"
        ).fetchall()
    finally:
        conn.close()
    assert runs == [("error",)], f"expected exactly one 'error' fetch_runs row, got {runs}"


def test_pilot_indicators_excludes_the_eight_single_country_configs():
    from src.validation.config_schema import load_and_validate_all

    indicators, _sources = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    pilots = si.pilot_indicators(indicators)
    assert "EUROSTAT_GDP_Q_MEUR" not in pilots
    assert "EC_CONS_CONF_BE" not in pilots
    assert "GDP_VOLUME_EUROPE" in pilots
    assert "GDP_PC_PPS_COUNTRY" in pilots
    assert "POPULATION_COUNTRY" in pilots
    # Eurostat additional domains batch (docs/data_catalog.md, 2026-09-15):
    # 18 more country-level indicators, still picked up by the same
    # discriminator (source_id: eurostat, fetch.geographies: allowlist).
    assert "VALUE_ADDED_TOTAL_EUROPE" in pilots
    assert "RD_PERSONNEL_EUROPE" in pilots
    # Dropped candidates (live adapter gap, see docs/data_catalog.md) must
    # never silently reappear here.
    assert "EMPLOYMENT_NATACCOUNTS_EUROPE" not in pilots
    assert "LABOUR_PRODUCTIVITY_GROWTH_EUROPE" not in pilots
    assert "FERTILITY_RATE_EUROPE" not in pilots
    # And a NUTS 2 config, even though it also matches "eurostat +
    # allowlist", must still be excluded (geo_levels: [nuts2]).
    assert "VALUE_ADDED_GROWTH_NUTS2" not in pilots
    assert len(pilots) == 25


@pytest.mark.parametrize(
    "code,period,expected",
    [
        # Eurostat additional domains batch (docs/data_catalog.md, 2026-09-15):
        # a handful of real-data sanity checks, one per domain group, against
        # the exact Belgian figure verified live before the batch's own
        # handoff was written -- these are raw ingested Eurostat values, not
        # computed, so a ballpark real-data check is enough (no hand-computed
        # expected value the way a derived statistic would need).
        ("GINI_COEFFICIENT_EUROPE", "2022", 24.7),
        ("POVERTY_RATE_EUROPE", "2022", 13.1),
        ("GOV_BALANCE_EUROPE", "2022", -3.5),
        ("POPULATION_EUROPE", "2024", 11817096),
        ("RD_EXPENDITURE_EUROPE", "2022", 3.21),
    ],
)
def test_a_real_belgian_value_lands_correctly(db, monkeypatch, code, period, expected):
    _one_real_pilot_indicator(monkeypatch, code)

    si.sync(db, from_dir=FIXTURES)

    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute(
            "SELECT value FROM observations WHERE indicator_id = ? AND geo_id = 'be:country' "
            "AND period = ?",
            (code, period),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None, f"no {code} row for be:country/{period}"
    assert row[0] == pytest.approx(expected, rel=1e-6)
