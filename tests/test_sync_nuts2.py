"""Tests for scripts/sync_nuts2.py -- the Europe NUTS 2 batch's regional
loader (Europe NUTS 2, batch B2, docs/features/europe_nuts2.md).

tests/fixtures/sync_nuts2/GDP_PC_PPS_NUTS2.json is a REAL Eurostat
nama_10r_2gdp response (fetched 2026-09-14), trimmed to four geo codes
(BE21, BE10, BEZZ, DE21) over three years (2022-2024) -- every other cell
removed, offsets recomputed so the trimmed cube is still internally
consistent. ONE cell is synthetic: BEZZ (extra-regio) never carries a real
value in the live nama_10r_2gdp response (confirmed 2026-09-14 against the
full response), so a value was added by hand at BEZZ/2024 -- the only way to
exercise the pseudo-region-skip path with this dataset at all. Every other
cell is Eurostat's own real number.
"""

import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import sync_nuts2 as sn2  # noqa: E402

from src.db import migrate  # noqa: E402

FIXTURES = REPO / "tests" / "fixtures" / "sync_nuts2"


@pytest.fixture
def db(tmp_path):
    import load_geography

    path = tmp_path / "test.db"
    migrate.run(path, migrations_dir=REPO / "migrations")
    load_geography.load(path, REPO / "config" / "geography", allow_unverified=True)
    return path


def _one_real_nuts2_indicator(monkeypatch, code: str) -> None:
    from src.validation.config_schema import load_and_validate_all

    indicators, _sources = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    monkeypatch.setattr(sn2, "nuts2_indicators", lambda _cfgs: {code: indicators[code]})


def test_reference_rows_only_needs_no_network(tmp_path):
    path = tmp_path / "test.db"
    migrate.run(path, migrations_dir=REPO / "migrations")

    fetched, changed = sn2.sync(path, reference_rows_only=True)

    assert (fetched, changed) == (0, 0)
    conn = sqlite3.connect(str(path))
    try:
        n_indicators = conn.execute(
            "SELECT COUNT(*) FROM indicators WHERE source_id = 'eurostat'"
        ).fetchone()[0]
        n_geo = conn.execute("SELECT COUNT(*) FROM geographies WHERE level = 'nuts2'").fetchone()[0]
    finally:
        conn.close()
    assert n_indicators == 3
    from src.geography.nuts2 import load_nuts2_rows

    assert n_geo == len(load_nuts2_rows())


def test_replaying_the_trimmed_real_fixture_writes_kept_regions_only(db, monkeypatch):
    """BE21, BE10, DE21 (real, allowlisted) are written; BEZZ (pseudo-region,
    even though it carries a value in this fixture) is not."""
    _one_real_nuts2_indicator(monkeypatch, "GDP_PC_PPS_NUTS2")

    fetched, changed = sn2.sync(db, from_dir=FIXTURES)

    assert fetched == 9  # 3 real regions x 3 years; BEZZ excluded
    assert changed == fetched
    conn = sqlite3.connect(str(db))
    try:
        geo_ids = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT geo_id FROM observations WHERE indicator_id = 'GDP_PC_PPS_NUTS2'"
            )
        }
        be10_2024 = conn.execute(
            "SELECT value, status FROM observations WHERE indicator_id = 'GDP_PC_PPS_NUTS2' "
            "AND geo_id = 'be10:nuts2' AND period = '2024'"
        ).fetchone()
    finally:
        conn.close()
    assert geo_ids == {"be21:nuts2", "be10:nuts2", "de21:nuts2"}
    assert be10_2024 == (76000.0, "provisional")


def test_a_second_replay_writes_no_new_vintage(db, monkeypatch):
    _one_real_nuts2_indicator(monkeypatch, "GDP_PC_PPS_NUTS2")
    sn2.sync(db, from_dir=FIXTURES)
    _fetched, changed = sn2.sync(db, from_dir=FIXTURES)
    assert changed == 0


def test_an_unknown_geography_fails_the_fetch_and_writes_nothing_for_it(db, tmp_path, monkeypatch):
    """A NUTS-2-shaped code neither allowlisted, excluded, nor a known
    aggregate must stop that indicator's fetch entirely -- no partial
    write, no silent drop."""
    _one_real_nuts2_indicator(monkeypatch, "GDP_PC_PPS_NUTS2")
    baddir = tmp_path / "bad_fixtures"
    baddir.mkdir()
    cube = {
        "id": ["unit", "geo", "time"],
        "size": [1, 2, 1],
        "dimension": {
            "unit": {"category": {"index": {"PPS_EU27_2020_HAB": 0}}},
            "geo": {"category": {"index": {"BE21": 0, "ZZ99": 1}}},
            "time": {"category": {"index": {"2023": 0}}},
        },
        "value": {"0": 1.0, "1": 2.0},
        "status": {},
    }
    (baddir / "GDP_PC_PPS_NUTS2.json").write_text(json.dumps(cube), encoding="utf-8")

    with pytest.raises(sn2.Nuts2GeographyError, match="ZZ99"):
        sn2.sync(db, from_dir=baddir)

    conn = sqlite3.connect(str(db))
    try:
        n = conn.execute(
            "SELECT COUNT(*) FROM observations WHERE indicator_id = 'GDP_PC_PPS_NUTS2'"
        ).fetchone()[0]
        status = conn.execute(
            "SELECT status FROM fetch_runs WHERE source_id = 'eurostat' ORDER BY fetch_run_id DESC"
        ).fetchall()
    finally:
        conn.close()
    assert n == 0
    assert status[0] == ("error",)


def test_a_licence_excluded_geography_is_skipped_not_written(db, tmp_path, monkeypatch):
    _one_real_nuts2_indicator(monkeypatch, "GDP_PC_PPS_NUTS2")
    fixdir = tmp_path / "uk_fixture"
    fixdir.mkdir()
    cube = {
        "id": ["unit", "geo", "time"],
        "size": [1, 2, 1],
        "dimension": {
            "unit": {"category": {"index": {"PPS_EU27_2020_HAB": 0}}},
            "geo": {"category": {"index": {"BE21": 0, "UKC1": 1}}},
            "time": {"category": {"index": {"2023": 0}}},
        },
        "value": {"0": 1.0, "1": 2.0},
        "status": {},
    }
    (fixdir / "GDP_PC_PPS_NUTS2.json").write_text(json.dumps(cube), encoding="utf-8")

    fetched, _changed = sn2.sync(db, from_dir=fixdir)

    assert fetched == 1  # BE21 only
    conn = sqlite3.connect(str(db))
    try:
        geo_ids = {row[0] for row in conn.execute("SELECT DISTINCT geo_id FROM observations")}
    finally:
        conn.close()
    assert "ukc1:nuts2" not in geo_ids
    assert not any(g.startswith("uk") for g in geo_ids)


def test_pilot_indicators_returns_exactly_the_country_level_indicators():
    """The one change made to sync_international.py for this batch --
    pilot_indicators() excluding geo_levels: [nuts2] configs -- must not
    change its result for the country-level indicators: the five pilot
    ones, plus GDP_PC_PPS_COUNTRY and POPULATION_COUNTRY added by the Europe
    countries batch (docs/features/europe_countries.md). Those two read the
    same datasets as the NUTS 2 configs but are country-level, so they
    belong here and never in nuts2_indicators()."""
    sys.path.insert(0, str(REPO / "scripts"))
    import sync_international as si

    from src.validation.config_schema import load_and_validate_all

    indicators, _sources = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    result = si.pilot_indicators(indicators)
    assert set(result) == {
        "GDP_VOLUME_EUROPE",
        "HICP_ANNUAL_RATE_EUROPE",
        "UNEMPLOYMENT_RATE_EUROPE",
        "GOV_DEBT_EUROPE",
        "CONSUMER_CONFIDENCE_EUROPE",
        "GDP_PC_PPS_COUNTRY",
        "POPULATION_COUNTRY",
    }


def test_nuts2_indicators_and_pilot_indicators_never_overlap():
    import sync_international as si

    from src.validation.config_schema import load_and_validate_all

    indicators, _sources = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    assert set(sn2.nuts2_indicators(indicators)) & set(si.pilot_indicators(indicators)) == set()
    assert set(sn2.nuts2_indicators(indicators)) == {
        "GDP_PC_PPS_NUTS2",
        "POPULATION_NUTS2",
        "UNEMPLOYMENT_RATE_NUTS2",
    }
