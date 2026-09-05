from pathlib import Path

import pytest

from belgian_macro_db import MacroDatabase

INDICATOR_META = {
    "name": "Test Indicator",
    "frequency": "A",
    "unit": "percent_yy",
    "source_agency": "TEST",
    "description": "A test indicator",
    "url": "https://example.test",
}


@pytest.fixture
def db(tmp_path: Path) -> MacroDatabase:
    database = MacroDatabase(tmp_path / "test.db")
    yield database
    database.close()


def test_upsert_observations_then_get_latest(db):
    db.upsert_indicator("TEST_IND", INDICATOR_META)
    db.upsert_observations(
        "TEST_IND",
        [
            {"period": "2020", "value": 1.0, "obs_status": "A"},
            {"period": "2021", "value": 2.0, "obs_status": "A"},
        ],
    )
    latest = db.get_latest("TEST_IND")
    assert latest["period"] == "2021"
    assert latest["value"] == 2.0


def test_upsert_observations_overwrites_existing_period(db):
    db.upsert_indicator("TEST_IND", INDICATOR_META)
    db.upsert_observations("TEST_IND", [{"period": "2020", "value": 1.0, "obs_status": "A"}])
    db.upsert_observations("TEST_IND", [{"period": "2020", "value": 5.0, "obs_status": "R"}])
    latest = db.get_latest("TEST_IND")
    assert latest["value"] == 5.0
    assert latest["obs_status"] == "R"


def test_get_latest_returns_none_for_unknown_indicator(db):
    assert db.get_latest("DOES_NOT_EXIST") is None


def test_log_fetch_records_status(db):
    db.log_fetch("TEST_IND", 3, "OK")
    db.log_fetch("TEST_IND", 0, "ERROR", "boom")
    history = db.get_fetch_history()
    assert history[0]["status"] == "ERROR"
    assert history[0]["msg"] == "boom"
    assert history[1]["status"] == "OK"


def test_upsert_forecasts_then_get_all(db):
    n = db.upsert_forecasts(
        [
            {"institution": "FPB", "indicator": "GDP_VOL", "year": "2026", "value": 1.5},
            {"institution": "FPB", "indicator": "GDP_VOL", "year": "2027", "value": 1.8},
        ]
    )
    assert n == 2
    fc = db.get_all_forecasts()
    assert len(fc) == 2
    assert set(fc["year"]) == {"2026", "2027"}
