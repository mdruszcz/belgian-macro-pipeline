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
    def fake_fetch(url):
        if "fail" in url:
            raise ValueError("simulated source failure")
        return [{"period": "2020", "value": 1.0, "obs_status": "A"}]

    monkeypatch.setattr(bmdb.NBBFetcher, "fetch", staticmethod(fake_fetch))
    monkeypatch.setattr(bmdb.FPBFetcher, "fetch", staticmethod(lambda: []))

    assert bmdb.fetch_all(db) is False
    history = {e["code"]: e["status"] for e in db.get_fetch_history()}
    assert history["OK_IND"] == "OK"
    assert history["FAIL_IND"] == "ERROR"


def test_fetch_all_returns_true_when_everything_succeeds(db, monkeypatch):
    monkeypatch.setattr(
        bmdb.NBBFetcher,
        "fetch",
        staticmethod(lambda url: [{"period": "2020", "value": 1.0, "obs_status": "A"}]),
    )
    monkeypatch.setattr(bmdb.FPBFetcher, "fetch", staticmethod(lambda: []))

    assert bmdb.fetch_all(db) is True


def test_fetch_all_returns_false_when_forecasts_fail(db, monkeypatch):
    monkeypatch.setattr(
        bmdb.NBBFetcher,
        "fetch",
        staticmethod(lambda url: [{"period": "2020", "value": 1.0, "obs_status": "A"}]),
    )

    def fake_fpb_fetch():
        raise ValueError("simulated FPB failure")

    monkeypatch.setattr(bmdb.FPBFetcher, "fetch", staticmethod(fake_fpb_fetch))

    assert bmdb.fetch_all(db) is False
    history = {e["code"]: e["status"] for e in db.get_fetch_history()}
    assert history["FPB_FORECASTS"] == "ERROR"
