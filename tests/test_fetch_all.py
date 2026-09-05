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
