import sqlite3
from pathlib import Path

import requests

from src.db import migrate
from src.fetchers.nbb import NBB_CSV_HEADER, NBBSource

SDMX_CSV = (
    "TIME_PERIOD,OBS_VALUE,OBS_STATUS\r\n"
    "2023-Q1,1.5,A\r\n"
    "2023-Q2,1.8,A\r\n"
    "2023-Q2,1.9,P\r\n"  # duplicate period -- last row wins, no warning
    "2023-Q3,,A\r\n"  # blank value -- skipped
    "2023-Q4,not_a_number,A\r\n"  # unparseable -- skipped
)


class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        pass


def test_nbb_source_parses_sdmx_csv_identically_to_before_the_refactor(tmp_path, monkeypatch):
    """NBBFetcher.fetch's original behaviour, preserved verbatim in
    NBBSource._parse: per-period dedup keeping the last row, blank/unparseable
    rows silently skipped, sorted by period."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    captured_headers = {}

    def fake_get(url, timeout, headers):
        captured_headers.update(headers)
        return _FakeResponse(SDMX_CSV.encode())

    monkeypatch.setattr("src.fetchers.base.requests.get", fake_get)

    rows = NBBSource().fetch("https://example.test/nbb", cache_key="TEST_IND")

    assert rows == [
        {"period": "2023-Q1", "value": 1.5, "obs_status": "A"},
        {"period": "2023-Q2", "value": 1.9, "obs_status": "P"},
    ]
    assert captured_headers == NBB_CSV_HEADER


def test_nbb_source_reports_pre_dedup_rows_read(tmp_path, monkeypatch):
    """5 raw CSV rows in the fixture (one duplicate period, one blank value,
    one unparseable value) collapse to 2 written rows -- rows_read must show
    the raw count, not silently equal rows_written."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(SDMX_CSV.encode())
    )
    migrations_dir = Path(__file__).resolve().parents[1] / "migrations"
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=migrations_dir)
    conn = sqlite3.connect(str(db_path))

    NBBSource().fetch("https://example.test/nbb", cache_key="TEST_IND", conn=conn)

    rows_read, rows_written = conn.execute(
        "SELECT rows_read, rows_written FROM fetch_runs"
    ).fetchone()
    assert (rows_read, rows_written) == (5, 2)


def test_nbb_source_propagates_http_errors(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: (_ for _ in ()).throw(requests.exceptions.ConnectionError("down")),
    )
    monkeypatch.setattr("src.fetchers.base.time.sleep", lambda *_: None)

    try:
        NBBSource().fetch("https://example.test/nbb", cache_key="TEST_IND")
        raise AssertionError("expected ConnectionError")
    except requests.exceptions.ConnectionError:
        pass
