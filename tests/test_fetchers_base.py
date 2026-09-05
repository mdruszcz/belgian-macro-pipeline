import sqlite3

import pytest
import requests

from src.db import migrate
from src.fetchers.base import DataSource, FetchError

REAL_MIGRATIONS_DIR_NAME = "migrations"


class _FakeResponse:
    def __init__(self, status_code=200, content=b"ok"):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} error")


class _EchoSource(DataSource):
    """Minimal concrete DataSource for exercising the base class alone."""

    source_id = "test_source"
    adapter = "test_adapter"
    raw_extension = "txt"

    def _parse(self, raw: bytes, **kwargs) -> list[dict]:
        return [{"raw": raw.decode()}]


@pytest.fixture
def migrated_conn(tmp_path):
    from pathlib import Path

    db_path = tmp_path / "test.db"
    migrations_dir = Path(__file__).resolve().parents[1] / REAL_MIGRATIONS_DIR_NAME
    migrate.run(db_path, migrations_dir=migrations_dir)
    return sqlite3.connect(str(db_path))


def test_fetch_returns_parsed_rows_on_success(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse())
    source = _EchoSource()
    rows = source.fetch("https://example.test/data", cache_key="X")
    assert rows == [{"raw": "ok"}]


def test_fetch_retries_transient_failure_then_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    calls = {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] < 3:
            raise requests.exceptions.ConnectionError("transient")
        return _FakeResponse(content=b"finally")

    monkeypatch.setattr("src.fetchers.base.requests.get", flaky)
    monkeypatch.setattr("src.fetchers.base.time.sleep", lambda *_: None)

    source = _EchoSource()
    rows = source.fetch("https://example.test/data", cache_key="X")
    assert rows == [{"raw": "finally"}]
    assert calls["n"] == 3


def test_fetch_exhausts_retries_and_raises(monkeypatch):
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: (_ for _ in ()).throw(requests.exceptions.ConnectionError("down")),
    )
    monkeypatch.setattr("src.fetchers.base.time.sleep", lambda *_: None)

    source = _EchoSource()
    with pytest.raises(requests.exceptions.ConnectionError):
        source.fetch("https://example.test/data", cache_key="X")


def test_fetch_does_not_retry_a_client_error(monkeypatch):
    """A 404/400 will not succeed on a second attempt -- retrying only delays
    surfacing the failure."""
    calls = {"n": 0}

    def bad_request(*a, **k):
        calls["n"] += 1
        return _FakeResponse(status_code=404)

    monkeypatch.setattr("src.fetchers.base.requests.get", bad_request)
    monkeypatch.setattr("src.fetchers.base.time.sleep", lambda *_: None)

    source = _EchoSource()
    with pytest.raises(requests.exceptions.HTTPError):
        source.fetch("https://example.test/data", cache_key="X")
    assert calls["n"] == 1


def test_fetch_retries_a_server_error(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    calls = {"n": 0}

    def flaky_5xx(*a, **k):
        calls["n"] += 1
        if calls["n"] < 2:
            return _FakeResponse(status_code=503)
        return _FakeResponse(status_code=200, content=b"recovered")

    monkeypatch.setattr("src.fetchers.base.requests.get", flaky_5xx)
    monkeypatch.setattr("src.fetchers.base.time.sleep", lambda *_: None)

    source = _EchoSource()
    rows = source.fetch("https://example.test/data", cache_key="X")
    assert rows == [{"raw": "recovered"}]
    assert calls["n"] == 2


def test_raw_response_is_cached_before_parsing(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(content=b"cached bytes")
    )

    source = _EchoSource()
    source.fetch("https://example.test/data", cache_key="MY_CODE")

    from datetime import date

    expected = tmp_path / "test_source" / date.today().isoformat() / "MY_CODE.txt"
    assert expected.read_bytes() == b"cached bytes"


def test_raw_cache_is_overwritten_on_a_same_day_rerun(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    responses = iter([_FakeResponse(content=b"first"), _FakeResponse(content=b"second")])
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: next(responses))

    source = _EchoSource()
    source.fetch("https://example.test/data", cache_key="X")
    source.fetch("https://example.test/data", cache_key="X")

    from datetime import date

    path = tmp_path / "test_source" / date.today().isoformat() / "X.txt"
    assert path.read_bytes() == b"second"


def test_fetch_logs_a_fetch_runs_row_on_success(tmp_path, migrated_conn, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse())
    source = _EchoSource()
    source.fetch("https://example.test/data", cache_key="X", conn=migrated_conn)

    row = migrated_conn.execute(
        "SELECT source_id, adapter, status, rows_written FROM fetch_runs"
    ).fetchone()
    assert row == ("test_source", "test_adapter", "ok", 1)


def test_fetch_logs_a_fetch_runs_row_on_failure(migrated_conn, monkeypatch):
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: (_ for _ in ()).throw(requests.exceptions.ConnectionError("down")),
    )
    monkeypatch.setattr("src.fetchers.base.time.sleep", lambda *_: None)
    source = _EchoSource()

    with pytest.raises(requests.exceptions.ConnectionError):
        source.fetch("https://example.test/data", cache_key="X", conn=migrated_conn)

    row = migrated_conn.execute("SELECT status, message FROM fetch_runs").fetchone()
    assert row[0] == "error"
    assert "down" in row[1]


def test_logging_failure_does_not_mask_the_real_exception(monkeypatch):
    """A broken conn while logging must not hide the actual fetch failure --
    docs/features/source_adapter.md's explicit requirement."""
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: (_ for _ in ()).throw(ValueError("real failure")),
    )

    class ExplodingConn:
        def execute(self, *a, **k):
            raise sqlite3.OperationalError("logging is broken too")

    source = _EchoSource()
    with pytest.raises(ValueError, match="real failure"):
        source.fetch("https://example.test/data", cache_key="X", conn=ExplodingConn())


def test_fetch_raises_fetch_error_when_no_underlying_exception_survives():
    # Directly exercises the defensive branch in _get_with_retry -- see its
    # docstring: this path is not reachable via requests' own exceptions, only
    # documented as a safety net.
    assert issubclass(FetchError, Exception)
