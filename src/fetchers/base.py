"""
DataSource -- the shared fetch/cache/retry/log scaffolding every adapter
subclasses (Block D). See docs/features/source_adapter.md for the full design
and the reasoning behind each decision below; this module implements it.

Only `_parse()` varies per adapter. Everything else -- the HTTP GET with
retry, writing the raw response to data/raw/{source_id}/{date}/ before
anything touches it, and logging the attempt to `fetch_runs` -- is written
once here so a future adapter (Statbel, WalStat) costs an hour of subclassing
instead of a day of reimplementing all four.
"""

import logging
import sqlite3
import time
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger("fetchers")

REQUEST_TIMEOUT = 30
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = (1, 2, 4)

RAW_CACHE_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


class FetchError(Exception):
    """A fetch failed after exhausting retries, or the response could not be parsed."""


def _is_retryable(exc: Exception | None, status_code: int | None) -> bool:
    """Retry a transient failure; never retry a client error that will not
    succeed on a second attempt."""
    if isinstance(exc, requests.exceptions.RequestException):
        return True
    if status_code is not None and 500 <= status_code < 600:
        return True
    return False


class DataSource(ABC):
    """Template method: GET-with-retry -> cache raw -> parse -> log fetch_runs.

    Subclasses set three class attributes and implement `_parse`:
        source_id      -- matches sources.source_id (e.g. "nbb")
        adapter        -- matches sources.adapter / fetch_runs.adapter (e.g. "nbb")
        raw_extension  -- the cached file's extension: "csv" | "json" | "xlsx"
    """

    source_id: str
    adapter: str
    raw_extension: str

    def fetch(
        self,
        url: str,
        *,
        cache_key: str,
        conn: sqlite3.Connection | None = None,
        **parse_kwargs,
    ) -> list[dict]:
        started_at = datetime.now(timezone.utc).isoformat()
        status = "ok"
        message = ""
        http_status: int | None = None
        rows_written = 0
        rows_read: int | None = None

        try:
            raw, http_status = self._get_with_retry(url)
            self._cache_raw(raw, cache_key)
            rows = self._parse(raw, **parse_kwargs)
            rows_written = len(rows)
            rows_read = self._rows_read_hint(rows) or rows_written
            return rows
        except Exception as exc:
            status = "error"
            message = str(exc)
            raise
        finally:
            finished_at = datetime.now(timezone.utc).isoformat()
            if conn is not None:
                # Logging must never mask the real fetch/parse error: keep it
                # in its own try/except so a sqlite failure here cannot hide
                # the exception this `finally` block is already propagating.
                try:
                    self._log_run(
                        conn,
                        started_at=started_at,
                        finished_at=finished_at,
                        status=status,
                        rows_read=rows_read or 0,
                        rows_written=rows_written,
                        http_status=http_status,
                        message=message,
                    )
                except Exception:
                    log.exception("Failed to write fetch_runs row for %s", self.source_id)

    def _rows_read_hint(self, rows: list[dict]) -> int | None:
        """Overridden by adapters that can report a pre-dedup/pre-filter count
        cheaply; the default duplicates rows_written rather than fabricating
        a distinct number (docs/features/source_adapter.md, Assumptions)."""
        return None

    @abstractmethod
    def _parse(self, raw: bytes, **kwargs) -> list[dict]:
        """Adapter-specific parsing. Receives the raw response bytes exactly
        as cached to disk."""

    def _get_with_retry(self, url: str) -> tuple[bytes, int | None]:
        """GET with retry on connection-level failures and 5xx responses.

        `resp.raise_for_status()` raises `requests.exceptions.HTTPError`,
        which is itself a `RequestException` subclass -- catching that in the
        same except block as connection errors would retry a 404 three times,
        directly contradicting "never retry a client error" (caught by
        test_fetch_does_not_retry_a_client_error). So the network-level
        try/except wraps only the `requests.get()` call; the HTTP-status
        retry decision is a separate, explicit check on `resp.status_code`,
        and `raise_for_status()` is only ever reached once that decision has
        already been made not to retry -- so raising there is always terminal.
        """
        log.info("GET %s...", url[:90])
        for attempt in range(MAX_ATTEMPTS):
            try:
                resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=self._request_headers())
            except requests.exceptions.RequestException as exc:
                if attempt < MAX_ATTEMPTS - 1:
                    log.warning(
                        "%s fetching %s, retrying (attempt %d/%d)",
                        exc,
                        url[:60],
                        attempt + 1,
                        MAX_ATTEMPTS,
                    )
                    time.sleep(BACKOFF_SECONDS[attempt])
                    continue
                raise

            if _is_retryable(None, resp.status_code) and attempt < MAX_ATTEMPTS - 1:
                log.warning(
                    "HTTP %d from %s, retrying (attempt %d/%d)",
                    resp.status_code,
                    url[:60],
                    attempt + 1,
                    MAX_ATTEMPTS,
                )
                time.sleep(BACKOFF_SECONDS[attempt])
                continue

            resp.raise_for_status()
            return resp.content, resp.status_code

        raise FetchError(f"Exhausted retries fetching {url}")

    def _request_headers(self) -> dict:
        """Overridden by adapters that need a specific Accept header (NBB's
        SDMX-CSV negotiation)."""
        return {}

    def _cache_raw(self, raw: bytes, cache_key: str) -> Path:
        """Write the raw response before anything parses it. One file per
        (source, day, cache_key) -- overwritten on a same-day re-run, so the
        cache never grows unbounded across reruns of the same indicator.

        cache_key disambiguates calls that share a source_id: fetch() is
        invoked once per indicator for the time-series sources, so the
        indicator code is what actually distinguishes otherwise-identical
        (source_id, date) paths.
        """
        day_dir = RAW_CACHE_DIR / self.source_id / date.today().isoformat()
        day_dir.mkdir(parents=True, exist_ok=True)
        path = day_dir / f"{cache_key}.{self.raw_extension}"
        path.write_bytes(raw)
        return path

    def _log_run(
        self,
        conn: sqlite3.Connection,
        *,
        started_at: str,
        finished_at: str,
        status: str,
        rows_read: int,
        rows_written: int,
        http_status: int | None,
        message: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO fetch_runs
                (source_id, adapter, started_at, finished_at, status,
                 rows_read, rows_written, http_status, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self.source_id,
                self.adapter,
                started_at,
                finished_at,
                status,
                rows_read,
                rows_written,
                http_status,
                message,
            ),
        )
        conn.commit()


class TimeSeriesSource(DataSource):
    """Contract: `_parse` returns list[dict] of exactly
    {"period": str, "value": float, "obs_status": str}. NBBSource and
    EurostatSource implement this; tests/test_source_contract.py enforces it.
    """


class ForecastSource(DataSource):
    """Contract: `_parse` returns list[dict] of exactly
    {"institution": str, "indicator": str, "year": str,
     "value": float | None, "updated_at": str}. FPBSource implements this.

    Deliberately not the same contract as TimeSeriesSource -- see
    docs/features/source_adapter.md, Non-goals, for why forcing one shape
    across both would be a real behaviour change to the forecasts table.
    """
