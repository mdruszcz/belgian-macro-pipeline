"""
NBBSource -- fetches NBB SDMX-CSV time series.

_parse is NBBFetcher.fetch's original body, unchanged: the HTTP GET and
raise_for_status() now live in DataSource._get_with_retry instead, but the
CSV-DictReader parsing, per-period dedup (last row for a period wins, no
warning) and blank/unparseable-row skipping are identical to before this
refactor -- verified by tests/test_nbb_source.py against a fixture response.
"""

import csv
import io

from src.fetchers.base import TimeSeriesSource

NBB_CSV_HEADER = {"Accept": "application/vnd.sdmx.data+csv;version=2.0.0"}


class NBBSource(TimeSeriesSource):
    source_id = "nbb"
    adapter = "nbb"
    raw_extension = "csv"

    def __init__(self):
        self._last_raw_row_count = 0

    def _request_headers(self) -> dict:
        return NBB_CSV_HEADER

    def _rows_read_hint(self, rows: list[dict]) -> int | None:
        """Reports the pre-dedup row count: the SDMX response can contain
        more raw rows than survive the per-period dedup below, and
        rows_read/rows_written diverging is itself useful fetch_runs signal
        (a revision landed, or a duplicate period appeared)."""
        return self._last_raw_row_count

    def _parse(self, raw: bytes, **kwargs) -> list[dict]:
        text = raw.decode("utf-8")
        seen: dict[str, dict] = {}
        raw_count = 0
        for row in csv.DictReader(io.StringIO(text)):
            raw_count += 1
            period = row.get("TIME_PERIOD", "").strip()
            value = row.get("OBS_VALUE", "").strip()
            status = row.get("OBS_STATUS", "").strip()
            if not period or not value:
                continue
            try:
                val = float(value)
            except ValueError:
                continue
            seen[period] = {"period": period, "value": val, "obs_status": status}
        self._last_raw_row_count = raw_count
        return sorted(seen.values(), key=lambda x: x["period"])
