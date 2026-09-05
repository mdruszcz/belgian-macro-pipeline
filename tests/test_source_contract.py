"""
Contract test for TimeSeriesSource adapters (Block D). Parametrized over
NBBSource and EurostatSource -- not FPBSource, which deliberately keeps its
own shape; see docs/features/source_adapter.md, Non-goals.

Turns "every time-series adapter returns the same shape" from an aspiration
into a regression test: a future adapter that returns the wrong types fails
here, not as a downstream KeyError in upsert_observations.
"""

import json

import pytest

from src.fetchers.eurostat import EurostatSource
from src.fetchers.nbb import NBBSource

SDMX_CSV = "TIME_PERIOD,OBS_VALUE,OBS_STATUS\r\n2023-Q1,1.5,A\r\n"

DBNOMICS_JSON = json.dumps({"series": {"docs": [{"period": ["2020-Q1"], "value": [1.5]}]}}).encode()


class _FakeResponse:
    def __init__(self, content: bytes):
        self.content = content
        self.status_code = 200

    def raise_for_status(self):
        pass


ADAPTERS = [
    pytest.param(lambda: NBBSource(), SDMX_CSV.encode(), id="nbb"),
    pytest.param(lambda: EurostatSource(source_id="eurostat"), DBNOMICS_JSON, id="eurostat"),
]


@pytest.mark.parametrize("make_source,fixture_bytes", ADAPTERS)
def test_time_series_contract(tmp_path, monkeypatch, make_source, fixture_bytes):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(fixture_bytes)
    )

    rows = make_source().fetch("https://example.test/x", cache_key="X")

    assert rows, "fixture must produce at least one row to be a meaningful contract check"
    for row in rows:
        assert set(row.keys()) == {"period", "value", "obs_status"}
        assert isinstance(row["period"], str)
        assert isinstance(row["value"], float)
        assert isinstance(row["obs_status"], str)
