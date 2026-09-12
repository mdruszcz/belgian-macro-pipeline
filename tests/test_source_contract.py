"""
Contract test for TimeSeriesSource adapters (Block D). Parametrized over
NBBSource and EurostatSource -- not FPBSource, which deliberately keeps its
own shape; see docs/features/source_adapter.md, Non-goals.

Turns "every time-series adapter returns the same shape" from an aspiration
into a regression test: a future adapter that returns the wrong types fails
here, not as a downstream KeyError in upsert_observations.
"""

import json
import sqlite3
import sys
from pathlib import Path

import pytest

from src.fetchers.eurostat import EurostatSource
from src.fetchers.nbb import NBBSource
from src.fetchers.walstat import WalStatSource

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

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


# --- the municipal contract ---------------------------------------------------
#
# A third shape (docs/features/source_adapter.md): one fetch returns many
# geographies, each row a real geo_id resolved for its period. WalStat is the
# first adapter whose rows carry a NIS code, so its case runs against a real
# geography load; StatbelSource resolves by name and keeps its own tests.

WALSTAT_JSON = json.dumps(
    [
        {
            "ins": "92094",
            "type_entite": "Commune",
            "entite": "Namur",
            "periode": "année 2024",
            "valeur": "2319.7",
        },
        {
            "ins": "52011",
            "type_entite": "Commune",
            "entite": "Charleroi",
            "periode": "Année 2024",
            "valeur": "3154.5",
        },
    ],
    ensure_ascii=False,
).encode("utf-8")


@pytest.fixture(scope="module")
def geo_conn(tmp_path_factory):
    import load_geography

    from src.db import migrate

    db = tmp_path_factory.mktemp("contract") / "geo.db"
    migrate.run(db, migrations_dir=REPO / "migrations")
    load_geography.load(db, REPO / "config" / "geography", allow_unverified=True)
    conn = sqlite3.connect(str(db))
    yield conn
    conn.close()


def test_municipal_time_series_contract_walstat(tmp_path, monkeypatch, geo_conn):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(WALSTAT_JSON)
    )

    rows = WalStatSource().fetch(
        "https://example.test/x", cache_key="X", geo_conn=geo_conn, reconcile=False
    )

    assert rows, "fixture must produce at least one row to be a meaningful contract check"
    for row in rows:
        assert set(row.keys()) == {"geo_id", "period", "value", "status"}
        assert isinstance(row["geo_id"], str) and row["geo_id"].startswith("be:mun:")
        assert isinstance(row["period"], str)
        assert isinstance(row["value"], float)
        assert row["status"] in {"final", "provisional", "estimate", "revised", "suppressed", "na"}
    # The raw response was cached before parsing, the base class's own promise.
    assert list(tmp_path.rglob("X.json")), "raw response not cached under RAW_CACHE_DIR"
