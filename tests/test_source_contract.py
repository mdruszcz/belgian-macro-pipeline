"""
Contract test for TimeSeriesSource adapters (Block D). Parametrized over
NBBSource and DBnomicsSource -- not FPBSource, which deliberately keeps its
own shape; see docs/features/source_adapter.md, Non-goals. EurostatSource
(the direct-Eurostat adapter, international pilot PR 1) implements a third,
different shape, MultiGeoTimeSeriesSource, checked separately below -- it
returns many geographies from one fetch, so period/value/obs_status alone is
not its contract.

Turns "every time-series adapter returns the same shape" from an aspiration
into a regression test: a future adapter that returns the wrong types fails
here, not as a downstream KeyError in upsert_observations.
"""

import io
import json
import sqlite3
import sys
import zipfile
from pathlib import Path

import pytest

from src.fetchers.bankruptcies import BankruptciesSource
from src.fetchers.dbnomics import DBnomicsSource
from src.fetchers.eurostat import EurostatSource
from src.fetchers.nbb import NBBSource
from src.fetchers.population_movement import PopulationMovementSource
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
    pytest.param(lambda: DBnomicsSource(source_id="ameco_ec"), DBNOMICS_JSON, id="dbnomics"),
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


# --- the multi-geo contract ----------------------------------------------------
#
# A fourth shape (docs/features/source_adapter.md): one fetch returns every
# geography a dataset carries, not one already-known geography.

EUROSTAT_JSON = json.dumps(
    {
        "id": ["unit", "geo", "time"],
        "size": [1, 2, 1],
        "dimension": {
            "unit": {"category": {"index": {"PC_GDP": 0}}},
            "geo": {"category": {"index": {"BE": 0, "DE": 1}}},
            "time": {"category": {"index": {"2023": 0}}},
        },
        "value": {"0": 100.0, "1": 90.0},
        "status": {},
    }
).encode()


def test_multi_geo_time_series_contract_eurostat(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(EUROSTAT_JSON)
    )

    rows = EurostatSource().fetch("https://example.test/x", cache_key="X", dataset="x")

    assert rows, "fixture must produce at least one row to be a meaningful contract check"
    for row in rows:
        assert set(row.keys()) == {"geo", "period", "value", "obs_status"}
        assert isinstance(row["geo"], str)
        assert isinstance(row["period"], str)
        assert row["value"] is None or isinstance(row["value"], float)
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


#: Block NS2's four new WalStat indicators -- the two period forms (01/01,
#: 31/12) and the bare-year form must each be a config the shared loader
#: accepts, walstat-sourced, with a fetch.query naming a real series id.
WALSTAT_NS2_INDICATOR_IDS = frozenset(
    {
        "GRAPA_RECIPIENTS_SHARE_65_PLUS",
        "BIM_BENEFICIARIES_SHARE",
        "PREPAYMENT_METERS_ELECTRICITY_SHARE",
        "PREPAYMENT_METERS_GAS_SHARE",
    }
)


@pytest.mark.parametrize("indicator_id", sorted(WALSTAT_NS2_INDICATOR_IDS))
def test_walstat_ns2_indicators_are_configured_and_walstat_sourced(indicator_id):
    from src.validation.config_schema import load_and_validate_all

    indicators, _sources = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    assert indicator_id in indicators, f"{indicator_id} has no config or failed validation"
    cfg = indicators[indicator_id]
    assert cfg["source_id"] == "walstat"
    assert cfg["unit"] == "percent"
    assert cfg["geo_levels"] == ["municipal"]
    query = cfg["fetch"]["query"]
    assert query.startswith("/json/") and query.endswith("/com+period=all"), query


# --- the bankruptcies municipal contract, with a documented deviation --------
#
# BankruptciesSource is a MunicipalTimeSeriesSource, but geo_id is the raw
# NIS string (never resolved inline -- see src/fetchers/bankruptcies.py's
# module docstring for why: resolution needs a pinned period only
# scripts/sync_bankruptcies.py knows) and each row carries a fifth key,
# `indicator_id`, since one fetch here emits two indicators
# (BANKRUPTCIES, BANKRUPTCY_JOBS_LOST) per commune-month cell rather than
# one row per indicator per fetch. The shared base four keys are still
# checked -- {"geo_id","period","value","status"} is a subset of every row's
# keys, exactly as the shared contract requires -- just not asserted as the
# row's *entire* key set, which is why this is its own test rather than a
# parametrize entry alongside WalStat above.

_BANKRUPTCIES_HEADER = (
    "MS_COUNTOF_BANKRUPTCIES|MS_COUNTOF_FULL_TIME_WORKERS|MS_COUNTOF_PART_TIME_WORKERS|"
    "MS_COUNTOF_SELF_EMPLOYED_WORKERS|MS_COUNTOF_WORKERS|CD_YEAR|CD_MONTH|CD_EMPLOYMENT_CLASS|"
    "TX_EMPLOYMENT_CLASS_DESCR_FR|TX_EMPLOYMENT_CLASS_DESCR_NL|CD_LEGAL_FORM|"
    "TX_LEGAL_FORM_DESCR_FR|TX_LEGAL_FORM_DESCR_NL|CD_MUNTY_REFNIS|TX_MUNTY_DESCR_FR|"
    "TX_MUNTY_DESCR_NL|CD_DSTR_REFNIS|TX_ADM_DSTR_DESCR_FR|TX_ADM_DSTR_DESCR_NL|CD_PROV_REFNIS|"
    "TX_PROV_DESCR_FR|TX_PROV_DESCR_NL|CD_RGN_REFNIS|TX_RGN_DESCR_FR|TX_RGN_DESCR_NL|"
    "CD_NACE_REV2_CLASS|TX_NACE_REV2_CLASS|TX_NACE_REV2_CLASS_FR|TX_NACE_REV2_CLASS_NL|"
    "TX_NACE_REV2_GROUP|TX_NACE_REV2_GROUP_FR|TX_NACE_REV2_GROUP_NL|TX_NACE_REV2_DIVISION|"
    "TX_NACE_REV2_DIVISION_FR|TX_NACE_REV2_DIVISION_NL|TX_NACE_REV2_SECTION|"
    "TX_NACE_REV2_SECTION_FR|TX_NACE_REV2_SECTION_NL|CD_COMPANY_DURATION|"
    "TX_COMPANY_DURATION_FR|TX_COMPANY_DURATION_NL"
)
_BANKRUPTCIES_ROW = (
    "1|0|0|0|1|2026|8|1|0 - 4 salariés|0 - 4 werknemers|1|SNC|VOF|11001|Commune|Gemeente|"
    "0|D|D|0|P|P|0|R|R|4711|x|x|x|x|x|x|x|x|x|x|x|0200|x|x"
)


def _bankruptcies_zip_bytes() -> bytes:
    text = "﻿" + "\n".join([_BANKRUPTCIES_HEADER, _BANKRUPTCIES_ROW]) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("TF_BANKRUPTCIES.txt", text.encode("utf-8"))
    return buf.getvalue()


def test_municipal_time_series_contract_bankruptcies(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(_bankruptcies_zip_bytes()),
    )

    rows = BankruptciesSource().fetch("https://example.test/x.zip", cache_key="X")

    assert rows, "fixture must produce at least one row to be a meaningful contract check"
    for row in rows:
        assert {"geo_id", "period", "value", "status"} <= set(row.keys())
        assert isinstance(row["geo_id"], str)
        assert isinstance(row["period"], str)
        assert isinstance(row["value"], float)
        assert row["status"] in {"final", "provisional", "estimate", "revised", "suppressed", "na"}
        assert row["indicator_id"] in {"BANKRUPTCIES", "BANKRUPTCY_JOBS_LOST"}
    # The raw response was cached before parsing, the base class's own promise.
    assert list(tmp_path.rglob("X.zip")), "raw response not cached under RAW_CACHE_DIR"


# --- the population-movement municipal contract, same deviation --------------
#
# PopulationMovementSource is a MunicipalTimeSeriesSource, but `geo_id` is
# the raw NIS string (never resolved inline -- resolution needs a per-row
# DERIVED period, sheet_year + 1, that only scripts/sync_population_movement.py
# computes) and each row carries a fifth key, `indicator_id`, since one fetch
# emits up to four indicators (BIRTHS, DEATHS, INTERNAL_MIGRATION_NET,
# INTERNATIONAL_MIGRATION_NET) per commune-year cell. See
# src/fetchers/population_movement.py's module docstring.

sys.path.insert(0, str(REPO / "tests"))
from test_population_movement_source import (  # noqa: E402
    REAL_2025_AARTSELAAR,
    REAL_2025_LIEGE,
    REAL_2025_NAMUR,
    _make_workbook,
)


def test_municipal_time_series_contract_population_movement(tmp_path, monkeypatch):
    raw = _make_workbook({"2025": [REAL_2025_NAMUR, REAL_2025_LIEGE, REAL_2025_AARTSELAAR]})
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(raw))

    rows = PopulationMovementSource().fetch("https://example.test/x.xlsx", cache_key="X")

    assert rows, "fixture must produce at least one row to be a meaningful contract check"
    for row in rows:
        assert {"geo_id", "period", "value", "status"} <= set(row.keys())
        assert isinstance(row["geo_id"], str)
        assert isinstance(row["period"], str)
        assert isinstance(row["value"], float)
        assert row["status"] in {"final", "provisional", "estimate", "revised", "suppressed", "na"}
        assert row["indicator_id"] in {
            "BIRTHS",
            "DEATHS",
            "INTERNAL_MIGRATION_NET",
            "INTERNATIONAL_MIGRATION_NET",
        }
    # The raw response was cached before parsing, the base class's own promise.
    assert list(tmp_path.rglob("X.xlsx")), "raw response not cached under RAW_CACHE_DIR"
