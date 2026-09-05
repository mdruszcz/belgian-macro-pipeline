import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import port_existing_indicators as port_mod  # noqa: E402

import belgian_macro_db as bmdb  # noqa: E402
from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"

VALID_SOURCE_NBB = {
    "source_id": "nbb",
    "name": "NBB",
    "agency": "NBB",
    "adapter": "nbb",
    "base_url": "https://example.test/nbb",
    "licence": None,
    "catalog_ref": None,
    "cadence": "daily",
    "is_active": True,
}

VALID_SOURCE_DBNOMICS = {
    "source_id": "dbnomics_eurostat",
    "name": "Eurostat",
    "agency": "Eurostat",
    "adapter": "dbnomics",
    "base_url": "https://example.test/dbnomics",
    "licence": None,
    "catalog_ref": None,
    "cadence": "daily",
    "is_active": True,
}


def _write_config(tmp_path, indicators: dict, sources: list[dict]):
    indicators_dir = tmp_path / "indicators"
    sources_dir = tmp_path / "sources"
    indicators_dir.mkdir(parents=True)
    sources_dir.mkdir(parents=True)
    for source in sources:
        (sources_dir / f"{source['source_id']}.yaml").write_text(yaml.dump(source))
    for code, ind in indicators.items():
        (indicators_dir / f"{code}.yaml").write_text(yaml.dump(ind))
    return indicators_dir, sources_dir


def _indicator(code, source_id="nbb", country=None, direction="higher_is_better"):
    doc = {
        "id": code,
        "name": {"en": code, "fr": code, "nl": code},
        "unit": "percent_yy",
        "frequency": "Q",
        "source_id": source_id,
        "geo_levels": ["national"],
        "preferred_direction": direction,
        "display": None,
    }
    if country:
        doc["country"] = country
    return doc


def test_map_obs_status_known_codes():
    assert port_mod.map_obs_status("A") == "final"
    assert port_mod.map_obs_status("P") == "provisional"
    assert port_mod.map_obs_status("E") == "estimate"
    assert port_mod.map_obs_status("M") == "na"


def test_map_obs_status_unknown_code_raises():
    with pytest.raises(ValueError, match="Unrecognized"):
        port_mod.map_obs_status("Z")
    with pytest.raises(ValueError, match="Unrecognized"):
        port_mod.map_obs_status("")


@pytest.mark.parametrize(
    "period,frequency,expected",
    [
        ("2024", "A", ("2024-01-01", "2024-12-31")),
        ("2024-Q1", "Q", ("2024-01-01", "2024-03-31")),
        ("2024-Q3", "Q", ("2024-07-01", "2024-09-30")),
        ("2024-02", "M", ("2024-02-01", "2024-02-29")),  # leap year
        ("2023-02", "M", ("2023-02-01", "2023-02-28")),  # non-leap year
        ("2024-07-01", "D", ("2024-07-01", "2024-07-01")),
    ],
)
def test_derive_period_bounds(period, frequency, expected):
    assert port_mod.derive_period_bounds(period, frequency) == expected


@pytest.fixture
def migrated_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    return db_path


def test_port_skips_non_belgium_indicators(monkeypatch, tmp_path, migrated_db):
    fake_sources = {
        "GDP_QUARTERLY_YY": {
            "name": "GDP",
            "url": "https://example.test/nbb",
            "frequency": "Q",
            "unit": "percent_yy",
            "source_agency": "NBB",
            "type": "nbb",
        },
        "EUROSTAT_GDP_Q_MEUR_DE": {  # non-Belgium: must be skipped
            "name": "GDP DE",
            "url": "https://example.test/dbnomics",
            "frequency": "Q",
            "unit": "index_2010",
            "source_agency": "Eurostat",
            "type": "dbnomics",
        },
    }
    config_dir = tmp_path / "config"
    _write_config(
        config_dir,
        {
            "GDP_QUARTERLY_YY": _indicator("GDP_QUARTERLY_YY"),
            "EUROSTAT_GDP_Q_MEUR_DE": _indicator(
                "EUROSTAT_GDP_Q_MEUR_DE", source_id="dbnomics_eurostat", country="DE"
            ),
        },
        [VALID_SOURCE_NBB, VALID_SOURCE_DBNOMICS],
    )

    monkeypatch.setattr(bmdb, "SOURCES", fake_sources)
    monkeypatch.setattr(port_mod, "SOURCES", fake_sources)
    monkeypatch.setattr(port_mod, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(
        port_mod.NBBFetcher,
        "fetch",
        staticmethod(lambda url: [{"period": "2024-Q1", "value": 1.5, "obs_status": "A"}]),
    )

    port_mod.port(migrated_db, run_date="2026-09-05")

    conn = sqlite3.connect(str(migrated_db))
    codes = {r[0] for r in conn.execute("SELECT indicator_id FROM observations")}
    assert codes == {"GDP_QUARTERLY_YY"}
    conn.close()


def test_dbnomics_rows_mapped_to_final(monkeypatch, tmp_path, migrated_db):
    fake_sources = {
        "EUROSTAT_GDP_Q_MEUR": {
            "name": "GDP",
            "url": "https://example.test/dbnomics",
            "frequency": "Q",
            "unit": "index_2010",
            "source_agency": "Eurostat/DBnomics",
            "type": "dbnomics",
        }
    }
    config_dir = tmp_path / "config"
    _write_config(
        config_dir,
        {"EUROSTAT_GDP_Q_MEUR": _indicator("EUROSTAT_GDP_Q_MEUR", source_id="dbnomics_eurostat")},
        [VALID_SOURCE_DBNOMICS],
    )

    monkeypatch.setattr(port_mod, "SOURCES", fake_sources)
    monkeypatch.setattr(port_mod, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(
        port_mod.DBnomicsFetcher,
        "fetch",
        staticmethod(
            lambda url, unit="": [{"period": "2024-Q1", "value": 100.0, "obs_status": "A"}]
        ),
    )

    port_mod.port(migrated_db, run_date="2026-09-05")

    conn = sqlite3.connect(str(migrated_db))
    status = conn.execute("SELECT status FROM observations").fetchone()[0]
    assert status == "final"
    conn.close()


def test_port_idempotent_same_day(monkeypatch, tmp_path, migrated_db):
    fake_sources = {
        "GDP_QUARTERLY_YY": {
            "name": "GDP",
            "url": "https://example.test/nbb",
            "frequency": "Q",
            "unit": "percent_yy",
            "source_agency": "NBB",
            "type": "nbb",
        }
    }
    config_dir = tmp_path / "config"
    _write_config(
        config_dir, {"GDP_QUARTERLY_YY": _indicator("GDP_QUARTERLY_YY")}, [VALID_SOURCE_NBB]
    )

    monkeypatch.setattr(port_mod, "SOURCES", fake_sources)
    monkeypatch.setattr(port_mod, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(
        port_mod.NBBFetcher,
        "fetch",
        staticmethod(lambda url: [{"period": "2024-Q1", "value": 1.5, "obs_status": "A"}]),
    )

    port_mod.port(migrated_db, run_date="2026-09-05")
    port_mod.port(migrated_db, run_date="2026-09-05")

    conn = sqlite3.connect(str(migrated_db))
    count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    assert count == 1
    conn.close()
