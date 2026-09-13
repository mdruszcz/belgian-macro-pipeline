"""The international series reach `observations` -- pipeline repair part 3.

Until this fix scripts/sync_to_canonical.py kept Belgian national series only.
Ten configured foreign series -- the five GDP volume indices (DE, EA, ES, FR,
NL), EC_CONS_CONF_EU and four LABOUR_COST_* -- were fetched every day into
legacy_observations and never synced, while config/national_sections.yaml told
readers the fetch had been timing out. The two existing tests
(tests/test_sync_to_canonical.py, tests/test_port_existing_indicators.py) both
hand-insert their own rows, so neither could see that gap in the real data.
The last test here reads the real one.
"""

import shutil
import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import port_existing_indicators as port_mod  # noqa: E402
import sync_to_canonical as sync_mod  # noqa: E402

import belgian_macro_db as bmdb  # noqa: E402
from src.db import migrate  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

SOURCE_DBNOMICS = {
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
SOURCE_MANUAL = {
    **SOURCE_DBNOMICS,
    "source_id": "statbel",
    "agency": "Statbel",
    "adapter": "manual",
}


def _indicator(code, country=None, source_id="dbnomics_eurostat"):
    doc = {
        "id": code,
        "name": {"en": code, "fr": code, "nl": code},
        "unit": "index_2010",
        "frequency": "Q",
        "source_id": source_id,
        "geo_levels": ["national"],
        "preferred_direction": "neutral",
        "display": None,
    }
    if country:
        doc["country"] = country
    return doc


SOURCES = {"dbnomics_eurostat": SOURCE_DBNOMICS, "statbel": SOURCE_MANUAL}


def test_a_belgian_series_goes_to_be_country():
    geo = port_mod.geography_for_indicator("GDP_BE", _indicator("GDP_BE"), SOURCES)
    assert geo["geo_id"] == "be:country"


@pytest.mark.parametrize(
    "country,geo_id,level",
    [
        ("DE", "de:country", "country"),
        ("ES", "es:country", "country"),
        ("FR", "fr:country", "country"),
        ("NL", "nl:country", "country"),
        ("EA", "ea:aggregate", "eu_aggregate"),
        ("EU27_2020", "eu27_2020:aggregate", "eu_aggregate"),
    ],
)
def test_a_foreign_series_goes_to_its_own_geography(country, geo_id, level):
    """Every country the configs use today. Not an allowlist of indicator ids:
    LABOUR_COST_FR is placed by its country exactly like the GDP series."""
    geo = port_mod.geography_for_indicator("X", _indicator("X", country), SOURCES)
    assert (geo["geo_id"], geo["level"]) == (geo_id, level)
    assert geo["nis_code"] is None


def test_a_fetched_series_with_an_unknown_country_raises_rather_than_vanishing():
    """UK is a real Eurostat code, but the international pilot's allowlist
    (config/geography/international_excluded.csv) deliberately excludes it --
    so it is still unknown to COUNTRY_GEOS, exactly like a typo would be."""
    with pytest.raises(port_mod.UnknownCountryError, match="UK"):
        port_mod.geography_for_indicator("GDP_UK", _indicator("GDP_UK", "UK"), SOURCES)


def test_a_series_the_national_fetch_does_not_deliver_has_no_geography():
    """Municipal and hand-loaded sources never pass through this sync."""
    assert (
        port_mod.geography_for_indicator("POP", _indicator("POP", source_id="statbel"), SOURCES)
        is None
    )


def test_every_configured_country_has_a_geography():
    """PR-time guard for the raise above: a new foreign indicator whose
    country has no geography fails here instead of in the daily run."""
    indicators, sources = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    for code, cfg in sorted(indicators.items()):
        port_mod.geography_for_indicator(code, cfg, sources)  # raises if unknown


@pytest.fixture
def foreign_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REPO / "migrations")
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE legacy_indicators (code TEXT PRIMARY KEY);
        CREATE TABLE legacy_observations (indicator_code TEXT, period TEXT, value REAL,
                                           obs_status TEXT, fetched_at TEXT);
        INSERT INTO legacy_observations VALUES
            ('GDP_BE', '2024-Q1', 118.2, 'A', 'x'),
            ('GDP_DE', '2024-Q1', 109.7, 'A', 'x'),
            ('CONF_EU', '2024-Q1', -14.6, 'A', 'x');
        """)
    conn.commit()
    conn.close()

    config_dir = tmp_path / "config"
    (config_dir / "indicators").mkdir(parents=True)
    (config_dir / "sources").mkdir(parents=True)
    (config_dir / "sources" / "dbnomics_eurostat.yaml").write_text(yaml.dump(SOURCE_DBNOMICS))
    fake_sources = {}
    for code, country in (("GDP_BE", None), ("GDP_DE", "DE"), ("CONF_EU", "EU27_2020")):
        (config_dir / "indicators" / f"{code}.yaml").write_text(
            yaml.dump(_indicator(code, country))
        )
        fake_sources[code] = {
            "name": code,
            "url": "https://example.test/dbnomics",
            "frequency": "Q",
            "unit": "index_2010",
            "source_agency": "Eurostat",
            "type": "dbnomics",
        }
    monkeypatch.setattr(bmdb, "SOURCES", fake_sources)
    monkeypatch.setattr(sync_mod, "SOURCES", fake_sources)
    monkeypatch.setattr(sync_mod, "CONFIG_DIR", config_dir)
    return db_path


def test_the_sync_writes_foreign_series_under_their_own_geography(foreign_db):
    checked, changed = sync_mod.sync(foreign_db, vintage="v1")
    assert (checked, changed) == (3, 3)

    conn = sqlite3.connect(str(foreign_db))
    rows = conn.execute(
        "SELECT indicator_id, geo_id, value, is_latest FROM observations ORDER BY indicator_id"
    ).fetchall()
    levels = dict(conn.execute("SELECT geo_id, level FROM geographies"))
    fk = conn.execute("PRAGMA foreign_key_check").fetchall()
    conn.close()
    assert rows == [
        ("CONF_EU", "eu27_2020:aggregate", -14.6, 1),
        ("GDP_BE", "be:country", 118.2, 1),
        ("GDP_DE", "de:country", 109.7, 1),
    ]
    assert levels["de:country"] == "country"
    assert levels["eu27_2020:aggregate"] == "eu_aggregate"
    assert fk == []


def test_a_foreign_series_keeps_the_vintage_rule(foreign_db):
    """Unchanged on the second run: no new vintage, same as a Belgian series."""
    sync_mod.sync(foreign_db, vintage="v1")
    _checked, changed = sync_mod.sync(foreign_db, vintage="v2")
    assert changed == 0


def test_no_configured_series_is_left_in_the_legacy_tables_only(tmp_path):
    """THE REGRESSION TEST THAT WOULD HAVE CAUGHT IT, on the real data: every
    indicator with rows in legacy_observations and a config under
    config/indicators/ must have rows in `observations` once the daily sync
    has run. Runs the real sync over a copy of the committed database -- the
    sync reads only legacy tables, no network."""
    committed = REPO / "data" / "belgian_macro.db"
    if not committed.is_file():
        pytest.skip("committed database not present")
    db = tmp_path / "copy.db"
    shutil.copy2(committed, db)

    sync_mod.sync(db)

    configured = {p.stem for p in (REPO / "config" / "indicators").glob("*.yaml")}
    conn = sqlite3.connect(str(db))
    try:
        legacy = {
            r[0] for r in conn.execute("SELECT DISTINCT indicator_code FROM legacy_observations")
        }
        synced = {r[0] for r in conn.execute("SELECT DISTINCT indicator_id FROM observations")}
    finally:
        conn.close()
    stranded = sorted((legacy & configured) - synced)
    assert not stranded, f"configured series fetched but never synced: {stranded}"
