import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_statbel as sync_mod  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"
FIXTURE = Path(__file__).parent / "fixtures" / "statbel_local_units_sample.json"


class _FakeResponse:
    def __init__(self, content: bytes):
        self.content = content
        self.status_code = 200

    def raise_for_status(self):
        pass


@pytest.fixture
def db_with_config(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    # observations.geo_id is a real FK -- the four communes this test writes
    # to must actually exist in geographies first.
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)

    config_dir = tmp_path / "config"
    (config_dir / "indicators").mkdir(parents=True)
    (config_dir / "sources").mkdir(parents=True)
    (config_dir / "sources" / "statbel.yaml").write_text(
        yaml.dump(
            {
                "source_id": "statbel",
                "name": "Statbel Bestat API",
                "agency": "Statbel",
                "adapter": "statbel",
                "base_url": "https://example.test/bestat",
                "licence": "CC BY 4.0",
                "catalog_ref": "test",
                "cadence": "quarterly",
                "is_active": True,
            }
        )
    )
    (config_dir / "indicators" / "LOCAL_UNITS_BY_COMMUNE.yaml").write_text(
        yaml.dump(
            {
                "id": "LOCAL_UNITS_BY_COMMUNE",
                "name": {"en": "Local business units", "fr": "Unités", "nl": "Vestigingen"},
                "unit": "count",
                "frequency": "Q",
                "source_id": "statbel",
                "geo_levels": ["municipal"],
                "preferred_direction": "higher_is_better",
                "description": {"en": "Test"},
                "fetch": {"query": "/views/test/result/JSON"},
                "display": None,
            }
        )
    )

    monkeypatch.setattr(sync_mod, "CONFIG_DIR", config_dir)
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path / "raw")
    return db_path


def _mock_fetch(monkeypatch, content: bytes = None):
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(content or FIXTURE.read_bytes()),
    )


def test_first_sync_inserts_observations_per_commune(db_with_config, monkeypatch):
    _mock_fetch(monkeypatch)
    fetched, changed = sync_mod.sync(db_with_config)
    assert fetched == 4  # 5 fixture rows minus the 1 unattributed
    assert changed == 4

    conn = sqlite3.connect(str(db_with_config))
    rows = conn.execute(
        "SELECT geo_id, value, status, is_latest FROM observations "
        "WHERE indicator_id = 'LOCAL_UNITS_BY_COMMUNE' ORDER BY geo_id"
    ).fetchall()
    assert rows == [
        ("be:mun:11001", 2232.0, "final", 1),
        ("be:mun:11002", 66381.0, "final", 1),
        ("be:mun:46021", 8596.0, "final", 1),
        ("be:mun:62093", 1068.0, "final", 1),
    ]


def test_unchanged_resync_creates_no_new_vintage(db_with_config, monkeypatch):
    _mock_fetch(monkeypatch)
    sync_mod.sync(db_with_config)
    fetched, changed = sync_mod.sync(db_with_config)
    assert fetched == 4
    assert changed == 0

    conn = sqlite3.connect(str(db_with_config))
    count = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE indicator_id = 'LOCAL_UNITS_BY_COMMUNE'"
    ).fetchone()[0]
    assert count == 4  # no duplicate vintages


def test_changed_value_flips_is_latest(db_with_config, monkeypatch):
    _mock_fetch(monkeypatch)
    sync_mod.sync(db_with_config)

    changed_content = FIXTURE.read_text(encoding="utf-8").replace("2232.0", "9999.0")
    _mock_fetch(monkeypatch, changed_content.encode("utf-8"))
    fetched, changed = sync_mod.sync(db_with_config)
    assert changed == 1

    conn = sqlite3.connect(str(db_with_config))
    rows = conn.execute(
        "SELECT value, is_latest FROM observations "
        "WHERE indicator_id = 'LOCAL_UNITS_BY_COMMUNE' AND geo_id = 'be:mun:11001' "
        "ORDER BY vintage"
    ).fetchall()
    assert rows == [(2232.0, 0), (9999.0, 1)]


def test_reference_rows_are_created(db_with_config, monkeypatch):
    _mock_fetch(monkeypatch)
    sync_mod.sync(db_with_config)

    conn = sqlite3.connect(str(db_with_config))
    source = conn.execute(
        "SELECT source_id, adapter FROM sources WHERE source_id='statbel'"
    ).fetchone()
    assert source == ("statbel", "statbel")
    indicator = conn.execute(
        "SELECT indicator_id, is_additive, aggregation_method FROM indicators "
        "WHERE indicator_id = 'LOCAL_UNITS_BY_COMMUNE'"
    ).fetchone()
    assert indicator == ("LOCAL_UNITS_BY_COMMUNE", 1, "sum")


def test_fetch_runs_row_reflects_the_excluded_row(db_with_config, monkeypatch):
    _mock_fetch(monkeypatch)
    sync_mod.sync(db_with_config)

    conn = sqlite3.connect(str(db_with_config))
    rows_read, rows_written = conn.execute(
        "SELECT rows_read, rows_written FROM fetch_runs"
    ).fetchone()
    assert (rows_read, rows_written) == (5, 4)
