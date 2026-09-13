"""Tests for load_observations_csv.load_many() -- loading a
one_csv_per_indicator directory store's files into a working database in one
process (international pilot PR 1). See docs/features/international.md,
"Assemble at scale".
"""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from load_observations_csv import ObservationsCsvError, load_many  # noqa: E402

COLUMNS = (
    "indicator_id",
    "geo_id",
    "period",
    "vintage",
    "value",
    "status",
    "period_start",
    "period_end",
    "is_latest",
    "created_at",
)


def _write_csv(path: Path, indicator_id: str, geo_id: str, period: str, value: str) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerow(
            {
                "indicator_id": indicator_id,
                "geo_id": geo_id,
                "period": period,
                "vintage": "2026-09-13T00:00:00+00:00",
                "value": value,
                "status": "final",
                "period_start": f"{period}-01-01",
                "period_end": f"{period}-12-31",
                "is_latest": "1",
                "created_at": "2026-09-13T00:00:00+00:00",
            }
        )


@pytest.fixture
def eurostat_indicator_configs(tmp_path, monkeypatch):
    """Two minimal, real, validating indicator configs + the eurostat source
    config, so _ensure_reference_rows can resolve them without touching the
    committed config directory."""
    config_dir = tmp_path / "config"
    (config_dir / "indicators").mkdir(parents=True)
    (config_dir / "sources").mkdir(parents=True)
    (config_dir / "sources" / "eurostat.yaml").write_text(
        yaml.dump(
            {
                "source_id": "eurostat",
                "name": "Eurostat",
                "agency": "Eurostat",
                "adapter": "eurostat",
                "catalog_ref": "docs/data_catalog.md#eurostat",
                "is_active": True,
            }
        )
    )
    for code in ("GDP_VOL_EU", "HICP_RATE_EU"):
        (config_dir / "indicators" / f"{code}.yaml").write_text(
            yaml.dump(
                {
                    "id": code,
                    "name": {"en": code, "fr": code, "nl": code},
                    "unit": "percent",
                    "frequency": "A",
                    "source_id": "eurostat",
                    "geo_levels": ["national"],
                    "preferred_direction": "neutral",
                    "display": None,
                }
            )
        )
    import load_observations_csv

    monkeypatch.setattr(load_observations_csv, "CONFIG_DIR", config_dir)
    return config_dir


def test_load_many_loads_every_file_with_one_rebuild_run(tmp_path, eurostat_indicator_configs):
    # be:country is the one geography guaranteed to exist after
    # load_geography.load() alone: the international allowlist's own
    # geographies (de:country, fr:country, ...) are inserted by
    # scripts/sync_international.py --reference-rows-only, a step this test
    # does not need to reach to exercise load_many()'s own mechanics.
    store_dir = tmp_path / "international"
    store_dir.mkdir()
    _write_csv(store_dir / "GDP_VOL_EU.csv", "GDP_VOL_EU", "be:country", "2023", "100.0")
    _write_csv(store_dir / "HICP_RATE_EU.csv", "HICP_RATE_EU", "be:country", "2023", "2.5")

    db = tmp_path / "working.db"
    n = load_many(
        db,
        sorted(store_dir.glob("*.csv")),
        run_source_id="eurostat",
    )

    assert n == 2
    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute(
            "SELECT indicator_id, geo_id, value FROM observations ORDER BY indicator_id"
        ).fetchall()
        runs = conn.execute("SELECT source_id, adapter FROM fetch_runs").fetchall()
    finally:
        conn.close()
    assert rows == [
        ("GDP_VOL_EU", "be:country", 100.0),
        ("HICP_RATE_EU", "be:country", 2.5),
    ]
    # ONE rebuild row for the whole batch, not one per file.
    assert runs == [("eurostat", "rebuild")]


def test_load_many_with_an_empty_list_loads_nothing_and_does_not_raise(tmp_path):
    """An in_db directory store with nothing fetched yet: no files, so no
    indicator to resolve a source for -- but scripts/sync_international.py
    --reference-rows-only (step 5 of build_staging_db.py, run before this
    step) has already inserted the 'eurostat' sources row this rebuild's own
    fetch_runs row references, which this test recreates directly rather
    than reaching through the real script."""
    from src.db import migrate as migrate_mod

    db = tmp_path / "working.db"
    migrate_mod.run(db)
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) VALUES "
        "('eurostat', 'Eurostat', 'Eurostat', 'eurostat', 'docs/data_catalog.md#eurostat')"
    )
    conn.commit()
    conn.close()

    n = load_many(db, [], run_source_id="eurostat")

    assert n == 0
    conn = sqlite3.connect(str(db))
    try:
        assert conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0
        # Migrations and geography still applied even with nothing to load.
        assert conn.execute("SELECT COUNT(*) FROM geographies").fetchone()[0] > 0
    finally:
        conn.close()


def test_load_many_refuses_a_missing_file(tmp_path, eurostat_indicator_configs):
    db = tmp_path / "working.db"
    with pytest.raises(ObservationsCsvError, match="No observations CSV"):
        load_many(db, [tmp_path / "does_not_exist.csv"], run_source_id="eurostat")


def test_load_many_refuses_a_header_only_file(tmp_path, eurostat_indicator_configs):
    store_dir = tmp_path / "international"
    store_dir.mkdir()
    empty = store_dir / "GDP_VOL_EU.csv"
    with empty.open("w", newline="", encoding="utf-8") as fh:
        csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n").writeheader()

    db = tmp_path / "working.db"
    with pytest.raises(ObservationsCsvError, match="header but no rows"):
        load_many(db, [empty], run_source_id="eurostat")
