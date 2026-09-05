import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_population as sync_mod  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"

# A real current commune (Antwerp) and a real historical predecessor
# (Puurs, merged into Puurs-Sint-Amands on 2019-01-01) -- proves resolve_geo's
# period-awareness actually gets exercised, not just the easy current-commune
# case.
_HEADER = "CD_REFNIS|TX_DESCR_FR|CD_AGE|MS_POPULATION"

# Ages chosen to land one row in each band, so a commune written as
# (nis, total) is split predictably across the three age indicators.
_DEFAULT_AGES = (10, 40, 70)


def _write_year(pop_dir: Path, year: int, rows: list[tuple[str, int]]) -> None:
    """Each (nis, total) is split evenly across one age in each band, with any
    remainder going to the working-age row, so the three bands always sum back
    to exactly `total`."""
    lines = [_HEADER]
    for nis, pop in rows:
        share = pop // 3
        for age in _DEFAULT_AGES:
            value = share if age != 40 else pop - 2 * share
            lines.append(f"{nis}|Test|{age}|{value}")
    (pop_dir / f"TF_SOC_POP_STRUCT_{year}.txt").write_text(
        "﻿" + "\n".join(lines) + "\n", encoding="utf-8"
    )


def _write_year_by_age(pop_dir: Path, year: int, rows: list[tuple[str, int, int]]) -> None:
    """Explicit (nis, age, population) rows, for the age-band tests."""
    lines = [_HEADER] + [f"{nis}|Test|{age}|{pop}" for nis, age, pop in rows]
    (pop_dir / f"TF_SOC_POP_STRUCT_{year}.txt").write_text(
        "﻿" + "\n".join(lines) + "\n", encoding="utf-8"
    )


@pytest.fixture
def db_with_config(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
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
                "licence": "test",
                "catalog_ref": "test",
                "cadence": "annual",
                "is_active": True,
            }
        )
    )
    for indicator_id in ["POPULATION_BY_COMMUNE", *sync_mod.BAND_INDICATOR_IDS.values()]:
        (config_dir / "indicators" / f"{indicator_id}.yaml").write_text(
            yaml.dump(
                {
                    "id": indicator_id,
                    "name": {"en": "Population", "fr": "Population", "nl": "Bevolking"},
                    "unit": "count",
                    "frequency": "A",
                    "source_id": "statbel",
                    "geo_levels": ["municipal"],
                    "preferred_direction": "contextual",
                    "description": {"en": "Test"},
                    "display": None,
                }
            )
        )
    monkeypatch.setattr(sync_mod, "CONFIG_DIR", config_dir)

    pop_dir = tmp_path / "population"
    pop_dir.mkdir()
    return db_path, pop_dir


def test_first_sync_resolves_current_and_historical_geo_ids(db_with_config):
    db_path, pop_dir = db_with_config
    # 2018: Puurs still exists in its own right (old NIS 12030).
    # 2019: Puurs has merged into Puurs-Sint-Amands (new NIS 12041).
    _write_year(pop_dir, 2018, [("11002", 520000), ("12030", 17000)])
    _write_year(pop_dir, 2019, [("11002", 523000), ("12041", 28000)])

    rows_read, rows_written, unresolved = sync_mod.sync(db_path, pop_dir)
    assert rows_read == 4  # (commune, year) pairs
    # Four indicators written per pair: the total plus three age bands.
    assert rows_written == 16
    assert unresolved == 0

    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT geo_id, period, value FROM observations "
        "WHERE indicator_id = 'POPULATION_BY_COMMUNE' ORDER BY geo_id, period"
    ).fetchall()
    assert ("be:mun:11002", "2018", 520000.0) in rows
    assert ("be:mun:11002", "2019", 523000.0) in rows
    # The pre-merger year resolves to the historical predecessor's own geo_id
    # (be:mun:12030, valid 1977-01-01..2019-01-01), not the successor's --
    # population "of Puurs in 2018" is Puurs, not the commune it later
    # merged into.
    assert ("be:mun:12030", "2018", 17000.0) in rows
    assert ("be:mun:12041", "2019", 28000.0) in rows


def test_unchanged_resync_creates_no_new_vintage(db_with_config):
    db_path, pop_dir = db_with_config
    _write_year(pop_dir, 2024, [("11002", 540000)])
    sync_mod.sync(db_path, pop_dir)
    rows_read, rows_written, unresolved = sync_mod.sync(db_path, pop_dir)
    assert rows_written == 0

    conn = sqlite3.connect(str(db_path))
    count = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE indicator_id = 'POPULATION_BY_COMMUNE'"
    ).fetchone()[0]
    assert count == 1


def test_changed_value_flips_is_latest(db_with_config):
    db_path, pop_dir = db_with_config
    _write_year(pop_dir, 2024, [("11002", 540000)])
    sync_mod.sync(db_path, pop_dir)

    _write_year(pop_dir, 2024, [("11002", 541000)])
    _, rows_written, _ = sync_mod.sync(db_path, pop_dir)
    # The total and all three bands moved, so all four get a new vintage.
    assert rows_written == 4

    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT value, is_latest FROM observations "
        "WHERE indicator_id = 'POPULATION_BY_COMMUNE' AND geo_id = 'be:mun:11002' "
        "ORDER BY vintage"
    ).fetchall()
    assert rows == [(540000.0, 0), (541000.0, 1)]


def test_unknown_nis_is_skipped_not_raised(db_with_config):
    db_path, pop_dir = db_with_config
    _write_year(pop_dir, 2024, [("11002", 540000), ("99999", 100)])
    rows_read, rows_written, unresolved = sync_mod.sync(db_path, pop_dir)
    assert rows_read == 2
    assert rows_written == 4  # one resolvable commune x four indicators
    assert unresolved == 1


def test_reference_rows_are_created(db_with_config):
    db_path, pop_dir = db_with_config
    _write_year(pop_dir, 2024, [("11002", 540000)])
    sync_mod.sync(db_path, pop_dir)

    conn = sqlite3.connect(str(db_path))
    indicator = conn.execute(
        "SELECT indicator_id, is_additive, aggregation_method FROM indicators "
        "WHERE indicator_id = 'POPULATION_BY_COMMUNE'"
    ).fetchone()
    assert indicator == ("POPULATION_BY_COMMUNE", 1, "sum")


def test_fetch_runs_row_reflects_read_and_written(db_with_config):
    db_path, pop_dir = db_with_config
    _write_year(pop_dir, 2024, [("11002", 540000), ("99999", 100)])
    sync_mod.sync(db_path, pop_dir)

    conn = sqlite3.connect(str(db_path))
    rows_read, rows_written = conn.execute(
        "SELECT rows_read, rows_written FROM fetch_runs"
    ).fetchone()
    assert (rows_read, rows_written) == (2, 4)


def test_age_bands_are_written_as_separate_indicators(db_with_config):
    """Ages land in the band they belong to, and the three bands are stored as
    raw counts -- the dependency ratio is Block G's job, not stored here."""
    db_path, pop_dir = db_with_config
    _write_year_by_age(
        pop_dir,
        2024,
        [
            ("11002", 5, 1000),  # 0_14
            ("11002", 14, 500),  # 0_14 (boundary, inclusive)
            ("11002", 15, 4000),  # 15_64 (boundary, inclusive)
            ("11002", 64, 100),  # 15_64 (boundary, inclusive)
            ("11002", 65, 900),  # 65_PLUS (boundary, inclusive)
            ("11002", 100, 50),  # 65_PLUS (top-coded "100 and over")
        ],
    )
    sync_mod.sync(db_path, pop_dir)

    conn = sqlite3.connect(str(db_path))
    values = dict(
        conn.execute(
            "SELECT indicator_id, value FROM observations "
            "WHERE geo_id = 'be:mun:11002' AND period = '2024' AND is_latest = 1"
        ).fetchall()
    )
    assert values["POPULATION_AGE_0_14"] == 1500.0
    assert values["POPULATION_AGE_15_64"] == 4100.0
    assert values["POPULATION_AGE_65_PLUS"] == 950.0


def test_bands_sum_exactly_to_the_total(db_with_config):
    """The total is derived by summing the bands rather than by a second pass
    over a 100 MB file, so this invariant is what guarantees the total stayed
    correct when age banding was introduced."""
    db_path, pop_dir = db_with_config
    _write_year_by_age(pop_dir, 2024, [("11002", 3, 111), ("11002", 33, 222), ("11002", 83, 333)])
    sync_mod.sync(db_path, pop_dir)

    conn = sqlite3.connect(str(db_path))
    values = dict(
        conn.execute(
            "SELECT indicator_id, value FROM observations "
            "WHERE geo_id = 'be:mun:11002' AND period = '2024' AND is_latest = 1"
        ).fetchall()
    )
    band_sum = sum(values[i] for i in sync_mod.BAND_INDICATOR_IDS.values())
    assert values["POPULATION_BY_COMMUNE"] == band_sum == 666.0


def test_dependency_ratio_is_not_stored(db_with_config):
    """CLAUDE.md rule 6 / CONTROL G: the ratio is computed from these bands by
    the derived engine, never persisted as if it were a source observation."""
    db_path, pop_dir = db_with_config
    _write_year_by_age(pop_dir, 2024, [("11002", 10, 100), ("11002", 40, 300)])
    sync_mod.sync(db_path, pop_dir)

    conn = sqlite3.connect(str(db_path))
    stored = {r[0] for r in conn.execute("SELECT DISTINCT indicator_id FROM observations")}
    assert not any("DEPENDENCY" in i or "RATIO" in i for i in stored)
