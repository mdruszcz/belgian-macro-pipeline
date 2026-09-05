import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography as load_mod  # noqa: E402

from src.db import migrate  # noqa: E402
from src.geography.resolve import (  # noqa: E402
    UnknownGeographyError,
    period_to_date,
    resolve_geo,
    resolve_to_current,
)

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"


@pytest.fixture
def loaded_db(tmp_path):
    """A migrated database with the real committed geography CSVs loaded.

    Uses the real config rather than a fixture so that the counts asserted below
    are counts of Belgium, not of a toy hierarchy.
    """
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_mod.load(db_path, REAL_CONFIG_DIR)
    return db_path


def test_load_writes_expected_row_counts(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    counts = dict(conn.execute("SELECT level, COUNT(*) FROM geographies GROUP BY level"))
    assert counts["country"] == 1
    assert counts["region"] == 3
    assert counts["province"] == 10
    assert counts["arrondissement"] == 43
    # 565 current communes plus the 55 ended by the 2019 and 2025 waves.
    assert counts["municipality"] == 620


def test_every_current_commune_resolves_to_belgium(loaded_db):
    """No orphans and no cycles. An orphaned commune vanishes from regional
    aggregates silently, understating a region with no error anywhere."""
    conn = sqlite3.connect(str(loaded_db))
    reached = conn.execute("""
        WITH RECURSIVE up(geo_id, root, depth) AS (
            SELECT geo_id, geo_id, 0 FROM geographies
            WHERE level = 'municipality' AND valid_to IS NULL
            UNION ALL
            SELECT g.parent_geo_id, up.root, up.depth + 1
            FROM up JOIN geographies g ON g.geo_id = up.geo_id
            WHERE g.parent_geo_id IS NOT NULL AND up.depth < 10
        )
        SELECT COUNT(DISTINCT root) FROM up WHERE geo_id = 'be:country'
        """).fetchone()[0]
    total = conn.execute(
        "SELECT COUNT(*) FROM geographies WHERE level='municipality' AND valid_to IS NULL"
    ).fetchone()[0]
    assert reached == total == 565


def test_load_is_idempotent(loaded_db):
    """The loader may be re-run; a second pass must not duplicate or drop rows."""
    before = sqlite3.connect(str(loaded_db)).execute("SELECT COUNT(*) FROM geographies").fetchone()
    load_mod.load(loaded_db, REAL_CONFIG_DIR)
    after = sqlite3.connect(str(loaded_db)).execute("SELECT COUNT(*) FROM geographies").fetchone()
    assert before == after


def test_load_preserves_the_country_row_observations_depend_on(loaded_db):
    """Every existing observation references be:country by foreign key, so the
    loader must upsert it rather than replace it."""
    conn = sqlite3.connect(str(loaded_db))
    row = conn.execute(
        "SELECT name_en, valid_from, nis_code FROM geographies WHERE geo_id = 'be:country'"
    ).fetchone()
    assert row == ("Belgium", "1830-01-01", "01000")
    assert conn.execute("SELECT COUNT(*) FROM pragma_foreign_key_check").fetchone()[0] == 0


def test_brussels_communes_parent_through_arrondissement_to_region(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    parent = conn.execute(
        "SELECT parent_geo_id FROM geographies WHERE geo_id = 'be:mun:21004'"
    ).fetchone()[0]
    assert parent == "be:arr:21000"
    grandparent = conn.execute(
        "SELECT parent_geo_id FROM geographies WHERE geo_id = 'be:arr:21000'"
    ).fetchone()[0]
    assert grandparent == "be:reg:04000"


def test_resolve_geo_is_period_aware(loaded_db):
    """The point of the whole block: a 2015 file about Kruibeke must resolve to
    Kruibeke, not to the commune that replaced it in 2025."""
    conn = sqlite3.connect(str(loaded_db))
    assert resolve_geo(conn, "46013", "2015") == "be:mun:46013"
    assert resolve_geo(conn, "46013", "2024-Q4") == "be:mun:46013"
    assert resolve_geo(conn, "46030", "2025-Q1") == "be:mun:46030"


def test_resolve_geo_raises_on_unknown_code(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    with pytest.raises(UnknownGeographyError, match="not present"):
        resolve_geo(conn, "99999", "2024")


def test_resolve_geo_raises_outside_the_validity_window(loaded_db):
    """Kruibeke ceased to exist in 2025; asking about it afterwards must fail
    rather than silently returning its successor."""
    conn = sqlite3.connect(str(loaded_db))
    with pytest.raises(UnknownGeographyError, match="no row covers"):
        resolve_geo(conn, "46013", "2026")


def test_successor_chain_walks_to_the_current_entity(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    assert resolve_to_current(conn, "be:mun:46013") == "be:mun:46030"
    assert resolve_to_current(conn, "be:mun:11001") == "be:mun:11001"


def test_successor_chain_detects_cycles(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    conn.execute(
        "UPDATE geographies SET successor_geo_id = 'be:mun:46013' WHERE geo_id = 'be:mun:46030'"
    )
    with pytest.raises(UnknownGeographyError, match="Cycle"):
        resolve_to_current(conn, "be:mun:46013")


@pytest.mark.parametrize(
    "period,expected",
    [
        ("2024", "2024-01-01"),
        ("2024-Q1", "2024-01-01"),
        ("2024-Q3", "2024-07-01"),
        ("2024-07", "2024-07-01"),
        ("2024-07-15", "2024-07-15"),
    ],
)
def test_period_to_date(period, expected):
    assert period_to_date(period) == expected


def test_period_to_date_rejects_unknown_shapes():
    with pytest.raises(ValueError, match="Unrecognized period format"):
        period_to_date("2024-7")
