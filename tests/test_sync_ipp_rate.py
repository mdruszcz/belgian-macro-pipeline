"""Tests for scripts/sync_ipp_rate.py.

Uses the `year_bytes=` parameter of sync() (in-memory fixture workbooks
built with openpyxl, same header layout as
tests/test_spf_finances_source.py) to exercise the real parse + name
resolution + write logic with no network. Values are hand-copied from the
real 2024/2025/2026 files (docs/features/ipp_rate.md, measured 2026-09-23).
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_ipp_rate  # noqa: E402

from src.db import migrate  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_spf_finances_source import REAL_2026_ROWS, _make_workbook  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    return db_path


def _observations(db_path, geo_id=None, period=None):
    conn = sqlite3.connect(str(db_path))
    q = "SELECT geo_id, period, value, status FROM observations WHERE indicator_id = ?"
    params = ["MUN_IPP_ADDITIONAL_RATE"]
    if geo_id is not None:
        q += " AND geo_id = ?"
        params.append(geo_id)
    if period is not None:
        q += " AND period = ?"
        params.append(period)
    rows = conn.execute(q + " ORDER BY geo_id, period", params).fetchall()
    conn.close()
    return rows


# --- reference rows -----------------------------------------------------------


def test_reference_rows_only_needs_no_network(db):
    read, written = sync_ipp_rate.sync(db, reference_rows_only=True)
    assert (read, written) == (0, 0)
    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT is_additive, aggregation_method, unit FROM indicators WHERE indicator_id = ?",
        ("MUN_IPP_ADDITIONAL_RATE",),
    ).fetchone()
    conn.close()
    assert row == (0, "not_applicable", "percent")


# --- hand-computed values, real 2026 rows -------------------------------------


def test_hand_computed_2026_values_land_correctly(db):
    raw = _make_workbook(REAL_2026_ROWS)
    sync_ipp_rate.sync(db, year_bytes={2026: raw})

    def value(geo_id):
        rows = _observations(db, geo_id, "2026")
        assert len(rows) == 1
        return rows[0]

    # Knokke-Heist's 0 is a real published rate -- CLAUDE.md rule 26.
    assert value("be:mun:31043") == ("be:mun:31043", "2026", 0.0, "final")
    assert value("be:mun:41002") == ("be:mun:41002", "2026", 7.5, "final")  # Aalst
    assert value("be:mun:11001") == ("be:mun:11001", "2026", 5.0, "final")  # Aartselaar

    # Saint-Nicolas (Liège, 62093) and Sint-Niklaas (46021) are distinct
    # rows, resolved to distinct communes -- the one documented override.
    assert value("be:mun:62093") == ("be:mun:62093", "2026", 8.5, "final")
    assert value("be:mun:46021") == ("be:mun:46021", "2026", 7.5, "final")


def test_period_is_the_tax_year_exactly_as_published(db):
    raw = _make_workbook(REAL_2026_ROWS)
    sync_ipp_rate.sync(db, year_bytes={2026: raw})
    rows = _observations(db, "be:mun:41002")
    assert rows == [("be:mun:41002", "2026", 7.5, "final")]


# --- the 581-commune 2024 map ---------------------------------------------


def test_2024_file_resolves_against_the_581_commune_map(db):
    """2024 predates the 2025-01-01 merger -- 581 communes, not 565. Every
    name in a 2024-shaped file must resolve against the commune map valid
    at 2024-01-01, not today's 565-commune map."""
    conn = sqlite3.connect(str(db))
    count_2024 = conn.execute(
        "SELECT COUNT(*) FROM geographies WHERE level = 'municipality' AND "
        "valid_from <= '2024-01-01' AND (valid_to IS NULL OR valid_to > '2024-01-01')"
    ).fetchone()[0]
    names = conn.execute(
        "SELECT name_fr FROM geographies WHERE level = 'municipality' AND "
        "valid_from <= '2024-01-01' AND (valid_to IS NULL OR valid_to > '2024-01-01') "
        "AND name_fr != 'Saint-Nicolas' ORDER BY name_fr"
    ).fetchall()
    conn.close()
    assert count_2024 >= 500, "fixture geography config must cover pre-2025-merger communes"

    rows = [(name, 5.0) for (name,) in names]
    raw = _make_workbook(rows)
    read, written = sync_ipp_rate.sync(db, year_bytes={2024: raw})
    assert written == len(rows)
    for _geo_id, period, value, status in _observations(db):
        assert period == "2024"
        assert value == 5.0
        assert status == "final"


# --- refusals ------------------------------------------------------------


def test_unmatched_name_refuses_the_whole_run(db):
    raw = _make_workbook([("Not A Real Commune", 5.0)])
    with pytest.raises(SystemExit):
        sync_ipp_rate.sync(db, year_bytes={2026: raw})
    assert _observations(db) == []


def test_ambiguous_name_without_a_declared_override_refuses(db, monkeypatch):
    """Simulates a hypothetical second ambiguous name the code has no
    override for -- NAME_OVERRIDES is patched empty so the real
    Saint-Nicolas row falls through to the ambiguous-name branch."""
    monkeypatch.setattr(sync_ipp_rate, "NAME_OVERRIDES", {})
    raw = _make_workbook([("Saint-Nicolas", 8.5), ("Sint-Niklaas", 7.5)])
    with pytest.raises(SystemExit):
        sync_ipp_rate.sync(db, year_bytes={2026: raw})
    assert _observations(db) == []


def test_partial_failure_writes_nothing_not_even_the_resolvable_rows(db):
    raw = _make_workbook([("Aalst", 7.5), ("Not A Real Commune", 5.0)])
    with pytest.raises(SystemExit):
        sync_ipp_rate.sync(db, year_bytes={2026: raw})
    assert _observations(db) == [], "refusing the run must never write a partial series"


def test_changed_header_refuses_the_whole_run(db):
    from src.fetchers.spf_finances import IppRateSchemaError

    raw = _make_workbook(REAL_2026_ROWS, header=("Commune", "Rate"))
    with pytest.raises(IppRateSchemaError):
        sync_ipp_rate.sync(db, year_bytes={2026: raw})
    assert _observations(db) == []


def test_empty_file_refuses(db):
    """Not a valid XLSX at all -- openpyxl itself refuses (BadZipFile), the
    same "fail loudly, never guess" outcome CLAUDE.md rule 13 requires, just
    surfaced by the library before this adapter's own schema checks run."""
    import zipfile

    with pytest.raises(zipfile.BadZipFile):
        sync_ipp_rate.sync(db, year_bytes={2026: b""})


# --- idempotence and determinism ------------------------------------------


def test_running_twice_writes_no_new_vintage_the_second_time(db):
    raw = _make_workbook(REAL_2026_ROWS)
    sync_ipp_rate.sync(db, year_bytes={2026: raw})
    _read, written = sync_ipp_rate.sync(db, year_bytes={2026: raw})
    assert written == 0


def test_running_twice_leaves_the_row_count_identical(db):
    raw = _make_workbook(REAL_2026_ROWS)
    sync_ipp_rate.sync(db, year_bytes={2026: raw})
    first_count = len(_observations(db))

    sync_ipp_rate.sync(db, year_bytes={2026: raw})
    second_count = len(_observations(db))
    assert second_count == first_count


def test_output_is_deterministic_regardless_of_input_row_order(db):
    raw_a = _make_workbook(REAL_2026_ROWS)
    raw_b = _make_workbook(list(reversed(REAL_2026_ROWS)))

    db_a = db
    sync_ipp_rate.sync(db_a, year_bytes={2026: raw_a})
    rows_a = _observations(db_a)

    db_b = db.parent / "test_b.db"
    migrate.run(db_b, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_b, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    sync_ipp_rate.sync(db_b, year_bytes={2026: raw_b})
    rows_b = _observations(db_b)

    assert [(g, p, v, s) for g, p, v, s in rows_a] == [(g, p, v, s) for g, p, v, s in rows_b]


# --- name normalisation ----------------------------------------------------


def test_normalize_name_strips_accents_case_and_punctuation():
    assert sync_ipp_rate._normalize_name("Saint-Nicolas") == "saint nicolas"
    assert sync_ipp_rate._normalize_name("SAINT NICOLAS") == "saint nicolas"
    assert sync_ipp_rate._normalize_name("Liège") == "liege"
    assert sync_ipp_rate._normalize_name("Ličge") != sync_ipp_rate._normalize_name("Liege")
