"""Tests for scripts/sync_bankruptcies.py.

Uses the `zip_bytes=` parameter of sync() (a fixture zip built in-memory,
same 41-column header as tests/test_bankruptcies_source.py) to exercise the
real parse + pinned-resolution + zero-fill logic with no network and no
landing-page HTML fixture needed -- link discovery is tested separately in
tests/test_bankruptcies_source.py.
"""

import io
import sqlite3
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_bankruptcies  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"

HEADER = (
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
_TAIL = "1|0 - 4 salariés|0 - 4 werknemers|1|SNC|VOF|{nis}|Commune|Gemeente|0|D|D|0|P|P|0|R|R|4711|x|x|x|x|x|x|x|x|x|x|x|0200|x|x"


def _row(bankruptcies: float, workers: float, nis: str, year: int, month: int) -> str:
    return f"{bankruptcies}|0|0|0|{workers}|{year}|{month}|{_TAIL.format(nis=nis)}"


def _make_zip(data_rows: list[str]) -> bytes:
    text = "﻿" + "\n".join([HEADER, *data_rows]) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("TF_BANKRUPTCIES.txt", text.encode("utf-8"))
    return buf.getvalue()


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    return db_path


def _observations(db_path, indicator_id, geo_id=None):
    conn = sqlite3.connect(str(db_path))
    q = "SELECT geo_id, period, value, status FROM observations WHERE indicator_id = ?"
    params = [indicator_id]
    if geo_id is not None:
        q += " AND geo_id = ?"
        params.append(geo_id)
    rows = conn.execute(q + " ORDER BY geo_id, period", params).fetchall()
    conn.close()
    return rows


# --- reference rows -------------------------------------------------------


def test_reference_rows_only_needs_no_network(db):
    read, written = sync_bankruptcies.sync(db, reference_rows_only=True)
    assert (read, written) == (0, 0)
    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT is_additive, aggregation_method FROM indicators WHERE indicator_id = 'BANKRUPTCIES'"
    ).fetchone()
    assert row == (1, "sum")
    row2 = conn.execute(
        "SELECT is_additive, aggregation_method FROM indicators "
        "WHERE indicator_id = 'BANKRUPTCY_JOBS_LOST'"
    ).fetchone()
    assert row2 == (1, "sum")
    conn.close()


# --- the zero rule ----------------------------------------------------------


def test_a_live_commune_with_no_row_in_a_month_gets_an_explicit_zero(db):
    # 11001 (Aartselaar) has a row in 2026-08; 21004 (Bruxelles) never appears
    # -- so it must still get a 0.0/final row for that same month.
    raw = _make_zip([_row(1, 1, "11001", 2026, 8)])
    sync_bankruptcies.sync(db, zip_bytes=raw)
    rows = _observations(db, "BANKRUPTCIES", "be:mun:21004")
    assert rows == [("be:mun:21004", "2026-08", 0.0, "final")]


def test_a_row_present_is_not_overwritten_by_the_zero_fill(db):
    raw = _make_zip([_row(3, 4, "11001", 2026, 8)])
    sync_bankruptcies.sync(db, zip_bytes=raw)
    rows = _observations(db, "BANKRUPTCIES", "be:mun:11001")
    assert rows == [("be:mun:11001", "2026-08", 3.0, "final")]
    rows_jobs = _observations(db, "BANKRUPTCY_JOBS_LOST", "be:mun:11001")
    assert rows_jobs == [("be:mun:11001", "2026-08", 4.0, "final")]


def test_herstappe_never_in_the_file_gets_a_full_series_of_zeros(db):
    """Herstappe (73028) never appears in the real file across 21 years --
    the zero-fill must iterate the geography map, not the file's own NIS
    codes, or Herstappe would be silently dropped rather than zero-filled."""
    raw = _make_zip(
        [
            _row(1, 1, "11001", 2026, 6),
            _row(1, 1, "11001", 2026, 7),
            _row(1, 1, "11001", 2026, 8),
        ]
    )
    sync_bankruptcies.sync(db, zip_bytes=raw)
    rows = _observations(db, "BANKRUPTCIES", "be:mun:73028")
    assert rows == [
        ("be:mun:73028", "2026-06", 0.0, "final"),
        ("be:mun:73028", "2026-07", 0.0, "final"),
        ("be:mun:73028", "2026-08", 0.0, "final"),
    ]


def test_every_live_commune_is_present_for_every_month_in_range(db):
    raw = _make_zip([_row(1, 1, "11001", 2026, 6), _row(1, 1, "11001", 2026, 7)])
    sync_bankruptcies.sync(db, zip_bytes=raw)
    conn = sqlite3.connect(str(db))
    live_count = conn.execute(
        "SELECT COUNT(*) FROM geographies WHERE level = 'municipality' AND valid_to IS NULL"
    ).fetchone()[0]
    written_count = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE indicator_id = 'BANKRUPTCIES' AND period = '2026-06'"
    ).fetchone()[0]
    conn.close()
    assert written_count == live_count


# --- upper (and lower) boundary --------------------------------------------


def test_no_period_after_the_file_maximum_is_written(db):
    raw = _make_zip([_row(1, 1, "11001", 2026, 6)])
    sync_bankruptcies.sync(db, zip_bytes=raw)
    conn = sqlite3.connect(str(db))
    max_period = conn.execute(
        "SELECT MAX(period) FROM observations WHERE indicator_id = 'BANKRUPTCIES'"
    ).fetchone()[0]
    conn.close()
    assert max_period == "2026-06"


def test_no_period_before_the_file_minimum_is_written(db):
    raw = _make_zip([_row(1, 1, "11001", 2026, 6)])
    sync_bankruptcies.sync(db, zip_bytes=raw)
    conn = sqlite3.connect(str(db))
    min_period = conn.execute(
        "SELECT MIN(period) FROM observations WHERE indicator_id = 'BANKRUPTCIES'"
    ).fetchone()[0]
    conn.close()
    assert min_period == "2026-06"


# --- pinned resolution -------------------------------------------------------


def test_a_row_dated_2010_resolves_via_the_pinned_period(db):
    """23106 does not resolve for its OWN period (2010-03) directly -- it
    only exists from 2025-01-01 in the geography table -- but every row here
    resolves at PINNED_PERIOD ("2026"), exactly as sync_police.py /
    sync_realestate.py already do for their own sources, so a 2010 row for
    this code must load without raising."""
    raw = _make_zip([_row(1, 1, "23106", 2010, 3)])
    read, written = sync_bankruptcies.sync(db, zip_bytes=raw)
    rows = _observations(db, "BANKRUPTCIES", "be:mun:23106")
    assert ("be:mun:23106", "2010-03", 1.0, "final") in rows


def test_an_unresolvable_code_raises(db):
    raw = _make_zip([_row(1, 1, "99999", 2026, 6)])
    with pytest.raises(SystemExit, match="did not resolve"):
        sync_bankruptcies.sync(db, zip_bytes=raw)


# --- idempotence and determinism --------------------------------------------


def test_running_twice_on_unchanged_input_leaves_the_row_count_identical(db):
    raw = _make_zip([_row(1, 1, "11001", 2026, 6)])
    sync_bankruptcies.sync(db, zip_bytes=raw)
    conn = sqlite3.connect(str(db))
    first_count = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE indicator_id IN ('BANKRUPTCIES', 'BANKRUPTCY_JOBS_LOST')"
    ).fetchone()[0]
    conn.close()

    sync_bankruptcies.sync(db, zip_bytes=raw)
    conn = sqlite3.connect(str(db))
    second_count = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE indicator_id IN ('BANKRUPTCIES', 'BANKRUPTCY_JOBS_LOST')"
    ).fetchone()[0]
    conn.close()
    assert second_count == first_count


def test_running_twice_writes_no_new_vintage_the_second_time(db):
    raw = _make_zip([_row(1, 1, "11001", 2026, 6)])
    sync_bankruptcies.sync(db, zip_bytes=raw)
    _read, written = sync_bankruptcies.sync(db, zip_bytes=raw)
    assert written == 0
