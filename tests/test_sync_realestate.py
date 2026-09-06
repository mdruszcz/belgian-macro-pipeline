"""Tests for scripts/sync_realestate.py.

The real source file is a large hand-downloaded Statbel workbook, gitignored
under data/raw/statbel/census2021/. Faked here as a small in-memory
equivalent matching the real layout (row 0 the top banner, row 1 the
category label per block, row 2 the column names, repeated once per
property-type block) rather than depending on the real file.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_realestate  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"


class FakeSheet:
    def __init__(self, rows):
        self._rows = rows

    def iter_rows(self, values_only=True):
        return iter(self._rows)


class FakeWorkbook:
    def __init__(self, sheets):
        self._sheets = sheets
        self.sheetnames = list(sheets)

    def __getitem__(self, name):
        return self._sheets[name]

    def close(self):
        pass


HOUSES = sync_realestate.CATEGORY_LABEL


def _sheet(rows):
    header_row0 = [None] * 25
    header_row0[5] = "ventes de biens immobiliers"
    header_row1 = [None] * 25
    header_row1[5] = HOUSES
    header_row1[10] = "Maisons avec 2 ou 3 façades (type fermé + type demi-fermé)"
    header_row2 = (
        ["refnis", "localité", "année", "période", None]
        + [
            "nombre transactions",
            "prix médian(€)",
            "prix premier quartile(€)",
            "prix troisième quartile(€)",
        ]
        + [None] * 16
    )
    return FakeSheet([header_row0, header_row1, header_row2, *rows])


def _row(refnis, year, quarter, transactions, median):
    row = [None] * 25
    row[0], row[2], row[3] = refnis, year, quarter
    row[5], row[6] = transactions, median
    return row


@pytest.fixture(autouse=True)
def fake_openpyxl(monkeypatch):
    """Swaps in a controllable workbook for every test in this file, keyed by
    a module-level dict the test itself populates before calling sync()."""
    state = {}

    def fake_load_workbook(path, read_only=True):
        return state["workbook"]

    import openpyxl

    monkeypatch.setattr(openpyxl, "load_workbook", fake_load_workbook)
    return state


def _use(state, rows):
    state["workbook"] = FakeWorkbook({sync_realestate.SHEET_NAME: _sheet(rows)})


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    return db_path


# --- reading the file --------------------------------------------------


def test_finds_the_houses_category_by_its_own_label_not_a_fixed_column(fake_openpyxl):
    _use(fake_openpyxl, [_row("11001", 2024, "Q1", 10, 300000.0)])
    by_period = sync_realestate._read_quarterly_rows(Path("unused"))
    assert by_period["2024-Q1"]["11001"] == (10.0, 300000.0)


def test_a_missing_category_block_is_refused_rather_than_guessed(fake_openpyxl):
    sheet_rows = [
        [None] * 25,
        [None] * 25,  # no "Toutes les maisons..." label anywhere
        ["refnis", "localité", "année", "période"] + [None] * 21,
    ]
    fake_openpyxl["workbook"] = FakeWorkbook({sync_realestate.SHEET_NAME: FakeSheet(sheet_rows)})
    with pytest.raises(ValueError, match="category block"):
        sync_realestate._read_quarterly_rows(Path("unused"))


def test_a_missing_sheet_is_refused_rather_than_guessed(fake_openpyxl):
    fake_openpyxl["workbook"] = FakeWorkbook({"Some Other Sheet": FakeSheet([])})
    with pytest.raises(ValueError, match="has no sheet"):
        sync_realestate._read_quarterly_rows(Path("unused"))


def test_a_null_median_is_kept_as_none_not_coerced_to_zero(fake_openpyxl):
    _use(fake_openpyxl, [_row("11001", 2024, "Q1", 3, None)])
    by_period = sync_realestate._read_quarterly_rows(Path("unused"))
    assert by_period["2024-Q1"]["11001"] == (3.0, None)


# --- annual totals: only a complete year is summed ----------------------


def test_a_year_with_all_four_quarters_is_summed(fake_openpyxl):
    by_period = {
        "2024-Q1": {"11001": (10.0, 1.0)},
        "2024-Q2": {"11001": (20.0, 1.0)},
        "2024-Q3": {"11001": (5.0, 1.0)},
        "2024-Q4": {"11001": (15.0, 1.0)},
    }
    totals = sync_realestate._annual_transaction_totals(by_period)
    assert totals == {"2024": {"11001": 50.0}}


def test_a_year_with_fewer_than_four_quarters_is_dropped_entirely():
    """The real case this exists for: 2026 has only a Q1 file so far. Summing
    just Q1 and calling it '2026' would be exactly ONEM's part-year-total
    mistake -- an understated total presented as a full year."""
    by_period = {
        "2026-Q1": {"11001": (23.0, 1.0)},
        "2025-Q1": {"11001": (10.0, 1.0)},
        "2025-Q2": {"11001": (10.0, 1.0)},
        "2025-Q3": {"11001": (10.0, 1.0)},
        "2025-Q4": {"11001": (10.0, 1.0)},
    }
    totals = sync_realestate._annual_transaction_totals(by_period)
    assert totals == {"2025": {"11001": 40.0}}


def test_a_none_transaction_count_does_not_poison_the_annual_sum():
    by_period = {
        "2024-Q1": {"11001": (10.0, 1.0)},
        "2024-Q2": {"11001": (None, None)},
        "2024-Q3": {"11001": (5.0, 1.0)},
        "2024-Q4": {"11001": (15.0, 1.0)},
    }
    totals = sync_realestate._annual_transaction_totals(by_period)
    assert totals == {"2024": {"11001": 30.0}}


# --- geography: the pinned-period rule -----------------------------------


def test_a_2019_merger_created_commune_resolves_at_the_pinned_period(db):
    import sqlite3

    conn = sqlite3.connect(str(db))
    from src.geography.resolve import resolve_geo

    # Kruisem (45068) did not exist before 2019-01-01. The pin (2025) is
    # AFTER both the 2019 and 2025 merger waves, so it must resolve.
    assert resolve_geo(conn, "45068", sync_realestate.PINNED_PERIOD) == "be:mun:45068"
    conn.close()


# --- end-to-end load ------------------------------------------------------


def test_first_sync_writes_both_indicators(db, fake_openpyxl, tmp_path):
    _use(
        fake_openpyxl,
        [
            _row("11001", 2024, "Q1", 10, 300000.0),
            _row("11001", 2024, "Q2", 10, 305000.0),
            _row("11001", 2024, "Q3", 10, 310000.0),
            _row("11001", 2024, "Q4", 10, 315000.0),
        ],
    )
    read, written = (tmp_path / "placeholder.xlsx").write_bytes(b"") or sync_realestate.sync(
        db, tmp_path / "placeholder.xlsx"
    )
    assert read == 5  # 1 annual (commune,year) row + 4 quarterly (commune,quarter) rows
    import sqlite3

    conn = sqlite3.connect(str(db))
    annual = conn.execute(
        "SELECT value, status FROM observations WHERE indicator_id = 'HOUSE_SALES_TRANSACTIONS'"
    ).fetchone()
    assert annual == (40.0, "provisional")  # only year loaded -> it is "the latest"
    medians = conn.execute(
        "SELECT period, value, status FROM observations "
        "WHERE indicator_id = 'MEDIAN_HOUSE_PRICE' ORDER BY period"
    ).fetchall()
    assert medians == [
        ("2024-Q1", 300000.0, "final"),
        ("2024-Q2", 305000.0, "final"),
        ("2024-Q3", 310000.0, "final"),
        ("2024-Q4", 315000.0, "provisional"),
    ]
    conn.close()


def test_resync_with_unchanged_values_writes_no_new_vintage(db, fake_openpyxl, tmp_path):
    rows = [
        _row("11001", 2024, "Q1", 10, 300000.0),
        _row("11001", 2024, "Q2", 10, 300000.0),
        _row("11001", 2024, "Q3", 10, 300000.0),
        _row("11001", 2024, "Q4", 10, 300000.0),
    ]
    _use(fake_openpyxl, rows)
    (tmp_path / "placeholder.xlsx").write_bytes(b"") or sync_realestate.sync(
        db, tmp_path / "placeholder.xlsx"
    )
    read, written = (tmp_path / "placeholder.xlsx").write_bytes(b"") or sync_realestate.sync(
        db, tmp_path / "placeholder.xlsx"
    )
    assert written == 0


def test_an_unresolvable_code_raises(db, fake_openpyxl, tmp_path):
    _use(fake_openpyxl, [_row("99999", 2024, "Q1", 10, 300000.0)])
    with pytest.raises(SystemExit, match="did not resolve"):
        (tmp_path / "placeholder.xlsx").write_bytes(b"") or sync_realestate.sync(
            db, tmp_path / "placeholder.xlsx"
        )


def test_reference_rows_mark_median_price_not_aggregatable(db):
    sync_realestate.sync(db, reference_rows_only=True)
    import sqlite3

    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT is_additive, aggregation_method FROM indicators WHERE indicator_id = 'MEDIAN_HOUSE_PRICE'"
    ).fetchone()
    assert row == (0, "not_applicable")
    row2 = conn.execute(
        "SELECT is_additive, aggregation_method FROM indicators "
        "WHERE indicator_id = 'HOUSE_SALES_TRANSACTIONS'"
    ).fetchone()
    assert row2 == (1, "sum")
    conn.close()


def test_reference_rows_correction_propagates_via_on_conflict_update(db):
    """Matches sync_police.py's own fix: a config correction after the first
    load must actually reach the database, not be silently ignored."""
    import sqlite3

    conn = sqlite3.connect(str(db))
    sync_realestate.sync(db, reference_rows_only=True)
    conn.execute(
        "UPDATE indicators SET name_en = 'Stale Name' WHERE indicator_id = 'MEDIAN_HOUSE_PRICE'"
    )
    conn.commit()
    conn.close()

    sync_realestate.sync(db, reference_rows_only=True)

    conn = sqlite3.connect(str(db))
    name = conn.execute(
        "SELECT name_en FROM indicators WHERE indicator_id = 'MEDIAN_HOUSE_PRICE'"
    ).fetchone()[0]
    assert name != "Stale Name"
    conn.close()
