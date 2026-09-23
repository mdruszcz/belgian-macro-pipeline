"""Tests for scripts/sync_population_movement.py.

Uses the `xlsx_bytes=` parameter of sync() (an in-memory fixture workbook
built with openpyxl, same header layout as
tests/test_population_movement_source.py) to exercise the real parse +
per-row geography resolution + write logic with no network -- link
discovery is tested separately in tests/test_population_movement_source.py.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_population_movement  # noqa: E402

from src.db import migrate  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_population_movement_source import (  # noqa: E402
    ROW2,
    ROW3,
    ROW4,
    _data_row,
    _make_workbook,
)

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"

REAL_2025_NAMUR = _data_row("92094", 1036, 1118, -300, 826)
REAL_2025_LIEGE = _data_row("62063", 2016, 2024, -1293, 2135)
REAL_2025_AARTSELAAR = _data_row("11001", 131, 170, 34, 0)


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


# --- reference rows -----------------------------------------------------------


def test_reference_rows_only_needs_no_network(db):
    read, written = sync_population_movement.sync(db, reference_rows_only=True)
    assert (read, written) == (0, 0)
    conn = sqlite3.connect(str(db))
    for indicator_id in (
        "BIRTHS",
        "DEATHS",
        "INTERNAL_MIGRATION_NET",
        "INTERNATIONAL_MIGRATION_NET",
    ):
        row = conn.execute(
            "SELECT is_additive, aggregation_method FROM indicators WHERE indicator_id = ?",
            (indicator_id,),
        ).fetchone()
        assert row == (1, "sum"), indicator_id
    conn.close()


# --- hand-computed values, real 2025 sheet -----------------------------------


def test_hand_computed_2025_values_land_correctly(db):
    raw = _make_workbook({"2025": [REAL_2025_NAMUR, REAL_2025_LIEGE, REAL_2025_AARTSELAAR]})
    sync_population_movement.sync(db, xlsx_bytes=raw)

    def value(indicator, geo_id):
        rows = _observations(db, indicator, geo_id)
        assert len(rows) == 1
        return rows[0][2]

    assert value("BIRTHS", "be:mun:92094") == 1036.0
    assert value("DEATHS", "be:mun:92094") == 1118.0
    assert value("INTERNAL_MIGRATION_NET", "be:mun:92094") == -300.0
    assert value("INTERNATIONAL_MIGRATION_NET", "be:mun:92094") == 826.0

    assert value("BIRTHS", "be:mun:62063") == 2016.0
    assert value("DEATHS", "be:mun:62063") == 2024.0
    assert value("INTERNAL_MIGRATION_NET", "be:mun:62063") == -1293.0
    assert value("INTERNATIONAL_MIGRATION_NET", "be:mun:62063") == 2135.0

    assert value("BIRTHS", "be:mun:11001") == 131.0
    assert value("DEATHS", "be:mun:11001") == 170.0
    assert value("INTERNAL_MIGRATION_NET", "be:mun:11001") == 34.0


def test_observation_period_is_the_sheet_year(db):
    """period is the sheet's own year -- births/deaths/migration happened
    during that calendar year, and (since the sheet_year+1 audit fix) that
    is also the year the geography lookup itself now uses."""
    raw = _make_workbook({"2025": [REAL_2025_NAMUR]})
    sync_population_movement.sync(db, xlsx_bytes=raw)
    rows = _observations(db, "BIRTHS", "be:mun:92094")
    assert rows == [("be:mun:92094", "2025", 1036.0, "final")]


# --- geography: resolved at the row's OWN sheet year (post-audit-fix) --------


def test_a_2011_row_under_the_old_code_resolves_to_the_geography_valid_then():
    """55010 (old Enghien) existed 1977-2019; 51067 (its 2019 successor) is a
    different geo_id. A row on the "2011" sheet under 55010 must resolve to
    be:mun:55010 -- checked against the commune grid as of 2011-01-01 (the
    sheet's OWN year, not sheet_year + 1), where 55010 is still the live
    code -- not to 51067, which did not exist yet."""

    def db_fixture(tmp_path):
        db_path = tmp_path / "test.db"
        migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
        load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
        return db_path

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        db_path = db_fixture(Path(tmp))
        raw = _make_workbook({"2011": [_data_row("55010", 100, 90, 5, 2)]})
        sync_population_movement.sync(db_path, xlsx_bytes=raw)
        rows = _observations(db_path, "BIRTHS", "be:mun:55010")
        assert rows == [("be:mun:55010", "2011", 100.0, "final")]
        # And 51067 (the 2019 successor code) must have NOTHING for 2011 --
        # proving this was not silently resolved to the wrong commune.
        assert _observations(db_path, "BIRTHS", "be:mun:51067") == []


def test_bastogne_2025_row_resolves_from_its_valid_from_date(db):
    """82039 (Bastogne+Bertogne) is valid_from 2024-12-02. Resolved at its
    OWN sheet year, a '2024' sheet row under 82039 does NOT resolve (as of
    2024-01-01 the merger had not happened yet) -- it is the exact
    transition-sheet exclusion this batch's fix introduces (see
    test_sheet_2024_transition_codes_are_excluded_loudly_and_match_expected
    below). A '2025' sheet row under 82039 DOES resolve, since 82039 is live
    well before 2025-01-01."""
    raw = _make_workbook({"2025": [_data_row("82039", 50, 40, 3, 1)]})
    sync_population_movement.sync(db, xlsx_bytes=raw)
    rows = _observations(db, "BIRTHS", "be:mun:82039")
    assert rows == [("be:mun:82039", "2025", 50.0, "final")]


# --- transition-sheet exclusion (the P0 audit fix) ---------------------------


def test_sheet_2018_transition_codes_are_excluded_loudly_and_match_expected(db):
    """The 18 codes the 2019-01-01 merger created already appear on sheet
    "2018" (Statbel's own transition-sheet convention -- see
    docs/features/population_movement.md). Resolved at sheet 2018's own
    year, none of them resolves (they do not exist yet as of 2018-01-01);
    since this exact set is declared in
    sync_population_movement._EXPECTED_TRANSITION_EXCLUSIONS, the run must
    NOT refuse, must NOT write anything for these codes, and must exclude
    exactly this set -- no more, no fewer."""
    new_2019_codes = [
        "12041",
        "44083",
        "44084",
        "44085",
        "45068",
        "51067",
        "51068",
        "51069",
        "55085",
        "55086",
        "57096",
        "57097",
        "58001",
        "58002",
        "58003",
        "58004",
        "72042",
        "72043",
    ]
    rows = [_data_row(c, 10, 5, 1, 1) for c in new_2019_codes]
    raw = _make_workbook({"2018": rows})
    read, written = sync_population_movement.sync(db, xlsx_bytes=raw)
    assert written == 0
    assert read == 18 * 4
    conn = sqlite3.connect(str(db))
    n = conn.execute("SELECT COUNT(*) FROM observations WHERE period = '2018'").fetchone()[0]
    conn.close()
    assert n == 0


def test_sheet_2024_transition_codes_are_excluded_loudly_and_match_expected(db):
    """82039 (Bastogne+Bertogne, valid_from 2024-12-02) plus the twelve other
    2025-01-01 merger codes all already appear on sheet "2024". Resolved at
    sheet 2024's own year, none of the thirteen resolves."""
    new_2025_codes = [
        "82039",
        "23106",
        "37021",
        "37022",
        "44086",
        "44087",
        "44088",
        "46029",
        "46030",
        "71071",
        "71072",
        "73110",
        "73111",
    ]
    rows = [_data_row(c, 10, 5, 1, 1) for c in new_2025_codes]
    raw = _make_workbook({"2024": rows})
    read, written = sync_population_movement.sync(db, xlsx_bytes=raw)
    assert written == 0
    assert read == 13 * 4
    conn = sqlite3.connect(str(db))
    n = conn.execute("SELECT COUNT(*) FROM observations WHERE period = '2024'").fetchone()[0]
    conn.close()
    assert n == 0


def test_a_transition_sheet_still_writes_its_ordinary_rows(db):
    """The exclusion only drops the forward-mapped codes -- an ordinary
    commune on the SAME transition sheet (Namur, unaffected by the 2019
    merger) still resolves and is written normally. The expected-set guard
    is all-or-nothing per sheet (below the module's own docstring), so this
    fixture supplies the complete declared 2018 exclusion set alongside
    Namur -- a PARTIAL set is covered separately by
    test_an_undeclared_transition_exclusion_refuses_the_whole_run."""
    new_2019_codes = [
        "12041",
        "44083",
        "44084",
        "44085",
        "45068",
        "51067",
        "51068",
        "51069",
        "55085",
        "55086",
        "57096",
        "57097",
        "58001",
        "58002",
        "58003",
        "58004",
        "72042",
        "72043",
    ]
    raw = _make_workbook(
        {"2018": [REAL_2025_NAMUR] + [_data_row(c, 10, 5, 1, 1) for c in new_2019_codes]}
    )
    read, written = sync_population_movement.sync(db, xlsx_bytes=raw)
    assert written == 4  # Namur's four indicators; all 18 transition codes excluded
    rows = _observations(db, "BIRTHS", "be:mun:92094")
    assert rows == [("be:mun:92094", "2018", 1036.0, "final")]


def test_an_undeclared_transition_exclusion_refuses_the_whole_run(db):
    """A code that fails to resolve at its own sheet year but is NOT in
    _EXPECTED_TRANSITION_EXCLUSIONS for that sheet must refuse the run --
    the expected-set guard, not the ordinary unresolved-code path, and it
    must still name what went wrong."""
    # 12041 is only declared for sheet "2018", not "2017" -- an off-by-one
    # in a hypothetical future regression would land here.
    raw = _make_workbook({"2017": [REAL_2025_NAMUR, _data_row("12041", 10, 5, 1, 1)]})
    with pytest.raises(SystemExit):
        sync_population_movement.sync(db, xlsx_bytes=raw)
    assert _observations(db, "BIRTHS", "be:mun:92094") == []


# --- row-filtering trap: non-municipality codes, e.g. 20001/20002 -----------


def test_province_codes_are_dropped_not_written_and_not_unresolved():
    """20001/20002 (the two Brabant provinces) are 5-digit codes not ending
    in '000' -- the exact trap a naive filter would fall into. They resolve
    to level='province', not 'municipality', and must be silently dropped:
    no observation written, and the run must NOT refuse (they are not
    unresolved, they are correctly resolved to a non-municipal level)."""
    raw = _make_workbook({"2025": [REAL_2025_NAMUR, _data_row("20001", 999, 999, 999, 999)]})
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
        load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
        read, written = sync_population_movement.sync(db_path, xlsx_bytes=raw)
        # Only Namur's 4 indicators written; 20001 contributed nothing.
        assert written == 4
        conn = sqlite3.connect(str(db_path))
        n = conn.execute(
            "SELECT COUNT(*) FROM observations WHERE geo_id LIKE '%20001%'"
        ).fetchone()[0]
        conn.close()
        assert n == 0


def test_belgium_row_01000_is_dropped_not_written():
    raw = _make_workbook({"2025": [_data_row("01000", 100000, 110000, 0, 40000)]})
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
        load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
        read, written = sync_population_movement.sync(db_path, xlsx_bytes=raw)
        assert written == 0
        # One data row in the sheet, but the adapter emits one output row
        # per indicator (four) for it -- rows_read counts those, mirroring
        # scripts/sync_bankruptcies.py's own convention.
        assert read == 4


# --- an unresolvable code refuses the whole run ------------------------------


def test_an_unresolvable_code_refuses_the_whole_run(db):
    raw = _make_workbook({"2025": [REAL_2025_NAMUR, _data_row("99999", 1, 1, 1, 1)]})
    with pytest.raises(SystemExit, match="did not resolve|resolved to a municipality"):
        sync_population_movement.sync(db, xlsx_bytes=raw)
    # And NOTHING from the same run was written -- never a partial load.
    assert _observations(db, "BIRTHS", "be:mun:92094") == []


# --- header moved refuses (propagated from the adapter) ----------------------


def test_header_schema_change_refuses(db):
    row4 = list(ROW4)
    row4[3] = "SOMETHING ELSE"
    import io

    import openpyxl

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    ws = wb.create_sheet("2025")
    ws.append(["title"] + [None] * 24)
    ws.append(ROW2)
    ws.append(ROW3)
    ws.append(row4)
    ws.append(REAL_2025_NAMUR)
    buf = io.BytesIO()
    wb.save(buf)
    with pytest.raises(Exception, match="NAISSANCES"):
        sync_population_movement.sync(db, xlsx_bytes=buf.getvalue())


# --- idempotence and determinism ---------------------------------------------


def test_running_twice_writes_no_new_vintage_the_second_time(db):
    raw = _make_workbook({"2025": [REAL_2025_NAMUR]})
    sync_population_movement.sync(db, xlsx_bytes=raw)
    _read, written = sync_population_movement.sync(db, xlsx_bytes=raw)
    assert written == 0


def test_running_twice_leaves_the_row_count_identical(db):
    raw = _make_workbook({"2025": [REAL_2025_NAMUR, REAL_2025_LIEGE]})
    sync_population_movement.sync(db, xlsx_bytes=raw)
    conn = sqlite3.connect(str(db))
    first_count = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE indicator_id IN "
        "('BIRTHS', 'DEATHS', 'INTERNAL_MIGRATION_NET', 'INTERNATIONAL_MIGRATION_NET')"
    ).fetchone()[0]
    conn.close()

    sync_population_movement.sync(db, xlsx_bytes=raw)
    conn = sqlite3.connect(str(db))
    second_count = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE indicator_id IN "
        "('BIRTHS', 'DEATHS', 'INTERNAL_MIGRATION_NET', 'INTERNATIONAL_MIGRATION_NET')"
    ).fetchone()[0]
    conn.close()
    assert second_count == first_count


def test_output_sorted_by_indicator_geo_period_is_deterministic(db):
    """Two runs against equivalent but differently-ordered input rows must
    write byte-identical CSVs when exported -- same (indicator_id, geo_id,
    period) sort the offload step uses."""
    raw_a = _make_workbook({"2025": [REAL_2025_NAMUR, REAL_2025_LIEGE]})
    raw_b = _make_workbook({"2025": [REAL_2025_LIEGE, REAL_2025_NAMUR]})

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        db_a = Path(tmp) / "a.db"
        migrate.run(db_a, migrations_dir=REAL_MIGRATIONS_DIR)
        load_geography.load(db_a, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
        sync_population_movement.sync(db_a, xlsx_bytes=raw_a)

        db_b = Path(tmp) / "b.db"
        migrate.run(db_b, migrations_dir=REAL_MIGRATIONS_DIR)
        load_geography.load(db_b, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
        sync_population_movement.sync(db_b, xlsx_bytes=raw_b)

        rows_a = _observations(db_a, "BIRTHS")
        rows_b = _observations(db_b, "BIRTHS")
        assert rows_a == rows_b


# --- duplicate guard (P2), mirrors src/fetchers/walstat.py:353 --------------


def test_a_duplicate_nis_within_one_sheet_raises(db):
    """The same CODE INS appearing twice on one sheet is a source anomaly,
    not something to silently resolve by keeping the last row -- the same
    discipline src/fetchers/walstat.py's own duplicate guard applies. Not
    observed in the real file (verified: 622 codes, 622 distinct, in the
    largest sheet, 2025), but the workbook is outside this pipeline's
    control."""
    raw = _make_workbook(
        {"2025": [REAL_2025_NAMUR, _data_row("92094", 1, 1, 1, 1)]}  # same NIS twice
    )
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        sync_population_movement.sync(db, xlsx_bytes=raw)
    # And NOTHING from the same run was written -- never a partial load.
    assert _observations(db, "BIRTHS", "be:mun:92094") == []


def test_the_same_nis_on_two_different_sheets_is_not_a_duplicate(db):
    """The duplicate guard is keyed on (indicator, nis, sheet_year) -- the
    same commune legitimately appears on every sheet it has a row for."""
    raw = _make_workbook(
        {
            "2024": [REAL_2025_NAMUR],
            "2025": [REAL_2025_NAMUR],
        }
    )
    read, written = sync_population_movement.sync(db, xlsx_bytes=raw)
    assert written == 8  # 4 indicators x 2 sheet years
    assert len(_observations(db, "BIRTHS", "be:mun:92094")) == 2


# --- stale vintage retirement (a pre-fix run's wrong-geo_id rows) -----------


def test_a_stale_row_from_a_pre_fix_run_is_retired_not_left_is_latest(db):
    """Simulates what a run of this script made BEFORE the sheet_year+1 ->
    sheet_year audit fix left behind: an is_latest=1 row for a transition
    code, written under period=2018 (the code did not exist at
    2018-01-01, but the old code resolved geography at 2019-01-01, where it
    did). A fresh run must retire that stale row (is_latest -> 0, never
    deleted) even though the fixed resolution no longer writes anything for
    that same (indicator, geo_id, period) key -- upsert_observation() alone
    would never revisit a key it is not writing this run."""
    conn = sqlite3.connect(str(db))
    # Hand-plant the pre-fix row exactly as the old (buggy) sync would have
    # written it: BIRTHS for be:mun:12041 (a 2019-merger code), period=2018.
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('statbel', 'population_movement', '2026-01-01T00:00:00Z', 'ok')"
    )
    old_fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO observations (indicator_id, geo_id, period, vintage, value, status, "
        "period_start, period_end, is_latest, fetch_run_id, created_at) VALUES "
        "('BIRTHS', 'be:mun:12041', '2018', '2026-01-01T00:00:00Z', 218.0, 'final', "
        "'2018-01-01', '2018-12-31', 1, ?, '2026-01-01T00:00:00Z')",
        (old_fetch_run_id,),
    )
    conn.commit()
    conn.close()

    assert _observations(db, "BIRTHS", "be:mun:12041") == [("be:mun:12041", "2018", 218.0, "final")]

    # A fresh sync of sheet 2018 that correctly excludes 12041 (own-period
    # resolution) must retire the stale row rather than leave it is_latest.
    # The expected-set guard is all-or-nothing per sheet, so this fixture
    # supplies the complete declared 2018 exclusion set (same fixture shape
    # as test_a_transition_sheet_still_writes_its_ordinary_rows).
    new_2019_codes = [
        "12041",
        "44083",
        "44084",
        "44085",
        "45068",
        "51067",
        "51068",
        "51069",
        "55085",
        "55086",
        "57096",
        "57097",
        "58001",
        "58002",
        "58003",
        "58004",
        "72042",
        "72043",
    ]
    raw = _make_workbook(
        {"2018": [REAL_2025_NAMUR] + [_data_row(c, 999, 999, 1, 1) for c in new_2019_codes]}
    )
    sync_population_movement.sync(db, xlsx_bytes=raw)

    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT value, is_latest FROM observations WHERE indicator_id = 'BIRTHS' "
        "AND geo_id = 'be:mun:12041' AND period = '2018'"
    ).fetchall()
    conn.close()
    # The old row is still there (append-only -- never deleted) but no
    # longer is_latest, and no NEW is_latest row was written to replace it
    # (12041 for 2018 is an excluded transition code; nothing should be
    # current for that key at all).
    assert rows == [(218.0, 0)]
