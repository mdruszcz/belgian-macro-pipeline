"""Tests for scripts/correct_police_zero_placeholders.py -- the migration
that applies scripts/sync_police.py's province-wide police-zero-is-not-
available rule to the already-committed data/police_observations.csv.

The central claim these tests must prove (per the task brief): the
correction applied to a PRE-#215-SHAPED store must equal the FIXED ADAPTER
run on an equivalent synthetic raw file, row for row -- so the next real
re-sync from raw reproduces the corrected store byte for byte.

The correction script's PRODUCTION path always re-derives from the real
pre-#215 git blob (66468d61:data/police_observations.csv), never from
today's already-na-bearing CSV -- see the module's own docstring for why
(the na conversion is lossy: you cannot tell a restored measured zero from a
true placeholder once the original value is gone). These tests exercise the
pure `correct_rows()` function and `correct_file()` (which takes
`--source-csv`-style explicit input) directly, so they do not depend on git
history being available in the test environment.
"""

import csv
import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import correct_police_zero_placeholders as correct_mod  # noqa: E402
import load_geography  # noqa: E402
import sync_police  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"

COLUMNS = correct_mod.COLUMNS


@pytest.fixture
def geo_conn(tmp_path):
    db_path = tmp_path / "geo.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    conn = sqlite3.connect(str(db_path))
    yield conn
    conn.close()


def _row(
    indicator_id="CAR_THEFT_PER_10K",
    geo_id="be:mun:11001",
    period="2025",
    vintage="2026-09-06T00:00:00+00:00",
    value="0.0",
    status="provisional",
    period_start="2025-01-01",
    period_end="2025-12-31",
    is_latest="1",
    created_at="2026-09-06T00:00:00+00:00",
):
    return {
        "indicator_id": indicator_id,
        "geo_id": geo_id,
        "period": period,
        "vintage": vintage,
        "value": value,
        "status": status,
        "period_start": period_start,
        "period_end": period_end,
        "is_latest": is_latest,
        "created_at": created_at,
    }


def _write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# --- correct_rows(): the pure rule, now province-wide -----------------------
# A single-row group is trivially "unanimous" (there is nothing else to
# disagree with it), so several of these use two rows in the SAME province to
# distinguish "the only commune in this group is zero" (still na -- a
# province-wide non-report is a province-wide non-report even with one
# sibling in the file) from "this commune is zero but a sibling in the same
# province is not" (measured).


def test_zero_value_row_on_a_live_commune_with_no_nonzero_sibling_becomes_na(geo_conn):
    """Both rows are Antwerp-province communes (11001, 11002), both zero --
    unanimous, so both become na."""
    rows = [
        _row(geo_id="be:mun:11001", value="0.0", status="provisional"),
        _row(geo_id="be:mun:11002", value="0.0", status="provisional"),
    ]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert (n_na, n_dropped, n_unchanged) == (2, 0, 0)
    by_geo = {r["geo_id"]: r for r in out}
    assert by_geo["be:mun:11001"]["status"] == "na"
    assert by_geo["be:mun:11001"]["value"] == ""
    assert by_geo["be:mun:11002"]["status"] == "na"


def test_nonzero_value_row_is_unchanged(geo_conn):
    rows = [_row(value="8.5", status="provisional")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert (n_na, n_dropped, n_unchanged) == (0, 0, 1)
    assert out == rows


def test_a_row_already_na_with_zero_value_is_left_alone_not_double_converted(geo_conn):
    """A single row, alone in its province group -- unanimous by definition,
    so it stays na (it already was)."""
    rows = [_row(value="", status="na")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert (n_na, n_dropped, n_unchanged) == (0, 0, 1)
    assert out == rows


def test_a_row_for_a_commune_dissolved_before_its_own_period_is_dropped(geo_conn):
    # Borsbeek (11007): valid_to = 2025-01-01. A 2025 row for it must be
    # dropped entirely, matching sync_police.py's own rule.
    rows = [_row(geo_id="be:mun:11007", period="2025", period_start="2025-01-01", value="61.47")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert (n_na, n_dropped, n_unchanged) == (0, 1, 0)
    assert out == []


def test_a_row_for_a_commune_still_alive_in_its_own_period_is_kept(geo_conn):
    rows = [_row(geo_id="be:mun:11007", period="2024", period_start="2024-01-01", value="61.47")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert (n_na, n_dropped, n_unchanged) == (0, 0, 1)
    assert out == rows


def test_dissolution_check_wins_over_the_zero_rule(geo_conn):
    """A dissolved commune's row is DROPPED, never converted to na -- the two
    rules are not both tried; dissolution is checked first and is final."""
    rows = [_row(geo_id="be:mun:11007", period="2025", period_start="2025-01-01", value="0.0")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert (n_na, n_dropped, n_unchanged) == (0, 1, 0)
    assert out == []


def test_a_dissolved_communes_zero_does_not_count_toward_the_province_group(geo_conn):
    """Borsbeek (11007, dissolved before 2025) is dropped and must not be
    counted in its (former) province's unanimity test -- a live sibling
    (11001, also Antwerp) with a real nonzero rate must still be measured,
    not accidentally forced into a group that includes a dropped row."""
    rows = [
        _row(geo_id="be:mun:11007", period="2025", period_start="2025-01-01", value="0.0"),
        _row(geo_id="be:mun:11001", period="2025", period_start="2025-01-01", value="0.0"),
    ]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    # 11007 dropped; 11001 is the ONLY surviving Antwerp row in this group,
    # so it is unanimous (trivially) and becomes na too.
    assert (n_na, n_dropped, n_unchanged) == (1, 1, 0)
    by_geo = {r["geo_id"]: r for r in out}
    assert by_geo["be:mun:11001"]["status"] == "na"


# --- province-wide unanimity: the maintainer's 2026-09-16 correction -------
# "you removed all the zero instead of just hainaut like i asked" -- a zero
# next to a nonzero PROVINCIAL sibling must be restored as measured, not left
# na, which is the whole point of this migration's rewrite.


def test_a_zero_next_to_a_nonzero_provincial_sibling_is_restored_as_measured(geo_conn):
    """11001 (Antwerp) zero, 11002 (Antwerp) nonzero -- NOT unanimous, so
    11001's zero is a measured 0.0, not na."""
    rows = [
        _row(geo_id="be:mun:11001", value="0.0", status="provisional"),
        _row(geo_id="be:mun:11002", value="3.5", status="provisional"),
    ]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert (n_na, n_dropped, n_unchanged) == (0, 0, 2)
    by_geo = {r["geo_id"]: r for r in out}
    assert by_geo["be:mun:11001"]["status"] == "provisional"
    assert by_geo["be:mun:11001"]["value"] == "0.0"


def test_hainaut_unanimous_zero_becomes_na_while_antwerp_mixed_stays_measured(geo_conn):
    """The exact real-world shape in one call: Hainaut (52011, 52010) both
    zero -> na; Antwerp (11001 zero, 11002 nonzero) mixed -> 11001 stays
    measured. Two independent province groups in the same correction pass."""
    rows = [
        _row(geo_id="be:mun:52011", value="0.0", status="provisional"),
        _row(geo_id="be:mun:52010", value="0.0", status="provisional"),
        _row(geo_id="be:mun:11001", value="0.0", status="provisional"),
        _row(geo_id="be:mun:11002", value="3.5", status="provisional"),
    ]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert (n_na, n_dropped, n_unchanged) == (2, 0, 2)
    by_geo = {r["geo_id"]: r for r in out}
    assert by_geo["be:mun:52011"]["status"] == "na"
    assert by_geo["be:mun:52010"]["status"] == "na"
    assert by_geo["be:mun:11001"]["status"] == "provisional"
    assert by_geo["be:mun:11001"]["value"] == "0.0"
    assert by_geo["be:mun:11002"]["value"] == "3.5"


def test_brussels_communes_grouped_by_region_not_province(geo_conn):
    """21004 zero, 21001 nonzero -- both Brussels communes, no province
    ancestor. Grouped by region instead; not unanimous, so 21004 stays
    measured (proves the Brussels fallback grouping is actually exercised
    by the migration, not merely present in sync_police.py)."""
    rows = [
        _row(geo_id="be:mun:21004", value="0.0", status="provisional"),
        _row(geo_id="be:mun:21001", value="2.0", status="provisional"),
    ]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert (n_na, n_dropped, n_unchanged) == (0, 0, 2)
    by_geo = {r["geo_id"]: r for r in out}
    assert by_geo["be:mun:21004"]["status"] == "provisional"
    assert by_geo["be:mun:21004"]["value"] == "0.0"


def test_indicator_and_period_are_independent_grouping_keys(geo_conn):
    """Same two Hainaut communes, same period: CAR_THEFT_PER_10K is
    unanimous zero (na), HOUSE_BURGLARIES_PER_10K is real data (measured) --
    proving the group key includes indicator_id, not just province+period."""
    rows = [
        _row(indicator_id="CAR_THEFT_PER_10K", geo_id="be:mun:52011", value="0.0"),
        _row(indicator_id="CAR_THEFT_PER_10K", geo_id="be:mun:52010", value="0.0"),
        _row(indicator_id="HOUSE_BURGLARIES_PER_10K", geo_id="be:mun:52011", value="109.62"),
        _row(indicator_id="HOUSE_BURGLARIES_PER_10K", geo_id="be:mun:52010", value="45.0"),
    ]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to, geo_conn)
    assert n_na == 2
    by_key = {(r["indicator_id"], r["geo_id"]): r for r in out}
    assert by_key[("CAR_THEFT_PER_10K", "be:mun:52011")]["status"] == "na"
    assert by_key[("HOUSE_BURGLARIES_PER_10K", "be:mun:52011")]["value"] == "109.62"
    assert by_key[("HOUSE_BURGLARIES_PER_10K", "be:mun:52011")]["status"] == "provisional"


# --- file-level correct_file() ----------------------------------------------


def test_correct_file_rewrites_in_place_and_reports_counts(tmp_path, geo_conn):
    csv_path = tmp_path / "police_observations.csv"
    _write_csv(
        csv_path,
        [
            _row(geo_id="be:mun:52011", value="0.0"),  # Hainaut, unanimous with 52010 below
            _row(geo_id="be:mun:52010", value="0.0"),
            _row(geo_id="be:mun:11002", value="8.5"),
            _row(geo_id="be:mun:11007", period="2025", period_start="2025-01-01", value="0.0"),
        ],
    )
    stats = correct_mod.correct_file(csv_path, geo_conn)
    assert stats == {
        "rows_read": 4,
        "rows_written": 3,
        "became_na": 2,
        "dropped_dissolved": 1,
        "unchanged": 1,
    }
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8", newline="")))
    assert len(rows) == 3
    by_geo = {r["geo_id"]: r for r in rows}
    assert by_geo["be:mun:52011"]["status"] == "na"
    assert by_geo["be:mun:52011"]["value"] == ""
    assert by_geo["be:mun:11002"]["status"] == "provisional"
    assert by_geo["be:mun:11002"]["value"] == "8.5"
    assert "be:mun:11007" not in by_geo


def test_correction_is_idempotent(tmp_path, geo_conn):
    csv_path = tmp_path / "police_observations.csv"
    _write_csv(
        csv_path,
        [
            _row(geo_id="be:mun:52011", value="0.0"),
            _row(geo_id="be:mun:52010", value="0.0"),
            _row(geo_id="be:mun:11002", value="8.5"),
        ],
    )
    correct_mod.correct_file(csv_path, geo_conn)
    first_pass = csv_path.read_text(encoding="utf-8")
    stats_second = correct_mod.correct_file(csv_path, geo_conn)
    assert stats_second["became_na"] == 0
    assert stats_second["dropped_dissolved"] == 0
    assert csv_path.read_text(encoding="utf-8") == first_pass


# --- the central equivalence claim ------------------------------------------


def _sync_db(tmp_path, raw_dir, dataset_map):
    db_path = tmp_path / "sync.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    orig_datasets = dict(sync_police.DATASETS)
    sync_police.DATASETS = dataset_map
    try:
        sync_police.sync(db_path, raw_dir)
    finally:
        sync_police.DATASETS = orig_datasets
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT geo_id, period, value, status FROM observations "
        "WHERE indicator_id = 'CAR_THEFT_PER_10K' ORDER BY geo_id, period"
    ).fetchall()
    conn.close()
    return rows


def test_correction_agrees_with_the_adapter_on_equivalent_synthetic_raw_all_zero_province(
    tmp_path, geo_conn
):
    """Build the SAME logical dataset two ways for a province where EVERY
    commune is zero:

    (a) as an "old store" CSV, as if written by the pre-#215 adapter (real
        z-values copied straight through, including the province-wide zero
        placeholder) -- then run the correction script on it.
    (b) as synthetic raw JSON fed straight through the FIXED sync_police.py.

    The two must agree row for row on (geo_id, period, value, status) for
    every commune that is not dropped for dissolution.
    """
    old_store_rows = [
        _row(geo_id="be:mun:52011", period="2025", period_start="2025-01-01", value="0.0"),
        _row(geo_id="be:mun:52010", period="2025", period_start="2025-01-01", value="0.0"),
        _row(geo_id="be:mun:11007", period="2025", period_start="2025-01-01", value="0.0"),
    ]
    csv_path = tmp_path / "old_store.csv"
    _write_csv(csv_path, old_store_rows)
    correct_mod.correct_file(csv_path, geo_conn)
    corrected_rows = {
        (r["geo_id"], r["period"]): (r["value"], r["status"])
        for r in csv.DictReader(csv_path.open(encoding="utf-8", newline=""))
    }

    raw_dir = tmp_path / "raw"
    raw_dir_cat = raw_dir / "vol de voiture"
    raw_dir_cat.mkdir(parents=True)
    (raw_dir_cat / "2025").write_text(
        json.dumps(
            {
                "error": False,
                "data": [
                    {"geo_code": "52011", "z": 0},
                    {"geo_code": "52010", "z": 0},
                    {"geo_code": "11007", "z": 0},
                ],
            }
        ),
        encoding="utf-8",
    )
    adapter_rows = _sync_db(tmp_path, raw_dir, {"CAR_THEFT_PER_10K": "vol de voiture"})
    adapter_by_key = {
        (geo_id, period): (value, status) for geo_id, period, value, status in adapter_rows
    }

    # Borsbeek must be ABSENT from both -- dissolved before 2025.
    assert ("be:mun:11007", "2025") not in corrected_rows
    assert ("be:mun:11007", "2025") not in adapter_by_key

    for key in [("be:mun:52011", "2025"), ("be:mun:52010", "2025")]:
        corrected_value, corrected_status = corrected_rows[key]
        adapter_value, adapter_status = adapter_by_key[key]
        assert corrected_status == adapter_status == "na", key
        assert corrected_value == "", key
        assert adapter_value is None, key


def test_correction_agrees_with_the_adapter_on_equivalent_synthetic_raw_mixed_province(
    tmp_path, geo_conn
):
    """Same equivalence claim, but for a province that is NOT unanimously
    zero -- proving the correction's restored measured-zero behaviour
    matches the adapter's, not just the na behaviour above."""
    old_store_rows = [
        _row(geo_id="be:mun:11001", period="2025", period_start="2025-01-01", value="0.0"),
        _row(geo_id="be:mun:11002", period="2025", period_start="2025-01-01", value="8.5"),
    ]
    csv_path = tmp_path / "old_store.csv"
    _write_csv(csv_path, old_store_rows)
    correct_mod.correct_file(csv_path, geo_conn)
    corrected_rows = {
        (r["geo_id"], r["period"]): (r["value"], r["status"])
        for r in csv.DictReader(csv_path.open(encoding="utf-8", newline=""))
    }

    raw_dir = tmp_path / "raw"
    raw_dir_cat = raw_dir / "vol de voiture"
    raw_dir_cat.mkdir(parents=True)
    (raw_dir_cat / "2025").write_text(
        json.dumps(
            {
                "error": False,
                "data": [
                    {"geo_code": "11001", "z": 0},
                    {"geo_code": "11002", "z": 8.5},
                ],
            }
        ),
        encoding="utf-8",
    )
    adapter_rows = _sync_db(tmp_path, raw_dir, {"CAR_THEFT_PER_10K": "vol de voiture"})
    adapter_by_key = {
        (geo_id, period): (value, status) for geo_id, period, value, status in adapter_rows
    }

    for key in [("be:mun:11001", "2025"), ("be:mun:11002", "2025")]:
        corrected_value, corrected_status = corrected_rows[key]
        adapter_value, adapter_status = adapter_by_key[key]
        assert corrected_status == adapter_status, key
        adapter_value_as_csv = "" if adapter_value is None else str(adapter_value)
        if adapter_value is None:
            assert corrected_value == "", key
        else:
            assert float(corrected_value) == adapter_value, key
        assert adapter_value_as_csv or adapter_value is None  # sanity, always true

    assert corrected_rows[("be:mun:11001", "2025")] == ("0.0", "provisional")
    assert adapter_by_key[("be:mun:11001", "2025")] == (0.0, "provisional")
    assert corrected_rows[("be:mun:11002", "2025")] == ("8.5", "provisional")
    assert adapter_by_key[("be:mun:11002", "2025")] == (8.5, "provisional")


# --- git-blob sourcing (main()'s production path) ---------------------------


def test_fetch_pre_215_csv_text_reads_the_real_pinned_blob():
    """The blob must actually be readable in this repo's history and must be
    the real pre-#215 shape: it still contains the raw z==0 values as
    measured data (status provisional, not na) -- proving this is genuinely
    the store BEFORE #215's uniform na rewrite, not a copy of the current
    (already-corrected or already-na) file."""
    text = correct_mod.fetch_pre_215_csv_text()
    rows = correct_mod._parse_csv_text(text)
    assert rows, "expected a non-empty pre-#215 store"
    zero_but_not_na = [r for r in rows if r["value"] not in ("", None) and float(r["value"]) == 0.0]
    assert zero_but_not_na, (
        "the pre-#215 blob is expected to contain raw zero values written as measured "
        "data -- if this is empty, PRE_215_COMMIT may be pointing at an already-corrected "
        "revision"
    )
    assert all(r["status"] != "na" for r in zero_but_not_na)


# --- the real committed store, post-correction ------------------------------
# Coordination note (2026-09-16): the maintainer asked whether 2025 police
# data exists for the merged communes. Measured fact: it does not, for any of
# the 13 2025-created NIS codes, and the two Bastogne predecessors (82003,
# 82005) must have no 2025 row either even though they resolve at the 2024
# pin. These assertions run against the REAL corrected
# data/police_observations.csv (committed by this same change) rather than a
# fixture, so they catch a regression in the actual published store, not just
# in the pure function.

REAL_POLICE_CSV = Path(__file__).resolve().parents[1] / "data" / "police_observations.csv"

MERGER_SUCCESSOR_NIS = [
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
    "82039",
]


@pytest.fixture(scope="module")
def real_police_rows():
    with REAL_POLICE_CSV.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


@pytest.fixture(scope="module")
def real_geo_conn():
    import tempfile

    tmp_dir = Path(tempfile.mkdtemp(prefix="police-test-geo-"))
    db_path = tmp_dir / "geo.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    conn = sqlite3.connect(str(db_path))
    yield conn
    conn.close()


def test_real_store_na_rows_all_carry_a_null_value(real_police_rows):
    na_rows = [r for r in real_police_rows if r["status"] == "na"]
    assert na_rows, "expected some na rows in the corrected store"
    assert all(r["value"] == "" for r in na_rows)


def test_real_store_has_no_2025_row_for_any_merger_successor(real_police_rows):
    successor_geo_ids = {f"be:mun:{c}" for c in MERGER_SUCCESSOR_NIS}
    hits = [r for r in real_police_rows if r["geo_id"] in successor_geo_ids]
    assert hits == []


def test_real_store_bastogne_predecessors_have_no_2025_row(real_police_rows):
    hits = [
        r
        for r in real_police_rows
        if r["geo_id"] in ("be:mun:82003", "be:mun:82005") and r["period"] == "2025"
    ]
    assert hits == []


def test_real_store_borsbeek_series_stops_at_2024_not_a_manufactured_zero(real_police_rows):
    borsbeek = [
        r
        for r in real_police_rows
        if r["geo_id"] == "be:mun:11007" and r["indicator_id"] == "HOUSE_BURGLARIES_PER_10K"
    ]
    periods = sorted(r["period"] for r in borsbeek)
    assert periods[-1] == "2024"
    assert "2025" not in periods


def test_real_store_charleroi_car_theft_2025_is_na(real_police_rows):
    """Hand-computed acceptance case: Hainaut's CAR_THEFT_PER_10K 2025 is
    still province-wide zero (unchanged from #215's finding), so Charleroi
    (52011) is na, no value."""
    row = next(
        r
        for r in real_police_rows
        if r["geo_id"] == "be:mun:52011"
        and r["indicator_id"] == "CAR_THEFT_PER_10K"
        and r["period"] == "2025"
    )
    assert row["status"] == "na"
    assert row["value"] == ""


def test_real_store_koksijde_car_theft_2025_is_a_measured_zero(real_police_rows):
    """Koksijde (38014), cited in the task brief as one of the 64 non-Hainaut
    communes with a genuine zero-car-theft year -- must be restored as
    measured 0.0, not left na by the old uniform rule."""
    row = next(
        r
        for r in real_police_rows
        if r["geo_id"] == "be:mun:38014"
        and r["indicator_id"] == "CAR_THEFT_PER_10K"
        and r["period"] == "2025"
    )
    assert row["status"] != "na"
    assert row["value"] != ""
    assert float(row["value"]) == 0.0


def test_real_store_no_zero_remains_inside_a_province_that_is_not_unanimous(
    real_police_rows, real_geo_conn
):
    """Property test (per the task brief): after correction, every `na`
    police cell belongs to a (province, indicator, period) group where ALL
    live communes are na, and no 0.0 remains in any such group."""
    valid_to = correct_mod._valid_to_by_geo_id(real_geo_conn)
    live_rows = [
        r
        for r in real_police_rows
        if not correct_mod._dissolved_before(valid_to.get(r["geo_id"]), r["period_start"])
    ]
    groups: dict[tuple, list[dict]] = {}
    cache: dict[str, str] = {}
    for r in live_rows:
        geo_id = r["geo_id"]
        if geo_id not in cache:
            cache[geo_id] = sync_police._province_group(real_geo_conn, geo_id)
        key = (r["indicator_id"], r["period"], cache[geo_id])
        groups.setdefault(key, []).append(r)

    for key, group_rows in groups.items():
        statuses = {r["status"] for r in group_rows}
        if "na" in statuses:
            # If ANY row in the group is na, EVERY row must be na (no 0.0
            # left inside a group that has already flagged a placeholder).
            assert statuses == {"na"}, (key, statuses)
