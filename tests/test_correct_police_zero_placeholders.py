"""Tests for scripts/correct_police_zero_placeholders.py -- the migration
that applies scripts/sync_police.py's police-zero-is-not-available rule to
the already-committed data/police_observations.csv.

The central claim these tests must prove (per the task brief): the
correction applied to an OLD-SHAPE store must equal the FIXED ADAPTER run on
an equivalent synthetic raw file, row for row -- so the next real re-sync
from raw reproduces the corrected store byte for byte.
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


# --- correct_rows(): the pure rule ------------------------------------------


def test_zero_value_row_on_a_live_commune_becomes_na(geo_conn):
    rows = [_row(value="0.0", status="provisional")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to)
    assert (n_na, n_dropped, n_unchanged) == (1, 0, 0)
    assert out == [{**rows[0], "value": "", "status": "na"}]


def test_nonzero_value_row_is_unchanged(geo_conn):
    rows = [_row(value="8.5", status="provisional")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to)
    assert (n_na, n_dropped, n_unchanged) == (0, 0, 1)
    assert out == rows


def test_a_row_already_na_with_zero_value_is_left_alone_not_double_converted(geo_conn):
    rows = [_row(value="", status="na")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to)
    assert (n_na, n_dropped, n_unchanged) == (0, 0, 1)
    assert out == rows


def test_a_row_for_a_commune_dissolved_before_its_own_period_is_dropped(geo_conn):
    # Borsbeek (11007): valid_to = 2025-01-01. A 2025 row for it must be
    # dropped entirely, matching sync_police.py's own rule.
    rows = [_row(geo_id="be:mun:11007", period="2025", period_start="2025-01-01", value="61.47")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to)
    assert (n_na, n_dropped, n_unchanged) == (0, 1, 0)
    assert out == []


def test_a_row_for_a_commune_still_alive_in_its_own_period_is_kept(geo_conn):
    rows = [_row(geo_id="be:mun:11007", period="2024", period_start="2024-01-01", value="61.47")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to)
    assert (n_na, n_dropped, n_unchanged) == (0, 0, 1)
    assert out == rows


def test_dissolution_check_wins_over_the_zero_rule(geo_conn):
    """A dissolved commune's row is DROPPED, never converted to na -- the two
    rules are not both tried; dissolution is checked first and is final."""
    rows = [_row(geo_id="be:mun:11007", period="2025", period_start="2025-01-01", value="0.0")]
    valid_to = correct_mod._valid_to_by_geo_id(geo_conn)
    out, n_na, n_dropped, n_unchanged = correct_mod.correct_rows(rows, valid_to)
    assert (n_na, n_dropped, n_unchanged) == (0, 1, 0)
    assert out == []


# --- file-level correct_file() ----------------------------------------------


def test_correct_file_rewrites_in_place_and_reports_counts(tmp_path, geo_conn):
    csv_path = tmp_path / "police_observations.csv"
    _write_csv(
        csv_path,
        [
            _row(geo_id="be:mun:11001", value="0.0"),
            _row(geo_id="be:mun:11002", value="8.5"),
            _row(geo_id="be:mun:11007", period="2025", period_start="2025-01-01", value="0.0"),
        ],
    )
    stats = correct_mod.correct_file(csv_path, geo_conn)
    assert stats == {
        "rows_read": 3,
        "rows_written": 2,
        "became_na": 1,
        "dropped_dissolved": 1,
        "unchanged": 1,
    }
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8", newline="")))
    assert len(rows) == 2
    by_geo = {r["geo_id"]: r for r in rows}
    assert by_geo["be:mun:11001"]["status"] == "na"
    assert by_geo["be:mun:11001"]["value"] == ""
    assert by_geo["be:mun:11002"]["status"] == "provisional"
    assert by_geo["be:mun:11002"]["value"] == "8.5"
    assert "be:mun:11007" not in by_geo


def test_correction_is_idempotent(tmp_path, geo_conn):
    csv_path = tmp_path / "police_observations.csv"
    _write_csv(csv_path, [_row(value="0.0"), _row(geo_id="be:mun:11002", value="8.5")])
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


def test_correction_agrees_with_the_adapter_on_equivalent_synthetic_raw(tmp_path, geo_conn):
    """Build the SAME logical dataset two ways:

    (a) as an "old store" CSV, as if written by the pre-fix adapter (real
        z-values copied straight through, including the zero placeholders
        the old code wrote as measured data) -- then run the correction
        script on it.
    (b) as synthetic raw JSON fed straight through the FIXED sync_police.py.

    The two must agree row for row on (geo_id, period, value, status) for
    every commune that is not dropped for dissolution -- proving a future
    real re-sync from raw reproduces what the correction already produced.
    """
    # A live commune with a real nonzero rate, a live commune whose rate is
    # the ambiguous zero placeholder, and Borsbeek (11007) which is live at
    # the 2024 pin but dissolved before 2025 -- the sharpest case.
    old_store_rows = [
        _row(geo_id="be:mun:11001", period="2025", period_start="2025-01-01", value="8.5"),
        _row(geo_id="be:mun:11002", period="2025", period_start="2025-01-01", value="0.0"),
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
                    {"geo_code": "11001", "z": 8.5},
                    {"geo_code": "11002", "z": 0},
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

    # The two live communes must match exactly between the two paths.
    for key in [("be:mun:11001", "2025"), ("be:mun:11002", "2025")]:
        corrected_value, corrected_status = corrected_rows[key]
        adapter_value, adapter_status = adapter_by_key[key]
        # corrected_rows values are CSV strings ("" for NULL); adapter_rows
        # values come straight from sqlite (None for NULL, float otherwise).
        adapter_value_as_csv = "" if adapter_value is None else str(adapter_value)
        assert corrected_status == adapter_status, key
        if adapter_value is None:
            assert corrected_value == "", key
        else:
            assert float(corrected_value) == adapter_value, key
        assert adapter_value_as_csv or adapter_value is None  # sanity, always true

    assert corrected_rows[("be:mun:11001", "2025")] == ("8.5", "provisional")
    assert adapter_by_key[("be:mun:11001", "2025")] == (8.5, "provisional")
    assert corrected_rows[("be:mun:11002", "2025")] == ("", "na")
    assert adapter_by_key[("be:mun:11002", "2025")] == (None, "na")


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


def test_real_store_has_no_zero_valued_rows_left(real_police_rows):
    for row in real_police_rows:
        if row["value"] not in ("", None):
            assert float(row["value"]) != 0.0, row


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
