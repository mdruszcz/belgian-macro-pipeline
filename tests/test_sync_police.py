"""Tests for scripts/sync_police.py.

The real source files are hand-fetched JSON from police.be, committed under
data/raw/police/ (gitignored). Faked here as small in-memory equivalents
rather than depending on the exact real files, so these tests do not
silently start passing or failing if they are ever refreshed or renamed.
"""

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_police  # noqa: E402

from src.db import migrate  # noqa: E402
from src.geography.resolve import UnknownGeographyError  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"


def _write_year_file(directory: Path, year: str, rows: list[dict]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / year).write_text(json.dumps({"error": False, "data": rows}), encoding="utf-8")


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    conn = sqlite3.connect(str(db_path))
    yield db_path, conn
    conn.close()


def _one_category_dir(tmp_path, dirname: str = "cambriolage") -> Path:
    """A raw_dir containing only ONE of the four real categories, so a test
    does not need to fake all four just to exercise one behaviour. sync()
    still requires every configured category to have at least one file, so
    callers that use this must monkeypatch DATASETS down to that one entry."""
    return tmp_path / dirname


# --- reading a file ---------------------------------------------------------


def test_reads_geo_code_and_z_into_a_flat_rate_map(tmp_path):
    path = tmp_path / "f.json"
    path.write_text(
        json.dumps({"error": False, "data": [{"geo_code": "11001", "z": 76.77}]}),
        encoding="utf-8",
    )
    assert sync_police._read_rates(path) == {"11001": 76.77}


def test_an_error_true_payload_is_refused_rather_than_read(tmp_path):
    path = tmp_path / "f.json"
    path.write_text(json.dumps({"error": True, "data": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="error=true"):
        sync_police._read_rates(path)


def test_a_missing_data_list_is_refused_rather_than_guessed(tmp_path):
    path = tmp_path / "f.json"
    path.write_text(json.dumps({"error": False, "something_else": []}), encoding="utf-8")
    with pytest.raises(ValueError, match="no usable 'data' list"):
        sync_police._read_rates(path)


# --- year-file discovery -----------------------------------------------------


def test_years_available_reads_purely_numeric_filenames(tmp_path):
    d = tmp_path / "cambriolage"
    _write_year_file(d, "2017", [])
    _write_year_file(d, "2024", [])
    (d / "notes.txt").write_text("not a year")
    assert sync_police._years_available(d) == ["2017", "2024"]


def test_years_available_is_empty_for_a_missing_directory(tmp_path):
    assert sync_police._years_available(tmp_path / "does_not_exist") == []


# --- geography: the pinned-period rule --------------------------------------


def test_a_current_commune_resolves_at_the_pinned_period(db):
    db_path, conn = db
    assert sync_police._resolve(conn, "11001") == "be:mun:11001"


def test_a_pre_merger_only_commune_resolves_to_its_own_geo_id(db):
    """Borsbeek (11007) merged into Antwerp on 2025-01-01. The pinned period
    (2024) is BEFORE that merger, so it must resolve to Borsbeek itself, not
    silently attach to whatever absorbed it."""
    db_path, conn = db
    assert sync_police._resolve(conn, "11007") == "be:mun:11007"


def test_a_2019_merger_created_commune_resolves_even_for_an_older_filename(db):
    """Kruisem (45068) did not exist before 2019-01-01. The pinned period
    (2024) is AFTER that merger, so it resolves fine regardless of which
    year's FILE the row came from -- proving the rule is 'resolve at the
    pinned period', not 'resolve at the file's own year', which would raise
    for exactly this commune on a pre-2019 file."""
    db_path, conn = db
    assert sync_police._resolve(conn, "45068") == "be:mun:45068"


def test_a_code_unknown_at_the_pinned_period_raises(db):
    db_path, conn = db
    with pytest.raises(UnknownGeographyError):
        sync_police._resolve(conn, "99999")


# --- the zero-valued placeholder rule ---------------------------------------


def test_an_unresolvable_zero_valued_code_is_skipped_not_fatal(db, tmp_path, monkeypatch):
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "cambriolage",
        "2024",
        [
            {"geo_code": "11001", "z": 76.77},
            {"geo_code": "-1", "z": 0},
            {"geo_code": "99999999", "z": 0},  # not one of the real file's codes
        ],
    )
    read, written = sync_police.sync(db_path, raw_dir)
    assert read == 3
    assert written == 1  # only the one real commune


def test_an_unresolvable_nonzero_code_is_fatal(db, tmp_path, monkeypatch):
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2024", [{"geo_code": "-1", "z": 12.5}])
    with pytest.raises(SystemExit, match="did not resolve"):
        sync_police.sync(db_path, raw_dir)


# --- status: latest year provisional, earlier years final -------------------


def test_only_the_latest_year_in_each_category_is_provisional(db, tmp_path, monkeypatch):
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2017", [{"geo_code": "11001", "z": 100.0}])
    _write_year_file(raw_dir / "cambriolage", "2024", [{"geo_code": "11001", "z": 76.77}])
    sync_police.sync(db_path, raw_dir)

    rows = dict(
        conn.execute(
            "SELECT period, status FROM observations "
            "WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K' AND geo_id = 'be:mun:11001'"
        ).fetchall()
    )
    assert rows == {"2017": "final", "2024": "provisional"}


def test_a_single_year_category_has_that_one_year_provisional(db, tmp_path, monkeypatch):
    """The three categories with only a 2025 file each -- their one year is
    automatically 'the latest', so it is provisional, same as the multi-year
    category's own latest year."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "vol de voiture", "2025", [{"geo_code": "11001", "z": 5.0}])
    sync_police.sync(db_path, raw_dir)
    status = conn.execute(
        "SELECT status FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K'"
    ).fetchone()
    assert status == ("provisional",)


# --- resolution is pinned regardless of the file's own year -----------------


def test_a_2019_merger_created_commune_loads_from_a_pre_2019_filename(db, tmp_path, monkeypatch):
    """The real-world case this whole design exists for: Kruisem carries a
    value in police.be's own '2000' file despite not existing until 2019.
    That must load, resolved to Kruisem's real geo_id, not raise."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2000", [{"geo_code": "45068", "z": 131.86}])
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (1, 1)
    row = conn.execute(
        "SELECT geo_id, value FROM observations WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K'"
    ).fetchone()
    assert row == ("be:mun:45068", 131.86)


# --- end-to-end load, all four categories -----------------------------------


def test_first_sync_loads_every_configured_category(db, tmp_path):
    db_path, conn = db
    raw_dir = tmp_path / "raw"
    for dirname in sync_police.DATASETS.values():
        _write_year_file(raw_dir / dirname, "2025", [{"geo_code": "11001", "z": 1.0}])
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (4, 4)
    loaded = {
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT indicator_id FROM observations WHERE geo_id = 'be:mun:11001'"
        )
    }
    assert loaded == set(sync_police.DATASETS)


def test_a_category_with_no_files_at_all_raises(db, tmp_path):
    db_path, conn = db
    raw_dir = tmp_path / "raw"  # empty -- no category directories exist
    with pytest.raises(FileNotFoundError, match="no year-named file"):
        sync_police.sync(db_path, raw_dir)


def test_resync_with_unchanged_values_writes_no_new_vintage(db, tmp_path, monkeypatch):
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2024", [{"geo_code": "11001", "z": 76.77}])
    sync_police.sync(db_path, raw_dir)
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (1, 0)


def test_reference_rows_mark_every_indicator_not_aggregatable(db):
    db_path, conn = db
    sync_police.sync(db_path, reference_rows_only=True)
    placeholders = ",".join("?" * len(sync_police.DATASETS))
    rows = conn.execute(
        f"SELECT indicator_id, is_additive, aggregation_method FROM indicators "
        f"WHERE indicator_id IN ({placeholders})",
        list(sync_police.DATASETS),
    ).fetchall()
    assert len(rows) == len(sync_police.DATASETS)
    for _indicator_id, is_additive, aggregation_method in rows:
        assert (is_additive, aggregation_method) == (0, "not_applicable")


def test_reference_rows_only_needs_no_source_files(db, tmp_path):
    db_path, conn = db
    read, written = sync_police.sync(db_path, tmp_path / "does_not_exist", reference_rows_only=True)
    assert (read, written) == (0, 0)


# --- z == 0 on a resolvable commune is `na`, never a measured zero ----------
# Maintainer direction, 2026-09-16: "Hainaut is missing data, not 0 for car
# theft." See the module docstring's "z == 0 ON A REAL, RESOLVABLE COMMUNE"
# section.


def test_zero_on_a_resolvable_commune_is_loaded_as_na_not_zero(db, tmp_path, monkeypatch):
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "vol de voiture", "2025", [{"geo_code": "11001", "z": 0}])
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (1, 1)
    row = conn.execute(
        "SELECT value, status FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K' "
        "AND geo_id = 'be:mun:11001'"
    ).fetchone()
    assert row == (None, "na")


def test_zero_on_an_unresolvable_code_is_still_skipped_as_placeholder(db, tmp_path, monkeypatch):
    """The pre-existing rule for a code that names NO geography at all must
    be unchanged by the new rule for a code that DOES resolve."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "vol de voiture",
        "2025",
        [{"geo_code": "11001", "z": 3.5}, {"geo_code": "-1", "z": 0}],
    )
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (2, 1)  # only 11001 written; -1 is a placeholder, not a row
    row = conn.execute(
        "SELECT value, status FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K'"
    ).fetchone()
    assert row == (3.5, "provisional")


def test_nonzero_value_on_a_resolvable_commune_is_unaffected(db, tmp_path, monkeypatch):
    """A single-year file, same shape as test_a_single_year_category_has_that_
    one_year_provisional -- its one year is 'the latest', hence provisional;
    the point here is only that a nonzero value is untouched by the new na
    handling, not the final/provisional rule (already covered elsewhere)."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2024", [{"geo_code": "11001", "z": 76.77}])
    sync_police.sync(db_path, raw_dir)
    row = conn.execute(
        "SELECT value, status FROM observations WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K' "
        "AND period = '2024'"
    ).fetchone()
    assert row == (76.77, "provisional")


def test_a_whole_provinces_worth_of_zeros_all_become_na(db, tmp_path, monkeypatch):
    """The actual shape of the real defect: every commune of a province
    present in the file is z: 0 for this indicator/year -- none of them may
    collapse into a confirmed-zero province. All four codes are real Antwerp
    communes and the file contains no OTHER Antwerp commune, so relative to
    this file's own universe the province is unanimous."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    codes = ["11001", "11002", "11004", "11005"]  # real Antwerp-province communes
    _write_year_file(raw_dir / "vol de voiture", "2025", [{"geo_code": c, "z": 0} for c in codes])
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (len(codes), len(codes))
    rows = conn.execute(
        "SELECT value, status FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K'"
    ).fetchall()
    assert len(rows) == len(codes)
    assert all(row == (None, "na") for row in rows)


# --- maintainer correction, 2026-09-16: the rule must be PROVINCE-WIDE, not
# "every zero anywhere is na". PR #215 over-applied Decision 1 above to every
# zero in the file; the maintainer's actual complaint ("Hainaut is missing
# data, not 0 for car theft") was about one province, and the fix must only
# treat a zero as a placeholder when the WHOLE province (or region, for
# Brussels) sharing that indicator/year is zero. See the module docstring's
# "THE RULE IS PROVINCE-WIDE" section and docs/decisions/0012's amendment.


def test_a_lone_zero_next_to_nonzero_provincial_siblings_is_a_measured_zero(
    db, tmp_path, monkeypatch
):
    """Real shape outside Hainaut: most provinces have a handful of communes
    with a genuine zero sitting next to many nonzero siblings. 11001 zero,
    11002/11004/11005 nonzero, all real Antwerp-province communes -> 11001's
    zero must be stored as a measured 0.0, not na, because its own province
    is NOT unanimous."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "vol de voiture",
        "2025",
        [
            {"geo_code": "11001", "z": 0},
            {"geo_code": "11002", "z": 3.5},
            {"geo_code": "11004", "z": 7.0},
            {"geo_code": "11005", "z": 1.2},
        ],
    )
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (4, 4)
    rows = dict(
        conn.execute(
            "SELECT geo_id, value FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K'"
            " AND status != 'na'"
        ).fetchall()
    )
    assert rows["be:mun:11001"] == 0.0
    row = conn.execute(
        "SELECT status FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K' "
        "AND geo_id = 'be:mun:11001'"
    ).fetchone()
    assert row == ("provisional",)


def test_one_province_all_zero_another_province_mixed_same_file(db, tmp_path, monkeypatch):
    """The exact real-world shape: Hainaut (52011, 52010) unanimously zero for
    an indicator while Antwerp (11001 zero, 11002 nonzero) is mixed in the
    SAME file/year. Hainaut's zeros must become na; Antwerp's lone zero must
    stay a measured 0.0 -- the two provinces are judged independently."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "vol de voiture",
        "2025",
        [
            {"geo_code": "52011", "z": 0},  # Hainaut (Charleroi)
            {"geo_code": "52010", "z": 0},  # Hainaut
            {"geo_code": "11001", "z": 0},  # Antwerp -- lone zero
            {"geo_code": "11002", "z": 3.5},  # Antwerp -- real reading
        ],
    )
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (4, 4)
    by_geo = {
        geo_id: (value, status)
        for geo_id, value, status in conn.execute(
            "SELECT geo_id, value, status FROM observations "
            "WHERE indicator_id = 'CAR_THEFT_PER_10K'"
        ).fetchall()
    }
    assert by_geo["be:mun:52011"] == (None, "na")
    assert by_geo["be:mun:52010"] == (None, "na")
    assert by_geo["be:mun:11001"] == (0.0, "provisional")
    assert by_geo["be:mun:11002"] == (3.5, "provisional")


def test_charleroi_car_theft_2025_is_na_house_burglaries_unchanged(db, tmp_path, monkeypatch):
    """Hand-computed acceptance case from the task brief: Charleroi (52011)
    CAR_THEFT_PER_10K 2025 -> na (its whole province, Hainaut, is zero in
    this file); HOUSE_BURGLARIES_PER_10K 2025, a real nonzero reading,
    -> 109.62 unchanged, in the SAME sync run."""
    db_path, conn = db
    monkeypatch.setattr(
        sync_police,
        "DATASETS",
        {"CAR_THEFT_PER_10K": "vol de voiture", "HOUSE_BURGLARIES_PER_10K": "cambriolage"},
    )
    raw_dir = tmp_path / "raw"
    # Hainaut unanimous zero for car theft (only Hainaut communes in this file).
    _write_year_file(
        raw_dir / "vol de voiture",
        "2025",
        [{"geo_code": "52011", "z": 0}, {"geo_code": "52010", "z": 0}],
    )
    # Real burglary rate for Charleroi, real nonzero rate for its neighbour --
    # not unanimous, so untouched regardless.
    _write_year_file(
        raw_dir / "cambriolage",
        "2025",
        [{"geo_code": "52011", "z": 109.62}, {"geo_code": "52010", "z": 45.0}],
    )
    sync_police.sync(db_path, raw_dir)
    car_theft = conn.execute(
        "SELECT value, status FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K' "
        "AND geo_id = 'be:mun:52011'"
    ).fetchone()
    assert car_theft == (None, "na")
    burglaries = conn.execute(
        "SELECT value, status FROM observations WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K' "
        "AND geo_id = 'be:mun:52011'"
    ).fetchone()
    assert burglaries == (109.62, "provisional")


def test_brussels_communes_group_by_region_not_province(db, tmp_path, monkeypatch):
    """The 19 Brussels communes have no province ancestor -- see
    _province_group's docstring. A lone Brussels zero next to a nonzero
    Brussels sibling must be treated as measured (region not unanimous),
    proving the fallback grouping is exercised, not merely present."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "vol de voiture",
        "2025",
        [{"geo_code": "21004", "z": 0}, {"geo_code": "21001", "z": 2.0}],
    )
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (2, 2)
    by_geo = {
        geo_id: (value, status)
        for geo_id, value, status in conn.execute(
            "SELECT geo_id, value, status FROM observations "
            "WHERE indicator_id = 'CAR_THEFT_PER_10K'"
        ).fetchall()
    }
    assert by_geo["be:mun:21004"] == (0.0, "provisional")
    assert by_geo["be:mun:21001"] == (2.0, "provisional")


def test_brussels_all_zero_in_file_groups_as_na_via_region(db, tmp_path, monkeypatch):
    """All Brussels communes present in the file are zero -> region-wide
    non-report, same treatment as a province-wide one."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "vol de voiture",
        "2025",
        [{"geo_code": "21004", "z": 0}, {"geo_code": "21001", "z": 0}],
    )
    sync_police.sync(db_path, raw_dir)
    by_geo = {
        geo_id: (value, status)
        for geo_id, value, status in conn.execute(
            "SELECT geo_id, value, status FROM observations "
            "WHERE indicator_id = 'CAR_THEFT_PER_10K'"
        ).fetchall()
    }
    assert by_geo["be:mun:21004"] == (None, "na")
    assert by_geo["be:mun:21001"] == (None, "na")


def test_koksijde_car_theft_zero_with_real_burglaries_is_measured(db, tmp_path, monkeypatch):
    """Small non-Hainaut commune cited in the task brief: Koksijde (38014),
    car theft 0 alongside a nonzero sibling in the same province (West
    Flanders / Flandre occidentale) -- must be a measured 0.0."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "vol de voiture",
        "2025",
        [{"geo_code": "38014", "z": 0}, {"geo_code": "38002", "z": 4.0}],
    )
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (2, 2)
    row = conn.execute(
        "SELECT value, status FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K' "
        "AND geo_id = 'be:mun:38014'"
    ).fetchone()
    assert row == (0.0, "provisional")


def test_borsbeek_dissolved_commune_gets_no_2025_row_under_new_rule(db, tmp_path, monkeypatch):
    """Confirms the province-wide rewrite did not disturb the pre-existing
    dissolved-commune behaviour: Borsbeek (11007) merged into Antwerp on
    2025-01-01, so a 2025 file's row for it is dropped entirely, regardless
    of what its (former) province's other communes report."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "vol de voiture",
        "2025",
        [{"geo_code": "11007", "z": 0}, {"geo_code": "11001", "z": 3.0}],
    )
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (2, 1)
    row = conn.execute(
        "SELECT * FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K' "
        "AND geo_id = 'be:mun:11007'"
    ).fetchone()
    assert row is None


# --- _province_group() -------------------------------------------------------


def test_province_group_returns_the_province_ancestor_for_an_ordinary_commune(db):
    db_path, conn = db
    assert sync_police._province_group(conn, "be:mun:52011") == "be:prov:50000"  # Hainaut
    assert sync_police._province_group(conn, "be:mun:11001") == "be:prov:10000"  # Antwerp


def test_province_group_falls_back_to_the_region_for_a_brussels_commune(db):
    db_path, conn = db
    assert sync_police._province_group(conn, "be:mun:21004") == "be:reg:04000"


# --- a commune dissolved before the file's own year gets NO row -------------


def test_a_commune_dissolved_before_the_files_year_gets_no_row(db, tmp_path, monkeypatch):
    """Borsbeek (11007) merged into Antwerp on 2025-01-01 (valid_to). It still
    resolves at the fixed PINNED_PERIOD (2024-01-01, before its own merger),
    but a 2025 file's row for it must be dropped entirely -- it did not exist
    to be measured in 2025. Not `na`: no row at all."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2025", [{"geo_code": "11007", "z": 61.47}])
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (1, 0)
    row = conn.execute(
        "SELECT * FROM observations WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K' "
        "AND geo_id = 'be:mun:11007'"
    ).fetchone()
    assert row is None


def test_a_commune_alive_for_the_whole_file_year_still_gets_a_row(db, tmp_path, monkeypatch):
    """Same Borsbeek code, but a 2024 file -- it was alive for all of 2024
    (valid_to is 2025-01-01, after 2024's period_start), so the row must be
    kept as usual."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"HOUSE_BURGLARIES_PER_10K": "cambriolage"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "cambriolage", "2024", [{"geo_code": "11007", "z": 61.47}])
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (1, 1)
    row = conn.execute(
        "SELECT geo_id, value, status FROM observations "
        "WHERE indicator_id = 'HOUSE_BURGLARIES_PER_10K' AND period = '2024'"
    ).fetchone()
    assert row == ("be:mun:11007", 61.47, "provisional")


def test_bastogne_predecessors_get_no_2025_row_even_though_they_resolve_at_the_2024_pin(
    db, tmp_path, monkeypatch
):
    """The sharpest case (coordinator finding, 2026-09-16): Bastenaken-old
    (82003) and Bertogne (82005) both dissolved 2024-12-02 -- BEFORE the 2024
    pin's own reference date of 2024-01-01 is irrelevant here; what matters is
    that valid_to (2024-12-02) is before 2025's period_start (2025-01-01), so
    a 2025 file's row for either must be dropped, even though _resolve()
    happily resolves them (they existed at 2024-01-01). Their 2025 rows in
    the real store were 0.0 placeholders anyway, but this must hold for any
    value."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(
        raw_dir / "vol de voiture",
        "2025",
        [{"geo_code": "82003", "z": 0}, {"geo_code": "82005", "z": 0}],
    )
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (2, 0)
    rows = conn.execute(
        "SELECT * FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K'"
    ).fetchall()
    assert rows == []


def test_bastogne_merger_successor_has_no_2025_code_in_the_real_grid_and_gets_no_rows(
    db, tmp_path, monkeypatch
):
    """82039 (the merged Bastogne/Bastenaken) did not exist at the 2024 pin,
    so a file that somehow carried its code would raise (unresolvable,
    nonzero) or be silently skipped (unresolvable, zero) exactly like any
    other code the fixed grid does not recognise -- proving this pipeline
    makes no attempt to load police figures for a 2025-merger successor.
    police.be's real 2025 file, as fetched 2026-09-06, contains no such code
    at all (the sync succeeds only because every code in it resolves at the
    2024 pin), so this asserts the defensive behaviour if one ever appeared."""
    db_path, conn = db
    monkeypatch.setattr(sync_police, "DATASETS", {"CAR_THEFT_PER_10K": "vol de voiture"})
    raw_dir = tmp_path / "raw"
    _write_year_file(raw_dir / "vol de voiture", "2025", [{"geo_code": "82039", "z": 0}])
    read, written = sync_police.sync(db_path, raw_dir)
    assert (read, written) == (1, 0)  # skipped as an unresolvable zero placeholder
    rows = conn.execute(
        "SELECT * FROM observations WHERE indicator_id = 'CAR_THEFT_PER_10K'"
    ).fetchall()
    assert rows == []


def test_dissolved_before_helper_uses_the_exclusive_valid_to_boundary(db):
    db_path, conn = db
    # Borsbeek: valid_to = 2025-01-01 (exclusive upper bound of its life).
    assert sync_police._dissolved_before(conn, "be:mun:11007", "2025-01-01") is True
    assert sync_police._dissolved_before(conn, "be:mun:11007", "2024-01-01") is False
    # A currently-live commune (valid_to IS NULL) is never dissolved.
    assert sync_police._dissolved_before(conn, "be:mun:11001", "2099-01-01") is False
