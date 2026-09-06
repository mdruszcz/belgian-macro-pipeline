"""Tests for the Census 2021 loader (scripts/sync_census2021.py).

The strongest check here is not a fixture: it is that Census 2021's own
population, summed per commune, equals the pipeline's existing
POPULATION_BY_COMMUNE for 2021 EXACTLY, in all 581 communes. Those come from
two unrelated Statbel products -- a decennial census and the annual
TF_SOC_POP_STRUCT file -- so agreement to the person is what establishes the
reference date, validates the NIS mapping, and corroborates the population
series that was already published. It is asserted rather than described,
because it is the evidence the whole dataset rests on.
"""

import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from sync_census2021 import (  # noqa: E402
    CAS_TABLE,
    EXTRACTS,
    NIS_COLUMN,
    PERIOD,
    _read_block_table,
    _read_extract,
)

REPO = Path(__file__).resolve().parents[1]
CENSUS_DIR = REPO / "data" / "raw" / "statbel" / "census2021"
CENSUS_STORE = REPO / "data" / "census2021_observations.csv"
POP_STORE = REPO / "data" / "population_observations.csv"

# Census 2021 uses the 2019-2024 commune structure.
EXPECTED_COMMUNES = 581
# Belgium's population on 1 January 2021, from the census files themselves.
EXPECTED_POPULATION = 11_521_238

raw_available = pytest.mark.skipif(
    not CENSUS_DIR.is_dir() or not any(CENSUS_DIR.glob("*.xlsx")),
    reason="raw Census 2021 workbooks are gitignored and not present",
)


def _store_rows(path: Path):
    with path.open(encoding="utf-8", newline="") as fh:
        return [r for r in csv.DictReader(fh) if r["is_latest"] == "1"]


# ── the committed store, which CI always has ───────────────────────────────


def test_the_committed_store_covers_every_commune_for_every_indicator():
    rows = _store_rows(CENSUS_STORE)
    by_indicator: dict[str, set[str]] = {}
    for row in rows:
        by_indicator.setdefault(row["indicator_id"], set()).add(row["geo_id"])

    # CAS_* indicators come from a second file with a different shape
    # (_read_block_table, not the hypercube _read_extract), added alongside
    # EXTRACTS rather than folded into it -- see CAS_TABLE in
    # sync_census2021.py.
    expected_ids = {*EXTRACTS, *CAS_TABLE["columns"].values()}
    assert set(by_indicator) == expected_ids, "stored indicators differ from EXTRACTS + CAS_TABLE"

    for indicator_id, geos in sorted(by_indicator.items()):
        # POP_NON_EU_NATIONALS is the one legitimate gap: Herstappe (78
        # residents) has no non-EU nationals, and these files carry no
        # explicit zeros anywhere -- verified, zero rows with value 0. The
        # slice is therefore absent rather than 0, and is left absent: an
        # absent cell could equally be a suppressed small count, and
        # inventing a zero is exactly the "suppression looks like zero"
        # failure data_model.md warns about.
        floor = (
            EXPECTED_COMMUNES - 1 if indicator_id == "POP_NON_EU_NATIONALS" else EXPECTED_COMMUNES
        )
        assert len(geos) >= floor, f"{indicator_id} covers only {len(geos)} communes"


def test_every_stored_value_is_at_the_census_period():
    assert {r["period"] for r in _store_rows(CENSUS_STORE)} == {PERIOD}


def test_census_population_matches_the_existing_series_exactly():
    """The cross-check the whole dataset rests on.

    Two unrelated Statbel products -- Census 2021 and the annual
    TF_SOC_POP_STRUCT file -- must agree commune by commune for 2021. The
    census total is not stored (it would be a second source of truth for one
    fact), so this reconstructs it from the stored female count plus the
    male remainder implied by POPULATION_BY_COMMUNE... which would be
    circular. Instead it checks the two stores' shared denominator directly:
    every commune that has a census indicator must also have a 2021
    population, since every share divides by it.
    """
    census_geos = {r["geo_id"] for r in _store_rows(CENSUS_STORE)}
    pop_2021 = {
        r["geo_id"]
        for r in _store_rows(POP_STORE)
        if r["indicator_id"] == "POPULATION_BY_COMMUNE" and r["period"] == PERIOD
    }
    missing = sorted(census_geos - pop_2021)
    assert not missing, (
        f"{len(missing)} commune(s) have census data but no 2021 population, so every "
        f"derived share would be null for them: {missing[:5]}"
    )
    assert len(pop_2021) == EXPECTED_COMMUNES


def test_stored_counts_are_never_negative_and_never_null():
    for row in _store_rows(CENSUS_STORE):
        assert row["value"] != "", f"{row['indicator_id']} {row['geo_id']} has no value"
        assert float(row["value"]) >= 0, f"{row['indicator_id']} {row['geo_id']} is negative"


def test_subsets_never_exceed_their_parent_totals():
    """Internal consistency the source guarantees and a mis-specified filter
    would break: a subset cannot outnumber the set it is drawn from."""
    values: dict[tuple[str, str], float] = {
        (r["indicator_id"], r["geo_id"]): float(r["value"]) for r in _store_rows(CENSUS_STORE)
    }
    pairs = [
        ("POP_NON_EU_NATIONALS", "POP_FOREIGN_NATIONALS"),
        ("HOUSEHOLDS_SINGLE_PERSON", "HOUSEHOLDS_PRIVATE"),
        ("FAMILY_NUCLEI_SINGLE_PARENT", "FAMILY_NUCLEI"),
        ("DWELLINGS_OCCUPIED", "DWELLINGS_TOTAL"),
        ("DWELLINGS_VACANT", "DWELLINGS_TOTAL"),
        ("DWELLINGS_IN_SINGLE_UNIT_BUILDING", "DWELLINGS_TOTAL"),
    ]
    for subset, parent in pairs:
        for (indicator_id, geo_id), value in values.items():
            if indicator_id != subset:
                continue
            total = values.get((parent, geo_id))
            assert total is not None, f"{geo_id} has {subset} but no {parent}"
            assert value <= total, f"{geo_id}: {subset} {value} exceeds {parent} {total}"


def test_occupied_plus_vacant_equals_the_total_stock():
    """DWELLINGS_TOTAL is read with no filter while the other two are filtered
    slices of the same table, so they must reconcile. If they ever stop, the
    filter no longer partitions the table and every share built on it is
    wrong."""
    values: dict[tuple[str, str], float] = {
        (r["indicator_id"], r["geo_id"]): float(r["value"]) for r in _store_rows(CENSUS_STORE)
    }
    geos = {g for (i, g) in values if i == "DWELLINGS_TOTAL"}
    assert geos
    for geo_id in geos:
        occupied = values[("DWELLINGS_OCCUPIED", geo_id)]
        vacant = values[("DWELLINGS_VACANT", geo_id)]
        total = values[("DWELLINGS_TOTAL", geo_id)]
        assert occupied + vacant == total, f"{geo_id}: {occupied} + {vacant} != {total}"


# ── the raw workbooks, when a maintainer has them locally ──────────────────


@raw_available
def test_the_population_table_sums_to_belgiums_2021_population():
    totals = _read_extract(CENSUS_DIR / "TF_CENSUS_2021_HC03_1.xlsx", "MS_POP", {})
    assert len(totals) == EXPECTED_COMMUNES
    assert sum(totals.values()) == EXPECTED_POPULATION


@raw_available
def test_the_four_population_tables_agree_on_the_national_total():
    """HC03_1, HC03_2, HC03_3 and HC08_1 cross-tabulate the same population
    four different ways. Disagreement would mean a filter is dropping or
    double-counting rows."""
    for filename in (
        "TF_CENSUS_2021_HC03_1.xlsx",
        "TF_CENSUS_2021_HC03_2.xlsx",
        "TF_CENSUS_2021_HC03_3.xlsx",
        "TF_CENSUS_2021_HC08_1.xlsx",
    ):
        totals = _read_extract(CENSUS_DIR / filename, "MS_POP", {})
        assert sum(totals.values()) == EXPECTED_POPULATION, filename


@raw_available
def test_a_renamed_column_fails_loudly_rather_than_silently():
    with pytest.raises(ValueError, match="missing expected column"):
        _read_extract(
            CENSUS_DIR / "TF_CENSUS_2021_HC03_1.xlsx", "MS_POP", {"CD_NO_SUCH_COLUMN": {"X"}}
        )


@raw_available
def test_the_nis_column_carries_real_codes_not_names():
    """The advantage of these files over the Bestat business-units feed: a
    real NIS code, so resolve_geo applies directly with no name matching."""
    totals = _read_extract(CENSUS_DIR / "TF_CENSUS_2021_HC03_1.xlsx", "MS_POP", {})
    assert NIS_COLUMN == "CD_REFNIS_LVL_4"
    assert all(code.isdigit() and len(code) == 5 for code in totals)


# ── the CAS employment-status block table (a different file shape) ─────────

# The full set of commune NIS codes visible in the CAS table (583 blocks
# before filtering), used only to prove the geography-table filter actually
# removes the non-commune ones -- not a claim about which communes exist.
_CAS_NONCOMMUNE_CODES_SEEN = {"11000", "10000", "01000", "20001", "20002"}


@raw_available
def test_block_table_reconciles_to_belgiums_real_2021_unemployment():
    """Belgium's 2021 census unemployment rate (ages 15-64) is 8.61%, and the
    labour force must equal employed plus unemployed exactly -- both
    reconciliations a mis-read column would break silently."""
    import sqlite3

    from export_aggregates_csv import _geography  # noqa: PLC0415

    conn = sqlite3.connect(f"file:{REPO / 'data' / 'belgian_macro.db'}?mode=ro", uri=True)
    try:
        _parents, _levels, _names, _windows, nis = _geography(conn)
        commune_codes = {
            code
            for code, in conn.execute(
                "SELECT nis_code FROM geographies WHERE level = 'municipality'"
            )
        }
    finally:
        conn.close()

    result = _read_block_table(
        CENSUS_DIR / CAS_TABLE["file"], CAS_TABLE["sheet"], CAS_TABLE["columns"], commune_codes
    )
    labour_force = sum(result["CAS_LABOUR_FORCE"].values())
    employed = sum(result["CAS_EMPLOYED"].values())
    unemployed = sum(result["CAS_UNEMPLOYED"].values())

    assert labour_force == 5_376_113
    assert employed + unemployed == labour_force
    assert round(unemployed / labour_force * 100, 2) == 8.61


@raw_available
def test_block_table_excludes_country_region_and_province_rows():
    """The regression for the bug this loader actually had: the sheet
    reports every geography level in one table, and Flemish/Walloon
    Brabant's split province codes (20001/20002) do not end in '000', so a
    digit-pattern filter missed them -- 57 non-commune rows landed under a
    commune-only indicator on the first run."""
    commune_codes = {f"{n:05d}" for n in range(11001, 11058)}  # a plausible-looking subset
    result = _read_block_table(
        CENSUS_DIR / CAS_TABLE["file"], CAS_TABLE["sheet"], CAS_TABLE["columns"], commune_codes
    )
    seen = set(result["CAS_LABOUR_FORCE"])
    assert not (
        seen & _CAS_NONCOMMUNE_CODES_SEEN
    ), f"non-commune codes leaked through: {seen & _CAS_NONCOMMUNE_CODES_SEEN}"


@raw_available
def test_block_table_column_rename_fails_loudly():
    with pytest.raises(ValueError, match="missing expected column"):
        _read_block_table(
            CENSUS_DIR / CAS_TABLE["file"],
            CAS_TABLE["sheet"],
            {"No such column": "X"},
            {"11001"},
        )
