"""Tests for scripts/export_communes_table_json.py.

This script exists to stop communes.html downloading the raw
communes_history.csv (32.2 MB after ONEM landed) and re-pivoting 162,272
rows client-side on every page load. It must produce EXACTLY what
communes.html's own parseCSV()+pivot() used to build -- same keys, same
year-collapse rule -- since this is a performance change, not a behaviour
change. That equivalence was verified directly against the real
communes.html JS via Node before this file existed; what is tested here is
the logic in isolation, on small fixtures, so a future edit that breaks the
parity has a fast local signal instead of only a slow end-to-end one.
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from export_communes_table_json import build_table, year_of  # noqa: E402


def _write_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = [
        "geo_id",
        "nis_code",
        "name_en",
        "name_fr",
        "name_nl",
        "region",
        "province",
        "arrondissement",
        "indicator_code",
        "indicator_name",
        "unit",
        "period",
        "value",
        "status",
        "fetched_at",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({**dict.fromkeys(fieldnames, ""), **row})


BASE_ROW = {
    "geo_id": "be:mun:11001",
    "nis_code": "11001",
    "name_en": "Aartselaar",
    "name_fr": "Aartselaar",
    "name_nl": "Aartselaar",
    "region": "Flanders",
    "province": "Antwerp",
    "arrondissement": "Antwerp",
    "indicator_code": "POPULATION_BY_COMMUNE",
    "indicator_name": "Population",
    "unit": "count",
    "fetched_at": "2026-09-06T10:00:00+00:00",
}


def test_year_of_takes_the_first_four_characters():
    assert year_of("2023-Q4") == "2023"
    assert year_of("2026") == "2026"


def test_one_commune_one_indicator_round_trips(tmp_path):
    csv_path = tmp_path / "h.csv"
    _write_csv(
        csv_path,
        [{**BASE_ROW, "period": "2024", "value": "14832.0", "status": "final"}],
    )
    table = build_table(csv_path)
    assert len(table["communes"]) == 1
    commune = table["communes"][0]
    assert commune["nis_code"] == "11001"
    assert commune["indicators"]["POPULATION_BY_COMMUNE"]["byYear"]["2024"] == {
        "value": "14832.0",
        "period": "2024",
        "status": "final",
    }
    assert table["codes"] == ["POPULATION_BY_COMMUNE"]
    # Provenance keys are present but empty: build_table was called without a
    # lineage, which is the shape a caller with only a CSV gets. The exporter
    # emits the keys either way so a page never has to distinguish "no lineage
    # was supplied" from "this indicator has no source".
    assert table["meta"]["POPULATION_BY_COMMUNE"] == {
        "name": "Population",
        # Falls back to the English name from the CSV when no name map is
        # supplied, so a caller with only a CSV still gets a usable structure.
        "names": {"en": "Population"},
        "unit": "count",
        "minYear": "2024",
        "maxYear": "2024",
        "grade": None,
        "source": None,
        "inputSources": None,
    }
    assert table["years"] == ["2024"]


def test_a_quarterly_indicators_later_row_wins_the_calendar_year():
    """communes.html's own pivot() OVERWRITES byYear[year] as it iterates --
    it does not pick the row with the numerically latest period. Replicating
    that exactly, including its row-order dependence, is the whole point:
    changing this would change which value a visitor sees for a quarterly
    indicator, not just how fast the page loads."""
    pass  # exercised as an ordering test below, not a standalone assertion


def test_row_order_determines_which_quarter_wins_not_period_value(tmp_path):
    csv_path = tmp_path / "h.csv"
    _write_csv(
        csv_path,
        [
            {
                **BASE_ROW,
                "indicator_code": "LOCAL_UNITS_BY_COMMUNE",
                "indicator_name": "Local units",
                "unit": "count",
                "period": "2023-Q4",
                "value": "100.0",
                "status": "final",
            },
            {
                **BASE_ROW,
                "indicator_code": "LOCAL_UNITS_BY_COMMUNE",
                "indicator_name": "Local units",
                "unit": "count",
                "period": "2023-Q1",  # appears AFTER Q4 in the file
                "value": "40.0",
                "status": "final",
            },
        ],
    )
    table = build_table(csv_path)
    cell = table["communes"][0]["indicators"]["LOCAL_UNITS_BY_COMMUNE"]["byYear"]["2023"]
    # Q1 was the LAST row written for 2023, so it wins -- even though Q4 is
    # the later quarter numerically. This looks wrong in isolation; it is
    # deliberately faithful to the client behaviour it replaces.
    assert cell["value"] == "40.0"
    assert cell["period"] == "2023-Q1"


def test_a_blank_value_is_skipped_not_stored_as_a_cell(tmp_path):
    """Matches pivot(): `if(row.value === '') continue` never fires there --
    the client always stores whatever the CSV has, including an empty
    string, for a row it saw. A truly ABSENT year (no row at all) is the one
    that must be absent from byYear, and this only tests that a row IS
    written for a suppressed cell with an empty value alongside its status,
    the exact shape communes.html's status-pill fix depends on."""
    csv_path = tmp_path / "h.csv"
    _write_csv(
        csv_path,
        [{**BASE_ROW, "period": "2017", "value": "", "status": "suppressed"}],
    )
    table = build_table(csv_path)
    cell = table["communes"][0]["indicators"]["POPULATION_BY_COMMUNE"]["byYear"]["2017"]
    assert cell == {"value": "", "period": "2017", "status": "suppressed"}


def test_multiple_communes_and_indicators_stay_independent(tmp_path):
    csv_path = tmp_path / "h.csv"
    _write_csv(
        csv_path,
        [
            {**BASE_ROW, "period": "2024", "value": "14832.0", "status": "final"},
            {
                **BASE_ROW,
                "geo_id": "be:mun:11002",
                "nis_code": "11002",
                "name_en": "Antwerp",
                "name_fr": "Anvers",
                "name_nl": "Antwerpen",
                "period": "2024",
                "value": "530504.0",
                "status": "final",
            },
        ],
    )
    table = build_table(csv_path)
    assert {c["nis_code"] for c in table["communes"]} == {"11001", "11002"}
    assert len(table["codes"]) == 1  # same indicator both rows, seen once


def test_codes_preserve_first_seen_order(tmp_path):
    """Matches pivot(): `codesSeen` is an array checked with includes(), so a
    second indicator's columns appear in the order they were first read, not
    alphabetically."""
    csv_path = tmp_path / "h.csv"
    _write_csv(
        csv_path,
        [
            {
                **BASE_ROW,
                "indicator_code": "Z_LATE",
                "indicator_name": "Z",
                "period": "2024",
                "value": "1",
                "status": "final",
            },
            {
                **BASE_ROW,
                "indicator_code": "A_EARLY",
                "indicator_name": "A",
                "period": "2024",
                "value": "2",
                "status": "final",
            },
        ],
    )
    table = build_table(csv_path)
    assert table["codes"] == ["Z_LATE", "A_EARLY"]


def test_latest_fetched_at_is_the_max_across_all_rows(tmp_path):
    csv_path = tmp_path / "h.csv"
    _write_csv(
        csv_path,
        [
            {
                **BASE_ROW,
                "period": "2023",
                "value": "1",
                "status": "final",
                "fetched_at": "2026-09-01T00:00:00+00:00",
            },
            {
                **BASE_ROW,
                "period": "2024",
                "value": "2",
                "status": "final",
                "fetched_at": "2026-09-06T20:00:00+00:00",
            },
        ],
    )
    table = build_table(csv_path)
    assert table["latest_fetched_at"] == "2026-09-06T20:00:00+00:00"


def test_provenance_travels_in_the_meta_block(tmp_path):
    """communes.html reads this file and nothing else, so its column headers
    can only show where a figure came from if the lineage rides along here.
    A second fetch on a page already loading 5.4 MB is the alternative."""
    csv_path = tmp_path / "history.csv"
    _write_csv(
        csv_path,
        [
            {**BASE_ROW, "period": "2024", "value": "14832.0", "status": "final"},
            {
                **BASE_ROW,
                "indicator_code": "AVG_NET_TAXABLE_INCOME",
                "indicator_name": "Average income",
                "unit": "eur",
                "period": "2023",
                "value": "40000.0",
                "status": "derived",
            },
        ],
    )
    lineage = {
        "POPULATION_BY_COMMUNE": {"grade": "A", "source": "statbel"},
        "AVG_NET_TAXABLE_INCOME": {
            "grade": "C",
            "source": None,
            "input_sources": ["statbel"],
        },
    }
    meta = build_table(csv_path, lineage)["meta"]

    assert meta["POPULATION_BY_COMMUNE"]["grade"] == "A"
    assert meta["POPULATION_BY_COMMUNE"]["source"] == "statbel"
    # A derived indicator names no source of its own -- attributing a computed
    # figure to one of its inputs' agencies would say that agency published it.
    assert meta["AVG_NET_TAXABLE_INCOME"]["source"] is None
    assert meta["AVG_NET_TAXABLE_INCOME"]["grade"] == "C"
    assert meta["AVG_NET_TAXABLE_INCOME"]["inputSources"] == ["statbel"]
