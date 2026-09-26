"""Contracts for the per-commune ISE-class summary (export_schools_by_commune.py)."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from export_schools_by_commune import ByCommuneExportError, summarize  # noqa: E402

NAMUR = {"fr": "Namur", "nl": "Namen", "en": "Namur"}
LIEGE = {"fr": "Saint-Nicolas", "nl": "Saint-Nicolas", "en": "Saint-Nicolas"}


def record(**changes):
    base = {
        "site": 1,
        "school": "École A",
        "type": "Fondamental ordinaire",
        "formula": "FO",
        "street": "Rue X",
        "number": "1",
        "postcode": 5000,
        "ed": None,
        "hed": None,
        "nis": "92094",
        "commune": NAMUR,
        "network": "WBE",
        "lat": 50.46,
        "lon": 4.86,
    }
    base.update(changes)
    return base


def source(records, unassigned=0):
    return {
        "source": "https://example/ise",
        "publisher": "Fédération Wallonie-Bruxelles",
        "license": "CC BY",
        "year": 2025,
        "records": records,
        "register_source": "https://example/register",
        "register_year": 2026,
        "unassigned_sites": unassigned,
    }


def test_mean_of_4_7_12_is_7_point_7():
    records = [
        record(site=1, ed="4"),
        record(site=2, ed="7"),
        record(site=3, ed="12"),
    ]
    result = summarize(source(records))
    fo = result["communes"]["92094"]["FO"]
    assert fo["mean_class"] == 7.7
    assert fo["min_class"] == 4
    assert fo["max_class"] == 12
    assert fo["sites"] == 3
    assert fo["sites_with_class"] == 3


def test_3a_counts_as_3():
    records = [record(site=1, ed="3a"), record(site=2, ed="4")]
    result = summarize(source(records))
    fo = result["communes"]["92094"]["FO"]
    assert fo["mean_class"] == 3.5
    assert fo["min_class"] == 3


def test_null_class_excluded_from_mean_but_counted_in_sites():
    records = [record(site=1, ed="10"), record(site=2, ed=None)]
    result = summarize(source(records))
    fo = result["communes"]["92094"]["FO"]
    assert fo["sites"] == 2
    assert fo["sites_with_class"] == 1
    assert fo["mean_class"] == 10.0


def test_formula_with_zero_classed_sites_is_null_not_zero():
    records = [record(site=1, ed=None, formula="FO")]
    result = summarize(source(records))
    entry = result["communes"]["92094"]
    assert entry["FO"]["mean_class"] is None
    assert entry["SO"]["mean_class"] is None
    assert entry["SO"]["sites"] == 0


def test_fo_and_so_never_mixed():
    records = [
        record(site=1, ed="2", formula="FO"),
        record(site=1, ed="18", formula="SO"),
    ]
    result = summarize(source(records))
    entry = result["communes"]["92094"]
    assert entry["FO"]["mean_class"] == 2.0
    assert entry["SO"]["mean_class"] == 18.0
    assert entry["sites"] == 1  # one distinct site, counted once


def test_site_with_no_nis_is_excluded_from_every_commune_and_only_counted_globally():
    records = [
        record(site=1, nis="92094"),
        record(site=2, nis=None, commune=None),
    ]
    result = summarize(source(records, unassigned=1))
    assert "92094" in result["communes"]
    assert result["unassigned_sites"] == 1
    total_sites_recorded = sum(c["sites"] for c in result["communes"].values())
    assert total_sites_recorded == 1  # the unassigned site is not in any commune


def test_no_belgium_or_province_aggregate_is_computed():
    records = [record(site=1, nis="92094"), record(site=2, nis="62093", commune=LIEGE)]
    result = summarize(source(records))
    assert set(result.keys()) == {
        "source",
        "publisher",
        "license",
        "year",
        "register_year",
        "unassigned_sites",
        "communes",
    }
    assert "belgium" not in result
    assert "province" not in result
    assert "region" not in result


def test_refuses_when_input_has_no_commune_assignment():
    unassigned_record = {k: v for k, v in record().items() if k != "nis"}
    with pytest.raises(ByCommuneExportError, match="commune assignment"):
        summarize(source([unassigned_record]))


def test_site_ids_sorted_and_deterministic():
    records = [record(site=5), record(site=1), record(site=3)]
    result = summarize(source(records))
    assert result["communes"]["92094"]["site_ids"] == [1, 3, 5]


def test_two_runs_produce_identical_bytes():
    records = [record(site=1, ed="4"), record(site=2, ed="7")]
    first = json.dumps(summarize(source(records)), sort_keys=True)
    second = json.dumps(summarize(source(records)), sort_keys=True)
    assert first == second
