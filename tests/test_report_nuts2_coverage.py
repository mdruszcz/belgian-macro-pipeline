"""Unit tests for scripts/report_nuts2_coverage.py's pure functions --
NUTS 2 code recognition (incl. the 'ZZ'/'XX' pseudo-region suffixes), the
licence split by country prefix (incl. Greece's 'EL' code and the
international.csv/international_excluded.csv 'fail loudly' rule), and the
data-vs-geometry code diff. No network; all fixtures are small and inline.

This batch (B1) writes documents only -- these tests exist so the numbers
in docs/features/europe_nuts2.md are backed by something that keeps
passing, not just a one-off script run."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.report_nuts2_coverage import (  # noqa: E402
    diff_codes,
    geometry_ids,
    is_extra_regio,
    is_nuts2_code,
    nuts2_country_prefix,
    raw_flag_census,
    raw_rows,
    split_by_licence,
)

# --------------------------------------------------------------------------
# is_nuts2_code
# --------------------------------------------------------------------------


def test_real_nuts2_codes_recognized():
    for code in ["BE10", "LU00", "FR10", "EL11", "TRA1"]:
        assert is_nuts2_code(code), code


def test_nuts0_and_nuts1_codes_rejected_too_short():
    assert not is_nuts2_code("BE")  # NUTS 0 (country)
    assert not is_nuts2_code("BE1")  # NUTS 1


def test_nuts3_codes_rejected_too_long():
    assert not is_nuts2_code("BE100")  # NUTS 3


def test_lowercase_prefix_rejected():
    # Eurostat's own codes are always uppercase; a lowercase geo_id
    # (this pipeline's Belgian NIS-keyed style, e.g. 'be:prov:...') must
    # never be mistaken for a NUTS code by this heuristic.
    assert not is_nuts2_code("be10")


def test_empty_and_garbage_rejected():
    assert not is_nuts2_code("")
    assert not is_nuts2_code("EU27_2020")


# --------------------------------------------------------------------------
# is_extra_regio / pseudo-region suffixes
# --------------------------------------------------------------------------


def test_zz_extra_regio_recognized():
    for code in ["DEZZ", "FRZZ", "ITZZ"]:
        assert is_extra_regio(code), code


def test_xx_not_regionalised_recognized():
    # Demography's 'Not regionalised/Unknown NUTS 2' pseudo-codes -- a
    # different Eurostat convention from 'ZZ' extra-regio, found in
    # demo_r_pjanaggr3's own geo-dimension labels (2026-09-14).
    for code in ["ALXX", "FRXX", "HUXX", "MKXX"]:
        assert is_extra_regio(code), code


def test_real_single_region_country_00_code_not_extra_regio():
    # '00' is a real NUTS 2 code (the whole country is one region), not a
    # pseudo-region marker -- must not be caught by the same rule as 'ZZ'/'XX'.
    for code in ["LU00", "IS00", "LI00"]:
        assert not is_extra_regio(code), code


def test_ordinary_region_not_extra_regio():
    assert not is_extra_regio("BE10")


def test_extra_regio_implies_nuts2_shaped():
    # A pseudo-region code is still 4 characters -- is_extra_regio must not
    # accidentally match something is_nuts2_code() would reject.
    assert is_extra_regio("DEZZ") and is_nuts2_code("DEZZ")


# --------------------------------------------------------------------------
# nuts2_country_prefix
# --------------------------------------------------------------------------


def test_country_prefix_is_first_two_characters():
    assert nuts2_country_prefix("BE10") == "BE"
    assert nuts2_country_prefix("EL11") == "EL"  # Greece, not 'GR'


# --------------------------------------------------------------------------
# split_by_licence -- full branch coverage: kept / dropped / unresolved
# --------------------------------------------------------------------------


ALLOWED = {"BE", "DE", "EL"}  # Greece is EL, deliberately included to guard
EXCLUDED = {"UK", "XK", "US", "JP"}


def test_allowlisted_prefix_is_kept():
    kept, dropped, unresolved = split_by_licence({"BE10", "DE21"}, ALLOWED, EXCLUDED)
    assert kept == {"BE10", "DE21"}
    assert dropped == {}
    assert unresolved == set()


def test_greece_el_prefix_is_kept_not_confused_with_gr():
    kept, _dropped, _unresolved = split_by_licence({"EL11"}, ALLOWED, EXCLUDED)
    assert kept == {"EL11"}


def test_excluded_prefix_is_dropped_and_grouped_by_country():
    kept, dropped, unresolved = split_by_licence({"UKC1", "UKD1", "XK00"}, ALLOWED, EXCLUDED)
    assert kept == set()
    assert dropped == {"UK": {"UKC1", "UKD1"}, "XK": {"XK00"}}
    assert unresolved == set()


def test_prefix_in_neither_list_is_unresolved_not_silently_dropped_or_kept():
    # CLAUDE.md rule 13 / international.py's own rule: a geography decided
    # neither way must fail loudly, never be silently included or excluded.
    kept, dropped, unresolved = split_by_licence({"CH01"}, ALLOWED, EXCLUDED)
    assert kept == set()
    assert dropped == {}
    assert unresolved == {"CH01"}


def test_mixed_batch_splits_into_all_three_buckets():
    codes = {"BE10", "UKC1", "CH01"}
    kept, dropped, unresolved = split_by_licence(codes, ALLOWED, EXCLUDED)
    assert kept == {"BE10"}
    assert dropped == {"UK": {"UKC1"}}
    assert unresolved == {"CH01"}


def test_empty_input_produces_empty_everything():
    kept, dropped, unresolved = split_by_licence(set(), ALLOWED, EXCLUDED)
    assert kept == set() and dropped == {} and unresolved == set()


# --------------------------------------------------------------------------
# geometry_ids / diff_codes
# --------------------------------------------------------------------------


def _topojson_fixture(ids: list[str]) -> dict:
    return {
        "type": "Topology",
        "objects": {
            "nutsrg": {
                "type": "GeometryCollection",
                "geometries": [{"type": "Polygon", "properties": {"id": i, "na": i}} for i in ids],
            }
        },
    }


def test_geometry_ids_reads_nutsrg_layer():
    topo = _topojson_fixture(["BE10", "BE21", "FR10"])
    assert geometry_ids(topo) == {"BE10", "BE21", "FR10"}


def test_diff_codes_both_directions():
    data = {"BE10", "BE21", "FR10"}
    geometry = {"BE10", "BE21", "NL11"}
    diff = diff_codes(data, geometry)
    assert diff["in_data_not_geometry"] == {"FR10"}
    assert diff["in_geometry_not_data"] == {"NL11"}


def test_diff_codes_identical_sets_is_empty_both_ways():
    same = {"BE10", "BE21"}
    diff = diff_codes(same, set(same))
    assert diff["in_data_not_geometry"] == set()
    assert diff["in_geometry_not_data"] == set()


# --------------------------------------------------------------------------
# raw_flag_census / raw_rows -- the fallback used when EurostatSource._parse
# refuses an unrecognized OBS_FLAG (e.g. lfst_r_lfu3rt's compound 'bu'/'bd').
# --------------------------------------------------------------------------


def _jsonstat_fixture() -> bytes:
    import json as _json

    # A tiny real JSON-stat 2.0 cube: dims [freq(1), geo(2), time(2)], time
    # is the fastest-varying dimension, so offset = geo_pos*2 + time_pos*1.
    payload = {
        "id": ["freq", "geo", "time"],
        "size": [1, 2, 2],
        "dimension": {
            "freq": {"category": {"index": {"A": 0}}},
            "geo": {"category": {"index": {"BE10": 0, "BE21": 1}}},
            "time": {"category": {"index": {"2023": 0, "2024": 1}}},
        },
        # linear offset = geo_pos*2 + time_pos*1 (time is the fastest-varying dim)
        "value": {"0": 10.0, "1": 11.0, "3": 13.0},
        "status": {"3": "bu"},
    }
    return _json.dumps(payload).encode("utf-8")


def test_raw_flag_census_counts_compound_flags():
    census = raw_flag_census(_jsonstat_fixture())
    assert census == {"bu": 1}


def test_raw_rows_keeps_raw_flag_string_verbatim():
    rows = raw_rows(_jsonstat_fixture())
    by_geo_period = {(r["geo"], r["period"]): r for r in rows}
    assert by_geo_period[("BE10", "2023")]["obs_status"] == ""
    assert by_geo_period[("BE10", "2023")]["value"] == 10.0
    assert by_geo_period[("BE21", "2024")]["obs_status"] == "bu"
    assert by_geo_period[("BE21", "2024")]["value"] == 13.0


def test_raw_rows_skips_positions_with_neither_value_nor_flag():
    rows = raw_rows(_jsonstat_fixture())
    # offset 2 (BE21, 2023) has neither a value nor a status -- no row.
    pairs = {(r["geo"], r["period"]) for r in rows}
    assert ("BE21", "2023") not in pairs
