"""Tests for the international pilot's geography allowlist -- PR-time guard
for config/geography/international.csv + international_excluded.csv
(src/geography/international.py). See docs/features/international.md,
"Geography", for why the licence decides this list, not raw availability."""

import pytest

from src.geography.international import (
    GEO_COLUMNS,
    InternationalGeographyError,
    check_allowlist_integrity,
    load_country_geos,
    load_excluded,
    load_international_rows,
)


def test_the_real_allowlist_has_no_integrity_problems():
    assert check_allowlist_integrity() == []


def test_every_code_maps_to_exactly_one_geo_id():
    rows = load_international_rows()
    assert len(rows) > 0
    for code, row in rows.items():
        assert row["geo"]["geo_id"], f"{code} has no geo_id"


def test_allowlist_and_excluded_are_disjoint():
    rows = load_international_rows()
    excluded = load_excluded()
    assert set(rows) & set(excluded) == set()


@pytest.mark.parametrize("code", ["UK", "XK", "US", "JP"])
def test_uk_xk_us_jp_are_excluded(code):
    excluded = load_excluded()
    rows = load_international_rows()
    assert code in excluded
    assert code not in rows


def test_exactly_two_pilot_aggregates():
    rows = load_international_rows()
    pilot_aggregates = sorted(
        code
        for code, row in rows.items()
        if row["scope"] == "pilot" and row["basis"] == "aggregate"
    )
    assert pilot_aggregates == ["EA21", "EU27_2020"]
    assert rows["EU27_2020"]["geo"]["geo_id"] == "eu27_2020:aggregate"
    assert rows["EA21"]["geo"]["geo_id"] == "ea21:aggregate"


def test_ea_is_legacy_scope_and_a_different_geo_id_than_ea21():
    rows = load_international_rows()
    assert rows["EA"]["scope"] == "legacy"
    assert rows["EA"]["geo"]["geo_id"] == "ea:aggregate"
    assert rows["EA"]["geo"]["geo_id"] != rows["EA21"]["geo"]["geo_id"]


def test_belgium_reuses_the_canonical_be_country_geo_id():
    """The pilot's own multi-country fetch will return a BE row too; it must
    land on the exact same geo_id the Belgian geography hierarchy already
    uses, not a second Belgium."""
    rows = load_international_rows()
    assert rows["BE"]["geo"]["geo_id"] == "be:country"


def test_load_country_geos_matches_the_be_country_geo_key_order():
    """port_existing_indicators.py inserts a geography positionally
    (tuple(geo.values())) -- the key order must match BE_COUNTRY_GEO's."""
    geos = load_country_geos()
    assert list(geos["DE"].keys()) == list(GEO_COLUMNS)


def test_a_duplicate_eurostat_code_is_refused(tmp_path):
    bad = tmp_path / "international.csv"
    bad.write_text(
        "eurostat_code,geo_id,level,name_nl,name_fr,name_en,valid_from,basis,scope,listed_on\n"
        "XX,xx:country,country,X,X,X,2020-01-01,eu,pilot,2026-09-13\n"
        "XX,xx:country,country,X,X,X,2020-01-01,eu,pilot,2026-09-13\n",
        encoding="utf-8",
    )
    with pytest.raises(InternationalGeographyError, match="duplicate eurostat_code"):
        load_international_rows(bad)


def test_an_invalid_scope_is_refused(tmp_path):
    bad = tmp_path / "international.csv"
    bad.write_text(
        "eurostat_code,geo_id,level,name_nl,name_fr,name_en,valid_from,basis,scope,listed_on\n"
        "XX,xx:country,country,X,X,X,2020-01-01,eu,sideways,2026-09-13\n",
        encoding="utf-8",
    )
    with pytest.raises(InternationalGeographyError, match="expected 'pilot' or 'legacy'"):
        load_international_rows(bad)


def test_a_missing_file_is_refused(tmp_path):
    with pytest.raises(InternationalGeographyError, match="Missing"):
        load_international_rows(tmp_path / "does_not_exist.csv")
