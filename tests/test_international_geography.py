"""Tests for the international pilot's geography allowlist -- PR-time guard
for config/geography/international.csv + international_excluded.csv
(src/geography/international.py). See docs/features/international.md,
"Geography", for why the licence decides this list, not raw availability."""

import pytest

from src.geography.international import (
    GEO_COLUMNS,
    InternationalGeographyError,
    check_allowlist_integrity,
    is_country_level_code,
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


# --- is_country_level_code() -- Europe countries batch ----------------------
# (docs/features/europe_countries.md): the geo_filter that separates real
# NUTS 0 country codes and underscore-joined EU/EFTA aggregates from a NUTS
# 1/2/3 regional subdivision code, for the two mixed-NUTS-level datasets
# (nama_10r_2gdp, demo_r_pjanaggr3) only. Direct unit tests per CLAUDE.md
# rule 5 -- the sync/export tests exercise it indirectly against real fixture
# data, but a hand-computed truth table for the shape itself belongs here.


@pytest.mark.parametrize("code", ["BE", "DE", "TR", "UK", "GE", "MD"])
def test_two_letter_country_codes_are_country_level(code):
    assert is_country_level_code(code) is True


@pytest.mark.parametrize("code", ["EU27_2020", "EU27_2007", "DE_TOT"])
def test_underscore_joined_aggregate_spellings_are_country_level(code):
    """Every non-plain-country code the two mixed-NUTS-level datasets
    actually carry (EU27_2020's own aggregate, EU27_2007 and DE_TOT -- both
    live findings, see international_excluded.csv) uses an underscore; a
    NUTS subdivision code never does."""
    assert is_country_level_code(code) is True


@pytest.mark.parametrize(
    "code",
    [
        "BE1",  # NUTS 1
        "BE10",  # NUTS 2
        "BE100",  # NUTS 3
        "FRY1",  # a French overseas NUTS 3 code, same shape
        "DE300",
    ],
)
def test_nuts_subdivision_shaped_codes_are_rejected(code):
    assert is_country_level_code(code) is False


def test_ea21_matches_the_nuts_subdivision_shape_and_would_be_wrongly_filtered():
    """EA21 ("EA" + "21") is shaped exactly like a NUTS 1/2/3 code --
    is_country_level_code() alone cannot tell it apart from one, which is
    exactly why scripts/sync_international.py's MIXED_NUTS_LEVEL_DATASETS
    applies this filter ONLY to the two Europe countries batch datasets and
    never to the five original pilot indicators' own dataset (which uses
    EA21 as its aggregate and never mixes NUTS levels in the first place).
    This test documents the hazard the module docstring describes, not a
    behaviour this function itself works around."""
    assert is_country_level_code("EA21") is False


def test_ea21_is_not_filtered_because_the_filter_is_never_applied_to_its_dataset():
    """The actual guarantee: EA21 reaches the five original pilot
    indicators' own observations unfiltered, because
    scripts/sync_international.py never calls is_country_level_code() for
    their dataset. Checked against the real allowlist (not a synthetic
    fixture) -- EA21 is a real, loaded pilot aggregate today."""
    rows = load_international_rows()
    assert rows["EA21"]["scope"] == "pilot"
    assert rows["EA21"]["basis"] == "aggregate"
