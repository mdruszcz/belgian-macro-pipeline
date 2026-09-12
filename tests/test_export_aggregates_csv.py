"""Tests for the aggregate exporter (Block L).

Two of these are regressions for bugs the first working version of this
exporter actually had, both of which produced numbers rather than crashes:

  * coverage above 100% -- more communes contributing than existed -- because
    the fiscal file expresses every year on the 2019 commune map while the
    denominator was the calendar's own map for each year;
  * 607 communes in 2019 and 592 in 2025, neither of which ever existed,
    because a year-granularity date comparison counted a merger boundary
    twice: predecessors leaving on 2019-01-01 and successors arriving on
    2019-01-01 both matched the year 2019.

Both are pinned against the real committed store, not a fixture, because the
numbers that make them wrong are the real Belgian commune counts.
"""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from export_aggregates_csv import (  # noqa: E402
    _geography,
    _observations,
    _period_start,
    _universe_resolver,
    canonical,
    export_aggregates_csv,
)

REPO = Path(__file__).resolve().parents[1]
DB = REPO / "data" / "belgian_macro.db"
# Every committed manual store, exactly as both workflows pass them via
# --extra-observations. A store missing here would leave the real derived
# configs referencing inputs nothing provides, which load_and_validate_derived
# rightly rejects -- so this list has to stay in step with the workflows.
STORES = (
    REPO / "data" / "population_observations.csv",
    REPO / "data" / "fiscal_income_observations.csv",
    REPO / "data" / "census2021_observations.csv",
    REPO / "data" / "realestate_observations.csv",
)

# The commune counts this repo derived and then independently corroborated in
# Block C: 589 before the 2019 wave, 581 between the waves, 565 after 2025.
COMMUNES_BY_YEAR = {
    "2016": 589,
    "2018": 589,
    "2019": 581,
    "2023": 581,
    "2024": 581,
    "2025": 565,
    "2026": 565,
}


def test_period_start_handles_the_forms_the_data_uses():
    assert _period_start("2023") == "2023-01-01"
    assert _period_start("2023-Q1") == "2023-01-01"
    assert _period_start("2023-Q4") == "2023-10-01"
    assert _period_start("2023-07") == "2023-07-01"
    # An unrecognised form is passed through rather than coerced into a wrong
    # date, so it fails visibly instead of quietly shifting a boundary.
    assert _period_start("garbage") == "garbage"


@pytest.mark.skipif(not DB.is_file(), reason="committed database not present")
def test_the_commune_universe_matches_the_real_counts_per_year():
    """The regression for "607 communes in 2019".

    A merger boundary must not count both the predecessors leaving and the
    successors arriving. These are the counts Block C verified against
    Statbel's own files, so a future change that reintroduces double-counting
    fails here with a recognisably wrong number.
    """
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        _parents, levels, _names, windows, _nis = _geography(conn)
    finally:
        conn.close()

    # No observations, so nothing is treated as a pinned vintage and the
    # resolver returns the calendar universe.
    universe_of, pinned = _universe_resolver(levels, windows, [])
    assert pinned == []

    for year, expected in COMMUNES_BY_YEAR.items():
        assert len(universe_of("ANY", year)) == expected, f"wrong commune count for {year}"


@pytest.mark.skipif(not DB.is_file(), reason="committed database not present")
def test_2024_anchors_to_the_start_of_the_year_not_the_end():
    """Bastogne and Bertogne merged on 2024-12-02, so 2024 has 581 communes at
    its start and 580 at its end. Statbel publishes population as of 1
    January and its 2024 file carries 581, so the universe must anchor to the
    start -- anchoring to the end would put coverage above 100%."""
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        _parents, levels, _names, windows, _nis = _geography(conn)
    finally:
        conn.close()
    universe_of, _pinned = _universe_resolver(levels, windows, [])

    universe = universe_of("ANY", "2024")
    assert len(universe) == 581
    # The successor did not exist on 2024-01-01; its two predecessors did.
    assert "be:mun:82039" not in universe


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_a_back_casting_source_is_measured_against_its_own_vintage():
    """The regression for coverage above 100%.

    The fiscal file names 18 communes for 2005 that did not exist until 2019.
    Those indicators must therefore be detected as pinned to one map and
    measured against it, rather than against 2005's real 589.
    """
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        _parents, levels, _names, windows, _nis = _geography(conn)
        rows = _observations(conn, STORES)
    finally:
        conn.close()

    universe_of, pinned = _universe_resolver(levels, windows, rows)

    assert "FISCAL_TOT_NET_TAXABLE_INC" in pinned, "back-casting fiscal file not detected"
    # Population is published on each year's own map and must NOT be pinned,
    # or its 589/581/565 progression would collapse to one number.
    assert "POPULATION_BY_COMMUNE" not in pinned

    # The pinned vintage is the file's own 581 codes, in every year it covers.
    assert len(universe_of("FISCAL_TOT_NET_TAXABLE_INC", "2005")) == 581
    assert len(universe_of("FISCAL_TOT_NET_TAXABLE_INC", "2023")) == 581
    # Population still varies by year.
    assert len(universe_of("POPULATION_BY_COMMUNE", "2016")) == 589
    assert len(universe_of("POPULATION_BY_COMMUNE", "2026")) == 565


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_no_aggregate_reports_impossible_coverage(tmp_path):
    """End to end on the real store: contributed must never exceed expected.

    Worth asserting as a property rather than only via the two causes above,
    because it is the visible symptom of any future mismatch between the map
    the data uses and the map the denominator counts.
    """
    out = tmp_path / "aggregates.csv"
    written = export_aggregates_csv(DB, out, STORES)
    assert written > 0

    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    impossible = [r for r in rows if int(r["coverage_n"]) > int(r["coverage_of"])]
    assert not impossible, (
        f"{len(impossible)} aggregate(s) claim more contributing communes than existed, "
        f"e.g. {impossible[0]['name_en']} {impossible[0]['indicator_code']} "
        f"{impossible[0]['period']}: {impossible[0]['coverage_n']}/{impossible[0]['coverage_of']}"
    )


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_the_limburg_figure_the_naive_method_got_wrong(tmp_path):
    """The measured headline from docs/features/comparison.md, Correction 2.

    Aggregating only today's communes puts Limburg's 2023 taxable income at
    EUR 16.076bn because Hasselt has no fiscal row after the 2025 mergers.
    The correct figure, over the communes that existed in 2023, is
    EUR 21.278bn -- a 24.4% difference that looks entirely plausible either
    way, which is exactly why it is pinned here.
    """
    out = tmp_path / "aggregates.csv"
    export_aggregates_csv(DB, out, STORES)
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    limburg = [
        r
        for r in rows
        if r["geo_id"] == "be:prov:70000"
        and r["indicator_code"] == "FISCAL_TOT_NET_TAXABLE_INC"
        and r["period"] == "2023"
    ]
    assert len(limburg) == 1
    billions = float(limburg[0]["value"]) / 1e9
    assert billions == pytest.approx(21.278, abs=0.001)
    assert billions != pytest.approx(16.076, abs=0.001)
    # All 42 of its 2023 communes contributed, so this is not a partial total
    # that happens to look right.
    assert limburg[0]["coverage_n"] == limburg[0]["coverage_of"] == "42"


@pytest.mark.skipif(not DB.is_file(), reason="committed database not present")
def test_a_versioned_ancestor_is_the_same_geography_as_its_current_self():
    """Regression: Antwerp province was short two communes at 100% coverage.

    The geographies table records historical VERSIONS of an arrondissement or
    province whose boundaries changed -- `be:prov:10000@1977-01-01` beside the
    current `be:prov:10000`. Borsbeek and Zwijndrecht hang off the versioned
    Antwerp arrondissement, so aggregating by raw geo_id sent their figures to
    a separate Antwerp province and left the real one EUR 0.737bn short of
    2023 taxable income -- while its coverage read a confident 67/67.
    """
    assert canonical("be:prov:10000@1977-01-01") == "be:prov:10000"
    assert canonical("be:prov:10000") == "be:prov:10000"
    assert canonical(None) is None

    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        versioned = [
            row[0] for row in conn.execute("SELECT geo_id FROM geographies WHERE geo_id LIKE '%@%'")
        ]
        parents, levels, _names, _windows, _nis = _geography(conn)
    finally:
        conn.close()

    # The rows exist -- if a future geography reload stops producing them, this
    # test is no longer guarding anything and should be revisited rather than
    # silently passing.
    assert versioned, "no versioned geography rows found; the collapse is untested"

    # After canonicalisation no versioned id survives, and every parent
    # pointer targets a geography that exists in the same map.
    assert not [g for g in parents if "@" in g]
    assert not [p for p in parents.values() if p and "@" in p]
    for geo_id, parent in parents.items():
        if parent is not None:
            assert parent in levels, f"{geo_id} parents to unknown {parent}"


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_every_province_matches_the_independently_computed_figure(tmp_path):
    """The whole Correction 2 table from docs/features/comparison.md, whose
    "correct" column was computed by a separate ad-hoc script that matched
    provinces by NAME rather than by geo_id. Two independent routes to the
    same six numbers; the geo_id route was wrong for Antwerp until the
    versioned-ancestor collapse landed."""
    out = tmp_path / "aggregates.csv"
    export_aggregates_csv(DB, out, STORES)
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    expected_bn = {
        "be:prov:70000": 21.278,  # Limburg
        "be:prov:40000": 39.875,  # East Flanders
        "be:prov:80000": 6.759,  # Luxembourg
        "be:prov:30000": 29.970,  # West Flanders
        "be:prov:20001": 32.236,  # Flemish Brabant
        "be:prov:10000": 46.890,  # Antwerp -- the one that was short
    }
    actual = {
        r["geo_id"]: float(r["value"]) / 1e9
        for r in rows
        if r["indicator_code"] == "FISCAL_TOT_NET_TAXABLE_INC" and r["period"] == "2023"
    }
    for geo_id, expected in expected_bn.items():
        assert actual[geo_id] == pytest.approx(expected, abs=0.002), geo_id
