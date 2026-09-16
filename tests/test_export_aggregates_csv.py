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
    _universe_rows,
    canonical,
    export_aggregates_csv,
)

from src.stores import DEFAULT_STORES_PATH, extra_csv_paths  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
DB = REPO / "data" / "belgian_macro.db"
# Every committed extra_csv store, read from the one registry the workflows and
# the Makefile read (config/stores.yaml). A store missing here would leave the
# real derived configs referencing inputs nothing provides, which
# load_and_validate_derived rightly rejects. This used to be a hand-written
# list of five that had already dropped the police store.
STORES = tuple(REPO / p for p in extra_csv_paths(DEFAULT_STORES_PATH))

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
    universe_of, pinned = _universe_resolver(levels, windows, set())
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
    universe_of, _pinned = _universe_resolver(levels, windows, set())

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
        universe_rows = _universe_rows(conn, STORES)
    finally:
        conn.close()

    universe_of, pinned = _universe_resolver(levels, windows, universe_rows)

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


# ---------------------------------------------------------------------------
# Coverage-denominator regression: the universe must count a commune that
# EXISTED, never only a commune that happens to have a printable value.
#
# ONEM masks any figure under 10 for privacy (status=suppressed, value NULL).
# Those communes still filed; a denominator built only from rows with a
# value drops them, which does exactly what rule 3 (and the aggregate_additive
# docstring) forbids: it inflates coverage by shrinking the universe around
# the gap instead of reporting it. Measured on the real working database
# (ONEM/WalStat live only there since the docs/decisions/0006 cutover, never
# in the committed data/belgian_macro.db -- see tests/conftest.py's
# `working_db` fixture): PART_TIME_BENEFIT_RECIPIENTS's value-filtered union
# across 2017-2026 was 527 communes, a count that has never existed at any
# point in Belgian municipal geography (the real vintages, confirmed by
# querying `geographies` directly, are 589 / 581 / 580 / 565). Every one of
# ONEM's periods actually carries all 565 of today's communes, suppressed
# rows included, so 565 is the correct pinned vintage.
# ---------------------------------------------------------------------------


def _universe_of_and_pinned(conn):
    _parents, levels, _names, windows, _nis = _geography(conn)
    universe_rows = _universe_rows(conn, STORES)
    return _universe_resolver(levels, windows, universe_rows)


def test_the_real_vintage_sizes_are_589_581_580_565():
    """Ground truth for every other test below, read directly from the
    geographies table rather than assumed -- the maintainer's brief named
    589/581/565, but Bastogne+Bertogne's 2024-12-02 merger (mid-year, not on
    a year boundary) creates a fourth, narrow real vintage: 580."""
    if not DB.is_file():
        pytest.skip("committed database not present")
    # Count communes valid at each real boundary.
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        boundaries = ["1990-01-01", "2019-01-01", "2024-06-01", "2024-12-02", "2025-01-01"]
        counts = set()
        for asof in boundaries:
            (n,) = conn.execute(
                "SELECT COUNT(*) FROM geographies WHERE level='municipality' "
                "AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)",
                (asof, asof),
            ).fetchone()
            counts.add(n)
    finally:
        conn.close()
    assert counts == {589, 581, 580, 565}


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_universe_rows_includes_suppressed_communes_the_value_filter_drops(working_db):
    """The mechanism, isolated: _universe_rows must see a commune that ONEM
    suppressed (value NULL, status=suppressed) even though _observations
    (the sum input) correctly excludes it. Hand-counted against the real
    working database: PART_TIME_BENEFIT_RECIPIENTS 2021 has 487 communes with
    a final value and 78 more with status=suppressed -- 565 in total, every
    commune on today's map."""
    conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
    try:
        universe_rows = _universe_rows(conn, STORES)
        value_rows = _observations(conn, STORES)
    finally:
        conn.close()

    with_value = {
        g for i, g, p, _v in value_rows if i == "PART_TIME_BENEFIT_RECIPIENTS" and p == "2021"
    }
    any_status = {
        g for i, g, p in universe_rows if i == "PART_TIME_BENEFIT_RECIPIENTS" and p == "2021"
    }
    assert len(with_value) == 487
    assert len(any_status) == 565
    suppressed_only = any_status - with_value
    assert len(suppressed_only) == 78
    # Every suppressed geo_id counts toward the universe though it contributes
    # no value -- exactly what makes the denominator honest.
    assert suppressed_only <= any_status


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_part_time_benefit_recipients_pinned_vintage_is_565_not_527(working_db):
    """The exact red-team finding: coverage.of was 527 for
    PART_TIME_BENEFIT_RECIPIENTS at be:country, 2017-2021 -- a community
    count that never existed. 527 was the value-filtered union across all
    ten periods (measured directly against data/onem_observations.csv: per
    period it ranges from 399 to 511 communes with a printable value, and the
    union of those ten sets is 527). The honest pinned vintage, once
    suppressed rows count toward "reports on this map", is 565: every single
    period actually carries all 565 of today's communes."""
    conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
    try:
        universe_of, pinned = _universe_of_and_pinned(conn)
    finally:
        conn.close()

    assert "PART_TIME_BENEFIT_RECIPIENTS" in pinned
    for period in ("2017", "2018", "2019", "2020", "2021", "2025", "2026"):
        universe = universe_of("PART_TIME_BENEFIT_RECIPIENTS", period)
        assert len(universe) == 565, f"{period}: expected the real 565 vintage, got {len(universe)}"
    # 527 must never appear -- it is not a real geography snapshot.
    assert len(universe_of("PART_TIME_BENEFIT_RECIPIENTS", "2021")) != 527


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_part_time_benefit_recipients_2021_is_suppressed_in_published_output(working_db, tmp_path):
    """487 communes reported a value out of the real 565-commune vintage:
    86.2%, below the 90% gate. The cell must be ABSENT from aggregates.csv
    entirely -- not present with value 0 or an empty string, which would
    collapse "suppressed" into "explicit zero" or "missing", exactly what
    rule 26 forbids."""
    out = tmp_path / "aggregates.csv"
    export_aggregates_csv(working_db, out, STORES)
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    cell = [
        r
        for r in rows
        if r["geo_id"] == "be:country"
        and r["indicator_code"] == "PART_TIME_BENEFIT_RECIPIENTS"
        and r["period"] == "2021"
    ]
    assert (
        cell == []
    ), f"expected be:country PART_TIME_BENEFIT_RECIPIENTS 2021 suppressed, got {cell}"


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_luxembourg_province_2019_is_suppressed(working_db, tmp_path):
    """be:prov:80000 (Luxembourg), PART_TIME_BENEFIT_RECIPIENTS, 2019.

    The pinned universe is today's 565-commune map (see
    test_part_time_benefit_recipients_pinned_vintage_is_565_not_527), and
    _expected_children counts a commune toward Luxembourg using its CURRENT
    parent pointer -- so the denominator here is however many of today's
    communes trace up to Luxembourg, not however many existed there in 2019.
    Measured directly against the real working database rather than assumed:
    43, not the calendar's 44 -- one 2019 Luxembourg commune's current
    successor now sits under a different province (a merger, the same
    "denominator map != calendar map" fact the pinned-vintage design exists
    to handle, see Limburg 2018 in the PR description). Of those 43: 29
    status=final rows, 14 status=suppressed -- 29/43 = 67.4%, still well
    under the 90% gate -- must be absent from output either way."""
    conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
    try:
        universe_of, _pinned = _universe_of_and_pinned(conn)
        _parents, levels, _names, _windows, _nis = _geography(conn)
        parents = _parents
    finally:
        conn.close()

    from src.analytics.aggregate import ancestors_of

    universe_2019 = universe_of("PART_TIME_BENEFIT_RECIPIENTS", "2019")
    luxembourg_members = [
        g for g in universe_2019 if "be:prov:80000" in ancestors_of(g, parents, levels)
    ]
    assert len(luxembourg_members) == 43

    out = Path(str(working_db) + ".agg.csv")
    export_aggregates_csv(working_db, out, STORES)
    try:
        with out.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    finally:
        out.unlink(missing_ok=True)

    cell = [
        r
        for r in rows
        if r["geo_id"] == "be:prov:80000"
        and r["indicator_code"] == "PART_TIME_BENEFIT_RECIPIENTS"
        and r["period"] == "2019"
    ]
    assert cell == [], f"expected Luxembourg 2019 suppressed, got {cell}"


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_limburg_province_2018_100pct_is_not_this_bug(working_db, tmp_path):
    """be:prov:70000 (Limburg), PART_TIME_BENEFIT_RECIPIENTS, 2018 reads
    38/38 = 100% both BEFORE and AFTER this fix -- confirmed by running both
    versions against the same real working database. It is NOT an instance
    of the coverage-denominator bug this batch fixes: Limburg's pinned-map
    ancestor count genuinely shrank from 44 (its 2018 calendar membership) to
    38 (its membership under today's post-2025-merger map, which is what
    this indicator's ALREADY-pinned vintage uses), and none of the 6
    communes that merged away had a suppressed row for this period -- so the
    value-filter bug this batch fixes never touched this cell either way.
    Suppressing it to 38/44 would require measuring province ancestry against
    the CALENDAR map while measuring the indicator's own coverage against its
    PINNED map -- comparing two different maps, exactly the failure mode
    _universe_resolver's docstring already documents and this fix must not
    reintroduce. Pinned here as a regression guard and flagged in the PR
    rather than silently forced to a different number."""
    out = tmp_path / "aggregates.csv"
    export_aggregates_csv(working_db, out, STORES)
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    cell = [
        r
        for r in rows
        if r["geo_id"] == "be:prov:70000"
        and r["indicator_code"] == "PART_TIME_BENEFIT_RECIPIENTS"
        and r["period"] == "2018"
    ]
    assert len(cell) == 1
    assert cell[0]["coverage_n"] == cell[0]["coverage_of"] == "38"


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_control_activation_measures_paid_2025_unchanged_and_published(working_db, tmp_path):
    """ACTIVATION_MEASURES_PAID at be:country, 2025: genuinely complete over
    the real 565-commune vintage (no suppressed rows for this indicator), so
    this fix must change nothing about it -- still published, still 565/565,
    still the same value. A fix that touches indicators it has no business
    touching is itself a bug."""
    out = tmp_path / "aggregates.csv"
    export_aggregates_csv(working_db, out, STORES)
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    cell = [
        r
        for r in rows
        if r["geo_id"] == "be:country"
        and r["indicator_code"] == "ACTIVATION_MEASURES_PAID"
        and r["period"] == "2025"
    ]
    assert len(cell) == 1
    assert cell[0]["coverage_n"] == cell[0]["coverage_of"] == "565"
    assert cell[0]["coverage_pct"] == "100.0"
    assert float(cell[0]["value"]) == pytest.approx(675608949.59, abs=0.01)


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_control_fiscal_pinned_vintage_2023_still_581(working_db, tmp_path):
    """FISCAL_TOT_NET_TAXABLE_INC at be:country, 2023: the Statbel fiscal
    file's own legitimate pinned vintage (the 2019 map of 581) must be
    untouched by this fix -- it has no suppressed rows to recover, so its
    denominator and value must be exactly what they were before."""
    out = tmp_path / "aggregates.csv"
    export_aggregates_csv(working_db, out, STORES)
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    cell = [
        r
        for r in rows
        if r["geo_id"] == "be:country"
        and r["indicator_code"] == "FISCAL_TOT_NET_TAXABLE_INC"
        and r["period"] == "2023"
    ]
    assert len(cell) == 1
    assert cell[0]["coverage_of"] == "581"
    assert cell[0]["coverage_n"] == "581"
    assert float(cell[0]["value"]) == pytest.approx(274707991587.37, abs=1.0)


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_control_census_2021_and_local_units_vintages_unregressed(working_db, tmp_path):
    """Two more must-not-regress vintages named in the brief: a census2021
    indicator (calendar-universe, not pinned) keeps of=581 for 2021, and
    LOCAL_UNITS_BY_COMMUNE (pinned to today's map, its only period is
    2023-Q4) keeps of=565. Neither has suppressed rows, so this fix changes
    neither."""
    out = tmp_path / "aggregates.csv"
    export_aggregates_csv(working_db, out, STORES)
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    pop_female = [
        r
        for r in rows
        if r["geo_id"] == "be:country"
        and r["indicator_code"] == "POP_FEMALE"
        and r["period"] == "2021"
    ]
    assert len(pop_female) == 1
    assert pop_female[0]["coverage_of"] == "581"

    local_units = [
        r
        for r in rows
        if r["geo_id"] == "be:country" and r["indicator_code"] == "LOCAL_UNITS_BY_COMMUNE"
    ]
    assert len(local_units) == 1
    assert local_units[0]["period"] == "2023-Q4"
    assert local_units[0]["coverage_of"] == "565"


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_be_country_cells_carry_a_real_national_vintage_size(working_db, tmp_path):
    """At be:country, `of` is the whole national commune count for that
    indicator's universe, so it must land on one of the four real vintage
    sizes (589/581/580/565 -- confirmed independently against the
    geographies table by test_the_real_vintage_sizes_are_589_581_580_565).

    Deliberately restricted to be:country: at arrondissement/province/region
    level `of` is correctly the SUBSET of a vintage that sits under that
    particular geography (e.g. one arrondissement's slice of a 565-commune
    universe is nowhere near 565), so checking every level against the four
    national totals is not the invariant that actually holds -- see
    test_every_cells_of_matches_an_independently_recomputed_universe_count
    for the property that does hold everywhere."""
    out = tmp_path / "aggregates.csv"
    written = export_aggregates_csv(working_db, out, STORES)
    assert written > 0
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    real_vintages = {589, 581, 580, 565}
    country_rows = [r for r in rows if r["geo_id"] == "be:country"]
    assert country_rows, "expected at least one be:country cell in the real output"
    bad_of = [r for r in country_rows if int(r["coverage_of"]) not in real_vintages]
    assert not bad_of, (
        f"{len(bad_of)} be:country cell(s) have a coverage_of that matches no real "
        f"commune vintage, e.g. {bad_of[0]['name_en']} {bad_of[0]['indicator_code']} "
        f"{bad_of[0]['period']}: of={bad_of[0]['coverage_of']}"
    )


@pytest.mark.skipif(
    not DB.is_file() or not all(s.is_file() for s in STORES),
    reason="committed stores not present",
)
@pytest.mark.slow
def test_every_cells_of_matches_an_independently_recomputed_universe_count(working_db, tmp_path):
    """Property test over the whole real output, at every level. For each
    published SUM (additive) cell, independently recompute `of` from first
    principles -- the pinned-or-calendar universe for that (indicator,
    period), narrowed to the communes whose ancestry includes that geo_id --
    and require it to match exactly what got published, using the SAME
    ancestry helper the engine uses (src.analytics.aggregate.ancestors_of)
    so this is a genuine cross-check rather than a restatement of
    _expected_children.

    This is the property that actually holds at every level, unlike a fixed
    set of national totals: an arrondissement's `of` is correctly a small
    subset of a 565-commune universe, not 565 itself.

    A RECOMPUTE (derived ratio) cell is checked differently, not skipped:
    aggregate()'s _weakest_input_coverage deliberately gives a ratio the
    coverage of whichever input has the WORSE coverage PERCENTAGE (see
    src/analytics/aggregate.py) -- e.g. SHARE_POP_ON_UNEMPLOYMENT_BENEFIT's
    inputs UNEMPLOYED_JOBSEEKERS (pinned to today's 565-commune map) and
    POPULATION_BY_COMMUNE (the 2017 calendar's own 589) do not share a
    universe, so a smaller `of` does not reliably mean a smaller `pct` --
    measured directly: Arrondissement Aat 2017 has 11 of the pinned map's
    communes but only 8 of the calendar's, yet UNEMPLOYED_JOBSEEKERS is the
    worse PERCENTAGE there. So the ratio's expectation is read from the
    ALREADY-PUBLISHED rows of its own inputs at that geo/period (both must
    be published too, or the ratio would have been dropped alongside them --
    see the None-coverage short-circuit in aggregate()), picking by
    coverage_pct exactly as the engine does, rather than re-deriving a
    percentage this test would have to reconstruct `n` to compute.

    Also asserts no published cell sits below the 90% suppression gate --
    the engine suppresses below that itself, so a violation here would mean
    this recomputation disagrees with the engine about the universe, not
    that the gate was skipped.
    """
    conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
    try:
        _parents, levels, _names, windows, _nis = _geography(conn)
        parents = _parents
        universe_of, _pinned = _universe_of_and_pinned(conn)
    finally:
        conn.close()

    from src.analytics.aggregate import ancestors_of
    from src.validation.config_schema import load_and_validate_derived

    out = tmp_path / "aggregates.csv"
    written = export_aggregates_csv(working_db, out, STORES)
    assert written > 0
    with out.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    present = {r["indicator_code"] for r in rows}
    derived_cfgs = load_and_validate_derived(
        Path(__file__).resolve().parents[1] / "config" / "indicators" / "derived", present
    )
    ratio_inputs = {
        indicator_id: (config.get("derived") or {}).get("inputs") or []
        for indicator_id, config in derived_cfgs.items()
    }
    published = {(r["indicator_code"], r["geo_id"], r["period"]): r for r in rows}

    def expected_of(indicator_id: str, geo_id: str, period: str) -> int:
        universe = universe_of(indicator_id, period)
        return sum(
            1 for commune_id in universe if geo_id in ancestors_of(commune_id, parents, levels)
        )

    mismatches = []
    for r in rows:
        indicator_id, geo_id, period = r["indicator_code"], r["geo_id"], r["period"]
        if indicator_id in ratio_inputs:
            input_rows = [
                published.get((dep, geo_id, period)) for dep in ratio_inputs[indicator_id]
            ]
            if not all(input_rows):
                # An input did not survive the gate at this cell -- the
                # ratio should not have survived either; the mismatch would
                # show up as a spurious ratio row, not a wrong `of`, and is
                # out of scope for this property (a suppression-completeness
                # check, not a denominator check).
                continue
            worst = min(input_rows, key=lambda ir: float(ir["coverage_pct"]))
            expected = int(worst["coverage_of"])
        else:
            expected = expected_of(indicator_id, geo_id, period)
        if int(r["coverage_of"]) != expected:
            mismatches.append((r, expected))

    assert not mismatches, (
        f"{len(mismatches)} cell(s) have a published coverage_of that disagrees with an "
        f"independent recomputation, e.g. {mismatches[0][0]['name_en']} "
        f"{mismatches[0][0]['indicator_code']} {mismatches[0][0]['period']}: "
        f"published of={mismatches[0][0]['coverage_of']}, recomputed={mismatches[0][1]}"
    )

    below_gate = [r for r in rows if float(r["coverage_pct"]) < 90.0]
    assert not below_gate, (
        f"{len(below_gate)} published cell(s) sit below the 90% suppression gate, e.g. "
        f"{below_gate[0]['name_en']} {below_gate[0]['indicator_code']} {below_gate[0]['period']}: "
        f"{below_gate[0]['coverage_pct']}%"
    )
