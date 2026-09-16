"""Tests for src/analytics/backaggregate.py -- merger back-aggregation.

Values in the AVG_NET_TAXABLE_INCOME / FISCAL_TOT_NET_TAXABLE_INC tests are
hand-computed from the real 2023 fiscal_income_observations.csv rows for
Borgloon (be:mun:73009) and Tongeren (be:mun:73083), CLAUDE.md rule 5:

    FISCAL_TOT_NET_TAXABLE_INC  73009 = 280,373,890.32
    FISCAL_TOT_NET_TAXABLE_INC  73083 = 759,840,493.45
    FISCAL_NBR_NON_ZERO_INC     73009 = 7,045
    FISCAL_NBR_NON_ZERO_INC     73083 = 19,654

    sum(total)  = 1,040,214,383.77
    sum(count)  = 26,699
    mean        = 1,040,214,383.77 / 26,699 = 38,960.7965... -> 38,960.80

    (naive mean of the two commune averages, which the rule forbids:
     280,373,890.32/7,045 = 39,798.42...
     759,840,493.45/19,654 = 38,660.00...
     average of those two  = 39,229.21 -- NOT the answer, by EUR 268.42)
"""

import pytest

from src.analytics.backaggregate import (
    CycleError,
    lineage_from_geographies,
    methods_from_metadata,
    only_missing_cells,
    reconstruct,
    reconstruct_additive,
    reconstruct_ratios,
    resolve_successor,
    territory_consistent_growth,
    territory_series,
)

BORGLOON = "be:mun:73009"
TONGEREN = "be:mun:73083"
TONGEREN_BORGLOON = "be:mun:73111"

FISCAL_LINEAGE = [
    (BORGLOON, "2025-01-01", TONGEREN_BORGLOON),
    (TONGEREN, "2025-01-01", TONGEREN_BORGLOON),
]

FISCAL_META = {
    "FISCAL_TOT_NET_TAXABLE_INC": ("Total net taxable income", "eur"),
    "FISCAL_NBR_NON_ZERO_INC": ("Non-zero income returns", "count"),
}
FISCAL_ADDITIVE = {"FISCAL_TOT_NET_TAXABLE_INC", "FISCAL_NBR_NON_ZERO_INC"}

FISCAL_RAW = [
    (
        BORGLOON,
        "FISCAL_TOT_NET_TAXABLE_INC",
        "Total net taxable income",
        "eur",
        "2023",
        280373890.32,
        "final",
        "c1",
    ),
    (
        TONGEREN,
        "FISCAL_TOT_NET_TAXABLE_INC",
        "Total net taxable income",
        "eur",
        "2023",
        759840493.45,
        "final",
        "c2",
    ),
    (
        BORGLOON,
        "FISCAL_NBR_NON_ZERO_INC",
        "Non-zero income returns",
        "count",
        "2023",
        7045.0,
        "final",
        "c1",
    ),
    (
        TONGEREN,
        "FISCAL_NBR_NON_ZERO_INC",
        "Non-zero income returns",
        "count",
        "2023",
        19654.0,
        "final",
        "c2",
    ),
]

AVG_NET_TAXABLE_INCOME_CONFIG = {
    "AVG_NET_TAXABLE_INCOME": {
        "name": {"en": "Average net taxable income per tax return"},
        "unit": "eur",
        "derived": {
            "function": "mean_from_total",
            "inputs": ["FISCAL_TOT_NET_TAXABLE_INC", "FISCAL_NBR_NON_ZERO_INC"],
        },
    }
}
AVG_META = {"AVG_NET_TAXABLE_INCOME": ("Average net taxable income per tax return", "eur")}


# ── The hand-computed ratio, and the wrong answer it must not equal ────────


def test_avg_net_taxable_income_is_the_sum_ratio_not_the_mean_of_means():
    lineage = lineage_from_geographies(FISCAL_LINEAGE)
    components = reconstruct_additive(FISCAL_RAW, lineage, FISCAL_ADDITIVE, FISCAL_META)
    ratios = reconstruct_ratios(components, lineage, AVG_NET_TAXABLE_INCOME_CONFIG)

    row = next(r for r in ratios if r[1] == "AVG_NET_TAXABLE_INCOME" and r[4] == "2023")
    assert row[0] == TONGEREN_BORGLOON
    assert row[5] == pytest.approx(38960.80, abs=0.01)
    # The whole point of the rule: NOT the mean of the two commune averages.
    assert row[5] != pytest.approx(39229.21, abs=0.01)


def test_fiscal_total_net_taxable_income_sums_exactly():
    lineage = lineage_from_geographies(FISCAL_LINEAGE)
    components = reconstruct_additive(FISCAL_RAW, lineage, FISCAL_ADDITIVE, FISCAL_META)
    row = next(
        r
        for r in components
        if r[1] == "FISCAL_TOT_NET_TAXABLE_INC" and r[0] == TONGEREN_BORGLOON and r[4] == "2023"
    )
    assert row[5] == pytest.approx(1040214383.77, abs=0.01)
    assert row[6] == "reconstructed"


# ── Gap-fill, never restatement ─────────────────────────────────────────────


def test_a_successor_with_its_own_value_keeps_it_antwerp():
    """Antwerp (11002) already publishes its own fiscal total; Borsbeek's
    (11007) predecessor value must never be added to it."""
    antwerp = "be:mun:11002"
    borsbeek = "be:mun:11007"
    lineage_rows = [(borsbeek, "2025-01-01", antwerp)]
    raw = [
        (
            antwerp,
            "FISCAL_TOT_NET_TAXABLE_INC",
            "Total net taxable income",
            "eur",
            "2023",
            11097000000.0,
            "final",
            "c",
        ),
        (
            borsbeek,
            "FISCAL_TOT_NET_TAXABLE_INC",
            "Total net taxable income",
            "eur",
            "2023",
            300000000.0,
            "final",
            "c",
        ),
    ]
    out = reconstruct(
        raw,
        lineage_rows,
        indicator_is_additive={"FISCAL_TOT_NET_TAXABLE_INC": True},
        indicator_meta=FISCAL_META,
    )
    # No reconstructed row at all for this cell -- Antwerp's published figure
    # is untouched, not overwritten and not summed with Borsbeek's.
    assert not any(r[0] == antwerp and r[4] == "2023" for r in out)


def test_only_missing_cells_drops_a_cell_the_successor_already_has():
    reconstructed = [
        (
            TONGEREN_BORGLOON,
            "FISCAL_TOT_NET_TAXABLE_INC",
            "n",
            "eur",
            "2023",
            1.0,
            "reconstructed",
            "",
        ),
        (
            TONGEREN_BORGLOON,
            "FISCAL_TOT_NET_TAXABLE_INC",
            "n",
            "eur",
            "2024",
            2.0,
            "reconstructed",
            "",
        ),
    ]
    existing = [
        (TONGEREN_BORGLOON, "FISCAL_TOT_NET_TAXABLE_INC", "n", "eur", "2023", 999.0, "final", ""),
    ]
    out = only_missing_cells(reconstructed, existing)
    periods = {r[4] for r in out}
    assert periods == {"2024"}


# ── All predecessors or nothing ─────────────────────────────────────────────


def test_a_partial_predecessor_set_yields_an_absent_cell_not_a_partial_sum():
    lineage = lineage_from_geographies(FISCAL_LINEAGE)
    partial_raw = [
        (BORGLOON, "FISCAL_TOT_NET_TAXABLE_INC", "n", "eur", "2023", 280373890.32, "final", "c"),
        # Tongeren's 2023 cell is suppressed -- must block the whole cell.
        (TONGEREN, "FISCAL_TOT_NET_TAXABLE_INC", "n", "eur", "2023", None, "suppressed", "c"),
    ]
    components = reconstruct_additive(
        partial_raw, lineage, {"FISCAL_TOT_NET_TAXABLE_INC"}, FISCAL_META
    )
    assert not any(r[4] == "2023" for r in components)


def test_a_predecessor_row_missing_entirely_also_blocks_the_cell():
    lineage = lineage_from_geographies(FISCAL_LINEAGE)
    partial_raw = [
        (BORGLOON, "FISCAL_TOT_NET_TAXABLE_INC", "n", "eur", "2023", 280373890.32, "final", "c"),
        # Tongeren has no row at all for 2023.
    ]
    components = reconstruct_additive(
        partial_raw, lineage, {"FISCAL_TOT_NET_TAXABLE_INC"}, FISCAL_META
    )
    assert components == []


def test_an_na_status_also_blocks_the_cell_distinctly_from_suppressed():
    lineage = lineage_from_geographies(FISCAL_LINEAGE)
    partial_raw = [
        (BORGLOON, "FISCAL_TOT_NET_TAXABLE_INC", "n", "eur", "2023", 280373890.32, "final", "c"),
        (TONGEREN, "FISCAL_TOT_NET_TAXABLE_INC", "n", "eur", "2023", None, "na", "c"),
    ]
    components = reconstruct_additive(
        partial_raw, lineage, {"FISCAL_TOT_NET_TAXABLE_INC"}, FISCAL_META
    )
    assert components == []


# ── Refuse the four police rates explicitly ─────────────────────────────────


def test_police_rates_are_never_reconstructed_for_any_successor():
    police_ids = (
        "CAR_THEFT_PER_10K",
        "DOMESTIC_VIOLENCE_PER_10K",
        "HOUSE_BURGLARIES_PER_10K",
        "THEFT_FROM_VEHICLE_PER_10K",
    )
    lineage_rows = [
        (BORGLOON, "2025-01-01", TONGEREN_BORGLOON),
        (TONGEREN, "2025-01-01", TONGEREN_BORGLOON),
    ]
    raw = [(BORGLOON, pid, pid, "per_10k", "2023", 10.0, "final", "c") for pid in police_ids] + [
        (TONGEREN, pid, pid, "per_10k", "2023", 20.0, "final", "c") for pid in police_ids
    ]
    meta = {pid: (pid, "per_10k") for pid in police_ids}
    out = reconstruct(
        raw,
        lineage_rows,
        indicator_is_additive=dict.fromkeys(police_ids, False),
        indicator_meta=meta,
    )
    assert out == []


def test_methods_from_metadata_refuses_non_additive_indicators():
    methods = methods_from_metadata(
        {"CAR_THEFT_PER_10K": False, "FISCAL_TOT_NET_TAXABLE_INC": True}
    )
    assert methods["CAR_THEFT_PER_10K"] == "refuse"
    assert methods["FISCAL_TOT_NET_TAXABLE_INC"] == "sum"


# ── Lineage: driven by successor_geo_id, never by date ──────────────────────


def test_bastogne_is_selected_despite_its_2024_12_02_date_not_2025():
    """The exact trap named in the handoff: Bastogne's crosswalk rows carry
    valid_to = 2024-12-02, not 2025-01-01. A wave filter on "2025" would drop
    it; lineage_from_geographies must not filter on date at all."""
    bastenaken = "be:mun:82003"
    bertogne = "be:mun:82005"
    bastogne = "be:mun:82039"
    lineage = lineage_from_geographies(
        [(bastenaken, "2024-12-02", bastogne), (bertogne, "2024-12-02", bastogne)]
    )
    assert set(lineage[bastogne]) == {bastenaken, bertogne}


def test_a_two_hop_lineage_resolves():
    """A -> B -> C: A's predecessor status must resolve all the way to C,
    the entity that exists today, not stop at the intermediate B."""
    a, b, c = "be:mun:A", "be:mun:B", "be:mun:C"
    lineage = lineage_from_geographies([(a, "2010-01-01", b), (b, "2020-01-01", c)])
    assert set(lineage[c]) == {a, b}
    assert resolve_successor(a, {}) is None  # sanity: no links, no resolution


def test_resolve_successor_two_hop_directly():
    from src.analytics.backaggregate import GeographyLink

    links = {
        "A": GeographyLink("A", "2010-01-01", "B"),
        "B": GeographyLink("B", "2020-01-01", "C"),
        "C": GeographyLink("C", None, None),
    }
    assert resolve_successor("A", links) == "C"
    assert resolve_successor("B", links) == "C"
    assert resolve_successor("C", links) is None


def test_resolve_successor_cycle_guard_raises():
    from src.analytics.backaggregate import GeographyLink

    links = {
        "A": GeographyLink("A", "2010-01-01", "B"),
        "B": GeographyLink("B", "2010-01-01", "A"),
    }
    with pytest.raises(CycleError):
        resolve_successor("A", links)


def test_2019_wave_is_in_scope_no_hardcoded_wave_date():
    """Driving off lineage rather than a date necessarily includes the 2019
    wave too -- this is a consequence, not a feature to special-case, and the
    module must not filter it out."""
    pred = "be:mun:OLD2019"
    succ = "be:mun:NEW2019"
    lineage = lineage_from_geographies([(pred, "2019-01-01", succ)])
    assert lineage[succ] == [pred]


# ── Full reconstruct() acceptance shape ─────────────────────────────────────


def test_reconstruct_end_to_end_produces_both_components_and_ratio():
    out = reconstruct(
        FISCAL_RAW,
        FISCAL_LINEAGE,
        indicator_is_additive={"FISCAL_TOT_NET_TAXABLE_INC": True, "FISCAL_NBR_NON_ZERO_INC": True},
        indicator_meta={**FISCAL_META, **AVG_META},
        derived_configs=AVG_NET_TAXABLE_INCOME_CONFIG,
    )
    ids = {r[1] for r in out}
    assert "FISCAL_TOT_NET_TAXABLE_INC" in ids
    assert "FISCAL_NBR_NON_ZERO_INC" in ids
    assert "AVG_NET_TAXABLE_INCOME" in ids
    ratio_row = next(r for r in out if r[1] == "AVG_NET_TAXABLE_INCOME")
    assert ratio_row[5] == pytest.approx(38960.80, abs=0.01)


# ── FINDING 1: gap-fill must cover derived ratios too ──────────────────────


def test_a_reconstructed_ratio_is_dropped_when_the_successor_already_has_it():
    """AVG_NET_TAXABLE_INCOME never appears in `raw_rows` -- it is
    derived-only -- so the plain `existing = [row for row in raw_rows ...]`
    gap-fill guard is structurally blind to it. Antwerp already has its own
    live AVG_NET_TAXABLE_INCOME/2023 (from compute(), passed in here as
    `existing_derived_cells`); the reconstructed sum-ratio for that same key
    must be dropped, not emitted alongside it."""
    out = reconstruct(
        FISCAL_RAW,
        FISCAL_LINEAGE,
        indicator_is_additive={"FISCAL_TOT_NET_TAXABLE_INC": True, "FISCAL_NBR_NON_ZERO_INC": True},
        indicator_meta={**FISCAL_META, **AVG_META},
        derived_configs=AVG_NET_TAXABLE_INCOME_CONFIG,
        existing_derived_cells={(TONGEREN_BORGLOON, "AVG_NET_TAXABLE_INCOME", "2023")},
    )
    assert not any(r[1] == "AVG_NET_TAXABLE_INCOME" for r in out)
    # The raw components are untouched by this guard -- it targets only the
    # derived-cell key it was given.
    assert any(r[1] == "FISCAL_TOT_NET_TAXABLE_INC" for r in out)


def test_existing_derived_cells_is_scoped_to_its_own_key_not_every_ratio():
    """The guard must key on (geo_id, indicator_id, period) exactly, not drop
    every ratio just because compute() already produced something for that
    successor -- e.g. a different period must survive untouched."""
    out = reconstruct(
        FISCAL_RAW,
        FISCAL_LINEAGE,
        indicator_is_additive={"FISCAL_TOT_NET_TAXABLE_INC": True, "FISCAL_NBR_NON_ZERO_INC": True},
        indicator_meta={**FISCAL_META, **AVG_META},
        derived_configs=AVG_NET_TAXABLE_INCOME_CONFIG,
        existing_derived_cells={(TONGEREN_BORGLOON, "AVG_NET_TAXABLE_INCOME", "2099")},
    )
    ratio_row = next(r for r in out if r[1] == "AVG_NET_TAXABLE_INCOME")
    assert ratio_row[4] == "2023"
    assert ratio_row[5] == pytest.approx(38960.80, abs=0.01)


def test_existing_derived_cells_defaults_to_empty_and_changes_nothing():
    """Callers that never pass the new parameter (export_communes_csv.py,
    which always passes derived_configs=None anyway) must see byte-identical
    behaviour to before this parameter existed."""
    with_default = reconstruct(
        FISCAL_RAW,
        FISCAL_LINEAGE,
        indicator_is_additive={"FISCAL_TOT_NET_TAXABLE_INC": True, "FISCAL_NBR_NON_ZERO_INC": True},
        indicator_meta={**FISCAL_META, **AVG_META},
        derived_configs=AVG_NET_TAXABLE_INCOME_CONFIG,
    )
    with_empty = reconstruct(
        FISCAL_RAW,
        FISCAL_LINEAGE,
        indicator_is_additive={"FISCAL_TOT_NET_TAXABLE_INC": True, "FISCAL_NBR_NON_ZERO_INC": True},
        indicator_meta={**FISCAL_META, **AVG_META},
        derived_configs=AVG_NET_TAXABLE_INCOME_CONFIG,
        existing_derived_cells=(),
    )
    assert with_default == with_empty


# ── territory_series / territory_consistent_growth ─────────────────────────
# Maintainer decision 2026-09-16: "compute all growth of current territory".
#
# ANTWERP figures are the real is_latest=1 POPULATION_BY_COMMUNE rows from
# data/population_observations.csv (be:mun:11002 / be:mun:11007), read
# directly and summed by hand -- not typed from the handoff, re-derived from
# the source rows:
#   POPULATION_BY_COMMUNE 11002 2016 = 517,042.0   2020 = 529,247.0
#                                2024 = 544,759.0   2025 = 562,002.0
#                                2026 = 565,615.0
#   POPULATION_BY_COMMUNE 11007 2016 =  10,540.0   2020 =  10,949.0
#   T(2016) = 517,042 + 10,540 = 527,582.0
#   T(2020) = 529,247 + 10,949 = 540,196.0
#   POPULATION_CHANGE_5Y 2025 = 562,002 / 540,196 - 1        = 4.036683 %
#   POPULATION_CAGR_10Y 2026  = (565,615 / 527,582)^0.1 - 1  = 0.698522 %
# The SPLICED (wrong, pre-fix) values used Antwerp's own row alone as the
# base: 562,002 / 529,247 - 1 = 6.188982 %, (565,615 / 517,042)^0.1 - 1 =
# 0.901938 % -- both asserted as the values that must NOT come out.

ANTWERP = "be:mun:11002"
BORSBEEK = "be:mun:11007"
ANTWERP_LINEAGE_ROWS = [(BORSBEEK, "2025-01-01", ANTWERP)]

_POP_META = {"POPULATION_BY_COMMUNE": ("Population", "count")}


def _pop_row(geo_id, period, value, status="final"):
    return (geo_id, "POPULATION_BY_COMMUNE", "Population", "count", period, value, status, "c")


ANTWERP_POP_RAW = [
    _pop_row(ANTWERP, "2016", 517042.0),
    _pop_row(ANTWERP, "2020", 529247.0),
    _pop_row(ANTWERP, "2024", 544759.0),
    _pop_row(ANTWERP, "2025", 562002.0),
    _pop_row(ANTWERP, "2026", 565615.0),
    _pop_row(BORSBEEK, "2016", 10540.0),
    _pop_row(BORSBEEK, "2020", 10949.0),
    # Borsbeek stops reporting after its 2025-01-01 merger -- no 2025/2026 row.
]

POPULATION_CHANGE_5Y_CONFIG = {
    "POPULATION_CHANGE_5Y": {
        "name": {"en": "Population change over 5 years"},
        "unit": "percent",
        "derived": {"function": "five_year_change", "inputs": ["POPULATION_BY_COMMUNE"]},
    }
}
POPULATION_CAGR_10Y_CONFIG = {
    "POPULATION_CAGR_10Y": {
        "name": {"en": "10y CAGR"},
        "unit": "percent_per_year",
        "derived": {
            "function": "cagr",
            "inputs": ["POPULATION_BY_COMMUNE"],
            "args": {"years": 10},
        },
    }
}


def test_territory_series_antwerp_2020_sums_own_plus_borsbeek():
    lineage = lineage_from_geographies(ANTWERP_LINEAGE_ROWS)
    series = territory_series(ANTWERP_POP_RAW, lineage, "POPULATION_BY_COMMUNE")
    value, used_predecessor = series[ANTWERP]["2020"]
    assert value == pytest.approx(540196.0)
    assert used_predecessor is True


def test_territory_series_antwerp_2025_is_its_own_row_only_borsbeek_gone():
    """Post-merger, Borsbeek reports nothing -- T is just Antwerp's own row,
    and used_predecessor_data must be False: this base is NOT reconstructed."""
    lineage = lineage_from_geographies(ANTWERP_LINEAGE_ROWS)
    series = territory_series(ANTWERP_POP_RAW, lineage, "POPULATION_BY_COMMUNE")
    value, used_predecessor = series[ANTWERP]["2025"]
    assert value == pytest.approx(562002.0)
    assert used_predecessor is False


def test_antwerp_population_change_5y_2025_is_4_04_percent_not_the_spliced_6_19():
    lineage = lineage_from_geographies(ANTWERP_LINEAGE_ROWS)
    territory = territory_series(ANTWERP_POP_RAW, lineage, "POPULATION_BY_COMMUNE")
    out = territory_consistent_growth(territory, POPULATION_CHANGE_5Y_CONFIG)
    row = next(r for r in out if r[0] == ANTWERP and r[4] == "2025")
    assert row[5] == pytest.approx(4.036683, abs=1e-5)
    assert row[5] != pytest.approx(6.188982, abs=1e-3)  # the pre-fix spliced value
    # The base period (2020) used Borsbeek's predecessor row, so the growth
    # figure itself carries the discontinuity label.
    assert row[6] == "reconstructed"


def test_antwerp_population_cagr_10y_2026_is_0_70_percent_not_the_spliced_0_90():
    lineage = lineage_from_geographies(ANTWERP_LINEAGE_ROWS)
    territory = territory_series(ANTWERP_POP_RAW, lineage, "POPULATION_BY_COMMUNE")
    out = territory_consistent_growth(territory, POPULATION_CAGR_10Y_CONFIG)
    row = next(r for r in out if r[0] == ANTWERP and r[4] == "2026")
    assert row[5] == pytest.approx(0.698522, abs=1e-5)
    assert row[5] != pytest.approx(0.901938, abs=1e-3)  # the pre-fix spliced value
    assert row[6] == "reconstructed"  # base year 2016 used Borsbeek's row


def test_a_later_antwerp_growth_cell_whose_base_is_post_merger_is_not_marked_reconstructed():
    """A hypothetical POPULATION_CHANGE_5Y for 2030 (base 2025) would use only
    Antwerp's own row on both ends -- status must read 'derived', not
    'reconstructed', once the territory has stopped needing predecessor data."""
    lineage = lineage_from_geographies(ANTWERP_LINEAGE_ROWS)
    raw = ANTWERP_POP_RAW + [_pop_row(ANTWERP, "2030", 600000.0)]
    territory = territory_series(raw, lineage, "POPULATION_BY_COMMUNE")
    out = territory_consistent_growth(territory, POPULATION_CHANGE_5Y_CONFIG)
    row = next(r for r in out if r[0] == ANTWERP and r[4] == "2030")
    assert row[6] == "derived"


# BASTOGNE (82039) is a WHOLE-COMMUNE successor (no own row before 2025) --
# real rows from data/population_observations.csv for its two predecessors,
# Bastenaken (82003) and Bertogne (82005):
#   POPULATION_BY_COMMUNE 82003 2020 = 16,276.0
#   POPULATION_BY_COMMUNE 82005 2020 =  3,656.0
#   T(2020) = 16,276 + 3,656 = 19,932.0  (matches reconstruct_additive's own
#   gap-filled 2020 value for the same fixture, since Bastogne has no own row
#   that year -- T and gap-fill agree exactly in the pre-merger-only case)
#   POPULATION_BY_COMMUNE 82039 2025 (own, real) = 20,940.0
#   POPULATION_CHANGE_5Y 2025 = 20,940 / 19,932 - 1 = 5.057194 %

BASTOGNE = "be:mun:82039"
BASTENAKEN = "be:mun:82003"
BERTOGNE = "be:mun:82005"
BASTOGNE_LINEAGE_ROWS = [
    (BASTENAKEN, "2024-12-02", BASTOGNE),
    (BERTOGNE, "2024-12-02", BASTOGNE),
]
BASTOGNE_POP_RAW = [
    _pop_row(BASTENAKEN, "2020", 16276.0),
    _pop_row(BERTOGNE, "2020", 3656.0),
    _pop_row(BASTOGNE, "2025", 20940.0),
]


def test_bastogne_population_change_5y_2025_from_reconstructed_2020_base():
    lineage = lineage_from_geographies(BASTOGNE_LINEAGE_ROWS)
    territory = territory_series(BASTOGNE_POP_RAW, lineage, "POPULATION_BY_COMMUNE")
    assert territory[BASTOGNE]["2020"] == (pytest.approx(19932.0), True)
    out = territory_consistent_growth(territory, POPULATION_CHANGE_5Y_CONFIG)
    row = next(r for r in out if r[0] == BASTOGNE and r[4] == "2025")
    assert row[5] == pytest.approx(5.057194, abs=1e-5)
    assert row[6] == "reconstructed"


def test_a_growth_cell_whose_endpoint_AND_base_are_both_reconstructed_is_still_marked():
    """Bastogne did not exist before 2025, so a growth cell computed for an
    EARLIER year (both endpoint and base pre-merger, both summed from
    predecessors) must still read 'reconstructed' -- checking only the base
    would happen to give the same answer here (base is always at least as
    close to the merger as the endpoint), but this proves the endpoint side
    of the check is not a no-op: both years' T come from predecessor sums.
    T(2016) = Bastenaken(2016) + Bertogne(2016) = 15,580 + 3,483 = 19,063.
    T(2021) = Bastenaken(2021) + Bertogne(2021) = 16,296 + 3,750 = 20,046.
    """
    lineage = lineage_from_geographies(BASTOGNE_LINEAGE_ROWS)
    raw = [
        _pop_row(BASTENAKEN, "2016", 15580.0),
        _pop_row(BERTOGNE, "2016", 3483.0),
        _pop_row(BASTENAKEN, "2021", 16296.0),
        _pop_row(BERTOGNE, "2021", 3750.0),
    ]
    territory = territory_series(raw, lineage, "POPULATION_BY_COMMUNE")
    assert territory[BASTOGNE]["2016"] == (pytest.approx(19063.0), True)
    assert territory[BASTOGNE]["2021"] == (pytest.approx(20046.0), True)
    out = territory_consistent_growth(territory, POPULATION_CHANGE_5Y_CONFIG)
    row = next(r for r in out if r[0] == BASTOGNE and r[4] == "2021")
    expected = (20046.0 / 19063.0 - 1) * 100
    assert row[5] == pytest.approx(expected, abs=1e-6)
    assert row[6] == "reconstructed"


def test_a_partial_lineage_period_yields_no_territory_entry_for_that_period():
    """Rule 26 / all-predecessors-or-nothing: if Bertogne never reports 2020
    at all, T(2020) must not exist -- not a partial sum of Bastenaken alone."""
    lineage = lineage_from_geographies(BASTOGNE_LINEAGE_ROWS)
    partial_raw = [
        _pop_row(BASTENAKEN, "2020", 16276.0),
        # Bertogne has no 2020 row at all.
        _pop_row(BASTOGNE, "2025", 20940.0),
    ]
    territory = territory_series(partial_raw, lineage, "POPULATION_BY_COMMUNE")
    assert "2020" not in territory.get(BASTOGNE, {})
    # And therefore no POPULATION_CHANGE_5Y 2025 cell is produced either --
    # the endpoint (2025) exists but the base (2020) does not, so the growth
    # figure is ABSENT, never computed from a partial base.
    out = territory_consistent_growth(territory, POPULATION_CHANGE_5Y_CONFIG)
    assert not any(r[0] == BASTOGNE and r[4] == "2025" for r in out)


def test_a_suppressed_predecessor_also_blocks_the_territory_period():
    lineage = lineage_from_geographies(BASTOGNE_LINEAGE_ROWS)
    partial_raw = [
        _pop_row(BASTENAKEN, "2020", 16276.0),
        _pop_row(BERTOGNE, "2020", None, status="suppressed"),
        _pop_row(BASTOGNE, "2025", 20940.0),
    ]
    territory = territory_series(partial_raw, lineage, "POPULATION_BY_COMMUNE")
    assert "2020" not in territory.get(BASTOGNE, {})


def test_a_commune_with_no_lineage_row_is_absent_from_territory_series():
    """An ordinary commune (no successor_geo_id row anywhere) never appears
    as a key in territory_series's output at all -- the caller falls back to
    the plain engine-computed growth for it, unchanged."""
    lineage = lineage_from_geographies(BASTOGNE_LINEAGE_ROWS)
    other_raw = [_pop_row("be:mun:99999", "2020", 1000.0), _pop_row("be:mun:99999", "2025", 1100.0)]
    territory = territory_series(other_raw, lineage, "POPULATION_BY_COMMUNE")
    assert "be:mun:99999" not in territory


def test_mun_revenue_growth_1y_never_qualifies_its_input_is_derived_not_raw():
    """MUN_REVENUE_GROWTH_1Y / MUN_EXPENDITURE_GROWTH_1Y take a DERIVED
    per-capita ratio as input, never a raw additive indicator -- so
    territory_series is never even called for them by the real exporter (see
    export_communes_history_csv.py's input_id/is_additive filter). This test
    documents the shape territory_consistent_growth would see if it were
    mistakenly pointed at a per-capita config: since the "territory" mapping
    passed in is keyed by the RAW indicator, a config naming a different
    input than what territory was built from produces no rows at all --
    proving the two can never silently cross-apply."""
    lineage = lineage_from_geographies(ANTWERP_LINEAGE_ROWS)
    territory = territory_series(ANTWERP_POP_RAW, lineage, "POPULATION_BY_COMMUNE")
    revenue_growth_config = {
        "MUN_REVENUE_GROWTH_1Y": {
            "name": {"en": "Revenue growth"},
            "unit": "percent",
            "derived": {
                "function": "growth_rate",
                "inputs": ["MUN_REVENUE_ORDINARY_PER_CAPITA"],
                "args": {"years": 1},
            },
        }
    }
    # growth_rate itself is not in _GROWTH_FUNCTION_HORIZONS (only
    # five_year_change and cagr are), so this produces nothing regardless --
    # confirming a 1-year WalStat growth indicator can never be swept in by
    # this function even if a caller passed it a population territory map.
    out = territory_consistent_growth(territory, revenue_growth_config)
    assert out == []
