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
    assert ratio_row[6] == "reconstructed"
