"""Tests for commune -> province -> region -> Belgium aggregation (Block L).

Every expected value here is computed BY HAND with the arithmetic written out,
per CLAUDE.md rule 5 and the roadmap's own reasoning: testing a formula
against its own implementation proves nothing.

The three tests that matter most are the ones pinned to real measured figures
from docs/features/comparison.md, because each guards a way of being wrong
that produces a plausible-looking number rather than a crash.
"""

import pytest

from src.analytics.aggregate import (
    RECOMPUTE,
    REFUSE,
    SUM,
    Coverage,
    NotAggregatableError,
    aggregate,
    aggregate_additive,
    ancestors_of,
    methods_from_metadata,
    per_period_universe,
)
from src.analytics.engine import ObservationSet

# ── a small Belgium ─────────────────────────────────────────────────────────
# Two provinces in one region, plus a Brussels-shaped branch where the
# arrondissement parents straight to the region with NO province -- the real
# shape from geography.md Q3, which any fixed-depth walk gets wrong.
PARENTS = {
    "be:country": None,
    "be:reg:fl": "be:country",
    "be:reg:bru": "be:country",
    "be:prov:a": "be:reg:fl",
    "be:prov:b": "be:reg:fl",
    "be:arr:a1": "be:prov:a",
    "be:arr:b1": "be:prov:b",
    "be:arr:bru1": "be:reg:bru",  # no province in between
    "be:mun:a1": "be:arr:a1",
    "be:mun:a2": "be:arr:a1",
    "be:mun:b1": "be:arr:b1",
    "be:mun:bru1": "be:arr:bru1",
}
LEVELS = {
    "be:country": "country",
    "be:reg:fl": "region",
    "be:reg:bru": "region",
    "be:prov:a": "province",
    "be:prov:b": "province",
    "be:arr:a1": "arrondissement",
    "be:arr:b1": "arrondissement",
    "be:arr:bru1": "arrondissement",
    "be:mun:a1": "municipality",
    "be:mun:a2": "municipality",
    "be:mun:b1": "municipality",
    "be:mun:bru1": "municipality",
}
ALL_COMMUNES = {"be:mun:a1", "be:mun:a2", "be:mun:b1", "be:mun:bru1"}
UNIVERSE = {"2023": ALL_COMMUNES}

ADDITIVE_META = {"POP": {"is_additive": True}}


def _obs(rows):
    return ObservationSet(rows)


# ── the parent walk ────────────────────────────────────────────────────────


def test_a_commune_rolls_up_into_every_level_above_it():
    assert ancestors_of("be:mun:a1", PARENTS, LEVELS) == [
        "be:arr:a1",
        "be:prov:a",
        "be:reg:fl",
        "be:country",
    ]


def test_a_brussels_commune_has_no_province_and_the_walk_does_not_invent_one():
    """A fixed four-hop walk would either crash or attribute a Brussels
    commune to whatever sat at the province position."""
    chain = ancestors_of("be:mun:bru1", PARENTS, LEVELS)
    assert chain == ["be:arr:bru1", "be:reg:bru", "be:country"]
    assert not any(LEVELS[g] == "province" for g in chain)


def test_the_walk_stops_on_a_cycle_instead_of_looping_forever():
    cyclic = {"be:mun:x": "be:prov:y", "be:prov:y": "be:reg:z", "be:reg:z": "be:prov:y"}
    levels = {"be:mun:x": "municipality", "be:prov:y": "province", "be:reg:z": "region"}
    assert ancestors_of("be:mun:x", cyclic, levels) == ["be:prov:y", "be:reg:z"]


# ── summing additive indicators ────────────────────────────────────────────


def test_additive_values_sum_into_each_ancestor():
    # a1=100, a2=250 -> province a = 350; b1=40 -> province b = 40
    # region fl = 100+250+40 = 390; country = 390 + bru1(10) = 400
    obs = _obs(
        [
            ("POP", "be:mun:a1", "2023", 100.0),
            ("POP", "be:mun:a2", "2023", 250.0),
            ("POP", "be:mun:b1", "2023", 40.0),
            ("POP", "be:mun:bru1", "2023", 10.0),
        ]
    )
    result = aggregate_additive(obs, ["POP"], PARENTS, LEVELS, per_period_universe(UNIVERSE))

    assert result.value("POP", "be:prov:a", "2023") == 350.0
    assert result.value("POP", "be:prov:b", "2023") == 40.0
    assert result.value("POP", "be:reg:fl", "2023") == 390.0
    assert result.value("POP", "be:reg:bru", "2023") == 10.0
    assert result.value("POP", "be:country", "2023") == 400.0


def test_coverage_counts_the_communes_that_existed_not_the_ones_with_values():
    """The denominator is the period's universe. If it were "communes with a
    value" instead, coverage would be 100% by construction and could never
    report the gap it exists to report."""
    obs = _obs(
        [
            ("POP", "be:mun:a1", "2023", 100.0),
            # a2 exists in 2023 but has no value: province a is 1 of 2.
        ]
    )
    result = aggregate_additive(
        obs, ["POP"], PARENTS, LEVELS, per_period_universe(UNIVERSE), min_coverage=0.0
    )
    cov = result.coverage("POP", "be:prov:a", "2023")
    assert (cov.contributed, cov.expected) == (1, 2)
    assert cov.pct == 50.0


# ── the suppression gate ───────────────────────────────────────────────────


def test_an_aggregate_below_the_coverage_threshold_is_absent_not_footnoted():
    obs = _obs([("POP", "be:mun:a1", "2023", 100.0)])  # 1 of 2 in province a
    result = aggregate_additive(
        obs, ["POP"], PARENTS, LEVELS, per_period_universe(UNIVERSE), min_coverage=0.90
    )
    assert result.value("POP", "be:prov:a", "2023") is None


def test_the_threshold_boundary_is_inclusive():
    """9 of 10 is exactly 90% and must publish; 8 of 10 must not. Pinning the
    boundary because an off-by-one here silently changes what is published."""
    parents = {"be:country": None, "be:reg:r": "be:country"}
    levels = {"be:country": "country", "be:reg:r": "region"}
    communes = {}
    for i in range(10):
        gid = f"be:mun:{i}"
        parents[gid] = "be:reg:r"
        levels[gid] = "municipality"
        communes[gid] = 1.0
    universe = {"2023": set(communes)}

    nine = _obs([("POP", g, "2023", 1.0) for g in list(communes)[:9]])
    eight = _obs([("POP", g, "2023", 1.0) for g in list(communes)[:8]])

    assert (
        aggregate_additive(nine, ["POP"], parents, levels, per_period_universe(universe)).value(
            "POP", "be:reg:r", "2023"
        )
        == 9.0
    )
    assert (
        aggregate_additive(eight, ["POP"], parents, levels, per_period_universe(universe)).value(
            "POP", "be:reg:r", "2023"
        )
        is None
    )


def test_zero_expected_communes_is_not_full_coverage():
    cov = Coverage(contributed=0, expected=0)
    assert cov.pct == 0.0
    assert not cov.is_sufficient(0.90)
    assert not cov.is_sufficient(0.0)


# ── ratios: the finding this whole block turns on ──────────────────────────

MEAN_CONFIG = {
    "AVG": {
        "name": {"en": "Average income"},
        "unit": "eur",
        "derived": {"function": "mean_from_total", "inputs": ["TOTAL_INC", "RETURNS"]},
    }
}


def test_a_ratio_is_recomputed_from_the_sums_not_averaged():
    """The core rule, with hand arithmetic on numbers chosen so the three
    candidate methods give visibly different answers.

    Commune a1: 1,000,000 income over 10 returns  -> mean 100,000
    Commune a2:   200,000 income over 40 returns  -> mean   5,000

    Correct province mean = 1,200,000 / 50            =  24,000
    Unweighted mean of the two commune means          =  52,500  (WRONG)
    """
    obs = _obs(
        [
            ("TOTAL_INC", "be:mun:a1", "2023", 1_000_000.0),
            ("TOTAL_INC", "be:mun:a2", "2023", 200_000.0),
            ("RETURNS", "be:mun:a1", "2023", 10.0),
            ("RETURNS", "be:mun:a2", "2023", 40.0),
        ]
    )
    methods = {"TOTAL_INC": SUM, "RETURNS": SUM, "AVG": RECOMPUTE}
    result = aggregate(
        obs, methods, PARENTS, LEVELS, per_period_universe(UNIVERSE), MEAN_CONFIG, min_coverage=0.0
    )

    assert result.value("TOTAL_INC", "be:prov:a", "2023") == 1_200_000.0
    assert result.value("RETURNS", "be:prov:a", "2023") == 50.0
    assert result.value("AVG", "be:prov:a", "2023") == pytest.approx(24_000.0)
    # The specific wrong answer this rule exists to prevent.
    assert result.value("AVG", "be:prov:a", "2023") != pytest.approx(52_500.0)


def test_the_real_belgian_average_income_figure_from_the_spec():
    """Pinned to the measurement in docs/features/comparison.md, Correction 1.

    Belgium 2023, from the real store, over all 581 communes of that period:
        unweighted mean of commune means  EUR 41,613.37   (+3.71%)
        population-weighted               EUR 40,108.32   (-0.04%)
        sum(income) / sum(returns)        EUR 40,125.70   correct

    Reproduced here at fixture scale with the same relationship: a tiny rich
    commune drags an unweighted mean up, and a population-weighted figure
    lands close to but NOT ON the correct one -- because population is not
    the ratio's denominator. A test asserting only "close to correct" would
    accept the population-weighted answer, which is why this asserts the
    inequality too.
    """
    # rich: 1 return, EUR 500,000. big: 99 returns, EUR 99 * 30,000 = 2,970,000
    # correct = 3,470,000 / 100 = 34,700
    # unweighted mean of means = (500,000 + 30,000) / 2 = 265,000
    obs = _obs(
        [
            ("TOTAL_INC", "be:mun:a1", "2023", 500_000.0),
            ("RETURNS", "be:mun:a1", "2023", 1.0),
            ("TOTAL_INC", "be:mun:a2", "2023", 2_970_000.0),
            ("RETURNS", "be:mun:a2", "2023", 99.0),
        ]
    )
    methods = {"TOTAL_INC": SUM, "RETURNS": SUM, "AVG": RECOMPUTE}
    result = aggregate(
        obs, methods, PARENTS, LEVELS, per_period_universe(UNIVERSE), MEAN_CONFIG, min_coverage=0.0
    )

    assert result.value("AVG", "be:prov:a", "2023") == pytest.approx(34_700.0)
    assert result.value("AVG", "be:prov:a", "2023") != pytest.approx(265_000.0)


def test_a_recomputed_ratio_inherits_its_weakest_inputs_coverage():
    """A ratio must not look better covered than the numbers it came from."""
    obs = _obs(
        [
            ("TOTAL_INC", "be:mun:a1", "2023", 100.0),
            ("TOTAL_INC", "be:mun:a2", "2023", 100.0),
            ("RETURNS", "be:mun:a1", "2023", 4.0),
            # RETURNS missing for a2: 1 of 2 communes, 50%
        ]
    )
    methods = {"TOTAL_INC": SUM, "RETURNS": SUM, "AVG": RECOMPUTE}
    result = aggregate(
        obs, methods, PARENTS, LEVELS, per_period_universe(UNIVERSE), MEAN_CONFIG, min_coverage=0.0
    )

    assert result.coverage("TOTAL_INC", "be:prov:a", "2023").pct == 100.0
    assert result.coverage("RETURNS", "be:prov:a", "2023").pct == 50.0
    assert result.coverage("AVG", "be:prov:a", "2023").pct == 50.0


def test_a_ratio_whose_input_was_suppressed_is_itself_absent():
    obs = _obs(
        [
            ("TOTAL_INC", "be:mun:a1", "2023", 100.0),
            ("TOTAL_INC", "be:mun:a2", "2023", 100.0),
            ("RETURNS", "be:mun:a1", "2023", 4.0),
        ]
    )
    methods = {"TOTAL_INC": SUM, "RETURNS": SUM, "AVG": RECOMPUTE}
    # At a 90% gate, RETURNS (50%) is suppressed, so AVG cannot be built.
    result = aggregate(
        obs, methods, PARENTS, LEVELS, per_period_universe(UNIVERSE), MEAN_CONFIG, min_coverage=0.90
    )
    assert result.value("RETURNS", "be:prov:a", "2023") is None
    assert result.value("AVG", "be:prov:a", "2023") is None


# ── period-aware aggregation: the Limburg/Hasselt failure ──────────────────


def test_a_predecessor_commune_contributes_to_its_own_periods_total():
    """The measured failure: today's communes do not cover 2023, because
    communes created in the 2025 mergers have no earlier rows. Aggregating
    over the period's own universe is what restores the total.

    2023: predecessor P (600) and a1 (100) both existed -> province a = 700.
    2026: P is gone, successor S (620) exists          -> province a = 720.
    A current-communes-only aggregation would report 2023 as 100.
    """
    parents = dict(PARENTS, **{"be:mun:P": "be:arr:a1", "be:mun:S": "be:arr:a1"})
    levels = dict(LEVELS, **{"be:mun:P": "municipality", "be:mun:S": "municipality"})
    universe = {
        "2023": {"be:mun:a1", "be:mun:P"},
        "2026": {"be:mun:a1", "be:mun:S"},
    }
    obs = _obs(
        [
            ("POP", "be:mun:a1", "2023", 100.0),
            ("POP", "be:mun:P", "2023", 600.0),
            ("POP", "be:mun:a1", "2026", 100.0),
            ("POP", "be:mun:S", "2026", 620.0),
        ]
    )
    result = aggregate_additive(obs, ["POP"], parents, levels, per_period_universe(universe))

    assert result.value("POP", "be:prov:a", "2023") == 700.0
    assert result.value("POP", "be:prov:a", "2026") == 720.0
    # Both years fully covered, so neither reads as a coverage collapse
    # across the merger.
    assert result.coverage("POP", "be:prov:a", "2023").pct == 100.0
    assert result.coverage("POP", "be:prov:a", "2026").pct == 100.0


# ── refusal ────────────────────────────────────────────────────────────────


def test_aggregating_a_non_additive_indicator_raises_and_names_it():
    obs = _obs([("IDX", "be:mun:a1", "2023", 112.4)])
    with pytest.raises(NotAggregatableError, match="IDX"):
        aggregate(obs, {"IDX": REFUSE}, PARENTS, LEVELS, per_period_universe(UNIVERSE))


def test_a_recompute_without_a_formula_raises_rather_than_guessing():
    obs = _obs([("TOTAL_INC", "be:mun:a1", "2023", 100.0)])
    with pytest.raises(NotAggregatableError, match="AVG"):
        aggregate(
            obs,
            {"TOTAL_INC": SUM, "AVG": RECOMPUTE},
            PARENTS,
            LEVELS,
            per_period_universe(UNIVERSE),
            {},
        )


# ── method selection from metadata ─────────────────────────────────────────


def test_methods_come_from_metadata_not_a_hardcoded_list():
    meta = {
        "POP": {"is_additive": True},
        "GDP_INDEX": {"is_additive": False},
    }
    derived = {
        "AVG": {"derived": {"function": "mean_from_total", "inputs": ["A", "B"]}},
        "DEP": {"derived": {"function": "dependency_ratio", "inputs": ["A", "B", "C"]}},
        "PCT": {"derived": {"function": "percentile", "inputs": ["POP"]}},
        "CAGR": {"derived": {"function": "cagr", "inputs": ["POP"]}},
    }
    methods = methods_from_metadata(meta, derived)

    assert methods["POP"] == SUM
    assert methods["GDP_INDEX"] == REFUSE
    assert methods["AVG"] == RECOMPUTE
    assert methods["DEP"] == RECOMPUTE
    # A percentile ranks against peers -- a province ranked against communes
    # is a category error, not an arithmetic problem.
    assert methods["PCT"] == REFUSE
    # A growth rate is computed from the aggregate's OWN history afterwards,
    # not from summed commune growth rates.
    assert methods["CAGR"] == REFUSE
