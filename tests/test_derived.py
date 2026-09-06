"""Unit tests for the derived-indicator function library (Block G).

Every expected value here is computed BY HAND, with the arithmetic written
out in a comment, per the roadmap's requirement:

    "Testing a formula against its own implementation proves nothing. Only an
     independently computed number is a test."

So none of these assertions was produced by running the function and pasting
the result. Where a real figure is used it is a real Statbel value from the
committed store, named as such.
"""

import math

import pytest

from src.analytics.derived import (
    DerivationError,
    cagr,
    dependency_ratio,
    five_year_change,
    growth_rate,
    index_base_100,
    per_capita,
    percentile,
    regional_share,
    share_of_total,
    shift_period_years,
    z_score,
)

# ── Period shifting ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "period,years,expected",
    [
        ("2026", 5, "2021"),
        ("2026-Q2", 5, "2021-Q2"),  # five YEARS = twenty quarters, same quarter
        ("2026-06", 5, "2021-06"),
        ("2020", 1, "2019"),
        ("2020-Q1", 10, "2010-Q1"),
    ],
)
def test_shift_period_years(period, years, expected):
    assert shift_period_years(period, years) == expected


def test_shift_period_rejects_an_unparseable_period():
    with pytest.raises(DerivationError, match="Unrecognized period format"):
        shift_period_years("2026/06", 1)


# ── growth_rate ─────────────────────────────────────────────────────────────


def test_growth_rate_simple():
    # (125 - 100) / 100 * 100 = 25.0 exactly
    assert growth_rate({"2020": 100.0, "2025": 125.0}, "2025", years=5) == 25.0


def test_growth_rate_negative():
    # (80 - 100) / 100 * 100 = -20.0 exactly
    assert growth_rate({"2024": 100.0, "2025": 80.0}, "2025") == -20.0


def test_growth_rate_on_real_antwerp_population():
    """Real committed figures: Antwerp 517,042 (2016) -> 565,615 (2026).

    By hand: 565615 - 517042 = 48573.
             48573 / 517042 = 0.09394401...
               517042 * 0.09     = 46533.78   remainder 2039.22
               517042 * 0.003    =  1551.126  remainder  488.094
               517042 * 0.0009   =   465.3378 remainder   22.7562
               517042 * 0.00004  =    20.68168 remainder   2.07452
               517042 * 0.000004 =     2.068168
             -> 0.093944 -> 9.3944 %
    """
    series = {"2016": 517042.0, "2026": 565615.0}
    assert growth_rate(series, "2026", years=10) == pytest.approx(9.3944, abs=1e-4)


def test_growth_rate_is_null_when_an_endpoint_is_missing():
    assert growth_rate({"2025": 125.0}, "2025", years=5) is None
    assert growth_rate({"2020": 100.0}, "2025", years=5) is None


def test_growth_rate_is_null_when_an_endpoint_is_none():
    assert growth_rate({"2020": None, "2025": 125.0}, "2025", years=5) is None


def test_growth_rate_is_null_not_infinite_when_dividing_by_zero():
    assert growth_rate({"2020": 0.0, "2025": 125.0}, "2025", years=5) is None


def test_five_year_change_spans_twenty_quarters_on_a_quarterly_series():
    """The reason periods are specified in years, not period counts: a naive
    'five periods back' would read 2025-Q1 and be wrong by 15 quarters."""
    series = {"2021-Q2": 100.0, "2025-Q1": 999.0, "2026-Q2": 110.0}
    # (110 - 100) / 100 * 100 = 10.0 exactly, using 2021-Q2 not 2025-Q1
    assert five_year_change(series, "2026-Q2") == 10.0


# ── cagr ────────────────────────────────────────────────────────────────────


def test_cagr_doubling_over_ten_years():
    """Doubling over 10 years: (2 ** 0.1 - 1) * 100.
    2 ** 0.1 = 1.071773462536...  ->  7.1773462536 % per year.
    Sanity: the rule of 72 says 72 / 7.18 = 10.0 years to double. ✓
    """
    assert cagr({"2016": 100.0, "2026": 200.0}, "2026", years=10) == pytest.approx(
        7.1773462536, abs=1e-9
    )


def test_cagr_quadrupling_over_two_years():
    # (4 ** 0.5 - 1) * 100 = (2 - 1) * 100 = 100.0 % per year, exactly
    assert cagr({"2024": 50.0, "2026": 200.0}, "2026", years=2) == pytest.approx(100.0)


def test_cagr_is_null_for_non_positive_endpoints():
    """A real root of a negative ratio is undefined; returning null beats
    returning a complex or fabricated number."""
    assert cagr({"2016": -5.0, "2026": 200.0}, "2026", years=10) is None
    assert cagr({"2016": 100.0, "2026": 0.0}, "2026", years=10) is None
    assert cagr({"2016": 0.0, "2026": 200.0}, "2026", years=10) is None


def test_cagr_rejects_a_non_positive_horizon():
    with pytest.raises(DerivationError, match="positive horizon"):
        cagr({"2026": 1.0}, "2026", years=0)


# ── index_base_100 ──────────────────────────────────────────────────────────


def test_index_base_100():
    # 120 / 80 * 100 = 150.0 exactly
    assert index_base_100({"2010": 80.0, "2026": 120.0}, "2026", "2010") == 150.0


def test_index_base_100_of_the_base_period_is_100():
    assert index_base_100({"2010": 80.0}, "2010", "2010") == 100.0


def test_index_base_100_is_null_when_the_base_is_zero():
    assert index_base_100({"2010": 0.0, "2026": 120.0}, "2026", "2010") is None


# ── Ratios ──────────────────────────────────────────────────────────────────


def test_per_capita():
    # 2500 / 500 = 5.0 exactly
    assert per_capita(2500.0, 500.0) == 5.0


def test_per_capita_is_null_without_a_denominator():
    """There is no national population indicator, so a national per-capita
    request has no denominator -- null, never a guessed one."""
    assert per_capita(2500.0, None) is None
    assert per_capita(2500.0, 0.0) is None


def test_share_of_total():
    # 25 / 200 * 100 = 12.5 exactly
    assert share_of_total(25.0, 200.0) == 12.5


def test_regional_share():
    # 300 / 1200 * 100 = 25.0 exactly
    assert regional_share(300.0, 1200.0) == 25.0


def test_dependency_ratio():
    """(young + old) / working * 100.
    (3000 + 2000) / 10000 * 100 = 5000 / 10000 * 100 = 50.0 exactly
    """
    assert dependency_ratio(3000.0, 10000.0, 2000.0) == 50.0


def test_dependency_ratio_is_null_with_no_working_age_population():
    assert dependency_ratio(3000.0, 0.0, 2000.0) is None


def test_dependency_ratio_is_null_if_any_band_is_missing():
    assert dependency_ratio(3000.0, 10000.0, None) is None


def test_a_suppressed_value_derives_to_null_not_zero():
    """Statbel suppresses small cells. Zero would make a quiet commune look
    like a collapsed one -- the suppression must reach here as None."""
    assert per_capita(None, 500.0) is None
    assert share_of_total(None, 200.0) is None
    assert growth_rate({"2020": 100.0, "2025": None}, "2025", years=5) is None


# ── z_score ─────────────────────────────────────────────────────────────────


def test_z_score_uses_population_standard_deviation():
    """Peers [1,2,3,4,5]: mean = 15/5 = 3.
    Squared deviations: 4, 1, 0, 1, 4 -> sum 10.
    POPULATION variance = 10/5 = 2  (sample variance would be 10/4 = 2.5).
    stdev = sqrt(2) = 1.41421356...
    Subject 5: (5 - 3) / sqrt(2) = 2 / 1.41421356 = 1.41421356
    """
    assert z_score(5.0, [1.0, 2.0, 3.0, 4.0, 5.0]) == pytest.approx(math.sqrt(2), abs=1e-9)


def test_z_score_at_the_mean_is_zero():
    assert z_score(3.0, [1.0, 2.0, 3.0, 4.0, 5.0]) == 0.0


def test_z_score_is_null_with_zero_variance():
    """Every peer identical: the score is undefined, not infinite."""
    assert z_score(5.0, [5.0, 5.0, 5.0]) is None


def test_z_score_is_null_with_fewer_than_two_peers():
    assert z_score(5.0, [5.0]) is None
    assert z_score(5.0, []) is None


def test_z_score_ignores_null_peers():
    """A commune with no value is not comparable and must not count as a zero
    in the mean."""
    assert z_score(5.0, [1.0, None, 2.0, 3.0, None, 4.0, 5.0]) == pytest.approx(
        math.sqrt(2), abs=1e-9
    )


# ── percentile ──────────────────────────────────────────────────────────────


def test_percentile_with_ties_shares_a_rank():
    """Peers [10, 20, 20, 30, 40], subject = 20.
    below = 1 (just the 10), equal = 2 (both 20s), N = 5.
    100 * (1 + 0.5 * 2) / 5 = 100 * 2 / 5 = 40.0

    Both communes holding 20 get 40.0 -- a definition that gave them different
    ranks would be indefensible in a meeting.
    """
    peers = [10.0, 20.0, 20.0, 30.0, 40.0]
    assert percentile(20.0, peers) == 40.0


def test_percentile_of_the_lowest_value():
    # below = 0, equal = 1, N = 5 -> 100 * 0.5 / 5 = 10.0
    assert percentile(10.0, [10.0, 20.0, 20.0, 30.0, 40.0]) == 10.0


def test_percentile_of_the_highest_value():
    # below = 4, equal = 1, N = 5 -> 100 * 4.5 / 5 = 90.0
    assert percentile(40.0, [10.0, 20.0, 20.0, 30.0, 40.0]) == 90.0


def test_percentile_never_returns_exactly_zero_or_one_hundred():
    """With the 0.5 x equal term the subject is always counted in N, so no
    commune is ever reported as beating 100% of communes including itself."""
    peers = [1.0, 2.0, 3.0]
    assert 0.0 < percentile(1.0, peers) < 100.0
    assert 0.0 < percentile(3.0, peers) < 100.0


def test_percentile_ignores_null_peers():
    # peers with values: [10, 20, 30]; subject 20 -> below=1, equal=1, N=3
    # 100 * (1 + 0.5) / 3 = 150 / 3 = 50.0
    assert percentile(20.0, [10.0, None, 20.0, 30.0, None]) == 50.0


def test_percentile_is_null_for_a_null_subject():
    assert percentile(None, [1.0, 2.0, 3.0]) is None


def test_percentile_is_null_with_no_peers():
    assert percentile(5.0, []) is None
    assert percentile(5.0, [None, None]) is None


def test_percentile_differs_from_an_interpolated_quantile():
    """Guards the documented choice. For [10,20,30,40] and subject 20 this
    definition gives 100 * (1 + 0.5) / 4 = 37.5. numpy's default 'linear'
    quantile rank would put 20 at 33.33. The two genuinely disagree, which is
    why the definition is written down rather than left to a library default.
    """
    assert percentile(20.0, [10.0, 20.0, 30.0, 40.0]) == 37.5
