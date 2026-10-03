"""src/analytics/live_counters.py -- hand-computed expected values (rule 5).

Every "real data" figure here is the Eurostat value this PR's own fetch
loaded 2026-10-03 (docs/data_catalog.md's new public-finance section):
GOV_REVENUE_BE/GOV_EXPENDITURE_BE 2022 & 2025, GOV_TAX_*_BE and
GOV_EXP_*_BE 2025/2024. Arithmetic shown in each test's own comment.
"""

from decimal import Decimal

import pytest

from src.analytics.live_counters import (
    LiveCounterError,
    Unavailable,
    annual_value,
    apply_shares,
    compute_breakdown,
    difference_segments,
    flow_segments,
    period_end_ms,
    population_segments,
    scale_for,
    shares_from_breakdown,
    stock_segments,
    trend_growth,
    year_bounds,
)

# ── year_bounds / period_end_ms ─────────────────────────────────────────────


def test_year_bounds_2026_is_1_january_at_fixed_plus_one():
    # 1 Jan 2026 00:00 +01:00 = 2025-12-31T23:00:00Z.
    # epoch seconds = 1766969999... computed directly: datetime(2026,1,1,tzinfo=+01:00)
    # .timestamp() -- given by the handoff as the expected value.
    assert year_bounds(2026) == (1767222000000, 1798758000000)


def test_year_bounds_2028_is_a_leap_year():
    start, end = year_bounds(2028)
    # 2028 is divisible by 4 and not by 100: 366 days.
    assert end - start == 366 * 24 * 60 * 60 * 1000 == 31_622_400_000


def test_year_bounds_is_the_same_instant_as_period_end_ms_of_the_prior_year():
    assert period_end_ms("2025") == year_bounds(2026)[0]


def test_period_end_ms_q1_2026_is_1_april():
    # Q1 2026 ends when Q2 starts: 1 April 2026 00:00 +01:00.
    assert period_end_ms("2026-Q1") == 1_774_998_000_000


def test_period_end_ms_q4_rolls_into_next_year():
    assert period_end_ms("2025-Q4") == year_bounds(2026)[0]


def test_period_end_ms_rejects_a_malformed_period():
    with pytest.raises(LiveCounterError):
        period_end_ms("2026-13")


# ── trend_growth ─────────────────────────────────────────────────────────────


def test_trend_growth_1000_to_1331_over_3_years_is_10_percent():
    # 1.1 ** 3 == 1.331 exactly, so (1331/1000)**(1/3) - 1 == 0.1 exactly.
    g = trend_growth({2023: 1000, 2026: 1331}, 2026, window=3)
    assert g == Decimal("0.1000000000")


def test_trend_growth_revenue_real_2022_to_2025():
    # (314736.4 / 274862.7) ** (1/3) - 1, rounded to 10 dp.
    g = trend_growth({2022: 274862.7, 2025: 314736.4}, 2025)
    assert g == Decimal("0.0461895754")


def test_trend_growth_spending_real_2022_to_2025():
    # (347956.3 / 294745.9) ** (1/3) - 1, rounded to 10 dp.
    g = trend_growth({2022: 294745.9, 2025: 347956.3}, 2025)
    assert g == Decimal("0.0568799130")


def test_trend_growth_missing_base_year_is_unavailable_not_raised():
    result = trend_growth({2026: 1331}, 2026, window=3)
    assert isinstance(result, Unavailable)
    assert result.reason == "missing_year:2023"


def test_trend_growth_missing_latest_year_is_unavailable():
    result = trend_growth({2023: 1000}, 2026, window=3)
    assert result == Unavailable("missing_year:2026")


def test_trend_growth_none_value_is_unavailable():
    result = trend_growth({2023: None, 2026: 1331}, 2026, window=3)
    assert result == Unavailable("missing_year:2023")


def test_trend_growth_non_positive_value_is_unavailable():
    assert trend_growth({2023: 0, 2026: 1331}, 2026) == Unavailable("non_positive_value")
    assert trend_growth({2023: -5, 2026: 1331}, 2026) == Unavailable("non_positive_value")


# ── annual_value / projection ────────────────────────────────────────────────


def test_annual_value_is_official_up_to_the_latest_year():
    series = {2024: 308044.6, 2025: 314736.4}
    assert annual_value(series, 2025, 2025, Decimal("0.05")) == Decimal("314736.4")


def test_annual_value_projects_with_the_rounded_growth_rate():
    g_rev = trend_growth({2022: 274862.7, 2025: 314736.4}, 2025)
    g_sp = trend_growth({2022: 294745.9, 2025: 347956.3}, 2025)
    # TR_2026 = 314736.4 * (1 + 0.0461895754) ** 1
    tr_2026 = annual_value({2025: 314736.4}, 2026, 2025, g_rev)
    te_2026 = annual_value({2025: 347956.3}, 2026, 2025, g_sp)
    assert tr_2026 == Decimal("329273.94067892456")
    assert te_2026 == Decimal("367748.02407180190")
    deficit_2026 = te_2026 - tr_2026
    assert deficit_2026 == Decimal("38474.08339287734")
    # Sanity: 2025's own OFFICIAL deficit (not projected) is TE - TR that year.
    deficit_2025_official = Decimal("347956.3") - Decimal("314736.4")
    assert deficit_2025_official == Decimal("33219.9")
    # The projected 2026 deficit is meaningfully wider than the latest
    # official one -- the ADR's own "not a forecast" warning.
    assert deficit_2026 > deficit_2025_official


def test_annual_value_propagates_an_unavailable_growth_rate():
    result = annual_value({2025: 100.0}, 2026, 2025, Unavailable("missing_year:2022"))
    assert result == Unavailable("missing_year:2022")


def test_annual_value_missing_official_year_is_unavailable():
    assert annual_value({}, 2025, 2025, Decimal("0.05")) == Unavailable("missing_year:2025")


# ── scale_for ────────────────────────────────────────────────────────────────


def test_scale_for_meur_is_one_million():
    assert scale_for("meur") == Decimal(1_000_000)


def test_scale_for_unknown_unit_raises():
    with pytest.raises(LiveCounterError):
        scale_for("widgets")


# ── flow_segments / difference_segments ─────────────────────────────────────


def test_a_flow_segment_evaluated_one_day_in_gives_one_million_eur():
    # A 365 M EUR/year flow: rate = 365,000,000 / (365 days in ms).
    # Value one day (86,400,000 ms) in = 365e6 * 1/365 = 1,000,000.00 EUR
    # exactly -- the browser's whole job is v0 + rate*(t - start).
    segments = flow_segments({2026: Decimal("365000000")}, 2026, 2026)
    assert not isinstance(segments, Unavailable)
    (segment,) = segments
    t = segment.start_ms + 86_400_000
    value = segment.v0 + segment.rate_per_ms * (t - segment.start_ms)
    assert value == Decimal("1000000.000000000000000000000")


def test_flow_segments_reset_to_zero_every_year():
    segments = flow_segments({2026: Decimal("100.00"), 2027: Decimal("200.00")}, 2026, 2027)
    assert [s.v0 for s in segments] == [Decimal("0"), Decimal("0")]
    assert [s.v1 for s in segments] == [Decimal("100.00"), Decimal("200.00")]
    # Continuous year boundary, no gap.
    assert segments[0].end_ms == segments[1].start_ms


def test_flow_segments_missing_year_is_unavailable():
    result = flow_segments({2026: Decimal("1")}, 2026, 2027)
    assert result == Unavailable("missing_year:2027")


def test_difference_segments_is_spending_minus_revenue():
    revenue = flow_segments({2026: Decimal("300")}, 2026, 2026)
    spending = flow_segments({2026: Decimal("350")}, 2026, 2026)
    (deficit,) = difference_segments(revenue, spending)
    assert deficit.v1 == Decimal("50")


def test_difference_segments_refuses_mismatched_boundaries():
    a = flow_segments({2026: Decimal("1")}, 2026, 2026)
    b = flow_segments({2027: Decimal("1")}, 2027, 2027)
    with pytest.raises(LiveCounterError):
        difference_segments(a, b)


# ── stock_segments (debt) ────────────────────────────────────────────────────


def test_debt_style_stock_segment_reaches_the_expected_value_at_year_end():
    # Anchor: 1000 M EUR at 2026-04-01 00:00 +01:00. Pace: 365 M EUR/year.
    # From the anchor to 2027-01-01 is 275 of 2026's 365 days, so the stock
    # gains 365,000,000 * 275/365 = 275,000,000 -> 1,275,000,000 EUR.
    anchor_ms = period_end_ms("2026-Q1")
    horizon_end = year_bounds(2027)[1]
    pace = {2026: Decimal("365000000"), 2027: Decimal("365000000")}
    segments = stock_segments(anchor_ms, Decimal("1000000000"), pace, horizon_end)
    assert not isinstance(segments, Unavailable)
    assert segments[0].v0 == Decimal("1000000000.00")
    assert segments[0].v1 == Decimal("1275000000.00")
    assert segments[0].end_ms == year_bounds(2027)[0]
    # Continuity: one segment's v1 is exactly the next one's v0.
    assert segments[1].v0 == segments[0].v1
    assert segments[1].v1 == Decimal("1640000000.00")  # 1,275M + 365M


def test_stock_segments_anchor_beyond_horizon_is_unavailable():
    anchor_ms = year_bounds(2030)[0]
    horizon_end = year_bounds(2027)[1]
    result = stock_segments(anchor_ms, Decimal("1"), {}, horizon_end)
    assert result == Unavailable("anchor_beyond_horizon")


def test_stock_segments_missing_pace_year_is_unavailable():
    anchor_ms = year_bounds(2026)[0]
    horizon_end = year_bounds(2028)[1]
    result = stock_segments(anchor_ms, Decimal("1"), {2026: Decimal("1")}, horizon_end)
    assert result == Unavailable("missing_year:2027")


# ── population_segments ──────────────────────────────────────────────────────


def test_population_segments_real_2025_to_2026():
    # pop_2026 - pop_2025 = 11,867,634 - 11,825,551 = 42,083/year.
    horizon_end = year_bounds(2028)[1]
    segments = population_segments(
        11_867_634,
        11_825_551,
        2026,
        horizon_end,
        coverage_latest_pct=100.0,
        coverage_previous_pct=100.0,
    )
    assert not isinstance(segments, Unavailable)
    assert segments[0].v0 == 11_867_634
    assert segments[0].v1 == 11_909_717  # + 42,083 over 2026
    # Halfway through 2026 (365-day, non-leap: half = 15,768,000,000 ms).
    t = segments[0].start_ms + 15_768_000_000
    value = segments[0].v0 + segments[0].rate_per_ms * (t - segments[0].start_ms)
    assert value == Decimal("11888675.50000000000000000000")
    # At 2027-01-01.
    assert segments[0].end_ms == year_bounds(2027)[0]


def test_population_segments_coverage_below_100_is_unavailable():
    horizon_end = year_bounds(2028)[1]
    result = population_segments(
        100, 90, 2026, horizon_end, coverage_latest_pct=97.2, coverage_previous_pct=100.0
    )
    assert result == Unavailable("coverage_below_100:97.2/100.0")
    result2 = population_segments(
        100, 90, 2026, horizon_end, coverage_latest_pct=100.0, coverage_previous_pct=99.9
    )
    assert isinstance(result2, Unavailable)


# ── compute_breakdown / shares_from_breakdown / apply_shares ────────────────

_NAMED_TAXES = {
    "social_contributions": Decimal("97512.6"),
    "pit": Decimal("75791.1"),
    "cit": Decimal("26308.0"),
    "vat": Decimal("39796.6"),
    "excise": Decimal("11186.1"),
}
_TAXAG_TOTAL = Decimal("282517.4")
# D.995 fix (fix round, 2026-10-03): GOV_TAX_SSC_TOTAL_BE's own na_item nets
# D.995 (taxes/SSC assessed but unlikely to be collected) out; TR does not.
# The GROSS tax total other_taxes computes against is taxag_total + D995,
# never taxag_total alone -- real fetched 2025 D995 value (live-checked,
# docs/data_catalog.md).
_D995_2025 = Decimal("895.2")
_GROSS_TAXAG_TOTAL = _TAXAG_TOTAL + _D995_2025  # 282517.4 + 895.2 = 283412.6
_TR_2025 = Decimal("314736.4")
_TOLERANCE = Decimal("0.5")

_NAMED_COFOG = {
    "old_age": Decimal("62983.9"),
    "survivors": Decimal("8808.3"),
    "health": Decimal("49579.9"),
    "education": Decimal("39283.6"),
    "defence": Decimal("7946.4"),
    "unemployment": Decimal("6574.9"),
    "sickness_disability": Decimal("24254.6"),
    "family": Decimal("13508.8"),
    "debt_transactions": Decimal("14475.6"),
    "economic_affairs": Decimal("39854.2"),
    "public_order": Decimal("10647.7"),
}
_COFOG_TOTAL = Decimal("335287.9")


def test_gross_taxag_total_does_not_exceed_total_revenue_for_the_breakdown_year():
    # Sanity the engine's two-level revenue breakdown depends on: the GROSS
    # taxag total (282,517.4 + 895.2 = 283,412.6) must not exceed TR
    # (314,736.4), or "non_tax_revenue" would be negative beyond what
    # clamping can absorb. This is the same compute_breakdown clamp/refuse
    # behaviour the handoff calls "the whole >= sum of parts / taxag <= TR
    # sanity check" -- now asserted against the gross total, not the
    # narrower one that used to silently absorb D.995.
    assert _GROSS_TAXAG_TOTAL <= _TR_2025


def test_revenue_breakdown_other_taxes_and_non_tax_revenue():
    # other_taxes = GROSS taxag total - sum(5 named)
    #             = 283412.6 - 250594.4 = 32818.2
    # (gross taxag total: 282517.4 + 895.2 = 283,412.6 -- GOV_TAX_SSC_TOTAL_BE
    # nets D.995 out, so it must be added back here, D.995 fix, fix round
    # 2026-10-03; sum of named: 97512.6+75791.1+26308.0+39796.6+11186.1 =
    # 250,594.4)
    inner = compute_breakdown(_GROSS_TAXAG_TOTAL, _NAMED_TAXES, "other_taxes", _TOLERANCE)
    assert inner["other_taxes"] == Decimal("32818.2")
    # non_tax_revenue = TR - GROSS taxag total = 314736.4 - 283412.6 = 31323.8
    outer = compute_breakdown(_TR_2025, inner, "non_tax_revenue", _TOLERANCE)
    assert outer["non_tax_revenue"] == Decimal("31323.8")
    assert sum(outer.values()) == _TR_2025

    shares = shares_from_breakdown(outer, _TR_2025)
    # D61 (social contributions) share = 97512.6 / 314736.4 = 0.309823077...
    assert shares["social_contributions"].quantize(Decimal("0.000000001")) == Decimal("0.309823077")
    assert sum(shares.values()) == Decimal(1)


def test_spending_breakdown_other_functions():
    # other_functions = COFOG total - sum(11 named) = 335287.9 - 277917.9 =
    # 57370.0 (sum of the 11 named parts is 277,917.9).
    breakdown = compute_breakdown(_COFOG_TOTAL, _NAMED_COFOG, "other_functions", _TOLERANCE)
    assert breakdown["other_functions"] == Decimal("57370.0")
    assert sum(breakdown.values()) == _COFOG_TOTAL

    shares = shares_from_breakdown(breakdown, _COFOG_TOTAL)
    # share = 57370.0 / 335287.9 = 0.171106682...
    assert shares["other_functions"].quantize(Decimal("0.000000001")) == Decimal("0.171106682")


def test_shares_applied_to_a_projected_year_sum_exactly_and_never_go_negative():
    breakdown = compute_breakdown(_COFOG_TOTAL, _NAMED_COFOG, "other_functions", _TOLERANCE)
    shares = shares_from_breakdown(breakdown, _COFOG_TOTAL)
    # A later year's own total differs from the breakdown year's -- shares
    # still sum exactly, because the designated remainder absorbs rounding.
    projected_total = Decimal("367748.02407180190")
    parts = apply_shares(shares, projected_total, "other_functions")
    assert sum(parts.values()) == projected_total
    assert all(v >= 0 for v in parts.values())


def test_breakdown_remainder_below_tolerance_is_unavailable():
    # remainder = 100 - 101 = -1, tolerance 0.5 -> below tolerance.
    result = compute_breakdown(Decimal("100"), {"a": Decimal("101")}, "r", _TOLERANCE)
    assert result == Unavailable("remainder_below_tolerance:-1")


def test_breakdown_remainder_within_tolerance_is_clamped_and_renormalised():
    # remainder = 100 - 100.4 = -0.4, within tolerance -> clamp to 0, then
    # rescale ALL parts (named + remainder) by 100 / 100.4 so they sum back
    # to exactly 100, with none negative.
    named = {"a": Decimal("60.2"), "b": Decimal("40.2")}
    result = compute_breakdown(Decimal("100"), named, "r", _TOLERANCE)
    assert not isinstance(result, Unavailable)
    assert sum(result.values()) == Decimal("100")
    assert all(v >= 0 for v in result.values())
    assert result["r"] == Decimal(0)
    # Both named parts shrank proportionally (neither kept its raw value).
    assert result["a"] < named["a"]
    assert result["b"] < named["b"]


def test_breakdown_non_negative_remainder_is_returned_as_is():
    result = compute_breakdown(Decimal("100"), {"a": Decimal("60")}, "r", _TOLERANCE)
    assert result == {"a": Decimal("60"), "r": Decimal("40")}
