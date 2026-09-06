"""Tests for peer positioning (Block L).

Expected values worked out by hand, per CLAUDE.md rule 5. The two that matter
most are the tie handling and the peer-set floor: the first is where a rank
definition is likeliest to be indefensible, and the second is the whole reason
this module exists rather than calling `derived.percentile` directly.
"""

import pytest

from src.analytics.ranking import MIN_PEERS_FOR_PERCENTILE, position, rank_within


def test_rank_is_one_for_the_highest_value():
    assert rank_within(10.0, [10.0, 5.0, 1.0]) == (1, 3)


def test_rank_counts_from_the_top_not_the_bottom():
    # 5 has one value above it, so it is 2nd of 3.
    assert rank_within(5.0, [10.0, 5.0, 1.0]) == (2, 3)
    assert rank_within(1.0, [10.0, 5.0, 1.0]) == (3, 3)


def test_tied_values_share_the_better_rank_and_ranks_may_skip():
    """Values 10, 10, 8 rank 1, 1, 3. Telling two communes with identical
    figures that one is 1st and the other 2nd would be indefensible, which is
    the same reason `percentile` gives them the same percentile."""
    peers = [10.0, 10.0, 8.0]
    assert rank_within(10.0, peers) == (1, 3)
    assert rank_within(8.0, peers) == (3, 3)


def test_nulls_are_excluded_from_the_universe_not_counted_as_zero():
    """A commune with no value is not a commune ranked last -- it is a commune
    that is not in the ranking, and n must say so."""
    assert rank_within(5.0, [10.0, 5.0, None, None]) == (2, 2)


def test_no_value_and_no_peers_yield_nothing_rather_than_a_default():
    assert rank_within(None, [1.0, 2.0]) is None
    assert rank_within(5.0, []) is None
    assert rank_within(5.0, [None, None]) is None
    assert position(None, [1.0, 2.0]) is None
    assert position(5.0, []) is None


# ── the peer-set floor ─────────────────────────────────────────────────────


def test_a_large_peer_set_gets_a_percentile():
    peers = list(range(100))  # 0..99, so 50 has 50 below it
    result = position(50.0, [float(v) for v in peers])
    assert result["n"] == 100
    assert result["rank"] == 50  # 49 values above it (51..99)... see below
    # Hand-check: values above 50 are 51..99 = 49 of them, so rank 50.
    assert result["pct"] == pytest.approx(100 * (50 + 0.5) / 100)


def test_a_small_peer_set_gets_a_rank_but_no_percentile():
    """Brussels-Capital has 19 communes. Over 19 items one rank step is 5.26
    percentile points, so a percentile there implies precision the sample
    cannot carry. The rank is reported instead -- "4th of 19" -- which is
    exactly as informative and claims nothing false."""
    peers = [float(v) for v in range(19)]
    result = position(15.0, peers)

    assert result["n"] == 19
    assert result["rank"] == 4  # values above 15 are 16, 17, 18
    assert result["pct"] is None, "a 19-commune percentile must be withheld"


def test_the_floor_boundary_is_inclusive():
    """Exactly 30 peers publishes; 29 does not. Pinned because an off-by-one
    here silently changes what reaches a public page."""
    thirty = [float(v) for v in range(MIN_PEERS_FOR_PERCENTILE)]
    twenty_nine = [float(v) for v in range(MIN_PEERS_FOR_PERCENTILE - 1)]

    assert position(0.0, thirty)["pct"] is not None
    assert position(0.0, twenty_nine)["pct"] is None
    # The rank and universe are reported either way -- withholding the
    # percentile must not withhold the position.
    assert position(0.0, twenty_nine)["rank"] == MIN_PEERS_FOR_PERCENTILE - 1
    assert position(0.0, twenty_nine)["n"] == MIN_PEERS_FOR_PERCENTILE - 1


def test_pct_is_present_but_empty_below_the_floor():
    """A caller must be able to distinguish "universe too small to express as
    a percentile" from "no data at all". The first has a rank; the second is
    None entirely."""
    small = position(1.0, [1.0, 2.0])
    assert small is not None
    assert "pct" in small and small["pct"] is None
    assert position(None, [1.0, 2.0]) is None


def test_the_subject_is_counted_in_its_own_universe():
    """565 communes ranked among 565, not 564: the commune is one of the
    things being ranked. Herstappe, the smallest, is 565th of 565."""
    peers = [float(v) for v in range(565)]
    result = position(0.0, peers)
    assert result["n"] == 565
    assert result["rank"] == 565
    # Hand-check against derived.percentile's definition:
    # 100 x (0 below + 0.5 x 1 equal) / 565 = 0.0885
    assert result["pct"] == pytest.approx(100 * 0.5 / 565)
