"""src/analytics/peers.py -- hand-computed expected values (CLAUDE.md rule 5).

FIXTURE (used throughout): 4 communes, NIS "10001".."10004", two raw
variables var_a, var_b, no transform:

    NIS     var_a   var_b
    10001   1       2
    10002   3       2
    10003   1       4
    10004   9       9

Hand computation, ddof=0 (population std, divide by N=4):

    mean_a = (1+3+1+9)/4 = 3.5
    var_a_pop = ((1-3.5)^2+(3-3.5)^2+(1-3.5)^2+(9-3.5)^2)/4
              = (6.25+0.25+6.25+30.25)/4 = 43/4 = 10.75
    std_a = sqrt(10.75) = 3.278719262151...

    mean_b = (2+2+4+9)/4 = 4.25
    var_b_pop = ((2-4.25)^2+(2-4.25)^2+(4-4.25)^2+(9-4.25)^2)/4
              = (5.0625+5.0625+0.0625+22.5625)/4 = 32.75/4 = 8.1875
    std_b = sqrt(8.1875) = 2.861380785565...

    z_a = (a - 3.5) / 3.278719262151
        10001: (1-3.5)/3.278719262151   = -0.762493
        10002: (3-3.5)/3.278719262151   = -0.152499
        10003: (1-3.5)/3.278719262151   = -0.762493
        10004: (9-3.5)/3.278719262151   =  1.677484

    z_b = (b - 4.25) / 2.861380785565
        10001: (2-4.25)/2.861380785565  = -0.786334
        10002: (2-4.25)/2.861380785565  = -0.786334
        10003: (4-4.25)/2.861380785565  = -0.087370
        10004: (9-4.25)/2.861380785565  =  1.660038

    Euclidean distance matrix (sqrt of sum of squared z-differences):
        d(10001,10002) = sqrt((-0.762493-(-0.152499))^2 + (-0.786334-(-0.786334))^2)
                        = sqrt((-0.609994)^2 + 0^2) = 0.609994
        d(10001,10003) = sqrt((-0.762493-(-0.762493))^2 + (-0.786334-(-0.087370))^2)
                        = sqrt(0^2 + (-0.698963)^2) = 0.698963
        d(10001,10004) = sqrt((-0.762493-1.677484)^2 + (-0.786334-1.660038)^2)
                        = sqrt((-2.439977)^2 + (-2.446372)^2) = sqrt(5.9535+5.9847) = 3.455173
        d(10002,10003) = sqrt((-0.152499-(-0.762493))^2 + (-0.786334-(-0.087370))^2)
                        = sqrt(0.609994^2 + (-0.698963)^2) = 0.927708
        d(10002,10004) = sqrt((-0.152499-1.677484)^2 + (-0.786334-1.660038)^2)
                        = sqrt((-1.829983)^2 + (-2.446372)^2) = 3.055089
        d(10003,10004) = sqrt((-0.762493-1.677484)^2 + (-0.087370-1.660038)^2)
                        = sqrt((-2.439977)^2 + (-1.747408)^2) = 3.001154

    So for k=1 (this fixture's small size), nearest peer of:
        10001 -> 10002 (0.609994) [vs 10003 at 0.698963, vs 10004 at 3.455173]
        10002 -> 10001 (0.609994) [vs 10003 at 0.927708]
        10003 -> 10001 (0.698963) [vs 10002 at 0.927708]
        10004 -> 10003 (3.001154) [closest of a bad lot]

All values below are asserted to 4-5 decimal places, matching this hand
arithmetic.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.analytics.peers import (
    MIN_PEERS_WITH_VALUE,
    PeerModelError,
    Variable,
    build_feature_matrix,
    nearest,
    pairwise_distances,
    pca_reduce,
    peer_stats,
    similarity_score,
    standardise,
)

NIS = ["10001", "10002", "10003", "10004"]


def _raw_values() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "var_a": [1.0, 3.0, 1.0, 9.0],
            "var_b": [2.0, 2.0, 4.0, 9.0],
        },
        index=NIS,
    )


NONE_VARS = (
    Variable("var_a", period="2026", transform="none", description="test var a"),
    Variable("var_b", period="2026", transform="none", description="test var b"),
)


# ── build_feature_matrix ──────────────────────────────────────────────────────


def test_feature_matrix_no_transform_passes_values_through():
    X = build_feature_matrix(_raw_values(), NONE_VARS)
    assert list(X.index) == NIS  # sorted
    assert X.loc["10001", "var_a"] == 1.0
    assert X.loc["10004", "var_b"] == 9.0


def test_feature_matrix_refuses_on_missing_value():
    values = _raw_values()
    values.loc["10002", "var_a"] = None
    with pytest.raises(PeerModelError) as exc:
        build_feature_matrix(values, NONE_VARS)
    assert "10002" in str(exc.value)
    assert "var_a" in str(exc.value)


def test_feature_matrix_refuses_on_non_finite_value():
    values = _raw_values()
    values.loc["10003", "var_b"] = float("inf")
    with pytest.raises(PeerModelError) as exc:
        build_feature_matrix(values, NONE_VARS)
    assert "10003" in str(exc.value)


def test_feature_matrix_refuses_on_non_positive_before_log():
    log_var = (Variable("var_a", period="2026", transform="log", description="logged"),)
    values = pd.DataFrame({"var_a": [1.0, 0.0, -2.0, 9.0]}, index=NIS)
    with pytest.raises(PeerModelError) as exc:
        build_feature_matrix(values, log_var)
    message = str(exc.value)
    assert "10002" in message  # value 0.0
    assert "10003" in message  # value -2.0


def test_feature_matrix_applies_natural_log():
    log_var = (Variable("var_a", period="2026", transform="log", description="logged"),)
    values = pd.DataFrame({"var_a": [1.0, np.e, np.e**2, 9.0]}, index=NIS)
    X = build_feature_matrix(values, log_var)
    assert X.loc["10001", "var_a"] == pytest.approx(0.0)
    assert X.loc["10002", "var_a"] == pytest.approx(1.0)
    assert X.loc["10003", "var_a"] == pytest.approx(2.0)


def test_feature_matrix_lists_every_offending_cell_not_just_first():
    values = _raw_values()
    values.loc["10001", "var_a"] = None
    values.loc["10004", "var_b"] = None
    with pytest.raises(PeerModelError) as exc:
        build_feature_matrix(values, NONE_VARS)
    message = str(exc.value)
    assert "10001" in message and "var_a" in message
    assert "10004" in message and "var_b" in message


# ── standardise ───────────────────────────────────────────────────────────────


def test_standardise_matches_hand_computation():
    X = build_feature_matrix(_raw_values(), NONE_VARS)
    Z, means, stds = standardise(X)

    assert means["var_a"] == pytest.approx(3.5)
    assert stds["var_a"] == pytest.approx(3.278719262151, abs=1e-9)
    assert means["var_b"] == pytest.approx(4.25)
    assert stds["var_b"] == pytest.approx(2.861380785565, abs=1e-9)

    assert Z.loc["10001", "var_a"] == pytest.approx(-0.762493, abs=1e-5)
    assert Z.loc["10002", "var_a"] == pytest.approx(-0.152499, abs=1e-5)
    assert Z.loc["10003", "var_a"] == pytest.approx(-0.762493, abs=1e-5)
    assert Z.loc["10004", "var_a"] == pytest.approx(1.677484, abs=1e-5)

    assert Z.loc["10001", "var_b"] == pytest.approx(-0.786334, abs=1e-5)
    assert Z.loc["10002", "var_b"] == pytest.approx(-0.786334, abs=1e-5)
    assert Z.loc["10003", "var_b"] == pytest.approx(-0.087370, abs=1e-5)
    assert Z.loc["10004", "var_b"] == pytest.approx(1.660038, abs=1e-5)


def test_standardise_equals_hand_computed_ddof0_formula_no_sklearn_import():
    """StandardScaler equivalence, proven without importing sklearn (module
    exclusion: no scikit-learn dependency anywhere): z equals
    (x - mean) / population_std computed independently here by hand-rolled
    numpy, matching src/analytics/peers.py's own numpy formula bit for bit."""
    X = build_feature_matrix(_raw_values(), NONE_VARS)
    Z, _means, _stds = standardise(X)

    raw = _raw_values()
    for col in ("var_a", "var_b"):
        arr = raw[col].to_numpy(dtype="float64")
        expected = (arr - arr.mean()) / arr.std(ddof=0)  # ddof=0 = population std
        for i, nis in enumerate(NIS):
            assert Z.loc[nis, col] == pytest.approx(expected[i], abs=1e-12)


def test_standardise_refuses_zero_std_column():
    values = pd.DataFrame({"var_a": [5.0, 5.0, 5.0, 5.0], "var_b": [1.0, 2.0, 3.0, 4.0]}, index=NIS)
    X = build_feature_matrix(values, NONE_VARS)
    with pytest.raises(PeerModelError) as exc:
        standardise(X)
    assert "var_a" in str(exc.value)


# ── pairwise_distances / nearest ──────────────────────────────────────────────


def test_pairwise_distances_matches_hand_computation():
    X = build_feature_matrix(_raw_values(), NONE_VARS)
    Z, _m, _s = standardise(X)
    D = pairwise_distances(Z)

    assert D.loc["10001", "10001"] == pytest.approx(0.0, abs=1e-9)
    assert D.loc["10001", "10002"] == pytest.approx(0.609994, abs=1e-5)
    assert D.loc["10001", "10003"] == pytest.approx(0.698963, abs=1e-5)
    assert D.loc["10001", "10004"] == pytest.approx(3.455173, abs=1e-4)
    assert D.loc["10002", "10003"] == pytest.approx(0.927708, abs=1e-5)
    assert D.loc["10002", "10004"] == pytest.approx(3.055089, abs=1e-4)
    assert D.loc["10003", "10004"] == pytest.approx(3.001154, abs=1e-4)
    # symmetric
    assert D.loc["10002", "10001"] == pytest.approx(D.loc["10001", "10002"])


def test_nearest_k1_matches_hand_ranking():
    X = build_feature_matrix(_raw_values(), NONE_VARS)
    Z, _m, _s = standardise(X)
    D = pairwise_distances(Z)
    result = nearest(D, NIS, k=1)

    assert result["10001"] == [("10002", pytest.approx(0.609994, abs=1e-5))]
    assert result["10002"] == [("10001", pytest.approx(0.609994, abs=1e-5))]
    assert result["10003"] == [("10001", pytest.approx(0.698963, abs=1e-5))]
    assert result["10004"] == [("10003", pytest.approx(3.001154, abs=1e-4))]


def test_nearest_never_returns_self():
    X = build_feature_matrix(_raw_values(), NONE_VARS)
    Z, _m, _s = standardise(X)
    D = pairwise_distances(Z)
    result = nearest(D, NIS, k=3)
    for nis, peers in result.items():
        assert nis not in {p for p, _d in peers}


def test_nearest_ties_broken_by_nis_ascending():
    # Two communes with IDENTICAL feature vectors -> distance 0 to each
    # other AND to a shared third point; the exact-zero tie must resolve by
    # NIS, not by input order or hash order.
    values = pd.DataFrame({"var_a": [1.0, 1.0, 5.0, 9.0], "var_b": [2.0, 2.0, 5.0, 9.0]}, index=NIS)
    X = build_feature_matrix(values, NONE_VARS)
    Z, _m, _s = standardise(X)
    D = pairwise_distances(Z)
    # 10001 and 10002 are identical: both distance 0 from each other.
    result = nearest(D, NIS, k=3)
    # 10003's three candidates all at distinct distances from 10001/10002
    # (equal to each other) then 10004 -- 10001 and 10002 tie for closest to
    # 10003, must appear NIS-ascending: 10001 before 10002.
    peers_of_3 = [p for p, _d in result["10003"]]
    assert peers_of_3.index("10001") < peers_of_3.index("10002")


def test_nearest_restricts_to_pool_for_region_lists():
    X = build_feature_matrix(_raw_values(), NONE_VARS)
    Z, _m, _s = standardise(X)
    D = pairwise_distances(Z)
    # 10001's region pool is only {10003, 10004} (excludes 10002 even though
    # it is closer nationally).
    pool = {"10001": ["10003", "10004"]}
    result = nearest(D, ["10001"], k=2, pool=pool)
    peers = {p for p, _d in result["10001"]}
    assert peers == {"10003", "10004"}


def test_nearest_refuses_when_pool_smaller_than_k():
    X = build_feature_matrix(_raw_values(), NONE_VARS)
    Z, _m, _s = standardise(X)
    D = pairwise_distances(Z)
    pool = {"10001": ["10003"]}  # only 1 candidate, k=2 requested
    with pytest.raises(PeerModelError):
        nearest(D, ["10001"], k=2, pool=pool)


# ── similarity_score ───────────────────────────────────────────────────────


def test_similarity_score_matches_hand_computation():
    # d=0.609994, d_max_national=3.455173 (10001's own worst-of-ten stand-in)
    # score = 100 * (1 - 0.609994/3.455173) = 82.345486...
    score = similarity_score(0.609994, 3.455173)
    assert score == pytest.approx(82.345486, abs=1e-4)


def test_similarity_score_is_100_at_zero_distance():
    assert similarity_score(0.0, 3.455173) == pytest.approx(100.0)


def test_similarity_score_refuses_non_positive_dmax():
    with pytest.raises(PeerModelError):
        similarity_score(1.0, 0.0)


# ── PCA ────────────────────────────────────────────────────────────────────


def test_pca_one_component_when_second_variable_is_a_copy_of_first():
    values = pd.DataFrame({"var_a": [1.0, 3.0, 1.0, 9.0], "var_b": [1.0, 3.0, 1.0, 9.0]}, index=NIS)
    X = build_feature_matrix(values, NONE_VARS)
    Z, _m, _s = standardise(X)
    scores, explained = pca_reduce(Z, min_variance=0.90)
    assert list(scores.columns) == ["pc1"]
    assert explained[0] == pytest.approx(1.0, abs=1e-9)


def test_pca_deterministic_sign_convention():
    values = pd.DataFrame({"var_a": [1.0, 3.0, 1.0, 9.0], "var_b": [1.0, 3.0, 1.0, 9.0]}, index=NIS)
    X = build_feature_matrix(values, NONE_VARS)
    Z, _m, _s = standardise(X)
    scores1, _ = pca_reduce(Z, min_variance=0.90)
    scores2, _ = pca_reduce(Z, min_variance=0.90)
    # Deterministic across repeated calls.
    pd.testing.assert_frame_equal(scores1, scores2)
    # The commune with the largest |z| (10004) should have a positive score
    # component under the "largest-|loading| positive" convention, since
    # its z-values are the largest in magnitude and positive.
    assert scores1.loc["10004", "pc1"] > 0


# ── peer_stats ───────────────────────────────────────────────────────────────
#
# FIXTURE (docstring of peer_stats itself repeats this; kept in sync by hand):
# commune value = 1800, ten peers with usable values in the ordinary case:
#     [1000, 1100, 1210, 1300, 1400, 900, 800, 1250, 1500, 1600]
# sorted: 800, 900, 1000, 1100, 1210, 1250, 1300, 1400, 1500, 1600
# peer_median (even count -> mean of the 5th and 6th) = (1210+1250)/2 = 1230.0
# deviation_pct = (1800-1230)/1230*100 = 46.34146341463415...
# position = 1 (nothing among itself+10 peers exceeds 1800), of = 11

TEN_PEERS = [1000.0, 1100.0, 1210.0, 1300.0, 1400.0, 900.0, 800.0, 1250.0, 1500.0, 1600.0]
SEVEN_PEERS = [1000.0, 1100.0, 1210.0, 1300.0, 1400.0, 900.0, 800.0]  # exactly the floor


def test_peer_stats_even_peer_count_ten_peers():
    result = peer_stats(1800.0, TEN_PEERS)
    assert result["peer_median"] == pytest.approx(1230.0)
    assert result["peers_with_value"] == 10
    assert result["position"] == 1
    assert result["of"] == 11
    assert result["deviation_pct"] == pytest.approx(46.34146341463415)


def test_peer_stats_odd_peer_count_exactly_seven_is_the_floor_and_still_computes():
    # Exactly MIN_PEERS_WITH_VALUE (7) -- the floor is inclusive, not exclusive.
    assert MIN_PEERS_WITH_VALUE == 7
    result = peer_stats(1800.0, SEVEN_PEERS)
    # sorted: 800, 900, 1000, 1100, 1210, 1300, 1400 -- odd count, middle value
    assert result["peer_median"] == pytest.approx(1100.0)
    assert result["peers_with_value"] == 7
    assert result["position"] == 1
    assert result["of"] == 8
    assert result["deviation_pct"] == pytest.approx(63.63636363636363)


def test_peer_stats_six_peers_is_below_the_floor_and_is_null():
    six_peers = SEVEN_PEERS[:6]
    result = peer_stats(1800.0, six_peers)
    assert result["peers_with_value"] == 6
    assert result["peer_median"] is None
    assert result["position"] is None
    assert result["of"] is None
    assert result["deviation_pct"] is None


def test_peer_stats_suppressed_peer_is_excluded_not_zeroed():
    # A None among the ten peers (suppressed/na for this period) must be
    # excluded from the median's inputs entirely -- never counted as a 0,
    # which would pull the median toward zero (CLAUDE.md rule 26).
    nine_plus_one_suppressed = TEN_PEERS + [None]
    with_suppressed = peer_stats(1800.0, nine_plus_one_suppressed)
    without_it = peer_stats(1800.0, TEN_PEERS)
    assert with_suppressed == without_it
    assert with_suppressed["peers_with_value"] == 10  # not 11


def test_peer_stats_commune_has_no_value_is_null():
    result = peer_stats(None, TEN_PEERS)
    assert result["peer_median"] is None
    assert result["peers_with_value"] == 0
    assert result["position"] is None
    assert result["of"] is None
    assert result["deviation_pct"] is None


def test_peer_stats_median_zero_is_null_deviation():
    # Ten peers whose two middle sorted values (5th and 6th of 10, 0-indexed
    # 4 and 5) are -50 and 50, averaging to a median of exactly 0.
    peers = [-500.0, -400.0, -300.0, -200.0, -50.0, 50.0, 200.0, 300.0, 400.0, 500.0]
    result = peer_stats(600.0, peers)
    assert result["peer_median"] == pytest.approx(0.0)
    assert result["peers_with_value"] == 10
    assert result["deviation_pct"] is None
    # Position/rank are still meaningful even though the deviation is withheld.
    assert result["position"] is not None


def test_peer_stats_negative_median_is_null_deviation_assumption():
    # A signed-balance indicator (e.g. INTERNAL_MIGRATION_NET) can have a
    # negative peer median. The spec does not state what deviation_pct means
    # against a negative base; this test pins the assumption documented in
    # peer_stats' own docstring: deviation_pct (and the whole benchmark) is
    # null whenever the median is <= 0, not only when it is exactly 0,
    # because a percentage-from-a-negative-base sentence is not meaningful.
    peers = [-800.0, -700.0, -600.0, -500.0, -450.0, -450.0, -200.0, -100.0, -50.0, -10.0]
    # sorted median (5th/6th of 10, 0-indexed 4 and 5) = (-450 + -450)/2 = -450
    result = peer_stats(-100.0, peers)
    assert result["peer_median"] == pytest.approx(-450.0)
    assert result["peers_with_value"] == 10
    assert result["deviation_pct"] is None
    # Position is unaffected by the sign-of-median question -- -100 beats
    # seven of its ten peers (-200,-450,-450,-500,-600,-700,-800) and loses
    # to two (-50,-10), plus itself included -- rank among itself+10 peers.
    assert result["position"] == 3
    assert result["of"] == 11


def test_peer_stats_ties_share_rank():
    # Two peers equal to the commune's own value: rank_within's convention
    # (a tie shares the best rank) applies unchanged.
    peers = [1800.0, 1800.0, 900.0, 800.0, 700.0, 600.0, 500.0]
    result = peer_stats(1800.0, peers)
    assert result["position"] == 1
    assert result["of"] == 8
