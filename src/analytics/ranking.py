"""Where a commune sits among its peers -- Block L, docs/features/comparison.md.

`derived.percentile` already computes the percentile rank itself. What this
module adds is the three things a published percentile needs in order to be
falsifiable, and the one case where it must not be published at all:

* THE UNIVERSE, stated. "80th percentile" is unfalsifiable without "of what,
  and when". Every result carries the peer-set size and the period.
* THE ORDINAL RANK alongside it, because "4th of 19" is what a bourgmestre
  repeats in a meeting, and it is checkable against the published data.
* A MINIMUM PEER SET. Measured: Belgium has 565 communes, Flanders 285,
  Wallonia 261 -- and Brussels-Capital just 19. Over 19 items one rank step is
  5.26 percentile points and the only attainable values are 2.6, 7.9, 13.2,
  ... Publishing "73.7th percentile in your region" from 19 observations reads
  as measured precision and is not. Below the floor the rank is reported and
  the percentile is withheld: exactly as informative, and it claims nothing
  false.

Pure: the caller assembles the peer values.
"""

from collections.abc import Iterable

from src.analytics.derived import percentile

# Below this many peers a percentile is withheld and only the rank is given.
# 30 is a judgement, not a measurement -- see comparison.md's open questions.
# Its practical effect today is precise and intended: Belgium (565), Flanders
# (285) and Wallonia (261) publish percentiles; Brussels-Capital (19) does not.
MIN_PEERS_FOR_PERCENTILE = 30


def rank_within(value: float | None, peers: Iterable[float | None]) -> tuple[int, int] | None:
    """(rank, n) with 1 = highest, ties sharing the best rank.

    Ties share a rank for the same reason `percentile` gives tied communes the
    same percentile: two communes with identical values cannot defensibly be
    told they are 4th and 5th. So values 10, 10, 8 rank 1, 1, 3 -- the ordinal
    equivalent of the `0.5 x equal` term, and it means ranks can skip.

    `peers` must include the subject; it is one of the things being ranked.
    """
    if value is None:
        return None
    values = [float(v) for v in peers if v is not None]
    if not values:
        return None
    subject = float(value)
    above = sum(1 for v in values if v > subject)
    return above + 1, len(values)


def position(
    value: float | None,
    peers: Iterable[float | None],
    min_peers: int = MIN_PEERS_FOR_PERCENTILE,
) -> dict | None:
    """Where `value` sits among `peers`, with the universe attached.

    Returns None when there is nothing to rank against. `pct` is None -- the
    key present but empty -- when the peer set is below the floor, so a caller
    can tell "too small a universe to express as a percentile" apart from "no
    data", which are different things to say to a reader.
    """
    peer_values = [v for v in peers if v is not None]
    ranked = rank_within(value, peer_values)
    if ranked is None:
        return None
    rank, n = ranked
    return {
        "pct": percentile(value, peer_values) if n >= min_peers else None,
        "rank": rank,
        "n": n,
    }
