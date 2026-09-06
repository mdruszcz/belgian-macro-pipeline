"""
The derived-indicator function library -- Block G,
docs/features/derived_indicators.md.

Every function here is PURE: it takes values and returns a value. None opens a
file, touches the database, or knows where an observation came from. That is
deliberate -- population lives in a committed CSV and everything else in
SQLite (ADR 0002), and a function that had to know which would be untestable
and would grow a second copy of the merge logic that
scripts/export_communes_csv.py already owns.

The three policies below are applied identically by every function, because
inconsistent null handling across functions is invisible in review and
corrosive in production:

  1. Any null input yields a null output. Never 0, never dropped, never
     carried forward from a neighbouring period.
  2. Division by zero yields null -- not infinity, not an exception. A commune
     with no working-age residents has no dependency ratio; that is a fact
     about the commune, not a crash.
  3. Nothing is rounded here. Rounding compounds through a chain (a per-capita
     figure feeding a growth rate), so it happens exactly once, at export.

A suppressed source value must reach these functions as None, never as 0 --
see the status enum in docs/features/data_model.md. Statbel suppresses small
cells, and 0 would make a quiet commune look like a collapsed one.
"""

import math
import re
from collections.abc import Iterable, Mapping

_YEAR = re.compile(r"^(\d{4})$")
_QUARTER = re.compile(r"^(\d{4})-Q([1-4])$")
_MONTH = re.compile(r"^(\d{4})-(\d{2})$")


class DerivationError(Exception):
    """A derivation that cannot be attempted at all -- as opposed to one that
    is simply undefined for this cell and returns null. Raised for structural
    mistakes (aggregating a non-additive indicator, an unparseable period),
    never for missing data."""


def shift_period_years(period: str, years: int) -> str:
    """The period `years` earlier, keeping the same frequency.

    Periods are shifted in YEARS rather than in period counts, which is what
    makes five_year_change span 20 quarters on a quarterly series without the
    caller having to know the frequency. Shifting the year component does that
    for free and is exact: five years before 2026-Q2 is 2021-Q2.
    """
    if match := _YEAR.match(period):
        return f"{int(match.group(1)) - years:04d}"
    if match := _QUARTER.match(period):
        return f"{int(match.group(1)) - years:04d}-Q{match.group(2)}"
    if match := _MONTH.match(period):
        return f"{int(match.group(1)) - years:04d}-{match.group(2)}"
    raise DerivationError(
        f"Unrecognized period format {period!r}. Expected YYYY, YYYY-Qn or YYYY-MM "
        "(docs/features/data_model.md)."
    )


def _at(series: Mapping[str, float | None], period: str) -> float | None:
    value = series.get(period)
    return None if value is None else float(value)


def _clean_peers(peers: Iterable[float | None]) -> list[float]:
    """Drop nulls from a peer set. A commune with no value is not comparable
    and must not silently count as a zero in the denominator."""
    return [float(p) for p in peers if p is not None]


# ── Time-series functions ────────────────────────────────────────────────────


def growth_rate(series: Mapping[str, float | None], period: str, years: int = 1) -> float | None:
    """Percentage change over `years`. None if either endpoint is missing, or
    if the earlier value is zero (the change is undefined, not infinite)."""
    current = _at(series, period)
    earlier = _at(series, shift_period_years(period, years))
    if current is None or earlier is None or earlier == 0:
        return None
    return (current - earlier) / earlier * 100.0


def five_year_change(series: Mapping[str, float | None], period: str) -> float | None:
    """Percentage change over five years -- growth_rate's common case, named
    because the roadmap treats five-year change as a headline figure."""
    return growth_rate(series, period, years=5)


def cagr(series: Mapping[str, float | None], period: str, years: int) -> float | None:
    """Compound annual growth rate, in percent per year.

    Both endpoints must be STRICTLY POSITIVE: the formula takes a real root of
    their ratio, which is undefined for a negative ratio and meaningless for a
    zero one. Returning null beats returning a complex or fabricated number.
    """
    if years <= 0:
        raise DerivationError(f"cagr needs a positive horizon, got years={years}")
    current = _at(series, period)
    earlier = _at(series, shift_period_years(period, years))
    if current is None or earlier is None or current <= 0 or earlier <= 0:
        return None
    return ((current / earlier) ** (1.0 / years) - 1.0) * 100.0


def index_base_100(
    series: Mapping[str, float | None], period: str, base_period: str
) -> float | None:
    """The value rebased so the base period equals 100."""
    current = _at(series, period)
    base = _at(series, base_period)
    if current is None or base is None or base == 0:
        return None
    return current / base * 100.0


# ── Ratio functions ─────────────────────────────────────────────────────────


def per_capita(value: float | None, population: float | None) -> float | None:
    """Value per inhabitant.

    The caller supplies the denominator explicitly, matched on the same
    geography and period -- this function cannot and must not guess one. There
    is no national population indicator in this pipeline, so a national
    per-capita figure has no denominator at all; the engine raises there rather
    than substituting a scaled national figure.
    """
    if value is None or population is None or population == 0:
        return None
    return value / population


def share_of_total(value: float | None, total: float | None) -> float | None:
    """Value as a percentage of a total."""
    if value is None or total is None or total == 0:
        return None
    return value / total * 100.0


def regional_share(commune_value: float | None, region_value: float | None) -> float | None:
    """A commune's share of its region, in percent.

    Only meaningful for additive indicators -- summing communes' GDP indices to
    a regional figure is nonsense. The engine enforces `is_additive` before
    calling this; see docs/features/derived_indicators.md.
    """
    return share_of_total(commune_value, region_value)


def dependency_ratio(
    young: float | None, working_age: float | None, old: float | None
) -> float | None:
    """(0-14 plus 65+) over 15-64, in percent -- the standard age dependency
    ratio.

    Computed from the three raw bands rather than stored, so a revision to any
    band recomputes it instead of leaving a stale figure (CONTROL G).
    """
    if young is None or working_age is None or old is None or working_age == 0:
        return None
    return (young + old) / working_age * 100.0


# ── Cross-sectional functions ───────────────────────────────────────────────


def z_score(value: float | None, peers: Iterable[float | None]) -> float | None:
    """Standard deviations from the peer mean.

    Population standard deviation (ddof=0), not sample: the peer set IS the
    whole population of communes, not a sample drawn from one. Null when fewer
    than two peers have values, or when every peer is identical (zero variance
    makes the score undefined, not infinite).

    CAVEAT, measured on the real 2026 data rather than assumed: Belgian
    commune sizes are heavily right-skewed (78 residents in Herstappe,
    565,615 in Antwerp), so a z-score on a raw COUNT is dominated by a handful
    of cities -- Antwerp scores 15.99, which is arithmetically correct and
    analytically useless. z_score belongs on rates, ratios and per-capita
    figures, not on raw counts. percentile is the safer headline for a skewed
    distribution because it is rank-based and therefore skew-insensitive.
    """
    if value is None:
        return None
    values = _clean_peers(peers)
    if len(values) < 2:
        return None
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    if variance == 0:
        return None
    return (float(value) - mean) / math.sqrt(variance)


def percentile(value: float | None, peers: Iterable[float | None]) -> float | None:
    """Percentile RANK: 100 x (below + 0.5 x equal) / N.

    This is what a non-analyst means by "we are in the 80th percentile" -- 80%
    of communes are below us -- and it is reproducible by hand from the
    published data, which matters when a commune disputes its rank.

    Deliberately NOT an interpolated quantile (numpy's default): the two differ
    by several points across 565 communes, and the interpolated version cannot
    be explained in one sentence in a meeting.

    The `0.5 x equal` term makes tied communes share a rank. A definition that
    gave two identical values different ranks would be indefensible.

    `peers` must include the subject itself -- it is one of the communes being
    ranked, so it belongs in N.
    """
    if value is None:
        return None
    values = _clean_peers(peers)
    if not values:
        return None
    subject = float(value)
    below = sum(1 for v in values if v < subject)
    equal = sum(1 for v in values if v == subject)
    return 100.0 * (below + 0.5 * equal) / len(values)
