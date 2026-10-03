"""Pure computation for the simulated public-finance "live counters" --
population, debt, deficit, revenue and spending (each with named parts),
ticking once a second in the browser from parameters this module computes.
docs/features/public_finance_live.md is the spec; docs/decisions/
0016-simulated-live-counters.md is why this is allowed to exist at all.

NO I/O, NO WALL CLOCK. Every function here is deterministic given its
arguments. scripts/export_live_counters.py is the only caller that reads
files or a clock -- and even there, `updated` in the payload is the max of
the INPUT payloads' own `updated` dates, never `datetime.now()` (CLAUDE.md
rule 35: identical inputs, identical output).

Two kinds of problem, two different responses (CLAUDE.md rule 13):

  * A CONFIG/SCHEMA problem -- an unknown unit, a malformed period string,
    segments whose boundaries do not line up -- is certainly wrong and
    raises `LiveCounterError`. The build fails loudly.

  * A DATA condition -- a missing trend year, a remainder too negative to
    clamp, population coverage short of 100%, a debt anchor beyond the
    simulation horizon -- is not a bug, just something this run cannot
    show. It comes back as `Unavailable(reason=...)`, never raised, so one
    broken counter or breakdown cannot block the whole daily site update.

Units: every money amount this module hands back is in EUR (not "meur"):
`scale_for()` is the one place a config unit becomes a EUR multiplier, and
it is a closed table -- an unlisted unit is a config error, never a
hand-typed scale (rules 36/41). Timestamps are milliseconds since the Unix
epoch, always at a FIXED +01:00 (CET) offset, never a DST-aware zoneinfo:
1 January is always CET in Belgium, so every YEAR boundary this module
computes is exact. A quarter boundary that falls in Belgian summer time
(1 April, 1 July, 1 October) is therefore up to one hour off true Brussels
local time -- accepted and documented in the ADR, because this is a
labelled simulation, not a legal clock.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, localcontext

CET = timezone(timedelta(hours=1))

#: unit -> EUR per unit. CLOSED: an id not in this table is a config error
#: (rule 41 -- a level is never a count; rule 36 -- never a hand-typed scale).
SCALE_EUR: Mapping[str, Decimal] = {
    "meur": Decimal(1_000_000),
    "count": Decimal(1),
}

#: trend_growth()'s own rounding -- the published `growth_rate` AND every
#: later extrapolation both use this rounded value, never the unrounded
#: ratio, so a projected value a reader can recompute from the published
#: growth rate matches this module's own number exactly.
GROWTH_DP = Decimal("0.0000000001")  # 10 dp
CENT = Decimal("0.01")


class LiveCounterError(ValueError):
    """A config or schema problem. Fails the build loudly (rule 13)."""


@dataclass(frozen=True)
class Unavailable:
    """A DATA condition. Never raised -- carried as an ordinary value so one
    broken counter cannot stop every other one from exporting."""

    reason: str


@dataclass(frozen=True)
class Segment:
    """One straight-line piece of a counter. `v0`/`v1` are EUR (or persons),
    already rounded the way the caller wants; `rate_per_ms` is derived from
    THOSE rounded endpoints, never the other way around, so a stock's `v1`
    at one segment is always exactly the next segment's `v0`."""

    start_ms: int
    end_ms: int
    v0: Decimal
    v1: Decimal
    rate_per_ms: Decimal

    def as_dict(self) -> dict:
        return {
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "v0": float(self.v0),
            "v1": float(self.v1),
            "rate_per_ms": float(self.rate_per_ms),
        }


def _ms(dt: datetime) -> int:
    return round(dt.timestamp() * 1000)


def _money(x: Decimal) -> Decimal:
    return x.quantize(CENT, rounding=ROUND_HALF_UP)


def _segment(start_ms: int, end_ms: int, v0: Decimal, v1: Decimal) -> Segment:
    if end_ms <= start_ms:
        raise LiveCounterError(f"segment end_ms {end_ms} is not after start_ms {start_ms}")
    with localcontext() as ctx:
        ctx.prec = 50
        rate = (v1 - v0) / Decimal(end_ms - start_ms)
    return Segment(start_ms, end_ms, v0, v1, rate)


def scale_for(unit: str) -> Decimal:
    """EUR per unit of `unit`. Raises LiveCounterError for anything not in
    the closed SCALE_EUR table -- a config problem, not a data one."""
    try:
        return SCALE_EUR[unit]
    except KeyError:
        raise LiveCounterError(
            f"no EUR scale declared for unit {unit!r} (known: {sorted(SCALE_EUR)})"
        ) from None


def year_bounds(year: int) -> tuple[int, int]:
    """(start_ms, end_ms) of `year`, 1 January 00:00 at a fixed +01:00 --
    exact, because 1 January is always CET in Belgium."""
    start = datetime(year, 1, 1, tzinfo=CET)
    end = datetime(year + 1, 1, 1, tzinfo=CET)
    return _ms(start), _ms(end)


def period_end_ms(period: str) -> int:
    """Start of the period immediately AFTER `period`, at +01:00 -- the
    instant an official value declared for `period` is "as of". Accepts
    "YYYY" and "YYYY-Qn". Anything else is a config/schema problem."""
    if len(period) == 4 and period.isdigit():
        return year_bounds(int(period) + 1)[0]
    if (
        len(period) == 7
        and period[:4].isdigit()
        and period[4] == "-"
        and period[5] == "Q"
        and period[6] in "1234"
    ):
        year, q = int(period[:4]), int(period[6])
        if q == 4:
            return year_bounds(year + 1)[0]
        return _ms(datetime(year, q * 3 + 1, 1, tzinfo=CET))
    raise LiveCounterError(f"period {period!r} is neither 'YYYY' nor 'YYYY-Qn'")


def trend_growth(
    series: Mapping[int, float | None], latest_year: int, window: int = 3
) -> Decimal | Unavailable:
    """(v_L / v_(L-window)) ** (1/window) - 1, rounded to 10 dp. A data
    condition (either year missing, None, or <= 0) comes back as
    Unavailable, never raised -- a trend series is allowed to be short."""
    base_year = latest_year - window
    v_latest = series.get(latest_year)
    v_base = series.get(base_year)
    if v_latest is None:
        return Unavailable(f"missing_year:{latest_year}")
    if v_base is None:
        return Unavailable(f"missing_year:{base_year}")
    if v_latest <= 0 or v_base <= 0:
        return Unavailable("non_positive_value")
    with localcontext() as ctx:
        ctx.prec = 50
        ratio = Decimal(str(v_latest)) / Decimal(str(v_base))
        growth = ratio ** (Decimal(1) / Decimal(window)) - 1
    return growth.quantize(GROWTH_DP, rounding=ROUND_HALF_UP)


def annual_value(
    series: Mapping[int, float | None],
    year: int,
    latest_year: int,
    growth: Decimal | Unavailable,
) -> Decimal | Unavailable:
    """The series' own value for `year` if it is official (year <=
    latest_year), else v_L * (1 + growth) ** (year - latest_year) -- in the
    series' NATIVE unit (not yet scaled to EUR). `growth` is always the
    ROUNDED trend_growth() result, so a projected value is exactly
    reproducible from the published growth rate."""
    if year <= latest_year:
        v = series.get(year)
        if v is None:
            return Unavailable(f"missing_year:{year}")
        return Decimal(str(v))
    if isinstance(growth, Unavailable):
        return growth
    v_latest = series.get(latest_year)
    if v_latest is None:
        return Unavailable(f"missing_year:{latest_year}")
    with localcontext() as ctx:
        ctx.prec = 50
        return Decimal(str(v_latest)) * (Decimal(1) + growth) ** (year - latest_year)


def flow_segments(
    annual_eur_by_year: Mapping[int, Decimal | Unavailable],
    start_year: int,
    end_year: int,
    *,
    round_cents: bool = True,
) -> list[Segment] | Unavailable:
    """One segment per year in start_year..end_year inclusive, resetting to
    v0=0 at the start of every year (CLAUDE.md: a flow is not a stock)."""
    segments: list[Segment] = []
    for year in range(start_year, end_year + 1):
        v1 = annual_eur_by_year.get(year)
        if v1 is None:
            return Unavailable(f"missing_year:{year}")
        if isinstance(v1, Unavailable):
            return v1
        if round_cents:
            v1 = _money(v1)
        start_ms, end_ms = year_bounds(year)
        segments.append(_segment(start_ms, end_ms, Decimal(0), v1))
    return segments


def difference_segments(a: Sequence[Segment], b: Sequence[Segment]) -> list[Segment]:
    """b - a, segment by segment. Both sequences must share EXACTLY the same
    boundaries (e.g. spending minus revenue, both built by flow_segments()
    over the same year range) -- a mismatch is a config/schema problem, not
    a data one, so it raises."""
    if len(a) != len(b):
        raise LiveCounterError(f"difference_segments: {len(a)} vs {len(b)} segments")
    out = []
    for sa, sb in zip(a, b, strict=True):
        if sa.start_ms != sb.start_ms or sa.end_ms != sb.end_ms:
            raise LiveCounterError(
                f"difference_segments: boundary mismatch {sa.start_ms, sa.end_ms} vs "
                f"{sb.start_ms, sb.end_ms}"
            )
        out.append(_segment(sa.start_ms, sa.end_ms, sb.v0 - sa.v0, sb.v1 - sa.v1))
    return out


def stock_segments(
    anchor_ms: int,
    anchor_value: Decimal,
    year_pace_eur: Mapping[int, Decimal | Unavailable],
    horizon_end_ms: int,
    *,
    round_cents: bool = True,
) -> list[Segment] | Unavailable:
    """Continuous segments for a stock (e.g. debt) from `anchor_ms` (with
    `anchor_value` there) through `horizon_end_ms`, one segment per calendar
    year (the first one partial, from the anchor to that year's end), each
    year's rate taken from `year_pace_eur[year]` -- EUR added over that
    FULL year (e.g. that year's deficit). A stock never resets at a year
    boundary: each segment's v1 is exactly the next one's v0."""
    if anchor_ms >= horizon_end_ms:
        return Unavailable("anchor_beyond_horizon")
    anchor_year = datetime.fromtimestamp(anchor_ms / 1000, tz=CET).year
    segments: list[Segment] = []
    value = _money(anchor_value) if round_cents else anchor_value
    cursor_ms = anchor_ms
    year = anchor_year
    while cursor_ms < horizon_end_ms:
        pace = year_pace_eur.get(year)
        if pace is None:
            return Unavailable(f"missing_year:{year}")
        if isinstance(pace, Unavailable):
            return pace
        year_start_ms, year_end_ms = year_bounds(year)
        segment_end_ms = min(year_end_ms, horizon_end_ms)
        with localcontext() as ctx:
            ctx.prec = 50
            fraction = Decimal(segment_end_ms - cursor_ms) / Decimal(year_end_ms - year_start_ms)
            next_value = value + pace * fraction
        if round_cents:
            next_value = _money(next_value)
        segments.append(_segment(cursor_ms, segment_end_ms, value, next_value))
        value = next_value
        cursor_ms = segment_end_ms
        year += 1
    return segments


def population_segments(
    pop_latest: float,
    pop_previous: float,
    latest_year: int,
    horizon_end_ms: int,
    *,
    coverage_latest_pct: float,
    coverage_previous_pct: float,
) -> list[Segment] | Unavailable:
    """One continuous segment per year from 1 January of `latest_year`
    through `horizon_end_ms`, growing by (pop_latest - pop_previous) EVERY
    year -- unrounded (persons, not money). Refuses (Unavailable) unless
    BOTH years have full (100%) coverage (rule: an undercounted base year
    must never anchor a counter)."""
    if coverage_latest_pct != 100.0 or coverage_previous_pct != 100.0:
        return Unavailable(f"coverage_below_100:{coverage_latest_pct}/{coverage_previous_pct}")
    delta = Decimal(str(pop_latest)) - Decimal(str(pop_previous))
    anchor_ms, _ = year_bounds(latest_year)
    year_pace = _ConstantPace(delta)
    return stock_segments(
        anchor_ms, Decimal(str(pop_latest)), year_pace, horizon_end_ms, round_cents=False
    )


class _ConstantPace(Mapping[int, Decimal]):
    """A Mapping that returns the same pace for every year -- population's
    growth delta does not change by year the way a deficit pace does."""

    def __init__(self, value: Decimal) -> None:
        self._value = value

    def __getitem__(self, key: int) -> Decimal:
        return self._value

    def get(self, key: int, default=None) -> Decimal:  # noqa: ARG002
        return self._value

    def __iter__(self):  # pragma: no cover - Mapping requires it, never used
        return iter(())

    def __len__(self) -> int:  # pragma: no cover - Mapping requires it
        return 0


def compute_breakdown(
    whole: Decimal,
    named: Mapping[str, Decimal],
    remainder_id: str,
    tolerance: Decimal,
) -> dict[str, Decimal] | Unavailable:
    """`named` parts plus one remainder part (`remainder_id`) that makes
    every part sum EXACTLY to `whole`. The remainder is `whole -
    sum(named)`:

      * remainder < -tolerance  -> Unavailable (the named parts overshoot
        the whole by more than can plausibly be rounding -- a real data
        problem, not simulated).
      * -tolerance <= remainder < 0 -> clamped to 0, then every part
        (named AND the remainder) is rescaled by whole / sum(all parts
        with the clamp applied) so they sum exactly to `whole` again, with
        no part ever negative.
      * remainder >= 0 -> returned as-is.
    """
    total_named = sum(named.values()) if named else Decimal(0)
    remainder = whole - total_named
    if remainder < -tolerance:
        return Unavailable(f"remainder_below_tolerance:{remainder}")
    if remainder >= 0:
        return {**named, remainder_id: remainder}
    # -tolerance <= remainder < 0: clamp then renormalise everything.
    clamped = {**named, remainder_id: Decimal(0)}
    total_clamped = sum(clamped.values())
    if total_clamped <= 0:
        return Unavailable("degenerate_zero_total")
    with localcontext() as ctx:
        ctx.prec = 50
        factor = whole / total_clamped
    rescaled = {k: v * factor for k, v in clamped.items() if k != remainder_id}
    rescaled[remainder_id] = whole - sum(rescaled.values())
    return rescaled


def shares_from_breakdown(parts: Mapping[str, Decimal], whole: Decimal) -> dict[str, Decimal]:
    """Each part's value divided by `whole` -- the fixed shares later
    reused against every projected year's own total."""
    if whole == 0:
        raise LiveCounterError("shares_from_breakdown: whole is zero")
    with localcontext() as ctx:
        ctx.prec = 50
        return {k: v / whole for k, v in parts.items()}


def apply_shares(shares: Mapping[str, Decimal], whole: Decimal, last_id: str) -> dict[str, Decimal]:
    """share * whole for every part except `last_id`, which instead takes
    whole - sum(the others) -- so the parts sum EXACTLY to `whole` for
    every year this is called with, not just the breakdown year."""
    with localcontext() as ctx:
        ctx.prec = 50
        others = {k: v * whole for k, v in shares.items() if k != last_id}
        last = whole - sum(others.values())
    return {**others, last_id: last}
