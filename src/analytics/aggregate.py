"""Roll commune figures up to arrondissement, province, region and Belgium --
Block L, docs/features/comparison.md.

Three rules carry all the risk here, and each one was measured on the real
data before being written down. All three are enforced, not documented-and-
hoped-for.

1. A RATIO IS NEVER AVERAGED ACROSS GEOGRAPHIES. Additive indicators are
   summed; a ratio is RECOMPUTED from those sums by the same function that
   produced it at commune level. Measured on the real store, Belgium 2023,
   over all 581 communes of that period: the unweighted mean of commune
   averages puts average net taxable income at EUR 41,613.37, and the
   population-weighted mean at EUR 40,108.32, against a correct EUR
   40,125.70. Population-weighting is wrong
   too, just less visibly -- population is not the denominator of that ratio,
   tax returns are. So it is not implemented, anywhere, deliberately.

2. AGGREGATE OVER THE GEOGRAPHIES THAT EXISTED IN THAT PERIOD, never today's.
   13 of today's 565 communes have no 2023 fiscal row, because the Statbel
   file predates the 2025 merger wave -- and they include Hasselt. Summing
   what the current-communes export shows gives Limburg EUR 16.076bn against
   a correct EUR 21.278bn: 24.4% short, EUR 5.2bn missing, and entirely
   plausible on screen. The caller therefore passes the period's own
   municipal universe, and predecessor communes carry their own periods.

3. COVERAGE IS REPORTED, AND GATES PUBLICATION. Every aggregate states how
   many geographies contributed out of how many existed, and is suppressed
   below the threshold. A total 24% short is not a number with a footnote.
   This matters because the error does not announce itself: measured, a
   missing-coverage TOTAL is off by -24.4% while the RATIO built from it is
   off only -1.10%, since numerator and denominator lose the same communes.

Pure by construction: nothing here opens a file or touches the database. The
caller assembles observations and the period universe, exactly as it already
does for ObservationSet.
"""

from collections.abc import Callable, Iterable, Mapping

from src.analytics.engine import ObservationSet, compute

# Aggregation method per indicator, resolved from indicator metadata.
SUM = "sum"
RECOMPUTE = "recompute"
REFUSE = "refuse"

DEFAULT_MIN_COVERAGE = 0.90

# Levels a commune figure rolls up into, in ascending order. Not every commune
# has every level -- Brussels communes have no province, their arrondissement
# parents straight to the region (docs/features/geography.md Q3) -- so the walk
# collects whatever levels actually exist rather than assuming a fixed depth.
AGGREGATE_LEVELS = ("arrondissement", "province", "region", "country")


class NotAggregatableError(Exception):
    """An indicator has no defensible aggregate and one was requested.

    Raised rather than returning a number. An index or a share cannot be
    summed and cannot be recomputed from components that do not exist; a
    province-level "GDP index" built by averaging commune indices would be
    arithmetically valid and analytically meaningless.
    """


class Coverage:
    """How much of a geography actually contributed to its aggregate."""

    __slots__ = ("contributed", "expected")

    def __init__(self, contributed: int, expected: int):
        self.contributed = contributed
        self.expected = expected

    @property
    def pct(self) -> float:
        # An expected count of zero means the geography had no communes in
        # that period at all, which is not 100% coverage of anything.
        return 0.0 if self.expected == 0 else self.contributed / self.expected * 100.0

    def is_sufficient(self, min_coverage: float = DEFAULT_MIN_COVERAGE) -> bool:
        return self.expected > 0 and (self.contributed / self.expected) >= min_coverage

    def as_dict(self) -> dict:
        return {"n": self.contributed, "of": self.expected, "pct": round(self.pct, 1)}

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Coverage({self.contributed}/{self.expected})"


class AggregateSet:
    """Aggregate values and their coverage, keyed like an ObservationSet."""

    def __init__(self) -> None:
        self._values: dict[tuple[str, str, str], float] = {}
        self._coverage: dict[tuple[str, str, str], Coverage] = {}

    def set(self, indicator_id: str, geo_id: str, period: str, value: float, cov: Coverage) -> None:
        self._values[(indicator_id, geo_id, period)] = value
        self._coverage[(indicator_id, geo_id, period)] = cov

    def value(self, indicator_id: str, geo_id: str, period: str) -> float | None:
        return self._values.get((indicator_id, geo_id, period))

    def coverage(self, indicator_id: str, geo_id: str, period: str) -> Coverage | None:
        return self._coverage.get((indicator_id, geo_id, period))

    def cells(self) -> list[tuple[str, str, str]]:
        return sorted(self._values)

    def rows(self) -> Iterable[tuple[str, str, str, float]]:
        """(indicator_id, geo_id, period, value) -- the ObservationSet shape."""
        for key in sorted(self._values):
            yield (*key, self._values[key])

    def __len__(self) -> int:
        return len(self._values)


def ancestors_of(
    geo_id: str, parents: Mapping[str, str | None], levels: Mapping[str, str]
) -> list[str]:
    """Every aggregate-level geography above a commune, nearest first.

    Tolerates a missing level rather than assuming four hops, and stops on a
    cycle instead of looping forever -- the same defence
    `_ancestor_names()` already applies in the exporters.
    """
    out: list[str] = []
    seen = {geo_id}
    current = parents.get(geo_id)
    while current and current not in seen:
        seen.add(current)
        if levels.get(current) in AGGREGATE_LEVELS:
            out.append(current)
        current = parents.get(current)
    return out


def per_period_universe(
    mapping: Mapping[str, set[str]],
) -> Callable[[str, str], set[str]]:
    """Adapt a plain period -> communes mapping to the universe callable.

    For the common case where every indicator is expressed in the calendar's
    own geography for each period.
    """
    return lambda _indicator_id, period: mapping.get(period, set())


def aggregate_additive(
    obs: ObservationSet,
    indicator_ids: Iterable[str],
    parents: Mapping[str, str | None],
    levels: Mapping[str, str],
    universe_of: Callable[[str, str], set[str]],
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> AggregateSet:
    """Sum additive commune figures into every ancestor geography.

    `universe_of(indicator_id, period)` returns the municipal geo_ids that
    indicator covers in that period. It is the coverage denominator, and it
    must not be "the communes that have a value" -- that would make coverage
    100% by construction and hide exactly the gap it exists to expose.

    WHY IT IS PER INDICATOR and not simply per period. Measured: coverage came
    out above 100% -- more communes contributing than existed -- for 283 cells.
    Not an arithmetic bug. Statbel's fiscal file expresses EVERY year from 2005
    to 2023 on the 2019 commune map (Block F documented this: it back-casts a
    fixed vintage), and LOCAL_UNITS_BY_COMMUNE reports its 2023-Q4 snapshot on
    today's 565. So for fiscal 2005 the contributors include 18 communes that
    did not exist until 2019, while the calendar universe for 2005 holds the
    589 that did. Dividing one map by the other is meaningless in both
    directions, and it happened to surface as an impossible number only
    because the mismatch ran that way.

    An indicator's coverage therefore has to be measured against the geography
    THAT INDICATOR is expressed in. The caller decides; see
    scripts/export_aggregates_csv.py, which detects a pinned vintage from the
    data rather than from a hand-maintained list.
    """
    result = AggregateSet()

    for indicator_id in indicator_ids:
        # (geo_id, period) -> [running total, contributing communes]
        totals: dict[tuple[str, str], list] = {}

        for commune_id, period in obs.cells(indicator_id):
            value = obs.value(indicator_id, commune_id, period)
            if value is None:
                continue
            for ancestor in ancestors_of(commune_id, parents, levels):
                entry = totals.setdefault((ancestor, period), [0.0, 0])
                entry[0] += value
                entry[1] += 1

        for (geo_id, period), (total, contributed) in totals.items():
            expected = _expected_children(
                geo_id, universe_of(indicator_id, period), parents, levels
            )
            cov = Coverage(contributed, expected)
            # Suppress rather than footnote. See rule 3.
            if not cov.is_sufficient(min_coverage):
                continue
            result.set(indicator_id, geo_id, period, total, cov)

    return result


def _expected_children(
    geo_id: str,
    universe: set[str],
    parents: Mapping[str, str | None],
    levels: Mapping[str, str],
) -> int:
    """How many of the indicator's communes sit under this geography."""
    return sum(1 for commune_id in universe if geo_id in ancestors_of(commune_id, parents, levels))


def aggregate(
    obs: ObservationSet,
    methods: Mapping[str, str],
    parents: Mapping[str, str | None],
    levels: Mapping[str, str],
    universe_of: Callable[[str, str], set[str]],
    derived_configs: Mapping[str, dict] | None = None,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> AggregateSet:
    """Aggregate every requested indicator to every level above the commune.

    `methods` maps indicator_id -> SUM | RECOMPUTE | REFUSE.

    Ratios are handled by summing their additive inputs and then running the
    EXISTING derived engine over the aggregate values -- there is deliberately
    no second derivation path here, so a formula fix propagates to commune and
    province figures alike (CONTROL G's reasoning, applied one level up).
    """
    refused = sorted(i for i, m in methods.items() if m == REFUSE)
    if refused:
        raise NotAggregatableError(
            f"no defensible aggregate for {refused}: an index or share can be neither "
            "summed nor recomputed from components. Fix the request, not the arithmetic."
        )

    additive = [i for i, m in methods.items() if m == SUM]
    result = aggregate_additive(obs, additive, parents, levels, universe_of, min_coverage)

    recompute = {i for i, m in methods.items() if m == RECOMPUTE}
    if not recompute:
        return result

    configs = {i: c for i, c in (derived_configs or {}).items() if i in recompute}
    missing = sorted(recompute - set(configs))
    if missing:
        raise NotAggregatableError(
            f"{missing} are marked for recomputation but have no derived config, so there "
            "is no formula to rebuild them from the summed components."
        )

    # Feed the engine the AGGREGATE values, not the commune ones: that is what
    # makes average income at province level total-income-over-total-returns
    # rather than a mean of means.
    agg_obs = ObservationSet(result.rows())
    present = {indicator_id for indicator_id, _geo, _period in result.cells()}

    # An input can be legitimately ABSENT here, unlike at commune level: the
    # coverage gate above suppresses a poorly-covered sum entirely. The engine
    # rightly raises on an input nothing provides -- that means a broken config
    # -- so a ratio whose inputs did not survive the gate is dropped before
    # the engine sees it. The ratio genuinely does not exist; that is the
    # correct outcome, not an error.
    buildable = {
        indicator_id: config
        for indicator_id, config in configs.items()
        if all(dep in present for dep in (config.get("derived") or {}).get("inputs") or [])
    }
    if not buildable:
        return result

    computed = compute(agg_obs, buildable, present)

    for indicator_id, config in buildable.items():
        for geo_id, period in sorted(computed.cells(indicator_id)):
            value = computed.value(indicator_id, geo_id, period)
            if value is None:
                continue
            # A recomputed ratio is only as covered as its weakest input: if
            # the numerator was suppressed the ratio cannot exist at all, and
            # if both survived it inherits the tighter of the two.
            cov = _weakest_input_coverage(result, config, geo_id, period)
            if cov is None:
                continue
            result.set(indicator_id, geo_id, period, value, cov)

    return result


def _weakest_input_coverage(
    result: AggregateSet, config: Mapping, geo_id: str, period: str
) -> Coverage | None:
    inputs = (config.get("derived") or {}).get("inputs") or []
    covs = [result.coverage(i, geo_id, period) for i in inputs]
    if not covs or any(c is None for c in covs):
        return None
    return min(covs, key=lambda c: c.pct)


def methods_from_metadata(
    indicator_meta: Mapping[str, Mapping],
    derived_configs: Mapping[str, Mapping] | None = None,
) -> dict[str, str]:
    """Decide each indicator's aggregation method from its own metadata.

    Config-driven rather than a hardcoded list, so a new municipal indicator
    aggregates correctly the day it lands:

    - additive (a count or a total) -> SUM
    - a derived indicator whose function can be re-run on summed inputs
      -> RECOMPUTE
    - anything else (an index, a share, a cross-sectional rank) -> REFUSE
    """
    # Cross-sectional functions rank a geography against peers; a province
    # ranked against communes is a category error, so they are refused rather
    # than recomputed. Period-relative functions are computed from the
    # aggregate's OWN history by the normal derived engine afterwards, not
    # here, so they are refused at this stage too.
    RECOMPUTABLE_FUNCTIONS = {"mean_from_total", "dependency_ratio", "per_capita"}

    methods: dict[str, str] = {}

    for indicator_id, meta in indicator_meta.items():
        methods[indicator_id] = SUM if meta.get("is_additive") else REFUSE

    for indicator_id, config in (derived_configs or {}).items():
        function = (config.get("derived") or {}).get("function")
        methods[indicator_id] = RECOMPUTE if function in RECOMPUTABLE_FUNCTIONS else REFUSE

    return methods
