"""
The derived-indicator engine -- Block G, docs/features/derived_indicators.md.

Turns the declarative configs in config/indicators/derived/*.yaml into values,
by topologically sorting them and calling the pure functions in derived.py.

Two things here carry most of the risk, and both are enforced rather than
documented-and-hoped-for:

1. CYCLE DETECTION. Derived-of-derived is expected (a per-capita figure
   feeding a growth rate). Without an explicit topological sort, results
   depend on evaluation order and produce nulls that move when unrelated
   config changes -- "maddening to debug", as the roadmap puts it. A cycle
   raises and names the whole cycle.

2. THE PEER SET IS PER PERIOD. Belgian communes merged in 2019 and 2025, and
   the observation store correctly holds historical predecessor communes for
   pre-merger years, so 589 communes existed in 2016-2018, 581 in 2019-2024
   and 565 in 2025-2026. ObservationSet.peers() returns only the geographies
   that actually have a value in the requested period, which is what makes a
   2016 percentile rank against 589 rather than today's 565. Ranking against
   the wrong denominator is exactly the failure the roadmap flags as "a
   commune arguing about its rank will notice".

CONTROL G: nothing here writes to `observations`. Derived values are computed
on the way to an export and never persisted as source data, so a formula fix
propagates instead of leaving two contradicting truths in the database.
"""

from collections.abc import Iterable, Mapping
from typing import Any

from src.analytics import derived

# The catalogue, resolved from the module itself so config and code cannot
# drift: a function named in a config but absent here fails validation.
FUNCTIONS = {
    name: getattr(derived, name)
    for name in (
        "growth_rate",
        "cagr",
        "five_year_change",
        "per_capita",
        "share_of_total",
        "index_base_100",
        "dependency_ratio",
        "z_score",
        "percentile",
        "regional_share",
        "mean_from_total",
    )
}

# Functions that rank or score a geography against its peers, and therefore
# need the whole cross-section for a period rather than one series.
CROSS_SECTIONAL = {"percentile", "z_score"}

# Functions taking several distinct indicators as positional inputs.
MULTI_INPUT = {
    "per_capita",
    "share_of_total",
    "regional_share",
    "dependency_ratio",
    "mean_from_total",
}


class CircularDependencyError(Exception):
    """Derived indicators reference each other in a loop."""


class UnknownFunctionError(Exception):
    """A config names a function that does not exist in the catalogue."""


class UnknownInputError(Exception):
    """A config names an input indicator that nothing provides."""


class ObservationSet:
    """A read-only view over observations from any store.

    Deliberately store-agnostic: population lives in a committed CSV and
    everything else in SQLite (ADR 0002), and the engine must not care.
    Callers assemble the rows; this only indexes them.
    """

    def __init__(self, rows: Iterable[tuple[str, str, str, float | None]]):
        """rows: (indicator_id, geo_id, period, value)."""
        self._by_cell: dict[tuple[str, str, str], float | None] = {}
        self._periods: dict[str, set[str]] = {}
        for indicator_id, geo_id, period, value in rows:
            self._by_cell[(indicator_id, geo_id, period)] = value
            self._periods.setdefault(indicator_id, set()).add(period)

    def value(self, indicator_id: str, geo_id: str, period: str) -> float | None:
        return self._by_cell.get((indicator_id, geo_id, period))

    def series(self, indicator_id: str, geo_id: str) -> dict[str, float | None]:
        """Every period this geography has for this indicator."""
        return {
            period: self._by_cell[(indicator_id, geo_id, period)]
            for period in self._periods.get(indicator_id, ())
            if (indicator_id, geo_id, period) in self._by_cell
        }

    def peers(self, indicator_id: str, period: str) -> dict[str, float | None]:
        """Every geography holding a value for this indicator in THIS period.

        This is the peer-set-by-period rule in one line: a geography that did
        not exist in the period simply has no cell, so it cannot enter the
        denominator.
        """
        return {
            geo_id: value
            for (ind, geo_id, per), value in self._by_cell.items()
            if ind == indicator_id and per == period
        }

    def cells(self, indicator_id: str) -> list[tuple[str, str]]:
        """(geo_id, period) pairs present for an indicator."""
        return [(g, p) for (i, g, p) in self._by_cell if i == indicator_id]

    def add(self, indicator_id: str, geo_id: str, period: str, value: float | None) -> None:
        self._by_cell[(indicator_id, geo_id, period)] = value
        self._periods.setdefault(indicator_id, set()).add(period)


def resolve_order(configs: Mapping[str, dict], available: Iterable[str] = ()) -> list[str]:
    """Topologically sort derived indicators so inputs are computed first.

    `available` names indicators that already exist (source indicators); an
    input that is neither available nor derived raises rather than silently
    producing nulls forever.

    Kahn's algorithm, with a deterministic tie-break so the order is stable
    across runs -- an unstable order would make export diffs noisy.
    """
    available = set(available)
    unknown = {
        (ind_id, dep)
        for ind_id, cfg in configs.items()
        for dep in cfg["derived"]["inputs"]
        if dep not in configs and dep not in available
    }
    if unknown:
        detail = ", ".join(f"{i} needs {d}" for i, d in sorted(unknown))
        raise UnknownInputError(
            f"Derived config references indicator(s) nothing provides: {detail}. "
            "Refusing to compute a column that would be null for every row."
        )

    pending = {
        ind_id: {d for d in cfg["derived"]["inputs"] if d in configs}
        for ind_id, cfg in configs.items()
    }
    order: list[str] = []
    while pending:
        ready = sorted(ind_id for ind_id, deps in pending.items() if not deps)
        if not ready:
            raise CircularDependencyError(
                "Derived indicators form a dependency cycle and cannot be ordered: "
                + _describe_cycle(pending)
            )
        for ind_id in ready:
            order.append(ind_id)
            del pending[ind_id]
        for deps in pending.values():
            deps.difference_update(ready)
    return order


def _describe_cycle(pending: Mapping[str, set[str]]) -> str:
    """Name one concrete cycle, so the error points at the config to fix
    rather than just asserting that a cycle exists."""
    start = sorted(pending)[0]
    seen: list[str] = []
    current = start
    while current not in seen:
        seen.append(current)
        remaining = sorted(pending.get(current, ()))
        if not remaining:
            break
        current = remaining[0]
    if current in seen:
        cycle = seen[seen.index(current) :] + [current]
        return " -> ".join(cycle)
    return " -> ".join(seen)


def compute(
    observations: ObservationSet,
    configs: Mapping[str, dict],
    available: Iterable[str] = (),
) -> ObservationSet:
    """Evaluate every derived indicator, in dependency order, into a copy of
    the observation set. Never mutates the caller's data and never writes to
    a database (CONTROL G)."""
    result = ObservationSet(
        (i, g, p, v) for (i, g, p), v in observations._by_cell.items()  # noqa: SLF001
    )
    for indicator_id in resolve_order(configs, available):
        spec = configs[indicator_id]["derived"]
        function_name = spec["function"]
        if function_name not in FUNCTIONS:
            raise UnknownFunctionError(
                f"{indicator_id}: no function named {function_name!r} in the catalogue. "
                f"Known: {', '.join(sorted(FUNCTIONS))}."
            )
        func = FUNCTIONS[function_name]
        inputs = spec["inputs"]
        args = dict(spec.get("args") or {})

        for geo_id, period in sorted(set(result.cells(inputs[0]))):
            value = _apply(func, function_name, result, inputs, geo_id, period, args)
            result.add(indicator_id, geo_id, period, value)
    return result


def _apply(
    func,
    function_name: str,
    observations: ObservationSet,
    inputs: list[str],
    geo_id: str,
    period: str,
    args: dict[str, Any],
) -> float | None:
    """Route one cell to the right calling convention. Kept explicit rather
    than clever: three genuinely different shapes (series, cross-section,
    several scalars) are easier to audit as a branch than as a generic
    dispatch."""
    if function_name in CROSS_SECTIONAL:
        subject = observations.value(inputs[0], geo_id, period)
        peers = observations.peers(inputs[0], period).values()
        return func(subject, peers)

    if function_name in MULTI_INPUT:
        values = [observations.value(name, geo_id, period) for name in inputs]
        return func(*values)

    # Time-series shape: a series plus the period being evaluated.
    series = observations.series(inputs[0], geo_id)
    return func(series, period, **args)
