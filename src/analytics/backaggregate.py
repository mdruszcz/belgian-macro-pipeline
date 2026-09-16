"""Reconstruct a merged-away commune's pre-merger history onto its successor
-- Block (merger back-aggregation), docs/decisions/0011 (lead-authored).

WHY THIS EXISTS. Every historical source still publishes on the pre-merger
map. The map today already IS the successor -- Borsbeek no longer exists, so
its history has to attach to Antwerp somewhere -- but every historical source
still publishes on the OLD map, so on their own the 13 communes created by the
2024-12/2025 merger wave show a fraction of the indicators an ordinary commune
does. This module reconstructs the missing cells, in the query layer only.

MAINTAINER DECISION: GAP-FILL, NEVER RESTATEMENT. A cell is reconstructed only
where the successor has NO observation of its own for that (indicator,
period). A published figure never changes -- Antwerp's own 2023 fiscal total
is never touched by Borsbeek's, even though Borsbeek is now part of Antwerp's
territory. See the module's test suite for the exact numbers this was checked
against.

THREE RULES CARRY THE RISK HERE, same shape as aggregate.py's, and the same
discipline: enforced, not documented-and-hoped-for.

1. GAP-FILL ONLY. `reconstruct()` never looks at what the successor already
   publishes for a cell it is about to fill in the OTHER direction -- the
   caller is responsible for only keeping cells the successor lacks (see
   `only_missing_cells`). This module answers "what WOULD the reconstructed
   value be", the caller decides "is there already a real one here".

2. ALL PREDECESSORS OR NOTHING. A reconstruction for a (indicator, period) is
   valid only when EVERY recorded predecessor has a value for it. A partial
   sum is refused, not footnoted -- there is no coverage concept here the way
   aggregate.py has one, because a merger successor either fully replaces its
   predecessors' territory or it does not exist as a reconstruction at all.
   Missing, suppressed, na, absent and an explicit zero are five different
   things and must never collapse (CLAUDE.md rule 26): only a real recorded
   value counts, so `status in {"suppressed", "na"}` or "no row at all" both
   block the whole cell, identically.

3. NEVER FEED THE ENGINE'S PEER SET. Reconstructed rows must never be added to
   the ObservationSet passed to `compute()` for percentiles, growth rates or
   any other cross-sectional or period-relative figure: the predecessors are
   already in that set on their own historical geo_id, and adding the
   successor alongside them double-counts the same territory. This module
   does not touch ObservationSet or compute() at all -- see engine.py and
   aggregate.py for that pass. Ratios are recomputed here in a SECOND,
   ISOLATED pass, over reconstructed components only, using the exact same
   functions the derived engine already uses (src/analytics/derived.py) so a
   formula fix propagates to both a live and a reconstructed cell alike.

LINEAGE IS DRIVEN BY successor_geo_id, NEVER BY A WAVE DATE. Bastogne's two
crosswalk rows carry valid_to = 2024-12-02, not 2025-01-01 -- a date filter
would silently drop it. `lineage_from_geographies` takes every row with
valid_to IS NOT NULL AND successor_geo_id IS NOT NULL, full stop, and walks
the chain recursively with a cycle guard (mirroring resolve_to_current in
src/geography/resolve.py) so a two-hop merger resolves too, even though no
chain in today's data is deeper than one hop.

has_partial_transfer is IGNORED here on purpose. It annotates the 1977
merger and, per src/geography/crosswalk.py, fires on Bastogne and Bertogne by
a coincidence of code overlap -- it carries no signal the "does this successor
have more than one predecessor" check does not already carry, so reading it
would be an extra branch with no extra correctness.

Pure by construction: nothing here opens a file, touches sqlite, or touches
`observations`. The caller assembles rows exactly as it already does for
ObservationSet, and applies the result -- never writes it back to the store.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

# Same three-way vocabulary as aggregate.py, so a reader who already knows one
# module recognises the other immediately.
SUM = "sum"
RECOMPUTE = "recompute"
REFUSE = "refuse"

# Functions recomputable from summed components -- copied from
# methods_from_metadata in aggregate.py rather than imported, because that
# function decides AGGREGATION method (commune -> province) and this module
# decides RECONSTRUCTION method (predecessor -> successor); the two questions
# happen to share an answer set today but are conceptually distinct call
# sites, and importing one to mean the other would make a future divergence
# (a function recomputable across geography but not across time, say) an
# invisible coupling instead of a visible duplication.
RECOMPUTABLE_FUNCTIONS = {
    "mean_from_total",
    "dependency_ratio",
    "per_capita",
    "share_of_total",
}

# Row shape shared with export_communes_history_csv.py's `raw` list and
# export_communes_csv.py's SQL result:
# (geo_id, indicator_id, name_en, unit, period, value, status, created_at).
Row = tuple[str, str, str, str, str, float | None, str, str]


class CycleError(Exception):
    """A successor_geo_id chain loops back on itself."""


@dataclass(frozen=True)
class GeographyLink:
    geo_id: str
    valid_to: str | None
    successor_geo_id: str | None


def resolve_successor(geo_id: str, links: Mapping[str, GeographyLink]) -> str | None:
    """Walk successor_geo_id to the entity that exists today, or None if
    `geo_id` never ceased to exist (nothing to resolve).

    Recursive by construction -- a commune merged twice needs two hops -- with
    a cycle guard so a data error raises instead of looping forever. Mirrors
    resolve_to_current in src/geography/resolve.py, reimplemented here rather
    than imported so this module stays free of any DB or file access (that
    function takes a live sqlite3.Connection).
    """
    seen = {geo_id}
    current = geo_id
    while True:
        link = links.get(current)
        if link is None or link.successor_geo_id is None:
            return None if current == geo_id else current
        successor = link.successor_geo_id
        if successor in seen:
            raise CycleError(
                f"successor_geo_id chain starting at {geo_id!r} cycles back to "
                f"{successor!r}. Refusing to loop forever."
            )
        seen.add(successor)
        current = successor


def lineage_from_geographies(
    rows: Iterable[tuple[str, str | None, str | None]],
) -> dict[str, list[str]]:
    """current successor geo_id -> its predecessor geo_ids (recorded order).

    `rows` is every (geo_id, valid_to, successor_geo_id) from `geographies` --
    the caller filters to `valid_to IS NOT NULL AND successor_geo_id IS NOT
    NULL` (or passes everything; a live-and-unmerged row's successor_geo_id is
    already NULL and is dropped here regardless, since `resolve_successor`
    returns None for it).

    Grouped by the FINAL successor after walking every hop, not by the
    immediate successor_geo_id column -- so a two-hop lineage (A -> B -> C)
    lists A as one of C's predecessors, not one of B's, exactly matching what
    resolve_to_current would report for A.
    """
    links = {
        geo_id: GeographyLink(geo_id, valid_to, successor) for geo_id, valid_to, successor in rows
    }
    out: dict[str, list[str]] = {}
    for geo_id in links:
        final = resolve_successor(geo_id, links)
        if final is None:
            continue
        out.setdefault(final, []).append(geo_id)
    return out


def methods_from_metadata(
    indicator_is_additive: Mapping[str, bool],
    derived_configs: Mapping[str, Mapping] | None = None,
) -> dict[str, str]:
    """Decide each indicator's RECONSTRUCTION method from its own metadata.

    Identical policy to aggregate.py's methods_from_metadata -- gate on
    is_additive for raw indicators, and on the derived function name for
    everything else -- but deliberately not the same function object, so this
    module stays importable with zero dependency on aggregate.py or the
    engine. `indicators.aggregation_method` is NEVER read: the column holds
    only 'not_applicable' (80 rows) and 'sum' (35 rows) today -- the forbidden
    'population_weighted' label was retired in PR #206 -- and nothing under
    src/ reads that column. Even 'sum' is not read here: is_additive and each
    derived config's own function name are this module's source of truth,
    exactly as in aggregate.py.
    """
    methods: dict[str, str] = {}
    for indicator_id, is_additive in indicator_is_additive.items():
        methods[indicator_id] = SUM if is_additive else REFUSE
    for indicator_id, config in (derived_configs or {}).items():
        function = (config.get("derived") or {}).get("function")
        methods[indicator_id] = RECOMPUTE if function in RECOMPUTABLE_FUNCTIONS else REFUSE
    return methods


def _index_rows(rows: Iterable[Row]) -> dict[tuple[str, str, str], Row]:
    """(geo_id, indicator_id, period) -> row, for the predecessor rows this
    module reads. Later rows win on a duplicate key, matching how a Python
    dict comprehension over the same rows would already behave -- and no
    predecessor carries more than one is_latest=1 row per (indicator, period)
    in the real store, so this never actually chooses among duplicates."""
    return {(row[0], row[1], row[4]): row for row in rows}


def reconstruct_additive(
    rows: Iterable[Row],
    lineage: Mapping[str, list[str]],
    additive_indicator_ids: Iterable[str],
    indicator_meta: Mapping[str, tuple[str, str]],
) -> list[Row]:
    """Sum every additive indicator's predecessor cells onto each successor,
    for periods where EVERY predecessor has a real recorded value.

    `indicator_meta` maps indicator_id -> (name_en, unit), used to label the
    synthetic row exactly as a live one would be labelled (so it slots into
    the same CSV/JSON shape with no special-casing downstream).

    A cell counts only when its status carries a real value: `status in
    {"suppressed", "na"}` or "row absent entirely" both block the WHOLE
    successor cell for that period, not just that one predecessor's share --
    rule 2 in the module docstring. This is what makes a partial sum
    impossible: either every predecessor contributes, or the cell does not
    exist as a reconstruction.
    """
    additive_indicator_ids = set(additive_indicator_ids)
    by_cell = _index_rows(r for r in rows if r[1] in additive_indicator_ids)

    # geo_id -> indicator_id -> period -> (value, created_at), restricted to
    # cells that carry a real value (never suppressed/na/absent).
    by_geo: dict[str, dict[str, dict[str, tuple[float, str]]]] = {}
    for (geo_id, indicator_id, period), row in by_cell.items():
        value, status, created_at = row[5], row[6], row[7]
        if value is None or status in ("suppressed", "na"):
            continue
        by_geo.setdefault(geo_id, {}).setdefault(indicator_id, {})[period] = (value, created_at)

    out: list[Row] = []
    for successor, predecessors in lineage.items():
        for indicator_id in additive_indicator_ids:
            if indicator_id not in indicator_meta:
                continue
            # Every period ANY predecessor has a cell for, so a period missing
            # from one predecessor but present in another is correctly caught
            # by the "all predecessors" check below rather than silently
            # skipped.
            periods: set[str] = set()
            for pred in predecessors:
                periods |= set(by_geo.get(pred, {}).get(indicator_id, {}))

            for period in sorted(periods):
                values = []
                created_ats = []
                complete = True
                for pred in predecessors:
                    cell = by_geo.get(pred, {}).get(indicator_id, {}).get(period)
                    if cell is None:
                        complete = False
                        break
                    values.append(cell[0])
                    created_ats.append(cell[1])
                if not complete:
                    continue  # rule 2: all predecessors or nothing.

                name_en, unit = indicator_meta[indicator_id]
                out.append(
                    (
                        successor,
                        indicator_id,
                        name_en,
                        unit,
                        period,
                        sum(values),
                        "reconstructed",
                        max(created_ats) if created_ats else "",
                    )
                )
    return out


def territory_series(
    rows: Iterable[Row],
    lineage: Mapping[str, list[str]],
    indicator_id: str,
) -> dict[str, dict[str, tuple[float, bool]]]:
    """T(C, p) -- the TERRITORY-CONSISTENT series for one additive indicator,
    for every successor in `lineage`. Maintainer decision 2026-09-16: "compute
    all growth of current territory".

    T(C, p) = C's own row for p (if it has one) PLUS every one of C's
    predecessors' own row for p (if it has one) -- summed over every
    geography whose territory is now part of C at today's map. This is NOT
    the same construction as `reconstruct_additive`'s gap-fill: gap-fill
    stops summing predecessors the moment the successor has ANY own row for
    that cell (a published figure never changes). T sums BOTH, because they
    describe DIFFERENT, non-overlapping territory for exactly as long as the
    predecessor still reported on its own -- Antwerp's own 2020 row (529,247)
    genuinely does not include Borsbeek (10,949), which still existed as its
    own reporting geography that year, so T(2020) = 540,196 is the true
    population of Antwerp's CURRENT territory in 2020, not a double-count.

    Returned as geo_id -> period -> (value, used_predecessor_data). The
    second element of the tuple is True whenever at least one predecessor
    contributed to that period's T -- the caller uses it to mark a growth
    figure whose BASE period used predecessor data, so a reader can tell
    (the discontinuity label, same convention as a `reconstructed` level).

    ALL-PREDECESSORS-OR-NOTHING still applies to the predecessor side: if the
    successor has no own row for p, T(p) exists only when EVERY predecessor
    has a real value for p (not suppressed, not na, not absent) -- exactly
    `reconstruct_additive`'s rule 2, reapplied here. If the successor DOES
    have its own row for p, that alone is enough for T(p) to exist (an
    ordinary commune with no lineage row for that period at all reduces to
    this case), and any predecessor that also reports for p is added on top,
    again all-or-nothing among the predecessors that exist for p -- a
    partial predecessor sum is refused even when the successor's own row
    could stand alone, because a HALF-territory addition is worse than none:
    it would silently move T away from both "successor alone" and "full
    current territory" without saying which partial view a reader is seeing.
    """
    by_cell = _index_rows(r for r in rows if r[1] == indicator_id)

    def _value(geo_id: str, period: str) -> float | None:
        row = by_cell.get((geo_id, indicator_id, period))
        if row is None:
            return None
        value, status = row[5], row[6]
        if value is None or status in ("suppressed", "na"):
            return None
        return value

    out: dict[str, dict[str, tuple[float, bool]]] = {}
    for successor, predecessors in lineage.items():
        periods: set[str] = set()
        for geo_id, _ind, _n, _u, period, *_ in rows:
            if geo_id == successor or geo_id in predecessors:
                periods.add(period)

        series: dict[str, tuple[float, bool]] = {}
        for period in sorted(periods):
            own = _value(successor, period)

            pred_values = [_value(pred, period) for pred in predecessors]
            any_pred_row = any((pred, indicator_id, period) in by_cell for pred in predecessors)
            all_preds_present = bool(predecessors) and all(v is not None for v in pred_values)

            if own is not None and not any_pred_row:
                # No predecessor reports for this period at all (post-merger
                # years, or an ordinary commune with a lineage row for a
                # different period only) -- T is just the successor's own row.
                series[period] = (own, False)
            elif all_preds_present:
                total = sum(v for v in pred_values if v is not None)
                if own is not None:
                    total += own
                series[period] = (total, True)
            # else: predecessors partially reported and cannot be summed
            # (rule above) -- T(p) is undefined for this period, own row or
            # not, so no entry is written.
        if series:
            out[successor] = series
    return out


# The two horizon functions growth-on-current-territory applies to, and the
# keyword each expects for its lookback in years -- deliberately NOT read
# from a generic "years" kwarg, because five_year_change hardcodes 5 and
# takes no `years` argument at all (see derived.py), so the horizon has to be
# named per function rather than assumed uniform.
_GROWTH_FUNCTION_HORIZONS = {
    "five_year_change": 5,
    "cagr": None,  # read from the config's own args.years instead.
}


def territory_consistent_growth(
    territory: Mapping[str, Mapping[str, tuple[float, bool]]],
    growth_configs: Mapping[str, Mapping],
) -> list[Row]:
    """Recompute a growth-shaped derived indicator on T instead of a raw
    series, for every successor `territory_series` produced a series for.

    `growth_configs` is the subset of derived configs whose function is one
    of `_GROWTH_FUNCTION_HORIZONS` and whose single input is the SAME raw
    additive indicator `territory` was built from (the caller filters this;
    see export_communes_history_csv.py) -- POPULATION_CHANGE_5Y and
    POPULATION_CAGR_10Y today, and nothing else, because MUN_REVENUE_GROWTH_1Y
    / MUN_EXPENDITURE_GROWTH_1Y take a DERIVED per-capita ratio as their
    input, not a raw additive indicator, so they never reach this function at
    all -- one rule (this function only ever sees an additive input's own
    territory series), no indicator-ID special case inside it.

    Uses the SAME functions as the live engine (src/analytics/derived.py),
    imported lazily for the same reason reconstruct_ratios does: this module
    promises not to require the engine as a hard dependency.

    A cell is written only when BOTH endpoints exist in `territory` (rule 26:
    absent, never a partial computation) -- exactly what growth_rate/cagr
    already refuse internally, re-derived here from T's own shape so a
    period where T itself is undefined never reaches the function at all.

    status is "reconstructed" whenever EITHER endpoint used predecessor data
    (`territory`'s own used_predecessor_data flag) -- the discontinuity
    label a reader needs on a growth figure that spans a now-different-shaped
    territory than pre-merger source data alone would show. Checking only
    the base would still be correct in practice -- a predecessor stops
    reporting once merged away, so if the LATER (endpoint) period needed a
    predecessor, the EARLIER (base) period necessarily did too, since it is
    at least as close to or further from the merger date -- but both are
    checked explicitly rather than relying on that one-directional argument
    silently staying true if a future lineage shape ever violates it.
    """
    from src.analytics import derived as derived_functions

    out: list[Row] = []
    for indicator_id, config in growth_configs.items():
        spec = config.get("derived") or {}
        function_name = spec.get("function")
        if function_name not in _GROWTH_FUNCTION_HORIZONS:
            continue
        years = _GROWTH_FUNCTION_HORIZONS[function_name] or (spec.get("args") or {}).get("years")
        if not years:
            continue
        func = getattr(derived_functions, function_name)
        name_en = (config.get("name") or {}).get("en", indicator_id)
        unit = config.get("unit", "")

        for geo_id, series in territory.items():
            plain_series = {period: value for period, (value, _used_pred) in series.items()}
            for period in sorted(series):
                base_period = derived_functions.shift_period_years(period, years)
                if base_period not in series:
                    continue  # endpoint exists, base does not -- absent, not partial.
                kwargs = {} if function_name == "five_year_change" else {"years": years}
                value = func(plain_series, period, **kwargs)
                if value is None:
                    continue
                _base_value, base_used_predecessor = series[base_period]
                _endpoint_value, endpoint_used_predecessor = series[period]
                status = (
                    "reconstructed"
                    if (base_used_predecessor or endpoint_used_predecessor)
                    else "derived"
                )
                out.append((geo_id, indicator_id, name_en, unit, period, value, status, ""))
    return out


def reconstruct_ratios(
    reconstructed_components: Iterable[Row],
    lineage: Mapping[str, list[str]],
    derived_configs: Mapping[str, Mapping],
) -> list[Row]:
    """Recompute the RECOMPUTE-method ratios from reconstructed components,
    in a second, isolated pass -- never from averaging predecessor ratios,
    and never by feeding reconstructed values back into the engine's own
    ObservationSet (rule 3 in the module docstring: that set is the peer set
    for percentile and friends, and reconstructed rows must never widen or
    shift it).

    `reconstructed_components` is the OUTPUT of `reconstruct_additive` --
    already summed, already gap-filled, already "all predecessors or
    nothing". A ratio is recomputed for a (successor, period) only when every
    one of its declared inputs survived that pass for that cell; otherwise
    the ratio genuinely does not exist yet, the same "refuse rather than
    invent" rule aggregate.py applies to a poorly-covered aggregate.

    NAME/UNIT COME FROM THE DERIVED CONFIG, NOT `indicators`. Every one of the
    nine recomputable ratios (AVG_NET_TAXABLE_INCOME and its siblings) is a
    derived-only indicator with no row of its own in the `indicators` table --
    exactly like export_communes_history_csv.py's own live-derived-rows loop,
    which reads `cfg["name"]["en"]` / `cfg["unit"]`, never `indicator_meta`,
    for the same reason. Requiring an `indicators` row here would silently
    produce zero reconstructed ratios for every one of the nine.

    Uses the exact functions in src/analytics/derived.py, imported lazily
    inside the function body so importing this module never requires the
    engine's dependency (there is none today, but the module docstring's
    promise is "nothing here opens a file or touches the database", not
    "nothing here imports anything") -- the value is recomputed by calling the
    SAME code as commune-level and aggregate-level derivation, so a formula
    fix here is impossible to accidentally diverge from those.
    """
    from src.analytics import derived as derived_functions

    by_geo_ind_period: dict[tuple[str, str, str], float] = {
        (geo_id, indicator_id, period): value
        for geo_id, indicator_id, _n, _u, period, value, _s, _c in reconstructed_components
        if value is not None
    }

    out: list[Row] = []
    for indicator_id, config in derived_configs.items():
        spec = config.get("derived") or {}
        function_name = spec.get("function")
        if function_name not in RECOMPUTABLE_FUNCTIONS:
            continue
        inputs = spec.get("inputs") or []
        if not inputs:
            continue
        func = getattr(derived_functions, function_name)
        name_en = (config.get("name") or {}).get("en", indicator_id)
        unit = config.get("unit", "")

        for successor in lineage:
            periods = None
            for input_id in inputs:
                cells = {
                    period
                    for (geo_id, ind_id, period) in by_geo_ind_period
                    if geo_id == successor and ind_id == input_id
                }
                periods = cells if periods is None else periods & cells
            if not periods:
                continue

            for period in sorted(periods):
                values = [by_geo_ind_period[(successor, input_id, period)] for input_id in inputs]
                value = func(*values)
                if value is None:
                    continue
                out.append(
                    (successor, indicator_id, name_en, unit, period, value, "reconstructed", "")
                )
    return out


def only_missing_cells(reconstructed: Iterable[Row], existing: Iterable[Row]) -> list[Row]:
    """Drop every reconstructed row for a (geo_id, indicator_id, period) the
    successor ALREADY publishes -- the gap-fill rule, enforced at the one
    place both reconstruction passes funnel through.

    `existing` is the successor's own real rows (any status, including a
    suppressed or na one -- the successor's own "we asked and were refused" is
    still the successor's own answer and must not be overwritten by a
    reconstruction, exactly as "no already-published value may change" says).
    """
    already = {
        (geo_id, indicator_id, period) for geo_id, indicator_id, _n, _u, period, *_ in existing
    }
    return [row for row in reconstructed if (row[0], row[1], row[4]) not in already]


def reconstruct(
    raw_rows: Iterable[Row],
    lineage_rows: Iterable[tuple[str, str | None, str | None]],
    indicator_is_additive: Mapping[str, bool],
    indicator_meta: Mapping[str, tuple[str, str]],
    derived_configs: Mapping[str, Mapping] | None = None,
    refused_indicator_ids: Iterable[str] = (),
    existing_derived_cells: Iterable[tuple[str, str, str]] = (),
) -> list[Row]:
    """The single entry point the two exporters call.

    `raw_rows` is every raw (non-derived) observation row available -- the
    history exporter's `raw` list, or the equivalent rows export_communes_csv
    assembles -- covering BOTH predecessor and successor geo_ids; this
    function reads predecessor rows to build sums and successor rows only to
    know what to leave alone (via `only_missing_cells`).

    `lineage_rows` is `(geo_id, valid_to, successor_geo_id)` straight from
    `geographies`, unfiltered by wave or date -- see the module docstring for
    why a date filter is the one mistake this function must never make.

    `refused_indicator_ids` lets a caller name indicators with no additive
    components at all (the four police per-10k rates: is_additive=0 with
    nothing to sum) so they are excluded from the additive pass even if a
    caller's indicator_is_additive mapping were ever wrong about them --
    belt-and-braces, since methods_from_metadata already refuses anything
    with is_additive=0.

    `existing_derived_cells` is `(geo_id, indicator_id, period)` for every
    DERIVED cell the successor already has from the engine's own live
    compute() pass -- e.g. AVG_NET_TAXABLE_INCOME for Antwerp 2023. The nine
    RECOMPUTE ratios never appear in `raw_rows` (they are derived-only, no row
    of their own in `observations`), so gap-fill rule 1 above is otherwise
    structurally blind to them: `existing` below is built from `raw_rows`
    alone and a reconstructed ratio would never collide with anything in it,
    even for a (geo_id, indicator, period) the successor already publishes.
    Without this, a caller that computes derived indicators (like the history
    exporter's compute() pass) could end up with BOTH a `reconstructed` ratio
    and the successor's own `derived` one for the same cell -- two rows for
    one key, with no rule saying which wins. Passing the already-computed
    keys here closes that gap at the source, so a caller never has to dedupe
    downstream or rely on append/sort order for correctness.
    """
    lineage = lineage_from_geographies(lineage_rows)
    if not lineage:
        return []

    methods = methods_from_metadata(indicator_is_additive, derived_configs)
    additive_ids = {
        i for i, m in methods.items() if m == SUM and i not in set(refused_indicator_ids)
    }

    components = reconstruct_additive(raw_rows, lineage, additive_ids, indicator_meta)

    ratio_configs = {
        i: c for i, c in (derived_configs or {}).items() if methods.get(i) == RECOMPUTE
    }
    ratios = reconstruct_ratios(components, lineage, ratio_configs) if ratio_configs else []

    reconstructed = components + ratios
    existing = [row for row in raw_rows if row[0] in lineage]
    filled = only_missing_cells(reconstructed, existing)

    already_derived = set(existing_derived_cells)
    if not already_derived:
        return filled
    return [row for row in filled if (row[0], row[1], row[4]) not in already_derived]
