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
    engine. `indicators.aggregation_method` is NEVER read: 27 rows carry
    'population_weighted', a method CLAUDE.md forbids and that was measured
    wrong for the two ratios in this pipeline, and nothing under src/ reads
    that column today. Fixing those 27 rows is a separate PR.
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
    return only_missing_cells(reconstructed, existing)
