"""Per-indicator peer benchmarks -- Block M, docs/features/peer_model.md
"Per-indicator benchmarks", ADR 0015.

For every municipal indicator (every raw indicator the committed history
CSVs carry, plus every derived indicator config/indicators/derived/*.yaml
defines -- EXCLUDING the cross-sectional functions percentile/z_score/rank
(any derived `function` that ranks or positions communes against each
other -- decided from config/indicators/derived/*.yaml's `function` field,
never from an indicator id, so a future rank-shaped function is excluded by
what it computes, not by name), and excluding any RAW published indicator
that is itself a rank or percentile (checked against its config's unit and
description -- none exists today, but the exclusion is not id-based either),
exactly as scripts/export_percentiles_csv.py already excludes
POPULATION_PERCENTILE, because a percentile of a percentile is meaningless),
at that indicator's own LATEST period (the newest period any commune has a
usable value for, same convention as export_percentiles_csv.py's
latest_only), for each of today's 565 communes and each of its two peer
lists (national, region) read from public/data/metadata/peers.json:

  peer_median         median of the up-to-10 peers with a usable value in
                      THAT SAME period
  peers_with_value    how many of the (up to 10) peers that was
  position / of       the commune's rank among itself + those peers
                      (1 = highest, ties share the best rank)
  deviation_pct       (value - peer_median) / peer_median * 100 -- null
                      when peer_median <= 0 (see deviation_withheld)
  deviation_withheld  "median_zero" or "median_negative" when deviation_pct
                      is null because the peer median is <= 0 (peer_median,
                      position and of are still shown -- the rank still
                      means something); omitted (key absent) when
                      deviation_pct is a real number
  selection_variable  true when this indicator IS, or is the NUMERATOR of,
                      one of the eleven peer-selection variables (ADR 0015
                      "Circularity") -- the page marks these "peers were
                      chosen partly on this figure". Derived from
                      src.analytics.peers.VARIABLES, not hand-maintained --
                      see SELECTION_VARIABLE_INDICATORS below.

All the arithmetic is src.analytics.peers.peer_stats; this script only
assembles the per-(indicator, commune) inputs and writes the payload.

Suppressed ("S") and na ("N") peer values are excluded from the peer set
entirely, never treated as zero (CLAUDE.md rule 26) -- same USABLE_STATUSES
set as scripts/export_peer_model.py. A benchmark whose result is entirely
null (peer_median is None) is OMITTED from the indicator's block rather than
written as a block of nulls, per the handoff. A median <= 0 is NOT entirely
null -- peer_median/position/of are kept and only deviation_pct is withheld
(see deviation_withheld above), so those entries are written, not omitted.

PUBLISHED ROWS ALWAYS WIN OVER THE ENGINE (bug fix, this batch). A derived
indicator (config/indicators/derived/*.yaml) is recomputed by the Block G
engine here ONLY for the (indicator, period, commune) cells that have NO
published row at all in the history CSVs. Any cell the history CSVs publish
-- for the commune itself, or for any of its peers -- is the value, and the
engine's own recomputation of that same cell is discarded. Before this fix,
`all_rows = raw_rows + derived_rows` let a later-indexed engine row silently
overwrite an earlier published one (`_index_rows` keeps the last value seen
per (indicator, period, nis)): public/data/peers/11002.json's
POPULATION_CHANGE_5Y (national) published the engine's 6.8373 while
data/communes_history.csv's own 2026 row for 11002 -- reused verbatim by the
2026-09-16 "growth on current territory" rule everywhere else on the site --
says 4.6478. This is now impossible: `build_derived_rows` is filtered, before
computing anything, down to indicator ids that have ZERO published rows in
the history CSVs (`_published_indicator_ids`), so a derived indicator
that is also published (all but a handful of the 23 -- see the module-level
constant list mirrored in the PR body) never reaches the engine at all here,
and the published row is what every peer list benchmarks.

BENCHMARK UNIVERSE AND WITHHELD REASONS (this batch). Every indicator the
exporter would otherwise consider a candidate -- published or derived, minus
nothing -- is the "universe". For every commune and every universe indicator,
EXACTLY ONE of two things is true: an entry exists in lists.<list>.<IND>, or
the indicator's code appears in withheld.<list> with a reason:

  "excluded"          the indicator is a percentile/z_score/rank function
                      (or a raw rank/percentile indicator) -- structurally
                      excluded from ever being benchmarked, same for every
                      commune.
  "no_current_value"  the commune has no usable value in P, the indicator's
                      own newest period across all communes. `own_period` is
                      the commune's own newest period with a usable value
                      BELOW P, or null if it has never had one. KNOWN GAP
                      (stated, not fixed, per the handoff): a Walloon-only
                      indicator on a Flemish commune reads this way too, with
                      own_period null -- indistinguishable here from a
                      genuine gap, because nothing in the committed data
                      marks "not applicable to this commune" as a fifth
                      state yet (docs/steps has an open [SPEC] for it).
  "few_peers"         the commune has a usable value in P, but fewer than
                      `MIN_PEERS_WITH_VALUE` (peers.py's constant, read from
                      code, never re-typed) of its 10 peers do.
                      `peers_with_value` is how many did.

Writes public/data/peers/<nis>.json, one per current commune:

  {model_version, variant, nis_code,
   peers: {national: [{nis, rank}] x10, region: [{nis, rank}] x10},
   min_peers_with_value: 7,
   withheld: {national: {IND: {reason, ...}}, region: {...}},
   lists: {national: {IND: {period, value, peer_median, peers_with_value,
                             position, of, deviation_pct,
                             [deviation_withheld], selection_variable}},
           region: {...}}}

`peers` carries no name and no distance/similarity -- just the nis code and
its rank in that list, read straight from public/data/metadata/peers.json.
A page wanting a peer's name looks it up in
public/data/metadata/geographies.json (or the commune index it already
loads), never hand-typed here (rule 36) and never duplicated from peers.json
(one join key, the nis code, per rule 25).

Deterministic: sorted keys, compact JSON, ensure_ascii=False, no embedded
clock, trailing newline -- CLAUDE.md rule 35, byte-identical rebuild.

Usage:  python scripts/export_peer_benchmarks.py
            [--history-dir data/communes_history]
            [--history-csv data/communes_history.csv]
            [--geographies config/geography/geographies.csv]
            [--peers-json public/data/metadata/peers.json]
            [--derived-dir config/indicators/derived]
            [--out-dir public/data/peers]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.export_peer_model import (  # noqa: E402
    _current_municipality_nis,
    _read_all_history_rows,
)
from src.analytics.engine import ObservationSet, compute  # noqa: E402
from src.analytics.peers import (  # noqa: E402
    MIN_PEERS_WITH_VALUE,
    SELECTION_VARIABLE_INDICATOR_IDS,
    peer_stats,
)
from src.validation.config_schema import load_and_validate_derived  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY_DIR = REPO_ROOT / "data" / "communes_history"
DEFAULT_HISTORY_CSV = REPO_ROOT / "data" / "communes_history.csv"
DEFAULT_GEOGRAPHIES = REPO_ROOT / "config" / "geography" / "geographies.csv"
DEFAULT_PEERS_JSON = REPO_ROOT / "public" / "data" / "metadata" / "peers.json"
DEFAULT_DERIVED_DIR = REPO_ROOT / "config" / "indicators" / "derived"
DEFAULT_OUT_DIR = REPO_ROOT / "public" / "data" / "peers"

# Same statuses export_peer_model.py treats as a usable measurement -- "S"
# (suppressed) and "N" (na) carry no usable value and are excluded, never
# treated as zero (rule 26).
USABLE_STATUSES = {"A", "P", "derived", "reconstructed"}

# Cross-sectional derived functions produce a rank/score OF the peer set
# itself; benchmarking one against a *different* peer set (10 communes) is
# meaningless the same way a percentile of a percentile is. Same exclusion
# scripts/export_percentiles_csv.py already applies to POPULATION_PERCENTILE.
# Matched against config/indicators/derived/*.yaml's `function` field, not
# against an indicator id (bug 2 fix) -- any function that ranks or positions
# communes against each other belongs here, however it is later named.
CROSS_SECTIONAL_FUNCTIONS = {"percentile", "z_score", "rank"}

# RAW (published, non-derived) indicators that are themselves a rank or a
# percentile -- excluded the same way, but there is no `function` field to
# check for a raw indicator, so this is checked by hand against every raw
# indicator_code's config/indicators/*.yaml (unit and description), not by
# id pattern-matching. Measured 2026-09-27 (see the PR body): none of the
# indicator_codes published in data/communes_history.csv or
# data/communes_history/*.csv is a rank or percentile in its own right --
# POPULATION_PERCENTILE is derived (function "percentile", already excluded
# above) and no other publishes a unit or description naming a rank/position.
# Kept as an explicit, reviewable set (not a heuristic over indicator names,
# rule 24) so a future raw rank/percentile indicator must be added here by a
# human reading its config, not picked up or missed by a string match.
RAW_RANK_OR_PERCENTILE_INDICATORS: frozenset[str] = frozenset()

# The eleven peer-selection variables (docs/features/peer_model.md, "The
# variable list"), each contributing every indicator id that IS that
# variable or is its NUMERATOR -- ADR 0015's "Circularity" note: showing
# either variable's own deviation must carry "peers were chosen partly on
# this figure". Derived from src.analytics.peers.VARIABLES's
# `indicator_ids` (the single source of truth), NOT hand-maintained here --
# adding a variable to VARIABLES with its indicator_ids set is the only way
# to change this set. Any indicator id that should be flagged WITHOUT being
# a selection variable or its numerator (there is none today) would go in a
# separate, explicitly named additive constant, never folded back into this
# derived set.
SELECTION_VARIABLE_INDICATORS: frozenset[str] = SELECTION_VARIABLE_INDICATOR_IDS


class BenchmarksExportError(ValueError):
    """The committed data does not support building the peer benchmarks as
    the spec requires -- raised rather than silently coercing (rule 13)."""


def _load_peers(peers_json_path: Path) -> dict:
    if not peers_json_path.exists():
        raise BenchmarksExportError(
            f"{peers_json_path} not found -- run scripts/export_peer_model.py first"
        )
    return json.loads(peers_json_path.read_text(encoding="utf-8"))


def find_duplicate_rows(rows: list[dict]) -> dict[tuple[str, str, str], int]:
    """(indicator_code, period, nis_code) -> how many rows the committed
    history CSVs carry for it, for every key that appears MORE THAN ONCE.

    Measured across the whole committed history (this batch, 2026-09-27):
    zero duplicates found -- see the PR body. `_index_rows` refuses (rule 13)
    the moment any are found, rather than silently keeping "whichever row
    happened to be read last", which is what the plain last-write-wins index
    used to do for a duplicate exactly as quietly as it did for the
    derived-vs-published collision bug 1 fixes above. If the daily job ever
    legitimately produces a duplicate (e.g. an adapter re-run appending
    instead of replacing), THIS FUNCTION IS WHAT FAILS FIRST, loudly, rather
    than the export silently picking one row -- do not weaken it to keep the
    daily job green; fix the producer, or -- only if duplicates turn out to
    be an expected, understood shape of the daily job -- change the refusal
    to a recorded, deliberate exception, never a silent drop.
    """
    counts: dict[tuple[str, str, str], int] = {}
    for row in rows:
        key = (row["indicator_code"], row["period"], row["nis_code"])
        counts[key] = counts.get(key, 0) + 1
    return {key: n for key, n in counts.items() if n > 1}


def _index_rows(rows: list[dict]) -> dict[tuple[str, str], dict[str, tuple[float | None, str]]]:
    """(indicator_code, period) -> {nis_code: (value_or_None, status)},
    same shape as export_peer_model.py's _index_by_indicator_period.

    Refuses (rule 13) rather than silently keeping the last row seen, the
    moment the committed history carries more than one row for the same
    (indicator, period, commune) -- see find_duplicate_rows. Today's data
    has none; if the daily job ever produces one, this must fail loudly, not
    pick a value.
    """
    duplicates = find_duplicate_rows(rows)
    if duplicates:
        sample = ", ".join(f"{i}/{p}/{n} (x{c})" for (i, p, n), c in sorted(duplicates.items())[:10])
        raise BenchmarksExportError(
            f"{len(duplicates)} (indicator, period, commune) key(s) have more than one row "
            f"in the committed history -- refusing rather than silently picking one: {sample}"
            + (" ..." if len(duplicates) > 10 else "")
        )
    index: dict[tuple[str, str], dict[str, tuple[float | None, str]]] = {}
    for row in rows:
        key = (row["indicator_code"], row["period"])
        bucket = index.setdefault(key, {})
        status = row["status"]
        if status in USABLE_STATUSES and row["value"] not in ("", None):
            bucket[row["nis_code"]] = (float(row["value"]), status)
        else:
            bucket.setdefault(row["nis_code"], (None, status))
    return index


def _published_indicator_ids(rows: list[dict]) -> frozenset[str]:
    """Every indicator_code that has AT LEAST ONE row in the committed
    history CSVs (raw rows only -- called before any derived row exists),
    regardless of that row's status. A derived indicator in this set has a
    published row somewhere and must never be overwritten by the engine's
    own recomputation of the same cell (bug 1 fix, see module docstring)."""
    return frozenset(row["indicator_code"] for row in rows)


def _indicator_names(rows: list[dict]) -> dict[str, str]:
    """indicator_code -> its English name, read verbatim from the first row
    seen for it (every row for an indicator carries the same indicator_name).
    Used only for derived indicators the history rows never carry a name
    for; raw indicator names come from the rows themselves."""
    names: dict[str, str] = {}
    for row in rows:
        names.setdefault(row["indicator_code"], row["indicator_name"])
    return names


def build_derived_rows(
    rows: list[dict], derived_dir: Path
) -> tuple[list[dict], dict[str, str], frozenset[str]]:
    """Every derived indicator's (nis, period, value) as history-row-shaped
    dicts, computed by the Block G engine over the raw rows -- same pattern
    export_percentiles_csv.py's _derived_values uses, cross-sectional
    functions excluded.

    BUG 1 FIX: a derived indicator that already has at least one published
    row in `rows` (_published_indicator_ids) is EXCLUDED from what the engine
    computes here -- entirely, not cell-by-cell. The published rows are the
    value for that indicator everywhere (this commune and every peer); this
    function only ever fills in a derived indicator that the history CSVs
    never publish a single row for. Recomputing a cell the CSVs already
    publish and letting it overwrite that published row (the pre-fix bug --
    see module docstring) is exactly what this guards against.

    Returns (rows, names, engine_only_ids) where `rows` have status "derived"
    (a real value) for every cell the engine produced (missing/None cells are
    omitted, not written as a null row), `names` maps each derived indicator
    id actually computed to its English name from its own YAML
    (config/indicators/derived's "name.en"), and `engine_only_ids` is exactly
    the set of derived indicator ids this function was willing to compute
    (cross-sectional functions and already-published indicators excluded) --
    the caller needs this set again to classify the *withheld* universe (an
    excluded derived indicator is withheld with reason "excluded" even where
    the engine never produces a row for it).
    """
    published_ids = _published_indicator_ids(rows)
    present = published_ids
    configs = load_and_validate_derived(derived_dir, present) if derived_dir.is_dir() else {}
    configs = {
        indicator_id: cfg
        for indicator_id, cfg in configs.items()
        if cfg["derived"]["function"] not in CROSS_SECTIONAL_FUNCTIONS
        and indicator_id not in published_ids
    }
    if not configs:
        return [], {}, frozenset()

    obs = ObservationSet(
        (row["indicator_code"], row["nis_code"], row["period"], float(row["value"]))
        for row in rows
        if row["status"] in USABLE_STATUSES and row["value"] not in ("", None)
    )
    result = compute(obs, configs, present)

    derived_rows: list[dict] = []
    for indicator_id in configs:
        for nis, period in result.cells(indicator_id):
            value = result.value(indicator_id, nis, period)
            if value is None:
                continue
            derived_rows.append(
                {
                    "nis_code": nis,
                    "indicator_code": indicator_id,
                    "indicator_name": configs[indicator_id]["name"]["en"],
                    "period": period,
                    "value": value,
                    "status": "derived",
                }
            )
    names = {i: cfg["name"]["en"] for i, cfg in configs.items()}
    return derived_rows, names, frozenset(configs)


def _latest_period_per_indicator(
    index: dict[tuple[str, str], dict[str, tuple[float | None, str]]],
) -> dict[str, str]:
    """indicator_code -> the newest period ANY commune has a row for
    (usable or not -- same "newest period seen" convention
    export_percentiles_csv.py uses; a benchmark is then withheld per-commune
    by peer_stats, never by silently picking an earlier period for one
    commune and a later one for another)."""
    newest: dict[str, str] = {}
    for indicator_id, period in index:
        if period > newest.get(indicator_id, ""):
            newest[indicator_id] = period
    return newest


def build_benchmark_universe(
    raw_rows: list[dict], derived_dir: Path
) -> tuple[frozenset[str], frozenset[str]]:
    """(candidate_ids, excluded_ids): every indicator this exporter considers
    at all, and the subset of those that are structurally excluded
    ("excluded" withheld reason -- percentile/z_score/rank derived functions,
    or a raw published rank/percentile indicator).

    `candidate_ids` is every raw published indicator_code PLUS every derived
    indicator_code config/indicators/derived/*.yaml defines (whether or not
    it is cross-sectionally excluded, and whether or not it is also
    published -- the universe is "every indicator this model could ever say
    something about", not just the ones that end up benchmarked). A commune
    and list where a candidate is neither entered in `lists` nor in
    `withheld` is a bug (see test_export_peer_benchmarks.py's exhaustiveness
    test), so this set must be complete, not merely "what happened to
    survive filtering".
    """
    published_ids = _published_indicator_ids(raw_rows)
    all_derived = load_and_validate_derived(derived_dir, published_ids) if derived_dir.is_dir() else {}
    cross_sectional_ids = frozenset(
        indicator_id
        for indicator_id, cfg in all_derived.items()
        if cfg["derived"]["function"] in CROSS_SECTIONAL_FUNCTIONS
    )
    candidate_ids = published_ids | frozenset(all_derived)
    excluded_ids = cross_sectional_ids | (RAW_RANK_OR_PERCENTILE_INDICATORS & candidate_ids)
    return candidate_ids, excluded_ids


def _own_usable_periods(
    index: dict[tuple[str, str], dict[str, tuple[float | None, str]]],
) -> dict[tuple[str, str], list[str]]:
    """(indicator_code, nis) -> every period that commune has a USABLE value
    for, sorted ascending -- used only to find the commune's own newest
    usable period BELOW the indicator's current period, for the
    "no_current_value" withheld reason's `own_period`."""
    periods: dict[tuple[str, str], list[str]] = {}
    for (indicator_id, period), bucket in index.items():
        for nis, (value, _status) in bucket.items():
            if value is None:
                continue
            periods.setdefault((indicator_id, nis), []).append(period)
    for key in periods:
        periods[key].sort()
    return periods


def build_benchmarks(
    communes: set[str],
    index: dict[tuple[str, str], dict[str, tuple[float | None, str]]],
    latest_period: dict[str, str],
    peers_model: dict,
    candidate_ids: frozenset[str],
    excluded_ids: frozenset[str],
) -> dict[str, dict]:
    """nis -> {model_version, variant, nis_code, peers, min_peers_with_value,
    withheld: {national: {...}, region: {...}}, lists: {national: {...},
    region: {...}}}.

    For every commune and every list, EXACTLY ONE of `lists[list][IND]` or
    `withheld[list][IND]` exists for every IND in `candidate_ids` --
    guaranteed here by construction (every candidate not in `excluded_ids`
    and not written to `lists_payload` falls through to a withheld reason,
    and every one of `excluded_ids` is withheld with reason "excluded"
    unconditionally, even when it happens to have no row in `index` at all),
    and re-checked by test_export_peer_benchmarks.py's exhaustiveness test
    over the real 565 files.
    """
    model_version = peers_model["model_version"]
    variant = peers_model["variant"]
    peer_lists = peers_model["communes"]
    own_periods = _own_usable_periods(index)

    result: dict[str, dict] = {}
    for nis in sorted(communes):
        lists_payload: dict[str, dict] = {"national": {}, "region": {}}
        withheld_payload: dict[str, dict] = {"national": {}, "region": {}}
        peers_payload: dict[str, list[dict]] = {"national": [], "region": []}
        for list_name in ("national", "region"):
            peer_nis_list = [entry["nis"] for entry in peer_lists[nis][list_name]]
            peers_payload[list_name] = [
                {"nis": entry["nis"], "rank": entry["rank"]} for entry in peer_lists[nis][list_name]
            ]
            for indicator_id in candidate_ids:
                if indicator_id in excluded_ids:
                    withheld_payload[list_name][indicator_id] = {"reason": "excluded"}
                    continue

                period = latest_period.get(indicator_id)
                if period is None:
                    # No row anywhere (raw or engine-computed) for this
                    # indicator at all -- a candidate that never actually
                    # produced any data. Withheld exactly like an ordinary
                    # "no current value", with both periods null rather than
                    # crashing on a period comparison against nothing.
                    withheld_payload[list_name][indicator_id] = {
                        "reason": "no_current_value",
                        "period": None,
                        "own_period": None,
                    }
                    continue
                bucket = index.get((indicator_id, period), {})
                own_value, _own_status = bucket.get(nis, (None, None))
                if own_value is None:
                    own_history = own_periods.get((indicator_id, nis), [])
                    own_period = next((p for p in reversed(own_history) if p < period), None)
                    withheld_payload[list_name][indicator_id] = {
                        "reason": "no_current_value",
                        "period": period,
                        "own_period": own_period,
                    }
                    continue

                peer_values = [bucket.get(peer_nis, (None, None))[0] for peer_nis in peer_nis_list]
                stats = peer_stats(own_value, peer_values)
                if stats["peer_median"] is None:
                    withheld_payload[list_name][indicator_id] = {
                        "reason": "few_peers",
                        "period": period,
                        "peers_with_value": stats["peers_with_value"],
                    }
                    continue

                entry = {
                    "period": period,
                    "value": own_value,
                    "peer_median": stats["peer_median"],
                    "peers_with_value": stats["peers_with_value"],
                    "position": stats["position"],
                    "of": stats["of"],
                    "deviation_pct": (
                        None if stats["deviation_pct"] is None else round(stats["deviation_pct"], 4)
                    ),
                    "selection_variable": indicator_id in SELECTION_VARIABLE_INDICATORS,
                }
                # deviation_withheld ("median_zero" / "median_negative") is
                # only present when deviation_pct is null for that reason --
                # key omitted (not written as null) when deviation_pct is a
                # real number, per docs/features/peer_model.md.
                if stats["deviation_withheld"] is not None:
                    entry["deviation_withheld"] = stats["deviation_withheld"]
                lists_payload[list_name][indicator_id] = entry

        result[nis] = {
            "model_version": model_version,
            "variant": variant,
            "nis_code": nis,
            "peers": peers_payload,
            "min_peers_with_value": MIN_PEERS_WITH_VALUE,
            "withheld": withheld_payload,
            "lists": lists_payload,
        }
    return result


def write_payloads(payloads: dict[str, dict], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for nis, payload in payloads.items():
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        (out_dir / f"{nis}.json").write_text(text + "\n", encoding="utf-8", newline="\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-dir", type=Path, default=DEFAULT_HISTORY_DIR)
    parser.add_argument("--history-csv", type=Path, default=DEFAULT_HISTORY_CSV)
    parser.add_argument("--geographies", type=Path, default=DEFAULT_GEOGRAPHIES)
    parser.add_argument("--peers-json", type=Path, default=DEFAULT_PEERS_JSON)
    parser.add_argument("--derived-dir", type=Path, default=DEFAULT_DERIVED_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()

    communes = _current_municipality_nis(args.geographies)
    if len(communes) != 565:
        raise SystemExit(f"expected 565 current municipalities, found {len(communes)}")

    peers_model = _load_peers(args.peers_json)
    missing_from_model = communes - set(peers_model["communes"])
    if missing_from_model:
        raise BenchmarksExportError(
            f"{args.peers_json} has no peer list for: {sorted(missing_from_model)}"
        )

    raw_rows = _read_all_history_rows(args.history_dir, args.history_csv)
    derived_rows, _derived_names, _engine_only_ids = build_derived_rows(raw_rows, args.derived_dir)
    candidate_ids, excluded_ids = build_benchmark_universe(raw_rows, args.derived_dir)

    all_rows = raw_rows + derived_rows
    index = _index_rows(all_rows)
    latest_period = _latest_period_per_indicator(index)

    payloads = build_benchmarks(
        communes, index, latest_period, peers_model, candidate_ids, excluded_ids
    )
    write_payloads(payloads, args.out_dir)

    total_bytes = sum((args.out_dir / f"{nis}.json").stat().st_size for nis in communes)
    largest = max(communes, key=lambda nis: (args.out_dir / f"{nis}.json").stat().st_size)
    largest_size = (args.out_dir / f"{largest}.json").stat().st_size
    print(f"wrote {len(communes)} files to {args.out_dir} ({total_bytes / 1e6:.3f} MB total)")
    print(f"largest: {largest}.json ({largest_size} bytes)")


if __name__ == "__main__":
    main()
