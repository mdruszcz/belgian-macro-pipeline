"""Per-indicator peer benchmarks -- Block M, docs/features/peer_model.md
"Per-indicator benchmarks", ADR 0015.

For every municipal indicator (every raw indicator the committed history
CSVs carry, plus every derived indicator config/indicators/derived/*.yaml
defines -- EXCLUDING the cross-sectional functions percentile/z_score,
exactly as scripts/export_percentiles_csv.py already excludes them, because
a percentile of a percentile is meaningless), at that indicator's own
LATEST period (the newest period any commune has a usable value for, same
convention as export_percentiles_csv.py's latest_only), for each of today's
565 communes and each of its two peer lists (national, region) read from
public/data/metadata/peers.json:

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

Writes public/data/peers/<nis>.json, one per current commune:

  {model_version, variant, nis_code,
   lists: {national: {IND: {period, value, peer_median, peers_with_value,
                             position, of, deviation_pct,
                             [deviation_withheld], selection_variable}},
           region: {...}}}

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
CROSS_SECTIONAL_FUNCTIONS = {"percentile", "z_score"}

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


def _index_rows(rows: list[dict]) -> dict[tuple[str, str], dict[str, tuple[float | None, str]]]:
    """(indicator_code, period) -> {nis_code: (value_or_None, status)},
    same shape as export_peer_model.py's _index_by_indicator_period."""
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


def _indicator_names(rows: list[dict]) -> dict[str, str]:
    """indicator_code -> its English name, read verbatim from the first row
    seen for it (every row for an indicator carries the same indicator_name).
    Used only for derived indicators the history rows never carry a name
    for; raw indicator names come from the rows themselves."""
    names: dict[str, str] = {}
    for row in rows:
        names.setdefault(row["indicator_code"], row["indicator_name"])
    return names


def build_derived_rows(rows: list[dict], derived_dir: Path) -> tuple[list[dict], dict[str, str]]:
    """Every derived indicator's (nis, period, value) as history-row-shaped
    dicts, computed by the Block G engine over the raw rows -- same pattern
    export_percentiles_csv.py's _derived_values uses, cross-sectional
    functions excluded.

    Returns (rows, names) where `rows` have status "derived" (a real value)
    for every cell the engine produced (missing/None cells are omitted, not
    written as a null row), and `names` maps each derived indicator id to
    its English name from its own YAML (config/indicators/derived's "name.en").
    """
    present = {row["indicator_code"] for row in rows}
    configs = load_and_validate_derived(derived_dir, present) if derived_dir.is_dir() else {}
    configs = {
        indicator_id: cfg
        for indicator_id, cfg in configs.items()
        if cfg["derived"]["function"] not in CROSS_SECTIONAL_FUNCTIONS
    }
    if not configs:
        return [], {}

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
    return derived_rows, names


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


def build_benchmarks(
    communes: set[str],
    index: dict[tuple[str, str], dict[str, tuple[float | None, str]]],
    latest_period: dict[str, str],
    peers_model: dict,
) -> dict[str, dict]:
    """nis -> {model_version, variant, nis_code, lists: {national: {...}, region: {...}}}."""
    model_version = peers_model["model_version"]
    variant = peers_model["variant"]
    peer_lists = peers_model["communes"]

    result: dict[str, dict] = {}
    for nis in sorted(communes):
        lists_payload: dict[str, dict] = {"national": {}, "region": {}}
        for list_name in ("national", "region"):
            peer_nis_list = [entry["nis"] for entry in peer_lists[nis][list_name]]
            for indicator_id, period in latest_period.items():
                bucket = index.get((indicator_id, period), {})
                own_value, own_status = bucket.get(nis, (None, None))
                if own_value is None:
                    continue  # no usable value for this commune -- nothing to benchmark
                peer_values = [bucket.get(peer_nis, (None, None))[0] for peer_nis in peer_nis_list]
                stats = peer_stats(own_value, peer_values)
                if stats["peer_median"] is None:
                    continue  # entirely null result -- omit rather than write nulls
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
    derived_rows, _derived_names = build_derived_rows(raw_rows, args.derived_dir)

    all_rows = raw_rows + derived_rows
    index = _index_rows(all_rows)
    latest_period = _latest_period_per_indicator(index)

    payloads = build_benchmarks(communes, index, latest_period, peers_model)
    write_payloads(payloads, args.out_dir)

    total_bytes = sum((args.out_dir / f"{nis}.json").stat().st_size for nis in communes)
    largest = max(communes, key=lambda nis: (args.out_dir / f"{nis}.json").stat().st_size)
    largest_size = (args.out_dir / f"{largest}.json").stat().st_size
    print(f"wrote {len(communes)} files to {args.out_dir} ({total_bytes / 1e6:.3f} MB total)")
    print(f"largest: {largest}.json ({largest_size} bytes)")


if __name__ == "__main__":
    main()
