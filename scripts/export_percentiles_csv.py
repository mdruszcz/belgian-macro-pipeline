"""Where each commune sits among its peers, nationally and within its own
region -- Block L, docs/features/comparison.md.

Separate from export_aggregates_csv.py because it answers a different
question about the same data. An aggregate asks "what is the total for this
province"; a percentile asks "where does this commune rank". They share the
observation-loading rules and nothing else, and their output contracts differ
(one row per geography here is a commune, not an ancestor).

THE PEER SET IS THE WHOLE RISK. Three rules, each measured:

1. Peers are the communes that existed IN THAT PERIOD -- 589 in 2016-2018,
   581 in 2019-2024, 565 from 2025. Ranking a 2016 commune against today's
   565 uses a denominator 24 too small. Block G established this for the
   national percentile and it holds identically for a regional one.

2. A REGIONAL peer set is the communes of that commune's own region, and
   region membership is taken from the period's own geography, walking the
   canonicalised parent chain. Brussels-Capital has 19 communes, so its
   regional percentile is withheld -- see rule 3.

3. Below 30 peers a percentile is not published, only the rank. Over 19
   items one rank step is 5.26 percentile points; "73.7th percentile" from 19
   observations reads as measured precision and is not.
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from export_aggregates_csv import (  # noqa: E402
    DEFAULT_DERIVED_DIR,
    _geography,
    _observations,
    canonical,
)

from src.analytics.aggregate import ancestors_of  # noqa: E402
from src.analytics.ranking import MIN_PEERS_FOR_PERCENTILE, position  # noqa: E402
from src.validation.config_schema import load_and_validate_derived  # noqa: E402


def _derived_values(rows, derived_dir: Path):
    """Commune-level derived indicators (average income, dependency ratio...),
    computed the same way the commune exports compute them.

    A percentile of a derived figure is exactly what a reader wants -- income
    per tax return ranks communes meaningfully, where a raw total mostly ranks
    them by size -- so they cannot be left out.
    """
    from src.analytics.engine import ObservationSet, compute

    present = {indicator_id for indicator_id, _g, _p, _v in rows}
    configs = load_and_validate_derived(derived_dir, present) if derived_dir.is_dir() else {}
    # Cross-sectional configs are excluded: a percentile OF a percentile is
    # meaningless, and POPULATION_PERCENTILE is what this script replaces.
    configs = {
        i: c
        for i, c in configs.items()
        if c["derived"]["function"] not in {"percentile", "z_score"}
    }
    if not configs:
        return []
    result = compute(ObservationSet(rows), configs, present)
    out = []
    for indicator_id in configs:
        for geo_id, period in result.cells(indicator_id):
            value = result.value(indicator_id, geo_id, period)
            if value is not None:
                out.append((indicator_id, geo_id, period, value))
    return out


def export_percentiles_csv(
    db_path: Path,
    out_path: Path,
    extra_observations: tuple[Path, ...] = (),
    derived_dir: Path = DEFAULT_DERIVED_DIR,
    min_peers: int = MIN_PEERS_FOR_PERCENTILE,
    latest_only: bool = True,
) -> int:
    conn = sqlite3.connect(str(db_path))
    parents, levels, names, _windows, nis = _geography(conn)
    current_communes = {
        canonical(row[0])
        for row in conn.execute(
            "SELECT geo_id FROM geographies WHERE level = 'municipality' AND valid_to IS NULL"
        )
    }
    indicator_names = dict(conn.execute("SELECT indicator_id, name_en FROM indicators"))
    rows = _observations(conn, extra_observations)
    conn.close()

    derived_cfgs = (
        load_and_validate_derived(derived_dir, {i for i, _g, _p, _v in rows})
        if derived_dir.is_dir()
        else {}
    )
    rows = rows + _derived_values(rows, derived_dir)
    for indicator_id, cfg in derived_cfgs.items():
        indicator_names.setdefault(indicator_id, cfg["name"]["en"])

    # region per commune, from the canonicalised parent chain. A commune with
    # no region (none today) simply gets no regional position rather than
    # being lumped into a default.
    region_of: dict[str, str | None] = {}
    for geo_id, level in levels.items():
        if level != "municipality":
            continue
        region_of[geo_id] = next(
            (a for a in ancestors_of(geo_id, parents, levels) if levels.get(a) == "region"),
            None,
        )

    # peer values per (indicator, period) and per (indicator, period, region)
    national: dict[tuple[str, str], dict[str, float]] = {}
    regional: dict[tuple[str, str, str], dict[str, float]] = {}
    for indicator_id, geo_id, period, value in rows:
        if levels.get(geo_id) != "municipality":
            continue
        national.setdefault((indicator_id, period), {})[geo_id] = value
        region = region_of.get(geo_id)
        if region:
            regional.setdefault((indicator_id, period, region), {})[geo_id] = value

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0

    # LATEST PERIOD ONLY, by default. Every historical period computes fine
    # and the full history came to 174,034 rows / 22 MB -- against a 25 MB
    # per-file commit guard, re-committed daily, for periods no page displays.
    # The percentile component shows one figure per indicator at its latest
    # period, so that is what is exported; --all-periods is there for a
    # researcher who wants the rest. Same latest-only contract as
    # communes_export.csv.
    if latest_only:
        newest: dict[str, str] = {}
        for indicator_id, period in national:
            if period > newest.get(indicator_id, ""):
                newest[indicator_id] = period
        national = {
            (indicator_id, period): peers
            for (indicator_id, period), peers in national.items()
            if newest[indicator_id] == period
        }

    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(
            [
                "geo_id",
                "nis_code",
                "name_en",
                "indicator_code",
                "indicator_name",
                "period",
                "value",
                "scope",
                "scope_name",
                "percentile",
                "rank",
                "peers",
            ]
        )
        for (indicator_id, period), peers in sorted(national.items()):
            region_peers_cache: dict[str, dict[str, float]] = {}
            for geo_id, value in sorted(peers.items()):
                # Only CURRENT communes get output rows; historical ones still
                # widen the peer set above. Same compute-wide/display-narrow
                # shape as export_communes_history_csv.py.
                if geo_id not in current_communes:
                    continue
                scopes = [("national", "Belgium", peers)]
                region = region_of.get(geo_id)
                if region:
                    if region not in region_peers_cache:
                        region_peers_cache[region] = regional.get(
                            (indicator_id, period, region), {}
                        )
                    scopes.append(("regional", names[region][0], region_peers_cache[region]))

                for scope, scope_name, peer_map in scopes:
                    pos = position(value, peer_map.values(), min_peers=min_peers)
                    if pos is None:
                        continue
                    writer.writerow(
                        [
                            geo_id,
                            nis[geo_id] or "",
                            names[geo_id][0],
                            indicator_id,
                            indicator_names.get(indicator_id, indicator_id),
                            period,
                            value,
                            scope,
                            scope_name,
                            "" if pos["pct"] is None else round(pos["pct"], 4),
                            pos["rank"],
                            pos["n"],
                        ]
                    )
                    written += 1
    return written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export national and regional peer positions for every commune indicator"
    )
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", default="data/percentiles.csv")
    ap.add_argument("--extra-observations", action="append", default=[], metavar="CSV")
    ap.add_argument("--derived-dir", type=Path, default=DEFAULT_DERIVED_DIR)
    ap.add_argument(
        "--all-periods",
        action="store_true",
        help="Export every period, not just each indicator's latest (22 MB on today's data)",
    )
    ap.add_argument(
        "--min-peers",
        type=int,
        default=MIN_PEERS_FOR_PERCENTILE,
        help="Withhold the percentile below this peer count, reporting only the rank",
    )
    args = ap.parse_args()
    n = export_percentiles_csv(
        Path(args.db),
        Path(args.out),
        tuple(Path(p) for p in args.extra_observations),
        args.derived_dir,
        args.min_peers,
        latest_only=not args.all_periods,
    )
    print(f"Exported {n} peer-position rows to {args.out}")


if __name__ == "__main__":
    main()
