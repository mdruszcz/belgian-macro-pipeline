"""Compute arrondissement / province / region / Belgium aggregates from the
commune observations and export them -- Block L, docs/features/comparison.md.

Sibling to export_communes_history_csv.py, and deliberately a separate file
for the same reason that one is separate from export_communes_csv.py: its
output has a different contract (one row per aggregate geography, carrying
coverage columns the commune exports do not have), and merging them would
force one to compromise for the other.

THE TWO TRAPS, both measured before this was written:

1. The engine must see the UNRESTRICTED observation set, including communes
   merged away since. 13 of today's 565 communes have no 2023 fiscal row
   because the Statbel file predates the 2025 merger wave -- and they include
   Hasselt. Feeding it the current-communes view puts Limburg's 2023 taxable
   income at EUR 16.076bn instead of EUR 21.278bn: 24.4% short, and entirely
   plausible on screen. Same two-stage shape export_communes_history_csv.py
   already uses -- compute wide, display narrow.

2. The coverage denominator is a commune universe derived from validity
   windows, NOT the set of communes that happen to have a value. The latter
   would make coverage 100% by construction and hide precisely the gap it
   exists to report. Getting that universe right took two corrections, both
   caught by measurement and both pinned by tests, because both produced
   plausible numbers rather than errors:

   * it is PER INDICATOR, not just per period. The fiscal file expresses
     every year from 2005 to 2023 on the 2019 commune map, so its 2005 rows
     name 18 communes that did not exist then; measured against 2005's real
     589 that gave coverage above 100% for 283 cells. A source pinned to one
     map is measured against that map -- detected from the data, not a list.
   * it anchors to the START of the period with valid_to EXCLUSIVE. Comparing
     years alone put 607 communes in 2019 and 592 in 2025, neither of which
     ever existed, by counting both the predecessors leaving and the
     successors arriving on the same January 1st.

   Verified: the denominators now reproduce 589 (2016-2018), 581 (2019-2024)
   and 565 (2025-2026) -- the counts Block C corroborated against Statbel's
   own files -- and no aggregate reports impossible coverage.
"""

import argparse
import csv
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.analytics.aggregate import (  # noqa: E402
    DEFAULT_MIN_COVERAGE,
    aggregate,
    methods_from_metadata,
)
from src.analytics.engine import ObservationSet  # noqa: E402
from src.validation.config_schema import load_and_validate_derived  # noqa: E402

DEFAULT_DERIVED_DIR = Path(__file__).resolve().parents[1] / "config" / "indicators" / "derived"


def _geography(conn: sqlite3.Connection):
    """parents, levels, names and validity windows for every geography --
    including historical ones, which is what lets a predecessor commune's
    value reach its province for the period it existed in."""
    parents: dict[str, str | None] = {}
    levels: dict[str, str] = {}
    names: dict[str, tuple[str, str, str]] = {}
    windows: dict[str, tuple[str, str | None]] = {}
    nis: dict[str, str | None] = {}
    for geo_id, level, parent, nis_code, en, fr, nl, valid_from, valid_to in conn.execute(
        "SELECT geo_id, level, parent_geo_id, nis_code, name_en, name_fr, name_nl, "
        "valid_from, valid_to FROM geographies"
    ):
        parents[geo_id] = parent
        levels[geo_id] = level
        names[geo_id] = (en, fr, nl)
        windows[geo_id] = (valid_from, valid_to)
        nis[geo_id] = nis_code
    return parents, levels, names, windows, nis


def _period_start(period: str) -> str:
    """First calendar day of a period, as an ISO date.

    Handles the annual and quarterly forms the municipal data actually uses
    (docs/features/data_model.md); anything else is returned as-is so an
    unexpected format fails visibly downstream rather than being coerced into
    a wrong date.
    """
    if re.fullmatch(r"\d{4}", period):  # 2023
        return f"{period}-01-01"
    if re.fullmatch(r"\d{4}-Q[1-4]", period):  # 2023-Q4
        year, quarter = period.split("-Q")
        return f"{year}-{(int(quarter) - 1) * 3 + 1:02d}-01"
    if re.fullmatch(r"\d{4}-\d{2}", period):  # 2023-10
        return f"{period}-01"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", period):  # already a date
        return period
    # Matched on SHAPE, not length: an earlier version tested len() == 7 and
    # so turned "garbage" into "garbage-01" -- the exact silent coercion this
    # is meant to avoid. Returned unchanged, to fail visibly downstream.
    return period


def _universe_resolver(
    levels: dict[str, str],
    windows: dict[str, tuple[str, str | None]],
    rows: list[tuple[str, str, str, float]],
):
    """Build the coverage denominator: which communes each indicator covers in
    each period.

    Normally that is the calendar truth -- the communes valid in that period,
    from their validity windows. But two of today's sources do not express
    themselves in the calendar's geography:

      * the Statbel fiscal file back-casts the 2019 commune map across every
        year from 2005 to 2023 (documented in Block F), so its 2005 rows name
        18 communes that did not exist until 2019;
      * LOCAL_UNITS_BY_COMMUNE reports its 2023-Q4 snapshot on today's 565.

    Measured, dividing one map by the other produced coverage above 100% for
    283 cells. So a PINNED indicator -- one whose geo_ids are not a subset of
    the calendar universe in some period -- is measured against its own
    vintage instead: the full set of communes it ever reports on. That still
    detects a real gap (a year missing communes scores below 100%) without
    comparing two different maps.

    Detected from the data, not from a hand-maintained list, so a new
    back-casting source is handled the day it lands rather than the day
    somebody notices.
    """
    valid_by_period: dict[str, set[str]] = {}

    def valid(period: str) -> set[str]:
        """Communes valid at the START of the period, with valid_to exclusive.

        BOTH halves of that were measured, not assumed. Comparing years only
        put 607 communes in 2019 and 592 in 2025 -- neither ever existed --
        because a merger year matched the predecessors leaving (valid_to
        2019-01-01) AND the successors arriving (valid_from 2019-01-01), so
        the boundary counted twice. Treating valid_to as exclusive and
        comparing full dates gives the real 581 and 565.

        The START of the period, not the end, because that is the moment the
        sources describe: Statbel publishes population as of 1 January. It
        matters for exactly one case in the data -- Bastogne and Bertogne
        merged on 2024-12-02, so 2024 has 581 communes at its start and 580 at
        its end, and the population file for 2024 carries 581. Anchoring to
        the end would put coverage above 100% for that year.
        """
        if period not in valid_by_period:
            start = _period_start(period)
            valid_by_period[period] = {
                geo_id
                for geo_id, level in levels.items()
                if level == "municipality"
                and windows[geo_id][0] <= start
                and (windows[geo_id][1] is None or windows[geo_id][1] > start)
            }
        return valid_by_period[period]

    observed: dict[str, dict[str, set[str]]] = {}
    for indicator_id, geo_id, period, _value in rows:
        observed.setdefault(indicator_id, {}).setdefault(period, set()).add(geo_id)

    pinned: dict[str, set[str]] = {}
    for indicator_id, by_period in observed.items():
        if any(not geos <= valid(period) for period, geos in by_period.items()):
            pinned[indicator_id] = {g for geos in by_period.values() for g in geos}

    def universe_of(indicator_id: str, period: str) -> set[str]:
        return pinned.get(indicator_id) or valid(period)

    return universe_of, sorted(pinned)


def _observations(conn: sqlite3.Connection, extra_csvs: tuple[Path, ...]):
    """Every is_latest municipal observation across both committed stores,
    UNRESTRICTED by current-commune status. See trap 1."""
    rows: list[tuple[str, str, str, float]] = []
    for indicator_id, geo_id, period, value in conn.execute(
        "SELECT o.indicator_id, o.geo_id, o.period, o.value FROM observations o "
        "JOIN geographies g ON g.geo_id = o.geo_id AND g.level = 'municipality' "
        "WHERE o.is_latest = 1 AND o.value IS NOT NULL"
    ):
        rows.append((indicator_id, geo_id, period, float(value)))

    for path in extra_csvs:
        if not path.is_file():
            raise FileNotFoundError(
                f"--extra-observations {path} does not exist. Refusing to compute "
                "aggregates that silently omit that source's indicators."
            )
        with path.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                if row["is_latest"] != "1" or row["value"] == "":
                    continue
                rows.append(
                    (row["indicator_id"], row["geo_id"], row["period"], float(row["value"]))
                )
    return rows


def export_aggregates_csv(
    db_path: Path,
    out_path: Path,
    extra_observations: tuple[Path, ...] = (),
    derived_dir: Path = DEFAULT_DERIVED_DIR,
    min_coverage: float = DEFAULT_MIN_COVERAGE,
) -> int:
    conn = sqlite3.connect(str(db_path))
    parents, levels, names, windows, nis = _geography(conn)

    indicator_meta = {
        indicator_id: {"is_additive": bool(is_additive), "name_en": name_en, "unit": unit}
        for indicator_id, is_additive, name_en, unit in conn.execute(
            "SELECT indicator_id, is_additive, name_en, unit FROM indicators"
        )
    }

    rows = _observations(conn, extra_observations)
    conn.close()

    obs = ObservationSet(rows)
    universe_of, pinned = _universe_resolver(levels, windows, rows)
    if pinned:
        print(
            "Coverage measured against each source's own commune vintage for: "
            + ", ".join(pinned)
            + " (these express every period on one fixed map -- see _universe_resolver)"
        )

    present = {indicator_id for indicator_id, _g, _p, _v in rows}
    derived_cfgs = load_and_validate_derived(derived_dir, present) if derived_dir.is_dir() else {}

    # Only indicators actually present, so an unrelated config cannot make
    # this refuse. methods_from_metadata decides SUM / RECOMPUTE / REFUSE from
    # metadata rather than a hardcoded list; REFUSE entries are dropped here
    # instead of raising, because refusing is the CORRECT answer for a
    # percentile or a growth rate -- they are simply not aggregated this way.
    methods = methods_from_metadata(
        {i: m for i, m in indicator_meta.items() if i in present}, derived_cfgs
    )
    methods = {i: m for i, m in methods.items() if m != "refuse"}

    result = aggregate(
        obs, methods, parents, levels, universe_of, derived_cfgs, min_coverage=min_coverage
    )

    display_names = {
        **{i: (m["name_en"], m["unit"]) for i, m in indicator_meta.items()},
        **{i: (c["name"]["en"], c["unit"]) for i, c in derived_cfgs.items()},
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(
            [
                "geo_id",
                "nis_code",
                "level",
                "name_en",
                "name_fr",
                "name_nl",
                "indicator_code",
                "indicator_name",
                "unit",
                "period",
                "value",
                "coverage_n",
                "coverage_of",
                "coverage_pct",
            ]
        )
        for indicator_id, geo_id, period in result.cells():
            value = result.value(indicator_id, geo_id, period)
            cov = result.coverage(indicator_id, geo_id, period)
            name_en, unit = display_names.get(indicator_id, (indicator_id, ""))
            en, fr, nl = names[geo_id]
            writer.writerow(
                [
                    geo_id,
                    nis[geo_id] or "",
                    levels[geo_id],
                    en,
                    fr,
                    nl,
                    indicator_id,
                    name_en,
                    unit,
                    period,
                    value,
                    cov.contributed,
                    cov.expected,
                    round(cov.pct, 1),
                ]
            )
            written += 1
    return written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export province/region/Belgium aggregates of the municipal observations"
    )
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", default="data/aggregates.csv")
    ap.add_argument("--extra-observations", action="append", default=[], metavar="CSV")
    ap.add_argument("--derived-dir", type=Path, default=DEFAULT_DERIVED_DIR)
    ap.add_argument(
        "--min-coverage",
        type=float,
        default=DEFAULT_MIN_COVERAGE,
        help="Suppress an aggregate below this share of its communes (default 0.90)",
    )
    args = ap.parse_args()
    n = export_aggregates_csv(
        Path(args.db),
        Path(args.out),
        tuple(Path(p) for p in args.extra_observations),
        args.derived_dir,
        args.min_coverage,
    )
    print(f"Exported {n} aggregate rows to {args.out}")


if __name__ == "__main__":
    main()
