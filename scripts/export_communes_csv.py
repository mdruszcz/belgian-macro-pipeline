"""
Export the canonical schema's latest municipal-level observations to a flat
CSV for communes.html -- the commune-data browser, sibling to all_data.html
(which covers be:country only via belgian_macro_export.csv).

One row per (commune, indicator): geo_id, nis_code, three-language name, the
region/province/arrondissement it sits under (walked from parent_geo_id, not
hardcoded -- Brussels communes have no province, see docs/features/geography.md
Q3, so the walk must tolerate a missing level rather than assume four hops),
indicator_code, indicator_name, unit, period, value, status, fetched_at.

Long format, not one-row-per-commune: today there is exactly one municipal
indicator (LOCAL_UNITS_BY_COMMUNE, Block F), but a second one should not
require changing this script or the page -- communes.html pivots indicators
into columns client-side from whatever indicator_codes actually appear.
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analytics.backaggregate import reconstruct  # noqa: E402
from src.stores import DEFAULT_STORES_PATH, resolve_extra_observations  # noqa: E402

# The five statuses migrations/001_core_schema.sql permits, all mapped, so
# none can fall through to its raw name in a published column. "S" arrived
# with ONEM, the first source that publishes privacy-masked cells: those are
# stored with a NULL value and status "suppressed" rather than as zero, so the
# letter has to distinguish "we know this is small and may not say" from
# "we have no reading". "revised" and "estimate" were already reachable from
# the observations table and were falling through unmapped.
STATUS_TO_LETTER = {
    "final": "A",
    "provisional": "P",
    "revised": "R",
    "estimate": "E",
    "suppressed": "S",
    "na": "N",
}


def _ancestor_names(conn: sqlite3.Connection, geo_id: str) -> dict:
    """Walk parent_geo_id up from a commune, keyed by level. Brussels
    communes have no province row (their arrondissement parents straight to
    the region), so this returns whatever levels actually exist rather than
    assuming a fixed depth."""
    names = {}
    current = geo_id
    seen = set()
    while current and current not in seen:
        seen.add(current)
        row = conn.execute(
            "SELECT level, name_en, parent_geo_id FROM geographies WHERE geo_id = ?",
            (current,),
        ).fetchone()
        if not row:
            break
        level, name_en, parent = row
        if level != "municipality":
            names[level] = name_en
        current = parent
    return names


def _all_rows_from_csv(csv_path: Path, indicator_meta: dict[str, tuple[str, str]]) -> list[tuple]:
    """Every is_latest=1 row in a committed observations CSV -- every period,
    every geo_id, no current-communes filter and no per-indicator "most
    recent period" collapse.

    `reconstruct()` needs this full-history shape (see its docstring at
    src/analytics/backaggregate.py:547-551): it reads a successor's own rows
    at every period to decide whether a cell is a genuine gap, not just its
    single latest snapshot. Handing it a latest-only view hides a real older
    row behind a newer one from ANOTHER indicator-period, which is exactly
    the Antwerp/Borsbeek bug this function exists to avoid: Antwerp kept its
    own pre-merger geo_id (be:mun:11002), so with a latest-only input its
    2024 real population (544,759) was invisible to the gap-fill check and
    Borsbeek's 11,379 was reconstructed and published in its place.

    Mirrors export_communes_history_csv.py's `_all_rows_from_csv` exactly
    (same row shape, same is_latest filter) -- kept as a separate copy here
    rather than imported, since the two scripts have no shared module today
    and this one is five lines.
    """
    if not csv_path.is_file():
        raise FileNotFoundError(
            f"--extra-observations {csv_path} does not exist. Refusing to silently "
            "export a commune file missing that source's indicators."
        )

    out = []
    missing: set[str] = set()
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["is_latest"] != "1":
                continue
            indicator_id = row["indicator_id"]
            if indicator_id not in indicator_meta:
                missing.add(indicator_id)
                continue
            name_en, unit = indicator_meta[indicator_id]
            out.append(
                (
                    row["geo_id"],
                    indicator_id,
                    name_en,
                    unit,
                    row["period"],
                    float(row["value"]) if row["value"] != "" else None,
                    row["status"],
                    row["created_at"],
                )
            )
    if missing:
        raise ValueError(
            f"{csv_path.name} references indicator(s) absent from the `indicators` "
            f"table: {sorted(missing)}. Refusing to guess their name and unit."
        )
    return out


def _latest_per_cell(rows: list[tuple]) -> list[tuple]:
    """Collapse a list of (geo_id, indicator_id, ..., period, value, status,
    created_at) rows to one per (geo_id, indicator_id): the most recent
    period, and a real (non-reconstructed) row beating a reconstructed one
    at the SAME period -- never sort/insertion order.

    This is the "most recent period per (geo_id, indicator)" contract
    communes_export.csv promises (module docstring, line 6) applied AFTER
    merger reconstruction adds its own rows to the set, not just to the raw
    per-source rows before reconstruction runs. Without this second pass, a
    reconstructed row for an old gap-filled period could still sort after a
    successor's own newer real row and win on insertion order alone.
    """
    best: dict[tuple[str, str], tuple] = {}
    for row in rows:
        key = (row[0], row[1])
        period = row[4]
        current = best.get(key)
        if current is None:
            best[key] = row
            continue
        current_period = current[4]
        if period > current_period:
            best[key] = row
        elif period == current_period:
            # Real beats reconstructed at the same period, regardless of
            # which one arrived first.
            if current[6] == "reconstructed" and row[6] != "reconstructed":
                best[key] = row
    # Deterministic order: by geo_id then indicator_id, never dict/insertion
    # order (rule 35 -- identical inputs must keep producing byte-identical
    # output).
    return [best[key] for key in sorted(best)]


def export_communes_csv(
    db_path: Path, out_path: Path, extra_observations: tuple[Path, ...] = ()
) -> int:
    conn = sqlite3.connect(str(db_path))
    communes = conn.execute(
        "SELECT geo_id, nis_code, name_en, name_fr, name_nl FROM geographies "
        "WHERE level = 'municipality' AND valid_to IS NULL ORDER BY nis_code"
    ).fetchall()
    ancestors = {geo_id: _ancestor_names(conn, geo_id) for geo_id, *_ in communes}

    # Two filters that matter now that a municipal indicator can have real
    # multi-year history (POPULATION_BY_COMMUNE, 11 periods) rather than a
    # single point (LOCAL_UNITS_BY_COMMUNE):
    #   1. Only CURRENT communes -- a pre-merger year's population resolves
    #      to the historical predecessor's own geo_id (resolve_geo is
    #      period-aware, see sync_population.py), which is correct for the
    #      canonical model but must not surface here as a phantom extra
    #      "commune" that no longer exists.
    #   2. Only the MOST RECENT period per (geo_id, indicator) -- is_latest
    #      marks "not superseded by a revision", not "the newest period", so
    #      an indicator with real history has many is_latest=1 rows at once.
    #      This page shows current commune data, one row per commune; a
    #      time-series view is a different, future feature.
    obs = conn.execute("""
        SELECT o.geo_id, o.indicator_id, i.name_en, i.unit, o.period, o.value,
               o.status, o.created_at
        FROM observations o
        JOIN indicators i ON o.indicator_id = i.indicator_id
        JOIN geographies g ON g.geo_id = o.geo_id
            AND g.level = 'municipality' AND g.valid_to IS NULL
        WHERE o.is_latest = 1
          AND o.period = (
              SELECT MAX(o2.period) FROM observations o2
              WHERE o2.indicator_id = o.indicator_id
                AND o2.geo_id = o.geo_id AND o2.is_latest = 1
          )
        ORDER BY o.geo_id, o.indicator_id
        """).fetchall()

    commune_by_id = {geo_id: (nis, en, fr, nl) for geo_id, nis, en, fr, nl in communes}

    if extra_observations:
        indicator_meta = {
            row[0]: (row[1], row[2])
            for row in conn.execute("SELECT indicator_id, name_en, unit FROM indicators")
        }
        indicator_is_additive = {
            row[0]: bool(row[1])
            for row in conn.execute("SELECT indicator_id, is_additive FROM indicators")
        }
        lineage_rows = conn.execute(
            "SELECT geo_id, valid_to, successor_geo_id FROM geographies "
            "WHERE valid_to IS NOT NULL AND successor_geo_id IS NOT NULL"
        ).fetchall()

        # Merger back-aggregation reads EVERY is_latest=1 row (every period,
        # not just each geo_id's newest) from each extra CSV, since that is
        # the only place these four sources' observations live
        # (src/analytics/backaggregate.py never opens a file itself).
        # reconstruct() needs the successor's own full history to tell a
        # genuine gap from a period a latest-only view would have hidden --
        # see `_all_rows_from_csv`'s docstring for the Antwerp/Borsbeek bug
        # this avoids. Only the gap-filled result is added to `obs`; a
        # successor's own value is never touched (see reconstruct()'s
        # gap-fill contract).
        for csv_path in extra_observations:
            all_rows = _all_rows_from_csv(csv_path, indicator_meta)
            reconstructed = reconstruct(
                all_rows,
                lineage_rows,
                indicator_is_additive=indicator_is_additive,
                indicator_meta=indicator_meta,
                # This exporter has no derived-config pass of its own (no
                # AVG_NET_TAXABLE_INCOME-style ratio is computed here at
                # all, live or reconstructed) -- only the raw SUM indicators
                # this CSV carries are reconstructable from it.
                derived_configs=None,
            )
            obs = obs + [row for row in all_rows if row[0] in commune_by_id] + reconstructed
        # Re-apply "most recent period per (geo_id, indicator)" across own +
        # reconstructed rows together, now that a CSV can contribute more
        # than one period per cell -- a real row wins over a reconstructed
        # one at the same period, never sort/insertion order (see
        # `_latest_per_cell`).
        obs = _latest_per_cell(obs)
    else:
        obs.sort(key=lambda r: (r[0], r[1]))

    conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    # csv.writer, not f-strings -- see export_canonical_csv.py for why. No
    # commune name contains a comma today, but indicator display names do.
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            [
                "geo_id",
                "nis_code",
                "name_en",
                "name_fr",
                "name_nl",
                "region",
                "province",
                "arrondissement",
                "indicator_code",
                "indicator_name",
                "unit",
                "period",
                "value",
                "status",
                "fetched_at",
            ]
        )
        for geo_id, indicator_id, ind_name, unit, period, value, status, created_at in obs:
            nis, name_en, name_fr, name_nl = commune_by_id[geo_id]
            a = ancestors.get(geo_id, {})
            writer.writerow(
                [
                    geo_id,
                    nis,
                    name_en,
                    name_fr,
                    name_nl,
                    a.get("region", ""),
                    a.get("province", ""),
                    a.get("arrondissement", ""),
                    indicator_id,
                    ind_name,
                    unit,
                    period,
                    value,
                    STATUS_TO_LETTER.get(status, status or ""),
                    created_at,
                ]
            )
    return len(obs)


def main() -> None:
    ap = argparse.ArgumentParser(description="Export municipal observations to communes.html's CSV")
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file")
    ap.add_argument("--out", default="data/communes_export.csv", help="Output CSV path")
    ap.add_argument(
        "--extra-observations",
        action="append",
        default=[],
        metavar="CSV",
        help=(
            "Committed observations CSV to merge in, for sources that cannot be "
            "fetched automatically and so are not in the database. Repeatable. "
            "Takes priority over --stores when given (see resolve_extra_observations)."
        ),
    )
    ap.add_argument(
        "--stores",
        default=str(DEFAULT_STORES_PATH),
        metavar="YAML",
        help=(
            "config/stores.yaml -- read for its extra_csv stores' paths when no "
            "--extra-observations is given. Pass '' to merge nothing by default."
        ),
    )
    args = ap.parse_args()
    n = export_communes_csv(
        Path(args.db),
        Path(args.out),
        resolve_extra_observations(args.extra_observations, args.stores),
    )
    print(f"Exported {n} rows to {args.out}")


if __name__ == "__main__":
    main()
