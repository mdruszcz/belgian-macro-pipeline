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
from pathlib import Path

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


def _latest_rows_from_csv(
    csv_path: Path, current_communes: set[str], indicator_meta: dict[str, tuple[str, str]]
) -> list[tuple]:
    """Rows from a committed observations CSV, filtered by the same two rules
    the SQL query applies: current communes only, and only the most recent
    period per (geo_id, indicator).

    Manual-only sources live in CSV rather than in the daily-committed
    database (docs/decisions/0002-split-committed-stores.md), so this export
    has to read both. Indicator name/unit still come from the `indicators`
    table -- those reference rows stay in the database even when their
    observations do not, so there is exactly one metadata path.
    """
    if not csv_path.is_file():
        raise FileNotFoundError(
            f"--extra-observations {csv_path} does not exist. Refusing to silently "
            "export a commune file missing that source's indicators."
        )

    best: dict[tuple[str, str], dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["is_latest"] != "1" or row["geo_id"] not in current_communes:
                continue
            key = (row["geo_id"], row["indicator_id"])
            if key not in best or row["period"] > best[key]["period"]:
                best[key] = row

    missing = sorted({k[1] for k in best if k[1] not in indicator_meta})
    if missing:
        raise ValueError(
            f"{csv_path.name} references indicator(s) absent from the `indicators` "
            f"table: {missing}. Refusing to guess their name and unit."
        )

    out = []
    for (geo_id, indicator_id), row in best.items():
        name_en, unit = indicator_meta[indicator_id]
        out.append(
            (
                geo_id,
                indicator_id,
                name_en,
                unit,
                row["period"],
                float(row["value"]) if row["value"] != "" else None,
                row["status"],
                row["created_at"],
            )
        )
    return out


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
        for csv_path in extra_observations:
            obs = obs + _latest_rows_from_csv(csv_path, set(commune_by_id), indicator_meta)
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
            "fetched automatically and so are not in the database. Repeatable."
        ),
    )
    args = ap.parse_args()
    n = export_communes_csv(
        Path(args.db), Path(args.out), tuple(Path(p) for p in args.extra_observations)
    )
    print(f"Exported {n} rows to {args.out}")


if __name__ == "__main__":
    main()
