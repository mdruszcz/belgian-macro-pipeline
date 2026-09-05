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
import sqlite3
from pathlib import Path

STATUS_TO_LETTER = {
    "final": "A",
    "provisional": "P",
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


def export_communes_csv(db_path: Path, out_path: Path) -> int:
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
    conn.close()

    commune_by_id = {geo_id: (nis, en, fr, nl) for geo_id, nis, en, fr, nl in communes}

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        f.write(
            "geo_id,nis_code,name_en,name_fr,name_nl,region,province,arrondissement,"
            "indicator_code,indicator_name,unit,period,value,status,fetched_at\n"
        )
        for geo_id, indicator_id, ind_name, unit, period, value, status, created_at in obs:
            nis, name_en, name_fr, name_nl = commune_by_id[geo_id]
            a = ancestors.get(geo_id, {})
            region = a.get("region", "")
            province = a.get("province", "")
            arrondissement = a.get("arrondissement", "")
            obs_status = STATUS_TO_LETTER.get(status, status or "")
            f.write(
                f"{geo_id},{nis},{name_en},{name_fr},{name_nl},{region},{province},"
                f"{arrondissement},{indicator_id},{ind_name},{unit},{period},{value},"
                f"{obs_status},{created_at}\n"
            )
    return len(obs)


def main() -> None:
    ap = argparse.ArgumentParser(description="Export municipal observations to communes.html's CSV")
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file")
    ap.add_argument("--out", default="data/communes_export.csv", help="Output CSV path")
    args = ap.parse_args()
    n = export_communes_csv(Path(args.db), Path(args.out))
    print(f"Exported {n} rows to {args.out}")


if __name__ == "__main__":
    main()
