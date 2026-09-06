"""
Export the canonical schema's latest observations (be:country, the 16
in-scope Belgium-only indicators) to the same 8-column CSV shape
dashboard.html already depends on:
    indicator_code,name,period,value,obs_status,unit,source_agency,fetched_at

`status` is mapped back to the single-letter convention the frontend already
branches on (final->A, provisional->P, everything else->'' -- the frontend
has no dedicated visual for estimate/revised/suppressed/na yet, so it falls
through to the existing generic '-' badge rather than a wrong one).

belgian_forecasts.csv is untouched -- forecasts stay on the legacy table,
out of scope per docs/decisions/0001-data-model.md.
"""

import argparse
import csv
import sqlite3
from pathlib import Path

STATUS_TO_LETTER = {
    "final": "A",
    "provisional": "P",
}


def status_to_obs_status(status: str) -> str:
    return STATUS_TO_LETTER.get(status, "")


def export_canonical_csv(db_path: Path, out_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute("""
        SELECT o.indicator_id, i.name_en, o.period, o.value, o.status,
               i.unit, s.agency, o.created_at
        FROM observations o
        JOIN indicators i ON o.indicator_id = i.indicator_id
        JOIN sources s ON i.source_id = s.source_id
        WHERE o.geo_id = 'be:country' AND o.is_latest = 1
        ORDER BY o.indicator_id, o.period
        """).fetchall()
    conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    # csv.writer, not f-strings: indicator display names legitimately contain
    # commas ("GDP volume index, Belgium (2010=100)"), and an unquoted comma
    # silently shifts every later column for that row. all_data.html parses
    # RFC 4180 properly, so quoting here is all that is needed.
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            [
                "indicator_code",
                "name",
                "period",
                "value",
                "obs_status",
                "unit",
                "source_agency",
                "fetched_at",
            ]
        )
        for indicator_id, name, period, value, status, unit, agency, created_at in rows:
            writer.writerow(
                [
                    indicator_id,
                    name,
                    period,
                    value,
                    status_to_obs_status(status),
                    unit,
                    agency,
                    created_at,
                ]
            )
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Export canonical observations to the dashboard CSV")
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file")
    ap.add_argument("--out", default="data/belgian_macro_export.csv", help="Output CSV path")
    args = ap.parse_args()
    n = export_canonical_csv(Path(args.db), Path(args.out))
    print(f"Exported {n} rows to {args.out}")


if __name__ == "__main__":
    main()
