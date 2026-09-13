"""
Export the canonical schema's latest dashboard observations.

Belgium only: every row is a be:country observation. The file has no geography
column, and everything downstream of it reads each row as Belgian --
export_explorer_payloads.py writes each code's series under be:country, and
export_site_payloads.py puts it in national.json.

That is why the DE/FR/NL GDP "helper series" this export used to let through
for dashboard.html's INT_GDP_COMP formula are gone. They produced no rows for
as long as the daily sync dropped foreign series; once pipeline repair part 3
made them arrive, the explorer published Germany's GDP labelled be:country
(measured on the real data). The foreign series live in `observations` under
their own geography; a page that compares countries needs a payload that says
which country each series is, not a Belgian file with guests in it.

`status` is mapped back to the single-letter convention the frontend
branches on: final->A, provisional->P, revised->R, estimate->E,
suppressed->S, na->N -- the same six letters and the same meanings
communes.html's statusPill() and MAP_STATUS_WORDS already use for municipal
data (communes.html:483-493, communes.html:877-880), so the two vocabularies
cannot diverge (Batch 8a). Until Batch 8a this mapped only final/provisional
and sent everything else to '' -- the docstring said that was only because
"the frontend has no dedicated visual for estimate/revised/suppressed/na
yet". Batch 8a's explorer.html builds that visual, so the precondition is
gone. This CHANGES a published download: the 9 rows the database has as
'revised' (all 2009 annual figures, later superseded by a later vintage)
move from an empty obs_status cell to 'R'.

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
    "revised": "R",
    "estimate": "E",
    "suppressed": "S",
    "na": "N",
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
        JOIN geographies g ON o.geo_id = g.geo_id
        WHERE o.is_latest = 1
          AND o.geo_id = 'be:country'
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
