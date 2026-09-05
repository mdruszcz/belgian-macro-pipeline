"""
Dump a set of indicators' observations to a canonical CSV -- the committed
store for data that can only ever be loaded by hand.

Why this exists: `statbel.fgov.be` is unreachable from CI, so the population
bulk files can only be downloaded and loaded manually. Keeping those rows in
the daily-committed SQLite file meant 95% of its bytes were near-static data
being re-committed every day, in a binary format that git cannot delta. See
docs/decisions/0002-split-committed-stores.md.

A CSV instead is text: git stores a small delta when one year is refreshed
rather than a fresh ~10 MB blob, and a data change shows up as readable line
diffs in the pull request rather than an opaque binary.

`fetch_run_id` is deliberately NOT exported. It is an audit id local to the
database that produced the row and means nothing outside it; the reverse
loader (scripts/load_observations_csv.py) opens its own run to satisfy the
NOT NULL foreign key.

Output is sorted by (indicator_id, geo_id, period, vintage) -- the
observations primary key -- so the file is byte-stable across runs and two
exports of unchanged data produce an empty diff.
"""

import argparse
import csv
import sqlite3
from pathlib import Path

COLUMNS = (
    "indicator_id",
    "geo_id",
    "period",
    "vintage",
    "value",
    "status",
    "period_start",
    "period_end",
    "is_latest",
    "created_at",
)


def export_observations(db_path: Path, out_path: Path, indicators: list[str]) -> int:
    conn = sqlite3.connect(str(db_path))
    placeholders = ",".join("?" for _ in indicators)
    rows = conn.execute(
        f"""
        SELECT {", ".join(COLUMNS)}
        FROM observations
        WHERE indicator_id IN ({placeholders})
        ORDER BY indicator_id, geo_id, period, vintage
        """,
        indicators,
    ).fetchall()
    conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(COLUMNS)
        writer.writerows(rows)
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    ap.add_argument("--db", required=True, help="Path to the SQLite DB to read from")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument(
        "--indicators",
        required=True,
        help="Comma-separated indicator_ids to export",
    )
    args = ap.parse_args()
    indicators = [i.strip() for i in args.indicators.split(",") if i.strip()]
    if not indicators:
        raise SystemExit("--indicators must name at least one indicator_id")
    n = export_observations(Path(args.db), Path(args.out), indicators)
    print(f"Exported {n} observations for {len(indicators)} indicator(s) to {args.out}")


if __name__ == "__main__":
    main()
