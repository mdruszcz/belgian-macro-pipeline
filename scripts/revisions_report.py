"""
Revision-detection report -- Block I, docs/features/vintages.md.

Lists every observation cell whose value changed, with the old and new value,
both vintages, and the delta. Two audiences for the same list: it is
analytically interesting on its own (a client's number moved and they should
know), and it is the early warning that a source changed methodology rather
than merely revised a number -- "a source that revises 900 cells at once has
not revised, it has redefined" (vintages.md).

Does NOT block the build. A revision is a legitimate event; failing on one
would be the same mistake Block H already avoided with LOCAL_UNITS_BY_COMMUNE
being three years stale -- a report that fails on the thing it exists to
report gets ignored within a week. The pathological case (a source returning
garbage rather than a real revision) is already covered by the row_collapse /
null_share validation rules, which DO block.
"""

import argparse
import sqlite3
import sys
from datetime import datetime, time, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.observations import parse_vintage  # noqa: E402

_COLUMNS = ("indicator_id", "geo_id", "period", "vintage", "value", "status")


def find_revisions(conn: sqlite3.Connection, since: datetime | None = None) -> list[dict]:
    """Every (indicator_id, geo_id, period) with more than one vintage,
    reduced to consecutive (previous, current) pairs, filtered to pairs
    where the CURRENT vintage is on or after `since`.

    Deliberately in Python rather than a SQL window function ordered by the
    vintage STRING: get_observations() already established that vintage
    must be compared as a parsed timestamp, not text, because a single row
    with a non-UTC offset would otherwise sort wrong. The store is small
    enough (thousands of rows, not millions) that grouping in Python costs
    nothing measurable.
    """
    rows = conn.execute(f"SELECT {', '.join(_COLUMNS)} FROM observations").fetchall()

    by_cell: dict[tuple[str, str, str], list[tuple[datetime, str]]] = {}
    for indicator_id, geo_id, period, vintage, value, status in rows:
        key = (indicator_id, geo_id, period)
        by_cell.setdefault(key, []).append((parse_vintage(vintage), vintage, value, status))

    revisions = []
    for (indicator_id, geo_id, period), versions in by_cell.items():
        versions.sort(key=lambda v: v[0])
        for prev, cur in zip(versions, versions[1:], strict=False):
            prev_dt, prev_vintage, prev_value, prev_status = prev
            cur_dt, cur_vintage, cur_value, cur_status = cur
            if since is not None and cur_dt < since:
                continue
            revisions.append(
                {
                    "indicator_id": indicator_id,
                    "geo_id": geo_id,
                    "period": period,
                    "old_value": prev_value,
                    "old_status": prev_status,
                    "old_vintage": prev_vintage,
                    "new_value": cur_value,
                    "new_status": cur_status,
                    "new_vintage": cur_vintage,
                }
            )

    def _sort_key(r: dict):
        rel = _relative_change(r["old_value"], r["new_value"])
        # Computable relative changes first, ranked by magnitude descending;
        # everything else (a suppression, a brand-new number with no
        # meaningful "percent change") after, in no particular order -- a
        # 0.1% GDP revision and a 40% one are not equally interesting, but a
        # None-vs-100 transition has no percentage to rank by at all.
        return (0, -abs(rel)) if rel is not None else (1, 0.0)

    revisions.sort(key=_sort_key)
    return revisions


def _relative_change(old_value, new_value) -> float | None:
    if old_value is None or new_value is None or old_value == 0:
        return None
    return (new_value - old_value) / abs(old_value) * 100.0


def _parse_since(value: str) -> datetime:
    """Opposite day-boundary rule from get_observations()'s as_of, and
    deliberately so: `--since 2026-03-01` must INCLUDE a revision written at
    2026-03-01T09:00, so a bare date here means the START of that day, not
    the end."""
    try:
        d = datetime.strptime(value, "%Y-%m-%d").date()
        return datetime.combine(d, time.min, tzinfo=timezone.utc)
    except ValueError:
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _format(r: dict) -> str:
    rel = _relative_change(r["old_value"], r["new_value"])
    rel_str = f"{rel:+.2f}%" if rel is not None else "n/a"
    return (
        f"{r['indicator_id']}/{r['geo_id']}/{r['period']}: "
        f"{r['old_value']!r} ({r['old_status']}, {r['old_vintage']}) -> "
        f"{r['new_value']!r} ({r['new_status']}, {r['new_vintage']})  "
        f"delta={rel_str}"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="List every revised observation cell")
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file")
    ap.add_argument(
        "--since",
        default=None,
        help="Only revisions written on or after this date/timestamp. Omit for all history.",
    )
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    since = _parse_since(args.since) if args.since else None
    revisions = find_revisions(conn, since)
    conn.close()

    if not revisions:
        print("No revisions" + (f" since {args.since}" if args.since else "") + ".")
        return 0

    print(f"{len(revisions)} revision(s)" + (f" since {args.since}" if args.since else "") + ":")
    for r in revisions:
        print("  " + _format(r))
    return 0


if __name__ == "__main__":
    sys.exit(main())
