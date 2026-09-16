"""One-time migration: apply the police-zero-is-not-available rule
(scripts/sync_police.py, docs/decisions/0012-police-zero-is-not-available.md)
to the ALREADY-COMMITTED data/police_observations.csv.

WHY THIS EXISTS SEPARATELY FROM THE ADAPTER FIX. CLAUDE.md rule 1 says data
changes only through adapters and migrations -- never by hand. The adapter
fix in sync_police.py governs every FUTURE re-sync from data/raw/police/, but
those raw files are hand-fetched by the maintainer and are not present on
every machine (they are absent here). The already-committed store was built
by the OLD adapter and carries the defect right now: this script is the
migration half of rule 1, a deterministic, testable rewrite of the store
using EXACTLY the same rule the fixed adapter applies, so that the next real
re-sync from raw reproduces this file byte for byte (see
tests/test_correct_police_zero_placeholders.py's
`test_correction_agrees_with_the_adapter_on_equivalent_synthetic_raw`).

THE RULE, restated (see sync_police.py's module docstring for the full
reasoning):

1. A row with value == 0.0 becomes status='na', value=NULL. police.be pairs
   a placeholder row with z: 0, and nothing in this file distinguishes that
   placeholder from a genuine zero for a real commune -- so no zero from
   this source can be trusted as measured (CLAUDE.md rules 13 and 26).

2. A row whose geo_id had already been dissolved by the START of the row's
   own period (`geographies.valid_to <= period_start`, valid_to being an
   EXCLUSIVE upper bound, read from the geographies table, never a date
   literal) is DROPPED ENTIRELY -- not `na`. It did not exist to be measured
   in that period.

Both rules apply regardless of the row's CURRENT status (a 0.0 row already
marked 'provisional' still becomes 'na'; a dropped row is dropped regardless
of its status), because the defect is in the VALUE, not in how this pipeline
already classified it.

WHAT THIS DOES NOT TOUCH. Any row with a nonzero value on a still-existing
geo_id is copied through unchanged, including its exact `vintage` and
`created_at` -- this is a correction of specific wrong rows, not a re-fetch,
and must not fabricate a new vintage for rows that were always right.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import load_geography  # noqa: E402

from src.db import migrate  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
GEOGRAPHY_CONFIG_DIR = REPO_ROOT / "config" / "geography"
DEFAULT_CSV = REPO_ROOT / "data" / "police_observations.csv"

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


def _valid_to_by_geo_id(conn: sqlite3.Connection) -> dict[str, str | None]:
    return dict(conn.execute("SELECT geo_id, valid_to FROM geographies"))


def _dissolved_before(valid_to: str | None, period_start: str) -> bool:
    """Same exclusive-upper-bound rule as scripts/sync_police.py's
    _dissolved_before(), restated here against a plain dict lookup instead of
    a live connection -- see the test that proves the two agree."""
    return valid_to is not None and valid_to <= period_start


def _open_geography_only_db() -> tuple[sqlite3.Connection, Path]:
    """A throwaway DB holding only migrations + geographies -- no
    observations, no police sync. Cheap (geography load is offline and fast)
    and keeps this script needing nothing but what is already committed."""
    tmp_dir = Path(tempfile.mkdtemp(prefix="police-correction-"))
    db_path = tmp_dir / "geo_only.db"
    migrate.run(db_path)
    load_geography.load(db_path, GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    conn = sqlite3.connect(str(db_path))
    return conn, db_path


def correct_rows(
    rows: list[dict], valid_to_by_geo_id: dict[str, str | None]
) -> tuple[list[dict], int, int, int]:
    """Apply the rule to already-parsed CSV rows (dicts with string values,
    as csv.DictReader produces). Returns (corrected_rows, n_became_na,
    n_dropped_dissolved, n_unchanged).

    Pure function of its inputs -- no I/O -- so it is what both this script
    and its equivalence test call, guaranteeing the CLI and the test can
    never silently diverge from each other.
    """
    out: list[dict] = []
    n_na = 0
    n_dropped = 0
    n_unchanged = 0

    for row in rows:
        geo_id = row["geo_id"]
        period_start = row["period_start"]
        valid_to = valid_to_by_geo_id.get(geo_id)

        if _dissolved_before(valid_to, period_start):
            n_dropped += 1
            continue

        value_raw = row["value"]
        is_zero = value_raw not in ("", None) and float(value_raw) == 0.0

        if is_zero and row["status"] != "na":
            corrected = dict(row)
            corrected["value"] = ""
            corrected["status"] = "na"
            out.append(corrected)
            n_na += 1
        else:
            out.append(dict(row))
            n_unchanged += 1

    return out, n_na, n_dropped, n_unchanged


def _read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict]) -> None:
    # Sorted by the observations primary key, same convention as
    # export_observations_csv.py, so the file stays byte-stable and a
    # correction that changes nothing produces an empty diff.
    rows_sorted = sorted(
        rows, key=lambda r: (r["indicator_id"], r["geo_id"], r["period"], r["vintage"])
    )
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        for row in rows_sorted:
            writer.writerow({col: row[col] for col in COLUMNS})


def correct_file(csv_path: Path, geo_conn: sqlite3.Connection) -> dict[str, int]:
    rows = _read_csv(csv_path)
    valid_to_by_geo_id = _valid_to_by_geo_id(geo_conn)
    corrected, n_na, n_dropped, n_unchanged = correct_rows(rows, valid_to_by_geo_id)
    _write_csv(csv_path, corrected)
    return {
        "rows_read": len(rows),
        "rows_written": len(corrected),
        "became_na": n_na,
        "dropped_dissolved": n_dropped,
        "unchanged": n_unchanged,
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Apply the police-zero-is-not-available rule to the committed "
        "data/police_observations.csv in place (one-time migration; CLAUDE.md rule 1)."
    )
    ap.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    args = ap.parse_args()

    if not args.csv.is_file():
        raise SystemExit(f"No such file: {args.csv}")

    geo_conn, _tmp_db_path = _open_geography_only_db()
    try:
        stats = correct_file(args.csv, geo_conn)
    finally:
        geo_conn.close()

    print(
        f"Read {stats['rows_read']} row(s) from {args.csv}. "
        f"{stats['became_na']} became na/NULL, "
        f"{stats['dropped_dissolved']} dropped (dissolved before their period), "
        f"{stats['unchanged']} unchanged. "
        f"Wrote {stats['rows_written']} row(s)."
    )


if __name__ == "__main__":
    main()
