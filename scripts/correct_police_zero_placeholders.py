"""One-time migration: apply the province-wide police-zero-is-not-available
rule (scripts/sync_police.py, docs/decisions/0012-police-zero-is-not-available.md,
as amended 2026-09-16) to the ALREADY-COMMITTED data/police_observations.csv.

WHY THIS EXISTS SEPARATELY FROM THE ADAPTER FIX. CLAUDE.md rule 1 says data
changes only through adapters and migrations -- never by hand. The adapter
fix in sync_police.py governs every FUTURE re-sync from data/raw/police/, but
those raw files are hand-fetched by the maintainer and are not present on
every machine (they are absent here). The already-committed store carries
the defect right now: this script is the migration half of rule 1, a
deterministic, testable rewrite of the store using EXACTLY the same rule the
fixed adapter applies, so that the next real re-sync from raw reproduces this
file byte for byte (see tests/test_correct_police_zero_placeholders.py's
`test_correction_agrees_with_the_adapter_on_equivalent_synthetic_raw`).

MUST RE-DERIVE FROM THE PRE-#215 STORE, NOT FROM TODAY'S. PR #215 already
rewrote every `z == 0` row on a resolvable commune to `status='na',
value=NULL` -- uniformly, regardless of its province. That NULL is
information loss: once a row is `na`, this script can no longer tell whether
its ORIGINAL value was a genuine small-commune zero (which the new
province-wide rule would restore as measured 0.0) or a true province-wide
placeholder (which stays `na`). Running this correction against the
CURRENT, already-corrected CSV would therefore silently leave every row
`na` forever -- exactly the bug the maintainer is pointing at ("you removed
all the zero instead of just hainaut like i asked"). So this script's
default SOURCE is the real `z` values as they existed at commit 66468d61
(the last commit before #215), read via `git show 66468d61:
data/police_observations.csv` -- never the working tree's current
data/police_observations.csv. `--source-csv` overrides this (used by tests
against a synthetic fixture); production use always re-derives from the
pinned pre-#215 blob, and `main()` refuses to default to
data/police_observations.csv as its own source for that reason.

THE RULE, restated (see sync_police.py's module docstring for the full
reasoning, including the maintainer's 2026-09-16 correction):

1. A row with value == 0.0 becomes status='na', value=NULL ONLY if every
   OTHER live (non-dissolved), resolvable row for the same (indicator_id,
   period, province-or-region-group) is ALSO exactly 0.0 -- a province-wide
   non-report, not a coincidence. Any other zero -- including a lone small
   commune's zero next to nonzero provincial siblings -- is copied through
   as a measured 0.0 with its original status. Grouping walks
   `geographies.parent_geo_id` to the `level = 'province'` ancestor; the 19
   Brussels communes have none, so they group by their region instead (see
   sync_police.py's `_province_group`, imported and reused here so the two
   scripts can never define "province" differently).

2. A row whose geo_id had already been dissolved by the START of the row's
   own period (`geographies.valid_to <= period_start`, valid_to being an
   EXCLUSIVE upper bound, read from the geographies table, never a date
   literal) is DROPPED ENTIRELY -- not `na`, and excluded from the
   province-wide unanimity test in (1) too, since it did not exist to be
   measured in that period. Unchanged from the original migration.

Both rules apply regardless of the row's CURRENT status, because the defect
is in the VALUE (or, for dissolution, in whether a row should exist at all),
not in how this pipeline already classified it.

WHAT THIS DOES NOT TOUCH. Any row with a nonzero value on a still-existing
geo_id is copied through unchanged, including its exact `vintage` and
`created_at` -- this is a correction of specific wrong rows, not a re-fetch,
and must not fabricate a new vintage for rows that were always right. A row
whose zero survives as measured 0.0 also keeps its original `vintage` and
`created_at` for the same reason -- it was always a real reading, just
mis-filed as `na` by #215.
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import load_geography  # noqa: E402
import sync_police  # noqa: E402

from src.db import migrate  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
GEOGRAPHY_CONFIG_DIR = REPO_ROOT / "config" / "geography"
DEFAULT_OUT_CSV = REPO_ROOT / "data" / "police_observations.csv"

# The last commit before PR #215 introduced the uniform (over-broad) zero
# rule this migration replaces. See the module docstring: this correction
# must always start from the REAL pre-#215 z-values, never from a store that
# has already collapsed some of them to NULL.
PRE_215_COMMIT = "66468d61"
PRE_215_PATH = "data/police_observations.csv"

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


def fetch_pre_215_csv_text(repo_root: Path = REPO_ROOT) -> str:
    """The real pre-#215 CSV content, straight from git history -- never the
    working tree. Raises if the blob cannot be read (e.g. a shallow clone
    missing that commit) rather than silently falling back to a wrong
    source (CLAUDE.md rule 13)."""
    result = subprocess.run(
        ["git", "show", f"{PRE_215_COMMIT}:{PRE_215_PATH}"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Could not read {PRE_215_PATH!r} at {PRE_215_COMMIT} (git show exited "
            f"{result.returncode}): {result.stderr.strip()}. This migration must run "
            "against the real pre-#215 store, not a guess -- fetch full history "
            "(git fetch --unshallow) if this is a shallow clone."
        )
    return result.stdout


def correct_rows(
    rows: list[dict], valid_to_by_geo_id: dict[str, str | None], geo_conn: sqlite3.Connection
) -> tuple[list[dict], int, int, int]:
    """Apply the province-wide rule to already-parsed CSV rows (dicts with
    string values, as csv.DictReader produces). Returns (corrected_rows,
    n_became_na, n_dropped_dissolved, n_unchanged).

    Pure function of its (rows, valid_to_by_geo_id, geo_conn) inputs -- the
    only I/O is geo_conn's own already-open, read-only lookups -- so it is
    what both this script and its equivalence test call, guaranteeing the
    CLI and the test can never silently diverge from each other.

    Two passes, matching sync_police.py's sync() exactly:

    PASS 1 drops any row for a commune dissolved before its own period and
    otherwise keeps every row that names a real, then-existing commune --
    that surviving set is the population PASS 2's unanimity test is computed
    over, grouped by (indicator_id, period, province-or-region).

    PASS 2 converts a zero to na only inside a group where EVERY surviving
    row is zero.
    """
    # PASS 1: dissolution.
    live_rows: list[dict] = []
    n_dropped = 0
    for row in rows:
        geo_id = row["geo_id"]
        period_start = row["period_start"]
        valid_to = valid_to_by_geo_id.get(geo_id)
        if _dissolved_before(valid_to, period_start):
            n_dropped += 1
            continue
        live_rows.append(row)

    # PASS 2: province-wide unanimity, grouped by (indicator_id, period,
    # province-or-region group) -- the same grouping key sync_police.py's
    # sync() uses (there, implicitly, per (indicator, year) file; here,
    # explicit, because one CSV holds every indicator and year at once).
    group_all_zero: dict[tuple[str, str, str], bool] = {}
    province_group_cache: dict[str, str] = {}

    def _group_for(geo_id: str) -> str:
        if geo_id not in province_group_cache:
            province_group_cache[geo_id] = sync_police._province_group(geo_conn, geo_id)
        return province_group_cache[geo_id]

    for row in live_rows:
        value_raw = row["value"]
        is_zero = value_raw not in ("", None) and float(value_raw) == 0.0
        key = (row["indicator_id"], row["period"], _group_for(row["geo_id"]))
        if key not in group_all_zero:
            group_all_zero[key] = is_zero
        else:
            group_all_zero[key] = group_all_zero[key] and is_zero

    out: list[dict] = []
    n_na = 0
    n_unchanged = 0
    for row in live_rows:
        value_raw = row["value"]
        is_zero = value_raw not in ("", None) and float(value_raw) == 0.0
        key = (row["indicator_id"], row["period"], _group_for(row["geo_id"]))

        if is_zero and group_all_zero[key]:
            if row["status"] != "na":
                corrected = dict(row)
                corrected["value"] = ""
                corrected["status"] = "na"
                out.append(corrected)
                n_na += 1
            else:
                out.append(dict(row))
                n_unchanged += 1
        else:
            # Either nonzero, or a zero that is NOT province-wide unanimous
            # -- a measured reading (including a measured zero). Copied
            # through with its ORIGINAL status/value untouched, per the
            # module docstring: this is a correction of specific wrong rows,
            # not a re-fetch.
            out.append(dict(row))
            n_unchanged += 1

    return out, n_na, n_dropped, n_unchanged


def _parse_csv_text(text: str) -> list[dict]:
    return list(csv.DictReader(text.splitlines()))


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


def correct_source_rows(
    source_rows: list[dict], geo_conn: sqlite3.Connection
) -> tuple[list[dict], dict[str, int]]:
    """correct_rows() plus the stats dict, factored out so both correct_file()
    and the CLI's default (git-blob-sourced) path share one code path."""
    valid_to_by_geo_id = _valid_to_by_geo_id(geo_conn)
    corrected, n_na, n_dropped, n_unchanged = correct_rows(
        source_rows, valid_to_by_geo_id, geo_conn
    )
    stats = {
        "rows_read": len(source_rows),
        "rows_written": len(corrected),
        "became_na": n_na,
        "dropped_dissolved": n_dropped,
        "unchanged": n_unchanged,
    }
    return corrected, stats


def correct_file(csv_path: Path, geo_conn: sqlite3.Connection) -> dict[str, int]:
    """Test/back-compat entry point: source AND destination are the same
    file. Production use goes through main(), whose source is always the
    pre-#215 git blob, not this file's own current contents."""
    rows = _read_csv(csv_path)
    corrected, stats = correct_source_rows(rows, geo_conn)
    _write_csv(csv_path, corrected)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Apply the province-wide police-zero-is-not-available rule to "
        "data/police_observations.csv (one-time migration; CLAUDE.md rule 1). Always "
        "re-derives from the pre-#215 git blob, never from the current, "
        "already-corrected working-tree file, unless --source-csv overrides it."
    )
    ap.add_argument(
        "--source-csv",
        type=Path,
        default=None,
        help="Read source rows from this CSV instead of the pre-#215 git blob "
        f"({PRE_215_COMMIT}:{PRE_215_PATH}). For tests only -- production runs should "
        "omit this so the correction is always computed from real, uncollapsed z-values.",
    )
    ap.add_argument("--out-csv", type=Path, default=DEFAULT_OUT_CSV)
    args = ap.parse_args()

    if args.source_csv is not None:
        if not args.source_csv.is_file():
            raise SystemExit(f"No such file: {args.source_csv}")
        source_rows = _read_csv(args.source_csv)
        source_desc = str(args.source_csv)
    else:
        source_rows = _parse_csv_text(fetch_pre_215_csv_text())
        source_desc = f"git blob {PRE_215_COMMIT}:{PRE_215_PATH}"

    geo_conn, _tmp_db_path = _open_geography_only_db()
    try:
        corrected, stats = correct_source_rows(source_rows, geo_conn)
    finally:
        geo_conn.close()

    _write_csv(args.out_csv, corrected)

    print(
        f"Read {stats['rows_read']} row(s) from {source_desc}. "
        f"{stats['became_na']} became na/NULL (province-wide zero), "
        f"{stats['dropped_dissolved']} dropped (dissolved before their period), "
        f"{stats['unchanged']} unchanged (including measured zeros restored from #215's "
        "over-broad na). "
        f"Wrote {stats['rows_written']} row(s) to {args.out_csv}."
    )


if __name__ == "__main__":
    main()
