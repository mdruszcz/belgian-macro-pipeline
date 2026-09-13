"""
Rebuild a working SQLite database from a committed observations CSV -- the
inverse of scripts/export_observations_csv.py.

The manual store is a CSV (see docs/decisions/0002-split-committed-stores.md);
this turns it back into something queryable. The resulting database is a
disposable build artefact, not a committed one -- data/local/ is gitignored.

Everything it needs is already committed and needs no network:
  - the schema, from migrations/ via src.db.migrate (which creates the full
    schema on an empty file)
  - `geographies`, from config/geography/*.csv via scripts/load_geography.py
    (idempotent, offline) -- required because observations.geo_id is a real
    NOT NULL foreign key
  - `indicators`/`sources`, from config/indicators/*.yaml

`fetch_run_id` is not carried in the CSV -- it is meaningless outside the
database that produced it -- so this opens a single run row to satisfy the
foreign key, marked in its message as a rebuild rather than a real fetch.
"""

import argparse
import csv
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import load_geography  # noqa: E402

from src.db import migrate  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "config"
GEOGRAPHY_CONFIG_DIR = CONFIG_DIR / "geography"


class ObservationsCsvError(Exception):
    """The CSV is missing, malformed, or references something undefined."""


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_ids: set[str]) -> None:
    """Create the sources/indicators rows the observations depend on, from
    the committed YAML configs. Fails loudly on an indicator the config does
    not define rather than inventing a placeholder (CLAUDE.md rule 13)."""
    indicator_configs, source_configs = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    missing = sorted(i for i in indicator_ids if i not in indicator_configs)
    if missing:
        raise ObservationsCsvError(
            f"CSV references indicator(s) with no config/indicators/*.yaml: {missing}. "
            "Refusing to invent metadata for them."
        )

    for indicator_id in sorted(indicator_ids):
        ind = indicator_configs[indicator_id]
        source_id = ind["source_id"]
        src = source_configs[source_id]
        conn.execute(
            """
            INSERT OR IGNORE INTO sources
                (source_id, name, agency, adapter, base_url, licence, catalog_ref,
                 cadence, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                source_id,
                src["name"],
                src["agency"],
                src["adapter"],
                src.get("base_url"),
                src.get("licence"),
                src.get("catalog_ref"),
                src.get("cadence"),
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'sum', 1, 0, ?, 1)
            """,
            (
                indicator_id,
                source_id,
                ind["name"]["nl"],
                ind["name"]["fr"],
                ind["name"]["en"],
                ind.get("description", {}).get("en", ""),
                ind["frequency"],
                ind["unit"],
                ind["preferred_direction"],
                f"config/indicators/{indicator_id}.yaml",
            ),
        )
    conn.commit()


def load(
    db_path: Path,
    csv_path: Path,
    allow_unverified: bool = True,
    run_source_id: str = "statbel",
    run_adapter: str = "rebuild",
) -> int:
    """Build `db_path` from `csv_path`. Returns the number of rows loaded.

    `run_source_id`/`run_adapter`: THE FORGED-HEARTBEAT FIX. This used to
    hardcode `source_id='statbel', adapter='statbel', status='ok'` for every
    rebuild, no matter which store's CSV was actually being loaded --
    forging Statbel's daily heartbeat every time this ran, which would have
    permanently defeated src.validation.rules.fetch_silence (it reads
    `MAX(started_at)` per source_id from fetch_runs) and fetch_error (it
    reads the MOST RECENT row per source_id), the two rules whose entire
    purpose is catching a source that has quietly stopped being fetched.

    CHOSEN FIX: adapter='rebuild' by default, not the CSV's own adapter name,
    and both of those rules now exclude adapter='rebuild' from their
    queries entirely (src/validation/rules.py) rather than trying to keep
    started_at honest. The alternative -- setting started_at from the CSV's
    max created_at -- was rejected: created_at is when the OBSERVATION was
    written, which for population is a date years in the past for old rows
    and would make every rebuild look like "last fetched years ago",
    tripping the very silence rule this is meant to leave alone. Excluding
    the adapter is unambiguous: a rebuild is definitionally not a fetch, so
    it should not appear as one under either name.

    `run_source_id` still defaults to 'statbel' (this script's only caller
    today is the population store, whose source_id is 'statbel') -- callers
    driven by config/stores.yaml (scripts/build_staging_db.py) pass the
    store's own `source_id` explicitly instead of relying on the default.
    """
    if not csv_path.is_file():
        raise ObservationsCsvError(f"No observations CSV at {csv_path}")

    with csv_path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        raise ObservationsCsvError(f"{csv_path} has a header but no rows.")

    migrate.run(db_path)
    load_geography.load(db_path, GEOGRAPHY_CONFIG_DIR, allow_unverified=allow_unverified)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_reference_rows(conn, {r["indicator_id"] for r in rows})

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO fetch_runs (source_id, adapter, started_at, finished_at, status, message)
           VALUES (?, ?, ?, ?, 'ok', ?)""",
        (
            run_source_id,
            run_adapter,
            now,
            now,
            f"rebuild from {csv_path.name} -- not a real fetch",
        ),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    for row in rows:
        conn.execute(
            """
            INSERT INTO observations
                (indicator_id, geo_id, period, vintage, value, status,
                 period_start, period_end, is_latest, fetch_run_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["indicator_id"],
                row["geo_id"],
                row["period"],
                row["vintage"],
                float(row["value"]) if row["value"] != "" else None,
                row["status"],
                row["period_start"],
                row["period_end"],
                int(row["is_latest"]),
                fetch_run_id,
                row["created_at"],
            ),
        )

    conn.execute(
        "UPDATE fetch_runs SET rows_read = ?, rows_written = ? WHERE fetch_run_id = ?",
        (len(rows), len(rows), fetch_run_id),
    )
    conn.commit()
    conn.close()
    return len(rows)


def _insert_rows(conn: sqlite3.Connection, rows: list[dict], fetch_run_id: int) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO observations
                (indicator_id, geo_id, period, vintage, value, status,
                 period_start, period_end, is_latest, fetch_run_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["indicator_id"],
                row["geo_id"],
                row["period"],
                row["vintage"],
                float(row["value"]) if row["value"] != "" else None,
                row["status"],
                row["period_start"],
                row["period_end"],
                int(row["is_latest"]),
                fetch_run_id,
                row["created_at"],
            ),
        )


def load_many(
    db_path: Path,
    csv_paths: list[Path] | tuple[Path, ...],
    *,
    allow_unverified: bool = True,
    run_source_id: str,
    run_adapter: str = "rebuild",
) -> int:
    """Load several committed observation CSVs -- a one_csv_per_indicator
    directory store's per-indicator files -- into `db_path` in ONE process.
    Returns the total number of rows loaded.

    WHY ONE PROCESS, NOT load() CALLED ONCE PER FILE (docs/features/international.md,
    "Assemble at scale"): migrations and geography are applied exactly once
    regardless of how many files there are, and the whole batch gets ONE
    'rebuild' fetch_runs row, not one per indicator -- a hundred international
    indicators must not turn into a hundred audit-trail rows for what is a
    single rebuild event. scripts/build_staging_db.py calls this (via this
    script's `--csv-dir`) for a one_csv_per_indicator store, the same way it
    calls `load()` (via `--csv`) for a single_csv one.

    An empty `csv_paths` is valid (an in_db directory store with nothing
    fetched yet) and loads zero rows without complaint -- unlike `load()`,
    which refuses a CSV with a header and no rows, there is no file at all
    here to have an empty body.
    """
    all_rows: list[dict] = []
    for csv_path in csv_paths:
        if not csv_path.is_file():
            raise ObservationsCsvError(f"No observations CSV at {csv_path}")
        with csv_path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
        if not rows:
            raise ObservationsCsvError(f"{csv_path} has a header but no rows.")
        all_rows.extend(rows)

    migrate.run(db_path)
    load_geography.load(db_path, GEOGRAPHY_CONFIG_DIR, allow_unverified=allow_unverified)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_reference_rows(conn, {r["indicator_id"] for r in all_rows})

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT INTO fetch_runs (source_id, adapter, started_at, finished_at, status, message)
           VALUES (?, ?, ?, ?, 'ok', ?)""",
        (
            run_source_id,
            run_adapter,
            now,
            now,
            f"rebuild from {len(csv_paths)} file(s) -- not a real fetch",
        ),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    _insert_rows(conn, all_rows, fetch_run_id)

    conn.execute(
        "UPDATE fetch_runs SET rows_read = ?, rows_written = ? WHERE fetch_run_id = ?",
        (len(all_rows), len(all_rows), fetch_run_id),
    )
    conn.commit()
    conn.close()
    return len(all_rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Rebuild a DB from committed observations CSV(s)")
    ap.add_argument("--db", required=True, help="Path to the SQLite DB to build")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--csv", help="A single committed observations CSV to load (layout: single_csv)"
    )
    group.add_argument(
        "--csv-dir",
        help="A one_csv_per_indicator store's directory -- every *.csv file in it is loaded "
        "in one process, one 'rebuild' fetch_runs row for the whole batch (load_many()).",
    )
    ap.add_argument(
        "--run-source-id",
        default="statbel",
        help="sources.source_id to record this rebuild's fetch_runs row under -- pass the "
        "CSV's real store source (config/stores.yaml). Defaults to 'statbel' for backward "
        "compatibility with the population store, this script's original only caller.",
    )
    ap.add_argument(
        "--run-adapter",
        default="rebuild",
        help="fetch_runs.adapter for this row. 'rebuild' (the default) is excluded from "
        "the fetch_silence/fetch_error rules -- see load()'s docstring for why this must "
        "never again be a real adapter name like 'statbel'.",
    )
    args = ap.parse_args()
    try:
        if args.csv_dir:
            csv_paths = sorted(Path(args.csv_dir).glob("*.csv"))
            n = load_many(
                Path(args.db),
                csv_paths,
                run_source_id=args.run_source_id,
                run_adapter=args.run_adapter,
            )
        else:
            n = load(
                Path(args.db),
                Path(args.csv),
                run_source_id=args.run_source_id,
                run_adapter=args.run_adapter,
            )
    except ObservationsCsvError as exc:
        print(f"\nCANNOT LOAD: {exc}\n", file=sys.stderr)
        raise SystemExit(2) from exc
    print(f"Loaded {n} observations into {args.db}")


if __name__ == "__main__":
    main()
