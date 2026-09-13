"""
Assemble a disposable working database at data/local/working.db --
the "staging" half of PR1 of the pipeline repair (config/stores.yaml plus
src/stores.py is the other half).

Since PR2 (the ONEM/WalStat cutover) this is the first step of `make all`,
CI's validation job and both workflows: every sync, the validation and every
export read the working copy, never the committed database. The daily
workflow then runs scripts/offload_stores.py, which dumps the in_db stores
back to their CSVs and writes the slim committed database. See
docs/decisions/0006-stores-split-by-volume.md.

Steps, in order:
  1. rm -f the previous working.db (and its -shm/-wal siblings, in case a
     previous run's connection was left open) -- scripts/load_observations_csv.py's
     INSERT is a plain INSERT, which raises IntegrityError on a second run
     into a file that already carries the same primary keys. data/local/ is
     gitignored (ADR 0002 calls it "a disposable local build artefact"), so
     deleting and rebuilding it is exactly the right instinct, not a risk.
  2. Copy data/belgian_macro.db onto it -- the auto-fetched national and
     LOCAL_UNITS_BY_COMMUNE rows already live there.
  3. Apply migrations (idempotent) -- a copy of an older committed DB might
     predate a migration the current code expects.
  4. Load geography (idempotent, offline, config/geography/*.csv only).
  5. Ensure every store's reference rows (scripts/ensure_reference_rows.py,
     driven by the same registry).
  6. Load every `in_db` store's CSV into the working copy (ONEM, ONEM's
     published rate, WalStat). Refuses first if the committed database still
     holds rows for those indicators -- they would collide on the primary key.
     The loader preserves vintage and is_latest exactly, which is what lets
     the syncs that follow decide "new vintage or unchanged" against the full
     history (src/db/vintages.py).

Deterministic and idempotent: run this twice and the second run reaches the
same state as the first (row-for-row), because every step is either
idempotent on its own (migrate, load_geography, ensure_reference_rows) or
starts from the freshly rm'd file (the copy and the CSV loads).
"""

import argparse
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.stores import (  # noqa: E402
    DEFAULT_STORES_PATH,
    LAYOUT_ONE_CSV_PER_INDICATOR,
    in_db_stores,
    load_stores,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DB = REPO_ROOT / "data" / "belgian_macro.db"
DEFAULT_WORKING_DB = REPO_ROOT / "data" / "local" / "working.db"


class StagingBuildError(Exception):
    pass


def _run(cmd: list) -> None:
    printable = " ".join(str(c) for c in cmd)
    print(f"$ {printable}")
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise StagingBuildError(f"step failed (exit {result.returncode}): {printable}")


def _refuse_a_source_db_that_still_holds_in_db_rows(working_db: Path, stores) -> None:
    """The committed database must not already carry an in_db store's rows --
    a database from before the cutover, say. Loading the CSV on top would hit
    the primary key halfway through; this says what is actually wrong first."""
    indicators = sorted({i for s in stores for i in s.indicators})
    if not indicators:
        return
    conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
    try:
        held = conn.execute(
            f"SELECT indicator_id, COUNT(*) FROM observations "
            f"WHERE indicator_id IN ({','.join('?' for _ in indicators)}) "
            f"GROUP BY indicator_id ORDER BY indicator_id",
            indicators,
        ).fetchall()
    finally:
        conn.close()
    if held:
        raise StagingBuildError(
            f"the source database already holds rows for in_db store indicators {held[:5]} -- "
            "those rows belong in their committed CSV, not the database. Is this a database "
            "from before the ONEM/WalStat cutover?"
        )


def build(
    source_db: Path = DEFAULT_SOURCE_DB,
    working_db: Path = DEFAULT_WORKING_DB,
    stores_path: Path = DEFAULT_STORES_PATH,
) -> Path:
    if not source_db.is_file():
        raise StagingBuildError(f"No source database at {source_db}")

    working_db.parent.mkdir(parents=True, exist_ok=True)

    # Step 1: rm -f, including WAL/SHM siblings a crashed previous run may
    # have left behind. missing_ok -- there may be nothing to remove yet.
    for suffix in ("", "-shm", "-wal"):
        Path(str(working_db) + suffix).unlink(missing_ok=True)

    # Step 2.
    shutil.copy2(source_db, working_db)

    # Step 3-5.
    _run([sys.executable, "-m", "src.db.migrate", "--db", str(working_db)])
    _run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "load_geography.py"),
            "--db",
            str(working_db),
        ]
    )
    _run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "ensure_reference_rows.py"),
            "--db",
            str(working_db),
            "--stores",
            str(stores_path),
        ]
    )

    # Step 6.
    stores = load_stores(stores_path)
    to_load = in_db_stores(stores)
    if not to_load:
        print("No in_db stores declared in the registry -- nothing to load.")
    _refuse_a_source_db_that_still_holds_in_db_rows(working_db, to_load)
    for store in to_load:
        path_flag = (
            ["--csv-dir", str(store.path)]
            if store.layout == LAYOUT_ONE_CSV_PER_INDICATOR
            else ["--csv", str(store.path)]
        )
        _run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "load_observations_csv.py"),
                "--db",
                str(working_db),
                *path_flag,
                "--run-source-id",
                store.source_id,
                "--run-adapter",
                "rebuild",
            ]
        )

    print(f"\nAssembled {working_db}")
    return working_db


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Assemble the disposable working database at data/local/working.db"
    )
    ap.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB))
    ap.add_argument("--working-db", default=str(DEFAULT_WORKING_DB))
    ap.add_argument("--stores", default=str(DEFAULT_STORES_PATH))
    args = ap.parse_args()
    try:
        build(Path(args.source_db), Path(args.working_db), Path(args.stores))
    except StagingBuildError as exc:
        print(f"\nSTAGING BUILD FAILED: {exc}\n", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
