"""
Assemble a disposable working database at data/local/working.db --
the "staging" half of PR1 of the pipeline repair (config/stores.yaml plus
src/stores.py is the other half).

NOT wired into `make all`, CI, or either workflow yet -- that cutover is PR2's,
deliberately kept separate so it can be atomic (CLAUDE.md rule 10: this PR
moves no data and changes no published artefact). `make assemble` runs this
alone, today, for anyone who wants to see it work.

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
  6. Load every `in_db` store's CSV into the working copy.
     IN THIS PR THAT SET IS EMPTY -- config/stores.yaml declares all six
     existing stores as `extra_csv` -- so step 6 must be a correct no-op with
     zero in_db stores. PR2 is what adds ONEM and WalStat here; this PR only
     has to prove the assembly line runs end to end with nothing on it yet.

Deterministic and idempotent: run this twice and the second run reaches the
same state as the first (row-for-row), because every step is either
idempotent on its own (migrate, load_geography, ensure_reference_rows) or
starts from the freshly rm'd file (the copy and the CSV loads).
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.stores import DEFAULT_STORES_PATH, in_db_stores, load_stores  # noqa: E402

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
        print(
            "No in_db stores declared in the registry -- nothing to load into the "
            "working database (expected in this PR; PR2 adds ONEM and WalStat here)."
        )
    for store in to_load:
        _run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "load_observations_csv.py"),
                "--db",
                str(working_db),
                "--csv",
                str(store.path),
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
