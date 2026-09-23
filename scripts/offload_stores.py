"""
Offload every `in_db` store out of the working database and write the slim
committed database -- the last half of PR2 of the pipeline repair
(scripts/build_staging_db.py is the first half; docs/decisions/0006-stores-split-by-volume.md
is the why).

The daily run no longer touches data/belgian_macro.db until the very end:

    assemble  -> data/local/working.db   (committed db + every in_db CSV)
    syncs     -> data/local/working.db
    validate, exports                    (read data/local/working.db)
    offload   -> every in_db store's committed CSV  +  data/belgian_macro.db

This script is that last step, and the ONLY writer of the committed database
in the daily workflow. Everything it produces is built in scratch files first
and verified; only then are they moved over the committed files with
os.replace. A failure at any point before that leaves every committed file
exactly as it was (CLAUDE.md rule 33's spirit, applied to the data stores).

THE ROW SET IS ONE EXPRESSION: `indicator_id IN (<every in_db store's declared
indicators>)`. The same list drives the dump and the delete, so a row can never
be deleted from the database without first being written to a CSV. Checks,
each of which refuses rather than guesses (rule 13):

  0. No -wal or -shm file sits beside the committed database. One means a
     program has it open in WAL mode (a DB viewer), or one that crashed left
     it; replacing the database under it would pair the new file with the old
     one's journal. A check made before any write, NOT a lock: a program that
     opens the database after it is not detected. Those files are never
     deleted here.
  1. Every indicator in the working db whose SOURCE is offloaded (onem,
     walstat) is declared by some in_db store. A newly configured ONEM
     indicator that nobody registered would otherwise stay in the committed
     database silently, re-growing it -- the failure this whole repair exists
     to end. tests/test_stores.py catches the same drift at PR time.
  2. Every primary key already in a store's committed CSV is still present in
     the working db. Observations are append-only (src/db/vintages.py never
     deletes), so a missing key means the working db was not assembled from
     the committed CSVs -- and offloading it would overwrite years of history
     with one day's fetch.
  3. Rows dumped == rows counted for the same expression.
  4. After the delete: zero offloaded rows remain, no rebuild fetch_runs row
     remains, PRAGMA foreign_key_check is empty, PRAGMA integrity_check is ok,
     and the remaining observation count is exactly total - offloaded.

PUBLISHING IS ONE os.replace PER FILE -- each in_db CSV, then the database --
and a filesystem offers no rename that covers several files at once. So,
before the first one, every committed file about to be replaced is copied into
a backup directory of this run's own, under the working database's directory
(data/local/offload_backup/<time>-<pid>/, gitignored; never beside the
committed files, where `git add data/` would stage a leftover), with a
manifest.txt naming the committed file each backup belongs to.

  * An exception while publishing puts every file already replaced back from
    its backup -- copied to a temporary file beside it, then moved over it, so
    the backup survives an interrupted restore and the file is never left
    truncated -- and a file that did not exist before is removed again. The
    original exception is then raised unchanged, and this run's backups are
    deleted.
  * If a file cannot be put back, PartialPublication is raised from the
    original exception, naming the files still new, and the backup directory
    is KEPT for a restore by hand.
  * A publish that completes deletes this run's backups.

What this does NOT cover: a power cut or a killed process in the middle of
publishing runs no code at all, so nothing is put back. What is left then is
this run's backup directory and its manifest, from which the files can be
restored by hand. Backup directories of earlier runs are never deleted
automatically: one of them may be the only copy from an earlier incident.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from export_observations_csv import export_observations  # noqa: E402

from src.stores import (  # noqa: E402
    DEFAULT_STORES_PATH,
    LAYOUT_ONE_CSV_PER_INDICATOR,
    in_db_stores,
    load_stores,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKING_DB = REPO_ROOT / "data" / "local" / "working.db"
DEFAULT_COMMITTED_DB = REPO_ROOT / "data" / "belgian_macro.db"

PK = ("indicator_id", "geo_id", "period", "vintage")

#: The one_csv_per_indicator dump key for a store's whole indicator list
#: (single_csv layout, unaffected by this pilot): there is one file, so one
#: pseudo-key stands in for "the whole store" rather than "one indicator".
_WHOLE_STORE = "__all__"

#: Beside the working database: one directory per run, never reused, never
#: cleared by another run.
BACKUP_DIR_NAME = "offload_backup"
BACKUP_MANIFEST = "manifest.txt"
#: The temporary file a restore copies a backup into, beside the committed file.
RESTORE_SUFFIX = ".restore.tmp"
#: What SQLite keeps beside a database open in WAL mode.
SIDECAR_SUFFIXES = ("-wal", "-shm")
_ABSENT = "(absent)"

# No handler configured here: the command line gets warnings on stderr from
# logging's last-resort handler, and the Dagster process keeps its own logging.
log = logging.getLogger("offload_stores")


class OffloadError(Exception):
    pass


class PartialPublication(Exception):
    """offload() stopped while replacing the committed files and could not put
    every one of them back. Not a refusal: files WERE changed. `left_new` still
    hold this run's version; `restored` hold their previous one again; the
    previous version of each is in `backup_dir`, listed in its manifest.txt."""

    def __init__(self, restored: list[Path], left_new: list[Path], backup_dir: Path) -> None:
        self.restored = tuple(restored)
        self.left_new = tuple(left_new)
        self.backup_dir = backup_dir
        still_new = "\n".join(f"  {p}" for p in self.left_new)
        put_back = "\n".join(f"  {p}" for p in self.restored) or "  (none)"
        super().__init__(
            "the offload stopped while publishing and could not put every committed file "
            f"back.\nStill this run's version:\n{still_new}\nPut back:\n{put_back}\n"
            f"The previous version of each file is kept in {backup_dir}; {BACKUP_MANIFEST} "
            f"there names the committed file each backup belongs to ({_ABSENT}: the file did "
            "not exist before, so remove it). Nothing in that directory is deleted "
            "automatically. Restore each file still new, one at a time -- from that directory, "
            "or with `git checkout -- <file>` -- never a whole directory."
        )


def _placeholders(n: int) -> str:
    return ",".join("?" for _ in range(n))


def _committed_keys(csv_path: Path) -> set[tuple[str, ...]]:
    if not csv_path.is_file():
        return set()
    with csv_path.open(encoding="utf-8", newline="") as fh:
        return {tuple(row[k] for k in PK) for row in csv.DictReader(fh)}


def _committed_keys_for_store(store) -> set[tuple[str, ...]]:
    """Every row already committed for `store`, across every file it has --
    one_csv_per_indicator stores have one file per indicator, some possibly
    absent (an in_db indicator with zero rows yet, tolerated by Store.csv_paths())."""
    keys: set[tuple[str, ...]] = set()
    for path in store.csv_paths():
        keys |= _committed_keys(path)
    return keys


def _refuse_an_open_committed_database(committed_db: Path) -> None:
    """Check 0 of the module docstring. Made before any write; not a lock."""
    sidecars = [committed_db.with_name(committed_db.name + s) for s in SIDECAR_SUFFIXES]
    present = [p for p in sidecars if p.exists()]
    if present:
        raise OffloadError(
            f"{' and '.join(p.name for p in present)} beside {committed_db}: a program has the "
            "committed database open (close the DB viewer, or whatever else is reading it), or "
            "one that crashed left them. Close it, then run the offload again. If nothing has "
            "it open any more, opening and closing the database once with sqlite3 removes "
            "them. They are never deleted automatically."
        )


def _check_every_offloaded_source_indicator_is_declared(
    conn: sqlite3.Connection,
    committed_conn: sqlite3.Connection,
    sources: set[str],
    declared: set[str],
) -> None:
    """Refuses if the working database holds a row for a source that has an
    in_db store, under an indicator that in_db store does not declare --
    UNLESS that indicator already sits in the COMMITTED database untouched
    (same source, present there already), meaning this run is not being
    asked to offload it and never has been.

    Before feat/ns1-bankruptcies, "a source has an in_db store" and "every
    indicator under that source is in_db" were the same fact for every
    offloaded source (onem, walstat, eurostat each had exactly one store,
    covering every indicator they configure). `statbel` breaks that: it now
    backs the new `bankruptcies` in_db store AND four pre-existing extra_csv
    stores (population, fiscal_income, census2021, realestate; their rows
    are never in the working database at all -- extra_csv never loads into
    any database, config/stores.yaml's own header comment -- so they were
    never a problem for this check) AND one indicator with no CSV store at
    all, LOCAL_UNITS_BY_COMMUNE (scripts/sync_statbel.py's own module
    docstring, "Non-goals": it stays directly in the small committed
    database by design, never offloaded). That last one WAS a real gap in
    this check, exposed (not created) by statbel gaining its first in_db
    store: LOCAL_UNITS_BY_COMMUNE's 565 rows are present in the working
    database (carried over from the committed database at assemble time,
    scripts/build_staging_db.py) and, before this fix, tripped "undeclared"
    even though nothing about it changed and no store was ever supposed to
    declare it.

    The committed-database comparison is what makes the fix exact rather
    than a second hardcoded list: an indicator is "already there, untouched"
    -- and excluded from `undeclared` -- only if it is ALREADY a row in the
    committed database before this offload writes anything. A genuinely new,
    never-before-seen indicator under an offloaded source that no in_db
    store declares (a config mistake, not LOCAL_UNITS_BY_COMMUNE's
    deliberate case) has no committed-database history and still refuses
    here exactly as before.
    """
    present = {
        row[0]
        for row in conn.execute(
            f"""SELECT DISTINCT o.indicator_id
                FROM observations o JOIN indicators i USING (indicator_id)
                WHERE i.source_id IN ({_placeholders(len(sources))})""",
            sorted(sources),
        )
    }
    undeclared = present - declared
    if not undeclared:
        return
    already_committed = {
        row[0]
        for row in committed_conn.execute(
            f"""SELECT DISTINCT indicator_id FROM observations
                WHERE indicator_id IN ({_placeholders(len(undeclared))})""",
            sorted(undeclared),
        )
    }
    still_undeclared = sorted(undeclared - already_committed)
    if still_undeclared:
        raise OffloadError(
            f"the working database holds rows for {still_undeclared}, whose source is "
            f"offloaded ({sorted(sources)}) but which no in_db store in config/stores.yaml "
            "declares, and which are not already present in the committed database. Add "
            "them to the right store's `indicators` -- refusing to leave them to re-grow "
            "the committed database."
        )


def _check_no_committed_row_is_lost(conn: sqlite3.Connection, store) -> None:
    committed = _committed_keys_for_store(store)
    if not committed:
        return
    working = {
        tuple(row)
        for row in conn.execute(
            f"""SELECT {", ".join(PK)} FROM observations
                WHERE indicator_id IN ({_placeholders(len(store.indicators))})""",
            store.indicators,
        )
    }
    lost = committed - working
    if lost:
        sample = sorted(lost)[:3]
        raise OffloadError(
            f"{store.name}: {len(lost)} row(s) in the committed {store.raw_path} are absent "
            f"from the working database (e.g. {sample}). Observations are append-only, so "
            "the working database was not assembled from the committed stores -- run "
            "scripts/build_staging_db.py first. Offloading now would erase that history."
        )


def _remove_backups(backup_dir: Path) -> None:
    """This run's backups, once they are no longer needed. A failure to remove
    them is reported, never raised: the committed files are already right."""
    shutil.rmtree(backup_dir, ignore_errors=True)
    if backup_dir.exists():
        log.warning("could not remove the offload backups in %s; remove them by hand", backup_dir)


def _back_up(destinations: list[Path], scratch: Path) -> tuple[Path, dict[Path, Path | None]]:
    """Copy every committed file about to be replaced into a new directory of
    this run's own. Returns it and {committed file: its backup, or None for a
    file that does not exist yet}. If backing up fails, nothing has been
    published: the incomplete directory is removed and the error raised."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup_dir = scratch / BACKUP_DIR_NAME / f"{stamp}-{os.getpid()}"
    backup_dir.mkdir(parents=True)
    backups: dict[Path, Path | None] = {}
    try:
        for i, dest in enumerate(destinations):
            if dest.is_file():
                backups[dest] = backup_dir / f"{i:03d}_{dest.name}"
                shutil.copy2(dest, backups[dest])
            else:
                backups[dest] = None
        lines = [
            f"# offload backup, {stamp}: the previous version of each committed file, in the",
            f"# order they are published. {_ABSENT}: the file did not exist before.",
            *(f"{b.name if b else _ABSENT}\t{dest}" for dest, b in backups.items()),
        ]
        (backup_dir / BACKUP_MANIFEST).write_text("\n".join(lines) + "\n", encoding="utf-8")
    except BaseException:
        shutil.rmtree(backup_dir, ignore_errors=True)
        raise
    return backup_dir, backups


def _restore(replaced: list[Path], backups: dict[Path, Path | None]) -> tuple[list, list]:
    """Put back every file in `replaced`, last replaced first, without consuming
    its backup. Returns (restored, left_new). Keeps going past a file it cannot
    put back, so as few as possible stay new."""
    restored: list[Path] = []
    left_new: list[Path] = []
    for dest in reversed(replaced):
        backup = backups[dest]
        try:
            if backup is None:
                dest.unlink(missing_ok=True)
            else:
                restoring = dest.with_name(dest.name + RESTORE_SUFFIX)
                shutil.copy2(backup, restoring)
                os.replace(restoring, dest)
            restored.append(dest)
        except Exception as exc:  # noqa: BLE001 -- reported below, never swallowed
            log.error("could not put %s back: %s: %s", dest, type(exc).__name__, exc)
            left_new.append(dest)
    return restored, left_new


def _publish_or_restore(
    publish: list[tuple[Path, Path]], backup_dir: Path, backups: dict[Path, Path | None]
) -> None:
    replaced: list[Path] = []
    try:
        for tmp_path, dest in publish:
            os.replace(tmp_path, dest)
            replaced.append(dest)
    except BaseException as exc:
        restored, left_new = _restore(replaced, backups)
        if left_new:
            raise PartialPublication(restored, left_new, backup_dir) from exc
        log.warning(
            "the offload stopped while publishing (%s: %s); every committed file it had "
            "replaced was put back: %s",
            type(exc).__name__,
            exc,
            ", ".join(str(p) for p in restored) or "none had been replaced yet",
        )
        _remove_backups(backup_dir)
        raise


def offload(
    working_db: Path = DEFAULT_WORKING_DB,
    committed_db: Path = DEFAULT_COMMITTED_DB,
    stores_path: Path = DEFAULT_STORES_PATH,
) -> dict[str, int]:
    """Returns {store name: rows written to its CSV}."""
    if not working_db.is_file():
        raise OffloadError(f"No working database at {working_db} -- run build_staging_db.py first")
    _refuse_an_open_committed_database(committed_db)

    stores = in_db_stores(load_stores(stores_path))
    if not stores:
        raise OffloadError("config/stores.yaml declares no in_db store -- nothing to offload")

    declared: list[str] = []
    for store in stores:
        overlap = sorted(set(store.indicators) & set(declared))
        if overlap:
            raise OffloadError(f"{store.name}: indicator(s) declared by two stores: {overlap}")
        declared.extend(store.indicators)
    sources = {s.source_id for s in stores}

    scratch = working_db.parent
    tmp_db = scratch / "committed.db.tmp"
    # {store.name: {key: tmp_path}} -- key is an indicator_id for a
    # one_csv_per_indicator store (one dump per file) or _WHOLE_STORE for a
    # single_csv one (one dump for the whole indicator list, unchanged from
    # before this pilot).
    tmp_csvs: dict[str, dict[str, Path]] = {
        s.name: (
            {i: scratch / f"{s.name}__{i}.csv.tmp" for i in s.indicators}
            if s.layout == LAYOUT_ONE_CSV_PER_INDICATOR
            else {_WHOLE_STORE: scratch / f"{s.path.name}.tmp"}
        )
        for s in stores
    }
    written: dict[str, int] = {}
    publish: list[tuple[Path, Path]] = []

    try:
        conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
        try:
            committed_conn = sqlite3.connect(f"file:{committed_db}?mode=ro", uri=True)
            try:
                _check_every_offloaded_source_indicator_is_declared(
                    conn, committed_conn, sources, set(declared)
                )
            finally:
                committed_conn.close()
            for store in stores:
                _check_no_committed_row_is_lost(conn, store)
            total = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            expected = {
                s.name: conn.execute(
                    f"SELECT COUNT(*) FROM observations "
                    f"WHERE indicator_id IN ({_placeholders(len(s.indicators))})",
                    s.indicators,
                ).fetchone()[0]
                for s in stores
            }
        finally:
            conn.close()

        # Dump. A one_csv_per_indicator store dumps one file per indicator
        # and, for an in_db store, writes NO file at all for an indicator
        # with zero rows -- a header-only CSV would make tomorrow's assemble
        # load nothing and look identical to "never fetched", which is not
        # the distinction that absence is for (Store.csv_paths()' own doc).
        for store in stores:
            store_total = 0
            for key, tmp_path in tmp_csvs[store.name].items():
                indicators = list(store.indicators) if key == _WHOLE_STORE else [key]
                n = export_observations(working_db, tmp_path, indicators)
                if key != _WHOLE_STORE and n == 0:
                    tmp_path.unlink(missing_ok=True)
                store_total += n
            if store_total != expected[store.name]:
                raise OffloadError(
                    f"{store.name}: dumped {store_total} rows but the working database counts "
                    f"{expected[store.name]} for the same indicators"
                )
            written[store.name] = store_total

        # Strip a copy. The backup API rather than a file copy, so a working db
        # left in WAL mode by a sync is copied as its real current state.
        tmp_db.unlink(missing_ok=True)
        src = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
        dst = sqlite3.connect(str(tmp_db))
        try:
            src.backup(dst)
        finally:
            src.close()
        try:
            dst.execute("PRAGMA foreign_keys=ON")
            dst.execute(
                f"DELETE FROM observations WHERE indicator_id IN ({_placeholders(len(declared))})",
                declared,
            )
            # The run rows build_staging_db.py opened to load the CSVs. Only their
            # now-deleted observations referenced them; a rebuild is not a fetch
            # and has no business in the committed audit trail.
            dst.execute("""DELETE FROM fetch_runs WHERE adapter = 'rebuild'
                   AND fetch_run_id NOT IN (SELECT DISTINCT fetch_run_id FROM observations)""")
            dst.commit()
            dst.execute("VACUUM")

            left = dst.execute(
                f"SELECT COUNT(*) FROM observations "
                f"WHERE indicator_id IN ({_placeholders(len(declared))})",
                declared,
            ).fetchone()[0]
            rebuilds = dst.execute(
                "SELECT COUNT(*) FROM fetch_runs WHERE adapter = 'rebuild'"
            ).fetchone()[0]
            remaining = dst.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
            fk = dst.execute("PRAGMA foreign_key_check").fetchall()
            integrity = dst.execute("PRAGMA integrity_check").fetchone()[0]
        finally:
            dst.close()

        offloaded = sum(written.values())
        if left:
            raise OffloadError(f"{left} offloaded row(s) survived the delete")
        if rebuilds:
            raise OffloadError(
                f"{rebuilds} rebuild fetch_runs row(s) are still referenced by rows that stay "
                "in the committed database -- something other than an in_db store was loaded "
                "as a rebuild"
            )
        if remaining != total - offloaded:
            raise OffloadError(
                f"committed database would hold {remaining} observations, expected "
                f"{total} - {offloaded} = {total - offloaded}"
            )
        if fk:
            raise OffloadError(f"foreign key violations in the stripped database: {fk[:5]}")
        if integrity != "ok":
            raise OffloadError(f"integrity_check on the stripped database: {integrity}")

        # Publish. CSVs first: if the process dies between the two, the old
        # committed database (which holds none of these rows either) plus the
        # new CSVs is still a consistent pair for the next assemble. Backed up
        # first, and put back on an exception (module docstring).
        for store in stores:
            for key, tmp_path in tmp_csvs[store.name].items():
                if not tmp_path.is_file():
                    continue  # a zero-row indicator: nothing to publish
                dest = store.path if key == _WHOLE_STORE else store.csv_for(key)
                publish.append((tmp_path, dest))
        publish.append((tmp_db, committed_db))
        backup_dir, backups = _back_up([dest for _, dest in publish], scratch)
        _publish_or_restore(publish, backup_dir, backups)
        _remove_backups(backup_dir)
    finally:
        for path in [tmp_db, *(p for d in tmp_csvs.values() for p in d.values())]:
            path.unlink(missing_ok=True)
        for _, dest in publish:
            dest.with_name(dest.name + RESTORE_SUFFIX).unlink(missing_ok=True)

    # Outside the publish: nothing below may undo a publish that completed.
    print(
        f"Offloaded {offloaded} observations to {len(stores)} store(s); "
        f"{committed_db} keeps {remaining}."
    )
    return written


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Dump every in_db store from the working db and write the slim committed db"
    )
    ap.add_argument("--working-db", default=str(DEFAULT_WORKING_DB))
    ap.add_argument("--committed-db", default=str(DEFAULT_COMMITTED_DB))
    ap.add_argument("--stores", default=str(DEFAULT_STORES_PATH))
    args = ap.parse_args()
    try:
        counts = offload(Path(args.working_db), Path(args.committed_db), Path(args.stores))
    except OffloadError as exc:
        print(f"\nOFFLOAD REFUSED: {exc}\nNo committed file was changed.\n", file=sys.stderr)
        return 1
    for name, n in counts.items():
        print(f"  {name}: {n} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
