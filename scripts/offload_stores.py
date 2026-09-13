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
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import sys
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


class OffloadError(Exception):
    pass


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


def _check_every_offloaded_source_indicator_is_declared(
    conn: sqlite3.Connection, sources: set[str], declared: set[str]
) -> None:
    present = {
        row[0]
        for row in conn.execute(
            f"""SELECT DISTINCT o.indicator_id
                FROM observations o JOIN indicators i USING (indicator_id)
                WHERE i.source_id IN ({_placeholders(len(sources))})""",
            sorted(sources),
        )
    }
    undeclared = sorted(present - declared)
    if undeclared:
        raise OffloadError(
            f"the working database holds rows for {undeclared}, whose source is offloaded "
            f"({sorted(sources)}) but which no in_db store in config/stores.yaml declares. "
            "Add them to the right store's `indicators` -- refusing to leave them to "
            "re-grow the committed database."
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


def offload(
    working_db: Path = DEFAULT_WORKING_DB,
    committed_db: Path = DEFAULT_COMMITTED_DB,
    stores_path: Path = DEFAULT_STORES_PATH,
) -> dict[str, int]:
    """Returns {store name: rows written to its CSV}."""
    if not working_db.is_file():
        raise OffloadError(f"No working database at {working_db} -- run build_staging_db.py first")

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

    try:
        conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
        try:
            _check_every_offloaded_source_indicator_is_declared(conn, sources, set(declared))
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
        # new CSVs is still a consistent pair for the next assemble.
        for store in stores:
            for key, tmp_path in tmp_csvs[store.name].items():
                if not tmp_path.is_file():
                    continue  # a zero-row indicator: nothing to publish
                dest = store.path if key == _WHOLE_STORE else store.csv_for(key)
                os.replace(tmp_path, dest)
        os.replace(tmp_db, committed_db)
        print(
            f"Offloaded {offloaded} observations to {len(stores)} store(s); "
            f"{committed_db} keeps {remaining}."
        )
        return written
    finally:
        for path in [tmp_db, *(p for d in tmp_csvs.values() for p in d.values())]:
            path.unlink(missing_ok=True)


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
