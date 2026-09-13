"""
The one declaration of every committed observation store -- loads and
validates config/stores.yaml (docs/features/store_registry.schema.json).

WHY THIS EXISTS. Six committed CSVs were spelled out independently in six
places -- Makefile's EXTRA variable, two GitHub workflows' repeated
--extra-observations flags, scripts/validate_data.py's DEFAULT_EXPORTS, and
tests/test_committed_stores_are_consistent.py's MANUAL_STORES -- and three of
those six disagreed about which stores existed. This module is the one place
that now enumerates them; every consumer (the four exporters, validate_data.py,
the test suite, scripts/build_staging_db.py) reads it instead of keeping its
own copy.

`mode` matters more than any other field here -- see config/stores.yaml's own
comment and docs/decisions/0002-split-committed-stores.md. `in_db` stores are
loaded into the disposable working database that scripts/build_staging_db.py
assembles; `extra_csv` stores are merged at export time via
--extra-observations and never loaded into any database. A store in both
would be double-counted by export_communes_history_csv.py, which takes every
is_latest=1 row from the DB and every row from each extra CSV with no dedup.

Fails loudly (CLAUDE.md rule 13) rather than guessing: an unknown mode, a
missing path, or an `indicators` list that has drifted from the CSV's actual
contents all raise StoreConfigError, never a silent partial load.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import jsonschema
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STORES_PATH = REPO_ROOT / "config" / "stores.yaml"
SCHEMA_PATH = REPO_ROOT / "docs" / "features" / "store_registry.schema.json"

MODE_IN_DB = "in_db"
MODE_EXTRA_CSV = "extra_csv"
VALID_MODES = (MODE_IN_DB, MODE_EXTRA_CSV)


class StoreConfigError(Exception):
    """config/stores.yaml is missing, malformed, or has drifted from the
    committed CSVs it describes."""


@dataclass(frozen=True)
class ReferenceRows:
    script: str
    args: tuple[str, ...] = ("--reference-rows-only",)


LAYOUT_SINGLE_CSV = "single_csv"
LAYOUT_ONE_CSV_PER_INDICATOR = "one_csv_per_indicator"
VALID_LAYOUTS = (LAYOUT_SINGLE_CSV, LAYOUT_ONE_CSV_PER_INDICATOR)


@dataclass(frozen=True)
class Store:
    name: str
    # Resolved absolute path: a file (single_csv) or a directory (one_csv_per_indicator).
    path: Path
    source_id: str
    mode: str
    indicators: tuple[str, ...]
    reference_rows: ReferenceRows | None = None
    reference_rows_reason: str | None = None
    raw_path: str = field(default="", repr=False)  # repo-relative, as declared
    layout: str = LAYOUT_SINGLE_CSV

    def csv_for(self, indicator_id: str) -> Path:
        """The committed CSV holding `indicator_id`'s rows.

        single_csv: every indicator shares `self.path`. one_csv_per_indicator:
        `self.path` is a directory and the file is named after the indicator
        id exactly (never a `*_observations.csv` suffix -- the whole point of
        this layout is that the file list is the indicator list, not a second
        thing to keep in sync with it).
        """
        if self.layout == LAYOUT_ONE_CSV_PER_INDICATOR:
            return self.path / f"{indicator_id}.csv"
        return self.path

    def csv_paths(self) -> tuple[Path, ...]:
        """Every committed CSV this store actually has ON DISK right now.

        single_csv: `(self.path,)`, always -- load_stores() already refused a
        missing file for this layout. one_csv_per_indicator: one path per
        declared indicator THAT HAS A FILE, in declared order -- an in_db
        store's indicator with zero rows fetched so far has no file yet, and
        that is tolerated here exactly as verify_indicator_lists() tolerates
        it (see that function's docstring): callers that iterate csv_paths()
        to load or validate a store never need their own absent-file
        special-case.
        """
        if self.layout == LAYOUT_ONE_CSV_PER_INDICATOR:
            return tuple(p for i in self.indicators if (p := self.csv_for(i)).is_file())
        return (self.path,)


def _load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def load_stores(
    path: Path | str = DEFAULT_STORES_PATH, *, repo_root: Path = REPO_ROOT
) -> dict[str, Store]:
    """Load and validate config/stores.yaml. Returns {name: Store}, in the
    order declared in the file (dict insertion order).

    Validates, in order:
      1. the document against the JSON Schema (shape, required fields, enum);
      2. every `mode` is one of the two known values;
      3. every `path` exists on disk;
      4. `reference_rows` xor `reference_rows_reason` is null (never both,
         never neither) -- a store with no way to rebuild its reference rows
         must say why, not just be silent about it.

    Does NOT check indicators-vs-CSV-contents here -- that is
    verify_indicator_lists(), kept separate because it is the slow one (reads
    every store's full CSV) and callers that only need path/mode metadata
    (the exporters, on every run) should not pay for it.
    """
    path = Path(path)
    if not path.is_file():
        raise StoreConfigError(f"No store registry at {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    schema = _load_schema()
    validator = jsonschema.Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(data), key=str)
    if errors:
        formatted = "\n".join(
            f"  {'/'.join(str(p) for p in e.absolute_path) or '(root)'} -- {e.message}"
            for e in errors
        )
        raise StoreConfigError(f"{path} fails its schema:\n{formatted}")

    stores: dict[str, Store] = {}
    problems: list[str] = []
    for name, entry in data["stores"].items():
        mode = entry["mode"]
        if mode not in VALID_MODES:
            # Belt and braces -- the schema enum already rejects this, but a
            # future schema change should not silently widen what this loader
            # accepts.
            problems.append(f"{name}: unknown mode {mode!r}, expected one of {VALID_MODES}")
            continue

        layout = entry.get("layout", LAYOUT_SINGLE_CSV)
        if layout not in VALID_LAYOUTS:
            problems.append(f"{name}: unknown layout {layout!r}, expected one of {VALID_LAYOUTS}")
            continue

        raw_path = entry["path"]
        resolved = (repo_root / raw_path).resolve()
        if layout == LAYOUT_ONE_CSV_PER_INDICATOR:
            if not resolved.is_dir():
                problems.append(
                    f"{name}: path {raw_path!r} is not a directory ({resolved}) -- "
                    "layout: one_csv_per_indicator needs a directory, not a single file"
                )
        elif not resolved.is_file():
            problems.append(f"{name}: path {raw_path!r} does not exist ({resolved})")

        ref = entry.get("reference_rows")
        reason = entry.get("reference_rows_reason")
        if ref is None and reason is None:
            problems.append(
                f"{name}: reference_rows is null but reference_rows_reason is not set -- "
                "say why this store has no way to rebuild its reference rows"
            )
        if ref is not None and reason is not None:
            problems.append(
                f"{name}: both reference_rows and reference_rows_reason are set -- pick one"
            )

        reference_rows = (
            ReferenceRows(
                script=ref["script"], args=tuple(ref.get("args", ["--reference-rows-only"]))
            )
            if ref is not None
            else None
        )

        stores[name] = Store(
            name=name,
            path=resolved,
            source_id=entry["source_id"],
            mode=mode,
            indicators=tuple(entry["indicators"]),
            reference_rows=reference_rows,
            reference_rows_reason=reason,
            raw_path=raw_path,
            layout=layout,
        )

    if problems:
        raise StoreConfigError(f"{path} is invalid:\n" + "\n".join(f"  {p}" for p in problems))

    return stores


def extra_csv_stores(stores: dict[str, Store]) -> tuple[Store, ...]:
    """The extra_csv stores, sorted by name for deterministic ordering
    regardless of the dict's declaration order -- callers that build a CLI
    argument list from this need it stable across runs."""
    return tuple(s for _, s in sorted(stores.items()) if s.mode == MODE_EXTRA_CSV)


def in_db_stores(stores: dict[str, Store]) -> tuple[Store, ...]:
    return tuple(s for _, s in sorted(stores.items()) if s.mode == MODE_IN_DB)


def extra_csv_paths(path: Path | str = DEFAULT_STORES_PATH) -> tuple[Path, ...]:
    """Convenience for the exporters: the extra_csv stores' paths, in one
    call. Raises StoreConfigError the same way load_stores does."""
    return tuple(s.path for s in extra_csv_stores(load_stores(path)))


def resolve_extra_observations(
    explicit: list[str] | tuple[str, ...], stores_path: str | Path | None
) -> tuple[Path, ...]:
    """What the four exporters' --extra-observations should resolve to.

    EXPLICIT --extra-observations WINS OUTRIGHT, and the registry is not
    consulted at all in that case. That is what lets
    tests/test_committed_stores_are_consistent.py's subprocess test (which
    builds its own --extra-observations list from the registry and passes
    it explicitly) keep working unchanged: if both an explicit list AND the
    --stores default were merged, the same store would be read twice --
    export_communes_csv.py's obs list has no dedup, so every extra_csv
    store's rows would silently double.

    Only when NO --extra-observations were passed does this fall back to the
    extra_csv stores declared at `stores_path` -- this is what lets the
    Makefile and workflows stop repeating six --extra-observations flags per
    exporter call and pass nothing (relying on the --stores default) or an
    explicit --stores config/stores.yaml instead.

    `stores_path` falsy (None or "") disables the registry fallback entirely,
    for a caller that explicitly wants a bare export with nothing merged in.
    """
    if explicit:
        return tuple(Path(p) for p in explicit)
    if not stores_path:
        return ()
    return extra_csv_paths(stores_path)


def _actual_indicator_ids(csv_path: Path) -> set[str]:
    with csv_path.open(encoding="utf-8", newline="") as fh:
        return {row["indicator_id"] for row in csv.DictReader(fh)}


def verify_indicator_lists(stores: dict[str, Store]) -> list[str]:
    """For every store, the registry's declared `indicators` must equal the
    distinct indicator_id values actually present in its CSV(s) -- no more,
    no less. This is what makes PR2's DB-to-CSV offload safe: an indicator
    that silently stopped appearing (or a new one that silently started)
    would otherwise go unnoticed.

    A one_csv_per_indicator store is checked file by file (Store.csv_paths()):
    each file's own indicator_id column must equal the file's own indicator
    (stem == indicator_id is the whole point of the layout -- a CSV whose
    rows disagree with its own filename is drift too), on top of the same
    declared-vs-actual check every store gets.

    ONE ASYMMETRY, for in_db stores only: a declared indicator may have no
    rows yet -- for one_csv_per_indicator that means no file at all
    (csv_paths() already omits it). An in_db store's CSV is a dump of what
    the daily fetch has delivered so far, and an indicator is configured (and
    must be declared, or scripts/offload_stores.py refuses to run) before its
    first successful fetch -- UNEMPLOYMENT_RATE_BIT was exactly that on the
    day of the cutover. An extra_csv store is loaded by hand in one go, so
    there a declared-but-absent indicator is still drift. An undeclared
    indicator in a CSV is drift in both modes.

    Returns a list of human-readable problem strings; empty means clean.
    Does not raise, so a caller can report every store's drift in one pass
    rather than stopping at the first (CLAUDE.md rule 9's spirit -- see
    scripts/validate_data.py's own docstring on the same point).
    """
    problems = []
    for name, store in sorted(stores.items()):
        if store.layout == LAYOUT_ONE_CSV_PER_INDICATOR:
            actual: set[str] = set()
            for path in store.csv_paths():
                in_file = _actual_indicator_ids(path)
                stray = sorted(in_file - {path.stem})
                if stray:
                    problems.append(
                        f"{name}: {path.name} carries indicator_id(s) other than its own "
                        f"filename: {stray}"
                    )
                actual |= in_file
        else:
            actual = _actual_indicator_ids(store.path)
        declared = set(store.indicators)
        missing = sorted(actual - declared)
        extra = sorted(declared - actual)
        if missing:
            problems.append(f"{name}: CSV has indicator(s) not declared in the registry: {missing}")
        if extra and store.mode == MODE_EXTRA_CSV:
            problems.append(f"{name}: registry declares indicator(s) absent from the CSV: {extra}")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Load and validate config/stores.yaml -- the committed observation store registry"
    )
    ap.add_argument("--stores", default=str(DEFAULT_STORES_PATH))
    ap.add_argument(
        "--verify-indicators",
        action="store_true",
        help="Also check every store's `indicators` list against its CSV's real contents (slower: reads every store).",
    )
    args = ap.parse_args()

    try:
        stores = load_stores(args.stores)
    except StoreConfigError as exc:
        print(f"\nINVALID: {exc}\n", file=sys.stderr)
        return 1

    print(f"{len(stores)} store(s) declared in {args.stores}:\n")
    for name, store in stores.items():
        ref = (
            f"{store.reference_rows.script} {' '.join(store.reference_rows.args)}"
            if store.reference_rows
            else f"(none: {store.reference_rows_reason})"
        )
        print(
            f"  {name:20s} mode={store.mode:9s} source={store.source_id:14s} "
            f"indicators={len(store.indicators):3d} path={store.raw_path}"
        )
        print(f"                       reference_rows: {ref}")

    if args.verify_indicators:
        problems = verify_indicator_lists(stores)
        if problems:
            print("\nINDICATOR DRIFT:", file=sys.stderr)
            for p in problems:
                print(f"  {p}", file=sys.stderr)
            return 1
        print("\nEvery store's declared indicators match its CSV exactly.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
