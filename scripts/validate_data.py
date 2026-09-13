"""
Run the validation rule catalogue against the committed stores -- Block H,
docs/features/validation.md.

EVERY STORE, NOT ONLY THE DATABASE (pipeline repair part 4). The rules query
SQLite, and the six hand-loaded stores in config/stores.yaml (`extra_csv`:
population, fiscal income, census, real estate, police, VAR) are never loaded
into any database -- they are merged at export time. Until part 4 they got a
field-count parse check and nothing else: no duplicate-latest check, no
bounds, no staleness. So this validates a throwaway copy of --db with every
extra_csv store loaded into it, the same rows the exporters publish. The
in_db stores (ONEM, WalStat) are already in --db. `--stores ''` validates
--db alone.

The copy is never kept and never written back, except the volume snapshot:
`--record-volume` stores the copy's counts in --db, because those are the
counts tomorrow's row_collapse check is compared with.

Exit codes are the whole point: a `fail` violation exits non-zero, which is
what stops the daily workflow before the export and commit steps. Validation
that only logs is decoration; the value is entirely in the blocking.

Prints EVERY violation rather than stopping at the first, because fixing five
problems one build at a time is how people give up on validation.
"""

import argparse
import sqlite3
import sys
import tempfile
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.exporters.provenance import DB_TO_CONFIG_SOURCE_ID  # noqa: E402
from src.stores import DEFAULT_STORES_PATH, extra_csv_stores, load_stores  # noqa: E402
from src.validation.config_schema import (  # noqa: E402
    load_and_validate_all,
    load_and_validate_derived,
)
from src.validation.rules import (  # noqa: E402
    FAIL,
    RULES,
    WARN,
    Context,
    Violation,
    has_failures,
    record_volume_snapshot,
    run_all,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
# THE WORKING COPY, NOT THE COMMITTED FILE (PR2 of the pipeline repair). Since
# ONEM and WalStat moved to committed CSVs, data/belgian_macro.db alone holds
# 1,939 of the ~86,000 observations; validating it would fire
# indicator_disappeared on eighteen indicators and fail every CI run. There is
# deliberately NO fallback to the committed file when the working copy is
# absent -- that would turn a hard failure into an intermittent one.
DEFAULT_DB = REPO_ROOT / "data" / "local" / "working.db"
DEFAULT_INDICATORS_DIR = REPO_ROOT / "config" / "indicators"
DEFAULT_SOURCES_DIR = REPO_ROOT / "config" / "sources"
DEFAULT_DERIVED_DIR = REPO_ROOT / "config" / "indicators" / "derived"


def _default_exports() -> tuple[Path, ...]:
    """The national/commune bulk exports, plus every registered store's
    committed CSV (extra_csv and in_db alike), read from config/stores.yaml
    (src/stores.py) rather than hand-listed here a fourth time.

    Used to be a hardcoded 6-path tuple naming only 3 of the (then) 6 manual
    stores -- census2021, realestate and police were missing, so those three
    were never parse-checked by this script even though they were exported
    and committed. The registry is now the one place the list is spelled
    out, so a new store is checked automatically once it is registered.
    """
    exports = [
        REPO_ROOT / "data" / "belgian_macro_export.csv",
        REPO_ROOT / "data" / "communes_export.csv",
        REPO_ROOT / "data" / "communes_history.csv",
    ]
    exports.extend(s.path for _, s in sorted(load_stores(DEFAULT_STORES_PATH).items()))
    return tuple(exports)


def _validation_copy(db_path: Path, stores_path: str, scratch: Path) -> tuple[Path, list]:
    """A copy of `db_path` with every extra_csv store loaded, and one FAIL
    violation per store that could not be loaded.

    A store that fails to load is reported like any other violation rather
    than raising, and the others still load, so one bad file does not hide
    the rest (this script's own rule: print every violation). The loader is
    scripts/load_observations_csv.py -- the same one that rebuilds a store --
    so a CSV this accepts is one the pipeline can actually read.
    """
    import load_observations_csv  # noqa: PLC0415 -- scripts/ is on sys.path when run

    copy = scratch / "validation.db"
    src = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    dst = sqlite3.connect(str(copy))
    try:
        src.backup(dst)
    finally:
        src.close()
        dst.close()

    problems = []
    if not stores_path:
        return copy, problems
    for store in extra_csv_stores(load_stores(stores_path)):
        try:
            load_observations_csv.load(
                copy, store.path, run_source_id=store.source_id, run_adapter="rebuild"
            )
        except (load_observations_csv.ObservationsCsvError, sqlite3.Error, ValueError) as exc:
            problems.append(
                Violation(
                    "store_loads",
                    FAIL,
                    f"{store.name} ({store.raw_path}) cannot be loaded: {exc}",
                )
            )
    return copy, problems


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate the committed data stores")
    ap.add_argument(
        "--db",
        default=str(DEFAULT_DB),
        help="Database to validate. Defaults to the assembled working copy "
        "(scripts/build_staging_db.py, `make assemble`).",
    )
    ap.add_argument(
        "--stores",
        default=str(DEFAULT_STORES_PATH),
        help="Store registry whose extra_csv stores are loaded into a throwaway copy of "
        "--db before validating. '' validates --db alone.",
    )
    ap.add_argument("--derived-dir", default=str(DEFAULT_DERIVED_DIR))
    ap.add_argument("--indicators-dir", default=str(DEFAULT_INDICATORS_DIR))
    ap.add_argument("--sources-dir", default=str(DEFAULT_SOURCES_DIR))
    ap.add_argument(
        "--record-volume",
        action="store_true",
        help=(
            "After a PASSING run, record the is_latest count per indicator so the next "
            "run can compare against it. Skipped on failure by design: recording a "
            "collapsed count makes it the new baseline and silences the alarm."
        ),
    )
    ap.add_argument(
        "--export",
        action="append",
        default=[],
        help="Published CSV to parse-check. Repeatable; defaults to the bulk "
        "exports plus every store in config/stores.yaml.",
    )
    ap.add_argument(
        "--summary-file",
        default=None,
        help=(
            "Append a markdown summary of every violation to this file. Point it at "
            "$GITHUB_STEP_SUMMARY so warnings appear on the run page instead of scrolling "
            "past in the log. Written even on a clean run, so 'checked, nothing wrong' and "
            "'never checked' are distinguishable."
        ),
    )
    ap.add_argument(
        "--warnings-as-errors",
        action="store_true",
        help="Treat warn as fail. Off by default -- see the severity rationale in the spec.",
    )
    args = ap.parse_args()

    if not Path(args.db).is_file():
        # sqlite3.connect would silently create an empty file and every rule
        # would then "pass" or fail for the wrong reason.
        print(
            f"No database at {args.db}. Run `make assemble` "
            "(python scripts/build_staging_db.py) first.",
            file=sys.stderr,
        )
        return 2

    # ignore_cleanup_errors: on Windows a connection the loader failed
    # half-way through can still hold the file when the directory is removed.
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as scratch:
        return _validate(args, Path(scratch))


def _validate(args, scratch: Path) -> int:
    validation_db, load_problems = _validation_copy(Path(args.db), args.stores, scratch)
    conn = sqlite3.connect(str(validation_db))
    conn.execute("PRAGMA foreign_keys=ON")

    violations = load_problems + run_all(
        build_context(
            conn,
            derived_dir=Path(args.derived_dir),
            indicators_dir=Path(args.indicators_dir),
            sources_dir=Path(args.sources_dir),
            exports=tuple(Path(p) for p in args.export),
        )
    )

    fails = [v for v in violations if v.severity == FAIL]
    warns = [v for v in violations if v.severity == WARN]

    for v in violations:
        stream = sys.stderr if v.severity == FAIL else sys.stdout
        print(f"::{'error' if v.severity == FAIL else 'warning'}::{v}", file=stream)

    print(f"\nValidation: {len(fails)} failure(s), {len(warns)} warning(s).")
    if not violations:
        print("All rules pass.")

    if args.summary_file:
        _write_summary(Path(args.summary_file), fails, warns)

    blocked = has_failures(violations) or (args.warnings_as_errors and warns)

    if args.record_volume and not blocked:
        n = record_volume(Path(args.db), counts_conn=conn)
        print(f"Recorded volume snapshot for {n} indicator(s) in {args.db}.")
    elif args.record_volume:
        print("Volume snapshot NOT recorded: this run failed, so the counts are not a baseline.")

    conn.close()
    return 1 if blocked else 0


def build_context(
    conn: sqlite3.Connection,
    *,
    derived_dir: Path = DEFAULT_DERIVED_DIR,
    indicators_dir: Path = DEFAULT_INDICATORS_DIR,
    sources_dir: Path = DEFAULT_SOURCES_DIR,
    exports: tuple[Path, ...] = (),
) -> Context:
    """The rule context for `conn`, from the same config this script's CLI reads.

    Shared with orchestration/checks.py, so the Dagster checks run the rule
    catalogue on exactly the context this script builds -- not a second copy
    of it. `exports` empty means the default list (_default_exports).
    """
    derived_ids = frozenset(
        load_and_validate_derived(derived_dir, _configured_indicator_ids(indicators_dir))
        if derived_dir.is_dir()
        else {}
    )
    return Context(
        conn=conn,
        derived_ids=derived_ids,
        exports=exports or _default_exports(),
        max_age_days=_staleness_allowances(indicators_dir, sources_dir),
        fetch_window_days=_fetch_windows(sources_dir),
    )


def record_volume(db_path: Path, counts_conn: sqlite3.Connection) -> int:
    """Record the volume snapshot into `db_path`, counted on `counts_conn`
    (the validation copy, which also holds the extra_csv stores). Only ever
    called after a passing run -- see --record-volume."""
    target = sqlite3.connect(str(db_path))
    try:
        return record_volume_snapshot(target, counts_conn=counts_conn)
    finally:
        target.close()


_CONFIG_TO_DB_SOURCE_ID = {v: k for k, v in DB_TO_CONFIG_SOURCE_ID.items()}


def _write_summary(path: Path, fails: list, warns: list) -> None:
    """A markdown summary of this run, appended to `path`.

    WHY THIS EXISTS. Warnings already print as ::warning:: annotations, which
    is not nothing -- but a daily run with warnings looks identical from the
    outside to a clean one, and nobody opens the log of a green build. The
    roadmap's reason for the stale-data alert is exactly that: "Sources go
    quiet without announcing it. You want to know before a client does."

    A CLEAN RUN WRITES A LINE TOO. Silence would make "validated, nothing
    wrong" indistinguishable from "the validation step never ran", which is
    the failure this is supposed to catch, one level up.
    """
    lines = ["## Data validation", ""]
    if not fails and not warns:
        lines.append(f"All {len(RULES)} rules pass. No failures, no warnings.")
    else:
        lines.append(
            f"**{len(fails)} failure(s), {len(warns)} warning(s)** across {len(RULES)} rules."
        )
        lines.append("")
        for label, group in (("Failures", fails), ("Warnings", warns)):
            if not group:
                continue
            lines.append(f"### {label}")
            lines.append("")
            lines.append("| Rule | Detail |")
            lines.append("| --- | --- |")
            for v in group:
                detail = v.message.replace("|", "\\|")
                suffix = f" ({v.count} rows)" if v.count > 1 else ""
                lines.append(f"| `{v.rule}` | {detail}{suffix} |")
            lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def _fetch_windows(sources_dir: Path) -> dict[str, int]:
    """`fetch_window_days` from the source configs, for the silence rule.

    Config rather than the `sources` table, and per source rather than parsed
    out of the free-text `cadence` string: turning "annual (manual, ad hoc)"
    into a number would be guessing at prose. Declaring a window is an
    editorial statement that CI is expected to fetch this source; a source
    without one is not checked, which is what keeps the hand-downloaded ones
    from warning forever.
    """
    if not sources_dir.is_dir():
        return {}

    # KEYED BY THE ID fetch_runs USES, not the one the config declares. Two of
    # them differ -- config `dbnomics_eurostat`/`dbnomics_ameco` versus
    # `eurostat`/`ameco_ec` in the database -- and reading the config id
    # straight would report both as "never fetched" when they are fetched
    # daily. The alias map lives in src/exporters/provenance.py, which already
    # owns the config-to-database source mapping and documents the evidence;
    # a second copy here is exactly how the two would drift.
    windows = {}
    for path in sorted(sources_dir.glob("*.yaml")):
        cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        days = cfg.get("fetch_window_days")
        if days is None:
            continue
        config_id = cfg["source_id"]
        db_id = _CONFIG_TO_DB_SOURCE_ID.get(config_id, config_id)
        windows[db_id] = int(days)
    return windows


def _staleness_allowances(indicators_dir: Path, sources_dir: Path) -> dict[str, int]:
    """`max_age_days` from the indicator configs, for the staleness rule.

    Config, not the database: the allowance is an editorial judgement about a
    source's publication behaviour that belongs next to the rest of the
    indicator's definition, and the indicators table has no column for it.
    """
    if not indicators_dir.is_dir() or not sources_dir.is_dir():
        return {}
    indicators, _sources = load_and_validate_all(indicators_dir, sources_dir)
    return {code: cfg["max_age_days"] for code, cfg in indicators.items() if "max_age_days" in cfg}


def _configured_indicator_ids(indicators_dir: Path) -> set[str]:
    """Known indicator ids from CONFIG, not from the database.

    Was reading the `indicators` table, which conflates "configured" with
    "already loaded" and made validation depend on load order: adding a
    derived indicator whose inputs are a manual-only source failed the build
    until someone happened to run the loader. Config is the declaration; the
    database is one consequence of it.

    Nothing is lost by not checking the database here. An input that is
    configured but never loaded is caught at compute time by the engine's
    own UnknownInputError, which refuses to "compute a column that would be
    null for every row" -- a better check, because it fires on the actual
    data rather than on a reference table.
    """
    ids = set()
    for path in sorted(indicators_dir.glob("*.yaml")):
        ids.add(yaml.safe_load(path.read_text(encoding="utf-8"))["id"])
    return ids


if __name__ == "__main__":
    sys.exit(main())
