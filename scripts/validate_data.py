"""
Run the validation rule catalogue against the committed stores -- Block H,
docs/features/validation.md.

Exit codes are the whole point: a `fail` violation exits non-zero, which is
what stops the daily workflow before the export and commit steps. Validation
that only logs is decoration; the value is entirely in the blocking.

Prints EVERY violation rather than stopping at the first, because fixing five
problems one build at a time is how people give up on validation.
"""

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.validation.config_schema import (  # noqa: E402
    load_and_validate_all,
    load_and_validate_derived,
)
from src.validation.rules import (  # noqa: E402
    FAIL,
    WARN,
    Context,
    has_failures,
    record_volume_snapshot,
    run_all,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INDICATORS_DIR = REPO_ROOT / "config" / "indicators"
DEFAULT_SOURCES_DIR = REPO_ROOT / "config" / "sources"
DEFAULT_DERIVED_DIR = REPO_ROOT / "config" / "indicators" / "derived"
DEFAULT_EXPORTS = (
    REPO_ROOT / "data" / "belgian_macro_export.csv",
    REPO_ROOT / "data" / "communes_export.csv",
    REPO_ROOT / "data" / "population_observations.csv",
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate the committed data stores")
    ap.add_argument("--db", default=str(REPO_ROOT / "data" / "belgian_macro.db"))
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
        help="Published CSV to parse-check. Repeatable; defaults to all three.",
    )
    ap.add_argument(
        "--warnings-as-errors",
        action="store_true",
        help="Treat warn as fail. Off by default -- see the severity rationale in the spec.",
    )
    args = ap.parse_args()

    conn = sqlite3.connect(str(args.db))
    conn.execute("PRAGMA foreign_keys=ON")

    derived_dir = Path(args.derived_dir)
    derived_ids = frozenset(
        load_and_validate_derived(derived_dir, _known_indicator_ids(conn))
        if derived_dir.is_dir()
        else {}
    )
    exports = tuple(Path(p) for p in args.export) or DEFAULT_EXPORTS

    violations = run_all(
        Context(
            conn=conn,
            derived_ids=derived_ids,
            exports=exports,
            max_age_days=_staleness_allowances(Path(args.indicators_dir), Path(args.sources_dir)),
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

    blocked = has_failures(violations) or (args.warnings_as_errors and warns)

    if args.record_volume and not blocked:
        n = record_volume_snapshot(conn)
        print(f"Recorded volume snapshot for {n} indicator(s).")
    elif args.record_volume:
        print("Volume snapshot NOT recorded: this run failed, so the counts are not a baseline.")

    conn.close()
    return 1 if blocked else 0


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


def _known_indicator_ids(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT indicator_id FROM indicators")}


if __name__ == "__main__":
    sys.exit(main())
