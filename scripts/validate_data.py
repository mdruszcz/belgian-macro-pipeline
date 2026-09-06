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

from src.validation.config_schema import load_and_validate_derived  # noqa: E402
from src.validation.rules import FAIL, WARN, Context, has_failures, run_all  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
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

    violations = run_all(Context(conn=conn, derived_ids=derived_ids, exports=exports))
    conn.close()

    fails = [v for v in violations if v.severity == FAIL]
    warns = [v for v in violations if v.severity == WARN]

    for v in violations:
        stream = sys.stderr if v.severity == FAIL else sys.stdout
        print(f"::{'error' if v.severity == FAIL else 'warning'}::{v}", file=stream)

    print(f"\nValidation: {len(fails)} failure(s), {len(warns)} warning(s).")
    if not violations:
        print("All rules pass.")

    if has_failures(violations) or (args.warnings_as_errors and warns):
        return 1
    return 0


def _known_indicator_ids(conn: sqlite3.Connection) -> set[str]:
    return {r[0] for r in conn.execute("SELECT indicator_id FROM indicators")}


if __name__ == "__main__":
    sys.exit(main())
