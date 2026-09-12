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

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.exporters.provenance import DB_TO_CONFIG_SOURCE_ID  # noqa: E402
from src.validation.config_schema import (  # noqa: E402
    load_and_validate_all,
    load_and_validate_derived,
)
from src.validation.rules import (  # noqa: E402
    FAIL,
    RULES,
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
    REPO_ROOT / "data" / "fiscal_income_observations.csv",
    REPO_ROOT / "data" / "var_unemployment_observations.csv",
    REPO_ROOT / "data" / "communes_history.csv",
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

    conn = sqlite3.connect(str(args.db))
    conn.execute("PRAGMA foreign_keys=ON")

    derived_dir = Path(args.derived_dir)
    indicators_dir = Path(args.indicators_dir)
    derived_ids = frozenset(
        load_and_validate_derived(derived_dir, _configured_indicator_ids(indicators_dir))
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
            fetch_window_days=_fetch_windows(Path(args.sources_dir)),
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
        n = record_volume_snapshot(conn)
        print(f"Recorded volume snapshot for {n} indicator(s).")
    elif args.record_volume:
        print("Volume snapshot NOT recorded: this run failed, so the counts are not a baseline.")

    conn.close()
    return 1 if blocked else 0


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
