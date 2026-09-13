"""The validation rules as asset checks on validated_working_database.

No rule is reimplemented here. run_validation() does what
scripts/validate_data.py does -- a throwaway copy of the working database with
the extra_csv stores loaded (_validation_copy), the same rule context
(build_context), the same catalogue (run_all) -- and each registered rule
becomes one check. FAIL rules are blocking: when one fails, nothing
downstream of the validated database is built in that run, which is what the
workflow's validation step does today. WARN rules are reported and never block.

`store_loads` is not in RULES: it is the violation _validation_copy reports
when a hand-loaded store cannot be loaded (pipeline repair part 4). It is a
FAIL there, so it is a blocking check here.

What the workflow's validation step also produced is kept: the ::error:: /
::warning:: annotation per violation, and validate_data.py's own markdown
summary when PipelinePaths.validation_summary names a file.
"""

import sqlite3
import tempfile
from pathlib import Path

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetKey,
    DagsterEventType,
    Failure,
    multi_asset_check,
)

from orchestration.paths import PipelinePaths
from orchestration.scripts import import_script
from src.validation.rules import FAIL, RULES, WARN, Violation

ASSET = "validated_working_database"
STORE_LOADS = "store_loads"

# name -> severity, from the rule registry itself.
SEVERITIES: dict[str, str] = {STORE_LOADS: FAIL, **{name: sev for name, (sev, _) in RULES.items()}}


def run_validation(paths: PipelinePaths) -> list[Violation]:
    validate_data = import_script("validate_data")
    db = paths.resolve(paths.working_db)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as scratch:
        copy, violations = validate_data._validation_copy(
            db, str(paths.resolve(paths.stores)), Path(scratch)
        )
        conn = sqlite3.connect(str(copy))
        try:
            conn.execute("PRAGMA foreign_keys=ON")
            violations = violations + validate_data.run_all(validate_data.build_context(conn))
        finally:
            conn.close()
    return violations


def record_volume(paths: PipelinePaths) -> int:
    """--record-volume, for the volume_history asset: counted on a fresh
    validation copy, written into the working database."""
    validate_data = import_script("validate_data")
    db = paths.resolve(paths.working_db)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as scratch:
        copy, _ = validate_data._validation_copy(
            db, str(paths.resolve(paths.stores)), Path(scratch)
        )
        conn = sqlite3.connect(str(copy))
        try:
            return validate_data.record_volume(db, counts_conn=conn)
        finally:
            conn.close()


@multi_asset_check(
    name="validation_rules",
    specs=[
        AssetCheckSpec(name, asset=ASSET, blocking=severity == FAIL, description=f"{severity} rule")
        for name, severity in sorted(SEVERITIES.items())
    ],
)
def validation_rules(context: AssetCheckExecutionContext, paths: PipelinePaths):
    violations = run_validation(paths)
    report(paths, violations)
    unknown = sorted({v.rule for v in violations} - set(SEVERITIES))
    if unknown:
        # A rule this module does not know is a schema change in the validator:
        # fail loudly rather than drop its violations (CLAUDE.md rule 13).
        raise Failure(f"violations from unregistered rules: {', '.join(unknown)}")
    for name, severity in sorted(SEVERITIES.items()):
        mine = [v for v in violations if v.rule == name]
        yield AssetCheckResult(
            check_name=name,
            asset_key=ASSET,
            passed=not mine,
            severity=AssetCheckSeverity.ERROR if severity == FAIL else AssetCheckSeverity.WARN,
            metadata={
                "violations": len(mine),
                "rows": sum(v.count for v in mine),
                "detail": "\n".join(v.message for v in mine)[:4000] or "none",
            },
        )


def report(paths: PipelinePaths, violations: list[Violation]) -> None:
    """What validate_data.py prints and writes, before any result is yielded --
    so a failing run still leaves its summary, as the workflow step made sure."""
    fails = [v for v in violations if v.severity == FAIL]
    warns = [v for v in violations if v.severity == WARN]
    for v in violations:
        print(f"::{'error' if v.severity == FAIL else 'warning'}::{v}", flush=True)
    print(f"Validation: {len(fails)} failure(s), {len(warns)} warning(s).", flush=True)
    if paths.validation_summary:
        import_script("validate_data")._write_summary(Path(paths.validation_summary), fails, warns)


def validation_status(instance, run_id: str) -> str:
    """The manifest's validation_status, from the check evaluations of one run.

    pass     every check ran in this run and no FAIL check failed (warnings
             do not count, as validate_data.py exits 0 on warnings);
    fail     a FAIL check failed -- in practice never published, since the
             checks are blocking and nothing downstream runs;
    unknown  the checks did not all run in this run (an export materialised
             on its own), which is what `make exports` reports too.
    """
    evaluations = {}
    for entry in instance.all_logs(run_id, of_type=DagsterEventType.ASSET_CHECK_EVALUATION):
        evaluation = entry.dagster_event.event_specific_data
        if evaluation.asset_key == AssetKey(ASSET):
            evaluations[evaluation.check_name] = evaluation
    if any(not e.passed and SEVERITIES.get(n) == FAIL for n, e in evaluations.items()):
        return "fail"
    if set(evaluations) != set(SEVERITIES):
        return "unknown"
    return "pass"
