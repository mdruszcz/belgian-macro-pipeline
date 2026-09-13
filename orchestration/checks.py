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
"""

import sqlite3
import tempfile
from pathlib import Path

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    Failure,
    multi_asset_check,
)

from orchestration.paths import PipelinePaths
from orchestration.scripts import import_script
from src.validation.rules import FAIL, RULES, Violation

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
