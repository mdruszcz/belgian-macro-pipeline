"""canonical: the validated working database, and the two things that only
make sense once validation has passed.

validated_working_database does no work. It is the point every source feeds
into and every export hangs off, and the asset the validation checks are
attached to. Its metadata says which source run it was built after -- only
the run it was explicitly given, never whatever manifest happens to be on disk.
"""

import json
from pathlib import Path

from dagster import AssetExecutionContext, Config, Failure, MaterializeResult, MetadataValue, asset

from orchestration import checks, manifest, run
from orchestration.assets import script_asset
from orchestration.assets.sources_manual import SPECS as MANUAL_SPECS
from orchestration.commands import COMMANDS, TRACKED
from orchestration.paths import PipelinePaths


class SourceRunConfig(Config):
    # Both set by the coordinator (orchestration/daily.py); empty on a manual run.
    source_manifest: str = ""
    coordinator_run_id: str = ""


def source_run_metadata(config: SourceRunConfig) -> dict:
    if not config.source_manifest:
        return {"source_run": manifest.NO_SOURCE_RUN}
    path = Path(config.source_manifest)
    if not path.is_file():
        # The coordinator wrote it before starting this run; if it is gone,
        # something broke, and "no source run" would be a lie.
        raise Failure(f"source manifest {path} does not exist")
    data = manifest.load(path)
    if config.coordinator_run_id and data.get("run_id") != config.coordinator_run_id:
        raise Failure(
            f"source manifest {path} belongs to run {data.get('run_id')}, "
            f"not {config.coordinator_run_id}"
        )
    red = manifest.red_sources(data)
    return {
        "source_run": data["run_id"],
        "red_sources": len(red),
        "red_source_names": ", ".join(red) or "none",
        "source_statuses": MetadataValue.json(data["sources"]),
    }


@asset(
    group_name="canonical",
    deps=["staging_db", *TRACKED, *(spec.key for spec in MANUAL_SPECS)],
    kinds={"sqlite"},
    description=(
        "The working database once every source has run. Carries the validation checks; "
        "every export is built from it."
    ),
)
def validated_working_database(
    context: AssetExecutionContext, config: SourceRunConfig, paths: PipelinePaths
) -> MaterializeResult:
    db = paths.resolve(paths.working_db)
    if not db.is_file():
        raise Failure(f"No working database at {db}. Materialise staging_db first.")
    source_ids = tuple(dict.fromkeys(s for n in TRACKED for s in COMMANDS[n].source_ids))
    metadata = {"working_db": str(db), **source_run_metadata(config)}
    metadata.update(run.source_snapshot(db, source_ids))
    context.log.info(json.dumps({k: str(v) for k, v in metadata.items()}, indent=2))
    return MaterializeResult(metadata=metadata)


@asset(
    group_name="canonical",
    deps=["validated_working_database"],
    description=(
        "Today's is_latest count per indicator, the baseline for tomorrow's row_collapse rule. "
        "Only recorded when every blocking check passed (validate_data.py --record-volume)."
    ),
)
def volume_history(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
    n = checks.record_volume(paths)
    return MaterializeResult(metadata={"indicators recorded": n})


revisions_report = script_asset(
    "revisions_report",
    group="canonical",
    deps=["validated_working_database"],
    description="Values revised today (scripts/revisions_report.py). Prints; writes no file.",
)

ASSETS = [validated_working_database, volume_history, revisions_report]
