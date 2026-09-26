"""The Dagster code location: assets, checks, jobs and the (stopped) schedule.

Jobs:
  assemble_working_database  staging_db alone. The coordinator stops if it fails,
                             as the workflow does (no continue-on-error there).
  fetch_sources              the eight outcomes the workflow gates auto-merge on.
                             One failing does not stop the others.
  validate_and_export        the validated database, its checks, and everything
                             built from it. Selects no source, so a red source
                             never blocks it -- the workflow's continue-on-error.
                             Writes no committed file: what the UI offers.
  validate_export_and_offload
                             the same, plus committed_stores (the offload), in ONE
                             run: a failed export or blocking check skips it in
                             that run. What the coordinator runs; committed_stores
                             refuses without the coordinator's run config.
  observe_manual_sources     observations of the hand-loaded stores.

The git/PR steps are not in this graph: they stay in daily_fetch.yml, after the
coordinator.
"""

import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    in_process_executor,
)

from orchestration.assets import canonical, derived, reference_data, sources_api, website
from orchestration.assets.sources_manual import SPECS as MANUAL_SPECS
from orchestration.assets.sources_manual import observe_manual_sources
from orchestration.checks import validation_rules
from orchestration.commands import TRACKED
from orchestration.paths import DEFAULT_WORKING_DB, PipelinePaths

DAILY_CRON = "0 5 * * *"  # daily_fetch.yml's cron, UTC

ASSETS = [
    *reference_data.ASSETS,
    *sources_api.ASSETS,
    *canonical.ASSETS,
    *derived.ASSETS,
    *website.ASSETS,
    *MANUAL_SPECS,
]

assemble_working_database = define_asset_job(
    "assemble_working_database", selection=AssetSelection.assets("staging_db")
)
fetch_sources = define_asset_job("fetch_sources", selection=AssetSelection.assets(*TRACKED))
EXPORT_SELECTION = AssetSelection.assets(
    "validated_working_database",
    "volume_history",
    "revisions_report",
    "commune_adjacency",
    "commune_typology",
    "commune_flows_export",
    "schools_ise_export",
    "schools_by_commune_export",
) | AssetSelection.groups("derived", "website")

validate_and_export = define_asset_job("validate_and_export", selection=EXPORT_SELECTION)
validate_export_and_offload = define_asset_job(
    "validate_export_and_offload",
    selection=EXPORT_SELECTION | AssetSelection.assets("committed_stores"),
)

daily_fetch_schedule = ScheduleDefinition(
    name="daily_fetch_sources",
    cron_schedule=DAILY_CRON,
    execution_timezone="UTC",
    job=fetch_sources,
    # Declared, never running: GitHub Actions is the scheduler, and starts
    # orchestration.daily itself. Only starts if
    # someone switches it on in a local UI with a daemon running.
    default_status=DefaultScheduleStatus.STOPPED,
    description=(
        "Local only and stopped. Mirrors daily_fetch.yml's cron; production runs stay on "
        "GitHub Actions. Use `make dagster-daily` for the full fetch -> validate -> export."
    ),
)

JOBS = [
    assemble_working_database,
    fetch_sources,
    validate_and_export,
    validate_export_and_offload,
    observe_manual_sources,
]


def default_paths() -> PipelinePaths:
    return PipelinePaths(
        working_db=os.environ.get("WORKING_DB", DEFAULT_WORKING_DB),
        build_id=os.environ.get("BUILD_ID", "local"),
        validation_summary=os.environ.get("VALIDATION_SUMMARY", ""),
    )


def build_defs(paths: PipelinePaths | None = None) -> Definitions:
    return Definitions(
        assets=ASSETS,
        asset_checks=[validation_rules],
        jobs=JOBS,
        schedules=[daily_fetch_schedule],
        resources={"paths": paths or default_paths()},
        # One SQLite working copy: never two assets writing it at once.
        executor=in_process_executor,
    )
