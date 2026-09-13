"""The daily sequence: what daily_fetch.yml runs in the runner, and
`make dagster-daily` runs locally.

    assemble_working_database -> fetch_sources -> validate_and_export

A plain Make recipe stops at the first failing command, and one Dagster run
blocks everything downstream of a failed asset. Today's workflow does
neither: a source that fails is recorded, the other sources and every export
still run, and the run ends red. This coordinator reproduces that:

  1. writes a per-run source manifest, every outcome `not_run`;
  2. stops (exit 1, no exports) if the working database cannot be assembled --
     the workflow's assemble step has no continue-on-error either;
  3. runs fetch_sources and records each of the eight tracked outcomes from
     the Dagster run result (not from fetch_runs: fetch_stocks.py never
     writes there, and a crash can come before any row is written);
  4. ALWAYS runs validate_and_export, handing it this run's manifest path;
  5. exits with one of three codes:
       0  everything green;
       3  exported, but a tracked outcome was not a success -- daily_fetch.yml
          goes on to offload and open the PR, which does not auto-merge;
       1  nothing publishable: assemble or validate_and_export failed (a
          blocking check included) -- the workflow stops, as it always has.

With --github-output FILE (the runner's $GITHUB_OUTPUT) it appends
`manifest=<path>` as soon as the manifest exists, so the auto-merge gate
reads this run's file and no other (python -m orchestration.manifest).
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dagster import DagsterInstance, Definitions

from orchestration import manifest
from orchestration.commands import TRACKED
from orchestration.paths import PipelinePaths

TAG = "belpulse/coordinator_run_id"

EXIT_OK = 0
EXIT_FAILED = 1
# Not 2: argparse and validate_data.py already use 2 for "refused to start".
EXIT_PARTIAL = 3


def _execute(defs: Definitions, job_name: str, instance, **kwargs):
    return defs.resolve_job_def(job_name).execute_in_process(
        instance=instance, raise_on_error=False, **kwargs
    )


def _root_message(event) -> str:
    error = event.event_specific_data.error
    while error.cause is not None:
        error = error.cause
    return error.message.strip().splitlines()[0][:500] if error.message else ""


def outcome(result, asset_name: str) -> dict:
    materialized = {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}
    if asset_name in materialized:
        return {"status": manifest.SUCCESS}
    for event in result.get_step_failure_events():
        if event.step_key == asset_name:
            return {"status": manifest.FAILED, "message": _root_message(event)}
    # Neither materialised nor failed: an upstream asset failed in this run.
    return {"status": manifest.SKIPPED}


def run_daily(
    defs: Definitions,
    paths: PipelinePaths,
    instance,
    now: datetime | None = None,
    github_output: Path | None = None,
) -> int:
    run_id = manifest.new_run_id(now)
    runs_dir = paths.resolve(paths.runs_dir)
    path = manifest.path_for(runs_dir, run_id)
    state = manifest.initial(run_id, now)
    manifest.write(path, state)
    if github_output is not None:
        with github_output.open("a", encoding="utf-8") as fh:
            fh.write(f"manifest={path}\n")
    tags = {TAG: run_id}
    print(f"Coordinator run {run_id}; source manifest {path}")

    # 1. Assemble. Without the working database there is nothing to export from.
    try:
        result = _execute(defs, "assemble_working_database", instance, tags=tags)
        state["assemble"] = outcome(result, "staging_db")
    except Exception as exc:  # noqa: BLE001 -- recorded, then the run stops red
        state["assemble"] = {"status": manifest.FAILED, "message": f"job crashed: {exc}"[:500]}
    manifest.write(path, state)
    if state["assemble"]["status"] != manifest.SUCCESS:
        print(f"Assemble failed: {state['assemble'].get('message', '')}. Nothing exported.")
        manifest.prune(runs_dir)
        return EXIT_FAILED

    # 2. Fetch. A crash leaves every outcome `not_run`, which counts as red.
    try:
        result = _execute(defs, "fetch_sources", instance, tags=tags)
        for name in TRACKED:
            state["sources"][name].update(outcome(result, name))
    except Exception as exc:  # noqa: BLE001 -- the exports still run
        state["fetch_crash"] = f"{exc}"[:500]
    manifest.write(path, state)

    # 3. Validate and export, always, on whatever the working database now holds.
    run_config = {
        "ops": {
            "validated_working_database": {
                "config": {"source_manifest": str(path), "coordinator_run_id": run_id}
            }
        }
    }
    try:
        result = _execute(defs, "validate_and_export", instance, tags=tags, run_config=run_config)
        state["validate_and_export"] = {
            "status": manifest.SUCCESS if result.success else manifest.FAILED
        }
    except Exception as exc:  # noqa: BLE001
        state["validate_and_export"] = {"status": manifest.FAILED, "message": f"{exc}"[:500]}
    state["finished_at"] = datetime.now(timezone.utc).isoformat()
    manifest.write(path, state)
    manifest.prune(runs_dir)

    red = manifest.red_sources(state)
    exported = state["validate_and_export"]["status"] == manifest.SUCCESS
    for name in TRACKED:
        s = state["sources"][name]
        print(f"  {name:<26} {s['status']}{': ' + s['message'] if s.get('message') else ''}")
    print(f"  {'validate_and_export':<26} {state['validate_and_export']['status']}")
    if not exported:
        return EXIT_FAILED
    return EXIT_PARTIAL if red else EXIT_OK


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Assemble, fetch, validate and export via Dagster")
    ap.add_argument(
        "--github-output",
        type=Path,
        default=None,
        help="Append manifest=<path> to this file (the runner's $GITHUB_OUTPUT).",
    )
    args = ap.parse_args(argv)

    from orchestration import defs
    from orchestration.definitions import default_paths

    paths = default_paths()
    home = paths.resolve("data/local/dagster_home")
    os.environ.setdefault("DAGSTER_HOME", str(home))
    os.makedirs(os.environ["DAGSTER_HOME"], exist_ok=True)
    with DagsterInstance.get() as instance:
        return run_daily(defs, paths, instance, github_output=args.github_output)


if __name__ == "__main__":
    sys.exit(main())
