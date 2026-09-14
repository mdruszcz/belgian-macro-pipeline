"""The daily sequence: what daily_fetch.yml runs in the runner, and
`make dagster-daily` runs locally.

    assemble_working_database -> fetch_sources -> validate_export_and_offload

A plain Make recipe stops at the first failing command, and one Dagster run
blocks everything downstream of a failed asset. The daily run needs neither:
a source that fails is recorded, the other sources and every export still
run, and the run ends red. This coordinator does that:

  1. writes a per-run manifest, every outcome `not_run`;
  2. stops (exit 1, no exports) if the working database cannot be assembled;
  3. runs fetch_sources and records each of the eight tracked outcomes from
     the Dagster run result (not from fetch_runs: fetch_stocks.py never
     writes there, and a crash can come before any row is written) -- unless
     --without-fetch;
  4. ALWAYS runs validate_export_and_offload, handing it this run's manifest:
     the checks, every export, and then committed_stores -- the offload, the
     only writer of data/belgian_macro.db and the in_db CSVs -- in the same
     Dagster run, so a failed export or blocking check skips it. This is the
     only caller that allows committed_stores to write, for its own run id;
  5. exits with one of three codes:
       0  everything green, offloaded;
       3  exported and offloaded, but a tracked outcome was not a success (or
          --without-fetch) -- daily_fetch.yml opens the PR, which does not
          auto-merge;
       1  nothing publishable: assemble, a blocking check, an export or the
          offload failed -- the workflow stops before committing anything. A
          refused offload changed no file; one that crashed while replacing
          its files may have replaced some of them (see committed_stores).

With --github-output FILE (the runner's $GITHUB_OUTPUT) it appends
`manifest=<path>` as soon as the manifest exists, so the auto-merge gate
reads this run's file and no other (python -m orchestration.manifest).

--without-fetch assembles, validates, exports and offloads what is committed,
with no network: for local trials and scripts/verify_dagster_parity.py. The
workflow never passes it.
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

# validate_and_export plus the offload, in one Dagster run.
PRODUCTION_JOB = "validate_export_and_offload"
OFFLOAD = "committed_stores"
# The two outcomes without which nothing is publishable.
PUBLISHED = ("validate_and_export", "offload")


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


def export_outcome(defs: Definitions, result) -> dict:
    """validate_and_export's outcome inside the production run: a success only if
    every asset that job builds materialised and no step but the offload failed
    -- what that job's own result.success meant when it ran on its own."""
    keys = defs.resolve_job_def("validate_and_export").asset_layer.selected_asset_keys
    materialized = {e.asset_key.to_user_string() for e in result.get_asset_materialization_events()}
    failures = [e for e in result.get_step_failure_events() if e.step_key != OFFLOAD]
    if failures:
        first = failures[0]
        return {"status": manifest.FAILED, "message": f"{first.step_key}: {_root_message(first)}"}
    if not {k.to_user_string() for k in keys} <= materialized:
        return {"status": manifest.FAILED, "message": "not every export was materialised"}
    return {"status": manifest.SUCCESS}


def run_daily(
    defs: Definitions,
    paths: PipelinePaths,
    instance,
    now: datetime | None = None,
    github_output: Path | None = None,
    fetch: bool = True,
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
    if fetch:
        try:
            result = _execute(defs, "fetch_sources", instance, tags=tags)
            for name in TRACKED:
                state["sources"][name].update(outcome(result, name))
        except Exception as exc:  # noqa: BLE001 -- the exports still run
            state["fetch_crash"] = f"{exc}"[:500]
    else:
        state["fetch_skipped"] = "--without-fetch"
        print("Fetch skipped (--without-fetch): every source stays not_run.")
    manifest.write(path, state)

    # 3. Validate, export and offload, always, on whatever the working database
    # now holds -- in ONE Dagster run, so a failed export or blocking check
    # skips the offload in that same run. Only this run config lets
    # committed_stores write, and only for this run's id and manifest.
    handed = {"source_manifest": str(path), "coordinator_run_id": run_id}
    run_config = {
        "ops": {
            "validated_working_database": {"config": handed},
            OFFLOAD: {"config": {"allow_committed_writes": True, **handed}},
        }
    }
    try:
        result = _execute(defs, PRODUCTION_JOB, instance, tags=tags, run_config=run_config)
        state["validate_and_export"] = export_outcome(defs, result)
        state["offload"] = outcome(result, OFFLOAD)
    except Exception as exc:  # noqa: BLE001 -- recorded; the run ends red
        state["validate_and_export"] = {
            "status": manifest.FAILED,
            "message": f"job crashed: {exc}"[:500],
        }
    state["finished_at"] = datetime.now(timezone.utc).isoformat()
    manifest.write(path, state)
    manifest.prune(runs_dir)

    red = manifest.red_sources(state)
    for name, s in [*state["sources"].items(), *((k, state[k]) for k in PUBLISHED)]:
        print(f"  {name:<26} {s['status']}{': ' + s['message'] if s.get('message') else ''}")
    if any(state[step]["status"] != manifest.SUCCESS for step in PUBLISHED):
        return EXIT_FAILED
    return EXIT_PARTIAL if red else EXIT_OK


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Assemble, fetch, validate, export and offload via Dagster"
    )
    ap.add_argument(
        "--github-output",
        type=Path,
        default=None,
        help="Append manifest=<path> to this file (the runner's $GITHUB_OUTPUT).",
    )
    ap.add_argument(
        "--without-fetch",
        action="store_true",
        help=(
            "Skip the sources: assemble, validate, export and offload what is committed, "
            "with no network. Exits 3 at best, since no source ran. For local trials and "
            "scripts/verify_dagster_parity.py; daily_fetch.yml never passes it."
        ),
    )
    args = ap.parse_args(argv)

    from orchestration import defs
    from orchestration.definitions import default_paths

    paths = default_paths()
    home = paths.resolve("data/local/dagster_home")
    os.environ.setdefault("DAGSTER_HOME", str(home))
    os.makedirs(os.environ["DAGSTER_HOME"], exist_ok=True)
    with DagsterInstance.get() as instance:
        return run_daily(
            defs, paths, instance, github_output=args.github_output, fetch=not args.without_fetch
        )


if __name__ == "__main__":
    sys.exit(main())
