# ADR 0007 — Dagster as a supervision layer over the scripts, GitHub Actions stays the scheduler

Date: 2026-09-13
Status: accepted (step 1); amended by steps 2 and 3 (below)
Revises: `docs/architecture.md`, "Execution model" ("No queue, scheduler, or server process.
GitHub Actions cron is the only orchestrator.") -- the second sentence still holds.

## Context

The pipeline runs as one long GitHub Actions workflow. Its failure handling is deliberate and has
been corrected several times (ADR 0006, pipeline repairs 1-4): every source may fail without
losing the others, validation blocks exports, partial days are not auto-merged. What it lacks is
visibility: lineage, per-source freshness, row counts and check results are only in run logs.

The maintainer asked for Dagster to supply that, on three conditions of approach: wrap the
stabilised scripts rather than rewrite them, replace the monolithic workflow progressively, and
start locally.

## Decision

1. **Dagster is a layer over the existing scripts, not a rewrite.** Each asset runs a command
   the Makefile or the workflow already runs (`orchestration/commands.py`, tested against both
   files). No formula, rule or data lives in `orchestration/`.

2. **No permanent server.** GitHub Actions remains the scheduler. Step 1 is local only: the UI
   runs on the maintainer's machine (`make dagster`), with its history under `data/local/`,
   never committed. In step 2 the workflow will start an ephemeral Dagster run inside the
   runner (cron → Dagster → fetch, validation, exports → PR → runner off). No Dagster+ and no
   hosted daemon.

3. **Today's failure semantics are kept by splitting the run, not by hiding failures.** A failed
   source is a red asset. The fetch and the exports are separate Dagster runs, sequenced by a
   small coordinator (`orchestration/daily.py`) that always runs the exports and ends non-zero if
   anything was red -- the equivalent of `continue-on-error` plus the auto-merge gate. Each run's
   outcomes go in a per-run manifest that is handed explicitly to the export run.

4. **Validation stays the validator.** The rules become asset checks by calling
   `scripts/validate_data.py`'s own functions; FAIL rules are blocking checks.

5. **Nothing runs by itself.** The schedule is declared stopped; no automation conditions.

6. **The committed database stays out of the graph** in step 1: offload remains a workflow
   step, and nothing in `orchestration/` names it.

7. **Pinned exactly** (`dagster==1.13.22`, `dagster-webserver==1.13.22`) and installed only
   for development and the local UI; the daily workflow does not install it in step 1.

## Consequences

- A second way to run the pipeline exists locally. Drift between the two is caught by tests
  (commands verbatim in the Makefile/workflow; the workflow's scripts all wrapped; the seven
  tracked outcomes equal the workflow's gate) and by `make verify-dagster-parity`.
- The dev install grows by about 50 packages.
- The local UI does not see production runs (still true after step 2: their history dies with
  the runner).
- `validation_status` in the manifest is `unknown` on the Dagster route, as with `make exports`;
  step 2 must derive it from the checks before production switches over. (Done in step 2.)

## Amendment -- step 2 (2026-09-13)

Production now runs through Dagster. `daily_fetch.yml` replaces its assemble, source, validation
and export steps with one step, `python -m orchestration.daily`, in the runner; offload and the
git/PR steps stay workflow steps. This revises decisions 2 and 7 above:

- the runner installs the `dagster` pin (read from `requirements-dagster.txt`, never retyped), not
  the web UI; `DAGSTER_HOME` is the runner's temporary directory and telemetry upload is off;
- the coordinator's exit code carries the old step semantics: 0 green, 3 exported with a red
  source (continue, no auto-merge), anything else stops before the offload;
- the auto-merge gate reads that run's manifest (`python -m orchestration.manifest`), by the path
  the coordinator reported;
- the published `validation_status` comes from the check evaluations of the same Dagster run.

Consequence: the Makefile and `commands.py` are now the only statement of each command line; the
lines the workflow used to run are frozen in `tests/test_orchestration.py` so the change of
runner cannot silently change a command. See `docs/features/orchestration.md`, "Step 2".

## Amendment -- step 3 (2026-09-14)

The offload joins the graph, and an asset may call a script's own function in process. This
revises decisions 1 and 6 above. ADR 0006's decision 4 -- the committed database is written once,
last, by one script -- holds in substance.

- **Decision 6 is replaced.** The committed database and the in_db CSVs are written by one asset,
  `committed_stores`, which calls `offload()` from `scripts/offload_stores.py`. It runs last, in the
  same Dagster run as validation and every export (`validate_export_and_offload`), so a failed
  export or blocking check skips it. It writes only when the coordinator's run config allows it,
  for that coordinator run; from the UI or `dagster job execute` it refuses. It still refuses
  rather than guesses, with the script's own checks.
- **Decision 1 is widened.** An asset may call a script's function instead of running its command
  line (`Command.function`). The command line stays in `commands.py` as the reference the drift and
  parity tests hold the call to. No formula, rule or data moves into `orchestration/`.
- The exit-code contract is kept: 0 green, 3 published with a red source, anything else stops
  before anything is committed -- which now includes a failed offload.

Consequence: a refused offload changes no file, but an error while `offload()` replaces its files
one by one can leave the checkout it ran in with some of them new. The run is red and nothing is
committed; the asset names the files concerned individually, from the registry. Restoring them
automatically is not implemented. See `docs/features/orchestration.md`, "Step 3".
