# Feature: Dagster orchestration layer

Status: step 1 done (local); step 2 built (production runs through Dagster), pending merge
Issue: none -- requested directly by the maintainer, 2026-09-13
Branch: feat/dagster-orchestration (step 1), feat/dagster-step2-runner (step 2)
Decision record: [ADR 0007](../decisions/0007-dagster-orchestration-layer.md)

## Problem

The daily pipeline is one GitHub Actions workflow (`.github/workflows/daily_fetch.yml`), about
25 shell steps in a row, plus `manual_sources.yml` and the `Makefile`. When something goes wrong
the only way to see it is to read a run log: which source is fresh or failing, which export
depends on which source, how many rows arrived, why an export did not run, and which inputs are
automated versus hand-loaded.

## Goal

A supervision layer over the pipeline that exists today, without rewriting it:

- every stabilised step is a Dagster asset, grouped as the maintainer listed them;
- lineage from each source to each published file is visible;
- the existing validation rules appear as asset checks, and the blocking ones still block;
- freshness is declared from the configuration that already states it;
- a schedule is declared, but nothing runs by itself;
- a local UI shows all of it.

What the pipeline looks like in the UI:

```
NBB · DBnomics · FPB ─ macro_legacy_fetch ─ canonical_observations ─┐
Statbel ─ statbel_local_units ──────────────────────────────────────┤
ONEM ─ onem_observations, onem_rates_observations ──────────────────┤
WalStat ─ walstat_observations ─────────────────────────────────────┤
Markets ─ market_data ──────────────────────────────────────────────┤
Manual stores (population, fiscal_income, census2021, realestate,   │
               police, var_unemployment) ───────────────────────────┤
                                                                     ↓
                                validated_working_database  (+ 20 checks)
                                                                     ↓
             derived: aggregates_csv, percentiles_csv, communes_table_json
                                                                     ↓
       website: national/commune CSVs, site_payloads, explorer, local_pages,
                page_documents, site_index
```

## Non-goals (step 1)

- No change to `daily_fetch.yml`, `manual_sources.yml` or `ci.yml`. Production keeps running
  exactly as before, outside Dagster.
- No change to the database, any adapter, any formula or any export.
- Offload (`scripts/offload_stores.py`, the only writer of `data/belgian_macro.db`) and the
  git/PR steps are not in the graph.
- No automation conditions, sensors, partitions, Dagster+ or permanent server.
- `belgian_macro_db.py` is not split into per-agency assets.
- No new check that is not already a validation rule.

## Seven conditions set by the maintainer, and what enforces each

| Condition | Enforced by |
|---|---|
| Assets are thin wrappers around the existing scripts | Every asset runs a command from one table, `orchestration/commands.py`. `test_every_command_is_one_the_makefile_runs_or_production_ran` requires each command to appear verbatim in the Makefile, or to equal the line `daily_fetch.yml` ran itself before step 2 (frozen in the test); the converse test requires every script that workflow ran between assemble and offload to be wrapped. |
| No formula or data duplicated in Dagster | Assets run the scripts as subprocesses and return metadata only (sizes, row counts, latest period). No I/O manager. |
| No change to the database or the exports | The only edit to existing code is a pure extraction in `scripts/validate_data.py` (`build_context`, `record_volume`), covered by its existing tests. Parity: `tests/test_orchestration_parity.py` and `scripts/verify_dagster_parity.py`. |
| Checks call the existing validators | `orchestration/checks.py` calls `_validation_copy`, `build_context` and `run_all` from `validate_data.py`. One check per entry of `RULES` plus `store_loads`; names and severities come from the registry. A parity test compares the check results with the CLI's output on the real data. |
| No Dagster history or internal file committed | `DAGSTER_HOME` and the run manifests live under `data/local/` (ignored); `.tmp_dagster_home_*/` and `.dagster/` are ignored too. Tested with `git check-ignore`. |
| Schedules inactive / local only | One schedule, `daily_fetch_sources`, `default_status=STOPPED`, and no automation condition anywhere (both tested). |
| Dagster produces the same outputs as today | Reduced parity in the suite (assembled database byte for byte, check results, four exporters), full parity on a committed HEAD with `make verify-dagster-parity`. |

## Approach

### Package

`orchestration/` at the top level (flat, like `src/`):

| Module | Role |
|---|---|
| `commands.py` | The one table: asset name → command line, outputs, workflow step id, source ids |
| `paths.py` | `PipelinePaths` resource: repository root, working database, output root, build id |
| `run.py` | `run_script` (subprocess from the repository root, output streamed to the Dagster log, non-zero exit → red asset) and metadata helpers |
| `assets/` | One module per group: `reference_data`, `sources_api`, `sources_manual`, `canonical`, `derived`, `website` |
| `checks.py` | The validation rules as one multi-asset check |
| `policies.py` | Freshness from `fetch_window_days` |
| `manifest.py` | The per-run source manifest |
| `daily.py` | The coordinator (`make dagster-daily`) |
| `definitions.py` | Assets, checks, jobs, the stopped schedule, in-process executor |

Scripts that have no output-path option (`belgian_macro_db.py`, `fetch_stocks.py`, the adjacency,
typology, explorer, page-document and site-index exporters) always write into the repository;
their assets refuse to run when `PipelinePaths.out_root` points elsewhere, so a test can never
make them overwrite tracked files.

### Dependencies, not sequence

The graph has the workflow's dependencies: `staging_db` first; `canonical_observations` after
`macro_legacy_fetch` (and before nothing else, as in the workflow); every source before
`validated_working_database`; every export after it; `site_payloads` before the pages and the
explorer. All jobs use the in-process executor, so no two assets ever write the working SQLite
database at the same time. The independent sources run one after another in an order Dagster
chooses; that order is not guaranteed and does not need to be, since none reads what another
wrote.

### Manual sources

One external asset (`AssetSpec`) per `extra_csv` store in `config/stores.yaml`, generated from
the registry. Dagster never materialises them; the `observe_manual_sources` job records their
row count, latest period, checksum and file date. In the UI they read as "observed", which is
the automated-versus-manual distinction.

### Jobs and failure behaviour

Today every source step has `continue-on-error: true`: a source being down must not throw away
the rest of the day's data; validation and exports still run; the PR is opened but not
auto-merged and the run ends red. Inside a single Dagster run a failed asset blocks everything
downstream of it, which would throw the day away. So:

| Job | Contains |
|---|---|
| `assemble_working_database` | `staging_db` |
| `fetch_sources` | the seven outcomes the workflow gates auto-merge on: `macro_legacy_fetch`, `canonical_observations`, `statbel_local_units`, `onem_observations`, `onem_rates_observations`, `walstat_observations`, `market_data` |
| `validate_and_export` | `validated_working_database` and its checks, `volume_history`, `revisions_report`, every derived and website asset. Selects no source, so a red source never blocks it. |
| `observe_manual_sources` | observations of the hand-loaded stores |

A failed source is a failed (red) asset -- never hidden by a try/except. The coordinator,
`python -m orchestration.daily` (`make dagster-daily`), reproduces the workflow's behaviour.
It is a Python program rather than a Make recipe because a recipe stops at the first non-zero exit:

1. creates a run id and writes `data/local/dagster_runs/{run_id}/sources.json` with every
   outcome `not_run`;
2. runs `assemble_working_database`; if it fails, stops with exit 1 and exports nothing (the
   workflow's assemble step has no continue-on-error either);
3. runs `fetch_sources` and records each of the seven outcomes (`success`, `failed` with the
   error, or `skipped` when an upstream asset failed) from the Dagster run result -- not from
   `fetch_runs`, which `fetch_stocks.py` never writes and which a crash can precede;
4. always runs `validate_and_export`, passing that manifest's path and run id as the
   configuration of `validated_working_database`;
5. exits 0 when everything is green; 3 when the exports ran but any of the seven is not a
   success (a failed `sync_to_canonical` counts like a failed source); 1 when nothing is
   publishable -- the assemble or `validate_and_export` failed.

A blocking check that fails stops every export and the volume baseline, as the workflow's
validation step does. The coordinator keeps the 30 most recent run directories.

### Source manifest

Per run, never a fixed path, so an old manifest can never pose as today's state.
`validated_working_database` reads only the path it was given. A manual `validate_and_export`
(from the UI) reports "no source run attached", even if older manifests exist on disk. A path
that was given but does not exist, or that belongs to another run id, fails the asset.

### Checks

`validation_rules` attaches 20 checks to `validated_working_database`: the 19 rules of
`src/validation/rules.py` plus `store_loads` (the failure `_validation_copy` reports when a
hand-loaded store cannot be loaded, pipeline repair part 4). FAIL rules are blocking; WARN rules
are reported and never block. A violation from a rule the module does not know fails the check
loudly rather than being dropped.

### Freshness

A source asset's fail window is its sources' `fetch_window_days`, read through
`validate_data._fetch_windows` (which already maps config ids to `fetch_runs` ids). Currently 7
days for the macro fetch, Statbel and both ONEM assets, 14 for WalStat. No window: the market data
(no source config) and every manual store. Per-indicator `max_age_days` stays the `staleness`
check -- it concerns the reference period, not when an asset last ran.

### Schedule

`daily_fetch_sources`, cron `0 5 * * *` UTC (the workflow's), targeting `fetch_sources`, declared
**stopped**. GitHub Actions remains the scheduler.

## What the local UI shows

Only local runs. Since step 2 production does run through Dagster, but inside the GitHub runner,
with its history in the runner's temporary directory, which disappears with the runner. The
local UI therefore shows the maintainer's own trials, never the production fetch of that
morning; production's record is the workflow log, the job summary and the PR body.

## Step 2 -- the runner

`daily_fetch.yml` no longer runs the pipeline's scripts itself. Its steps are now:

1. the open-PR check, Python, `pip install -r requirements.txt` plus the `dagster==` pin read from
   `requirements-dagster.txt` (not the web UI);
2. **one step, `python -m orchestration.daily --github-output "$GITHUB_OUTPUT"`** -- assemble,
   the seven sources, validation, the revisions report, every export and page. Its environment:
   `WORKING_DB` (workflow level, unchanged), `BUILD_ID` = the run id, `DAGSTER_HOME` and
   `VALIDATION_SUMMARY` under `$RUNNER_TEMP`, `DAGSTER_DISABLE_TELEMETRY`;
3. offload, the size guard, the gate, stage, PR, auto-merge and the final red step -- as before.

The job's timeout is 30 minutes (was 15): the rehearsal took about 15 minutes end to end, with the
Dagster install on top in the runner.

Failure semantics, unchanged from the day before step 2:

| Coordinator exit | Meaning | The workflow |
|---|---|---|
| 0 | all green | offloads, opens the PR, enables auto-merge |
| 3 | exported, but a source (or `sync_to_canonical`) was red | offloads, opens the PR, **no** auto-merge, run ends red |
| 1, or anything else | assemble or validation failed, or a crash | stops before the offload: no committed file changes, no PR |

The gate. `python -m orchestration.manifest <path>` reads the manifest of this run -- the path the
coordinator wrote into `$GITHUB_OUTPUT`, never a fixed one -- and prints one output per tracked
outcome under its old step id (`fetch_macro=success`, ...), `summary`, and `all_ok`. `all_ok` is
`true` only if the assemble, all seven outcomes and `validate_and_export` succeeded; a missing
outcome counts as not run, and a missing manifest fails the step (no PR). The PR body and the
final error message use `summary`.

`validation_status` in `public/data/manifest.json`. `site_payloads` reads the check evaluations of
its own Dagster run: `pass` when every check ran and no FAIL check failed (warnings allowed, as
`validate_data.py` exits 0 on warnings), `fail` if one did (never published in practice: the
checks block), `unknown` when the checks did not run in that run -- a payload export materialised
on its own, the same value `make exports` stamps. `scripts/verify_dagster_parity.py` therefore
masks this field alongside `build_date`.

The validation report. The checks print the same `::error::` / `::warning::` annotations as
`validate_data.py` and, when `VALIDATION_SUMMARY` is set, append its markdown summary with its own
writer; the workflow copies it to the job summary and the PR body, even on a failing run.

What the daily run now also rebuilds: `page_documents`, `site_index`, `commune_adjacency` and
`commune_typology`, which `make exports` ran but the workflow did not. All four are
deterministic (rule 35), so they produce a diff only when their inputs changed.

## Data / schema changes

None. No migration, no config change, no change to any published file.

## New data sources

None.

## Usage

```
pip install -r requirements-dagster.txt       # once
make assemble
make dagster                                  # UI at http://localhost:3000
make dagster-daily                            # full local run; needs the network
make verify-dagster-parity                    # after committing
```

On Windows without `make`: `.venv/Scripts/python.exe -m dagster dev -m orchestration`, with
`DAGSTER_HOME` set to the absolute path of `data/local/dagster_home`.

## Tests

- `tests/test_orchestration.py` (everyday tier): drift against the Makefile and workflow,
  dependencies, executor, stopped schedule, no automation conditions, checks = rule registry,
  manual specs, freshness = config, gitignore, run_script's command line, and the coordinator
  scenarios with fake scripts: one red source, a red canonical sync, a red macro fetch (canonical
  skipped), the one-run negative control, all green, failed assemble, crashed fetch job, failing
  and warning rules, stale / missing / foreign manifests, pruning.
- `tests/test_orchestration_parity.py` (slow): assembled database byte-identical; check results
  equal to `validate_data.py`'s output on the real data; four exporters byte-identical.
- Step 2, in `tests/test_orchestration.py`: the coordinator writes its own manifest path to the
  runner (also when the assemble fails); exit codes 0 / 3 / 1; the gate opens only when
  everything succeeded, and its command refuses a missing manifest; `validation_status` is
  `pass` after the checks, `pass` with a warning, `unknown` without the checks, `fail` after a
  failed blocking check; the checks write `validate_data.py`'s summary and annotations; the
  runner's environment reaches `PipelinePaths`.
- Step 2, in `tests/test_pipeline_cutover_wiring.py`: coordinator before offload before gate
  before PR; no pipeline script run as a step of its own; no `continue-on-error`; only exit 3
  let through; the gate reads `steps.daily.outputs.manifest` without a pipe; Dagster installed at
  the pinned version without the UI; history and telemetry kept in the runner.
- `scripts/verify_dagster_parity.py`: every file of `make assemble exports` versus the Dagster
  jobs, in two worktrees of the committed HEAD; refuses a dirty tree.

## Next steps (not in this batch)

- **After merging step 2:** watch the first production run (or start it by hand with
  `workflow_dispatch` on `develop`) -- the runner is the one environment the tests cannot reach.
- **Step 3.** Offload inside the graph; extract clean functions from the scripts one at a time.

## Open questions

- Should the "565 communes expected" check the maintainer mentioned become a validation rule?
  It would then appear as a check with no Dagster change.
- Split `belgian_macro_db.py` into NBB / DBnomics / FPB assets, so a single agency can be red
  on its own?
