# Feature: Dagster orchestration layer, step 1 (local)

Status: done (step 1)
Issue: none -- requested directly by the maintainer, 2026-09-13
Branch: feat/dagster-orchestration
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
| Assets are thin wrappers around the existing scripts | Every asset runs a command from one table, `orchestration/commands.py`. `test_every_command_is_one_the_makefile_or_the_workflow_already_runs` requires each command to appear verbatim in the Makefile or the workflow; the converse test requires every script the workflow runs between assemble and offload to be wrapped. |
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
5. exits 1 if any of the seven is not a success (a failed `sync_to_canonical` counts like a
   failed source) or if `validate_and_export` failed; 0 otherwise.

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

## What the local UI shows during step 1

Only local runs. Production runs on GitHub Actions do not go through Dagster yet and write
nothing to the local `DAGSTER_HOME`, so the UI shows the maintainer's own trials -- never the
production fetch of that morning.

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
- `scripts/verify_dagster_parity.py`: every file of `make assemble exports` versus the Dagster
  jobs, in two worktrees of the committed HEAD; refuses a dirty tree.

## Next steps (not in this batch)

- **Step 2 -- the runner.** `daily_fetch.yml` runs the coordinator in the runner (an ephemeral
  Dagster run: GitHub cron → Dagster → fetch, validation, exports → PR → runner off), then
  offload and the PR steps as today. Needs: installing Dagster in the workflow, deriving
  `--validation-status` from the check results (the Dagster route passes `unknown` today, as
  `make exports` does), the auto-merge gate reading the manifest, and updating
  `tests/test_pipeline_cutover_wiring.py`.
- **Step 3.** Offload inside the graph; extract clean functions from the scripts one at a time.

## Open questions

- Should the "565 communes expected" check the maintainer mentioned become a validation rule?
  It would then appear as a check with no Dagster change.
- Split `belgian_macro_db.py` into NBB / DBnomics / FPB assets, so a single agency can be red
  on its own?
