# Batch O — WalStat (IWEPS) Walloon municipal finance

```
Batch:                 O — Walloon municipal finance (docs/steps, Block O)
Base commit:           126d4a62 (origin/develop)
Final commit:          see branch feat/walstat-adapter
Files changed:         see below
Requirements completed: SPEC; WalStatSource; the nine raw series; four of the seven ratios
Deferred requirements: balance (not published by the source); personnel/expenditure and
                       structural balance (need a `difference` function → ADR, rule 19);
                       the daily-workflow step and `fetch_window_days` (commit-size decision)
Data-contract impact:  additive only — one new source id, nine new indicator ids, four derived
                       ids. No existing indicator, payload key or formula changes.
Commands executed:     see "Verification"
Tests passed:          37 (walstat source) + 8 (sync) + 5 (contract) + the touched suites;
                       full suite 3377 passed / 9 pre-existing Windows failures
Screenshots produced:  none (no page changes in this batch)
Performance results:   live sync 28,221 observations in ~90 s over 18 requests
Reviewer findings:     1 P0, 1 P1, 2 P2, 3 P3 — recorded in known-risks.md; P0/P1/P2 fixed
Known limitations:     below
Rollback procedure:    below
Next batch:            Batch 6 (macro.html); Batch C (the finance section on the commune page)
```

## What this batch does

Loads nine WalStat series — municipal revenue, expenditure and debt per inhabitant, each split
three ways, 2013 onward, for every Walloon commune — into `observations` through the shared
`DataSource` interface, and computes four ratios from config alone. **Nothing on the site
changes.** The finance section on the commune page is Batch C.

WalStat is the one approved catalogue source whose licence is already confirmed (CC0 for the
data, rows 8–9, verified from IWEPS' own FAQ on 2026-09-06). The maintainer said on 2026-09-11
"I'll confirm licences later — the website isn't even deployed"; recorded because it was said,
and it changes nothing here, because this source's licence was already settled.

## Files

- `src/fetchers/walstat.py` — `WalStatSource(MunicipalTimeSeriesSource)`, `walloon_communes_on`,
  `resolve_on_source_grid`, three error types.
- `scripts/sync_walstat.py` — the driver (`--from-dir`, `--reference-rows-only`).
- `config/sources/walstat.yaml`; `docs/features/source_config.schema.json` (adapter enum);
  `config/indicators/MUN_*_PER_CAPITA.yaml` ×9; `config/indicators/derived/MUN_*.yaml` ×4.
- `assets/commune_map.js`, `src/pages/resolve.py`, `scripts/export_local_pages.py` — the
  `eur_per_inhabitant` unit, mirrored across all three formatters.
- `src/validation/config_schema.py` — a derived config whose inputs are configured but not yet
  loaded is deferred, not fatal (the P0 below).
- `Makefile` (`fetch`). **Not** `.github/workflows/daily_fetch.yml` — see Known limitations.
- `docs/features/walstat_adapter.md`; `docs/implementation/batches/batch-O-walstat-spec.md`.
- Tests: `tests/test_walstat_source.py`, `tests/test_sync_walstat.py`,
  `tests/test_source_contract.py`, `tests/test_export_local_pages.py`,
  `tests/test_derived_engine.py`, `tests/fixtures/walstat_sample.json`.

## Three source conventions nobody documents

Each was found by running against the live API, and each is handled explicitly:

1. **`"non disponible"`** is written where an account was never filed. It is a MISSING reading —
   skipped and counted, never a zero and never a suppression (rule 26's five distinct states).
   8 of 3,143 readings per revenue series.
2. **Every year is published on today's commune grid.** A 2013 row carries the code the 2019
   Hainaut re-codings introduced. The adapter walks from that code to the single same-named
   predecessor valid in the period, and refuses where there are two or more — 66 rows per series.
3. **A merged commune is backcast.** Bastogne+Bertogne (merged 2024-12-02) has rows for years
   when it did not exist; its parts hold the real accounts. Skipped and counted, 12 rows.

## What the audit found

The full text of each is in `docs/implementation/known-risks.md` (2026-09-11).

- **P0 — the four ratios stopped every commune export on a database without WalStat rows.**
  Fixed: such a config is deferred, not fatal. Proved byte-identical on the committed data
  (36,455,348 bytes, same SHA before and after).
- **P1 — two codes resolving to one commune-year were accepted silently.** Fixed and tested.
- **P2 — the unit reached two of three formatters.** Fixed and tested in three languages.
- **P2 — no test ran the committed configs against the committed data**, which is why the P0
  shipped. Fixed: the four ratios now go through the engine from their YAML.
- **P3s** — `reconcile=False` exists for the contract test; `resolve_on_source_grid` now orders
  its candidate row rather than taking an arbitrary one (the auditor checked all 31 post-2012
  municipality rows and found no reachable misattribution today); the spec's predicted
  `fetch_silence` warning could not fire and that prediction has been removed.

## Verification

From `.venv`, on this branch:

- `ruff check .` — clean. `black --check` — 169 files unchanged.
- `python scripts/validate_config.py` — OK: 78 indicators, 8 sources, 17 derived.
- `python scripts/validate_data.py --db data/belgian_macro.db` — 0 failures, 3 warnings, all
  three pre-existing staleness warnings on national series.
- Live sync on a COPY of the database: 28,221 observations, 273 geo_ids, all 18 `fetch_runs`
  rows `ok`.
- Namur 2024 from the exported payload: revenue ordinary 2 319,7 / expenditure ordinary
  2 167,1 / debt total 2 965,5 €/hab, each equal to a direct API probe; the four ratios equal
  the hand computations to four decimals.
- The same export on the committed (reference-rows-only) database is byte-identical to the same
  export before this branch.
- No Flemish or Brussels commune carries any `MUN_` series; no province or region aggregate is
  built from a per-inhabitant figure.
- Full suite: 3377 passed, 9 failed, 17 skipped. All nine failures are pre-existing Windows
  ones in `tests/builder/`, `tests/security/` and `tests/test_geography_load.py` (a cp1252
  temp-file decode, socket and symlink cases); this branch touches none of those modules.

## Known limitations

1. **The daily-workflow step is not wired, and the committed database holds reference rows
   only.** Loading the nine series takes `data/belgian_macro.db` to 33.9 MB (rule 12's limit is
   25 MB) and `data/communes_history.csv` to 45.2 MB, past the workflows' own 40 MB commit
   tripwire — the first daily build would refuse to commit anything, for every source. Needs the
   maintainer's call: raise the tripwire (it went 25 → 40 MB for ONEM) or trim that CSV first.
   Everything else is done; the step plus `fetch_window_days: 14` is a few lines afterwards.
2. **Balance is not loaded and three of the seven ratios are not built** — see the docs/steps
   entry; personnel and balance need a source line that does not exist and a new derived
   function behind an ADR.
3. **Investment is extraordinary expenditure**, the Belgian municipal equivalent. Defensible,
   and a substitution worth stating.
4. **`metadata/indicators.json` publishes `direction: null` / `decimals: null` for the four
   ratios** — pre-existing for every derived indicator, not introduced here.
5. **Three copies of one formatting rule.** `eur_per_inhabitant` had to be added in three
   files. That is the shape of a future defect, not of this one.

## Rollback

Drop the nine indicators' observations and their `indicators` rows, the `walstat` `sources`
row, and revert the configs. No other indicator, payload or formula depends on them; the
deferral change in `config_schema.py` is independent and safe to keep.
