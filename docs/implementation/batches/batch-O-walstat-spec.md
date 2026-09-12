# Batch O — WalStat adapter: handoff and audit packet

Lead → builder → auditor packet for Block O (Walloon municipal finance), docs/steps L1310–1325.
Written before the audit, kept as the record of what the batch was asked to do.

## Objective

Load nine WalStat (IWEPS) municipal-finance series — revenue, expenditure and debt per
inhabitant, each split three ways — into `observations` for every Walloon commune, 2013 onward,
through the shared `DataSource` interface, with four ratios computed in the derived engine from
config alone. Nothing on the site changes in this batch (Batch C, the finance section on the
commune page, follows). The roadmap text this satisfies, quoted:

> [SPEC] Write /docs/features/walstat_adapter.md — How: Endpoints, indicator mapping, coverage
> from 2013, known gaps. / [BUILD] Implement WalStatSource — How: Standard adapter interface,
> fixtures for tests. / [BUILD] Load ordinary/extraordinary revenue and expenditure, debt, debt
> per head, investment, balance — How: Raw series only. / [BUILD] Compute the seven ratios —
> How: Debt/revenue, debt/inhabitant, personnel/expenditure, investment/expenditure, revenue
> growth, expenditure growth, structural balance, in the derived engine.

## Files

- `src/fetchers/walstat.py` — `WalStatSource(MunicipalTimeSeriesSource)`, `walloon_communes_on`,
  `resolve_on_source_grid`, three error types.
- `scripts/sync_walstat.py` — the driver, shape of `sync_statbel.py`; `--from-dir`,
  `--reference-rows-only`.
- `config/sources/walstat.yaml`; `docs/features/source_config.schema.json` (adapter enum);
  `config/indicators/MUN_*_PER_CAPITA.yaml` ×9; `config/indicators/derived/MUN_*.yaml` ×4.
- `assets/commune_map.js`, `src/pages/resolve.py` — the `eur_per_inhabitant` unit, mirrored.
- `Makefile` (`fetch`), `.github/workflows/daily_fetch.yml` (`sync_walstat` step + gate).
- `docs/features/walstat_adapter.md` — the spec, including what the live run taught.
- Tests: `tests/test_walstat_source.py`, `tests/test_sync_walstat.py`,
  `tests/test_source_contract.py` (municipal case), `tests/fixtures/walstat_sample.json`.

## Invariants (quoted)

- CLAUDE.md 3: "ALL observations use canonical NIS-based geo IDs resolved via resolve_geo(nis,
  period). Never trust a raw NIS from a source file."
- CLAUDE.md 5: "ALL derived statistics require unit tests with hand-computed expected values."
- CLAUDE.md 6: "NEVER write a derived value into observations as if it were source data."
- CLAUDE.md 8: "NEVER add a new data source without a row in docs/data_catalog.md approved by
  the maintainer." (WalStat: rows 8–9, Selected 10 row 5, licence CC0 verified 2026-09-06.)
- CLAUDE.md 13: "If a source schema changed, fail loudly. Never silently coerce or drop rows."
- CLAUDE.md 14: "Every new adapter must implement the DataSource interface and pass the shared
  contract test."
- CLAUDE.md 19: "Do not alter source adapters, geography resolution, or analytical formulas
  without a separate ADR and explicit maintainer approval." — hence no new derived function
  and no change to `resolve_geo`; the re-coding rule lives in the adapter.
- CLAUDE.md 26: missing, unavailable, suppressed, not-applicable and zero are five distinct
  states — "non disponible" is missing, never zero and never suppressed.
- CLAUDE.md Definitions: per-inhabitant figures have no defensible aggregate —
  `is_additive=0`, `aggregation_method='not_applicable'`; the 90 % coverage floor.

## Contracts

- `MunicipalTimeSeriesSource._parse` → `{"geo_id": str, "period": str, "value": float,
  "status": str}` (`src/fetchers/base.py`).
- `upsert_observation` keyed `(indicator_id, geo_id, period)`, insert-only-on-change.
- Source config schema requires `source_id, name, agency, adapter, is_active`; indicator config
  schema is `additionalProperties: false` (no `decimals` in YAML — it is set by the sync).
- Derived function signatures: `growth_rate(series, period, years)`,
  `share_of_total(value, total)`.

## Known risks specific to this change

- The API is undocumented beyond its URL grammar; three conventions were discovered live
  (the "non disponible" marker, publication on the current commune grid, a backcast row for the
  merged Bastogne). Each is handled explicitly and tested; a fourth would surface as a refusal.
- The 1:1 re-coding rule is geography logic living in an adapter. It is deliberately narrow
  (one predecessor, same name, validity covering the period) and verified against the real
  list of eleven 2019 re-codings.
- Two `fetch_runs` rows per series, as in `sync_statbel.py`.
- `metadata/indicators.json` publishes `direction: null`/`decimals: null` for derived
  indicators — pre-existing for every derived indicator, not introduced here.
- The maintainer said (2026-09-11) "I'll confirm licences later — the website isn't even
  deployed." Not needed for WalStat; recorded because it was said.

## Acceptance criteria

1. `validate_config.py` OK; `ruff`, `black --check` clean; `validate_data.py` on the committed
   DB: 0 failures (a `fetch_silence` WARN for WalStat until the first daily run is expected).
2. Every test in the three test files passes; the full suite shows no new failure.
3. A live sync on a copy of the database loads all nine series with every `fetch_runs` row
   `ok`, and the exported Namur payload carries the nine series with 2024 values 2 319,7 /
   2 167,1 / 2 965,5 (revenue ordinary / expenditure ordinary / debt total) and the four ratios
   matching the hand computations.
4. No Flemish or Brussels commune carries a WalStat series.
5. No file outside the list above changes.

## Tests required

Parse contract; every refusal (shape, entity type, period text, non-numeric value, duplicate,
unknown code, unexpected commune, sub-90 % year); the three source conventions; Bastogne for
2024 and 2025; `--from-dir` end to end with reference rows, idempotence, the error-marked run;
municipal contract case; four hand-computed ratios; the unit written as a rate in three
languages.

## Explicit exclusions

No change to `resolve_geo`, `derived.py`, existing adapters or `charts.js`; no totals in
euros; no personnel/balance ratios (ADR needed); no provinces/CPAS; no site changes; no
commits to develop; nothing merged.

## What the auditor is asked to check

Dimensions: data integrity, tests, release safety. Against the roadmap text quoted above:
is each ticked step actually done, is "raw series only" honoured, are the deferred ratios
named and justified, does any refusal path coerce or drop silently, is any existing test
weakened, would `make all` on a fresh clone still be offline and byte-identical (this batch
adds no committed data; the DB is untouched until the daily run). State what was not reviewed.
