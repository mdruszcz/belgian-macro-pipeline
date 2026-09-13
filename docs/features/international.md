# Feature: international data — all European countries, ~100 indicators

Status: draft
Issue: (maintainer direction, 2026-09-13 — no issue number yet)
Branch: feat/international-pilot

## Problem

BelPulse compares Belgium with almost nothing. Ten foreign national series exist (five GDP volume
indices, EU consumer confidence, four labour-cost indices), reaching `observations` only since
pipeline repair part 3, and no page shows them. The maintainer's direction is all European countries
and about a hundred indicators.

That is a different order of size from anything this pipeline holds. The daily run stopped on
2026-09-13 because a committed file outgrew its guard (ADR 0006), so the size has to be measured
before it is built, not discovered after.

## Goal

Two stages. This spec approves only the first.

**Stage 1 — the pilot (this spec).** Five indicators across every European country Eurostat
publishes, loaded through the normal pipeline into committed CSV stores, run daily for at least
14 days, with these measured and written back into this file:

1. real committed file size per indicator, and in total;
2. revision churn: rows added per day, and how many of those are revisions of an existing period;
3. daily run time added: fetch, assemble, validation, offload;
4. working-database size.

**Stage 2 — scale to ~100 indicators.** A separate approval, decided on stage 1's numbers.

## Non-goals

- **Any page.** No chart, map or country comparison in the pilot. The international chart
  (`dashboard.html`, `macro.html`'s card) needs a geography-aware payload and its own spec.
- **Non-Eurostat sources.** OECD, IMF, ECB and national institutes each need an approved row in
  `docs/data_catalog.md` first (CLAUDE.md rule 8), and new sources outside the catalogue are
  prohibited until the 50% milestone.
- **A second database.** Settled 2026-09-13 (ADR 0006): a growing committed binary is what stopped
  the daily run. International data goes into CSV stores in `config/stores.yaml`, same
  `observations` schema, vintages and validation.
- **Derived statistics or aggregates** built in this pipeline from foreign data. A publisher's own
  aggregate (Eurostat's `EU27_2020`) is loaded as published; nothing is summed or averaged here.

## Measured already (2026-09-13, before any code)

Real Eurostat responses through DBnomics, the path `dbnomics_eurostat` already uses.

**One request per dataset returns every country.** With a `dimensions` filter, one call returned
all geographies of a dataset. The current fetcher asks one series per country instead.

| Pilot indicator | Dataset | Freq. | Geographies | Rows (all periods) | Rows (≥ 2008) | Request |
|---|---|---|---|---|---|---|
| GDP volume, chain-linked 2010 | `namq_10_gdp` | Q | 42 | 5,268 | 2,960 | 0.3 s, 205 KB |
| HICP, annual rate | `prc_hicp_manr` | M | 45 | 14,231 | 9,267 | 1.0 s, 434 KB |
| Unemployment rate, SA | `une_rt_m` | M | 39 | 14,884 | 8,141 | 3.1 s, 463 KB |
| Government debt, % of GDP | `gov_10dd_edpt1` | A | 30 | 890 | 510 | 0.6 s, 43 KB |
| Consumer confidence, SA | `ei_bssi_m_r2` | M | 34 | 12,300 | 7,075 | 0.8 s, 371 KB |
| **Total** | | | | **47,573** | **27,953** | **~6 s** |

**File size.** In the committed 10-column CSV shape: **113 bytes per row**. The five pilot
indicators are 5.4 MB with full history, 3.2 MB from 2008. The largest single file is 1.7 MB.
Extrapolated to 100 indicators: ~950,000 rows / 108 MB with full history, ~560,000 rows / 63 MB from
2008. **One CSV per indicator** keeps every file far under the 40 MB commit guard. One CSV for
everything would be past GitHub's 100 MB hard limit.

**Load speed.** Inserting the 47,573 pilot rows into SQLite takes 0.9 s, ~50,000 rows/s, the same
whether row by row or with `executemany`. **Inserts are not the bottleneck**; 950,000 rows is
~19 s. The fixed cost per store is: `scripts/load_observations_csv.py`'s `load()` re-applies
migrations and reloads geography on every call (the 565-row VAR store takes 0.9 s),
`scripts/build_staging_db.py` starts one subprocess per store, and `ensure_reference_rows.py` one per
store too. At 100 stores that fixed cost, not the rows, is minutes.

**Working database.** 335 bytes per row including indexes: ~16 MB for the pilot, ~320 MB at full
scale. Gitignored and rebuilt each run, so it never reaches git. It does need runner disk and time.

## Proposed approach

### Storage

- New stores under `data/international/`, one CSV per indicator, `mode: in_db`, `source_id:
  dbnomics_eurostat`. Loaded into the working database by the assemble step and dumped back by
  `scripts/offload_stores.py`, exactly like ONEM.
- **Registry granularity.** One `config/stores.yaml` entry per file does not scale to 100. Add a
  directory form, one entry whose `path` is `data/international/` and whose indicators are "every
  configured indicator with `store: international`". It also needs one reference-rows script for the
  family. This is a change to `store_registry.schema.json` and `src/stores.py`; the offload's
  "declared list = row set" safety rule must hold for the directory form too.

### Fetching

- **A new dataset-level adapter**, not a loop over the existing one. `src/fetchers/eurostat.py`
  requests one series and keeps `series.docs[0]` only: pointed at a multi-country query it would
  silently keep one country and drop the rest. The new adapter requests a dataset with a
  `dimensions` filter, pages through `num_found`, and refuses (rule 13) when the response's geography
  set is not what the config expects.
- **Keep the 2008 cutoff or not** is decided per indicator in config, not hardcoded. Today it is
  hardcoded in `_parse`.
- **Cadence.** Monthly, quarterly and annual series fetched daily waste requests and produce no
  change most days. The pilot fetches daily anyway, *because* it is measuring churn; stage 2 adds a
  per-source cadence.

### Geography

- Eurostat's own codes, mapped explicitly: `EL` is Greece, `UK` the United Kingdom, `XK` Kosovo.
- **An explicit allowlist** in config (e.g. `config/geography/international.csv`), not the
  hardcoded `COUNTRY_GEOS` dict in `scripts/port_existing_indicators.py`, which has six entries. A
  geography the response carries but the allowlist does not name **fails the load**. Silently
  dropping or silently adding a country are both wrong numbers.
- **Aggregates are named after their composition** (`eu27_2020:aggregate`, established in part 3),
  because Eurostat publishes several at once and they change: the probe returned `EA`, `EA12`,
  `EA19`, `EA20`, `EA21` (Bulgaria's accession), `EU`, `EU28`, `EEA`, `EU27_2020`.
- Non-European geographies in the same datasets (`US`, `JP`) are excluded by the allowlist.
- Foreign geographies never reach Belgian published files. Part 3's `be:` filter on
  `geographies.json` and the Belgian-only national CSV already enforce that.

### Assemble at scale

Load every international CSV in **one process**, geography and migrations once, one rebuild
`fetch_runs` row per store family. That is where the minutes are (see *Load speed* above).

## Data / schema changes

- No change to `observations`, `indicators` or `geographies` columns.
- New `geographies` rows: one per allowlisted country and aggregate, `level` `country` or
  `eu_aggregate`, `nis_code` NULL.
- `config/indicators/*.yaml`: five pilot indicators, `source_id: dbnomics_eurostat`, `country`
  replaced by the dataset-level fetch description (one config per indicator, many geographies).
- `config/stores.yaml` + its schema: the directory form (above).
- An ADR if the directory form or the dataset adapter changes the adapter contract
  (`docs/features/source_adapter.md`, CLAUDE.md rule 14: the shared contract test).

## New data sources

None new: Eurostat via DBnomics is the approved `dbnomics_eurostat` row. **But its licence is still
`TODO` in `docs/data_catalog.md`**, recorded as "flagged, not blocking, since nothing new is being
introduced". Forty-five countries is new use. Eurostat's copyright page moved (the known URL returned
404 on 2026-09-13), so the terms are not quoted here from memory. **The maintainer must verify and
fill that row before the pilot is merged.**

## Tests

- Dataset adapter: a recorded multi-country fixture parses every geography; a geography outside the
  allowlist fails; a missing expected geography fails; paging reads every series `num_found` promises;
  shared contract test (rule 14).
- Store registry directory form: the offload still dumps and deletes one row set and refuses an
  undeclared indicator; round trip byte-identical, as for ONEM.
- Geography: every allowlisted code maps to exactly one geo id; no foreign geography reaches
  `geographies.json`.
- A test pinning that the Belgian national CSV and `national.json` stay Belgian after the pilot
  lands.
- Assemble timing is recorded in the batch report, not asserted in a test.

## Assumptions and open questions

1. **Which countries count as "European"?** Decision for the maintainer. Eurostat's pilot datasets
   cover the EU27, EFTA (`CH`, `IS`, `NO`; `LI` rarely), `UK`, and candidates (`AL`, `BA`, `ME`,
   `MK`, `RS`, `TR`), with `XK` in some. `UA`, `MD` and `GE` appear in few datasets. Recommended
   default: every European code that at least one pilot dataset publishes, nothing inferred.
2. **Which aggregates?** Recommended: `EU27_2020` and the current euro area only (`EA20`, moving to
   `EA21` when Eurostat switches). Historical compositions (`EU28`, `EA19`, `EA12`) are not loaded.
3. **The licence** — above. Blocking.
4. **Full history or from 2008?** 108 MB vs 63 MB at 100 indicators. Recommended: from 2008 in the
   pilot, measured both ways.
5. Revision churn cannot be measured from one download. The pilot's 14 daily runs are the
   measurement; nothing in stage 2 is decided before they finish.

## Rollout / risks

- **`data/communes_history.csv` is 36.9 MB against the 39.06 MiB guard.** Unrelated to this spec
  but the same daily run: if it trips during the pilot, the day's international data is not
  committed either.
- Monthly Eurostat releases may revise long stretches of history at once. If a release rewrites
  thousands of rows, the CSV diff is large that day. This is the churn number stage 1 exists to
  measure.
- The daily workflow's 15-minute timeout. The pilot adds ~6 s of fetch and a few seconds of assemble.
  At 100 indicators without the one-process assemble, the timeout is at risk.
- Rollback: remove the store entries and CSVs, the pilot configs and geography rows. No Belgian table,
  export or page changes.
