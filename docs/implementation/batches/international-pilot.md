# Batch report: international pilot (PR 1)

Direct-Eurostat adapter (`src/fetchers/eurostat.py`) replacing DBnomics for
Eurostat data, five multi-country pilot indicators, and the cutover of the
eight pre-existing `dbnomics_eurostat` indicators onto it. This report
covers the implementation batch and the fixes made after independent audit.

## Country list

EU27 + EFTA (CH, IS, LI, NO) + the nine official EU candidate countries
(Albania, Bosnia and Herzegovina, Georgia, Moldova, Montenegro, North
Macedonia, Serbia, Türkiye, Ukraine), read from
https://european-union.europa.eu/principles-countries-history/eu-enlargement_en
on 2026-09-13. Kosovo is a potential candidate (not yet granted candidate
status) per that page and is excluded, alongside UK/US/JP.
`config/geography/international.csv` (43 rows) / `international_excluded.csv`
(10 rows: UK, XK, US, JP, EA12/EA19/EA20/EU/EU28/EEA).

## Audit fixes (after the first cutover)

An independent audit found two blockers and five should-fix items, all
fixed in separate commits on this branch:

1. **BLOCKER**: `belgian_macro_db.fetch_all`'s eurostat branch wrote
   canonical status words into `legacy_observations.obs_status`;
   `sync_to_canonical.py` reads that column expecting an SDMX letter and
   aborted the whole canonical sync. Fixed with a shared
   `src/fetchers/sdmx_status.py` (SDMX <-> canonical, one table, both
   directions).
2. **BLOCKER**: a multi-geo pilot indicator's own `be:country` row leaked
   into the Belgian national export (755 rows) beside the pre-existing
   national indicator for the same concept. `export_canonical_csv.py` now
   excludes every `is_multi_geo()` indicator.
3. **SHOULD-FIX**: the Belgian-only guard test wrote the export but never
   read it back, so it passed against the bug it claimed to catch. Rewritten
   to read the real exported CSV and `national.json`'s input.
4. **SHOULD-FIX**: `b`/`d` OBS_FLAG letters were mapped to `revised`, which
   is wrong -- Eurostat's OBS_FLAG codelist has no `revised`/`r` code at
   all (checked in full, 2026-09-14), and `revised` specifically means a
   later vintage superseded an earlier one, which neither flag asserts.
   Corrected to `final`; `f` (forecast) removed so it fails loudly instead
   of silently becoming `estimate`.
5. **SHOULD-FIX**: a flagged cube position with no value and a non-nullable
   status now raises `FetchError` instead of reaching the database.
6. **SHOULD-FIX**: a directory store's drift check only looked at declared
   files, so removing an indicator from the registry without deleting its
   CSV went undetected and the file kept loading.
   `verify_indicator_lists()` now globs the real directory;
   `load_observations_csv.py`'s `--csv-dir` (a glob) is gone in favour of a
   repeatable `--csv`, so nothing in the loading path globs a directory
   itself.
7. **SHOULD-FIX**: `sync_international.py` passed `conn=` into
   `EurostatSource.fetch()`, which logged a second `fetch_runs` row per
   indicator (the committed db had ten eurostat runs for five fetches); on
   a failure only the script's own row became `error`, so `fetch_error`
   (reading the highest `fetch_run_id`) could see the adapter's later `ok`
   row and miss the failure. Fixed by dropping `conn=` there.

Every fix has a regression test verified to fail with the fix reverted
(not committed).

## Cutover, redone cleanly after fixes 1/4/8

The first cutover (before the audit) is superseded: it committed 901 rows
as `revised` that fix 4 now correctly resolves to `final`, and re-fetching
on top of it would have written a fake new vintage for values that never
actually changed. Instead: `data/belgian_macro.db` was reset to the exact
pre-cutover blob (`git show e7f288c3:data/belgian_macro.db`, sha256
verified identical), the old `data/international/*.csv` files were
deleted, and the cutover was rebuilt from scratch with a fresh fetch.

- Pre-cutover committed db: 2,590 observations (verified byte-identical to
  the pre-PR blob).
- First offload (legacy-only, no network): 858 rows to 8 per-indicator
  CSVs -- byte-identical to the first cutover's own 8 legacy files (the
  audit fixes touch only the pilot's multi-geo parsing, not this path).
- Real fetch (`sync_international.py`, network, once, since 2008): 25,871
  observations, 25,871 new vintages (a genuinely empty working db, so
  every row is a first vintage, never a fake revision). Status
  distribution: 25,741 `final`, 124 `provisional`, 6 `estimate` -- zero
  `revised` (the 901 rows that were `revised` under the old mapping are
  now correctly `final`).
- Second offload: 26,729 rows total. Committed db after: 1,732
  observations (2,590 − 858). No duplicate (indicator_id, geo_id, period,
  vintage) keys across the 13 committed files (checked directly: 26,729
  rows, 26,729 unique keys).
- Second assemble+offload cycle: every CSV byte-identical (sha256, all 13
  files). `PRAGMA integrity_check` = ok, `foreign_key_check` = [],
  0 leftover `rebuild` fetch_runs rows, exactly 5 real `eurostat`-adapter
  fetch_runs rows (fix 7's own proof, on the real cutover: the pre-existing
  124 `eurostat`/`dbnomics`-adapter rows are historical DBnomics-era audit
  trail, untouched).
- `validate_data.py` on the assembled working db: 0 failures, 12 warnings
  (pre-existing staleness/unit-name warnings, none new, same 12 both
  cutovers).
- `export_canonical_csv.py` on the redone working db, content-compared
  (`diff --strip-trailing-cr`, since the committed file predates this PR
  and carries CRLF unrelated to it) against the committed
  `data/belgian_macro_export.csv`: byte-identical content, confirming the
  fix excludes exactly the 5 pilot indicators and changes nothing else.

## Revision disclosure

The DBnomics copy of the 8 migrated indicators was stale relative to
Eurostat's own current figures. Recomputed directly: fetched each of the 8
indicators fresh via the corrected direct-Eurostat adapter (real network,
2026-09-14), compared against the pre-cutover committed value for every
period both have, tolerance 0.05 (the committed values are stored to
1-2 decimals). These are genuine Eurostat revisions the stale DBnomics
copy had not picked up, not parse faults -- e.g. EC_CONS_CONF_BE 2008-01
moves from -11.5 to -11.6.

| Indicator | Revised / compared |
|---|---|
| EC_CONS_CONF_BE | 134 / 216 |
| EC_CONS_CONF_EU | 214 / 216 |
| EUROSTAT_GDP_Q_MEUR | 12 / 71 |
| EUROSTAT_GDP_Q_MEUR_DE | 54 / 71 |
| EUROSTAT_GDP_Q_MEUR_EA | 45 / 71 |
| EUROSTAT_GDP_Q_MEUR_ES | 2 / 71 |
| EUROSTAT_GDP_Q_MEUR_FR | 15 / 71 |
| EUROSTAT_GDP_Q_MEUR_NL | 8 / 71 |

The first real daily run of the national fetch through the new adapter will
publish all of these at once, as ordinary Eurostat revisions -- not a data
quality incident. This batch did not run that real daily national fetch
(only the pilot's own `sync_international.py` ran for real); the 8 legacy
CSVs committed here are still the pre-existing, unrevised values, moved
byte-for-byte from the committed database into their own files, not
refetched.

## Known limitation (not fixed, per instruction)

`config/geography/international.csv`'s `valid_from` column does not carry
one consistent meaning: EU/EFTA rows use accession-to-the-bloc date,
candidate rows use the date candidate status was granted, and the two
pilot aggregates use their own composition-effective dates. Left as-is --
nothing in this pilot reads this column's value, but it should get one
consistent meaning before a second column of rows makes the inconsistency
load-bearing.

## Committed sizes (from 2008)

| File | Rows | Size |
|---|---|---|
| GDP_VOLUME_EUROPE.csv | 2,808 | 388 KB |
| HICP_ANNUAL_RATE_EUROPE.csv | 7,574 | 1.1 MB |
| UNEMPLOYMENT_RATE_EUROPE.csv | 7,624 | 1.1 MB |
| GOV_DEBT_EUROPE.csv | 522 | 68 KB |
| CONSUMER_CONFIDENCE_EUROPE.csv | 7,343 | 1.1 MB |
| 8 legacy `EUROSTAT_GDP_Q_MEUR*`/`EC_CONS_CONF_*` files | 858 | 12–32 KB each |

`data/international/` total: 3.8 MB. Every file well under the 2 MB/25 MB
guards. Full-history-vs-committed-since-2008 size was not separately
measured this batch (explicitly descoped).

## Timings (this machine)

- `sync_international.py` (network, 5 datasets, full history since 2008):
  ~3m35s, both cutover attempts.
- `build_staging_db.py` (assemble, all 10 stores): well under a minute.
- `offload_stores.py` (4 in_db stores, ~117K rows dumped+stripped): a few
  seconds.

## Deviations from the plan

1. **OBS_FLAG mapping**, corrected after audit -- see "Audit fixes" above.
   Final table: `''`/`b`/`d` -> `final`, `p` -> `provisional`, `e` ->
   `estimate`, `c` -> `suppressed`, `z` -> `na`; `f` unrecognized (fails
   loudly).
2. **Consumer confidence has no separate `unit` filter.** `ei_bssi_m_r2`'s
   `indic` codes (`BS-CSMCI-BAL`) already encode the balance/unit; there is
   no `unit` dimension on this dataset. Filter is `{indic, s_adj}`.
3. **`EA` kept allowlisted but never written by the pilot** (`scope:
   legacy`), so `EUROSTAT_GDP_Q_MEUR_EA`'s national fetch keeps resolving it
   without the pilot publishing a second, ambiguous-vintage euro-area total
   beside `EA21`.
4. **Absent-but-allowlisted geography keeps the run `ok`**, per the plan;
   the codes are named in the `fetch_runs` message rather than failing the
   day over e.g. Ukraine having no unemployment series yet.

## Not done this batch (by instruction)

Full-history (uncommitted) size measurement; `docs/features/source_adapter.md`,
ADR 0008, `docs/data_catalog.md`, `docs/features/international.md`,
`docs/features/orchestration.md`, README were written by a separate docs
pass and reviewed, not authored, here.
