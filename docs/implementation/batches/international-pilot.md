# Batch report: international pilot (PR 1)

Direct-Eurostat adapter (`src/fetchers/eurostat.py`) replacing DBnomics for
Eurostat data, five multi-country pilot indicators, and the cutover of the
eight pre-existing `dbnomics_eurostat` indicators onto it.

## Country list

EU27 + EFTA (CH, IS, LI, NO) + the nine official EU candidate countries
(Albania, Bosnia and Herzegovina, Georgia, Moldova, Montenegro, North
Macedonia, Serbia, Türkiye, Ukraine), read from
https://european-union.europa.eu/principles-countries-history/eu-enlargement_en
on 2026-09-13. Kosovo is a potential candidate (not yet granted candidate
status) per that page and is excluded, alongside UK/US/JP.
`config/geography/international.csv` (43 rows) / `international_excluded.csv`
(10 rows: UK, XK, US, JP, EA12/EA19/EA20/EU/EU28/EEA).

## Cutover counts

- Committed `data/belgian_macro.db` before: 2,590 observations.
- First offload (legacy-only, no network): 858 rows to 8 per-indicator CSVs
  (`EUROSTAT_GDP_Q_MEUR*`, `EC_CONS_CONF_*`) — matches the 858 rows already
  in production before this PR exactly.
- Real fetch (`sync_international.py`, network, once, since 2008): 25,871
  rows across the 5 pilot indicators.
- Second offload: 26,729 rows total to `data/international/`. Committed db
  after: 1,732 observations (2,590 − 858).
- Second assemble+offload cycle: the 8 legacy CSVs came back byte-identical
  (sha256 match); the 5 pilot CSVs were unaffected (no second fetch).
  `PRAGMA integrity_check` = ok, `foreign_key_check` = [] throughout.
- `validate_data.py` on the assembled working db: 0 failures, 12 warnings
  (pre-existing staleness/unit-name warnings, none new).

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
  3m35s.
- `build_staging_db.py` (assemble, all 10 stores): well under a minute.
- `offload_stores.py` (4 in_db stores, ~117K rows dumped+stripped): a few
  seconds.

## Deviations from the plan

1. **OBS_FLAG mapping.** The plan's draft table only covered
   `p/e/f/c/z` + unmapped-as-final. Real data (all 5 datasets, full history,
   every allowlisted country) also carries `b` (break in series) and `d`
   (definition differs) — no `c`, `z`, or compound flags observed. Both
   mapped to `revised` (a comparability caveat, not a confidence flag; same
   read `port_existing_indicators.py`'s SDMX table already gives `B`). See
   `src/fetchers/eurostat.py`'s `FLAG_STATUS` comment.
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
`docs/features/orchestration.md`, README (a separate docs pass).
