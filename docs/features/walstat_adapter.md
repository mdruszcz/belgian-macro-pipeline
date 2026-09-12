# Feature: WalStat adapter — Walloon municipal finance

Status: draft
Issue: (Block O — Walloon municipal finance, docs/steps)
Branch: feat/walstat-adapter

## Problem

Municipal finance is the paid product's core (docs/steps, Block O), and the commune profile has
carried four empty "Finances locales" tiles since Batch 4 saying so. The source was selected in
Block E (docs/data_catalog.md rows 8–9, Selected 10 row 5) and its licence confirmed — CC0 for
the data, from IWEPS' own FAQ — but nothing had ever read it: the catalogue recorded a portal and
a DCAT index, not an endpoint, a unit, or a row shape. This pass reads the real thing and loads
it.

## Goal (this pass)

Nine WalStat series into `observations` for every Walloon commune, 2013 onward, through the
shared `DataSource` interface (Block D): municipal revenue, expenditure and debt — each as
total / ordinary / extraordinary (revenue, expenditure) or total / long-term / short-term (debt) —
plus four ratios in the derived engine, from config alone.

## Non-goals (this pass) — and why

- **Totals in euros.** WalStat publishes every series *per inhabitant* (`euro(s)/habitant`, read
  off the indicator fiche in a browser, 2026-09-11). No total exists in the source, and
  multiplying by a population would be this pipeline computing a figure the source does not
  publish. The roadmap's "raw series only" is honoured as *what the source has*.
- **Personnel costs, balance, structural balance.** Not published on WalStat's "Pouvoirs
  locaux" theme. A balance would need revenue *minus* expenditure, a `difference` function
  `src/analytics/derived.py` does not have; adding one is an analytical-formula change and needs
  an ADR (CLAUDE.md rule 19). Deferred, recorded in `docs/implementation/known-risks.md`.
- **The implicit municipal tax rate** (series 831102_0, 2004–2023, percent). A different period
  range and a different kind of figure; a candidate for a later pass, not folded in blind.
- **Provinces and CPAS accounts** (811502–811505, 811507). Different entity types; out of scope.
- **Flemish and Brussels communes.** WalStat covers Wallonia. The page must say "Walloon source"
  for the other 303 communes rather than show a blank — that is the UI batch's job, and the
  five-states rule (CLAUDE.md rule 26) is why.

## Data source

- **API**, no key, no registration:
  `https://opendata.iweps.be/api/data/{json|csv}/{indicateur_id}_{ordre}/{options}`, options joined
  with `+`: `com` (every commune) or `ins=NNNNN`, and `period=all|last`. Documented on
  `https://walstat.iweps.be/walstat.php?action=open-data`, which also states the licence:
  "Les données et le catalogue sont distribués sous Licence CC0".
- **Series** (theme 17, "Pouvoirs locaux"; ids read off the catalogue page):

  | series | indicator | what |
  |---|---|---|
  | 811500_0 / _1 / _2 | `MUN_REVENUE_{TOTAL,ORDINARY,EXTRAORDINARY}_PER_CAPITA` | recettes globales / ordinaires / extraordinaires |
  | 811501_0 / _1 / _2 | `MUN_EXPENDITURE_{TOTAL,ORDINARY,EXTRAORDINARY}_PER_CAPITA` | dépenses globales / ordinaires / extraordinaires |
  | 811506_0 / _1 / _2 | `MUN_DEBT_{TOTAL,LONG_TERM,SHORT_TERM}_PER_CAPITA` | dettes globales / à long terme / à court terme |

  Unit `euro(s)/habitant`; source "SPW - Intérieur et action sociale - Département des Finances
  locales, Communauté germanophone - DG Stat"; periods "année 2013" … "année 2024"; last
  modified 17–18/03/2026. IWEPS' own definition, quoted in each config: expenditure rests on
  "imputations comptables à l'exercice global" (amounts actually spent, not commitments);
  revenue on "droits nets constatés".
- **Row shape**, the whole of it:
  `{"ins":"92094","type_entite":"Commune","entite":"Namur","periode":"année 2024","valeur":"2319.7"}`.
  No metadata, no unit, no status, no masking flag. `periode` varies in capitalisation
  ("Année 2023" occurs). Probed 2026-09-11: `811500_1/com+period=last` returns 262 rows, all
  communes, INS prefixes 2/5/6/8/9 only.
- **Coverage arithmetic.** 262 is the number of Walloon communes on 2024-01-01: 261 today plus
  Bastogne (82003) and Bertogne (82005), merged into 82039 on 2024-12-02. The reference set for
  a year is read from the geography table's parent chain (`walloon_communes_on`), never
  hardcoded, which is why `_parse` requires a connection.
- **What the first live run taught (2026-09-11), all three now handled and tested.** The API's
  documentation stops at the URL grammar; these are the source's actual conventions:
  1. **`"non disponible"`** is WalStat's own marker for an account not filed — 8 cells per
     series today (51019, 61041 and 91114 for 2024; 53068 for 2020–2024).
     A missing reading: skipped, counted in `rows_read`, never written as 0 and never as
     `suppressed` (nothing is withheld; the account does not exist yet). Any other non-numeric
     value is still refused.
  2. **Every year is published on the current commune grid.** The eleven Hainaut communes
     re-coded on 2019-01-01 (Enghien 55010→51067, Silly 55039→51068, Lessines 55023→51069,
     Seneffe 52063→55085, Manage 52043→55086, Mouscron 54007→57096, Comines-Warneton
     54010→57097, La Louvière 55022→58001, Binche 56011→58002, Estinnes 56085→58003,
     Morlanwelz 56087→58004) appear under their new code for 2013–2018 too. Same commune, same
     name, one predecessor: `resolve_on_source_grid` walks the geography table's own
     `successor_geo_id` back one step, only when that step is 1:1 and the names agree, and
     loads the value under the code valid that year. Verified against the real list. A code
     with two predecessors is a merger and is never substituted.
  3. **Bastogne+Bertogne (82039) is listed for every year back to 2013** with a backcast
     per-inhabitant figure (2013: 2 045,4, between Bastogne's 2 225,7 and Bertogne's 1 208,8),
     beside its two parts. For a year in which the merged commune did not exist, that row is
     skipped and counted — never loaded as a third commune, never silently dropped. For 2024
     the two parts load (they existed on 2024-01-01); from 2025 only 82039 will.
  4. **A commune can be absent from a year altogether** (53068 from the 2017 revenue series).
     That is a missing reading, recorded in `missing` and reported by the sync — not a refusal:
     refusing 261 real accounts for one absent one would be the worse error. An **unexpected**
     commune (a Flemish code in a Walloon series) is refused, and a year covering **fewer than
     90 %** of the communes is refused as a partial response — the pipeline's existing coverage
     floor for aggregates (CLAUDE.md, Definitions), not a new threshold.
  Live result on a copy of the database: 28 221 observations, 9 series × 12 years, 273
  distinct geo_ids (262 current codes + 11 re-coded predecessors), 18 `fetch_runs` rows all
  `ok`, `validate_data` 0 failures. Namur 2024 in the exported payload equals the API probe
  (2 319,7 / 2 167,1 / 2 965,5), the four ratios match the hand computations to four decimals,
  and Antwerp's payload carries no WalStat series.
- **A fixture from the real API**: `tests/fixtures/walstat_sample.json`, the 2013–2024 rows for
  Namur (92094) and Charleroi (52011) on all nine series, fetched 2026-09-11 and committed verbatim.
  Namur 2024: revenue ordinary 2 319,7, expenditure ordinary 2 167,1, debt total 2 965,5 €/hab.
  Namur 2013 shows the accounts' own identity: total expenditure 1 722,1 = ordinary 1 438,7 +
  extraordinary 283,4.

### Contract — the fourth `MunicipalTimeSeriesSource`

`src/fetchers/walstat.py:WalStatSource` returns `{"geo_id", "period", "value", "status"}` and is
the first municipal adapter that resolves geography through `resolve_geo(nis, period)` alone —
WalStat publishes the NIS code, Statbel's Bestat did not. Every check refuses rather than
coerces (rule 13):

1. the response is a bare JSON array of objects with exactly the five documented keys;
2. every row's `type_entite` is `Commune` (the same ids answer `provinces` with other types);
3. `periode` matches `^ann[ée]e\s+(\d{4})$` case-insensitively;
4. `valeur` parses as a number — an empty value is not zero and not a suppression, because
   WalStat documents no masking rule;
5. no `(ins, year)` twice;
6. the NIS code resolves for that year — directly, or through the 1:1 re-coding rule above
   (`WalStatGeographyError` otherwise); a merged commune's backcast for a year before the
   merger is skipped and counted;
7. per year, no commune outside Wallonia, and at least 90 % of the Walloon communes valid on
   that year's first day (`walloon_communes_on`, a recursive walk of the parent chain to
   `be:reg:03000`); communes absent within that floor are recorded as missing readings.

Status is `final` on every row: closed accounts, and no provisional marker exists to carry.

## Loading

`scripts/sync_walstat.py --db data/belgian_macro.db`, the shape of `sync_statbel.py`: the
indicators it loads are every `config/indicators/*.yaml` with `source_id: walstat`, each carrying
its own `fetch.query` (`/json/811500_1/com+period=all`), so adding a series is config. Reference
rows: `INSERT OR IGNORE` the source, upsert each indicator's names/direction from YAML with
`is_additive=0`, `aggregation_method='not_applicable'`, `decimals=1` — per-inhabitant figures
cannot be summed or averaged to a province (docs/decisions/0003). Observations go through
`upsert_observation` (Block I, insert-only-on-change). `--from-dir` replays cached files with the
same parse and the same refusals; `--reference-rows-only` needs no network.

Wired into `make fetch` and, since 2026-09-12, `.github/workflows/daily_fetch.yml`
(`sync_walstat`, `continue-on-error`, in the failure gate, right after `sync_onem`).

That wiring was held for one day: exporting `data/communes_history.csv` with the nine series
loaded measured 45.2 MB against the workflow's 40 MB commit tripwire (measured on a copy,
2026-09-11) — the first daily build after the load would have refused to commit anything, for
every source, not only WalStat's. Fixed at the source rather than by raising the tripwire again
(it had already been raised twice, for ONEM and for the real-estate refresh):
`export_communes_history_csv.py` now writes the last 10 calendar years by default and the
complete series with `--all-periods`, mirroring `export_percentiles_csv.py`'s own precedent.
The full series still reaches every page that reads it — `export_communes_table_json.py` and
`export_site_payloads.py` are pointed at a gitignored `data/communes_history_full.csv`
(`--all-periods`), never at the trimmed file that gets committed — and the trim is applied only
to which rows are WRITTEN, after the derived engine has already computed on the untrimmed
series, so a 10-year CAGR still sees the value 10 years back even though that row itself falls
outside the output window. Verified against a live sync on a copy of the database: the
committed file lands at 32.5 MB (was 19.7 MB before WalStat, would have been 45.2 MB
untrimmed), the gitignored full file at 44.8 MB, and Namur's `communes/92094.json` still
carries its complete 2013-2024 WalStat history — unaffected by the trim, as designed.

Ratios, config only (`config/indicators/derived/`): `MUN_REVENUE_GROWTH_1Y`,
`MUN_EXPENDITURE_GROWTH_1Y` (`growth_rate`, years 1), `MUN_INVESTMENT_SHARE_OF_EXPENDITURE`
(`share_of_total`, extraordinary over total expenditure), `MUN_DEBT_TO_REVENUE` (`share_of_total`,
total debt over total revenue). Per-inhabitant on both sides, so the population denominators
cancel and each ratio equals the one on totals.

Unit `eur_per_inhabitant` is new; `assets/commune_map.js` (`formatValue`, `tickLabel`,
`unitSuffix`) and `src/pages/resolve.py` (`_format_value`) render it as "€ 2 319,7 / hab." in each
language rather than as a euro total.

## Tests

- `tests/test_walstat_source.py`: the happy path on the committed fixture against a real
  geography load; every refusal above, each from a hand-built response; the Bastogne year.
- `tests/test_sync_walstat.py`: `--from-dir` into a migrated temp database, rows and vintages,
  reference rows, not-applicable aggregation, idempotence (second run writes nothing).
- `tests/test_source_contract.py`: a municipal contract case for WalStat.
- Hand-computed ratios (rule 5), all from the fixture: Namur debt 2024 vs 2023 =
  (2965.5 − 3077.5) / 3077.5 × 100 = −3.6393 %; Charleroi = (6239.3 − 5561.7) / 5561.7 × 100 =
  +12.1833 %; Namur investment share 2024 = 566.4 / 2733.5 × 100 = 20.7207 %; Namur debt to
  revenue 2024 = 2965.5 / 3000.6 × 100 = 98.8302 %.

## Assumptions and open questions

- **Every row is `final`.** WalStat publishes no status. If IWEPS ever revises a year, the
  vintage machinery records the change; the status word will not say "revised" because the
  source never will.
- **262 communes for 2024** is read as Bastogne and Bertogne still reporting separately for the
  2024 accounts. If a future year returns the merged commune for an earlier period, check 7
  refuses it and the reason is in the message.
- **The maintainer's "I'll confirm licences later — the website isn't even deployed"
  (2026-09-11).** Recorded because it was said; it does not apply here, since WalStat's licence
  was confirmed on 2026-09-06 and no other source is added in this pass.

## Rollout / risks

- Nothing changes for any existing indicator; the new rows are additive and behind their own
  source id. Rollback is dropping the nine indicators' observations and configs.
- `fetch_window_days: 14` is now set, alongside the workflow step that gives it a `fetch_runs`
  row to measure against -- declared with no such row, the `fetch_silence` rule would have
  warned on every build, forever. It fires correctly from the step's first run onward: silent
  for 14 days after a successful fetch, then WARN if the runner stops reaching
  opendata.iweps.be.
- The committed database still carries the nine reference rows and the `sources` row, no
  observations, on this branch: loading them takes it from 19.8 MB to 32.3 MB (measured with
  the trim above already in place), safely under CLAUDE.md rule 12's 25 MB single-file
  threshold on its own, well under the workflows' 40 MB tripwire. The daily run loads the
  observations itself the first time it runs with the new step; nothing here does it
  pre-emptively; a derived config whose inputs are configured but not yet in the store is
  DEFERRED by `load_and_validate_derived` — left out of that export, never an error — so the
  four ratios appear the first build after the nine series load, and until then every other
  export is untouched (audit P0-1: before this, they stopped every export on a database
  without WalStat rows).
- Two `fetch_runs` rows per series, as in `sync_statbel.py`: the adapter logs its own
  (`rows_read` counting the unavailable and backcast rows), and the script writes the one its
  observations reference. A series that refuses marks the script's row `error` too, so the
  `fetch_error` rule sees it.
- The exporter's `_indicator_index` reads `direction`/`decimals` from the `indicators` table,
  which has no row for a derived indicator, so the four ratios publish `direction: null` and
  `decimals: null` in `metadata/indicators.json` — the same as every existing derived indicator
  (`UNEMPLOYMENT_RATE_COM`, `POPULATION_CHANGE_5Y`). Pre-existing; recorded in known-risks.
- The API is undocumented beyond the open-data page. Check 1 is what tells the maintainer the
  day it changes.
