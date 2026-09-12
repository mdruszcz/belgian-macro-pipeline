# ONEM's published unemployment rate on the commune page

Approved by the maintainer: 2026-09-12.
Decision record: [ADR 0005](../decisions/0005-onem-published-rate.md).
Catalogue row: `docs/data_catalog.md`, "ONEM/RVA — the published unemployment RATE".

## The problem this closes

The commune page has had an unemployment headline since the beginning, and none of the
candidates was both current and a real rate:

| Indicator | What it is | Why it was not enough |
|---|---|---|
| `UNEMPLOYMENT_RATE_COM` | Statbel Census 2021 register rate | One period, frozen at 2021, 552 communes |
| `ADMIN_UNEMPLOYMENT_RATE_COM` | Steunpunt Werk administrative rate | 2024, and administrative rather than a labour-force rate |
| `UNEMPLOYMENT_RATE_BIT` | ILO/LFS rate, the internationally comparable one | Wallonia only, and only to 2023 |
| `UNEMPLOYMENT_CLAIMANT_RATE_WORKING_AGE` | ONEM claimants over the 15-64 population | The denominator is not a labour force, so it reads about a third of a real rate |

`docs/data_catalog.md` recorded the reason as "ONEM publishes no local labour-force denominator".
**That was wrong**, and the correction is in place there. ONEM publishes a commune-level rate with
a real denominator and computes it themselves.

## What was added

Two indicators, both `source_id: onem`, loaded by `scripts/sync_onem_rates.py`:

- **`UNEMPLOYMENT_RATE_INSURED`** (annual) — the level. Full history from 2017. The running year is
  `provisional` because ONEM's annual row averages only the months published so far.
- **`UNEMPLOYMENT_RATE_INSURED_MONTHLY`** (monthly) — the shape. The last 18 months, a file-size
  limit and not a data one; see "Known costs" below.

Both are the same published quotient:

> Le taux de chômage résulte de la division du nombre de CCI demandeurs d'emploi par le nombre
> d'assurés contre le chômage.
> Source: calculs ONEM sur base des données de l'ONEM, de l'ONSS et de l'INAMI.

**Nothing in this pipeline performs that division.** The numerator is `UNEMPLOYED_JOBSEEKERS`, which
we already load; the denominator is assembled by ONEM across three institutions and is not published
anywhere. So the rate is loaded as source data, attributed to ONEM, exactly like Statbel's census
rate or Steunpunt Werk's administrative rate.

## Why there are two and not one

The maintainer's instruction, 2026-09-12: *"on affiche les deux, il est important de voir les effets
de la fin des allocations."*

Belgium's rate sits between 6.26 % and 6.85 % from 2023 through February 2026, then reads 5.47 in
March and 4.41 in April. The fall shows up in **546 of 565 communes**. Time-limiting unemployment
benefit takes people out of the CCI-DE numerator whether or not they find work, so this is an
administrative break, not a labour market improving.

An annual mean hides where that happened. A monthly series shows it. Both ship, and the break is
written into both indicator descriptions in all three languages and into the section blurb on the
page. **A reader told "Namur 6.74 %, down 3.9 points in a year" without that context concludes
something false about Namur.**

Namur, as published:

| Period | Rate |
|---|---|
| 2026-01 | 10.23 |
| 2026-02 | 9.88 |
| 2026-03 | 8.20 |
| 2026-04 | 6.74 |
| 2026-06 | 6.74 |

## Where it appears

- **Hero row** on the commune page: `UNEMPLOYMENT_RATE_INSURED` replaces
  `ADMIN_UNEMPLOYMENT_RATE_COM`, which moves into the Employment section rather than disappearing.
- **Employment section**: headline `UNEMPLOYMENT_RATE_INSURED` (so it gets the chart, the comparison
  table and the percentile), then the monthly series, then the administrative rate, then the ONEM
  claimant counts.
- All of it through `config/local_sections.yaml`. No indicator id was added to any page or script —
  rule 2 and rule 24.

## How the file is fetched

The interactive map at `interactivestats.services.rvaonem.fgov.be` is a JSF application with a
session, a `ViewState` and AJAX POSTs, which looks like something that must be driven with a
browser. It does not: the page loads `papaparse`, and its own controller
(`js/UnemploymentRatesController.js`) names the CSV it parses.

```
https://interactivestats.services.rvaonem.fgov.be/interactivestats/csvResource/interact_taux_V1.csv
```

One plain GET. 2.0 MB, 77,376 rows, the whole published history. Verified 2026-09-12 that the bytes
are identical with and without the browser's session cookies, so this is an ordinary download of a
published file, and the new step in `daily_fetch.yml` is the live check that GitHub's runners reach
that host.

## What the loader refuses

Every one of these stops the load rather than degrading it:

- a header that is not exactly `jaar;maand;level;zonegeog;graad;diff1an`, **including a reordering**
  — the rows are zipped positionally, so `graad` and `diff1an` swapped would load a change in
  percentage points as a level;
- a blank or non-numeric `graad`. The file has no masked cells today, which makes a coerced zero the
  live hazard rather than a theoretical one (rule 26);
- a `maand` outside 1-13;
- a commune set that is not **exactly** the municipalities in force today, in both directions. An
  unknown code means we would drop a figure; a missing commune means its page shows a blank while
  every neighbour shows a rate, which reads as "no unemployment here";
- a `diff1an` that disagrees with our own 12-month difference by more than 0.02 pp.

That last one is the file auditing the parser. `diff1an` is **not loaded** — it is a derived value
and rule 6 forbids writing one in as source data — but across 69,264 pairs it equals our own
difference to within 0.01 pp of rounding, so a shifted column or a mis-built key fails before
anything is written.

## The one true zero

Herstappe, roughly eighty residents, reads exactly `0.00` in 42 of its 124 periods and a real value
in the other 82. **That is an observed zero** and is stored as the number 0.0. A zero that became
NULL would read as "not published"; a NULL that became zero would read as "no unemployment". Tested
both ways against the real file.

## What is deliberately NOT done

- **No province, region or arrondissement figure.** The denominator is unpublished, so ADR 0003's
  recompute-from-sums is impossible, and `population_weighted` is forbidden by CLAUDE.md and wrong
  here anyway. ONEM's own aggregates are skipped too: their level 2 splits Wallonia from the
  German-speaking community, which is not this repository's Wallonia. Belgium (zone 99) is
  unambiguous and is loaded. Both indicators carry `aggregation_method = not_applicable`.
- **No `diff1an`**, see above.
- **No smoothing of the March 2026 break.** Carried and labelled.
- **Period-accurate geography resolution.** ONEM restates all periods on today's 565-commune map and
  31 of those codes did not exist in January 2017, so resolution is against current municipalities —
  the same pinned-vintage treatment `scripts/sync_onem.py` already applies to the same publisher.

## Known costs, recorded rather than hidden

- **The committed database is 37.9 MB against `daily_fetch.yml`'s own 39.06 MB guard**, and was
  already past CLAUDE.md rule 12's 25 MB ceiling before this change. That guard, not data quality,
  is why the monthly window is 18 months: 24 months measures 39.45 MB and fails it.

  **The fix is not to stop committing the database.** `daily_fetch.yml` checks it out, applies
  pending migrations and *appends*; it never rebuilds it. It is the only store that holds the
  automated sources — ONEM, WalStat, Statbel, the macro series — and superseded vintages exist
  nowhere else at all. Feeding it is right; dropping it would lose data.

  What is actually big is **index, 62 % of the file**, measured with `dbstat`:

  | object | size | share |
  |---|---|---|
  | `observations` (the data) | 13.60 MB | 35.9 % |
  | the primary key's own B-tree | 7.31 MB | 19.3 % |
  | `idx_obs_series` | 4.91 MB | 13.0 % |
  | `idx_obs_geo_indicator` | 4.10 MB | 10.8 % |
  | `idx_obs_indicator_period` | 3.48 MB | 9.2 % |
  | `idx_obs_geo_period` | 2.55 MB | 6.7 % |
  | `idx_obs_run` | 0.88 MB | 2.3 % |
  | everything else | 1.05 MB | 2.8 % |

  An index holds no information. The five secondary ones rebuild from the data in **0.68 s**
  (measured on 86k rows) and their DDL is already `CREATE INDEX IF NOT EXISTS` in
  `migrations/002_indexes.sql`. Not committing them gives **21.96 MB** — inside rule 12's ceiling
  for the first time since WalStat landed — and **17.1 MB of headroom** instead of 1.18 MB, which
  is about five more years of monthly detail.

  One obvious-looking idea was measured and **rejected**: making `observations` `WITHOUT ROWID` to
  fold that 7.31 MB primary-key B-tree into the table makes the file **bigger, 51.14 MB**, because
  a `WITHOUT ROWID` table's secondary indexes each store the whole primary key — four TEXT columns
  here — as their row locator. It only pays combined with dropping the secondary indexes
  (14.38 MB), which is option one plus a schema migration for no extra gain.
- The monthly series is kept out of the committed `data/communes_history.csv` (already 35 MB) but
  does reach the gitignored `--all-periods` file that feeds the payloads, so the commune page has
  the whole stored series.

## Tests

`tests/test_sync_onem_rates.py` — 43 tests: the shape refusals, the rule-26 states, the `diff1an`
identity including per-zone and per-level keying, the coverage check in both directions, the
provisional rule derived from month coverage, the monthly window arithmetic, the levels that are and
are not loaded, and three tests against the real cached file whose expected values were read off
ONEM's own PDF export before this loader existed.

`tests/test_unemployment_rates.py` — extended to six measures, asserting the new pair is source data
and not derived, differs from its twin only in frequency, names every other measure it is not,
records the March 2026 break in all three languages, and claims no geographic level it cannot
defend.
