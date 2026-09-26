# Feature: manual-only sources

Status: done
Issue: (arises from Block F — Statbel adapter, docs/steps)
Branch: refactor/split-committed-stores

## Problem

Some approved datasets can never be fetched by CI. Statbel's `TF_SOC_POP_STRUCT` bulk files
(population by commune, and the age bands derived from them) live on `statbel.fgov.be`, which is
unreachable from this pipeline's network context — confirmed three separate ways, with DNS resolving
fine, so it is a connection-level block rather than DNS or a 403. Each file is ~100 MB and must be
downloaded by hand.

Until now those rows sat in `data/belgian_macro.db`, which the daily workflow re-commits every day.
That meant 10.56 MB of data that changes roughly annually was re-committed daily, in a binary format
git cannot delta. See [ADR 0002](../decisions/0002-split-committed-stores.md) for the measurements.

## Goal

- Manual-only data has a durable, reviewable home in the repository that does not ride along with the
  daily commit.
- The published exports are identical whether an indicator came from the database or the manual
  store — the split is invisible downstream.
- A fresh clone can rebuild a queryable database from what is committed, with no network.

## Non-goals

- Automating the download. It is blocked at the network layer; pretending otherwise would produce a
  workflow that fails every night.
- Committing the raw `TF_SOC_POP_STRUCT_*.txt` files (~1.1 GB). They stay under `data/raw/`, which is
  gitignored.

## The committed stores

Every observation store is declared once, in [`config/stores.yaml`](../../config/stores.yaml)
(loaded by `src/stores.py`). That file is the authority; this table is a reading aid and names no
indicator the registry does not.

| Store | Holds | Mode | Committed | Written by |
|---|---|---|---|---|
| `data/belgian_macro.db` | national macro, `LOCAL_UNITS_BY_COMMUNE`, geography, indicator metadata, fetch and volume history | — | daily, by the bot | the daily run's offload (`committed_stores`) |
| `data/population_observations.csv` | `POPULATION_BY_COMMUNE`, `POPULATION_AGE_0_14/_15_64/_65_PLUS` | `extra_csv` | only when refreshed by hand | the procedure below |
| `data/fiscal_income_observations.csv` | `FISCAL_TOT_NET_TAXABLE_INC`, `FISCAL_NBR_NON_ZERO_INC`, `FISCAL_TOT_TAXES`, `FISCAL_TOT_MUNICIP_TAXES` | `extra_csv` | only when refreshed by hand | [fiscal_income.md](fiscal_income.md) |
| `data/census2021_observations.csv` | 17 Census 2021 counts: labour force status, citizenship, birthplace, sex, marital status, households, family nuclei, dwellings | `extra_csv` | only when refreshed by hand (decennial) | the procedure below |
| `data/realestate_observations.csv` | `MEDIAN_HOUSE_PRICE`, `HOUSE_SALES_TRANSACTIONS` | `extra_csv` | only when refreshed by hand | `scripts/sync_realestate.py` |
| `data/police_observations.csv` | four crime rates per 10,000 inhabitants | `extra_csv` | only when refreshed by hand | `scripts/sync_police.py` |
| `data/var_unemployment_observations.csv` | `ADMIN_UNEMPLOYMENT_RATE_COM`, annual administrative unemployment rate for ages 15–64 | `extra_csv` | only when the Tableau crosstab is refreshed by hand | [var_unemployment.md](var_unemployment.md) |
| `data/onem_observations.csv` | seven ONEM/RVA unemployment and benefit series | `in_db` | daily, by the bot | the daily run's offload (`committed_stores`) |
| `data/onem_rates_observations.csv` | `UNEMPLOYMENT_RATE_INSURED`, `UNEMPLOYMENT_RATE_INSURED_MONTHLY` | `in_db` | daily, by the bot | the daily run's offload (`committed_stores`) |
| `data/walstat_observations.csv` | ten WalStat (IWEPS) municipal series | `in_db` | daily, by the bot | the daily run's offload (`committed_stores`) |

Not an observation store, listed because it is generated from one:
`public/data/demography/{nis}.json` (five-year population bands by sex for the commune-profile
pyramid, written by `scripts/export_commune_age_sex.py` when the population source is refreshed).

Also not an observation store, and generated from the same download rather than a new one:
`public/data/demography_history/{nis}.json` (every year Statbel has published for
`TF_SOC_POP_STRUCT`, summed onto today's 565 communes, written by
`scripts/export_commune_age_sex_history.py` — see "Refreshing the age-pyramid history" below).

`extra_csv` stores are hand-loaded and merged at export time. `in_db` stores are fetched by CI,
loaded into the working database for the run, and dumped back by `scripts/offload_stores.py`
([ADR 0006](../decisions/0006-stores-split-by-volume.md)).

`data/local/` holds the disposable working database and is gitignored. It must stay ignored:
`daily_fetch.yml` runs `git add data/`, which would otherwise commit it.

## `statbel.fgov.be` reachability, re-checked 2026-09-06

Earlier notes in this file called the block "connection-level"; a later note in `data_catalog.md`
walked that back to "likely bot-protection" after search engines reported CAPTCHA pages at the same
URLs. **Re-checked directly and the original claim was right.** `curl -v` against
`statbel.fgov.be` shows DNS resolving cleanly on both address families, then the **TCP handshake
itself timing out** — not an HTTP-level CAPTCHA response, a connection that never completes. The
CAPTCHA a search engine sees is a separate, additional defence Statbel runs for crawlers; this
pipeline's network context is blocked before it ever gets that far. `data_catalog.md`'s "likely
bot-protection" framing is corrected by this note.

## Statbel's file-naming conventions for direct downloads

Maintainer-supplied 2026-09-06, from Statbel's own Drupal file structure — useful for knowing
exactly what to ask for on a manual download, and for recognising a file once downloaded.

**Convention 1 — pre-built table exports** (`.../Census2021/T01_CAS_AGE_COM_FR.XLSX`):

| Segment | Meaning |
|---|---|
| `T01`, `T02`, … | Table number |
| `CAS_AGE`, `EDU`, `ACT`, `MIG`, … | Theme: `CAS` = civil status, `AGE` = age, `EDU` = education, `ACT` = activity/labour, `MIG` = migration |
| `BE` / `REG` / `PROV` / `ARR` / `COM` / `SEC` | Geography: national / regional / provincial / arrondissement / **commune** / statistical sector |
| `FR` / `NL` | Language |

A commune-level, French file is therefore `*_COM_FR.XLSX` (extension casing varies).

**Convention 2 — the ten files already loaded here** (`TF_CENSUS_2021_HC03_1.xlsx`, …): Statbel's
bulk "hypercube" exports, `HCnn_m` numbered, bilingual columns rather than a language suffix, and
the shape `sync_census2021.py`'s `EXTRACTS` maps. A different corner of the same site — both are
legitimate, current Census 2021 data.

**Neither can be fetched from this pipeline's network context** — see the reachability note above —
so both still require a maintainer download.

## Refreshing Census 2021

Statbel publishes ~140 Census 2021 open datasets under CC BY 4.0, but only on
`statbel.fgov.be`, which automation cannot read. The Bestat API *does* carry Census 2021
(`IM_SOC_GEO_IND_CENSUS_2021`, `IM_SOC_GEO_NUC_CENSUS_2021`), but all 174 of its views were probed
across four locales and every one stops at province or arrondissement — none reaches commune. So the
workbooks are downloaded by hand from
`statbel.fgov.be/fr/open-data/consultez-tous-les-open-data-du-census-2021` into
`data/raw/statbel/census2021/` (gitignored), and then:

```bash
# 1. Disposable local database (schema + geography, both offline)
python -m src.db.migrate --db data/local/census.db
python scripts/load_geography.py --db data/local/census.db

# 2. Load the workbooks -- resolve_geo applies directly, since these files
#    carry a real NIS code in CD_REFNIS_LVL_4
python scripts/sync_census2021.py --db data/local/census.db

# 3. Write the committed store
python scripts/export_observations_csv.py --db data/local/census.db \
  --out data/census2021_observations.csv \
  --indicators "$(python -c "import sys;sys.path.insert(0,'scripts');from sync_census2021 import EXTRACTS;print(','.join(EXTRACTS))")"
```

**Which tables map to which indicators is in `EXTRACTS` in `scripts/sync_census2021.py`**, keyed by
the files' own coded columns rather than their French labels — the labels are prose that a future
publication could re-word, the codes are the file's keys. A renamed or missing column raises rather
than being patched around.

**The check that anchors the whole dataset:** summing Census 2021's population table gives 11,521,238
people over 581 communes, and the pipeline's independent `TF_SOC_POP_STRUCT` series gives the
identical total for 2021 with **all 581 communes matching exactly, commune by commune**. Two
unrelated Statbel products agreeing to the person is what establishes the reference date (1 January
2021) and validates the NIS mapping. `tests/test_census2021.py` asserts it rather than describing it.

**Total population is deliberately not stored again** — it is already `POPULATION_BY_COMMUNE`, and
the exact match proves the two are interchangeable. Shares are not stored either; they are derived
from these counts at export time so a correction propagates (CLAUDE.md rule 6).

## Refreshing population data

Download the year's file from Statbel by hand (URL and the six-vs-eleven-years reasoning are in
`scripts/plot_population_continuity.py`'s docstring) into `data/raw/statbel/population/`, zipped or
extracted. Then:

```bash
# 1. Build the local, disposable database (schema + geography, both offline)
python -m src.db.migrate --db data/local/manual.db
python scripts/load_geography.py --db data/local/manual.db

# 2. Load the raw files -- vintage/is_latest discipline as any other source
python scripts/sync_population.py --db data/local/manual.db

# 3. Write the committed store
python scripts/export_observations_csv.py --db data/local/manual.db \
  --out data/population_observations.csv \
  --indicators POPULATION_BY_COMMUNE,POPULATION_AGE_0_14,POPULATION_AGE_15_64,POPULATION_AGE_65_PLUS

# 4. Regenerate the published exports -- the latest-only snapshot and the
#    full year-by-year history (communes.html's year selector), which also
#    computes every configured derived indicator over the whole history
python scripts/export_communes_csv.py --db data/belgian_macro.db \
  --out data/communes_export.csv \
  --extra-observations data/population_observations.csv \
  --extra-observations data/fiscal_income_observations.csv
python scripts/export_communes_history_csv.py --db data/belgian_macro.db \
  --out data/communes_history.csv \
  --extra-observations data/population_observations.csv \
  --extra-observations data/fiscal_income_observations.csv

# 5. After export_site_payloads.py has refreshed public/data/communes, preserve
#    the source's age × sex dimensions for the visual population pyramid.
#    Use the Last-Modified date reported for the downloaded Statbel file.
python scripts/export_commune_age_sex.py \
  --source data/raw/statbel/population/TF_SOC_POP_STRUCT_2026.zip \
  --source-updated 2026-06-10
```

Then commit `data/population_observations.csv`, `data/communes_export.csv` and
`data/communes_history.csv`, plus the generated `public/data/demography/*.json`, and open a PR. The
age-by-sex exporter refuses to publish unless it finds every current commune and every pyramid sums
exactly to that commune's already-published population for the same year. The diff on each CSV is
readable line-by-line — that is the point of storing them as text.

Steps 3 and 4 are also available as `.github/workflows/manual_sources.yml`
(`workflow_dispatch` only), which regenerates the export from the already-committed CSV and opens a
PR. It deliberately does **not** attempt step 1 or 2: it cannot reach Statbel and does not have the
raw files.

## Refreshing the age-pyramid history

Statbel keeps every year of `TF_SOC_POP_STRUCT` on the same page as the current year — 2010
through 2026 all resolve at the time of writing, at the same URL pattern as step 5 above, with
only the year changing. Download every year you want into `data/raw/statbel/population/` (same
gitignored directory, still not committed — ~2.5 MB per year zipped), then:

```bash
python scripts/export_commune_age_sex_history.py \
  --source-dir data/raw/statbel/population \
  --output-dir public/data/demography_history
```

This reads every `.zip`/`.txt`/`.csv` file in `--source-dir` with the exact same column-picking
reader `export_commune_age_sex.py` already uses (so a schema Statbel changes is refused the same
way, in the same place, for both scripts), sums each year's counts onto the commune that holds that
territory TODAY, and writes one `public/data/demography_history/{nis}.json` per current commune:

```json
{"nis_code": "...", "source_id": "statbel", "bands": [{"from": 0, "to": 4}, ...],
 "years": [{"period": "2010", "reference_date": "2010-01-01",
            "male": [...21 band counts...], "female": [...21 band counts...],
            "coverage": {"found": 1, "expected": 1}}, ...]}
```

**Growth on current territory** (the maintainer's 2026-09-16 decision, already applied to
`POPULATION_CHANGE_5Y`/`POPULATION_CAGR_10Y` by `src/analytics/backaggregate.py`): a merged
commune's pre-merger years sum every predecessor's own counts onto the successor, because for
those years the predecessor and the successor are different, non-overlapping pieces of today's
territory — never averaged, never estimated. The lineage comes straight from
`config/geography/municipality_crosswalk.csv` (`old_nis` → `new_nis`), walked recursively with a
cycle guard exactly as `backaggregate.py`'s `resolve_successor` walks `successor_geo_id` — this
script stays a plain NIS-keyed CSV/JSON reader with no database access, so it reimplements that
walk rather than importing from `src/`, but the semantics (ignore `has_partial_transfer`,
lineage keyed by the FINAL successor after every hop) are identical on purpose.

**Coverage** records, per commune per year, how many of the expected contributing communes
(the commune itself, plus any predecessor that had not yet merged away) actually had rows in that
year's file. It is never used to fill or estimate a missing cell — a shortfall is published as a
smaller, honestly-labelled sum, not silently completed.

Verified against the real 17 files (2010–2026): commune counts step from 589 (2010–2018) to 581
(2019–2024) to 565 (2025–2026), exactly matching the crosswalk's two merger waves, and every merged
commune's `coverage` goes from `found == expected == 2` (predecessor still reporting) to
`found == expected == 1` (successor alone) at its own merger year, with the summed total moving
continuously across the boundary — no step, no gap. Namur (92094, no lineage) matches
`POPULATION_BY_COMMUNE` exactly for every year that indicator carries (2017–2026).

This does **not** touch `public/data/demography/{nis}.json` — that single-year payload keeps being
written by `export_commune_age_sex.py` exactly as before, byte-identical, since pages already read
it. It follows the same manual, hand-triggered route as the rest of this file: it is not part of
`local-automation/refresh-captcha-sources.ps1`'s monthly loop (that script only covers
bankruptcies, population movement and the IPP rate — sources whose sync code can run against an
already-downloaded file). A future year's history refresh is the same by-hand procedure as
"Refreshing population data" above, just pointed at a directory of files instead of one.

## Rebuilding from what is committed

```bash
python scripts/load_observations_csv.py --db data/local/manual.db \
  --csv data/population_observations.csv
```

Needs no network and no raw files: schema from `migrations/`, `geographies` from
`config/geography/*.csv`, indicator and source metadata from `config/*.yaml`.

## Tests

`tests/test_observations_csv_roundtrip.py`:

- the export is sorted and byte-stable, so an unchanged re-export produces an empty diff;
- CSV → database → CSV round-trips losslessly, with no foreign-key violations;
- **the commune export is byte-identical whether an indicator came from the database or the CSV** —
  the property that makes the split safe;
- the loader refuses an indicator with no config, and a missing CSV;
- `--extra-observations` refuses a path that does not exist, rather than silently publishing a
  commune export missing that source.

`tests/test_age_sex_history.py`:

- a two-year fixture with a predecessor/successor pair proves growth-on-current-territory: the
  pre-merger year sums both, hand-computed; the post-merger year is the successor alone;
- a year where a predecessor's row is simply missing (not merged, just absent) is published with
  `coverage.found < coverage.expected`, never silently treated as complete;
- a `.zip` and a bare `.txt` source are read identically;
- an unrecognised column layout raises (`MissingPopulationData`, from the shared reader), never
  guesses;
- two files claiming the same year raise, rather than one silently overwriting the other;
- a crosswalk row resolving to a NIS code absent from `geographies.json` raises;
- the successor-walk helper handles a two-hop chain and refuses a cycle;
- repeated export of the same inputs is byte-identical;
- the existing single-year `public/data/demography/*.json` and its exporter are unmodified by this
  change.

## Assumptions and open questions

- `fetch_run_id` is not carried in the CSV. A run id has no meaning outside the database that
  produced it, so the loader opens one run row marked as a rebuild. The consequence is that
  per-row provenance back to an original fetch run does not survive the round trip — acceptable
  because for a manual source the provenance is the committed file itself.
- The CSV is uncompressed. Gzip would take it from ~3.6 MB to ~0.2 MB, but a compressed file cannot
  be diffed in review, which was the main reason for choosing text over a second `.db`.

## Rollout / risks

- If a future manual source needs different reference metadata, `load_observations_csv.py`'s
  `_ensure_reference_rows` reads it from the indicator's own config; it raises rather than inventing
  metadata for an indicator with no config file.
- The two blocked fiscal-income datasets fit this path unchanged when their files are supplied.
