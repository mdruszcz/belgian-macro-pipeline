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

## The two stores

| Store | Holds | Committed | Written by |
|---|---|---|---|
| `data/belgian_macro.db` | anything CI can fetch: national macro, `LOCAL_UNITS_BY_COMMUNE` | daily, by the bot | `daily_fetch.yml` |
| `data/population_observations.csv` | `POPULATION_BY_COMMUNE`, `POPULATION_AGE_0_14/_15_64/_65_PLUS` | only when refreshed by hand | the procedure below |
| `data/fiscal_income_observations.csv` | `FISCAL_TOT_NET_TAXABLE_INC`, `FISCAL_NBR_NON_ZERO_INC`, `FISCAL_TOT_TAXES`, `FISCAL_TOT_MUNICIP_TAXES` | only when refreshed by hand | [fiscal_income.md](fiscal_income.md) |
| `data/census2021_observations.csv` | 13 Census 2021 counts: citizenship, birthplace, sex, marital status, households, family nuclei, dwellings | only when refreshed by hand (decennial) | the procedure below |
| `data/var_unemployment_observations.csv` | `ADMIN_UNEMPLOYMENT_RATE_COM`, annual administrative unemployment rate for ages 15–64 | only when the Tableau crosstab is refreshed by hand | [var_unemployment.md](var_unemployment.md) |

`data/local/` holds the disposable rebuild of the manual store and is gitignored. It must stay
ignored: `daily_fetch.yml` runs `git add data/`, which would otherwise commit it.

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
```

Then commit `data/population_observations.csv`, `data/communes_export.csv` and
`data/communes_history.csv`, and open a PR. The diff on each CSV is readable line-by-line — that is
the point of storing them as text.

Steps 3 and 4 are also available as `.github/workflows/manual_sources.yml`
(`workflow_dispatch` only), which regenerates the export from the already-committed CSV and opens a
PR. It deliberately does **not** attempt step 1 or 2: it cannot reach Statbel and does not have the
raw files.

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
