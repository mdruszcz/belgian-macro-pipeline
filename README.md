# 🇧🇪 BelPulse — Belgian economic intelligence pipeline

Fetches official Belgian statistics, normalizes them into one canonical model
(`indicator × geography × period × vintage`), computes deterministic analytics, validates the
result, and publishes both bulk downloads and per-commune payloads — daily, on GitHub Actions, for
free.

```
NBB · Eurostat/DBnomics · FPB · Statbel
        │
        ├─ adapters ──→ canonical SQLite schema ──→ validation (blocking) ──→ exports
        │                (+ committed CSV stores      ↑                        ├─ bulk CSV/JSON
        │                 for manual-only sources)    │                        └─ per-entity JSON
        │                                       derived engine                    payloads
        └─ all committed to this repo daily        (never stored)
```

Data is national **and municipal**: 565 communes, resolved through a period-aware NIS crosswalk that
survives the 2019 and 2025 merger waves.

## Live pages

| Page | What it is |
|---|---|
| [Dashboard](https://mdruszcz.github.io/belgian-macro-pipeline/dashboard.html) | Presented national view — sparklines, trend arrows, drill-down modals |
| [All observations](https://mdruszcz.github.io/belgian-macro-pipeline/all_data.html) | Every national observation, raw table |
| [Commune data](https://mdruszcz.github.io/belgian-macro-pipeline/communes.html) | Every municipal observation, every year, one row per commune |
| [/local](https://mdruszcz.github.io/belgian-macro-pipeline/local.html) | Single-commune profile: search, headline figures, eight fixed sections |
| [Index](https://mdruszcz.github.io/belgian-macro-pipeline/) · [About](https://mdruszcz.github.io/belgian-macro-pipeline/about.html) | Landing and about pages |

Every page is static and reads the committed data files directly — no server, no build step.

> **Note:** `/local` reads `public/data/`, which the daily workflow generates and commits. Until the
> first scheduled run after that wiring landed, the page will report that it cannot load the commune
> index. Run the exporter locally (below) to see it working now.

## What's in the data

Measured on the committed stores, not estimated:

| | Indicators | Geographies | Rows | Coverage |
|---|---|---|---|---|
| **National** | 17 published | `be:country` | 1,374 | 2000-Q1 → 2027 |
| **Municipal** | 14 (9 raw + 5 derived) | 565 communes | 93,061 | 2005 → 2026 |
| **Geography master** | — | 688 rows (622 currently valid: 1 country, 3 regions, 10 provinces, 43 arrondissements, 565 communes) | — | validity windows from 1830 |

Municipal indicators today: population (total and three age bands), local business units, four fiscal
income/tax totals, plus five derived figures — average net taxable income, dependency ratio, 5-year
population change, 10-year CAGR and population percentile.

**Known coverage gaps**, stated rather than hidden: 13 indicator configs carry no observations in the
canonical store. Ten are non-Belgian comparison series lost to repeated DBnomics read timeouts (see
[validation.md](docs/features/validation.md)); three are FPB forecast series whose values live in the
legacy `belgian_forecasts.csv` rather than the canonical schema, out of scope per
[ADR 0001](docs/decisions/0001-data-model.md). Municipal unemployment and housing data do not exist
in this pipeline at all — no dataset was selected for them, and `/local` says so on the page instead
of leaving a blank.

## How the daily pipeline works

`.github/workflows/daily_fetch.yml`, 06:00 CET:

1. **Migrate** — apply pending numbered SQL migrations (idempotent, tracked in `schema_migrations`)
2. **Fetch** — NBB SDMX, Eurostat via DBnomics, FPB XLSX; raw responses cached under `data/raw/`
3. **Sync** — normalize into the canonical schema; new values insert a new `vintage` rather than
   overwriting, so revisions are preserved
4. **Load geography** — offline, from committed `config/geography/*.csv`
5. **Validate** — range, referential, structural and volume rules. A `fail` **stops the run before
   anything reaches the exports or the commit**; warnings print and do not block
6. **Report revisions** — informational, never blocking
7. **Export** — bulk CSV/JSON, then per-entity JSON payloads under `public/data/`
8. **Commit** — opens a PR with the updated files; CI must pass before auto-merge

`.github/workflows/manual_sources.yml` (manual trigger) regenerates the same exports from the
hand-loaded stores, so export generation is identical whichever path produced the data.

## The two committed stores

Manual-only sources live in CSV, not in the daily-committed database — see
[ADR 0002](docs/decisions/0002-split-committed-stores.md) for the measurements behind that split.

| Store | Holds | Re-committed |
|---|---|---|
| [`data/belgian_macro.db`](data/belgian_macro.db) | everything CI can fetch: national macro, `LOCAL_UNITS_BY_COMMUNE`, geography, fetch/volume history | daily, by the bot |
| [`data/population_observations.csv`](data/population_observations.csv) | population by commune + three age bands | only when refreshed by hand |
| [`data/fiscal_income_observations.csv`](data/fiscal_income_observations.csv) | four fiscal income/tax totals by commune | only when refreshed by hand |

`statbel.fgov.be` is unreachable from CI (connection-level block, confirmed three ways), so those
files are downloaded by hand. The procedure is in
[manual_sources.md](docs/features/manual_sources.md); the split is invisible downstream, because
every exporter reads both stores.

## Published files

| File | Description |
|---|---|
| [`data/belgian_macro_export.csv`](data/belgian_macro_export.csv) · [`.json`](data/belgian_macro_export.json) | national time series, full history |
| [`data/communes_export.csv`](data/communes_export.csv) | municipal snapshot — latest period per (commune, indicator) |
| [`data/communes_history.csv`](data/communes_history.csv) | municipal full history, including derived indicators |
| [`data/belgian_forecasts.csv`](data/belgian_forecasts.csv) | multi-institution economic forecasts (legacy table) |
| [`data/metadata/indicators.json`](data/metadata/indicators.json) | indicator display metadata, generated from config |
| `public/data/**` | per-entity payloads: `national.json`, `communes/{nis}.json`, `indicators/{id}.json`, `metadata/geographies.json`, `manifest.json` |

Bulk downloads are kept deliberately — they are the credibility feature for researchers and
journalists, and they cost nothing. The largest per-commune payload (Antwerp, 14 indicators, full
history) measures 9.5 KB compact / 2.3 KB gzip.

## Repo layout

```
├── .github/workflows/     daily_fetch.yml · manual_sources.yml · ci.yml
├── config/
│   ├── indicators/        one YAML per indicator (+ derived/ for computed ones)
│   ├── sources/           one YAML per publisher
│   └── geography/         geographies, merger crosswalk, merger dates, EN exonyms
├── migrations/            001_core_schema · 002_indexes · 003_volume_history
├── src/
│   ├── fetchers/          base interface + nbb · eurostat · fpb · statbel adapters
│   ├── geography/         refnis loader · crosswalk · resolve_geo(nis, period)
│   ├── analytics/         derived.py (pure functions) · engine.py (topological sort)
│   ├── validation/        config_schema.py · rules.py
│   ├── db/                migrate · vintages (one shared writer) · observations (as-of queries)
│   └── exporters/         metadata.py
├── scripts/               sync_* · export_* · load_* · validate_* CLIs
├── data/                  the committed stores and published exports
├── docs/                  specs, decisions, roadmap, the plan of record (docs/steps)
├── tests/                 340 tests
├── *.html                 the static pages listed above
├── belgian_macro_db.py    legacy single-file ETL (still the national fetch entry point)
└── fetch_stocks.py        market data → data/stocks.json
```

## Local usage

```bash
pip install -r requirements.txt          # or: pip install -e ".[dev]" for tooling too

# National fetch/export (legacy CLI, still what the daily workflow calls)
python belgian_macro_db.py --fetch --latest
python belgian_macro_db.py --export csv       # or json
python belgian_macro_db.py --history          # fetch log

# Canonical pipeline
python -m src.db.migrate --db data/belgian_macro.db
python scripts/sync_to_canonical.py --db data/belgian_macro.db
python scripts/load_geography.py  --db data/belgian_macro.db
python scripts/validate_data.py   --db data/belgian_macro.db     # non-zero exit = blocked
python scripts/revisions_report.py --db data/belgian_macro.db

# Exports
python scripts/export_canonical_csv.py --db data/belgian_macro.db --out data/belgian_macro_export.csv
python scripts/export_communes_history_csv.py --db data/belgian_macro.db \
  --out data/communes_history.csv \
  --extra-observations data/population_observations.csv \
  --extra-observations data/fiscal_income_observations.csv
python scripts/export_site_payloads.py --db data/belgian_macro.db --out-dir public/data

# Then serve the pages locally
python -m http.server 8000    # → http://localhost:8000/local.html
```

`belgian_macro_db.py` exits non-zero if any source failed to fetch — check `--history` for details.

## Adding an indicator

Indicator and source metadata live in config, never in Python or HTML. Create
`config/indicators/YOUR_INDICATOR.yaml`:

```yaml
id: YOUR_INDICATOR
name: {en: Display Name, fr: Nom affiché, nl: Weergavenaam}
unit: percent_yy
frequency: A          # A=Annual, Q=Quarterly, M=Monthly, F=Forecast
source_id: nbb        # must match a source_id in config/sources/*.yaml
geo_levels: [national]
preferred_direction: higher_is_better   # lower_is_better | higher_is_better | neutral | contextual
fetch:
  query: A.2.INDICATOR_CODE.VZ.LY.N?startPeriod=2000&dimensionAtObservation=AllDimensions
display:
  category: gdp       # one of the categories in src/exporters/metadata.py
  title: {en: Full title, fr: Titre complet, nl: Volledige titel}
  sort_order: 20
```

Then `python scripts/validate_config.py`. Notes:

- `display: null` stores and fetches the indicator without giving it its own dashboard row.
- For a non-Belgian comparison series add `country: DE` (`FR`/`NL`/`ES`/`EA`/…). That field, not
  `display`, decides whether the series reaches the canonical schema.
- A **derived** indicator goes in `config/indicators/derived/` under its own schema, declares a
  function from `src/analytics/derived.py` and its inputs, and is computed on export — never stored.
- No new *source* enters the pipeline without an approved row in
  [docs/data_catalog.md](docs/data_catalog.md) (CLAUDE.md rule 8).

Full field reference: [indicator_config.md](docs/features/indicator_config.md).

## Development

```bash
pip install -e ".[dev]"
pre-commit install        # ruff · black · trailing whitespace · large-file guard

pytest -q                 # 340 tests
ruff check .
black --check .
python scripts/validate_config.py     # config schema + cross-file references
python scripts/validate_data.py       # the committed stores
```

CI (`ci.yml`) runs all of the above on every PR and on pushes to `develop`/`main`. Never commit
directly to `main` or `develop` — see [claude.md](claude.md) for the full operating rules.

## Project status

The plan of record is [`docs/steps`](docs/steps) — every step carries its own evidence note, and a
step is only ✅ when its stated method was actually carried out.

- **Done:** canonical data model, config-driven metadata, Belgian geography master with merger
  crosswalk, generic source adapters, data catalogue, Statbel adapter, derived-indicator engine,
  blocking validation layer, vintages with as-of queries, per-entity payload exports, and the
  `/local` interface (Blocks A–K).
- **Next:** Block L — commune/province/region/Belgium comparison, percentile components, and
  statically generated permanent URLs (`/local/{nis}`) with SEO metadata.
- **Not yet passed:** the 50% gate. Outstanding items are mostly maintainer checks (`[H]` steps) —
  spreadsheet re-computation of derived values, spot-checks against Statbel's own website, a
  real-device pass over `/local`, and commercial-reuse verification for 3 of the 10 selected
  datasets.

Phase II and III (peer model, municipal finance, scoring, signals, forecasting, scenarios, reports)
are specified in [docs/roadmap.md](docs/roadmap.md) and not started.

## Documentation

| Doc | Covers |
|---|---|
| [docs/steps](docs/steps) | the plan of record, with per-step evidence |
| [roadmap.md](docs/roadmap.md) | the three milestones, defined by what must be true |
| [architecture.md](docs/architecture.md) | as-is and target architecture |
| [data_model.md](docs/features/data_model.md) | tables, keys, period formats, status enum |
| [geography.md](docs/features/geography.md) | NIS semantics, mergers, `resolve_geo` |
| [source_adapter.md](docs/features/source_adapter.md) | the fetch → parse → normalize → validate → load contract |
| [derived_indicators.md](docs/features/derived_indicators.md) | function catalogue, null policy, percentile definition |
| [validation.md](docs/features/validation.md) | rule catalogue, severities, thresholds |
| [vintages.md](docs/features/vintages.md) | what creates a vintage; as-of queries |
| [site_payloads.md](docs/features/site_payloads.md) · [local_ui.md](docs/features/local_ui.md) | payload layout and the `/local` page |
| [data_catalog.md](docs/data_catalog.md) | every dataset, its licence and its reuse terms |
| [methodology.md](docs/methodology.md) · [styleguide.md](docs/styleguide.md) | published methodology, writing rules |

## Data sources, licence and attribution

- [NBB SDMX Dissemination API](https://nsidisseminate-stat.nbb.be/) — dataflow `DF_QNA_DISS`
  (Quarterly National Accounts)
- [DBnomics](https://db.nomics.world/) — Eurostat series
- [Federal Planning Bureau](https://www.plan.be/) — economic forecasts
- [Statbel](https://statbel.fgov.be/) — geography (REFNIS), population, fiscal income, business units

Statbel data carries binding conditions. Statbel publishes two licence documents that differ, and
this project satisfies the union of both until Statbel confirms which governs — full analysis in
[data_catalog.md](docs/data_catalog.md). Published output must credit the source, link the licence,
**state that changes were made**, carry the date of last update, and imply no endorsement:

> Source: Statbel (Direction générale Statistique — Statistics Belgium),
> [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Data has been modified: reshaped into a
> canonical schema, re-keyed to internal geography identifiers, with validity windows and a merger
> crosswalk derived. Statbel does not endorse this product or its use of the data.
> English geography names are unofficial translations, not Statbel labels.

⚠️ **These obligations are not yet implemented on the published pages.** They became live the moment
commune-level data was published, and landing them is the next item to fix — see the "Not yet done"
note in [data_catalog.md](docs/data_catalog.md).

This repository has no code licence file yet, so no reuse rights are granted for the code itself.
