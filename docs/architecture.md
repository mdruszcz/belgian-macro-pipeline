# Architecture

## As-is

### Repo layout

```
.
├── .github/workflows/daily_fetch.yml   GitHub Actions schedule (daily, 05:00 UTC)
├── belgian_macro_db.py                 Fetch + store + export CLI (single file)
├── fetch_stocks.py                     Separate script: market data → data/stocks.json
├── dashboard.html                      Static dashboard, reads CSV/JSON via raw GitHub URL
├── index.html, about.html              Static pages
├── shared-styles.css
├── data/
│   ├── belgian_macro.db                SQLite database (committed to git)
│   ├── belgian_macro_export.csv
│   ├── belgian_macro_export.json
│   ├── belgian_forecasts.csv
│   └── stocks.json
├── requirements.txt                    pandas, requests, openpyxl
├── pyproject.toml                      ruff/black/pytest config
├── .pre-commit-config.yaml             ruff, black, trailing-whitespace, large-file guard
├── tests/                              empty (no tests exist yet)
└── README.md
```

There is no `src/` package, no `config/` directory, and no `docs/data_catalog.md`. All indicator metadata lives inline in Python.

### Execution model

- `belgian_macro_db.py` and `fetch_stocks.py` are independent, standalone scripts. They do not
  share code, a common CLI, or a common data-access layer.
- Both are invoked directly by `.github/workflows/daily_fetch.yml`:
  1. `python belgian_macro_db.py --fetch --latest --export csv`
  2. `python fetch_stocks.py`
  3. `git add data/ && git commit && git push` (only if `data/` changed)
- The workflow also runs locally via `python belgian_macro_db.py` (defaults to `--fetch --latest`
  when no flags are given).
- No queue, scheduler, or server process. GitHub Actions cron is the only orchestrator.

### `belgian_macro_db.py` internals

- `SOURCES`: a module-level `dict` hardcoding ~21 indicators. Each entry carries `name`, `url`,
  `frequency`, `unit`, `source_agency`, `description` (optional), and `type` (`"nbb"` or
  `"dbnomics"`). Adding an indicator means editing this dict directly.
- `MacroDatabase`: wraps a single `sqlite3` connection (WAL mode). Owns schema creation
  (`_init_schema`, idempotent `CREATE TABLE IF NOT EXISTS`) and all reads/writes. No ORM,
  no migrations mechanism — schema changes are made by editing the `executescript` call.
- Fetchers, one class per source, each with a single `@staticmethod fetch(...)`:
  - `NBBFetcher`: GET SDMX CSV, dedupe by period, return sorted `list[dict]`.
  - `DBnomicsFetcher`: GET JSON, parse `series.docs[0]`, optionally rebase to `index_2010`.
  - `FPBFetcher`: GET an XLSX file, parse fixed cell offsets into forecast rows for 3 fixed
    indicator codes (`GDP_VOL`, `CPI`, `FISCAL_BAL`).
- `fetch_all(db)`: iterates `SOURCES`, calls the matching fetcher based on `type`, upserts
  observations, and logs each attempt (`OK`/`ERROR`) to `fetch_log` — errors in one indicator
  do not stop the others. Then fetches FPB forecasts once, separately.
- `export_data(db, fmt)`: dumps `observations` (joined with `indicators`) to CSV and, when
  non-empty, `forecasts` to a separate CSV. `fmt == "json"` is accepted by the CLI but not
  implemented in `export_data` — only `csv` currently writes a file.
- CLI (`argparse`): `--fetch`, `--latest`, `--dump`, `--export {csv,json}` (repeatable),
  `--history`, `--db <path>`. No subcommands.

### `fetch_stocks.py` internals

- Unrelated data domain (equities/FX/bond yields), not stored in SQLite — writes directly to
  `data/stocks.json`.
- Fetches Yahoo Finance chart API (via raw `urllib.request`, not `requests`) for `BEL20` and
  `EURUSD`, and DBnomics for BE/DE 10Y yields and the ECB main refinancing rate (via `requests`).
  Two different HTTP clients are used in the same file.
- Computes a BE–DE yield spread client-side from the two most recent fetched values; no history
  is retained, so "change" fields are either derived from the last two observations in the
  current response or hardcoded to `0` (spread) where no prior value is available.
- No shared error/logging convention with `belgian_macro_db.py`: uses bare `print()` instead of
  the `logging` module.

### Storage

- One SQLite file, `data/belgian_macro.db`, checked into git and overwritten by every daily run.
- No geographic dimension: observations are keyed by `(indicator_code, period)` only — there is
  no per-municipality or per-region breakdown anywhere in the current schema or fetchers.
- No `vintage`/revision tracking: an upsert on `(indicator_code, period)` overwrites the prior
  value in place; the previous value is not retained.
- No language/locale fields: all `name`/`description` strings are single-language (English).

### Frontend

- `dashboard.html`, `index.html`, `about.html` are static HTML files with inline/linked JS.
  They fetch the exported CSV/JSON directly from the raw GitHub URL of this repo (per README) —
  there is no API server.
- Per `README.md`, indicator display order/grouping is controlled by an `INDICATOR_META` object
  inside `dashboard.html` itself, which must be updated by hand to match `SOURCES`.

### Testing / quality tooling

- `pyproject.toml` defines ruff, black, and pytest configuration.
- `.pre-commit-config.yaml` runs ruff (`--fix`), black, `trailing-whitespace`, and
  `check-added-large-files` (`--maxkb=25000`) on commit.
- `tests/` exists but is empty (no test files, only `.gitkeep`) — there is currently no
  automated test coverage for either script.

## Target

The target architecture is the one described in `CLAUDE.md` ("BelPulse"). As-is/target
deltas below are stated factually, without recommending an order or method to close them.

- **Product framing**: a Belgian economic-intelligence platform that ingests official
  statistics, normalizes them into a canonical model, computes deterministic analytics, and
  publishes **municipality-level** profiles and reports — not only national/EU aggregate time
  series as today.
- **Canonical geo resolution**: every observation must carry a geo ID resolved via
  `resolve_geo(nis, period)`, using canonical NIS-based codes. Raw NIS codes from source files
  must never be trusted directly. The current schema has no geo dimension at all.
- **Indicator metadata source of truth**: all indicator-specific metadata (name, unit, source,
  description, etc.) must live in `config/indicators/*.yaml`, not in Python (current: the
  `SOURCES` dict in `belgian_macro_db.py`) and not in dashboard HTML/JS (current: README
  documents `INDICATOR_META` living inside `dashboard.html`).
- **Data source catalogue**: every data source requires a row in `docs/data_catalog.md`,
  approved by the maintainer, before it is added. No such file exists yet.
- **Adapter interface**: every ingestion adapter must implement a shared `DataSource` interface
  and pass a shared contract test. Currently there is no shared interface — `NBBFetcher`,
  `DBnomicsFetcher`, and `FPBFetcher` each expose an ad hoc `fetch(...)` static method, and
  `fetch_stocks.py` is a wholly separate, uncoordinated script.
- **Revisions / vintage tracking**: observations must be addressable by
  `(indicator_id, geo_id, period, vintage)` and carry a `status` of
  `final | provisional | estimate | revised | suppressed | na`. The current schema's primary
  key is `(indicator_code, period)` with no vintage or status enum — upserts destroy history.
- **Deterministic computation boundary**: derived statistics must be computed in Python, never
  by an LLM, and must never be written back into `observations` as if they were source data.
  Every derived statistic requires unit tests with hand-computed expected values. The current
  `fetch_stocks.py` computes a derived spread inline with no tests; `tests/` is empty.
- **Multilingual labels**: every user-facing string must preserve en/fr/nl labels. The current
  schema and HTML pages carry single-language (English) text only.
- **Aggregation rule**: national aggregates are population-weighted by default, with exceptions
  documented per indicator config. No aggregation logic exists yet since there is no geo
  dimension to aggregate over.
- **Schema-change handling**: adapters must fail loudly on a changed source schema, never
  silently coerce or drop rows. Current fetchers catch broad `Exception` per indicator, log an
  `ERROR` row, and continue — a source schema change (e.g. a renamed CSV column) would currently
  surface as a caught exception rather than a hard failure.
- **Test gates**: derived-value unit tests, a shared adapter contract test, and
  `tests/golden/` checks on any generated statistical text are required before completion is
  claimed. None of these exist in the current `tests/` directory.
- **Process**: work happens on `feat/<issue-number>-<slug>` branches against a linked spec in
  `docs/features/`, with the maintainer merging PRs. `docs/features/` does not exist yet in
  this repo.
