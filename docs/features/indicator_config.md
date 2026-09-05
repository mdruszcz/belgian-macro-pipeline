# Feature: Indicator and source configuration

Status: draft
Issue: Block B (docs/steps)
Branch: feat/block-b-config-metadata

## Problem

`dashboard.html` has a hand-written JS object `M` (19 entries) plus a hardcoded `SECTIONS`
object, holding every indicator's display name, tooltip title/description, section, sort order,
SDMX code, and forecast linkage, in three languages. `belgian_macro_db.py` separately hardcodes
a 26-entry `SOURCES` dict with fetch URLs, frequency, unit, and source agency. The same
indicator identity is split across two files in two languages of code, and adding one indicator
means touching both — this is what makes indicator #742 a five-file edit.

## Goal

One `config/indicators/{code}.yaml` file per indicator and one `config/sources/{id}.yaml` file
per data source, both validated by a JSON Schema wired into CI, exported to a single JSON file
the frontend fetches. Adding an indicator that already has a source configured requires writing
one YAML file and nothing else — no Python change, no HTML change.

## Non-goals

- `INT_GDP_COMP`, `EC_CONS_CONF`, `CONTRIB_SERIES`, `INTL_SERIES` — bespoke `dashboard.html` JS
  that synthesizes composite series or builds specific charts from hardcoded indicator-code
  arrays — are **not** made config-driven here. Left exactly as they are; a stated, documented
  gap in CONTROL B's "expect zero hardcoded indicator names" check.
- Municipal/regional geography is not implemented anywhere yet. `geo_levels` exists as a
  forward-looking field; every indicator today gets `["national"]`.
- Real licence verification for each source (`licence` field) — these sources are already in
  production use, not new additions; verifying and recording real licence terms is separate,
  already-flagged follow-up work (`docs/data_catalog.md`), not blocking this block.

## Proposed approach

### Field reconciliation

The roadmap's own field list (`id, name.{en,fr,nl}, category, unit, frequency, source, geo_levels,
display block`) and the current `M` object's fields don't use the same names. Every rename:

| Roadmap / discussion wording | Current `M` field | Final schema field |
|---|---|---|
| `id` | (object key only) | `id` (now an explicit field too, so a YAML file is self-describing alone) |
| `category` | `s` | `display.category` |
| `name.{en,fr,nl}` | `name.{en,fr,nl}` | `name.{en,fr,nl}` (top-level, unchanged) |
| — | `full.{en,fr,nl}` | `display.title.{en,fr,nl}` (renamed `full`→`title`) |
| — | `desc.{en,fr,nl}` | `display.description.{en,fr,nl}` |
| — | `o` | `display.sort_order` |
| — | `sdmx` | `display.sdmx_code` |
| `frequency` | `freq` (`'Q'\|'A'\|'M'\|'F'`) | `frequency` (same 4-way enum) |
| `unit` | (only in `SOURCES`) | `unit` |
| `source` | (only in `SOURCES`, as `source_agency`) | `source_id` (foreign-key-by-convention into `config/sources/*.yaml`) |
| `geo_levels` | (didn't exist) | `geo_levels` (new, forward-looking) |
| — | `combined:true` | `display.combined` |
| — | `forecast:true` + `fc_indicator` | `display.forecast_of` (single field replaces both booleans) |
| — | (absent for 12 codes) | `display: null` — explicit "fetched/stored, no dashboard row" |
| — | (only in `SOURCES`, as an f-string URL) | `fetch.query` — see below |

**New field not in any prior list: `fetch.query`.** `SOURCES`'s URLs are built from an f-string
(`f"{NBB_BASE}/Q.1.B1GM.VZ.LY.N?startPeriod=..."`), which a YAML file cannot express. Each
indicator config instead stores the part of the URL after its source's `base_url`, and the
loader concatenates `source.base_url + "/" + indicator.fetch.query` at load time. `fetch` is
omitted only for indicators with no live fetch (there are none today — every `SOURCES` entry has
a URL — but the field is optional in the schema for forward-compatibility with a hypothetical
manually-entered indicator).

### `config/sources/*.yaml` mirrors the canonical DB schema

`migrations/001_core_schema.sql`'s `sources` table already defines this shape
(`source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active`) —
`config/sources/*.yaml` reuses it exactly rather than inventing a second one. Four sources:

- `nbb` — National Bank of Belgium SDMX API.
- `dbnomics_eurostat` — collapses the two cosmetically-distinct `SOURCES` labels
  `"Eurostat"` and `"Eurostat/DBnomics"` into one: both are fetched by the identical
  `DBnomicsFetcher` against the identical `api.db.nomics.world` host; the label difference was
  never a real distinction, just an inconsistently-filled `description` field.
- `dbnomics_ameco` — AMECO series via DBnomics (same adapter, different upstream catalog).
- `fpb` — Federal Planning Bureau, forecasts. Not in `SOURCES` today (forecasts are fetched by
  `FPBFetcher` unconditionally, outside the `SOURCES` loop) — added here purely so the three
  forecast pseudo-indicator configs (`FC_GDP_VOL`, `FC_CPI`, `FC_FISCAL_BAL`) have a valid
  `source_id` to reference.

### Corrected during extraction

Two real bugs found while extracting `M`'s `freq` badges: `INDUSTRIAL_PROD` and `HICP` are both
fetched monthly (`SOURCES["...]["frequency"] == "M"`) but `M` displays `freq:'A'` for both — a
stale/wrong badge on the live dashboard today. The consolidated config uses the correct,
single-source-of-truth frequency (`M` for both), which changes what the dashboard displays for
these two rows. This is an intentional fix surfaced by unifying the two previously-duplicated
values, not a silent behavior change — noted here and in the PR.

### What stays hardcoded, on purpose

`INT_GDP_COMP` and `EC_CONS_CONF` are synthesized pseudo-rows with no real indicator behind
them (no `SOURCES` entry, no observations of their own) — they get no config file. The bespoke
JS in `fetchData()` that builds them, and `CONTRIB_SERIES`/`INTL_SERIES`'s hardcoded arrays,
are untouched.

## Data / schema changes

None to `migrations/*.sql` — this is entirely a new config layer plus a `belgian_macro_db.py`
loader change (`SOURCES` becomes a function call instead of a literal, same resulting shape).

## New data sources

No genuinely new source is added — `fpb` formalizes an existing, already-in-production fetch
(`FPBFetcher`) that was never given a `SOURCES`-equivalent entry. Per `CLAUDE.md` rule 8,
`docs/data_catalog.md` gains rows for all four sources; licence terms are marked TODO pending
separate verification, not blocking, since none of these are new fetches being introduced.

## Tests

- Schema validation: valid config passes; missing required field / wrong enum value / dangling
  `source_id` each rejected with a specific, field-naming error.
- `src/exporters/metadata.py`: a fixture indicator written to a temp config dir reaches the
  exported JSON — the roadmap's own named acceptance test, made concrete without a browser.
- `belgian_macro_db.py`'s loader reconstructs the exact effective dict `fetch_all()` already
  expects, for a real indicator.

## Assumptions and open questions

- `docs/steps`' literal export path (`public/data/metadata/indicators.json`) doesn't match this
  repo's real structure — there is no `public/` directory anywhere. Used `data/metadata/
  indicators.json` instead, following the existing `data/belgian_macro_export.csv` convention.
- `SECTIONS`' 8 categories are not moved to their own config directory — a whole directory for
  8 rows that rarely change is over-engineering; they're emitted as a small `categories` array
  in the same exported JSON, sourced from a Python constant in the exporter.

## Rollout / risks

- `daily_fetch.yml` installs from `requirements.txt`, not `pyproject.toml`'s dev extra — the new
  `pyyaml`/`jsonschema` dependency must land in both files, or the daily workflow breaks the
  moment `belgian_macro_db.py` tries to import `yaml` at module load time.
- The metadata fetch in `dashboard.html` is blocking: if it fails, the whole page shows the
  existing error state (no partial render), since section/title/sort-order all depend on it.
