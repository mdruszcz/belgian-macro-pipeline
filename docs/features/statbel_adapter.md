# Feature: Statbel adapter

Status: draft
Issue: (Block F — Statbel adapter, docs/steps)
Branch: feat/statbel-adapter-business-units

## Problem

Block E selected 10 municipal-level datasets; 4 are published by Statbel (fiscal income by
commune, fiscal income by statistical sector, population by commune, business/enterprise units
by commune). None has ever been fetched into this pipeline — every indicator in `observations`
today is national (`geo_id = 'be:country'`). This is the first source that must write real,
per-commune geography, and the first real test of whether `resolve_geo`/the geography table
built in Block C actually holds up against independent, real-world data rather than only its own
internal consistency checks.

## Goal (this pass)

- `StatbelSource`, on the Block D `DataSource` interface, fetches **business/enterprise units by
  commune** end to end: real HTTP fetch, real commune-name-to-`geo_id` resolution, real rows in
  the canonical `observations` table, with `fetch_runs` logging.
- Every commune name resolves to the correct, unique `geo_id` — including the one confirmed
  ambiguous case (below) — or the pipeline raises. It never silently drops or guesses a row.
- Fixture test proves the parsing and resolution logic without a network call.

## Non-goals (this pass) — and why

**Fiscal income by commune, fiscal income by statistical sector, and population by commune are
not built this pass.** Verified directly, not assumed:

- Every standard Bestat view for both fiscal-income datasources and the population datasource
  stops at province level (3 views total for fiscal income, checked exhaustively; 156 views for
  population, none named "commune"/"municipal").
- `statbel.fgov.be` — the open-data portal whose XLSX pages claim commune-level files for these
  three — refused every direct fetch attempt this session (timeouts), the same as during Block E.

Building an adapter for data whose actual column layout has never been seen would mean guessing
at exactly the thing this document's own `[SPEC]` step exists to prevent ("column-level mapping
written in advance is what stops an agent guessing which column is 'income'"). These three stay
blocked until either the site becomes reachable, or the maintainer supplies the file directly —
the same pattern already used for the geography files in Block C.

**Demography (beyond this one dataset), Housing, Mobility** — no dataset was selected for these
categories in Block E (real estate, cadastral stock and building permits were all `DEFERRED`).
Nothing to build against.

## Data source

**Business/enterprise units by commune** — Statbel Bestat API, datasource
`IM_EAF_LCL_UNIT_POP`, standard view `04f3d23b-9422-4d08-a856-ab547ccf7b38`
("Nombre d'unités établissements par commune").

```
GET https://bestat.statbel.fgov.be/bestat/api/views/04f3d23b-9422-4d08-a856-ab547ccf7b38/result/JSON
```

Response shape (verified by direct fetch, 566 rows):

```json
{"facts": [
  {"Région": "Région flamande", "Province": "Province d’Anvers",
   "Arrondissement": "Arrondissement d’Anvers", "Commune": "Aartselaar",
   "Trimestre": "4ème trimestre 2023", "Nombre d’établissements": 2232.0},
  ...
  {"Région": "Localisation indéterminée", "Province": null, "Arrondissement": null,
   "Commune": null, "Trimestre": "4ème trimestre 2023", "Nombre d’établissements": 9233.0}
]}
```

- **One row has `Commune: null`** — 9,233 establishments Statbel itself could not attribute to
  any commune ("Localisation indéterminée"). This is excluded from ingestion, not attributed to
  a geography that would misrepresent it. Counted in `rows_read`, not `rows_written` —
  `fetch_runs` shows the discrepancy rather than hiding it.
- **Only the latest quarter is available.** The datasource covers "since 2015 per quarter", but no
  standard view combines commune granularity with historical depth — confirmed in Block E's
  research (a region-level view has the full 2015–present series; the commune-level view has
  only the current quarter). Backfilling history needs a custom cross-tab built once through the
  Bestat UI, out of scope here.
- **Column names carry a Unicode right-quote** (`Région`, `d'Anvers` use `’` U+2019, not `'`) —
  must match exactly, not the ASCII apostrophe.

### Geography resolution — no NIS code in this data at all

Unlike every other source this pipeline ingests, this dataset identifies communes **only by
name** — `Commune`, `Arrondissement`, `Province`, `Région` as French text, no NIS code anywhere
in the response. `resolve_geo(nis, period)` (Block C) cannot be used directly; it takes a code,
not a name.

**A name-only match is unsafe on its own — proven, not assumed.** Cross-checking all 566 Bestat
commune names against this repo's own `config/geography/geographies.csv` (`name_fr`) found
`config/geography/geographies.csv` itself has two different current communes sharing the exact
name **"Saint-Nicolas"**: `be:mun:46021` (Sint-Niklaas, East Flanders) and `be:mun:62093`
(Saint-Nicolas, Liège province) — both derived faithfully from Statbel's own NIS9 reference file,
which gives "Saint-Nicolas" as the *official* French name for both. Belgium's own reference data
contains this ambiguity; it is not a transcription error in Block C.

Adding `Arrondissement` as a second key resolves 565 of 566 non-null rows uniquely with zero
collisions. The one exception is itself informative: **Bestat's live data calls the commune
`Sint-Niklaas`** (untranslated, correctly monolingual — Sint-Niklaas is a Flemish-only commune),
while `geographies.csv`'s `name_fr` (from the NIS9 file) says `Saint-Nicolas` for that same code.
Two Statbel systems — the static NIS9 reference file and the live Bestat API — disagree with each
other on this one commune's French name.

**Resolution strategy:**
1. Build `(commune_name_fr, arrondissement_name_fr) -> geo_id` from `geographies.csv` (current
   communes only).
2. One documented override: `("Sint-Niklaas", "Arrondissement de Saint-Nicolas") -> be:mun:46021`
   — the single case where Bestat's own naming diverges from the NIS9-derived table.
3. **Any commune name that resolves through neither the table nor the override raises.** A
   silent skip or a best-guess match is exactly the "plausible numbers attached to the wrong
   place" failure Block C's audit exists to prevent — this dataset is the first real test of
   that discipline against independent data, and it must fail loudly if Statbel ever adds a
   commune, renames one, or introduces a second ambiguity this override list doesn't cover.

### Contract — a third adapter shape

Block D defined `TimeSeriesSource` (one geography per fetch, `{period, value, obs_status}`) and
`ForecastSource` (`{institution, indicator, year, value, updated_at}`). This is neither: one
fetch returns **many geographies at once**. A third contract,
`MunicipalTimeSeriesSource(DataSource)`, returns
`list[{"geo_id": str, "period": str, "value": float, "status": str}]` — `status` uses the
canonical enum (`final`/`provisional`/…), not SDMX's `obs_status`, since this feeds
`observations` directly rather than the legacy tables `TimeSeriesSource` still targets.

## Loading

Writes directly to the canonical `observations` table — **not** through `legacy_observations` /
`sync_to_canonical.py`, which assume `geo_id = 'be:country'` throughout. A new script,
`scripts/sync_statbel.py`, reuses the same vintage/`is_latest` discipline
`sync_to_canonical.py` already established (full-timestamp vintage, insert-only-on-change,
hard `rowcount != 1` check — see `docs/features/data_model.md` §Vintage) but keyed per commune
`geo_id` instead of always the same national one.

## Tests

- Geo-name resolution: the Sint-Niklaas/Saint-Nicolas case resolves correctly via the override;
  an unknown name/arrondissement pair raises; a `Commune: null` row is excluded, not attributed.
- `StatbelSource._parse`: fixture JSON (a small trimmed sample, committed — Block F's own
  instruction), asserting the `MunicipalTimeSeriesSource` contract shape and the period-string
  parsing (`"4ème trimestre 2023"` → `"2023-Q4"`).
- `sync_statbel.py`: vintage/`is_latest` behaviour against a temp migrated DB — first sync
  inserts, unchanged re-sync creates no new vintage, changed value flips `is_latest` — mirroring
  `tests/test_sync_to_canonical.py`'s existing coverage of the same logic.

## Assumptions and open questions

- **`preferred_direction: higher_is_better`** for a raw commune-level business-unit count. This is
  a judgment call, not derived from anything Statbel states — more registered establishments
  reads as economically positive in the aggregate, but the indicator's own YAML description says
  so explicitly rather than leaving it implicit, since Block U's golden tests will later enforce
  directional phrasing off this field.
- **`status: 'final'`** for every row. Nothing in the datasource's own description discusses
  revisions to a past quarter's count; there is no `obs_status`-equivalent field in the Bestat
  response to read a real status from, unlike NBB/DBnomics.
- **Historical backfill remains blocked** on a manually-built Bestat custom view (Block E's
  finding). Not attempted here.

## Rollout / risks

- The Sint-Niklaas override is a single hardcoded entry, not a general mechanism. If Statbel adds
  a second such divergence later, this pipeline will raise (not silently mismap) — the fix is to
  extend the override list with the same evidence-first standard used here, not to make the
  resolver more permissive.
- This is the first indicator with `geo_levels: [municipal]` and no dashboard row (`display:
  null` — /local doesn't exist yet, Block K's job). Verify it does not appear anywhere the
  national dashboard iterates indicators expecting `geo_id = 'be:country'`.
