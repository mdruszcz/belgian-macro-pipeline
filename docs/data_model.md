# Data Model (Target)

This is the target canonical data model per `CLAUDE.md`. It supersedes the current SQLite
schema in `belgian_macro_db.py` (`indicators`, `observations`, `fetch_log`, `forecasts` keyed
by `(indicator_code, period)` only, no geo dimension, no vintage). This document is the
reference to review and approve before any migration is written; it is not itself a migration.

Five tables: `indicators`, `geo`, `observations`, `forecasts`, `fetch_log`.

Types are given as ANSI-ish SQL; adapt to the target engine (current engine is SQLite,
which does not enforce `CHECK`/`ENUM` the same way other engines do — constraints below should
be enforced at the application/adapter layer at minimum, and by the engine where supported).

---

## `indicators`

One row per indicator definition. This becomes the database mirror of the
`config/indicators/*.yaml` source of truth — the YAML is authoritative; this table is populated
from it, not edited directly.

| Column               | Type          | Constraints                                            |
|----------------------|---------------|---------------------------------------------------------|
| `indicator_id`        | TEXT          | PRIMARY KEY. Stable machine code, e.g. `GDP_QUARTERLY_YY`. |
| `name_en`             | TEXT          | NOT NULL |
| `name_fr`             | TEXT          | NOT NULL |
| `name_nl`             | TEXT          | NOT NULL |
| `description_en`      | TEXT          | NULL |
| `description_fr`      | TEXT          | NULL |
| `description_nl`      | TEXT          | NULL |
| `frequency`           | TEXT          | NOT NULL. CHECK IN (`'A'`, `'Q'`, `'M'`, `'D'`) — Annual/Quarterly/Monthly/Daily. |
| `unit`                | TEXT          | NOT NULL, e.g. `percent_yy`, `index_2010`, `balance`. |
| `preferred_direction` | TEXT          | NOT NULL. CHECK IN (`'lower_is_better'`, `'higher_is_better'`, `'neutral'`, `'contextual'`). |
| `source_agency`       | TEXT          | NOT NULL, e.g. `NBB`, `Eurostat/DBnomics`, `FPB`. |
| `aggregation_method`  | TEXT          | NOT NULL. CHECK IN (`'population_weighted'`, `'sum'`, `'unweighted_mean'`, `'not_applicable'`). Default `'population_weighted'` per national-aggregate rule; any exception must be set explicitly and is expected to be documented in the source YAML. |
| `api_url`             | TEXT          | NULL. Source endpoint template, informational only. |
| `config_path`         | TEXT          | NOT NULL. Path to the owning file under `config/indicators/`, for traceability back to the approved catalogue entry. |
| `is_active`           | BOOLEAN       | NOT NULL, DEFAULT TRUE. Soft-disable without deleting history. |

---

## `geo`

Canonical geography reference. Every observation resolves through this table via
`resolve_geo(nis, period)` — raw NIS codes from a source file are never trusted directly and
never stored on `observations` without having passed through this resolution.

| Column        | Type    | Constraints                                                       |
|---------------|---------|--------------------------------------------------------------------|
| `geo_id`      | TEXT    | PRIMARY KEY. Canonical internal geo identifier (stable across NIS renumbering/merges). |
| `nis_code`    | TEXT    | NOT NULL. Official NIS code valid at `valid_from`. |
| `level`       | TEXT    | NOT NULL. CHECK IN (`'municipality'`, `'province'`, `'region'`, `'country'`, `'eu'`). |
| `name_en`     | TEXT    | NOT NULL |
| `name_fr`     | TEXT    | NOT NULL |
| `name_nl`     | TEXT    | NOT NULL |
| `parent_geo_id` | TEXT  | NULL. FOREIGN KEY → `geo(geo_id)`. NULL only for the top-level entry (e.g. country/EU aggregate). |
| `population`  | INTEGER | NULL. Latest known population, used by `population_weighted` aggregation. |
| `valid_from`  | TEXT    | NOT NULL. Period from which this NIS code / boundary is valid. |
| `valid_to`    | TEXT    | NULL. Period this row stops being valid (NIS merges/renumbering); NULL = currently valid. |

`UNIQUE (nis_code, valid_from)` — a given NIS code may be reused across time (merges/splits),
so uniqueness is scoped to the validity window, not the code alone.

---

## `observations`

The canonical fact table. Composite primary key matches `CLAUDE.md` exactly:
`(indicator_id, geo_id, period, vintage)`.

| Column          | Type    | Constraints                                                              |
|-----------------|---------|----------------------------------------------------------------------------|
| `indicator_id`  | TEXT    | NOT NULL. FOREIGN KEY → `indicators(indicator_id)`. Part of PK. |
| `geo_id`        | TEXT    | NOT NULL. FOREIGN KEY → `geo(geo_id)`. Part of PK. |
| `period`        | TEXT    | NOT NULL. Format matches indicator frequency: `YYYY`, `YYYY-Qn`, `YYYY-MM`, or `YYYY-MM-DD`. Part of PK. |
| `vintage`       | TEXT    | NOT NULL. ISO timestamp (or revision label) identifying which release/revision this row represents. Part of PK. A later vintage never overwrites an earlier one — revisions are new rows. |
| `value`         | REAL    | NULL. NULL only permitted when `status = 'na'` or `'suppressed'`. |
| `status`        | TEXT    | NOT NULL. CHECK IN (`'final'`, `'provisional'`, `'estimate'`, `'revised'`, `'suppressed'`, `'na'`). |
| `is_latest`     | BOOLEAN | NOT NULL, DEFAULT TRUE. Exactly one row per `(indicator_id, geo_id, period)` has `is_latest = TRUE`; maintained by the adapter layer on insert of a new vintage, not by overwriting the prior row. |
| `source_ref`    | TEXT    | NULL. Raw identifier/URL for the specific source payload this value came from, for audit. |
| `fetched_at`    | TEXT    | NOT NULL. ISO timestamp of ingestion. |

Additional constraint: `CHECK (value IS NOT NULL OR status IN ('na', 'suppressed'))`.

---

## `forecasts`

Multi-institution forward-looking projections, kept separate from `observations` because a
forecast is not an observed fact and must never be written into `observations` as if it were
source data (per `CLAUDE.md` rule 6).

| Column          | Type  | Constraints                                                        |
|-----------------|-------|----------------------------------------------------------------------|
| `institution`   | TEXT  | NOT NULL. Forecasting body, e.g. `FPB`, `NBB`, `EC`. Part of PK. |
| `indicator_id`  | TEXT  | NOT NULL. FOREIGN KEY → `indicators(indicator_id)`. Part of PK. |
| `geo_id`        | TEXT  | NOT NULL. FOREIGN KEY → `geo(geo_id)`. Part of PK. Defaults to the country-level `geo_id` for national forecasts (current FPB forecasts have no sub-national breakdown). |
| `target_period` | TEXT  | NOT NULL. The period being forecast, same format rules as `observations.period`. Part of PK. |
| `vintage`       | TEXT  | NOT NULL. When this forecast figure was published/updated. Part of PK. |
| `value`         | REAL  | NULL. NULL permitted (source may publish a placeholder/no-data cell). |
| `published_at`  | TEXT  | NULL. Institution's own stated publication/update date, if provided by the source (distinct from `fetched_at`). |
| `fetched_at`    | TEXT  | NOT NULL. ISO timestamp of ingestion. |

---

## `fetch_log`

Ingestion audit trail. One row per adapter run attempt, per indicator/source.

| Column          | Type    | Constraints                                                     |
|-----------------|---------|--------------------------------------------------------------------|
| `id`            | INTEGER | PRIMARY KEY (auto-increment). |
| `source_id`     | TEXT    | NOT NULL. Either an `indicators.indicator_id` or another adapter-defined source key (e.g. `FPB_FORECASTS`). |
| `adapter`       | TEXT    | NOT NULL. Name of the `DataSource`-implementing adapter that ran. |
| `fetched_at`    | TEXT    | NOT NULL. ISO timestamp. |
| `rows_upserted` | INTEGER | NOT NULL. |
| `status`        | TEXT    | NOT NULL. CHECK IN (`'OK'`, `'ERROR'`, `'SCHEMA_CHANGED'`). `'SCHEMA_CHANGED'` is a distinct status from `'ERROR'` so a hard schema-drift failure (rule 13: never silently coerce or drop rows) is queryable separately from a transient fetch failure. |
| `message`       | TEXT    | NULL. Error detail or schema-diff detail when status is not `'OK'`. |

---

## Notes on deltas from the current schema

- Current `observations` PK is `(indicator_code, period)`; target is
  `(indicator_id, geo_id, period, vintage)`. This is an additive dimension change (geo, vintage),
  not a rename — a migration must decide what `geo_id` to backfill for existing rows (today's
  data is exclusively national/EU-aggregate) and what `vintage` to assign to existing values
  (they currently have no revision history to preserve, only a last-write value).
- Current schema has no `geo` table at all; it must be populated and versioned before
  `observations.geo_id` can be a real foreign key rather than a single hardcoded constant.
- Current `indicators` has one `name`/`description` (English); target requires `_en/_fr/_nl`
  columns, which requires a translation source that does not exist yet in this repo.
- Current `fetch_log` has no `adapter` column and no `SCHEMA_CHANGED` status; today's fetchers
  catch broad exceptions and log `'ERROR'` for both transient failures and structural source
  changes indistinguishably.
