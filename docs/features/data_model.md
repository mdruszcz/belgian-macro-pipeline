# Feature: Canonical data model (five tables)

Status: approved — frozen by [docs/decisions/0001-data-model.md](../decisions/0001-data-model.md)
Issue: #1, #2 (observations PK / geo resolution), #20 (approval)
Branch: feat/1-canonical-data-model

## Problem

The current schema in `belgian_macro_db.py` (`indicators`, `observations`, `fetch_log`,
`forecasts`) cannot express what the platform needs at the 50% milestone:

- No geography dimension at all. Every observation is implicitly national, so no
  municipality-level data can be stored, and Belgian municipal mergers have nowhere to live.
- `observations` is keyed `(indicator_code, period)`, so a re-fetch overwrites the prior value
  in place. Revisions destroy history, and there is no way to answer "what did we believe on
  date X".
- No status vocabulary. A suppressed cell, a provisional first estimate, and a final figure are
  all stored identically as a bare float.
- No source table. Provenance exists only as a free-text `source_agency` string on the
  indicator, so two sources publishing the same measure cannot be distinguished, and there is
  no machine-readable link to the approved row in `docs/data_catalog.md`.
- No indication of which direction is "good" for an indicator, which any comparison or ranking
  output needs before it can phrase a conclusion.

## Goal

A five-table canonical model exists as reviewed text (this document), with every column, type,
primary key, foreign key and index specified, such that:

- `observations` is keyed `(indicator_id, geo_id, period, vintage)` and never overwrites a
  prior vintage.
- Period strings, their frequencies, and their sort order are unambiguously defined.
- `status` and `preferred_direction` are closed enumerations.
- Municipal mergers, revisions, and multi-source conflicts each have a stated handling — or a
  stated, deliberate limitation.

## Non-goals

- Writing the migration itself, or any DDL/Python. This document is the reviewable artefact
  that must exist *before* SQL does.
- Sector/NACE breakdowns as a first-class dimension (see Review §2 — deliberately deferred).
- Deciding the physical engine. Types below are written for SQLite (the current engine) but
  chosen to port cleanly to Postgres.

## Proposed approach

Five tables: `geographies`, `indicators`, `observations`, `sources`, `fetch_runs`.

> **Deviation from `docs/data_model.md`, flagged per the template.** That earlier document named
> the tables `indicators`, `geo`, `observations`, `forecasts`, `fetch_log`. This spec renames
> `geo` → `geographies` and `fetch_log` → `fetch_runs`, and replaces `forecasts` with `sources`.
> Where the FPB forecasts now live is an open question — see Assumptions Q1. On approval,
> `docs/data_model.md` should be updated to match this document, which becomes authoritative.

Constraints are written as `CHECK`/`FOREIGN KEY` clauses. SQLite does not enforce foreign keys
unless `PRAGMA foreign_keys=ON` is set per-connection — so every FK below **must also be
enforced in the adapter layer** (or by a connection that reliably sets the pragma), since the DDL
alone is not a guarantee. `CHECK`-based enums are different: SQLite validates `CHECK` on every
write regardless of any pragma, so the enum columns below (`status`, `preferred_direction`,
`frequency`, `level`, `aggregation_method`) are genuinely enforced by the schema itself, with no
adapter-layer code required — confirmed by independent audit of the `001_core_schema.sql`
migration. What the DDL cannot express, and which does remain adapter-layer-only, is any
constraint that spans tables or requires domain logic — e.g. that `observations.period` matches
its indicator's declared `frequency` (§ Period format rules), or that exactly one row per
`(indicator_id, geo_id, period)` has `is_latest = 1` (§ Vintage).

---

### 1. `sources`

One row per upstream data provider/endpoint. The machine-readable counterpart to a
`docs/data_catalog.md` row (required by `CLAUDE.md` rule 8 before a source may be added).

| Column        | Type    | Constraints |
|---------------|---------|-------------|
| `source_id`   | TEXT    | **PK**. Stable slug, e.g. `nbb_sdmx_qna`, `eurostat_dbnomics`, `fpb_xlsx`. |
| `name`        | TEXT    | NOT NULL. Human-readable, e.g. "NBB SDMX Dissemination — Quarterly National Accounts". |
| `agency`      | TEXT    | NOT NULL. Publishing body: `NBB`, `Eurostat`, `FPB`, `AMECO`. |
| `adapter`     | TEXT    | NOT NULL. Name of the `DataSource` implementation that reads it. |
| `base_url`    | TEXT    | NULL. |
| `licence`     | TEXT    | NULL. Redistribution terms — needed before any figure is republished. |
| `catalog_ref` | TEXT    | NOT NULL. Anchor of the approving row in `docs/data_catalog.md`. |
| `cadence`     | TEXT    | NULL. Expected publication rhythm, e.g. `daily`, `monthly`, `quarterly`. Used to detect a source that has gone stale. |
| `is_active`   | INTEGER | NOT NULL DEFAULT 1 (boolean). Soft-disable without losing history. |

Index: `UNIQUE (source_id)` (implicit via PK). No other index needed — the table is tiny.

---

### 2. `geographies`

Canonical geography, versioned over time. Every `nis_code` seen in a source file must be
resolved through this table via `resolve_geo(nis, period)`; a raw NIS code is never stored on an
observation.

| Column            | Type    | Constraints |
|-------------------|---------|-------------|
| `geo_id`          | TEXT    | **PK**. Internal canonical id, stable across NIS renumbering. Convention: `be:mun:11002`, `be:prov:10000`, `be:reg:2000`, `be:country`, `eu:ea20`. |
| `nis_code`        | TEXT    | NULL for non-Belgian/aggregate entities. The official INS/NIS code valid during this row's window. |
| `level`           | TEXT    | NOT NULL. CHECK IN (`country`, `region`, `province`, `arrondissement`, `municipality`, `eu_aggregate`). |
| `name_nl`         | TEXT    | NOT NULL |
| `name_fr`         | TEXT    | NOT NULL |
| `name_en`         | TEXT    | NOT NULL |
| `parent_geo_id`   | TEXT    | NULL. FK → `geographies(geo_id)`. NULL only at the top of a hierarchy. |
| `valid_from`      | TEXT    | NOT NULL. `YYYY-MM-DD`. First date this entity exists with this identity/boundary. |
| `valid_to`        | TEXT    | NULL = currently valid. `YYYY-MM-DD`, exclusive upper bound. |
| `successor_geo_id`| TEXT    | NULL. FK → `geographies(geo_id)`. Set when this entity is absorbed by a merger; carries merger lineage (see Review §1). |
| `population`      | INTEGER | NULL. Most recent known population; the weight for `population_weighted` aggregation. |
| `area_km2`        | REAL    | NULL. |

Constraints and indexes:

- `UNIQUE (nis_code, valid_from)` — a NIS code may be reused after a merger, so uniqueness is
  scoped to the validity window, never to the code alone.
- `CHECK (valid_to IS NULL OR valid_to > valid_from)`
- `INDEX idx_geo_nis_period ON geographies(nis_code, valid_from, valid_to)` — the lookup
  `resolve_geo` performs on every ingested row.
- `INDEX idx_geo_level ON geographies(level)` — "all municipalities" queries.

---

### 3. `indicators`

One row per indicator definition. Populated **from** `config/indicators/*.yaml`, which stays
authoritative (`CLAUDE.md` rule 2); this table is a queryable mirror, never hand-edited.

| Column                | Type    | Constraints |
|-----------------------|---------|-------------|
| `indicator_id`        | TEXT    | **PK**. Stable code, e.g. `gdp_growth_yy`, `unemployment_rate`. |
| `source_id`           | TEXT    | NOT NULL. FK → `sources(source_id)`. An indicator belongs to exactly one source — see Review §3. |
| `name_nl` / `name_fr` / `name_en`                   | TEXT | NOT NULL (three columns) |
| `description_nl` / `description_fr` / `description_en` | TEXT | NULL (three columns) |
| `frequency`           | TEXT    | NOT NULL. CHECK IN (`A`, `Q`, `M`, `D`). Fixed per indicator — this is what makes period sorting well-defined. |
| `unit`                | TEXT    | NOT NULL. e.g. `percent_yy`, `index_2010`, `balance`, `eur`, `persons`. |
| `preferred_direction` | TEXT    | NOT NULL. CHECK IN (`lower_is_better`, `higher_is_better`, `neutral`, `contextual`). See §Enums. |
| `aggregation_method`  | TEXT    | NOT NULL. CHECK IN (`population_weighted`, `sum`, `unweighted_mean`, `not_applicable`). DEFAULT `population_weighted`. |
| `is_additive`         | INTEGER | NOT NULL (boolean). TRUE only if values may be summed across geographies (counts, euros). FALSE for rates/indices. Gates merger back-aggregation (Review §1). |
| `decimals`            | INTEGER | NOT NULL DEFAULT 1. Display precision; keeps rounding out of the phrasing layer. |
| `config_path`         | TEXT    | NOT NULL. Owning file under `config/indicators/`. |
| `is_active`           | INTEGER | NOT NULL DEFAULT 1 (boolean). |

Index: `INDEX idx_ind_source ON indicators(source_id)`.

---

### 4. `observations`

The fact table.

**Primary key: `(indicator_id, geo_id, period, vintage)`** — stated explicitly, as required.
Nothing else is part of the key. The consequences of that choice (notably for multi-source
conflicts) are worked through in Review §3.

| Column         | Type    | Constraints |
|----------------|---------|-------------|
| `indicator_id` | TEXT    | NOT NULL. FK → `indicators(indicator_id)`. **PK part 1.** |
| `geo_id`       | TEXT    | NOT NULL. FK → `geographies(geo_id)`. **PK part 2.** |
| `period`       | TEXT    | NOT NULL. Format per §Period rules; must match the indicator's `frequency`. **PK part 3.** |
| `vintage`      | TEXT    | NOT NULL. `YYYY-MM-DD` — the date the source published *this value*. **PK part 4.** See §Vintage. |
| `value`        | REAL    | NULL only when `status` is `suppressed` or `na`. |
| `status`       | TEXT    | NOT NULL. CHECK IN (`final`, `provisional`, `estimate`, `revised`, `suppressed`, `na`). |
| `period_start` | TEXT    | NOT NULL. `YYYY-MM-DD`, first day of the period. Derived at ingest; makes cross-frequency ordering and range queries correct (see §Sorting). |
| `period_end`   | TEXT    | NOT NULL. `YYYY-MM-DD`, last day of the period, inclusive. |
| `is_latest`    | INTEGER | NOT NULL (boolean). Exactly one TRUE per `(indicator_id, geo_id, period)`. Maintained transactionally on insert of a newer vintage. |
| `fetch_run_id` | INTEGER | NOT NULL. FK → `fetch_runs(fetch_run_id)`. Which run wrote this row. |
| `created_at`   | TEXT    | NOT NULL. ISO-8601 UTC timestamp of insertion (distinct from `vintage`, which is the source's date). |

Constraints and indexes:

- `CHECK (value IS NOT NULL OR status IN ('suppressed','na'))`
- `CHECK (period_end >= period_start)`
- `INDEX idx_obs_series ON observations(indicator_id, geo_id, period_start) WHERE is_latest = 1`
  — the time-series read path (partial index; on SQLite requires 3.8+, which is universal).
- `INDEX idx_obs_geo_period ON observations(geo_id, period_start) WHERE is_latest = 1` —
  "everything known about this municipality for this period", the commune-profile query.
- `INDEX idx_obs_run ON observations(fetch_run_id)` — for auditing/rolling back a bad run.

---

### 5. `fetch_runs`

One row per adapter execution. Replaces `fetch_log`, with the run as the unit rather than the
indicator, so a whole run can be audited or reverted.

| Column         | Type    | Constraints |
|----------------|---------|-------------|
| `fetch_run_id` | INTEGER | **PK**, autoincrement. |
| `source_id`    | TEXT    | NOT NULL. FK → `sources(source_id)`. |
| `adapter`      | TEXT    | NOT NULL. Implementation that ran. |
| `started_at`   | TEXT    | NOT NULL. ISO-8601 UTC. |
| `finished_at`  | TEXT    | NULL until the run terminates. |
| `status`       | TEXT    | NOT NULL. CHECK IN (`ok`, `partial`, `error`, `schema_changed`). `schema_changed` is distinct from `error` so a structural upstream change (`CLAUDE.md` rule 13) is queryable on its own and can page a human. |
| `rows_read`    | INTEGER | NOT NULL DEFAULT 0. Parsed from the source. |
| `rows_written` | INTEGER | NOT NULL DEFAULT 0. Actually persisted. A large gap between the two is the signal for a silent-drop bug. |
| `http_status`  | INTEGER | NULL. |
| `message`      | TEXT    | NULL. Error text or schema diff. |

Index: `INDEX idx_runs_source_started ON fetch_runs(source_id, started_at DESC)` — "when did
this source last succeed", which is how staleness is detected.

---

## Period format rules

One `period` TEXT column, plus `frequency` on the indicator. No separate year/quarter/month
columns.

| Frequency | Format       | Example      | `period_start` | `period_end` |
|-----------|--------------|--------------|----------------|--------------|
| `A`       | `YYYY`       | `2024`       | `2024-01-01`   | `2024-12-31` |
| `Q`       | `YYYY-Qn`    | `2024-Q3`    | `2024-07-01`   | `2024-09-30` |
| `M`       | `YYYY-MM`    | `2024-07`    | `2024-07-01`   | `2024-07-31` |
| `D`       | `YYYY-MM-DD` | `2024-07-01` | `2024-07-01`   | `2024-07-01` |

Rules:

1. Zero-padding is mandatory: `2024-07`, never `2024-7`. `Q` is uppercase.
2. A row's `period` format **must** match its indicator's `frequency`. An adapter that receives
   a period in the wrong shape fails the run (`schema_changed`) rather than coercing it.
3. Belgian fiscal years equal calendar years; no offset-year handling is defined. If a source
   ever publishes an offset fiscal year, that is an escalation, not something to coerce.

### Sorting

- **Within one indicator**, `frequency` is fixed, so all its periods share one format and plain
  lexicographic `ORDER BY period` is correct. This is the common case and needs no special
  handling.
- **Across frequencies**, lexicographic sorting is *wrong*: `'2024-Q3' > '2024-12'` because `Q`
  (0x51) sorts above digits in ASCII, so a quarterly period would sort after every month of its
  own year. Any query mixing frequencies — a commune profile showing annual and monthly
  indicators together, for instance — **must** `ORDER BY period_start`, never `period`. This is
  the reason `period_start` is a stored column rather than something computed at read time.
- Date-range filters must likewise use `period_start`/`period_end`, not string comparison on
  `period`.

### Vintage

`vintage` is the **source's publication date for that value**, not our fetch date — so the same
figure re-fetched daily does not accumulate one row per day. Rules:

- If the source exposes a release/vintage date (SDMX often does), use it.
- If it does not, use the date on which we first observed *this value* for this cell. A re-fetch
  returning an unchanged value must not create a new vintage; only a changed value does.
- Consequence: `(indicator_id, geo_id, period)` yields the full revision history, ordered by
  `vintage`; `is_latest = 1` selects the current belief. "What did we believe on date D" is
  `MAX(vintage) WHERE vintage <= D`.

## Enums

`status` — the state of a single value:

| Value         | Meaning |
|---------------|---------|
| `final`       | Source considers it definitive; not expected to change. |
| `provisional` | Published but explicitly subject to revision (typical first estimates). |
| `estimate`    | Modelled/imputed by the source rather than measured. |
| `revised`     | Supersedes an earlier published value for the same cell. |
| `suppressed`  | Withheld by the source, usually statistical disclosure control (small counts in a small commune). `value` IS NULL. **Not the same as missing** — it means a real value exists and is being withheld. |
| `na`          | Genuinely not applicable/not collected for this cell. `value` IS NULL. |

The `suppressed`/`na` split matters at municipality level, where disclosure control is common;
conflating them would let output say "no data" when the honest statement is "withheld".

`preferred_direction` — how to read a movement, consumed by any comparison/phrasing layer:

| Value               | Meaning |
|---------------------|---------|
| `higher_is_better`  | Employment rate, GDP growth. |
| `lower_is_better`   | Unemployment rate, poverty rate. |
| `neutral`           | Population, area — directionless; movement is not good or bad. |
| `contextual`        | Direction depends on interpretation and must not be judged automatically, e.g. population density, share of a sector. The phrasing layer must report the change without evaluative language. |

`contextual` exists so the generated-text layer has an explicit "do not editorialise" signal
rather than defaulting to `neutral` and silently losing the distinction.

## Data / schema changes

Everything above is new relative to the shipped schema. Migration concerns, for the
implementing issue rather than this spec:

- `geographies` must be populated (and merger lineage backfilled) before `observations.geo_id`
  can become a real FK rather than a constant.
- Existing `observations` rows are national and have no revision history: backfill
  `geo_id = 'be:country'`, `vintage` = the row's existing `fetched_at` date, `is_latest = 1`,
  `status = 'final'` (with the caveat in Q2 below).
- Existing `indicators` rows carry English-only text; NL/FR are unpopulated until translated.
- `sources` rows must be created alongside the `docs/data_catalog.md` entries, which are
  currently a stub.

## New data sources

None. This spec restructures storage for sources already in use (NBB SDMX, Eurostat/AMECO via
DBnomics, FPB). Their `docs/data_catalog.md` rows still need to be written and approved before
the `sources` table can be populated honestly.

## Tests

- `resolve_geo(nis, period)`: code valid in window; code after a merger resolves to the
  successor; code reused post-merger resolves to the right entity per period; unknown code
  raises (never silently returns NULL).
- Period parsing: each of the four formats maps to the correct `period_start`/`period_end`,
  including the leap-year cases `2024-02` → `2024-02-29` and `2023-02` → `2023-02-28`.
- Sorting: a mixed-frequency set ordered by `period_start` is correct where ordering by `period`
  is not — asserted directly, since this is the trap the model exists to avoid.
- Vintage: unchanged re-fetch creates no row; changed value creates a second vintage and flips
  `is_latest`; the earlier row survives with `is_latest = 0`.
- Enum enforcement at the adapter layer: an out-of-vocabulary `status` is rejected.
- `CHECK` semantics: `value IS NULL` accepted with `suppressed`/`na`, rejected otherwise.

## Assumptions and open questions

- **Q1 — Where do FPB forecasts live?** This spec's five tables have no `forecasts` table. Two
  options: (a) store them in `observations` with a forecaster `source_id` and a future `period`,
  or (b) keep a sixth table. (a) keeps one query path and is defensible because a published
  forecast *is* source data, not a derived value (so `CLAUDE.md` rule 6 is not violated) — but it
  puts projections and measurements in one table, where any careless query silently mixes them.
  If (a) is chosen, a `is_projection` flag or a dedicated `status` value is required, and the
  five-table count in the title becomes five tables plus a flag. **Needs a decision before
  implementation.**
- **Q2 — Backfilling `status` for existing rows.** NBB SDMX supplies `OBS_STATUS`, currently
  discarded into a free-text column. Whether historical rows can be honestly labelled `final`
  versus `provisional` depends on that field being re-read from the source. Assuming `final`
  wholesale would assert something we have not verified. Recommend re-fetching rather than
  guessing.
- **Q3 — Statistical sectors.** Belgian *statistical sectors* (sub-municipal, ~19k units) are not
  in the `level` enum. If commune profiles ever need sub-municipal granularity the enum grows,
  which is cheap; noted so it is a conscious later choice.
- **Q4 — `vintage` when the source gives no release date.** The "first date we saw this value"
  rule is a pragmatic substitute for a real publication date and will be wrong by up to one fetch
  interval. Acceptable for daily fetches; flagged in case any downstream use needs true release
  dates.
- I have not verified the exact NIS codes or dates of the 2019 and 2025 municipal merger waves.
  Those are Belgian administrative facts to be sourced from Statbel, not assumed here.

## Review

Against the four scenarios named in the request.

### 1. Municipal mergers

Handled by versioning: the absorbed municipalities keep their rows, get `valid_to` set to the
merger date, and `successor_geo_id` pointing at the new entity, which gets its own `geo_id` and
`valid_from`. Historical observations stay attached to the entity that actually reported them —
nothing is rewritten — and `resolve_geo(nis, period)` returns the entity valid *at that period*,
so a 2015 file about Sint-Amands resolves to Sint-Amands even though it no longer exists.

Two real limitations, both deliberate:

- **A merged municipality has no true history.** Puurs-Sint-Amands did not exist before 2019, so
  its pre-2019 series must be *reconstructed* from its predecessors. That is only valid where
  `indicators.is_additive = 1` (counts, euros). For a rate or an index, summing predecessors is
  arithmetically wrong, and a population-weighted recombination is an estimate, not a fact. The
  model therefore refuses to fabricate it: back-aggregation is a query-layer operation, gated on
  `is_additive`, and never written into `observations` (`CLAUDE.md` rule 6). Where it cannot be
  computed honestly, the profile shows a discontinuity — which is the truth.
- **Lineage is one generation deep per row.** `successor_geo_id` chains, so a municipality merged
  twice requires walking the chain recursively. Fine at Belgium's scale; noted so nobody assumes
  a single hop suffices.

The 2025 merger wave means this is live, not hypothetical: any commune-comparison output
spanning 2024–2025 crosses a boundary change and must state it rather than silently splicing.

### 2. Sector-level data

**This model does not support sector breakdowns, and that is a gap, not a solved problem.**
`observations` is keyed by indicator × geography × time only. Employment by NACE sector, for
instance, has nowhere to put "NACE C".

The three options:

- **(a) Encode the sector in `indicator_id`** — `employment_nace_c`, `employment_nace_f`, …
  Preserves the mandated PK exactly, needs no schema change, and works today. Cost: indicator
  cardinality multiplies (21 NACE sections × n measures), and "total employment across sectors"
  becomes a convention rather than a query.
- **(b) Add a `breakdown` column to the PK** — most expressive, but **changes the primary key
  that this spec was explicitly asked to fix**, so it is out of scope here and would need its own
  decision record.
- **(c) A separate sector fact table** — exceeds the five-table budget.

Recommendation: **(a) for now**, with a documented naming convention, and a
`docs/decisions/` ADR opened when sector data is genuinely required, so the cost of (b) is
weighed deliberately rather than discovered mid-implementation. Anyone reading this should know
the model has a ceiling here.

### 3. Multi-source conflicts

With the PK as mandated, `source_id` is *not* part of the key. If two sources published the same
`(indicator_id, geo_id, period, vintage)` they would collide and one would overwrite the other —
a silent wrong number, exactly the failure mode this platform must not have.

Resolved by construction rather than by widening the key: **`indicators.source_id` is NOT NULL
and an indicator belongs to exactly one source.** Eurostat's Belgian GDP and NBB's Belgian GDP
are therefore two distinct indicators, not one indicator with two providers. A collision is then
impossible by definition, and the PK stays as specified.

The cost is honest and worth stating: cross-source comparison ("do NBB and Eurostat agree?")
becomes a query across two indicator ids, and there is no built-in notion of a *preferred*
source for a measure. If that is later needed, the right shape is a `measures` concept above
indicators — a new table, and therefore a new decision — not smuggling `source_id` into the
observations key.

### 4. Revisions

This is the scenario the model handles best. `vintage` in the PK means a revision is an insert,
never an update; the prior value survives with `is_latest = 0`. `status = 'revised'` marks it
semantically, `MAX(vintage) WHERE vintage <= D` reconstructs any past belief, and `fetch_run_id`
ties every row to the run that wrote it, so a bad run is identifiable and reversible.

Two things the implementation must get right or the guarantee is hollow:

- `is_latest` must be flipped **in the same transaction** as the insert. Two rows with
  `is_latest = 1` for one cell would double-count in every downstream query; a periodic
  consistency assertion is warranted.
- Revisions arriving *out of order* (a source republishing an older vintage after a newer one)
  must not blindly set `is_latest`. The rule is `is_latest` follows `MAX(vintage)`, not
  "most recently inserted".

Suppression flips are also revisions: a cell moving `final` → `suppressed` is a new vintage with
a NULL value, and downstream output must show the withdrawal rather than continuing to display
the stale number it fetched last month.

## Rollout / risks

- **Highest risk: `geographies` is wrong or incomplete.** Every observation FKs into it, so a
  bad merger date or a missing NIS row silently misattributes data to the wrong commune —
  precisely the "wrong number published to a municipality" failure `CLAUDE.md` opens with. The
  table should be built from an authoritative Statbel list and spot-checked against known merger
  cases before any bulk ingest.
- The migration rewrites the fact table's key. It must run on a copy, with row counts and a
  sample of values compared before and after; `data/belgian_macro.db` is committed to git, so a
  bad migration is recoverable but would pollute history.
- `is_latest` maintenance is the subtlest piece of logic in the model and the easiest to get
  wrong under concurrency; the pipeline is single-writer today, which should stay true or be
  revisited explicitly.
- Enum enforcement lives in the adapter layer on SQLite. A future direct-SQL writer that bypasses
  the adapters would bypass every `CHECK` — the contract test should assert that adapters are the
  only write path.
