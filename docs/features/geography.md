# Feature: Belgian geography master

Status: draft
Issue: (Block C — Belgian geography master, docs/steps)
Branch: feat/geography-master

## Problem

The `geographies` table exists (`001_core_schema.sql`) and holds exactly one row: `be:country`.
Every indicator so far is national, so nothing has ever exercised the table's actual purpose —
resolving a Belgian statistical-authority NIS code to a canonical, time-versioned entity. Block F
(Statbel adapter) and everything above it in the roadmap (peer model, municipal finance,
percentiles, `/local`) is blocked on this: none of it can attach a value to a specific commune
without a real geography hierarchy and a working `resolve_geo(nis, period)`.

`data_model.md` already specifies the `geographies` schema and works through municipal mergers
in its Review §1 — that reasoning is not repeated here, only referenced. What is still missing
is everything operational: the actual rows (581+ communes, provinces, arrondissements, regions),
the merger crosswalk, and the resolution function itself.

## Goal

- Every level of the official Belgian administrative hierarchy — country, region, province,
  arrondissement, municipality — is loaded into `geographies` with correct `parent_geo_id`
  chains, sourced from an official Statbel file with a recorded download date.
- Every current commune resolves to `be:country` by walking `parent_geo_id`, with no orphans and
  no cycles (tested).
- `resolve_geo(nis_code, period)` exists, is used by every adapter, and returns the entity that
  was valid for that NIS code at that period — including pre-merger entities for historical data.
- A `municipality_crosswalk.csv` exists, covering at minimum the 2019 and 2025 Flemish merger
  waves, manually spot-checked against official merger lists (an `[H]` step — see below).
- Licence terms for the Statbel geography download are recorded in `docs/data_catalog.md`
  before any of this ships, per `CLAUDE.md` rule 8.

## Non-goals

- Geometry/boundary files (shapes for maps) beyond deciding *where* they will eventually live.
  Block C stores geometry separately from `geographies` (a `geometry_id` reference or static
  GeoJSON files) precisely so this spec and its loader don't have to solve rendering — that's
  Block X.
- Statistical sectors (sub-commune level). The roadmap step ("Load Belgium, regions, provinces,
  arrondissements, communes, statistical sectors") lists them, but nothing before Phase II uses
  sub-commune geography, and statistical-sector boundaries change independently of commune
  mergers. Loading them here without a consumer would be scope creep against `CLAUDE.md`'s own
  spirit (rule 10) — deferred to whichever block first needs them, tracked as an open question
  below (Q2).
- Re-deriving or second-guessing NIS codes. This model resolves codes; it does not mint them.
  Any code not present in the official Statbel file is an error, not something to infer.
- Wallonia-specific finance geography (WalStat regions, etc.) — Block O's concern, built on top
  of this, not part of it.

## Proposed approach

### NIS semantics

The NIS/INS code ("code INS" / "NIS-code") is Belgium's official numeric identifier for
administrative geography, assigned by Statbel (formerly INS). It is hierarchical only by
convention, not by structure — you cannot derive a commune's province from its digits alone, so
the hierarchy must come from the reference file's own parent links, never from string-slicing
the code:

- **Municipality**: 5-digit code, e.g. `11002` (Antwerpen). Unique among *currently active*
  communes, but a code can be **reassigned** after a merger deregisters it — this is exactly why
  `geographies` uniques `(nis_code, valid_from)` rather than `nis_code` alone (see
  `data_model.md` §2).
- **Arrondissement**: 5-digit code, distinct numbering space from municipalities (e.g. `11000`
  for Arrondissement Antwerpen). Purely administrative/judicial; not used for regional political
  boundaries.
- **Province**: 5-digit code (e.g. `10000` Antwerpen province). Brussels-Capital has no province.
- **Region**: 4-digit code (`2000` Flemish Region, `3000` Walloon Region, `4000` Brussels-Capital
  Region).
- **Country**: no NIS code. `geo_id = 'be:country'`, `nis_code = NULL`.

`geo_id` convention (already used by `be:country`, extended here):
`be:mun:<nis>`, `be:arr:<nis>`, `be:prov:<nis>`, `be:reg:<nis>`, `be:country`. Non-Belgian
aggregates (already present via existing indicators like `EUROSTAT_GDP_Q_MEUR_DE`) get
`eu_aggregate`-level rows once/if they need geography rows of their own — out of scope for this
block, whose subject is Belgium's own hierarchy.

### Source file

Statbel publishes the reference geography (the "Refnis" list plus the commune/province/region
structure) on its open-data portal. The `[H]` step below is downloading it and recording:
- the exact file and portal URL,
- the download date,
- the licence terms (into `docs/data_catalog.md`, required before this is built on).

This spec does not fabricate that URL or licence text — an agent has no way to verify either,
and a wrong licence claim here is exactly the kind of thing `CLAUDE.md` rule 8 exists to prevent.

### Loading

One loader per level (`load_regions`, `load_provinces`, `load_arrondissements`,
`load_municipalities`), each:
1. reads the official file for that level,
2. writes one `geographies` row per entity with `valid_from` = the file's own effective date (or
   `1830-01-01` for entities with no known creation date, matching the existing `be:country`
   row),
3. sets `parent_geo_id` from the file's own parent reference — never inferred from naming or code
   ranges.

Order matters: regions and provinces first (referenced as parents), then arrondissements, then
municipalities last (the largest set, ~581 rows, each referencing an arrondissement or region).

### Merger handling

Fully specified already in `data_model.md` Review §1 (versioning via `valid_to` +
`successor_geo_id`, `is_additive`-gated back-aggregation, no fabricated pre-merger history). This
spec adds only the operational piece that document deferred: `municipality_crosswalk.csv`.

**`municipality_crosswalk.csv`** — columns `old_nis, new_nis, valid_from, valid_to,
relationship`, where `relationship` is one of `merged`, `renamed`, `split`. This is the
authored-once, spot-checked artefact that lets a loader or a late-arriving 2015 source file
resolve `old_nis` without every adapter re-deriving merger logic. It is data, not code — reviewed
by a human (the `[H]` step below), not generated and trusted blind, because a wrong row here
produces a *plausible* wrong number silently (this block's Review focus).

### Resolution: `resolve_geo(nis, period)`

```
def resolve_geo(nis: str, period: str) -> str:  # returns geo_id
    date = period_to_date(period)  # first day of period, per data_model.md §Sorting
    row = query(
        "SELECT geo_id FROM geographies "
        "WHERE nis_code = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)",
        nis, date, date,
    )
    if row:
        return row.geo_id
    if nis in crosswalk.old_nis:
        return resolve_geo(crosswalk[nis].new_nis, period)  # only if period is pre-merger and
                                                              # crosswalk says this old code maps
                                                              # to a still-earlier entity chain
    raise UnknownGeographyError(nis, period)
```

Two things this must get right, both already implied by `data_model.md`'s uniqueness constraint
but stated explicitly since this is where a bug would actually manifest:
- A single `nis_code` can match multiple rows across time (reassignment after a merger); the
  `valid_from`/`valid_to` window, not the code alone, disambiguates.
- Unknown codes **raise**, they do not silently map to `NULL` or get skipped — matching
  `CLAUDE.md` rule 13 ("fail loudly, never silently coerce or drop rows").

### Geometry

Not loaded as part of `geographies` rows. Either a `geometries` table keyed by a `geometry_id`
foreign key, or static per-level GeoJSON files under `data/geometry/` keyed by `nis_code` —
whichever is cheaper is decided when Block X (maps) actually needs it, per the roadmap's own
ordering. Recorded here only so nobody accidentally inlines boundary polygons into `geographies`
or into an export payload before then.

## Data / schema changes

None. `geographies` (`001_core_schema.sql`) already has every column this block needs
(`parent_geo_id`, `valid_from`/`valid_to`, `successor_geo_id`, `population`, `area_km2`). This
block populates it and adds one new artefact, `municipality_crosswalk.csv`, plus the
`resolve_geo()` function — no migration required.

## New data sources

The Statbel geography reference file is a new source and needs a `docs/data_catalog.md` row with
verified licence terms before implementation proceeds — this is the `[H]` step immediately
following this spec in `docs/steps`, and it is the maintainer's step, not something an agent can
discharge (per `CLAUDE.md` rule 8 and this document's own Non-goals).

## Tests

- `resolve_geo`: known current commune resolves correctly; known pre-merger commune resolves to
  the historical entity for a pre-merger period and to the successor for a post-merger period;
  unknown NIS code raises `UnknownGeographyError`.
- Every municipality's `parent_geo_id` chain terminates at `be:country` — recursive-CTE walk,
  asserting zero orphans and zero cycles.
- Exact row counts per level, asserted as constants (per docs/steps' own rationale: a hardcoded
  expected count is what catches a loader silently dropping a province).
- Crosswalk: no `old_nis`/`new_nis` cycles; every `old_nis` has a `valid_to` matching the
  corresponding `geographies.valid_to`.

## Assumptions and open questions

- **Q1 — Exact source file identity.** This spec deliberately does not name a specific Statbel
  URL or file format (CSV/GeoJSON/shapefile) — that is the `[H]` download step's output, not an
  assumption to make in advance. The loader design above is format-agnostic on purpose.
- **Q2 — Statistical sectors.** Listed in the roadmap step but has no consumer before Phase II
  (see Non-goals). Recommend deferring until a block actually needs sub-commune geography, with
  a one-line note added here when that happens, rather than loading unused rows now.
- **Q3 — Brussels-Capital's missing province level.** `parent_geo_id` for Brussels communes must
  point directly at the region, skipping the province level entirely (Brussels has none). The
  loader must not assume every municipality has a province parent.

## Rollout / risks

- Wrong or hallucinated NIS codes are the single most damaging failure mode this feature can
  introduce — a mismapped commune silently attaches real numbers to the wrong place. This is
  exactly why the source file must be official (not scraped or LLM-recalled) and why the `[RED]`
  step in `docs/steps` ("find every way a NIS code could be silently mismapped") follows
  immediately after the build steps, before CONTROL C.
- Crosswalk errors are the second-highest risk and the hardest to catch automatically — a stray
  discontinuity in a merged commune's population chart is the actual test (CONTROL C's own `[H]`
  step), not a unit test against the crosswalk's own logic.
