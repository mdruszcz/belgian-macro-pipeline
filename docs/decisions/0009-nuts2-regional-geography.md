# ADR 0009 — A separate, allowlisted NUTS 2 geography table, never merged into the Belgian NIS table

Date: 2026-09-14
Status: Accepted (2026-09-14, maintainer)
Revises: none directly; extends [ADR 0008](0008-eurostat-dataset-adapter-and-directory-stores.md)'s
allowlist pattern one geographic level down, from country to NUTS 2 region.

## Context

`docs/features/europe_nuts2.md` (batch B1) proposes a NUTS 2 choropleth for three Eurostat
indicators (GDP per capita PPS, unemployment rate, population). Doing so needs a geography table
for roughly 300–360 European statistical regions this pipeline has never stored before. Two
tables already exist that look similar and are both wrong to extend:

- `config/geography/geographies.csv` is Belgium's canonical, NIS-keyed geography (CLAUDE.md
  rules 3/25: NIS is the one Belgian geographic key, resolved through `resolve_geo(nis, period)`
  everywhere). It already carries NUTS codes for Belgium's own regions/provinces/arrondissements
  in an existing `nuts` column — including 10 of Belgium's 11 real NUTS 2 regions (the
  provinces, `BE21`–`BE25`/`BE31`–`BE35`). Adding ~300 foreign rows to this table would put a
  non-NIS geography inside the one table every Belgian page, aggregate and `resolve_geo` call
  assumes is exhaustively Belgian.
- `config/geography/international.csv` (ADR 0008) is a *country*-level allowlist keyed by
  Eurostat's 2-letter country code, with a `level: country` or `eu_aggregate` row shape. A NUTS 2
  code is not a country and does not fit that row shape without changing what "level" means for
  every existing row.

The coverage report measured a genuine complication rather than a clean split: Brussels-Capital's
NUTS 2 code, `BE10`, is not tagged anywhere in `geographies.csv` today — the region row carries
NUTS 1 (`BE1`) and its one arrondissement carries NUTS 3 (`BE100`), because Brussels has no
province level to carry a NUTS 2 tag the way the other ten provinces do.

## Decision

1. **A new file, `config/geography/nuts2.csv`**, allowlisted the same way `international.csv`
   is: one row per NUTS 2 region this pipeline has decided to publish, `nuts_code,geo_id,
   country_prefix,name_en,name_fr,name_nl,nuts_version,valid_from`. `geo_id` follows
   `international.csv`'s lowercase-with-suffix convention (`fr10:nuts2`), and `level: nuts2` is a
   new `geographies.level` value distinct from `country`/`region`/`province`/etc. This file is
   never merged into `geographies.csv` and a NUTS 2 `geo_id` never carries a `nis_code`.
2. **Belgium's 10 existing NUTS-2-tagged provinces are read from `geographies.csv` as-is, not
   duplicated into `nuts2.csv`.** A NUTS 2 loader resolves a `BE2x`/`BE3x` code to its existing
   Belgian `geo_id` (`be:prov:...`) rather than minting a second, parallel geo_id for the same
   province — one province, one `geo_id`, exactly ADR 0008's own overlap rule for `BE` (already
   applied there to the country level; this extends it one level down).
3. **Brussels' `BE10` is an explicit alias, decided by the maintainer, not inferred.** This ADR
   does not resolve it — `docs/features/europe_nuts2.md`'s decisions list asks the maintainer to
   choose between aliasing `BE10` to the existing `be:reg:04000` row (a lookup-table entry, not a
   `geographies.csv` schema change) or accepting Brussels renders `missing` on the panel until
   that is decided.
4. **A second, explicit exclusion list for aggregate codes that are structurally NUTS-2-shaped
   but are not regions** (`EA`, `EA20`, `EA21`, `EU`, `EU27_2020`, `EU28`, `EFTA`, and any future
   one Eurostat adds) — the coverage report found `EA21` and `EFTA` passing a naive 4-character
   NUTS shape check inside real dataset responses. Per CLAUDE.md rule 13, a NUTS 2 loader must
   fail loudly on a code in neither the region allowlist nor this exclusion list, exactly like
   `international.py` already does for countries — never silently keep or silently drop it.
5. **The NUTS 2024 vintage of Nuts2json geometry**, self-hosted under `public/data/geo/nuts2/`,
   is the geometry this table's `nuts_version` column is written against. Measured 2026-09-14: it
   matches each of the three candidate datasets' latest published year with far fewer
   region-code mismatches than the 2021 vintage (10–23 vs. 46–65), and its own UK-region
   retirement lines up with the licence exclusion below for free.

## Alternatives rejected

- **Add a `level: nuts2` row shape directly to `international.csv`.** Rejected: that file's
  `scope`/`basis` columns (`pilot`/`legacy`, `eu`/`efta`/`candidate`/`aggregate`) describe country
  and aggregate identity, not sub-national structure, and every existing consumer
  (`src/geography/international.py`, `scripts/sync_international.py`) assumes one row per
  country. Reusing the file would either silently change what its existing rows mean or require
  every current reader to branch on a new column — more invasive than a second, purpose-shaped
  file.
- **Extend `geographies.csv` with foreign rows, `nis_code` NULL, the same way `international.csv`
  currently keeps country rows separate from it.** Rejected: unlike the country-level pilot,
  which never touched `geographies.csv` at all, this would be the first time a non-Belgian row
  entered the one table every Belgian aggregate, page and `resolve_geo` call treats as
  exhaustively Belgian and NIS-keyed (rule 25). The risk is not hypothetical: this table already
  has 10 Belgian rows carrying NUTS 2 tags in an existing column, so a bug that queries
  `geographies.csv` "by NUTS code" without also filtering `nis_code IS NOT NULL` would already
  see a plausible-looking but wrong mix of Belgian and foreign rows.
- **Derive NUTS 2 regions purely from the Nuts2json geometry file at render time, no CSV at
  all.** Rejected: the geometry file has no licence/allowlist concept and no stable place to
  attach `nuts_version`, `valid_from` or a Belgian-province alias decision — exactly the
  information CLAUDE.md rule 28 (source/unit/period/status/freshness from existing metadata,
  never hand-typed) requires living somewhere durable, not recomputed from a third-party file's
  shape on every export.

## Consequences

- A NUTS 2 loader (not built in this batch) needs two allowlists consulted together — the region
  list (`nuts2.csv`) and the aggregate-exclusion list (decision 4) — where the country pilot
  needed only one. A code in neither fails the fetch outright, matching ADR 0008's existing rule
  at one level down.
- Belgium's provinces are dual-registered by reference, not by copy: any change to a Belgian
  province's `geo_id` in `geographies.csv` (a boundary change, a merger) must be reflected in
  whatever mapping table resolves `BE2x`/`BE3x` codes to it — this is the same risk ADR 0008
  already accepted for the `BE` country-level overlap, extended one level down, not a new class
  of risk.
- The Brussels `BE10` gap is not solved by this ADR; it is named so it cannot be silently guessed
  at implementation time. Until the maintainer decides, a NUTS 2 loader has no row to resolve
  `BE10` to and must render it `missing`, not skip validation for it.
- Adding the ~300-region `nuts2.csv` table and the compound-`OBS_FLAG` adapter fix
  (`docs/features/europe_nuts2.md`, "Assumptions") are each their own PR; this ADR covers only
  the geography-table shape decision, not the fetch or adapter work.

## Amendment 2026-09-14 (batch B2, the implementation batch)

Decisions 2 and 3 above are **superseded**. Building the real `nuts2.csv` against live Eurostat
data surfaced a reason not visible from the coverage report alone: **`BE10` (Brussels-Capital) is
not a synthetic alias at all — it is a real code Eurostat's own `geo` dimension returns**, with its
own real observations, for every one of the three candidate datasets (confirmed 2026-09-14 against
`nama_10r_2gdp`, `lfst_r_lfu3rt`, `demo_r_pjanaggr3`). Brussels has no *province*-level row in
`geographies.csv` to carry a NUTS 2 tag (decision 3's own point stands), but that gap is about
`geographies.csv`'s shape, not about whether `BE10` is a real region — it is, exactly as real as
`BE21`.

Given that, dual-registering Belgium's 10 provinces by reference (decision 2) while aliasing
Brussels separately (decision 3) would have treated one real, live-data-bearing NUTS 2 region
(`BE10`) differently from the other ten for no reason connected to the data itself — and would
still have left a live risk decision 2 accepted on purpose for the country pilot (`BE` sharing one
`geo_id` between two Eurostat granularities) but that this batch's own risk list (`docs/features/
europe_nuts2.md`, "Rollout / risks") flags as exactly the kind of mixing rule 3/25 exist to
prevent: a NUTS-built regional figure landing under the same `geo_id` a NIS-built Belgian province
aggregate uses.

**Revised decision, this batch:**

1. **Every NUTS 2 region gets its own `:nuts2` geo_id in `config/geography/nuts2.csv` — Belgium's
   11 included.** `nuts2.csv` gains a `belgian_geo_id` column, filled **only** for the 11 Belgian
   rows, as a pure cross-reference for tooling and humans — **never** consulted by the loader
   (`scripts/sync_nuts2.py`) to choose an observation's `geo_id`. BE21–BE25/BE31–BE35 resolve
   `belgian_geo_id` by reading `geographies.csv`'s existing `nuts` column (a lookup, never
   hand-typed); `BE10` resolves it to `be:reg:04000` (the Brussels region row) by `geo_id`, with a
   sanity check that row's own `nuts` column still reads `BE1` (NUTS 1) — if that ever stops being
   true, the loader's reference-row generation is expected to fail loudly rather than silently
   keep a stale cross-reference.
2. **`geographies.csv` is unchanged by this revision too** — decision 1's rule ("never touch
   Belgium's NIS-keyed table") still holds; the only change is that the 10 provinces' rows are no
   longer *reused* for NUTS 2 observations, they are *referenced* by a separate `nuts2.csv` row
   that has its own `:nuts2` geo_id.
3. **Decision 4 (the non-region aggregate exclusion list) and decision 5 (the 2024 geometry
   vintage) are unchanged**, and are exactly as effective against a `nuts2.csv` that includes
   Belgium as against one that does not.

This removes the "Belgium's provinces are dual-registered by reference" risk from Consequences
above entirely for the 10 provinces (there is no second `geo_id` pointing at the same row any
more — `be21:nuts2` and `be:prov:10000` are two independent, if correlated, geographies) and
answers `docs/features/europe_nuts2.md`'s decision 6 (the Brussels alias) with what the live data
showed: no alias was needed, only a catalogue row like any other region's.
