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
is everything operational: the actual rows (565 communes, 43 arrondissements, 10 provinces, 3
regions), the merger crosswalk, and the resolution function itself.

## Goal

- Every level of the official Belgian administrative hierarchy — country, region, province,
  arrondissement, municipality — is loaded into `geographies` with correct `parent_geo_id`
  chains, sourced from an official Statbel file with a recorded download date.
- Every current commune resolves to `be:country` by walking `parent_geo_id`, with no orphans and
  no cycles (tested).
- `resolve_geo(nis_code, period)` exists, is used by every adapter, and returns the entity that
  was valid for that NIS code at that period — including pre-merger entities for historical data.
- A `municipality_crosswalk.csv` exists, covering the 2019 and 2025 merger waves, with every
  ambiguous row flagged for manual verification against official merger lists (an `[H]` step).
- Every user-facing name exists in NL, FR and EN (`CLAUDE.md` rule 7), with no fabricated
  translations — see "English names" below.
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

All codes in REFNIS are **five digits, zero-padded** — including the ones commonly written
shorter elsewhere. Verified against the real files, not recalled:

- **Municipality**: e.g. `11002` (Antwerpen). Unique among *currently active* communes, but a
  code can be **reassigned** after a merger deregisters it — this is exactly why `geographies`
  uniques `(nis_code, valid_from)` rather than `nis_code` alone (see `data_model.md` §2).
- **Arrondissement**: distinct numbering space from municipalities (e.g. `11000` for
  Arrondissement Antwerpen). Purely administrative/judicial. There are 43.
- **Province**: e.g. `10000` Antwerpen. Ten of them; Brussels-Capital has none.
  Note `20001` (Vlaams-Brabant) and `20002` (Brabant Wallon) break the `X0000` pattern, so
  province membership must never be inferred from the digits.
- **Region**: `02000` Flemish, `03000` Walloon, `04000` Brussels-Capital — **five** digits with a
  leading zero, not the four-digit forms (`2000`) used in some other sources.
- **Country**: `01000` (ROYAUME / HET RIJK). The `be:country` row carries this as its
  `nis_code`, so `resolve_geo('01000', …)` works; its `geo_id` stays `be:country`.

`geo_id` convention (already used by `be:country`, extended here):
`be:mun:<nis>`, `be:arr:<nis>`, `be:prov:<nis>`, `be:reg:<nis>`, `be:country`. Non-Belgian
aggregates (already present via existing indicators like `EUROSTAT_GDP_Q_MEUR_DE`) get
`eu_aggregate`-level rows once/if they need geography rows of their own — out of scope for this
block, whose subject is Belgium's own hierarchy.

### Source files

Downloaded by the maintainer from Statbel's open-data portal and kept under
`data/raw/statbel/`, which is gitignored (`/data/raw/**`) — they are multi-megabyte
spreadsheets, and committing them would make every refresh an unreviewable binary diff. The
pipeline consumes the derived CSVs in `config/geography/` instead; see "Derivation" below.

| File | Role |
|---|---|
| `Nis9_Nis6_refnis_names_01012026.xlsx` | **Primary.** 20,781 statistical-sector rows × 30 columns. Every row carries the *explicit* full hierarchy — sector → NIS6 sub-municipality → commune → arrondissement → province → region → country — with NL/FR/DE names and NUTS1/2/3 codes. All parent links come from here. |
| `REFNIS_DEFINITIEF.csv` | Pre-2019 vintage, 589 communes. |
| `REFNIS_2019.csv` | Post-2019 merger wave, 581 communes. |
| `REFNIS_2025.csv` | Post-2025 merger wave, 565 communes. Also the source of the per-commune **language regime** (`N`/`F`/`D`/`FN`), which drives `name_en`. |
| `Conversion Postal code_Refnis code_va01012025.xlsx` | Postal code → commune. Retained but **not** currently derived — no consumer until Block K's search. |

The REFNIS vintages are all UTF-8-with-BOM and CRLF, but **disagree on the delimiter**: 2025 is
pipe-delimited, 2019 and DEFINITIEF are semicolon-delimited. The parser detects it per file
rather than assuming.

Licence terms must be recorded in `docs/data_catalog.md` and confirmed by the maintainer before
this data is republished — `CLAUDE.md` rule 8. This spec deliberately does not assert licence
text an agent cannot verify.

### Derivation and loading

Two stages, deliberately separated:

1. **`scripts/derive_geography_csv.py`** — a *manual refresh* step, not part of the daily
   workflow. Reads the raw files above and writes small, diffable, committed CSVs:
   `config/geography/geographies.csv` (622 rows) and `municipality_crosswalk.csv` (55 rows).
   A reviewer can then see in a PR diff exactly which communes changed, instead of "a 2.7 MB
   binary changed".
2. **`scripts/load_geography.py --db …`** — reads only those CSVs, so a fresh clone runs the
   whole pipeline with no downloads. Rows are written parent-level-first (country → region →
   province → arrondissement → municipality) because the foreign key rejects a child whose
   parent does not yet exist. `be:country` is **upserted, never replaced** — every existing
   observation references it.

`parent_geo_id` comes from the NIS9 file's explicit parent columns, never from slicing digits off
a code. Brussels' communes have an empty province column in the source, so their arrondissement
(`21000`) parents straight to the region (`04000`) — handled explicitly rather than by a
null-coalescing accident that would silently reparent 19 communes.

Historical predecessor communes are loaded with `valid_to` and `successor_geo_id` set but
**`parent_geo_id` NULL**: the arrondissement they belonged to at the time is not recoverable from
these files, and inventing one would attach historical figures to a possibly-wrong province —
the Hasselt/Kortessem merger crossed an arrondissement boundary, so this is not hypothetical.
Hierarchy assertions are therefore scoped to currently-valid communes.

### Merger handling

Fully specified already in `data_model.md` Review §1 (versioning via `valid_to` +
`successor_geo_id`, `is_additive`-gated back-aggregation, no fabricated pre-merger history). This
spec adds only the operational piece that document deferred: `municipality_crosswalk.csv`.

**`municipality_crosswalk.csv`** — columns `old_nis, old_name_nl, old_name_fr, new_nis,
relationship, has_partial_transfer, valid_from, valid_to, evidence, verified, note`. 55 rows,
derived from official data rather than authored by hand, but still reviewed by a human (the
`[H]` step below) because a wrong row here produces a *plausible* wrong number silently.

`relationship` is one of:

| Value | Meaning | Example |
|---|---|---|
| `merged` | The successor took in more than one predecessor. | `46003` Beveren + `46013` Kruibeke + `11056` Zwijndrecht → `46030` |
| `absorbed` | Sole predecessor, different name — taken into an existing commune. | `11007` Borsbeek → `11002` Antwerpen |
| `recoded` | Sole predecessor, same name — the entity survived, only its code changed (the 2019 Hainaut arrondissement reform). | `55022` La Louvière → `58001` La Louvière |

`has_partial_transfer` is a **separate boolean**, not a fourth relationship value. A boundary
transfer (Statbel marks these `PARTIE DE …`, `MODIFICATION DE LIMITE`, or a trailing `*`) is
orthogonal to what happened to the commune itself: La Louvière was *recoded* and separately
received part of Familleureux. Folding that into `relationship` would overwrite the useful fact
with the incidental one.

#### Two methods, and why one is not enough

- **Vintage diff — authoritative for *whether* and *when*.** A commune in REFNIS vintage *N* and
  absent from *N+1* disappeared at that boundary. Direct documentary evidence: 26 communes end in
  the 2019 wave, 29 in the 2025 wave. Arrondissement `54000` (Mouscron) is replaced by `58000`
  (La Louvière) in the 2019 file, which dates the Hainaut reform to the same 2019-01-01.
- **NIS6 prefix rule — corroborating, and supplies *where to*.** In the NIS9 workbook, a
  sub-municipality code whose first five digits differ from its commune code preserves the
  *former* commune's NIS5. A diff alone cannot do this — it reports what vanished and what
  appeared, not which maps to which.

**The prefix rule alone is not sufficient, and this was found empirically, not assumed.** It
finds 53 of the 55 predecessors. It misses `73009` (Borgloon) and `73083` (Tongeren), whose
sub-municipality codes were *renumbered* under the new commune `73111` (Tongeren-Borgloon)
instead of preserving the old codes. Building the crosswalk on the prefix rule alone would have
silently orphaned both communes' entire pre-2025 history — precisely the failure the `[RED]`
step below exists to find. `tests/test_geography_crosswalk.py` carries this as a regression test.

Where the two methods disagree, or where the successor comes from the weaker name-matching
fallback, the row is **flagged in the `note` column and `verified` stays `false`**. Seven rows
are currently flagged (the two above, plus five partial transfers). CONTROL C must not be claimed
while any remain unresolved.

`valid_from` for a predecessor is the first vintage containing it; `valid_to` is the wave that
ended it. They must differ — `geographies` enforces `CHECK (valid_to > valid_from)`.

The one date not evidenced by these files is `1977-01-01`, the Belgian municipal merger that
created the modern commune structure, used as `valid_from` for everything present in the oldest
vintage. It is recorded as an assumption, not a derivation.

### Resolution: `resolve_geo(nis, period)`

Implemented in `src/geography/resolve.py`. **No separate crosswalk table exists at runtime** —
the crosswalk CSV is the *source*, and loading it sets `valid_to` + `successor_geo_id` on the
predecessor rows, so resolution works entirely off `geographies` using the schema exactly as
`data_model.md` intended:

```python
def resolve_geo(conn, nis: str, period: str) -> str:   # returns geo_id
    as_of = period_to_date(period)   # first day of the period; shape-based
    row = conn.execute(
        "SELECT geo_id FROM geographies "
        "WHERE nis_code = ? AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)",
        (nis, as_of, as_of),
    ).fetchone()
    if row:
        return row[0]
    raise UnknownGeographyError(...)   # unknown code, or known but not valid then
```

Walking *forward* to the entity that exists today is a separate, explicit call —
`resolve_to_current(conn, geo_id)` — so that resolution never silently substitutes a successor
for the entity actually asked about. It loops rather than reading a single hop, because lineage
chains (a commune merged twice), with a cycle guard.

`period_to_date` is shape-based (`YYYY`, `YYYY-Qn`, `YYYY-MM`, `YYYY-MM-DD`), since `resolve_geo`
receives no frequency argument. It overlaps `derive_period_bounds` in
`scripts/port_existing_indicators.py`, which *does* take a frequency; consolidating the two is
noted follow-up work rather than a refactor smuggled into this change.

Two things this must get right, both already implied by `data_model.md`'s uniqueness constraint
but stated explicitly since this is where a bug would actually manifest:
- A single `nis_code` can match multiple rows across time (reassignment after a merger); the
  `valid_from`/`valid_to` window, not the code alone, disambiguates.
- Unknown codes **raise**, they do not silently map to `NULL` or get skipped — matching
  `CLAUDE.md` rule 13 ("fail loudly, never silently coerce or drop rows").

### English names

`geographies.name_en` is NOT NULL, and **no source file supplies English** — Statbel gives NL, FR
and DE only. Inventing English names for 565 communes would be fabricating user-facing text, so:

1. **Default to the entity's own official-language name**, taken from `REFNIS_2025.csv`'s
   language-regime column: `N` → Dutch, `F` → French, `D` → German, `FN` (Brussels' 19 bilingual
   communes) → French. A Walloon commune therefore reads as `Charleroi`, not `Charleroi` via a
   Dutch column, and a Flemish one as `Gent` rather than `Gand`.
2. **Override from `config/geography/name_en_exonyms.csv`** — a hand-written, committed file of
   ~20 entities that English genuinely renames: Belgium, Flanders, Wallonia, Brussels-Capital
   Region, the ten provinces, and the handful of communes with real exonyms (Brussels, Antwerp,
   Ghent, Bruges, Ostend, Ypres). Every NIS code in it was checked against the official file.

Keeping the override list small and separate is the point: it is user-facing text, so the
maintainer reviews ~20 rows rather than 565. Historical predecessor communes get their Dutch name
as `name_en` — no exonym applies to a commune that no longer exists, and the honest placeholder
beats a fabricated translation.

### Geometry

Not loaded as part of `geographies` rows. Either a `geometries` table keyed by a `geometry_id`
foreign key, or static per-level GeoJSON files under `data/geometry/` keyed by `nis_code` —
whichever is cheaper is decided when Block X (maps) actually needs it, per the roadmap's own
ordering. Recorded here only so nobody accidentally inlines boundary polygons into `geographies`
or into an export payload before then.

## Data / schema changes

None. `geographies` (`001_core_schema.sql`) already has every column this block needs
(`parent_geo_id`, `valid_from`/`valid_to`, `successor_geo_id`, `population`, `area_km2`). This
block populates it and adds the derived CSVs plus the `resolve_geo()` function — **no migration
required**.

`population` and `area_km2` stay NULL: no source file here carries them. Population arrives with
Block F demography, and it is the weight `data_model.md` needs for `population_weighted`
aggregation — so that aggregation stays unavailable at municipal level until then, rather than
silently falling back to an unweighted mean.

**NUTS1/2/3 codes** are available in the NIS9 file and are captured in
`config/geography/geographies.csv`, but are **not loaded** into `geographies` — there is no
column, adding one needs migration 003, and nothing consumes them yet. They are carried in the
CSV so that adding them later does not mean re-parsing a 2.7 MB spreadsheet. This is the natural
join key to the Eurostat series the pipeline already fetches, so the column is likely worth
adding when a consumer exists.

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

- **~~Q1 — Exact source file identity.~~ Resolved.** The maintainer downloaded the files; they
  are named and characterized in "Source files" above.
- **Q2 — Statistical sectors.** Still deferred (see Non-goals). The 20,781 sector rows are parsed
  on every refresh anyway, since they carry the hierarchy, so loading them later is a small
  change — but nothing consumes sub-commune geography before Phase II, and 20,781 unused rows in
  `geographies` would be scope creep.
- **~~Q3 — Brussels-Capital's missing province level.~~ Confirmed and handled.** The source's
  province column is empty for all 19 Brussels communes; their arrondissement parents directly to
  the region. Covered by `test_brussels_communes_parent_through_arrondissement_to_region`.
- **Q4 — `1977-01-01` as `valid_from`.** The oldest vintage available is pre-2019, so nothing in
  these files evidences when the communes in it began. 1 January 1977 (the great merger of Belgian
  municipalities) is used as the structural epoch. It is right for the large majority but will be
  wrong for any commune altered between 1977 and the DEFINITIEF vintage. It matters only if
  pre-2019 municipal data is ever loaded, which nothing currently does.
- **Q5 — Historical predecessors have no parent.** Consequence: they cannot be aggregated to
  province or region for pre-merger periods. Fixing it properly needs each old vintage's
  hierarchy, which REFNIS encodes only as document order. Deferred until a consumer exists.
- **Q6 — Partial boundary transfers.** Five rows carry `has_partial_transfer = true`. A partial
  transfer cannot be expressed as a 1:1 lineage, so the affected cells' history is a judgement
  call the maintainer must make rather than the loader assuming one.

## Rollout / risks

- Wrong or hallucinated NIS codes are the single most damaging failure mode this feature can
  introduce — a mismapped commune silently attaches real numbers to the wrong place. This is
  exactly why the source file must be official (not scraped or LLM-recalled) and why the `[RED]`
  step in `docs/steps` ("find every way a NIS code could be silently mismapped") follows
  immediately after the build steps, before CONTROL C.
- Crosswalk errors are the second-highest risk and the hardest to catch automatically — a stray
  discontinuity in a merged commune's population chart is the actual test (CONTROL C's own `[H]`
  step), not a unit test against the crosswalk's own logic.
