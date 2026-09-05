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
   `config/geography/geographies.csv` (688 rows — 622 open windows plus 66 closed) and
   `municipality_crosswalk.csv` (55 rows).
   A reviewer can then see in a PR diff exactly which communes changed, instead of "a 2.7 MB
   binary changed".
2. **`scripts/load_geography.py --db …`** — reads only those CSVs, so a fresh clone runs the
   whole pipeline with no downloads. Rows are written parent-level-first (country → region →
   province → arrondissement → municipality) because the foreign key rejects a child whose
   parent does not yet exist. `be:country` is **upserted, never replaced** — every existing
   observation references it.

`parent_geo_id` for current entities comes from the NIS9 file's explicit parent columns, and for
historical ones from the REFNIS vintage that recorded them — never from slicing digits off a
code. Brussels' communes have an empty province column in the source, so their arrondissement
(`21000`) parents straight to the region (`04000`) — handled explicitly rather than by a
null-coalescing accident that would silently reparent 19 communes.

### Validity windows, at every level

Each row is one *validity window* of one entity, not one entity. 622 windows are open today; 66
are closed — the 55 communes ended by the merger waves, plus the aggregates whose territory
changed.

**Communes are not the only things that change**, and an earlier version of this design assumed
they were. It diffed only commune rows, so every arrondissement, province and region fell back to
the structural epoch. Two silent mismappings followed, both caught by the Block C `[RED]` audit:

- `resolve_geo('58000', '2015')` returned Arrondissement La Louvière, **created by the 2019
  Hainaut reform**. It now raises.
- `resolve_geo('57000', '2010')` returned "Tournai-Mouscron". Code `57000` meant *Tournai alone*
  (10 communes) before 2019 and Tournai-Mouscron (12) after — the same code, a larger territory.
  2015 Tournai figures were being attributed to an area two communes bigger. It now returns the
  Tournai-only window.

A window closes and a new one opens when an entity disappears, is renamed, or changes territory.
"Territory" is compared as the entity's **transitive set of communes, each mapped through the
merger crosswalk** — not its direct children. Both refinements were needed:

- Rolling down to communes stops Province Hainaut splitting in 2019, when its *arrondissements*
  were renumbered but it covered exactly the same ground.
- Canonicalizing through the crosswalk stops Arrondissement Sint-Niklaas splitting in 2025, when
  Beveren and Kruibeke merged *inside* it.

Name comparison normalizes apostrophes: Statbel writes `Arrondissement d'Anvers` with a straight
quote in one vintage and a typographic one in another, which would otherwise split a window on
punctuation.

`geo_id` is the primary key, so a code with several windows needs one id per window: the current
window keeps the plain id (`be:arr:57000`) and superseded ones are suffixed with the date they
began (`be:arr:57000@1977-01-01`). The suffix appears **only** where a code genuinely has more
than one window — a commune that simply ceased to exist keeps its plain id, since that is what
any stored observation about it would carry.

### Historical parents

Historical communes carry the parent they actually had, recovered by
`parse_refnis_hierarchy()` from the vintage that recorded them. REFNIS has no parent column, but
it is a nested outline, so reading it as a state machine reconstructs the hierarchy of any
vintage. Validated against the independently-derived NIS9 hierarchy for 2025: **622 entities,
zero level or parent mismatches.**

This replaced an earlier decision to leave `parent_geo_id` NULL, which was wrong in a way worth
recording: it silently removed 55 communes from every pre-2025 province and region aggregate —
a 2015 "Province of Limburg" total was missing Hasselt, with no error raised anywhere. Nothing
distinguished "root" from "unknown parent", which is exactly the shape of failure this feature is
supposed to prevent.

It also matters *which* parent: Kortessem sat in arrondissement `73000` and was merged into
Hasselt, which is in `71000`. Borrowing the successor's parent would file Kortessem's history
under the wrong arrondissement — plausible, and wrong.

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

`has_partial_transfer` is a **separate, informational boolean** — not a fourth relationship
value and not a review gate. Statbel marks these with `PARTIE DE …`, `MODIFICATION DE LIMITE`, or
a trailing `*`, and they are orthogonal to what happened to the commune: La Louvière was
*recoded* and separately received part of Familleureux.

It does not gate the review, and the reason is worth recording. Roughly **200 of these markers
exist nationwide** — they annotate the **1977** merger that created the modern communes, not the
2019 or 2025 waves. Gating on them flagged five rows (Seneffe, La Louvière, Silly, Bastogne,
Bertogne) purely because their codes happen to appear in this crosswalk, asking the maintainer to
verify lineages that were never in doubt. A gate that cries wolf gets ignored.

The obvious narrowing — flag only when the marked land ended up somewhere other than the row's
successor — turns out to be *unable to fire*: the prefix rule derives successors from exactly
those NIS6 rows, so territory that went elsewhere is already in the successor set and the row is
already flagged as split. The marker carries no signal the multi-successor check does not.

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
fallback, the row is **flagged in the `note` column and `verified` stays `false`**. Two rows
are currently flagged — Borgloon and Tongeren, whose successor came from name matching — and
**the loader refuses to run while any remain unsigned** — `scripts/load_geography.py` raises `UnverifiedCrosswalkError`
unless `verified` is `true` on every flagged row, or `--allow-unverified` is passed explicitly.
The audit found this gate missing: the loader read none of the flag columns and wrote exactly the
confident successor links the flags exist to question. CONTROL C must not be claimed while any
remain unresolved.

### Effective dates are not always the vintage boundary

A vintage diff can only date a change to the snapshot that first shows it, and every REFNIS file
is cut on 1 January. Belgian mergers usually take effect then — **but not always**.

Checking the derived crosswalk against Statbel's published merger table found exactly one
exception: **Bastogne + Bertogne merged on 2 December 2024**, a month before `REFNIS_2025.csv`
was cut. The diff therefore dated it 2025-01-01, and every lookup in that December window
resolved to the pre-merger communes.

`config/geography/merger_effective_dates.csv` records official dates where they differ from the
vintage boundary, cited to their source, and the derivation applies them over both the crosswalk
and the hierarchy windows. It is small, hand-checked and committed, in the same spirit as
`name_en_exonyms.csv`. The override is applied *after* sign-offs are matched, so correcting a
date does not read as a changed claim and reset the maintainer's verification.

All 14 lineages of the 2025 cycle were confirmed against that table — see "Verification status".

**A note on mid-period boundaries.** `resolve_geo` resolves a period as of its **first day**, so
December 2024 — which begins one day before the Bastogne merger — resolves to the predecessors.
Monthly data for that month straddles the change. The convention is deliberate and documented
rather than silently averaged; anything finer would require knowing how a source apportioned a
part-month, which no source states.

**A sign-off survives regeneration.** `derive_geography_csv.py` reads the committed crosswalk
before overwriting it and carries `verified=true` forward, keyed on the claim that was actually
checked — `(old_nis, new_nis, valid_to)`. A refresh that changes the successor or the wave resets
it, because that is a different claim from the one that was verified. Without this, every refresh
silently discarded the maintainer's work, which is the same failure as not having the gate.

The evidence travels in its own `verified_source` column, **not** in `note`: `note` is
regenerated on every derivation, so evidence recorded there would be silently overwritten. A
`verified=true` with no stated source is indistinguishable from one set by accident, and
`test_every_verified_row_cites_its_evidence` enforces that.

### Verification status

Checked against Statbel's official merger table (maintainer-supplied, 2026-09-05):

- **All 14 successors of the 2025 cycle match exactly** — same predecessors, same successor
  codes, no extras on either side. This includes `73111 ← 73009 Borgloon + 73083 Tongeren`, the
  pair the NIS6 prefix rule could not derive and the name-matching fallback recovered. Both are
  now signed off with their source recorded.
- **One date correction**, Bastogne/Bertogne, described above.
- The **2019 wave is not covered by that table** and its 26 rows remain evidenced only by the
  vintage diff. They are unflagged because their lineage is unambiguous from code prefixes, but
  they have not been checked against a published list.

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

`tests/test_geography_{parse,crosswalk,load}.py`, 42 tests.

- `resolve_geo`: current commune resolves; a pre-merger commune resolves to the historical entity
  for a pre-merger period; unknown code and out-of-window both raise `UnknownGeographyError`.
- Exact row counts per level, asserted as constants (per docs/steps' rationale: a hardcoded count
  is what catches a loader silently dropping a province).
- Every current commune's `parent_geo_id` chain terminates at `be:country` — recursive CTE, zero
  orphans, zero cycles — and zero orphans at 2015, 2020 and 2026.
- Parsers run against a synthetic `.xlsx` the test writes itself, since the real 2.7 MB workbook
  is gitignored and CI cannot depend on it.

Every audit finding has a named regression test, so each is pinned by the scenario that produced
it rather than by a general assertion:
`test_historical_communes_are_not_orphaned_from_the_hierarchy`,
`test_aggregates_do_not_resolve_before_they_existed`,
`test_arrondissement_that_changed_territory_has_separate_windows`,
`test_loader_refuses_unverified_crosswalk_rows`,
`test_resolve_to_current_refuses_a_dead_end`,
`test_loader_rejects_a_blank_valid_from`,
`test_german_speaking_communes_keep_their_own_name`,
`test_vintage_diff_is_authoritative_where_the_prefix_rule_is_blind`,
`test_internal_merger_does_not_split_the_parent_window`,
`test_name_comparison_ignores_apostrophe_style`.

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
- **~~Q5 — Historical predecessors have no parent.~~ Fixed.** Each old vintage's hierarchy *is*
  recoverable from REFNIS's document order; see "Historical parents" above. Zero orphans at any
  era, asserted by `test_historical_communes_are_not_orphaned_from_the_hierarchy`.
- **Q8 — 2019 wave dates are unverified.** The maintainer's merger table covers the 2025 cycle
  only. If any 2019 merger took effect on a date other than 1 January 2019, it carries the same
  error the Bastogne row did, and nothing here would detect it.
- **Q7 — Canonicalization blurs merged entities.** Comparing territory through the crosswalk
  means two communes that merged become one identity, so an arrondissement that *gained* a merged
  commune's land may not register as changed (arrondissement `71000` gaining Kortessem's territory
  in 2025 is absorbed into Hasselt's canonical identity). The alternative — comparing raw commune
  codes — produced false splits everywhere. Observations attach to communes, not arrondissements,
  so this is a limitation of aggregate-level history rather than of the data itself.
- **~~Q6 — Partial boundary transfers.~~ Resolved.** The markers describe the 1977 merger, and
  any territory that actually left is already caught as a multi-successor row. The column is
  informational; it no longer gates the review. Five rows carry it.

## Rollout / risks

- Wrong or hallucinated NIS codes are the single most damaging failure mode this feature can
  introduce — a mismapped commune silently attaches real numbers to the wrong place. This is
  exactly why the source file must be official (not scraped or LLM-recalled) and why the `[RED]`
  step in `docs/steps` ("find every way a NIS code could be silently mismapped") follows
  immediately after the build steps, before CONTROL C.
- Crosswalk errors are the second-highest risk and the hardest to catch automatically — a stray
  discontinuity in a merged commune's population chart is the actual test (CONTROL C's own `[H]`
  step), not a unit test against the crosswalk's own logic.
