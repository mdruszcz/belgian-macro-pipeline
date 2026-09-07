# Batch 9 — page-document schema

```
Batch: 9 (docs/features/page_builder.md; spec docs/features/block_contract.md)
Base commit: e7d28689 (Batch 2: shared presentation components, #103)
Final commit: <set at PR merge>
Files changed:
  docs/features/page_document.schema.json        (new, 285 lines — the JSON Schema)
  assets/belpulse/blocks/registry.json           (new, 405 lines — block registry, data only)
  src/pages/__init__.py                          (new — closed public API, 10 names)
  src/pages/document.py                          (new — load/validate entry point, guard order)
  src/pages/schema.py                            (new — structural validation, limits, errors)
  src/pages/semantics.py                         (new, 865 lines — the rejection list)
  src/pages/registry.py                          (new — registry accessor)
  src/pages/metadata.py                          (new — the read-only metadata bundle)
  src/pages/serialize.py                         (new — canonical dumps/loads)
  src/pages/migrations.py                        (new — forward-only migrations)
  tests/pages/test_page_document_schema.py       (new)
  tests/pages/test_page_document_semantics.py    (new)
  tests/pages/test_page_document_roundtrip.py    (new)
  tests/pages/test_page_document_migrations.py   (new)
  tests/pages/test_page_document_registry.py     (new)
  tests/security/test_page_document_hardening.py (new)
  tests/fixtures/pages/real_data.py              (new — reads ids from real payloads, rule 36)
  tests/fixtures/pages/builders.py               (new — document builders)
  docs/features/block_contract.md                (status line; aggregation_method correction)
  docs/features/data_binding.md                  (aggregation_method correction)
  docs/features/page_builder.md                  (batch table: deferral lifted)
  docs/steps                                     (Block WEB header note; Batch 9 entry)
  docs/implementation/known-risks.md             (6 new rows)
  docs/implementation/batches/batch-0-baseline.md (asset table: 2 new browser-served files)
Requirements completed:
  - JSON Schema for a page document at schema_version 1, matching block_contract.md's
    own example. Three breakpoints exactly (12/8/4 columns); a fourth or a missing
    one is rejected. Grid coordinates are integers.
  - Block registry as DATA (registry_version 1, forward-compat clause in the file):
    6 types -- hero, kpi_card (v1 AND v2), chart, comparison_table, map, rich_text.
    Every props schema marked props_schema_status: "provisional". Batch 10 owns the
    renderer that consumes it; this batch does not render anything.
  - Semantic validator enforcing all 12 rejection classes in block_contract.md via a
    CLOSED 21-code vocabulary (frozenset; constructing an error with an unlisted code
    raises, so the vocabulary cannot drift).
  - Canonical serialization: sort_keys, indent=2, ensure_ascii=False, allow_nan=False,
    trailing newline. Byte-stable round trip (rule 35, invariant 9).
  - Forward-only migrations preserving unknown fields, plus a real kpi_card v1->v2
    block migration so the mechanism is exercised rather than merely present.
  - Resource-exhaustion guards in the required order, boundary-tested both sides.
Deferred requirements:
  - Document-level migration from a superseded schema_version. NOT deferred by choice:
    schema_version 1 is the first version that has ever existed, so DOCUMENT_MIGRATIONS
    is empty by construction. The framework is proven through the block-version ladder
    and through refusal of versions 0 and 2. There is nothing to migrate from yet.
  - Recomputability of non-additive indicators -> Batch 14 (see "Aggregation" below).
  - Independent security-red-team review -> Batches 11 and 14 (see "Reviewer findings").
Data-contract impact: none. No exporter, schema, adapter, payload or route touched.
  The validator READS three published payloads and one config CSV; it writes nothing
  and opens no database.
Commands executed:
  python3 -m pytest -q                     (923 passed — measured by the lead, twice)
  python3 -m pytest -q  (pre-batch baseline) (754 passed — measured before any change)
  pre-commit run --files <all 18 new files>  (trailing-whitespace, large-files,
                                              ruff-check, black: all Passed)
  Independent lead verification scripts exercising all 21 error codes, the guard
  boundaries, migrations and the performance budget against the REAL registry and
  the REAL published metadata (not fixtures). Output reproduced below.
Tests passed: 923/923 full suite. 169 new tests (754 -> 923). Zero pre-existing tests
  modified, weakened, skipped or xfailed.
Screenshots produced: none — this batch renders nothing.
Performance results:
  Validation of a 30-block / 5-section document (24,752 bytes): 12.56 ms per call,
  against a 50 ms budget. load_metadata() 3.5 ms, once per validator instance.
  New tests add ~4 s to the suite. Zero new runtime dependencies (jsonschema>=4.0
  was already in requirements.txt; pyproject.toml untouched).
  Two new browser-served files: registry.json 11.6 KB, page_document.schema.json
  10.6 KB (the schema is not fetched by the browser; recorded for completeness).
Reviewer findings: repository-architect pre-check RAN before implementation --
  8 findings, 3 of them P1, all 3 changed the spec. security-red-team deliberately
  deferred. Visual/data/accessibility not applicable. Full detail below.
Known limitations: see "Known limitations" below.
Rollback procedure: revert the PR. Every code file is new; no build target, exporter,
  route or published payload is touched, and no page loads any of it yet. Effect on
  the public site: none, by construction.
Next batch: 10 (shared block renderer) consumes assets/belpulse/blocks/registry.json.
  11 (builder service) consumes src/pages/'s validator. Both are unblocked.
```

## 1. What the architecture pre-check caught, before a line was written

`repository-architect` reviewed the batch specification before implementation, as required.
It returned eight findings; three were P1 and all three changed the spec. This is the whole
argument for the pre-check step, so it is recorded in full rather than summarised.

**P1.1 — the validator was about to be blind to every national indicator.**
`public/data/metadata/indicators.json` is *municipal-only* — its own exporter docstring says
"One row per municipal indicator." The 17 national indicators (`GDP_ANNUAL_CY`, `HICP`,
`UNEMPLOYMENT_RATE`, `BUSINESS_CONFIDENCE`, …) live only in `public/data/national.json` and
appear in **no** metadata index at all. Measured independently: 52 municipal, 17 national,
**zero overlap**, 69 total.

Had the validator trusted the metadata index as "the list of indicators", every national
binding would have been rejected as `unknown_indicator` — which is the entire homepage
(Batch 3) and the entire macro page (Batch 6). It would have been found in Batch 3, months of
batches later, as "the schema is broken", and fixing it then means a `schema_version` bump.

The indicator universe is therefore the **union** of both files, with the additivity check
applying to the municipal half only, and there is a named regression test for it.

**P1.2 — `block_contract.md` was instructing implementers to enforce a prohibited rule.**
Its rejection list said "an aggregation the indicator's `is_additive`/`aggregation_method`
forbids". Verified independently:

- `aggregation_method` is a database column (`migrations/001_core_schema.sql`), **not**
  declarable in `config/indicators/*.yaml` (that schema is `additionalProperties: false` with
  no such property), and published in **no** payload.
- `grep aggregation_method src/ scripts/export_*.py` returns **zero hits**. It is read by
  nothing. The entire live rule is one line in `src/analytics/aggregate.py`:
  `methods[indicator_id] = SUM if meta.get("is_additive") else REFUSE`.
- 17 live indicators carry `aggregation_method = 'population_weighted'` — the exact method
  ADR 0003 measured as wrong and says "must not be added."

So enforcing the column as written would have encoded a prohibited aggregation method against
17 real indicators. Both `block_contract.md` and `data_binding.md` were corrected in place
(the latter additionally claimed the exporters already apply such a check — they do not).

**P1.3 — the page format nearly shipped monolingual.** The first revision of this batch's spec
never mentioned claude.md rule 7 at all. `seo.title`, `seo.description` and every accessible
name would have been plain strings. Caught by review, not by a test or a rule check.
Retrofitting it after Batches 10-15 had written documents would have cost a `schema_version`
bump plus a migration for every page. Every user-facing string is now a `{en, fr, nl}` object,
all three required, `additionalProperties: false`, copying
`docs/features/indicator_config.schema.json`'s `name` shape exactly.

The five non-P1 findings — schema file placement, the `src/pages/` vs `src/validation/` split,
`pyproject.toml` needing no change, pytest basename uniqueness, and the three NIS traps in §3
below — were all adopted as written.

## 2. A P1 PROCESS failure: three agents wrote this schema concurrently

**This is a finding against the lead (me), not against any implementer, and it is the most
important thing in this report.**

Two implementers were dispatched in the background. Before they had finished, the working tree
was inspected, found empty, and reported as such — the agents were still running, not failed.
I then re-dispatched `builder-core` without first confirming the original had terminated. The
result: **two `builder-core` instances and one `test-engineer` wrote `src/pages/` in the same
window.** Both `builder-core` instances independently detected and reported the collision;
neither caused it.

Concretely, `semantics.py`, `serialize.py`, `registry.py`, `metadata.py`, `migrations.py` and
`registry.json` were each overwritten mid-session by a second writer. Three substantive changes
arrived that way and survived by last-writer-wins rather than by review:

1. A `javascript:`/`vbscript:` scheme rule and a `"://"` absolute-URL rule applied to *every*
   string in the document, not only to URL-shaped props. **Reviewed and kept** — they close a
   real hole (a remote URL in `seo.description` previously passed clean) and they are
   structural rather than keyword-based, so they do not violate §12's prohibition.
2. `props_schema_status` lifted to the block-type level as well as per-version. **Kept**,
   cosmetic.
3. The grid-overlap threshold rewritten from per-breakpoint to all-breakpoint. **Overruled by
   the lead** — see §4.

Nothing was lost and the merged result is coherent; I have read the final `src/pages/` myself
and verified its behaviour independently (§6). But a schema settled by overwrite is not a
settled schema, and this batch was specifically the one where getting it wrong is expensive.

**Rule for the rest of the programme:** never re-dispatch an agent whose prior instance has not
been confirmed terminated, and never infer "the agent failed" from an empty working tree while
it is still running. Before Batch 10 starts, someone should read `src/pages/` as a whole once
more, as a unit, rather than as a diff.

## 3. The three NIS traps, and the boundary this batch must not cross

`geographies.json` (622 entries) is the right source for "is this NIS code real", and
existence-checking a published list is *not* geography resolution — so rule 25 and invariant 4
are respected. Three traps, all flagged by the pre-check and all handled:

- **Wrong level.** All 622 entries carry a `nis_code`, so a naive membership test happily
  passes `01000` (country) or `02000` (region) for a municipal binding. The entry's `level` is
  checked against what the binding's provider implies → `geo_level_mismatch`.
- **Merged communes.** `geographies.json` is current-only. All 55 `old_nis` codes in
  `config/geography/municipality_crosswalk.csv` are absent from it (29 from the 2024/25 merger
  cycle). Rejecting them is correct, but the *message* matters: a legitimate historical code
  yields `retired_nis_code` with "merged in 2025", not `invalid_nis_code`. Otherwise Batch 15
  and the maintainer read a real historical code as a typo. The crosswalk is read for the
  message text only.
- **The hard boundary.** `src/pages/` must never derive a `geo_id` from a NIS, walk
  `parent_geo_id`, apply the crosswalk to substitute a successor, or reason about
  period-dependent validity — ten NIS codes have two rows in `config/geography/geographies.csv`
  with different validity windows, and only `resolve_geo()` knows that. Auto-substituting a
  successor would be geography resolution and would need an ADR. **This boundary is written
  into `src/pages/semantics.py`'s module docstring**, so the next agent to open the file reads
  it before deciding to be helpful.

## 4. The one design decision the lead overruled

The collision left grid-overlap detection reporting a clash only when two blocks overlapped at
**every** breakpoint where both were visible. Its docstring justified this as tolerating
"reflow" — coordinates converging as the grid narrows from 12 to 8 to 4 columns.

That reasoning describes an automatic reflow engine. **This document format does not have
one.** Every breakpoint's `x/y/w/h` is explicitly authored; nothing derives the mobile
rectangle from the desktop one. So two blocks both declared at mobile `(0,0,4,2)` is not a
reflow artefact, it is an authored collision — exactly as much a mistake as the same collision
on desktop. The practical effect of the loose threshold was that **a real mobile collision
passed validation silently**, on the one viewport least likely to be caught by a later visual
audit.

Checked before deciding rather than asserted: the realistic test fixture is genuinely
non-overlapping at all three breakpoints, so tightening broke nothing. Overlap is now reported
per `(pair, breakpoint)`, with the breakpoint in both the error path
(`sections/0/blocks/1/layout/mobile`) and the message, so a rejection says which viewport to
go and fix. `allow_overlap: true` still exempts a section, and a block hidden at a breakpoint
occupies nothing there — so hiding one of two stacked blocks on mobile remains a legitimate
resolution. Seven tests now hold this line.

## 5. Aggregation: what is enforced, and the trap that was avoided

Enforced: a binding whose operation implies a **SUM** across geographies against a municipal
indicator published `additive: false` → `forbidden_aggregation`. `sum` is the only aggregate
function the schema can express at all, so there is nothing else to decide.

**Deliberately NOT enforced, and this matters:** `additive: false` alone does not mean "no
aggregate exists". 18 of the 52 municipal indicators are non-additive, yet this pipeline
genuinely publishes province-level `AVG_NET_TAXABLE_INCOME` by *recomputing* it from summed
components. A blunt "non-additive ⇒ refuse every aggregate" rule would have rejected a figure
the live site already shows.

Deciding recomputability requires `src/analytics/aggregate.py`'s
`RECOMPUTABLE_FUNCTIONS = {mean_from_total, dependency_ratio, per_capita, share_of_total}`,
which is an analytical decision table. **It is not copied into `src/pages/`** — rule 19 and
invariant 4. Batch 14 must relax this check by *importing* the analytics answer, never by
restating it. There is a test asserting Batch 9 does not read `aggregation_method` and does not
reject the real province average.

## 6. Independent lead verification, against the real payloads

Not the implementers' test suite — separate scripts run by the lead against the real registry
and the real published metadata. Verbatim:

| Case | Result |
|---|---|
| `GDP_ANNUAL_CY` / `HICP` / `UNEMPLOYMENT_RATE` (national) | **accepted** — the P1.1 regression |
| `AVG_NET_TAXABLE_INCOME` @ 52018 (municipal) | accepted |
| `NOT_A_REAL_INDICATOR` | `unknown_indicator` |
| NIS `11007` (merged 2025) | `retired_nis_code` |
| NIS `99999` (nonexistent) | `invalid_nis_code` |
| NIS `01000` (country) as municipal | `geo_level_mismatch` |
| duplicate block id | `duplicate_block_id` |
| overlap at **mobile only** | `grid_overlap` at `sections/0/blocks/1/layout/mobile` |
| same document, `allow_overlap: true` | accepted |
| unknown block type / `kpi_card` v9 | `unknown_block_type` / `unsupported_block_version` |
| mobile `x=4` (4-column grid) / desktop `w=12.0` | `invalid_grid_position` (both) |
| `/`, `/about.html`, `/local/{nis}/`, `/local/{nis}/fr/` | accepted |
| `/../../etc/passwd`, `/wat.html` | `invalid_route` |
| `https://evil.example.com/` as route | `invalid_route` + `unsafe_content` |
| `<script>` in `seo.title` | `unsafe_content` |
| remote URL in `seo.description` | `unsafe_content` |
| `javascript:` in a prop | `unsafe_content` |
| `seo.title` as a bare string; a label missing `nl` | `schema_violation` (both) — rule 7 |
| `sum` over `AVG_NET_TAXABLE_INCOME` (`additive: false`) | `forbidden_aggregation` |
| `sum` over `ACTIVATION_MEASURES_PAID` (`additive: true`) | accepted |
| `chart` with no accessible name | `missing_accessible_name` |
| `page_type: evil` | `unknown_page_type` |

**The must-PASS half, which is what stops the validator being switched off.** These are
legitimate trilingual Belgian labels, and every one validates clean — proving the unsafe-content
check is structural and not the SQL-keyword scan the spec forbids:

    "Union européenne : sélection d'indicateurs"        accepted
    "ORDER van de Vlaamse Regering"                     accepted
    "Revenu médian par déclaration — Namur (Wallonie)"  accepted
    "Bevolkingsdichtheid per km² — Sint-Genesius-Rode"  accepted
    "Chômage : insertion, cœur de métier, naïveté"      accepted

**Guards, both sides of every boundary:**

    512 blocks    accepted  |  513 blocks    too_many_blocks
    64 sections   accepted  |  65 sections   too_many_sections
    8192-char     accepted  |  8193-char     string_too_long
    >1 MiB raw              →  document_too_large
    5000-deep array         →  too_deep  (RecursionError converted, not a crash)

**Migrations:** unknown top-level field preserved, unknown block field preserved, `migrate()`
non-mutating; `schema_version` 0 and 2 both `unsupported_schema_version`; `kpi_card` v1 → v2
rewrites `show_sparkline` → `sparkline` and defaults `show_provenance`, preserving the rest.

**Round trip:** `dumps(loads(dumps(x))) == dumps(x)` → True, sorted keys, trailing newline.
NaN rejected on the way out (`ValueError`) *and* on the way in (`PageValidationError`) — the
trap being closed here is that `json.dumps` defaults to `allow_nan=True` and emits bare `NaN`,
which Python's own `json.loads` accepts back, so a naive round-trip test passes while producing
a file no other parser can read.

## 7. Deliberate decisions a later batch will want to know about

- **Registry lives at `assets/belpulse/blocks/registry.json`, not `config/blocks/`.** `assets/`
  is already served from the Pages root, so Batch 10's renderer fetches the same file the
  validator reads, in the browser, with no build step and no second copy — rule 30 keeps the
  site static. `config/` would have needed a build-time copy into `public/`.
- **Anything with its own error code is enforced in Python, not in the JSON Schema.** A
  jsonschema failure can only ever surface as `schema_violation`, so a schema-level `enum` on
  `page_type` or bounds on grid coordinates would have made the specific codes unreachable.
- **`peer` is absent from the provider enum entirely** — not present-but-disabled. Peer Model v1
  (Block M) does not exist. It fails as `schema_violation` naming the enum. The known-risks row
  asking for a clean "not available yet" rejection is satisfied in substance, though the message
  is a schema message rather than a bespoke one.
- **`revision` is required** at schema_version 1 (one implementer's report claimed it optional;
  the stricter behaviour is what shipped and is what Batch 16's publication transaction wants).
- **`context` is `additionalProperties: false` with only `nis`.** Batch 4's municipality
  template will need a schema change to add anything. A closed door on purpose — but it is a
  door, and block_contract.md's own open question about `context` is still open.
- **Blocks deliberately absent from the registry**, so Batch 10 knows these were choices:
  `ranking_list` (needs Peer Model / Fiscal Score, not built), `news_card` (no content source),
  `simulated_live_counter` (Batch 2 made it throw without `data-simulated="true"`; its contract
  is Batch 10's), the page shells and sidebars.
- **`page_type`'s eight values are provisional** until Batch 15 actually converts pages, exactly
  as block_contract.md's open question says. Adding one is a one-line change with no schema bump.
- **Serialization deliberately differs from the exporters.** Page documents are git-committed,
  human-diffed source: `indent=2`, `sort_keys=True`, trailing newline. The exporters' payloads
  are minified machine output: `separators=(",", ":")`, no `sort_keys`. Neither should be
  "fixed" to match the other; `serialize.py`'s docstring says so, because a later tidy-up that
  aligned them would break invariant 9.

## 8. Known limitations

- **Document-level migration is untested against a real superseded version**, because none
  exists. The first genuine `schema_version` bump is the first real exercise of that path.
- **The `on*` prop-key ban is a prefix match**, so a future legitimate prop named `onset_year`
  would be rejected. Confined to props subtrees. Worth knowing before Batch 10 names props.
- **`<`, `>` and `://` are banned in every string**, including `seo.title`/`seo.description`.
  Correct per invariant 5 and rule 23, but Batch 15 should expect to hit it when converting a
  page whose prose quotes a source URL.
- **`dumps()` raises the stdlib `ValueError` on a non-finite float**, not a
  `PageValidationError` — deliberate (a NaN is a bug in whatever built the document, not a
  finding about a user's page), but it is an inconsistency in the exception surface.
- **No independent adversarial review yet.** See below.
- **The seven data states are a "nothing precludes it" property, not an asserted one.**
  Verified against `assets/belpulse/components.css`'s own `data-state` values (`loading`,
  `missing`, `suppressed`, `unavailable`, `error`, plus `ready`/explicit-zero carrying no
  styling by design). The document format encodes no state at all — state is runtime — so
  nothing forces a block into a value-or-nothing binary. There is no single schema constraint
  to assert, so this is recorded rather than tested.

## 9. Audits: what ran, and what deliberately did not

**Ran:** `repository-architect` pre-implementation dependency/regression check — 8 findings,
3 P1, all 3 adopted (§1). Lead verification against real payloads (§6). The batch's own
hardening suite, `tests/security/test_page_document_hardening.py`.

**Deliberately deferred, not omitted: `security-red-team`.** The security-sensitive surface per
`data_binding.md` is binding *resolution* (Batch 14) and the builder HTTP API (Batch 11).
Neither exists yet, so a red-team pass now would be attacking a function with no caller and no
transport. The judgement is that the pass is worth more when there is an attack surface to
point it at.

**This is a recorded commitment, not a dismissal:** `docs/implementation/known-risks.md` now
carries a row requiring `security-red-team` to review `src/pages/` as part of **both** Batch 11
and Batch 14 acceptance — not merely the new code those batches add. If that row is ever closed
without such a review having run, this batch was accepted on a promise that was not kept.

**Not applicable:** visual, data and accessibility audits. This batch renders nothing, displays
no figure, and has no user-facing surface. Recording "no findings" from a visual audit that
could not have run would be exactly the "unaudited is not clean" confusion this programme's
reporting rules exist to prevent.
