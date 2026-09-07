# Batch 9 — page-document schema

```
Batch: 9 (docs/features/page_builder.md)
Base commit: e7d28689 (Batch 2: shared presentation components, #103)
Final commit: <set at PR merge>
Files changed:
  docs/features/page_document.schema.json   (new, 285 lines -- the JSON Schema itself)
  src/pages/__init__.py                     (new, public API surface)
  src/pages/schema.py                       (new, 310 -- schema load + structural validation)
  src/pages/semantics.py                    (new, 866 -- the 13 semantic checks)
  src/pages/registry.py                     (new, 193 -- block registry loader)
  src/pages/metadata.py                     (new, 206 -- the indicator/NIS universe)
  src/pages/document.py                     (new, 103 -- the one entry point)
  src/pages/migrations.py                   (new, 135 -- schema_version forward migration)
  src/pages/serialize.py                    (new, 94 -- stable-ordering round trip)
  assets/belpulse/blocks/registry.json      (new, block type/version/props declarations)
  tests/pages/test_page_document_schema.py     (new, 272)
  tests/pages/test_page_document_semantics.py  (new, 612)
  tests/pages/test_page_document_roundtrip.py  (new, 129)
  tests/pages/test_page_document_registry.py   (new, 109)
  tests/pages/test_page_document_migrations.py (new, 67)
  tests/security/test_page_document_hardening.py (new, 180)
  tests/fixtures/pages/builders.py, real_data.py (new -- fixtures read ids from
    real published payloads, never hand-typed values, rule 36)
  docs/features/block_contract.md           (status line; two spec corrections, below)
  docs/features/data_binding.md             (aggregation_method correction)
  docs/features/page_builder.md             (batch table: deferral lifted)
  docs/implementation/known-risks.md        (new rows, incl. the deferred red-team review)
  docs/steps                                (Batch 9 marked done; Block WEB header note)
Requirements completed:
  - JSON Schema for a page document per docs/features/block_contract.md, with
    additionalProperties:false throughout, so an unrecognised key never gets
    as far as having its value examined.
  - All 13 semantic checks, each its own function and each its own error code:
    page_type, route, context, duplicate block ids, block type/version, props,
    binding presence, accessible name, grid position, grid overlap, NIS,
    binding, content safety.
  - Block registry (assets/belpulse/blocks/registry.json) as the single
    declaration of block types, versions and prop shapes -- no runtime plugin
    loading, per block_contract.md's own non-goal.
  - Forward migration by schema_version (CURRENT_SCHEMA_VERSION == 1, so the
    migration table is empty BY CONSTRUCTION rather than by omission -- the
    machinery and its tests exist now so version 2 is not a retrofit).
  - Stable-ordering serialisation: two saves of an unchanged document are
    byte-identical (claude.md rule 35).
Deferred requirements:
  - Binding RESOLUTION (opening a payload, computing a value) is Batch 14 and
    is deliberately absent: nothing in src/pages/ reads an observation.
  - security-red-team review of the validator is deferred to Batches 11 and
    14, recorded as a deliberate decision in known-risks.md rather than
    skipped -- this batch has no network surface and no resolution path, so a
    red-team pass now would be reviewing an attack surface that does not yet
    exist. It is a gate on 11 and 14, not an optional extra.
Data-contract impact: none. No exporter, migration, payload or DB schema
  touched. Verified statically: src/pages/ imports nothing from
  src/analytics/, src/fetchers/, src/geography/, the exporters, or sqlite3.
Commands executed:
  python3 -m pytest -q                     (923 passed, from a 754 baseline)
  python3 -m pytest tests/pages/ tests/security/ -q  (239 passed)
  pre-commit run --all-files               (ruff, black, large-files: pass)
Tests passed: 923/923. +169 over baseline, no pre-existing test modified.
Screenshots produced: none -- this batch renders nothing (Batch 10).
Performance results: validation of the realistic fixture is well inside the
  batch's own limit; the oversized/deeply-nested guards are tested as
  rejection cases, not as timing cases.
Reviewer findings:
  P1 (repository-architect, pre-implementation) -- THE INDICATOR UNIVERSE WAS
    WRONG IN THE SPEC. block_contract.md said to validate a binding's
    indicator against "the published metadata", which reads as
    metadata/indicators.json. That file is MUNICIPAL-ONLY: 52 indicators. The
    17 national ones (GDP, inflation, unemployment) live only inside
    national.json's own "indicators" key and appear in NO metadata index.
    Overlap between the two sets is zero. Independently verified by the lead
    and again by the maintainer's own check. A validator built on the first
    file alone would have rejected every national binding -- that is, the
    whole homepage and the whole macro page. src/pages/metadata.py now treats
    the universe as the UNION of both files and records which side a code came
    from, because they carry different fields.
  P1 (repository-architect) -- aggregation_method MUST NOT BE ENFORCED.
    block_contract.md and data_binding.md both told a future implementer to
    check it. It is a database column (migrations/001_core_schema.sql) that is
    not declarable in config/indicators/*.yaml, not published in any payload,
    and read by nothing: src/analytics/aggregate.py's entire live rule is
    "SUM if is_additive else REFUSE". Worse, 17 live indicators carry
    aggregation_method = 'population_weighted', the method ADR 0003 measured
    as wrong. Enforcing the column as written would have encoded a prohibited
    method against real indicators. Both spec files corrected; a test asserts
    the column is NOT read.
  P1 (repository-architect) -- THE FORMAT NEARLY SHIPPED MONOLINGUAL. The
    first spec draft never mentioned rule 7. Every user-facing string in a
    page document is now {en, fr, nl}.
  P1 (lead, process -- recorded against the lead, not the agents) -- TWO
    builder-core INSTANCES WROTE src/pages/ CONCURRENTLY. The lead reported
    implementation as "with the implementers" while nothing was yet on disk,
    then re-dispatched builder-core without first confirming the original had
    terminated. Both instances flagged the collision themselves and nothing
    was lost, but a schema settled by last-writer-wins is not a settled
    schema. The merged result was read end-to-end by the lead and re-verified
    by the maintainer (test count, forbidden imports, the union fix, the
    overlap fix) before this report. Process lesson: confirm an agent has
    terminated, and confirm its output on disk, before re-dispatching or
    reporting status.
  Design finding (lead, overruled the implementer) -- GRID OVERLAP WAS
    UNDER-REPORTED. The merged implementation flagged a clash only when two
    blocks overlapped at EVERY breakpoint, on the reasoning that a narrowing
    grid reflows. This format has no reflow engine: every breakpoint's x/y/w/h
    is hand-declared, so two blocks both authored at mobile (0,0,4,2) is an
    authored collision that was passing validation silently. Now checked and
    reported per (pair, breakpoint), with a test for mobile-only, tablet-only
    and desktop-only collisions. The realistic fixture was checked at all
    three breakpoints before tightening: it is genuinely clean, so nothing
    legitimate breaks.
  Minor (maintainer, at review) -- data_binding.md's open question asks that a
    `peer` provider be rejected "with a clear 'not available yet' message".
    It IS rejected -- `peer` is deliberately absent from the provider enum, so
    a document naming it fails rather than being silently ignored -- but the
    failure is a generic enum violation, not the bespoke message the spec
    asked for. Accepted as-is: rejection is the load-bearing half. Worth a
    tailored message when Peer Model v1 lands (Block M).
Known limitations:
  - Content safety is structural, not a keyword blocklist, and this is
    deliberate: allowlisted keys and types per props schema, no `<` or `>` in
    any string anywhere, no `on*` keys, site-relative URLs only, rich text as
    an allowlisted node set rather than a sanitiser over raw HTML. A
    SQL-keyword scanner was explicitly rejected -- a legitimate French or
    Dutch label containing "Union" or "ORDER" would trip it, and a validator
    that fires on real content gets switched off.
  - The allowed page_type set still follows block_contract.md's own open
    question and will be pinned by whatever Batch 15 actually converts.
  - Whether context.nis is the only templated context variable stays open
    until the municipality-profile template (Batch 4) is designed.
Rollback procedure: revert the PR. src/pages/ is new and imported by nothing
  outside its own tests; assets/belpulse/blocks/registry.json is loaded only
  by it. No published route, payload or build step reads any of it yet, so a
  revert has zero effect on the live site.
Next batch: 10 (shared block renderer) consumes this schema, and per
  block_contract.md's own rollout note this batch should be reviewed carefully
  before that starts. Batches 3/4/6/7/8 (visual reproduction) remain
  independently available and are not blocked by either.
```
