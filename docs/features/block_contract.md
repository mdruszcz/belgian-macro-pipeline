# Feature: page-document schema and block contract

Status: Batch 9 IMPLEMENTED (2026-09-07) — the page document, its validator, the block registry
and forward migrations are live and tested. Batch 10 (the shared renderer that consumes the
registry) is not started. The "Semantic validation" list below is fully enforced, with one
bullet corrected during implementation (see the `aggregation_method` note) and one open
question resolved (`page_type`'s set is enumerated but provisional until Batch 15).
Full report: docs/implementation/batches/batch-9-page-document-schema.md
Issue: none (part of docs/features/page_builder.md, Batches 9-10)
Branch: feat/page-document-schema
Implementation: `docs/features/page_document.schema.json` (the schema),
`assets/belpulse/blocks/registry.json` (the registry), `src/pages/` (validator, serializer,
migrations). Report: `docs/implementation/batches/batch-9-page-document-schema.md`.

## Problem

A page assembled in the builder and a page rendered on the public site must be the exact same
tree of blocks, or "what you see while editing" stops meaning anything. That requires one
document format (validated, versioned) and one renderer that both the builder preview and the
public build call.

## Goal

- A JSON Schema for a page document that a malicious or malformed draft cannot pass.
- One block registry, consumed identically by the editor canvas and the public renderer.
- Every block declares its own required data shape; the renderer never guesses.

## Non-goals

- Runtime plugin loading of new block types from outside `assets/belpulse/blocks/`. New block
  types are added by editing the registry and going through review, exactly like a new indicator
  config.
- A generic "container" block that accepts arbitrary child HTML. See claude.md rule 22.

## Proposed approach

### Page document

```jsonc
{
  "schema_version": 1,
  "page_id": "homepage",
  "revision": 4,
  "route": "/",
  "page_type": "homepage",
  "theme": "light-institutional",
  "context": {},                 // e.g. {"nis": "52018"} for a municipality template
  "seo": {"title": {...}, "description": {...}},
  "sections": [
    {
      "id": "sec-hero",
      "blocks": [
        {
          "id": "blk-hero-1",
          "type": "hero",
          "version": 1,
          "props": {...},
          "binding": null,              // static block, no data
          "visibility": {"desktop": true, "tablet": true, "mobile": true},
          "layout": {
            "desktop": {"x": 0, "y": 0, "w": 12, "h": 4},
            "tablet":  {"x": 0, "y": 0, "w": 8,  "h": 5},
            "mobile":  {"x": 0, "y": 0, "w": 4,  "h": 6}
          }
        }
      ]
    }
  ]
}
```

Grid units, not pixels (12 / 8 / 4 columns for desktop / tablet / mobile), so a layout survives a
redesign of the underlying CSS grid.

### Block IDs

Stable, unique within a page, never reused after a block is deleted (so history/undo can refer to
a dead block id without colliding with a new one).

### Semantic validation (rejects, does not warn)

- Duplicate block ids
- Unknown block type or unsupported version
- Invalid route (must match an allowed pattern; no arbitrary paths)
- Invalid grid position (out of bounds, zero/negative size, overlap within a fixed-position
  section that doesn't allow it)
- A binding naming an indicator that doesn't exist in the published metadata
- A binding naming an invalid NIS code
- A binding requesting an aggregation the indicator's additivity forbids — concretely, a SUM
  across geographies against an indicator published `additive: false` (claude.md rule 27 / the
  existing Definitions section).

  **Corrected 2026-09-07, during Batch 9.** This bullet originally read
  "`is_additive`/`aggregation_method`". `aggregation_method` is a database column
  (`migrations/001_core_schema.sql`) that is **not declarable in `config/indicators/*.yaml`**,
  **not published in any payload**, and **read by nothing** — `src/analytics/aggregate.py`'s
  entire live rule is `SUM if meta.get("is_additive") else REFUSE`. Worse, 17 live indicators
  carry `aggregation_method = 'population_weighted'`, a method ADR 0003 measured as wrong and
  says "must not be added". Enforcing the column as written would have encoded a prohibited
  method against real indicators. The validator does not read it, and neither should Batch 14.

  Note also what this bullet does *not* license: `additive: false` alone does not mean "no
  aggregate exists". 13 of the 52 municipal indicators are derived and all carry
  `additive: false`, yet this pipeline genuinely publishes province-level
  `AVG_NET_TAXABLE_INCOME` by recomputing it. Deciding *recomputability* is Batch 14's job,
  using `src/analytics/aggregate.py`'s own answer — never a copy of it in `src/pages/`
  (claude.md rule 19, invariant 4).
- Missing accessible name on an interactive block
- Raw SQL, a `<script>` tag, a remote URL, or unsafe rich text anywhere in the document

### Versioning and migration

A block's `version` lets the registry keep old block instances rendering correctly while a new
version changes its props shape; `src/pages/migrations.py` upgrades old documents forward,
never silently drops a field it doesn't understand.

## Tests

- Every rejection case above, each as its own test
- A minimal valid document for every existing page type once converted
- Round-trip serialize/deserialize with stable key ordering (so two saves of an unchanged
  document are byte-identical, matching claude.md rule 35)
- Oversized and deeply nested documents (a resource-exhaustion guard, not just a correctness one)
- Migration from each superseded schema_version

## Assumptions and open questions

- The exact set of allowed `page_type` values is not yet fixed; it will match whatever page
  types Batch 15 actually converts (homepage, macro, micro, municipality-profile, explorer,
  data-explorer, map-explorer, blank).
- Whether `context.nis` is the only templated context variable, or whether other page types need
  their own context shape, is open until the municipality-profile template (Batch 4) is designed.

## Rollout / risks

Getting this schema wrong is expensive: every later batch writes documents against it. Batch 9
should be reviewed carefully before Batch 10 (the renderer) starts consuming it.
