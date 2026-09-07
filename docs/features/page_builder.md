# Feature: BelPulse redesign and repository-native page builder

Status: draft — Batch 0 in progress, Batches 1+ blocked on reference designs
Issue: none (maintainer-directed expansion of Phase I, added 2026-09-07)
Branch: feat/website-redesign-batch0 (Batch 0 only; each later batch gets its own branch)

## Problem

The current public pages (`index.html`, `dashboard.html`, `communes.html`, `map.html`,
`local.html`, `all_data.html`, `about.html`, plus 1,695 static `/local/{nis}` pages) are correct,
tested, licence-compliant and fast, but were built page by page with no shared visual design
system and no way to assemble a new page without writing HTML and JavaScript by hand. The
maintainer wants the site to match a set of supplied reference designs (homepage, macro, micro,
municipality profile) and wants a way to build and adjust pages visually going forward, for
promotional use.

## Goal

1. The supplied reference designs are faithfully reproduced, using real BelPulse data.
2. A small, repo-native drag-and-drop builder lets a non-developer assemble a page from typed
   blocks and bind them to real indicators, without writing JSON by hand.
3. Every existing URL, dataset, licence notice, language and static SEO page keeps working
   exactly as it does today.
4. The database, adapters, geography resolution and analytics are untouched by this work
   (ADR 0004).

## Non-goals

- Rewriting the public site in a JS framework (claude.md rules 17, 30; already prohibited
  before the 50% milestone).
- A hosted, multi-user CMS with accounts. That is explicitly Phase III/IV work
  (`app.belpulse.be`), after a paid pilot (see docs/steps, Block AB).
- Continuing the analytical roadmap (peer model, forecasting, scenarios, etc.) inside this
  effort. Those blocks (M-W) are unaffected and unblocked; this work is additive alongside them.
- Guessing at the reference designs. Nothing in Batches 1, 3, 4, 6 or 7 starts until the actual
  design files (images, a Figma export, or equivalent) are supplied. See "Open questions" below.

## Approach

This spec tracks the full plan; each batch below is also a line item in `docs/steps` so progress
is visible in the one place the maintainer already checks (claude.md rule 16).

### Architecture, in one paragraph

A page is a JSON document (`config/pages/{page_id}/{draft,published}.json`) describing sections
and blocks, each block typed and versioned, each block's data supplied by a **binding** — a
reference to an existing payload (national/municipal/indicator/geography/comparison/ranking),
never a raw query. One shared renderer (`assets/belpulse/renderer.js`) turns a page document into
DOM, and both the public site and the builder's own preview call that same renderer, so what the
maintainer sees while editing is what ships. The builder (`builder/`) edits `draft.json` only;
publishing validates the draft and atomically replaces `published.json`; the existing static
export reads only `published.json`.

### The 17 batches (0-16)

Full detail lives in `docs/implementation/batches/` as each one starts; this is the index.

| # | Batch | Depends on | Status |
|---|---|---|---|
| 0 | Protect the existing baseline | — | done |
| 1 | BelPulse design tokens | reference designs | done |
| 2 | Shared presentation components | Batch 1 | done |
| 3 | Homepage reproduction | Batch 2, reference designs | done (preview route, home.html) |
| 4 | Municipal profile redesign | Batch 2, reference designs (Namur) | not started, unblocked |
| 5 | Municipality explorer redesign | Batch 2 | not started, unblocked |
| 6 | Macroeconomics reproduction | Batch 2, reference designs | not started, unblocked |
| 7 | Microeconomics page | Batch 6 | not started |
| 8 | Data and map explorers | Batch 2 | not started, unblocked |
| 9 | Page-document schema | Batch 0 | done |
| 10 | Shared block renderer | Batch 9 | not started |
| 11 | Builder service | Batch 9 | not started |
| 12 | Builder shell | Batch 11 | not started |
| 13 | Drag, resize, responsive layouts | Batch 12 | not started |
| 14 | Data-binding engine | Batch 10 | not started |
| 15 | Convert pages into templates | Batches 3-8, 10, 14 | blocked (transitively) |
| 16 | Draft/publication transaction | Batch 11 | not started |

Batches 9-14 (the block engine and builder itself) do **not** need the reference designs and can
proceed once Batch 0 is closed. Batches 1, 3, 4, 6, 7 (visual reproduction) are blocked until the
designs arrive — building a design system from a description would be guessing, which rule 13
forbids for data and is just as wrong for a visual spec.

### The 20 architecture rules

Recorded as claude.md rules 17-36, not duplicated here. Every batch's acceptance criteria include
"no rule 17-36 violation," checked by the relevant audit (security, data, or architecture review).

### Agents

`.claude/agents/` gets one file per role in section 4 of the maintainer's plan
(`belpulse-lead`, `repository-architect`, `frontend-implementer`, `builder-core`, `builder-ui`,
`test-engineer`, `visual-auditor`, `data-auditor`, `security-red-team`, `release-engineer`,
`scan-assistant`). Model assignment follows Anthropic's own guidance: Opus 5 for architecture and
high-risk work, Sonnet 5 for most implementation, Haiku 4.5 for read-only inventories. Fable 5.1
is available as an optional final red-team pass, invoked manually, not part of the default
pipeline.

### Test matrix, audit plan, batch report format, severity levels

Reproduced without material change from the maintainer's plan — see
`docs/implementation/invariants.md` for the rule list and `docs/implementation/known-risks.md`
for what's flagged so far. The batch report template lives in
`docs/implementation/batches/_TEMPLATE.md`.

## Open questions for the maintainer

1. **Reference designs.** Batches 1, 3, 4, 6, 7 need the actual homepage / macro / micro /
   municipality (Namur) design files. Without them, nothing past Batch 0 and the block-engine
   batches (9-14) can start truthfully.
2. **What serves the builder.** The plan allows "a separate framework... for the builder
   interface only" but doesn't pick one. Recommendation: plain HTML/CSS/vanilla JS talking to a
   small Python HTTP server (`scripts/serve_builder.py`, stdlib `http.server` or a very small
   dependency like Flask) — consistent with the rest of the repo's dependency discipline and
   avoids introducing a build step (webpack/vite/npm) into a Python-first repository. Open for
   the maintainer to override.
3. **"More data."** The maintainer separately asked to expand Phase I with more data sources.
   This plan doesn't name any; needs a follow-up conversation and, per rule 8, a
   `docs/data_catalog.md` row approved before any adapter work starts.
4. **GATE 50% relationship.** This work is additive to Phase I but doesn't change the existing
   GATE 50% checklist (data depth, RED audit, watch two users, tag v0.5). Recorded as its own
   section in `docs/steps`, positioned before that gate, not folded into it — open to the
   maintainer to say otherwise.

## Rollout / risks

- Largest risk: reproducing a design nobody has verified is *reachable from real data* — a
  homepage panel that needs a national unemployment number this pipeline doesn't have, for
  example. Batch 3/6/7's data audits exist specifically to catch that before launch, not after.
- Second risk: scope. 17 batches plus 11 agent roles is a multi-week programme. Batch 0 and the
  block-engine batches (9-14) are the only parts that can start today; everything visual waits on
  the maintainer.
