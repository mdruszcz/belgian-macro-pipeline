# ADR 0004 — a repository-native website builder, not a hosted CMS

Status: accepted (2026-09-07)

## Context

BelPulse needs a redesigned public site (homepage, macro/micro views, municipality profile,
explorers) built from supplied reference designs, and a way for the maintainer to assemble and
edit pages visually without hand-editing JSON or HTML, and without touching the data pipeline.

Two shapes were possible: adopt a hosted CMS or a JS framework rewrite of the public site, or
build a small, repo-native editor that reads and writes versioned JSON page documents which a
shared renderer turns into static HTML at build time.

## Decision

Repository-native. A `builder/` app served locally (via `make builder`, in a Codespace or on a
laptop) edits `config/pages/{page_id}/draft.json` through a small local API
(`scripts/serve_builder.py`). Publishing validates a draft and, only on success, atomically
replaces `config/pages/{page_id}/published.json`. The existing static-export pipeline reads only
`published.json` and renders it through the same shared renderer the builder preview uses.

> **Superseded in part by Batch 10 (2026-09-07).** This decision named that renderer
> `assets/belpulse/renderer.js`, a browser-side implementation. That file was never written and
> must not be: a JavaScript renderer would put every published figure behind JavaScript, which
> breaks `scripts/export_local_pages.py`'s commitment that its output is "the crawler-visible
> copy of the data" and `docs/features/i18n.md`'s requirement that a reader with JavaScript
> disabled still gets a complete page. The one renderer is **Python**, `src/pages/render.py`, and
> both the static export and the builder preview (Batch 11) call `render_document()` directly.
> `assets/belpulse/blocks.json`/`blocks.js` remain browser-side for chart and map hydration and
> for Batch 12's block-library UI, which is a different consumer from the renderer. Everything
> else this decision records is unchanged.

This keeps the entire public site static and GitHub-Pages-compatible, keeps the canonical
database and the payload pipeline completely untouched by the builder, and keeps publishing an
explicit, reviewable, git-committed step rather than a live database write from a browser.

## Why not a hosted CMS or a framework rewrite

- **A hosted CMS** needs a server, a database of its own, and auth before day one -- all three
  are explicitly prohibited before the 50% milestone (see claude.md), and none of them are
  needed to let one person assemble pages from typed blocks.
- **A framework rewrite of the public site** (Next.js, React, etc.) would touch every page this
  pipeline already publishes correctly -- 1,695 static commune pages, trilingual, licence-tested,
  SEO-tested -- for a benefit (component reuse) the existing shared CSS-variable/vanilla-JS
  approach already gets close enough to, at a fraction of the risk. See CLAUDE.md's existing
  "Prohibited until the 50% milestone" list, which already banned this before this ADR existed.
- A repo-native builder can be reasoned about with the tools already used everywhere else in
  this repo: it is JSON, validated by a Python script, tested by pytest, and its output is a
  directory of static files `make all` already knows how to publish.

## What this rules out

- No arbitrary JavaScript inside a page document (rule 22). A page is data.
- No browser-side SQL and no direct SQLite access from the builder (rules 18, 20, 21). The
  builder edits page LAYOUT; it never edits or queries the canonical data.
- No automatic publish, commit or push from the browser (rules 32, 34). A human decides when a
  draft becomes public, and when the resulting files are committed.

## Consequences

- A second, small application (`builder/`) with its own local dev server exists in the repo. It
  is explicitly editor-only tooling, not part of the public site, and never ships to GitHub
  Pages.
- Every public page must be re-expressible as a tree of typed blocks with data bindings before
  it can be edited visually. Pages not yet converted stay hand-built HTML, exactly as today,
  until Batch 15 (docs/features/page_builder.md) converts them one at a time with a parity check.
- A companion ADR is expected once the draft/published split's failure modes are fully specified
  (see docs/features/page_builder.md, Batch 16) -- recorded here as a forward reference rather
  than duplicated.

## What would reverse this

If the builder needs to support more than one simultaneous editor with real-time collaboration,
or media upload/storage, the repo-native shape stops fitting and the hosted-platform work already
planned for after the paid-pilot gate (`app.belpulse.be`, Postgres, auth) should absorb it rather
than growing a second CMS inside the static-site repo.
