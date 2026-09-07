# Batch 0 — protect the existing baseline

```
Batch: 0 -- protect the existing baseline
Base commit: 6c36ddb3 (develop, before this batch)
Final commit: <filled in at commit time>
Files changed: docs/steps, claude.md, docs/features/*.md (new), docs/decisions/0004-*.md
  (new), docs/implementation/** (new), .claude/agents/** (new),
  scripts/capture_baseline_screenshots.py (new), .gitignore
Requirements completed: tag, full pipeline + test run, route inventory, asset/payload
  sizes, screenshots (desktop + mobile, 9 pages), Lighthouse on 4 core pages,
  worktree-clean check, invariants documented
Deferred requirements: none for Batch 0 itself
Data-contract impact: none -- no adapter, schema, geography-resolution or analytics
  file touched
Commands executed: `make all`; `npx lighthouse` x4 (CHROME_PATH set to Playwright's
  Chromium); scripts/capture_baseline_screenshots.py
Tests passed: 684/684 (full existing suite)
Screenshots produced: 18 (9 pages x 2 viewports), NOT committed -- see below
Performance results: see Lighthouse table below
Reviewer findings: none yet -- Batch 0 is the baseline, not itself reviewed
Known limitations: see "What Batch 0 does NOT cover" below
Rollback procedure: `git tag belpulse-v0.5-pre-redesign` marks this state; nothing
  destructive has happened, there is nothing to roll back
Next batch: 9 (page-document schema) and 11 (builder service) can start now; 1, 3, 4,
  6, 7 stay blocked on the reference designs
```

## 1. Baseline tag

`belpulse-v0.5-pre-redesign`, created locally on this branch's first commit. **Not pushed** —
tags are shared, visible state once pushed, and this one is an internal engineering checkpoint,
not the real `belpulse-v0.5` release the maintainer will tag separately at GATE 50% (docs/steps
already has that as its own `[H]` step). Ask before pushing it if you want it on GitHub.

## 2. Full pipeline and test suite

`make all` — install, schema, reference rows, validate, every export, tests. **684 passed, 0
failed**, fully offline from committed data, in under two minutes on this machine.

## 3. Every current route

Seven hand-built public pages at the repo root: `about.html`, `all_data.html`, `communes.html`,
`dashboard.html`, `index.html`, `local.html`, `map.html`.

1,695 static permanent commune pages under `local/`: 565 communes × 3 languages
(`local/{nis}/`, `local/{nis}/fr/`, `local/{nis}/nl/`). Confirmed all three languages present for
every commune — no gaps.

## 4. Current asset and payload sizes

| What | Size |
|---|---|
| `local/` (all 1,695 static pages) | 40 MB |
| `public/data/` (all generated payloads) | 39 MB |
| `public/data/communes/*.json` (565 files) | 37 MB |
| `public/data/indicators/*.json` (52 files) | 1.9 MB |
| `public/data/metadata/` | 144 KB |
| `assets/commune_map.js` + `.css` (shared map component) | 32 KB |
| `assets/i18n.js` (shared strings, 81 keys × 3 languages) | 32 KB |
| `assets/belpulse/` (Batch 1-2 tokens/layout/components/charts) | 80 KB |
| `assets/belpulse/blocks/registry.json` (Batch 9 block registry) | 11.6 KB |
| `data/geo/communes.geojson` (boundary file) | 1.2 MB |
| `data/communes_history.csv` (bulk export) | 37 MB |
| `data/communes_table.json` (communes.html's own payload) | 12 MB |
| `data/belgian_macro.db` (committed database) | 18 MB |

This is the number every later batch's asset-size comparison (release-engineer's release gate)
measures against. A regression is "grew unexpectedly against this table," not a guess.

**Amended as later batches add browser-served files.** The last two rows were added after
Batches 1-2 and Batch 9 respectively — `assets/belpulse/blocks/registry.json` is fetched by the
browser (Batch 10's renderer reads the same file the Python validator does, which is why it
lives under `assets/` rather than `config/`), so it belongs in this table rather than being
invisible to the next size audit. `docs/features/page_document.schema.json` (10.6 KB) is *not*
listed: it is validated against in Python at build time and never served to a visitor. Keep
appending rows here rather than starting a second table, or the release gate ends up comparing
against a baseline that no longer describes the site.

## 5. Screenshots

Captured at 1440×1000 (desktop) and 390×844 (mobile) for all 7 root pages plus a static
`/local/{nis}/` page and a searched `local.html?nis=` view — 18 images, zero console/page errors
across every capture. Script: `scripts/capture_baseline_screenshots.py`.

**Not committed to git.** 15 MB of new binary artefacts is exactly what claude.md rule 12 says
to ask about first, and a "before" screenshot set is exactly the kind of thing that would be
recaptured after every visual batch anyway (visual-auditor needs a fresh "after" set each time,
not a growing pile of "before" sets from every batch). Re-run the script against a local server
whenever a fresh baseline is needed; nothing here depends on the old images surviving in git
history.

## 6. Lighthouse baseline (performance / accessibility / best-practices / SEO)

Chrome from Playwright's own install (`CHROME_PATH` pointed at it — no separate Chrome install
needed). Ran against `index.html`, `communes.html`, `local.html?nis=11002` and `map.html`.

| Page | Performance | Accessibility | Best practices | SEO |
|---|---|---|---|---|
| `index.html` | 73 | 92 | 96 | 90 |
| `communes.html` | 46 | — | 96 | — |
| `local.html?nis=11002` | 91 | 94 | 96 | 90 |
| `map.html` | 59 | 96 | 100 | 77 |

`communes.html`'s and `map.html`'s accessibility/SEO categories didn't complete in this run
(worth re-running before treating that as a real gap rather than a tooling hiccup). The two low
performance scores are not surprising and not new information: `communes.html` fetches an 11 MB
precomputed table payload on load, and `map.html` fetches a 1.2 MB boundary file plus draws 565
SVG paths — both documented, deliberate trade-offs from when those pages were built, not a
regression to chase down in Batch 0. Recorded here so a later batch's Lighthouse run has
something real to compare against.

## What Batch 0 does NOT cover

- It does not audit the redesign itself — there's nothing to audit yet.
- It records Lighthouse for 4 pages, not all 7 + representative commune pages; broaden this
  before the release gate if the maintainer wants a fuller picture.
- Accessibility and SEO categories are missing for 2 of the 4 Lighthouse runs — re-run those two
  before relying on this table for an accessibility comparison specifically.
