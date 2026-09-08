# Batch 12 — builder shell (pre-implementation specification, revision 2)

> **Revision 2 supersedes revision 1 in five places, after the `repository-architect`
> pre-check.** Revision 1 asserted a round-trip property the code cannot deliver (P0), and
> specified a shell that **could not load itself** (P0: subresources cannot send the token
> header, and `_check_token` accepts a query token on `/` only). It also had no route able to
> reach `assets/belpulse/*.css`, so the preview could not be styled without copying the public
> design system. The architecture below is revision 2's; where this document and revision 1
> disagree, revision 2 governs. The findings are recorded in full at the end.

```
Batch: 12 (docs/features/page_builder.md; docs/steps "[BUILD] Batch 12 -- builder shell")
Base commit: 3270837a (Batch 11: builder service, feat/builder-service)
Branch: feat/builder-shell (off 3270837a)
Final commit: <set at PR merge>
Status: SPEC — not yet routed past architecture pre-check
```

## Objective

Make a valid page loadable, editable and saveable **without ever touching raw JSON**
(docs/steps' stated "why"). Batch 11 opened the API; Batch 12 is the first browser UI that
consumes it. Concretely: a page/blank-page selector, a block library fed by the real registry,
a structure tree, a canvas that previews through the same `render_document()` the public build
calls, six inspector panels (content / layout / appearance / data / accessibility / visibility),
and a top bar (undo/redo, preview viewport, save / validate / publish).

The single hardest acceptance property, and the one everything else is subordinate to:

> **Round-trip fidelity, restated (revision 2).** Loading a service-written, already-canonical
> draft that needs no migration, and saving it unedited, must produce a byte-identical
> `draft.json`. Where migration or canonicalisation *does* change the bytes, the change must be
> announced in the UI **before** the save, never applied silently. The shell may never drop or
> corrupt a value it does not have a control for.

Revision 1 stated this as unconditional byte-identity, and the architecture pre-check showed
that is false in three ways, all by design rather than by accident:

- `/api/document` returns the **migrated** document plus a `migrated` flag, so saving it back
  legitimately writes different bytes whenever a migration fired (`kpi_card` v1→v2 renames
  `show_sparkline`, adds `show_provenance`, bumps `version` —
  `/workspaces/belgian-macro-pipeline/src/pages/migrations.py`).
- `/api/save` re-serialises through `_canonical_text` → `dumps(sort_keys=True, indent=2,
  ensure_ascii=False)` + trailing newline. Canonicalisation **is** rule 35; a hand-edited draft
  with different key order or indentation is *supposed* to come back normalised.
- An "unrecognised prop" cannot reach disk at all: every props schema is
  `additionalProperties: false` and the document schema is closed at every level, so
  `/api/save` returns 422 first. Revision 1's "read-only passthrough field for an unrecognised
  prop" was dead code and is struck.

The property that survives is still the one that matters: **a builder which silently discards
or mangles a value is worse than no builder, because the loss is invisible until publish.**
Note this is a stronger claim than invariant 9, which is about two *builds* being
byte-identical, not about a disk round trip. First test written, never relaxed.

A fourth fidelity hazard the pre-check surfaced, which revision 1 missed entirely: **the shell
parses JSON in the browser**, so `1.0` becomes `1`, `1e3` becomes `1000`, and integers above
2^53 lose precision. Nothing in the current pipeline defends against this. It must have its own
test.

## Why this is split into two steps, run in sequence

`builder/app/` is empty, and the shell cannot reach the registry, its own stylesheet or its own
script through any route Batch 11 serves — `GET_ROUTES` is a closed set of five paths and `/`
returns a Python-generated bootstrap string stored nowhere. So Batch 12 needs a small, genuinely
security-relevant change inside `src/builder/service.py` before any interface work can start.
That is not routine UI work, and `builder-ui`'s charter says it consumes the core rather than
extends it.

- **12a — the service seam (`builder-core`).** The static-file route, the registry endpoint, the
  token handoff into a real page, and the undo/redo history primitive (which the agent charter
  assigns to `builder-core`, and which Batch 13 will extend rather than replace).
- **12b — the shell (`builder-ui`).** Everything else under `builder/app/`.

**They run strictly in sequence, never concurrently.** `docs/implementation/known-risks.md`'s
2026-09-07 process row (three agents writing `src/pages/` at once, six files overwritten
mid-session, three changes surviving by last-writer-wins) is a standing rule for the rest of
this programme, not a Batch 9 anecdote. 12b does not start until 12a is committed and its tests
pass.

## Included work

### 12a — service seam (builder-core)

**Decision, revision 2: there is no static-file route. `/` inlines the shell.** Revision 1's
`GET /app/{file}` is struck.

`bootstrap_html()` reads the shell's real files from `builder/app/` at request time — from a
**fixed tuple of module constants, with no user-controlled path component anywhere** — and
emits them inline in the one document. This is not a shortcut; it removes the single largest
piece of new attack surface in the batch, and it is the only option that resolves all three
P0/P1 findings at once instead of trading them against each other:

- There is no path to traverse, no allowlist to get wrong, no directory to list, and no
  filename derived from a request. A route that cannot be addressed cannot be attacked.
- The token problem disappears. Revision 1 was **self-contradictory**: it required the token
  header on `/app/*` *and* loaded those files via `<script src>` / `<link rel=stylesheet>`,
  which cannot send a header — and `_check_token` accepts a query token on `/` only. Every
  subresource would have 401'd. With the script inline, the token literal is already in the same
  closure. No global, no `localStorage`, no DOM attribute, no blob-URL dynamic import.
- The preview-CSS problem disappears. `bootstrap_html()` also **reads**
  `assets/belpulse/{tokens,components,layout,blocks}.css` and hands their text to the shell as a
  string constant for `srcdoc` injection. No second allowlist root under `assets/`, and — the
  point — **no copy of the public design system inside `builder/app/`**, which would have been
  exactly the drift Batch 10 exists to prevent and arguably a rule-20 violation.
- A stale cached `app.js` against a restarted server (new token) is impossible.

The files under `builder/app/` stay real, reviewable, separately-owned files. Only their
delivery changes.

Required of this step:

1. **`bootstrap_html()` rewritten** as a shell skeleton + inlined CSS + inlined JS + the token
   literal, keeping `history.replaceState` stripping `?token=`. Batch 11's demo controls go.
   **It must keep a `sandbox`ed preview `<iframe>` element in the Python-generated skeleton** —
   see "Tests you may not break" below; this is not cosmetic.
   Because the JS is embedded between `<script>` and `</script>`, a `</script` sequence inside
   any shell file would break out of the element. Escape on the way out **and** assert on the
   way in (a test that no file under `builder/app/` contains `</script`).
2. **`GET /api/registry`** — return the registry already loaded into `BuilderConfig`, so the
   block library never carries its own copy of the block list (two lists drift the first time a
   block type gains a prop). **`BuilderConfig.registry` is a frozen dataclass holding
   `MappingProxyType`s: `json.dumps` raises `TypeError`, and so does `dataclasses.asdict`.**
   Build a plain dict explicitly. Note also that `load_registry()` drops four top-level keys the
   file carries (`accessible_name_note`, `deliberately_absent`, `forward_compatibility`,
   `props_schema_note`) — so "the endpoint returns what the file says" is false and the test must
   assert against the **module**, not the file.
3. **A Content-Security-Policy on `/`, decided in this step and verified in a browser, not
   reasoned about.** `/` currently sends no CSP at all, which is the *only* reason revision 1's
   inline-`<style>`-in-`srcdoc` approach works: a `srcdoc` iframe inherits the embedder's
   policy. Adding a CSP later — the obvious hardening, and something the mandatory red-team pass
   is likely to ask for — silently kills the preview's styling. So it is decided here, with a
   per-response nonce for the inline script and whatever `style-src` the inherited policy needs.
   **Gate:** a Playwright test asserting the preview is *actually styled* in a real browser. If
   no working policy can be established, ship `/` with no CSP (today's behaviour, which works)
   and record it as an open P2 for the red-team pass. Shipping a CSP that breaks the canvas, or
   a canvas that breaks the next time someone adds a CSP, are both worse than saying so.
4. **`builder/app/history.js`** — the undo/redo primitive: a bounded stack over whole document
   states, with coalescing of rapid same-field edits, a `canUndo`/`canRedo`/`push`/`undo`/`redo`
   surface, and a documented extension point for Batch 13's drag operations. Bounded memory
   (see limits below). Owned by `builder-core`; `builder-ui` calls it and does not modify it.

**The module boundary is a capability, not a token.** However the shell's entry point is
invoked, it receives an `api(path, options)` closure that adds the token header — never the
token itself. That is non-negotiable and survives any later change to delivery.

### 12b — the shell (builder-ui)

5. **Page selector.** Lists pages from `GET /api/pages`; opens draft or published; creates a new
   page by `POST /api/save` with a new `page_id` (the store creates the directory — no new
   endpoint is needed, and none may be added). Surfaces the `migrated: true` flag from
   `/api/document` as an explicit "this document's schema_version moved on load" notice, never
   silently. **Verified by the pre-check:** the save path validates *before* `_ensure_page_dir`,
   so a rejected save leaves no empty directory behind.

   **A "blank" page is not blank, and this is the batch's second-largest trap.** The root
   `required` set is `schema_version, page_id, revision, route, page_type, theme, context, seo`,
   and `seo.title` must be a trilingual triple with all three entries **non-empty** — empty
   strings are a `schema_violation`. So new-page is a real form. Three constraints on it:
   - It must not prefill `fr`/`nl` from `en` (rule 7), and must not invent placeholder title
     text that reads as content (invariant 3).
   - `route` is a **closed allowlist living in `src/pages/semantics.py`**, which is out of scope
     and exposed by no endpoint. The form must **not** hard-code a copy of it — that list would
     drift the first time a route is added. Drive the field off `POST /api/validate` and surface
     the server's own message, which names the allowed shapes. Same for `page_type` and `theme`,
     which live only in the JSON Schema the browser cannot fetch.
   - `sections: []` validates, so a page may legitimately start with no blocks.
6. **Block library** driven entirely by `/api/registry`: the six declared types (`hero`,
   `kpi_card`, `chart`, `comparison_table`, `map`, `rich_text`), their supported versions, and
   their declared props. A type the registry does not declare does not appear.
7. **Structure tree.** Sections and blocks, selection, keyboard navigation, add/delete/duplicate,
   and move-up/move-down **as list operations only** (pointer drag and grid resize are Batch 13).
8. **Canvas.** The preview from `GET /preview`, injected via `srcdoc` into the sandboxed iframe,
   with a viewport switcher (see reference viewports). Selecting a block in the tree highlights
   it in the preview; the preview is read-only in this batch.
9. **Inspector, six panels.** content / layout / appearance / data / accessibility / visibility.
   Every field is generated from the registry's declared props where one exists; a prop with a
   `provisional` schema still gets a typed control. Where the shell has **no control** for a
   declared prop, the value is preserved verbatim and shown read-only — never dropped, and never
   silently rewritten. (Revision 1 called this an "unrecognised prop" passthrough; props schemas
   are `additionalProperties: false`, so an unrecognised prop cannot exist on disk. The real case
   is a *declared* prop the UI has not yet grown an editor for.)
   Trilingual fields are `{en,fr,nl}` triples with all three editable and none defaulted from
   another (rule 7); the shell must not copy `en` into `fr`/`nl`.
10. **Top bar.** Undo / redo (wired to `history.js`), preview viewport, and explicit
    Validate / Save / Publish buttons. Publish is a separate, deliberate action with a
    confirmation, never a side effect of saving (rule 32, invariant 10). A restore-from-version
    control reading `/api/versions`, with its own confirmation stating that restore overwrites
    the draft.

## Excluded work — belongs to a later batch, and may not be started here

| Excluded | Owner |
|---|---|
| Pointer drag, grid move/resize, collision handling, per-breakpoint layout editing, lock, debounced autosave | Batch 13 |
| Binding **resolution**; any searchable indicator catalogue or "pick an indicator" picker | Batch 14 |
| Templates as a concept. docs/steps says "page/template selector"; no template exists yet, so this batch ships a **page** selector plus new-blank-page and nothing that claims to be a template | Batch 15 |
| Test-gated publish, publish-builds-to-temp-dir | Batch 16 |
| Concurrent-save conflict detection (the `revision` field the service never reads) | Batch 16 — see known limitations |
| The six open P2/P3 renderer findings from Batch 11's red-team pass | their own follow-up specs |
| Any change to `src/pages/`, `src/builder/paths.py`, `src/builder/store.py`, exporters, or the static export | out of scope |

**The indicator-picker exclusion is deliberate and load-bearing.** `docs/implementation/known-risks.md`
records as **P1** that `public/data/metadata/indicators.json` is municipal-only (52 rows) and that
the 17 national indicators exist only in `public/data/national.json`, in no metadata index at all,
with monolingual names. A picker built in this batch would be built from the metadata index and
would silently drop every national figure. The data panel therefore edits binding descriptors
through typed controls and shows live `POST /api/validate` feedback against the server's own
validator — which already unions both files — and the catalogue waits for Batch 14, which owns
that asymmetry.

## Allowed files

**12a (builder-core) may touch, and nothing else:**

```
src/builder/service.py                                  (bootstrap_html, /api/registry, CSP)
builder/app/history.js                                  (new)
tests/builder/test_builder_shell_service.py             (new)
```

**12b (builder-ui) may touch, and nothing else:**

```
builder/app/*.js  builder/app/*.css                     (new; the exact file split is the
                                                         implementer's call, EXCEPT history.js
                                                         which is 12a's and read-only here)
tests/builder/test_builder_shell_e2e.py                 (new, Playwright)
tests/builder/test_builder_shell_static.py              (new, source-level assertions)
scripts/serve_builder.py                                (ONE string: the startup banner says
                                                         "Batch 11: API only, no UI yet")
Makefile                                                (ONE line: the `builder` help text
                                                         says "builder API")
docs/implementation/batches/batch-12-builder-shell.md   (the report)
docs/steps                                              (tick the Batch 12 line)
docs/implementation/known-risks.md                      (new rows only)
```

**Filename coupling, revision 2.** With no static route there is no filename allowlist, so 12b
adds shell files freely — but `bootstrap_html()` inlines a **fixed tuple** of them, so a new
file still needs one line in `src/builder/service.py`. 12b may make **that edit only** (adding a
filename to the tuple) and must say so in its report. Any other change to `service.py` from 12b
is out of scope and must be escalated, not made.

**Tests you may not break, and one you may not quietly relax.**
`tests/builder/test_builder_api.py` is in **neither** set. It contains
`test_bootstrap_html_does_not_embed_a_scriptable_same_origin_iframe`, which asserts the string
`bootstrap_html()` returns contains an `iframe` and a `sandbox` and neither `allow-same-origin`
nor `allow-scripts`. Moving the preview iframe out of the Python skeleton into `app.js` is the
natural refactor and it **breaks that test** — which is precisely why the skeleton must keep the
iframe element (12a item 1). If touching that file becomes unavoidable, that is an escalation to
the lead, not a silent edit, and the equivalent assertion must exist over `builder/app/*.js` in
`test_builder_shell_static.py` **as well as**, never instead of, the Python one.
`tests/security/test_builder_service_hardening.py` AST-scans `src/builder/*.py` and
`scripts/serve_builder.py`; it does **not** scan `builder/app/*.js`, so the JS source-level
assertions are genuinely new work.

**Never touched by either step** (invariant 4, ADR 0004, claude.md rule 20): the database
schema, `src/fetchers/`, `src/analytics/`, `resolve_geo()`, `src/pages/`, `src/builder/paths.py`,
`src/builder/store.py`, any exporter, any page-root HTML file, `assets/belpulse/*` (the public
design system — the builder's chrome gets its own CSS and may **read** the tokens file, not edit it).

## Data inputs

The shell consumes exactly four sources, all through the service, none read from disk by the
browser:

| Input | Route | Notes |
|---|---|---|
| Page list | `GET /api/pages` | |
| A document | `GET /api/document?page_id&which` | already migrated; `migrated` flag surfaced |
| Block types and props | `GET /api/registry` (12a, new) | the only source of truth for the library and inspector |
| Rendered preview | `GET /preview?page_id&which&lang` | a fragment, unvalidated on-disk content, no page shell |
| Version list | `GET /api/versions?page_id` | |

**Invariant 3 is a hard gate on this batch.** No file under `builder/app/` may contain a
hand-typed indicator value, a commune figure, or a mockup number. Block-library entries are
described by name, icon and the registry's own text — **not** by a thumbnail containing
"€ 42.3k" or "Namur 8.1%". Empty-state and placeholder copy names the field, never a plausible
value. This is the most likely single failure of this batch, because every builder UI on the
internet ships fake numbers in its block palette, and a data-auditor pass is required
specifically to check it.

**Preview data is a stand-in, and must be labelled as one.** `/preview` is handed
`preview_data(doc)`, never resolved data (Batch 14 does not exist). A bound block therefore
renders an "unavailable" state. The canvas must display an explicit, persistent notice that
real values do not appear in the preview until the data-binding engine lands — a reader who
mistakes a stand-in for a real figure is exactly the failure invariant 6 exists to prevent.

## Required states

Every one of these is a state the implementer must build and the tests must exercise. A state
that is "handled" by an unstyled browser error or a silent no-op is not handled.

**Document / API states**

1. No pages exist yet (first run — `config/pages/` holds only `README.md` today, so this is the
   state a maintainer actually meets first).
2. Document loads clean.
3. Document loaded with `migrated: true`.
4. `422 validation_failed` — field errors mapped back to the offending block and prop, in the
   inspector, not a raw JSON dump.
5. `413 guard_rejected` / `payload_too_large` — the service deliberately echoes no detail; the
   UI must say so plainly rather than showing an empty error.
6. `409 version_space_exhausted` on publish.
7. `401 unauthorized` — the ordinary case is a restarted server invalidating the token. The UI
   must say "restart `make builder` and reopen the printed URL", not "something went wrong".
8. Server unreachable / fetch rejected.
9. Save succeeded; publish succeeded; publish refused because the draft is invalid.
10. Restore: version list populated, version list empty, restore confirmed, restore cancelled.
11. Unsaved changes present, and an attempted navigation away from them.

**Block states** — invariant 6's five states (missing / unavailable / suppressed /
not-applicable / explicit-zero) stay five distinct, correctly-labelled states in the preview.
The shell renders none of them itself; it must not collapse, restyle or hide any of them,
and an explicit measured zero must never be presented as an absence.

## Reference viewports

**No reference design exists for the builder's own chrome, and none is required.** The P1
"reference designs missing" risk blocks the *public-page* batches (1, 3, 4, 6, 7). The builder
is a local tool, not a published page, so Batch 12 is unblocked. Its chrome follows
`assets/belpulse/tokens.css` for colour and type so the two do not clash, and is otherwise the
implementer's judgement.

**The shell** is a desktop tool: laid out for 1440 and 1280 wide. Below 1024 it must show an
explicit "the builder needs a wider window" notice rather than a broken or fake-responsive
layout — an honest refusal, not a squeeze.

**The preview viewport switcher** must match `assets/belpulse/blocks.css`'s real breakpoints
(desktop > 1024, tablet 641–1024, mobile ≤ 640) exactly, or the switcher lies about what ships:

| Label | Iframe width | Exercises |
|---|---|---|
| Desktop | 1280 | 12-column grid |
| Tablet | 834 | 8-column grid |
| Mobile | 390 | 4-column grid |

A screenshot at each of the three, of a real multi-block document, is a required artefact of
the batch report.

## Security constraints — each of these is P0 if broken

1. **The preview iframe keeps a bare `sandbox`.** Never `allow-same-origin`, never
   `allow-scripts`. Batch 11's red-team pass established that `/preview` renders *unvalidated*
   on-disk content on the same origin as the write API; either flag turns any renderer-escaping
   slip into token theft plus arbitrary writes under `config/pages/`.
2. **Consequence the implementer must plan for, not discover:** because the frame has no
   `allow-same-origin`, a `<link rel=stylesheet>` inside the `srcdoc` cannot load. The preview is
   styled by **inlining** the design-system CSS text into the `srcdoc` — text that
   `bootstrap_html()` reads from `assets/belpulse/*.css` and hands to the shell as a string
   constant (12a). The temptation to "just add `allow-same-origin` so the stylesheet loads" is
   the single most likely security regression in this batch. It is forbidden.
   Set `iframe.srcdoc` as a **property**, never by assembling attribute markup. Block highlight
   -on-select works under a bare sandbox because `src/pages/render.py` already emits
   `data-block-id` on each block wrapper: inject a `[data-block-id="…"]` rule into the inlined
   style, after checking the id against the schema's id pattern.
3. **Consequence 2:** `assets/belpulse/blocks.js` does not run in the preview, so chart and map
   blocks show their unhydrated slot. That is correct and must be labelled in the canvas, not
   worked around.
4. **The token** lives in a closure in the parent document only. Never `localStorage`, never
   `sessionStorage`, never a cookie, never a DOM attribute, never a URL after
   `history.replaceState`, never a log line, never a file under `builder/app/`.
5. **`builder/app/` is publicly fetchable.** GitHub Pages serves this repository's root, so
   every file the shell ships is downloadable at the public URL the moment it is committed
   (same mechanism as the committed-draft risk already recorded). Therefore: no token, no
   secret, no absolute filesystem path, no internal hostname in any of those files. Nothing on
   the public site may link to them.
6. **No `innerHTML` with server-derived or document-derived strings**, with exactly one
   exception: the preview `srcdoc`, which is what the sandbox is for. Everything else is
   `textContent` and DOM construction.
7. **No framework, no npm, no build step, no CDN, no external network request** (claude.md
   rules 17 and 30, ADR 0004). Plain HTML/CSS/vanilla JS.
8. **The shell never commits, pushes or tags** (rule 34).

## Accessibility

WCAG 2.1 AA on the shell itself. Every control in this batch is reachable and operable by
keyboard alone — including block add, delete, duplicate, move-up/move-down, panel switching,
viewport switching, and save/validate/publish. Visible focus everywhere. Save / validate /
publish outcomes announced through a live region. The preview iframe must not become a
keyboard trap. An automated axe pass plus a manual keyboard-only walkthrough are both required;
the axe pass alone does not close this.

## Tests

Written by `test-engineer` alongside implementation, not after. No existing test may be
weakened (this has been an explicit finding in an earlier batch and is checked at acceptance).

**12a** (`tests/builder/test_builder_shell_service.py`)

- `/api/registry` returns exactly what `src/pages/registry.py` loads — asserted **against the
  module**, so the two cannot drift, and covering the four top-level keys `load_registry()`
  drops. Plus the negative test that catches the obvious implementation: the response must
  serialise at all (a `MappingProxyType` handed to `json.dumps` is a 500).
- `/` no longer contains Batch 11's demo controls, still carries exactly one token literal, still
  calls `history.replaceState`, and still contains a `sandbox`ed iframe with neither
  `allow-same-origin` nor `allow-scripts`.
- No file inlined into `/` contains `</script`, and the inlining escapes it if one ever does.
- `GET_ROUTES` gained `/api/registry` and nothing else; no new route bypasses `_check_transport`
  or `_check_token`, both of which run before route lookup in `_dispatch`.
- The CSP gate from 12a item 3: whatever policy `/` ends up sending, a real browser still
  applies the inlined stylesheet inside the `srcdoc` preview.

**12b**

- **Round-trip fidelity (the headline test), as restated in the Objective:** load a
  service-written canonical draft needing no migration, save unedited → byte-identical
  `draft.json`. Plus the two announced-change cases: a document that migrates on load, and a
  hand-written draft with non-canonical key order/indentation — both must warn **before** the
  save, and neither may be written silently.
- **Browser JSON numeric fidelity:** a document carrying `1.0`, `1e3`, a large integer and a
  high-precision decimal survives load→save without re-representation, or the shell refuses and
  says so. This is a real hazard with no existing defence anywhere in the pipeline.
- Playwright E2E against a real server instance: create a blank page, add one block of each
  registry type, edit content in all three languages, save, validate a deliberately invalid
  document and see the error land on the right block, publish, check `published.json` changed
  and the previous version was snapshotted, restore, undo/redo across ≥10 operations, switch all
  three preview viewports.
- Every required state above, asserted as a visible, labelled UI state.
- Keyboard-only path through the full create → edit → save → publish flow, no pointer events.
- axe (or equivalent) accessibility scan, zero serious/critical.
- **Source-level assertions** (`test_builder_shell_static.py`): no file under `builder/app/`
  contains `allow-same-origin` or `allow-scripts`; no `localStorage`/`sessionStorage`; no
  external URL; no numeral that could read as an indicator value outside a CSS declaration or a
  version string (invariant 3, mechanised — a reviewer reading for this will miss one).
- Full suite green: `python3 -m pytest -q` and `pre-commit run --all-files`. The base count is
  reported as 1128; **verify it, do not quote it** — a baseline taken on trust is not a baseline.

**RESOLVED by the maintainer, 2026-09-07: Playwright becomes a declared dev dependency and CI
must run the browser tests.** It is assigned to `release-engineer` as its own scoped change with
its own commit (`pyproject.toml` dev extras + the CI workflow, including `playwright install` for
the browser binary — the Python package alone still skips). **The acceptance bar for 12b is
therefore that the browser tests RUN in CI and pass, not that they skip cleanly.** The paragraph
below records the problem as it stood; the skip-guard stays in the test files as a local-developer
convenience, but a skipped run in CI is now a failed gate, not an accepted one.

**Playwright is not a declared dependency, and this is a real gate problem.** It is importable
in this Codespace but appears in neither `pyproject.toml`'s dev extras nor `requirements.txt`,
and CI runs `pip install -e ".[dev]"` then the whole suite. So `test_builder_shell_e2e.py` must
`pytest.importorskip("playwright")` and skip cleanly when no browser is installed, or CI goes
red. The consequence is that **this batch's most important tests will not run in CI**, which
makes them documentation rather than a gate. Therefore: the implementer runs them locally and
pastes the real output into the report, **and** adds a `docs/implementation/known-risks.md` row
saying CI does not execute them. Adding Playwright to `pyproject.toml` would fix this properly
but is a dependency decision outside both allowed sets — flagged to the maintainer, not taken
here.

## Performance limits

Measured on loopback and recorded in the report; a limit with no number in the report is
treated as unmeasured, not as met.

| Limit | Budget |
|---|---|
| Shell interactive after `make builder` + open | < 1.0 s |
| Block selection → inspector populated | < 100 ms |
| Preview refresh, debounce after last keystroke | ≤ 400 ms |
| Preview round trip, 40-block document | ≤ 500 ms |
| Total `builder/app/` payload, uncompressed | ≤ 250 KB |
| Undo depth | ≥ 50 states, with bounded memory (states dropped from the tail, never the head) |
| External network requests made by the shell | 0 |

## Required audits before acceptance

A batch with no reviewer findings recorded is **unaudited**, not clean.

**Revised 2026-09-07: this is ONE `auditor` spawn, not five.** The agent roster is now
`belpulse-lead`, `builder`, `auditor`; the five review roles this table originally named no
longer exist, and their substantive rules live in `auditor.md`. The auditor reviews only the
dimensions a batch actually touched, so it must be told which and why:

| Dimension | In scope? | Why |
|---|---|---|
| **Security / write-path safety** | **Yes — the primary reason to audit at all** | This batch moves the session token into a real browser application and puts a UI in front of a service that writes to the filesystem and replaces published files. Attack the real behaviour, not the comments describing it — one comment in `service.py` already asserted a control that measurement disproved. |
| **Accessibility** | **Yes** | WCAG 2.1 AA on the shell; keyboard equivalent for every pointer action; focus order and visible focus; 200% zoom. An axe pass alone does not close this. |
| **Visual** | **Yes, but not against a reference** | There is no reference design for the builder's own chrome, so this is not a pixel diff. It is: no horizontal overflow, no clipping or overlap at 200% zoom, the three preview viewports (1280 / 834 / 390) actually producing the 12/8/4-column grids, and the `<1024px` refusal behaving as a refusal rather than a broken layout. |
| **Tests** | **Yes** | Specifically: was any existing test weakened, skipped or deleted to make new code pass, and do the browser tests genuinely **run** in CI rather than skipping silently. |
| **Data provenance** | **No — deliberately** | The shell renders no figures of its own; the preview is handed a stand-in, never resolved data. Invariant 3 still applies as a **source-level** check (no fabricated number in the block library, inspector placeholders or empty states), which the batch's own `test_builder_shell_static.py` mechanises. A binding-to-payload provenance audit has nothing to trace until Batch 14. |
| **Release safety / byte-identical rebuild** | **No** | Nothing published renders through any of this yet; `config/pages/` is wired into no exporter until Batch 15. |

## Rollback point

Base commit **ffb63237**, branch `feat/builder-shell`. (Revision 2 named 3270837a; ffb63237 is
3270837a plus a claude.md-only commit adding workflow rules 7-8, and branching off 3270837a would
have reverted those out of the working tree. The change was builder-core's call in 12a and it was
the right one — accepted, no rebase.) **12a is committed at 8f97da65.**

Rollback: delete `builder/app/`, revert `src/builder/service.py` to its 3270837a state, delete
`tests/builder/test_builder_app_routes.py`, `test_builder_history.py`,
`test_builder_shell_e2e.py`, `test_builder_shell_static.py`. Nothing else imports any of it, and
nothing in the static export, the exporters or the public site reads any of it — the same
isolation property Batch 11 verified. `config/pages/` content created while testing is data, not
code: delete the test page directories or leave them; neither affects the public site until an
explicit publish, and Batch 15 has not wired `config/pages/` into the export.

## Known limitations to carry into the report

- **Concurrent save has no conflict detection.** Two open builder tabs silently overwrite each
  other; the document carries a `revision` field the service never reads. Batch 11 recorded
  this; Batch 12 makes it *reachable by an ordinary user for the first time*, because until now
  there was no UI to open twice. Not fixed here (Batch 16), but it must be re-stated in the
  Batch 12 report and the known-risks row updated to say the exposure changed.
- **A saved document's `page_id` field is not checked against its directory** (Batch 11's own
  known limitation). The shell will make this easy to trip by creating pages. Flag it; do not
  fix it unilaterally.
- Chart and map blocks do not hydrate in the preview (security constraint 3).
- No real values in the preview until Batch 14.
- CI does not run the Playwright E2E tests (see Tests). Record it; do not paper over it.
- If no workable CSP for `/` is found, `/` ships with none — today's behaviour — and the
  red-team pass inherits it as an open P2.

## What the architecture pre-check changed, recorded so it is not re-litigated

`repository-architect` checked revision 1 against the code before any implementation. Seven
questions, seven answers; five of them changed this spec. Kept here because four of these are
mistakes a reader would otherwise assume were never made.

| # | Finding | Severity | Outcome |
|---|---|---|---|
| 1 | Round-trip byte-identity is false three ways: `/api/document` migrates, `/api/save` canonicalises (`sort_keys=True`), and an unrecognised prop cannot exist (`additionalProperties: false`) | **P0** | Property restated in the Objective; the passthrough-field requirement struck |
| 2 | The shell could not load itself: `/app/*` required a token header, and `<script src>` / `<link>` cannot send one — `_check_token` allows a query token on `/` only | **P0** | Static route struck entirely; `/` inlines the shell |
| 3 | Nothing serves `assets/`, so the preview CSS was unreachable; the fallback would have been a second copy of the public design system inside `builder/app/` | **P1** | `bootstrap_html()` reads the CSS; no copy, no second allowlist root |
| 4 | `json.dumps`/`asdict` on `BuilderConfig.registry` raises `TypeError` (`MappingProxyType`); `load_registry()` also drops four top-level keys | **P1** | Endpoint builds a plain dict; test asserts against the module |
| 5 | `/` sends no CSP, which is the only reason inline styles work in the `srcdoc` child; adding one later silently kills the preview | **P1** | CSP decided inside 12a, gated by a real-browser test |
| 6 | `test_bootstrap_html_does_not_embed_a_scriptable_same_origin_iframe` breaks under the natural refactor, and `test_builder_api.py` is in neither allowed set | **P1** | Skeleton keeps the iframe; touching that file is an escalation |
| 7 | Playwright undeclared; CI would go red or silently skip. `scripts/serve_builder.py` and the Makefile help both state "API only, no UI yet" and were in neither allowed set | **P1** | Skip-guard + local evidence + a known-risks row; both one-line strings added to 12b |
| 8 | Browser JSON re-representation (`1.0`→`1`, >2^53) has no defence anywhere in the pipeline | **P1** | New required test |
| 9 | A "blank" page needs 8 required root fields and a non-empty trilingual `seo.title`; `route`/`page_type`/`theme` are closed lists the browser cannot fetch | **P1** | New-page is a form driven by `/api/validate`, never a hard-coded copy of the list |
| 10 | `builder/app/` is untracked and empty; `_dispatch` runs transport+token checks before route lookup; the save path validates before creating a directory; nothing outside `service.py` and `test_builder_api.py` imports the route sets | — | Confirmations, no change |

**No new ADR is required** (rule 19): no schema, adapter, geography resolution or analytical
formula is touched, and ADR 0004 already authorises a local `builder/` app served by
`scripts/serve_builder.py`. Two decisions are recorded above rather than in an ADR, because a
later reader would otherwise assume the opposite was chosen: **there is no static-file route**,
and **the design-system CSS is read, never copied**.
```
