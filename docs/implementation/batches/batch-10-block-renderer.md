# Batch 10 — shared block renderer

```
Batch: 10 (docs/features/page_builder.md; contract docs/features/block_contract.md)
Base commit: 21d1b98f (Batch 3 second pass, #106) + Batch 4
Final commit: <set at PR merge>
Files changed:
  src/pages/render.py                          (new -- the renderer)
  assets/belpulse/blocks.css                   (new -- the responsive grid)
  assets/belpulse/blocks.js                    (new -- lazy hydration)
  src/pages/__init__.py                        (public surface + stale docstring fixed)
  tests/pages/test_page_document_render.py     (new, 43 tests)
  docs/implementation/batches/batch-10-block-renderer.md (this file)
Requirements completed:
  - ONE renderer, called by both the public build and the builder preview.
  - Every block type the registry declares renders, at every supported
    version (kpi_card v1 AND v2), enforced in both directions by a test.
  - Responsive CSS grid in GRID UNITS, 12 / 8 / 4 columns, all three
    breakpoints emitted in one inline style so a layout survives a redesign
    of the stylesheet.
  - Per-block failure isolation: a block that cannot render becomes a visible
    placeholder and the rest of the page still ships.
  - Lazy-loaded maps and charts, with the 1.2 MB boundary file fetched at
    most once per page and shared by every map block.
  - Byte-identical output for identical input (rule 35).
  - Trilingual (rule 7): language changes the text, never the structure.
Deferred requirements:
  - Binding RESOLUTION stays Batch 14. The renderer takes already-resolved
    data and renders what it is handed; it never fetches or computes a value.
  - Wiring a real page through this renderer is Batch 15 (template
    conversion). Nothing published renders through it yet.
Data-contract impact: none. No exporter, payload, schema or config touched.
Commands executed:
  python3 -m pytest -q                          (999 passed, from 956)
  python3 -m pytest tests/pages/test_page_document_render.py -q  (43 passed)
  pre-commit run --files <changed files>        (all pass)
  node -c assets/belpulse/blocks.js             (syntax valid)
  Playwright: a real document rendered by render.py, served with the real
  CSS and JS -- grid placement verified against the declared layout
  (kpi_card at columns 1-6, chart at 7-12, side by side as declared),
  hydration observed firing, zero console errors, no horizontal overflow.
Tests passed: 999/999. Zero pre-existing tests weakened.
```

## The architectural decision: the renderer is Python

Batch 10's requirement is that "what the maintainer sees while editing is
exactly what ships". The only way to *guarantee* that is one implementation —
two renderers sharing one registry drift the first time a block type gains a
prop, which is the failure this batch exists to prevent.

**Batch 9 left a hint pointing the other way**, and it is worth recording why
it was not followed. `registry.py`'s docstring says the registry lives under
`assets/` so "Batch 10's renderer can fetch the registry in the browser with
no build step". A browser renderer, though, would put every published figure
behind JavaScript — and this repo has two standing commitments against that:

- `scripts/export_local_pages.py` calls its output "the crawler-visible copy
  of the data", which is the entire point of the 565 static commune pages.
- `docs/features/i18n.md` requires that "a reader with JavaScript disabled
  still gets a complete page, licence notice included."

So the renderer is Python and emits static HTML with the figures inline
(rule 30). The builder service (Batch 11) renders its preview by calling the
same function — the builder is a local tool behind `make builder`, so a
process to call is available while editing, and none of that reaches the
published site. The registry staying browser-fetchable is still useful: Batch
12's block-library UI needs it. That is a *different consumer* from the
renderer, which is the distinction the Batch 9 docstring did not draw.

Two block types cannot be finished server-side — a chart is canvas pixels, a
map needs the boundary file — so those emit a slot that `blocks.js` fills.
Everything else, including the whole comparison table, is real HTML.

## Reviewer findings (self-review, all fixed before commit)

1. **A protocol-relative URL passed the renderer's own safety check — a real
   security hole I introduced.** `SAFE_HREF` was
   `^(/[A-Za-z0-9._~/-]*|#...)$`, and every character of `//evil.example`
   after the leading slash is inside that character class, so it matched. A
   browser resolves `//host` against the current scheme and leaves the site
   entirely — exactly what rule 23 exists to stop, sailing through the guard
   whose only job is to stop it. Found by my own parametrised test, not by
   reading the regex. Fixed with a `(?!/)`, plus a separate `..` rejection,
   since `.` and `/` are both legitimate path characters and
   `/a/../../etc/passwd` satisfies the class perfectly well.

   **Worth knowing: the same shape is in `registry.json`'s own
   `relative_url` pattern**, whose description promises "no protocol-relative
   //host" while the pattern does not enforce it. I verified against the real
   validator that Batch 9's `semantics.py` catches all of these separately as
   `unsafe_content` (`//evil.example`, `/a/../../etc/passwd`,
   `https://evil.example`, `javascript:`) — so **no document was ever
   accepted**; only my renderer's independent guard was fooled. That is
   precisely the case for defence in depth being redundant on purpose.

2. **The renderer labelled blocks with a state but never wired them into the
   state component.** The wrapper carried `data-state="loading"` but not the
   `bp-state` class and no body/message children, so none of Batch 2's seven-state
   CSS engaged: a KPI whose data had not resolved rendered as an ordinary
   card with an empty value — a missing figure looking like no figure, the
   exact failure the states were built to prevent. Caught by rendering the
   output in a browser and looking at it; the test asserting "state != ready"
   passed the whole time, because the state attribute was right and only the
   presentation was missing. Now stateful blocks get the full component
   wrapper, and `ready` deliberately carries no message so a real measured
   zero can never read as an absence (rule 26).

3. **One of my own tests asserted nothing.** `assert ... or True` — the
   dead-assertion pattern I have flagged in earlier batches, written into this
   one. Replaced with real assertions that state the division of
   responsibility: `text_in` is a lookup and returns the string as stored,
   `esc` is what makes it safe on the way out.

4. **A test compared page structure too naively** and failed on correct
   output: it stripped text nodes but not attribute values, and an accessible
   name is a translated string living in `aria-label`. The renderer was right;
   the test was wrong. Fixed the test rather than the renderer.

## Known limitations

- The props schemas in the registry are still marked `provisional`, as Batch 9
  left them. Rendering them for the first time did not require changing any,
  which is a useful signal, but they should be reviewed once a real page is
  converted (Batch 15).
- `blocks.js` hydrates on `IntersectionObserver` and falls back to hydrating
  everything immediately where that is unavailable — correctness over
  laziness, deliberately.
- No published route renders through this yet. Wiring the first real page is
  Batch 15's job, behind its own cutover gate.

Next batch: 11 (builder service) consumes this renderer for its preview, and
is where `security-red-team` review becomes due — Batch 9 deferred it to 11
and 14 precisely because there was no attack surface until then.
