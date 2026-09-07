# Batch 2 — shared presentation components

```
Batch: 2 (docs/features/page_builder.md)
Base commit: 59992ab7 (Batch 1: BelPulse design tokens, #102)
Final commit: <set at PR merge>
Files changed:
  assets/belpulse/layout.css       (new)
  assets/belpulse/components.css   (new)
  assets/belpulse/charts.js        (new)
  assets/belpulse/components.js    (new)
  assets/belpulse/tokens.css       (added --bp-on-accent)
  docs/design-references/component-gallery.html (new)
  tests/pages/test_shared_components.py (new, 53 tests)
Requirements completed:
  - Two page shells (analytical: top bar + left sidebar; editorial: top bar
    only, breadcrumb, hero, optional right sidebar) as reusable CSS, matching
    the two shells found in Batch 1's design-reference transcription.
  - Shared chart renderer (assets/belpulse/charts.js) generalising
    local.html's gc()/niceSteps()/drawSeries() into four canvas renderers
    (line, bar, donut, horizontal ranking) driven only by --bp-chart-1..8.
  - Component library (assets/belpulse/components.css): KPI card (full/
    compact/mini), stat tile, list panel, chart container, comparison table,
    ranking list, simulated live counter (with a hard runtime guard against
    ever being wired to real data), pull-quote, news card, commune preview
    card, feature/benefit tile, chip list, freshness/grade badge, map-card
    frame (reuses assets/commune_map.js per rule 29, not reimplemented).
  - Small interactive JS (assets/belpulse/components.js): tabs, the two-track
    theme toggle (same localStorage/data-theme pattern as
    assets/commune_map.css), the simulated-counter driver.
  - Component gallery (docs/design-references/component-gallery.html)
    demonstrating every component, plus a dedicated 7-state demonstration
    (loading/ready-incl.-explicit-zero/missing/suppressed/unavailable/error)
    for the two components that actually carry data state (KPI card, chart
    container, list panel) -- static components (news card, pull-quote,
    feature tile, chips) carry no data state and are shown ready-only.
Deferred requirements: none for this batch.
Data-contract impact: none. No exporter, schema, or payload shape touched.
Commands executed:
  python3 -m pytest -q                                  (754 passed)
  python3 -m pytest tests/pages/test_shared_components.py -q  (53 passed)
  pre-commit run --files <all new/changed files>         (ruff, black, trim-whitespace: pass)
  Ad hoc Playwright check (not part of the pytest suite, matching this
  repo's existing convention that DOM/canvas rendering is verified manually --
  see tests/test_communes_map_panel.py's own docstring): loaded the gallery
  headless in Chromium, zero console errors in both light and dark theme,
  full-page screenshots inspected in both themes, tab-switching interaction
  exercised programmatically (aria-selected / hidden state verified).
Tests passed: 754/754 (full suite), including all pre-existing tests
  unmodified.
Screenshots produced: not committed (rule 12) -- captured to the session
  scratchpad only, as Batch 0 established.
Performance results: n/a -- no page wired up to load these assets yet
  (that is Batches 3/4/6/7).
Reviewer findings (self-review, fixed before commit):
  - Duplicate `id="ranking-list"` on both an <h2> and the demo <ol> caused
    getElementById to silently resolve to the heading in some engines,
    leaving the actual list empty. Caught by rendering the gallery and
    inspecting the screenshot, not by reading the HTML. Fixed by renaming
    the list's id.
  - Two raw `#fff` values in layout.css (primary button label, active
    sidebar-nav item) violated the "no raw colour outside tokens.css" rule.
    Fixed by adding a new semantic token, `--bp-on-accent`, rather than
    reusing an existing background token for an unrelated purpose.
  - charts.js's gc() fallback used a raw hex (`#888`) for the case a CSS
    variable is missing entirely. Replaced with the `currentColor` keyword,
    which needs no token and degrades sensibly (inherits the surrounding
    text colour) instead of hardcoding a guess.
Known limitations:
  - The logo/wordmark conflict from Batch 1 is still unresolved; layout.css
    still ships the same documented placeholder mark.
  - No tablet/mobile mockups exist (Batch 1 finding, unchanged); layout.css's
    single breakpoint collapse is this repo's own existing convention, not
    informed by a reference design.
  - The simulated-live-counter's "why 2 per tick" pacing is illustrative,
    not derived from any real jobseeker-registration rate -- it is
    explicitly a decorative animation, never a data source (enforced at
    runtime by initSimulatedCounter's guard).
Rollback procedure: revert the PR; nothing outside assets/belpulse/ and
  docs/design-references/ and tests/pages/ is touched, and no page currently
  loads these files, so a revert has zero effect on any published route.
Next batch: 3 (homepage) -- or 4/6/7, per whichever the maintainer prioritises;
  all four are now unblocked on both design and shared components.
```
