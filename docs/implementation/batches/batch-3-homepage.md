# Batch 3 — homepage reproduction

```
Batch: 3 (docs/features/page_builder.md; design docs/design-references/homepage.md)
Base commit: 1c50e0ba (Batch 9: page-document schema, #104)
Final commit: <set at PR merge>
Files changed:
  home.html                          (new, 726 lines -- the page itself)
  assets/i18n.js                     (43 new trilingual keys under "homepage")
  assets/belpulse/charts.js          (3 real bugs fixed, see below)
  assets/belpulse/layout.css         (mobile top-bar overflow fixed)
  assets/belpulse/components.css     (.bp-chip fixed for <button> usage)
  tests/test_statbel_attribution.py  (home.html added to MUNICIPAL_PAGES)
  tests/test_map_ui_logic.py         (home.html added to MAP_PAGES)
  docs/implementation/batches/batch-3-homepage.md (this file)
Requirements completed:
  - Hero: eyebrow, H1, lead, two actions, editorial pull-quote, and the
    choropleth -- the shared map component (assets/commune_map.js), not a
    second implementation (rule 29).
  - Public finances band: shown HONESTLY UNAVAILABLE rather than filled with
    a plausible number (see "What the design asked for that this pipeline
    cannot show" below).
  - Key national indicators: real series from national.json, each with its
    own period and a real line chart (assets/belpulse/charts.js).
  - "Belgium in maps": theme tabs and indicator chips driven entirely by
    metadata/sections.json -- reorder config/local_sections.yaml and the
    homepage follows, with zero code change.
  - Featured commune: picked BY THE DATA (highest value of the first
    configured headline indicator), not hardcoded, per homepage.md's own
    open question.
  - CTA band with real counts from manifest.json (565 / 52 / 17).
  - Full licence attribution, registered in test_statbel_attribution.py
    exactly like communes.html/local.html/map.html, since this page also
    draws municipal figures through the choropleth.
  - Full i18n: 43 new keys, all three languages, zero indicator or commune
    name typed into the interface table (rule 2) -- every label comes from
    the payload.
  - Zero indicator codes in the page source (rule: config change, not a page
    edit) -- verified by tests/test_map_ui_logic.py::test_no_page_names_an_indicator,
    which now covers this file too.
Deferred requirements:
  - Not a page-document/block yet -- this is Batch 3 (visual reproduction),
    not Batch 15 (template conversion). Converting it into blocks against the
    Batch 9 schema is Batch 15's job, once Batch 10 (renderer) exists.
  - Not linked from index.html or promoted to the canonical route. Per
    Batch 15's own cutover gate, a redesigned page stays a preview until
    proven equivalent or better, reviewed independently, and only then
    switched -- never silently. Explicit comment at the top of home.html
    says so. index.html is untouched.
Data-contract impact: none. Reads five already-published payloads
  (metadata/indicators.json, metadata/sections.json, metadata/sources.json,
  national.json, manifest.json) plus the commune/indicator endpoints the
  shared map component already uses. Writes nothing, no exporter touched.
Commands executed:
  python3 -m pytest -q                              (940 passed, from 923)
  python3 -m pytest tests/test_statbel_attribution.py tests/test_map_ui_logic.py -q (81 passed)
  pre-commit run --files <all changed files>        (ruff, black, trailing-whitespace: pass)
  node -c assets/i18n.js                            (syntax valid)
  Playwright, headless Chromium: light + dark theme, desktop (1440px) +
  mobile (390px) viewports, English + French, zero console errors in every
  combination tested.
Tests passed: 940/940 full suite. Zero pre-existing tests weakened.
Screenshots produced: not committed (rule 12), captured to session scratchpad
  only, as every prior batch has done.
Performance results: not benchmarked -- this is a Batch 0-style asset-size
  concern for Batch 15 (real page conversion), not this preview batch. Loads
  five JSON payloads plus the commune boundary file, same as map.html.
Reviewer findings (self-review, all fixed before commit):
  1. FEATURED COMMUNE NAME/REGION BLANK. First pass read `profile.names` and
     `profile.parents`, neither of which exists on communes/{nis}.json --
     the real shape is `profile.name` (trilingual object) and flat
     `profile.province`/`profile.region` strings. Found by rendering the page
     and reading the actual DOM text ("11002" instead of "Antwerp"), not by
     re-reading the code.
  2. MAP CAPTION STATED SOMETHING FALSE. First draft claimed the map "opens
     on average net taxable income" as a GDP-per-capita substitute -- but the
     map actually opens on whatever the config lists first (population, in
     the current config), so the caption was simply wrong about the running
     page, not just imprecise. Reworded in all three languages to describe
     what the map actually does (opens on the first configured headline
     indicator) rather than a specific substitute figure.
  3. TWO INDICATOR-NAMING STRINGS SHIPPED DEAD AND RULE-BREAKING.
     homeMapTitle/homeGdpTitle/homeInflationTitle were drafted before the
     final markup, went unused once the page read titles from the payload
     instead, and typed an indicator's name into the interface table -- a
     rule 2 violation that would have drifted the moment an indicator's
     published name changed. Deleted from all three languages.
  4. CHART CANVASES SIZED AGAINST A GROWING GRID (real bug, not cosmetic).
     .bp-kpi-grid is `auto-fit, minmax(180px,1fr)`; drawing each chart
     synchronously right after appending its card measured the canvas against
     however many siblings existed AT THAT MOMENT, not the grid's final
     column count -- card 1 (alone) got measured at ~1334px, card 2 at
     ~638px, only card 3 (last, with all siblings present) got the correct
     406px. All three charts still LOOKED plausible, just squashed at
     different aspect ratios -- found by comparing the three canvases'
     baked-in pixel widths against each other, not by eye. Fixed in home.html
     by splitting the render into a build pass (append every card) and a
     separate draw pass (measure and draw only once the grid has settled).
     charts.js's own sizeCanvas() also hardened to measure the canvas's own
     getBoundingClientRect() rather than its parent's clientWidth, since a
     wrapped canvas's rendered width and its parent's content width are not
     always the same number.
  5. CHART X-AXIS LABEL OVERLAP, two separate causes:
     a. The skip interval (`34px` guessed, sized for a 4-digit year) collided
        as soon as periods were quarterly ("2020-Q3") or the canvas was
        narrow. Replaced with a MEASURED widest-label interval via
        ctx.measureText, so it self-corrects for any period format.
     b. Fixing (a) still let the second-to-last label collide with the LAST
        label specifically, because the last label is right-anchored (so its
        printed footprint extends further left than its x-coordinate alone
        suggests) while every other label is centred -- a plain "every Nth
        index" rule doesn't know that. Fixed by excluding any interior
        candidate whose centre falls inside the last label's actual left
        edge, computed from its own measured width.
  6. MOBILE TOP BAR CAUSED THE WHOLE PAGE TO SCROLL SIDEWAYS. layout.css's
     .bp-topbar had never been used by a real page below 1024px -- Batch 2
     shipped it against a component gallery, not a live page, and no mobile
     mockup exists for any of the four reference designs (recorded in Batch 1
     as an open item). The unwrapped flex row of logo + nav + two toggle
     groups pushed the DOCUMENT to 803px wide inside a 390px viewport. Fixed
     with a new @media(max-width:768px) block: the bar wraps, the nav scrolls
     within its own row instead of pushing, the CTA band's margins shrink.
     This is a layout.css fix, not a home.html one -- every future page using
     this shell inherits it.
  7. MAP-THEME CHIPS RENDERED NEAR-INVISIBLE IN DARK MODE. components.css's
     `.bp-chip` was written and reviewed only as an `<a>` (Batch 2's gallery
     used one for its static "chip/tag list" demo); home.html correctly uses
     `<button>` for a same-page filter action rather than a fake `<a href="#">`
     link, and a `<button>` carries UA chrome (a light grey face, its own
     text colour) that `.bp-chip` never explicitly overrode. In dark mode
     this produced light text on a light-grey button face -- found by looking
     at a real rendered mobile-dark page, not by reading the CSS, since the
     bug is invisible in light mode where the grey face and the border read
     as "a plain chip." Fixed in components.css (background:transparent,
     font:inherit, cursor:pointer, explicit reset) so the component now works
     identically as either element -- every future button-shaped chip
     inherits the fix. Added an aria-pressed selected state at the same time
     and wired it in home.html, so the chip for whatever indicator is
     currently drawn is visibly marked.
Known limitations:
  - National indicator names in national.json are English-only (no
    trilingual `name` field on that payload) -- the three national KPI cards
    read English text regardless of the reader's chosen language. This is a
    real gap in the national exporter/schema, not something this page can
    fix by typing translations into the interface table (that would be the
    same rule-2 violation reviewer finding 3 just removed). Recorded here for
    whoever scopes the national-payload trilingual work; not blocking this
    batch, since municipal indicator names (used everywhere else on the
    page) are already trilingual.
  - "Public finances" is shown as explicitly unavailable rather than built,
    per component-inventory.md's own finding: this pipeline's government
    series are contributions to GDP growth, not spending/revenue/debt levels,
    so there is no honest figure to show. A real implementation needs new
    national indicators, not a Batch 3 workaround.
  - No mobile mockup exists for any of the four reference designs (Batch 1's
    own recorded gap). The mobile layout here follows this repo's existing
    responsive convention rather than a supplied design.
Rollback procedure: revert the PR. home.html is a new, unlinked file; the
  four shared-component fixes (charts.js, layout.css, components.css) are
  net improvements with no consumer depending on the old (broken) behaviour,
  since Batch 2's gallery is the only other page using these files and none
  of its demos are affected by any of the four fixes. index.html, the live
  front door, is untouched.
Next batch: 4 (municipal profile), 6 (macro) or 7 (micro) -- all remain
  independently available and unblocked. Or Batch 10 (block renderer), which
  can now also draw on real bugs found in this batch (the canvas-sizing race
  in particular is exactly the kind of thing a shared renderer needs to get
  right once, for every block, rather than re-discovering per page).
```
