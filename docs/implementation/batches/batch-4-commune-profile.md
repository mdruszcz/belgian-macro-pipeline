# Batch 4 — commune profile redesign

```
Batch: 4 (docs/features/page_builder.md; design docs/design-references/municipality-profile.md
  plus the high-fidelity Namur design supplied 2026-09-07)
Base commit: 21d1b98f (Batch 3 second pass, #106)
Final commit: <set at PR merge>
Files changed:
  commune.html                       (new -- the page, a preview route)
  assets/i18n.js                     (62 new trilingual keys under "commune profile")
  tests/test_statbel_attribution.py  (commune.html added to MUNICIPAL_PAGES)
  tests/test_map_ui_logic.py         (commune.html added to MAP_PAGES)
  docs/implementation/batches/batch-4-commune-profile.md (this file)
Requirements completed:
  - The full designed layout: header with badges and actions, three-card hero
    (photo frame / locator map / intro + six stat tiles), section tabs, a
    two-column body, and the dark CTA + light footer the redesign now shares.
  - Every panel that has real data behind it is filled from real data:
    six key-figure cards, an indexed multi-series evolution chart, the
    comparison table (province / region / country, straight from each
    indicator's own published `comparison` block), per-indicator national
    ranks from the published `percentile`, thematic-focus tiles, and the
    commune drawn on the national map.
  - Every panel that does NOT have data is built at its designed size and
    position and rendered in the `unavailable` state with the specific
    reason, so the page doubles as a live checklist of the data work left.
  - Config-driven throughout: `headlines` drives the key figures, the hero
    tiles, the comparison rows and the ranks; `sections` drives the tabs and
    the focus tiles. ZERO indicator codes in the page, enforced against this
    file by test_no_page_names_an_indicator.
  - Licence notice carried and registered alongside communes.html /
    local.html / map.html / home.html, since this page publishes municipal
    figures.
  - Works for any commune (?nis=), not just the designed one -- verified
    against a second commune.
Deferred requirements:
  - Not a page document / block yet (Batch 15), and not promoted over
    local.html or the 565 static local/{nis}/ pages, which are untouched.
    Batch 15's cutover gate decides that, never a silent switch.
Data-contract impact: none. Reads already-published payloads only; writes
  nothing; no exporter, schema or config touched.
Commands executed:
  python3 -m pytest -q                                   (956 passed, from 940)
  pre-commit run --files <changed files>                 (all pass)
  node -c assets/i18n.js                                 (syntax valid)
  Playwright: desktop 1440px + mobile 390px, EN + FR, two different communes,
  zero console errors, no horizontal overflow.
Tests passed: 956/956. Zero pre-existing tests weakened.
Screenshots produced: not committed (rule 12).
```

## What the design asks for that this pipeline cannot show

Every one of these was **verified against the data, not assumed**. Each is a
missing data source, never a layout decision, and each is built at its
designed size and marked unavailable with its own reason:

| Panel in the design | Why it cannot be filled |
|---|---|
| Local finances (budget, spending, revenue, debt/inhabitant, debt trend) | Needs the Walloon municipal-finance source. **No config for it exists** — checked, zero matches. |
| Employment by sector | No sector breakdown is collected for communes at all. |
| Neighbouring communes (list and map) | **This pipeline holds no commune-adjacency table.** The comparison panel compares against province / region / country instead, which are published. |
| Named composite ranks ("economic dynamism", "residential attractiveness") | Need a peer model and a fiscal score, neither built. Per-indicator ranks **are** real and are shown. |
| Hero photograph and its editorial tagline | No photo library, no per-commune editorial copy. |
| Population by **statistical sector** | This pipeline holds commune boundaries only. |

And three where the design's label and the real figure differ, so the real one
is used and named as itself: the income figure is an **average** net taxable
income per return (the design says median), the housing figure is a median
**sale price in euro** (the design says a price per m²), and the age split has
**three** bands here, not four.

## The age donut: the data is there and it still cannot be drawn

This is the most interesting finding in the batch, and it is a **gap in the
data contract**, not in the page.

This pipeline holds three age bands whose latest values sum to **exactly** the
published population (16,782 + 73,781 + 24,894 = 115,457 for Namur). A donut
of shares is therefore computable in principle. It still cannot be built,
because **nothing published says which indicators partition a whole**. The
metadata index row carries unit, additivity, direction, decimals, coverage and
provenance — no part-of-whole relationship. Other additive counts (women,
married people) sit in the same configured section and are indistinguishable
from an age band by any published property.

That leaves only two ways to draw it, and both are refused:

1. **Hardcode the three indicator names** into the page — forbidden by the
   50%-gate rule, and enforced against this very file by
   `test_no_page_names_an_indicator`.
2. **Guess a subset that happens to add up.** The first implementation did a
   version of this and got it wrong: it took every additive count in the
   demography section, so women and married people were swept in, the sum blew
   past the population, and the panel silently refused. A subset-sum search
   would have "worked" today and produced a **silently wrong donut** the first
   time another additive count joined that section.

So the panel states what is missing. **Adding a part-of declaration to
`config/indicators/*.yaml` turns it on with no change to this page** — which is
the whole point of building it config-driven.

## Reviewer findings (self-review, all fixed before commit)

1. **The locator map rendered as an empty box.** It used the shared
   component's `focus()`, which outlines the commune *and zooms to it* — at
   that zoom the card showed one flat shape and read as a broken render. A
   locator's entire job is showing *where in the country* a commune sits.
   Fixed by highlighting via the component's own published `data-nis`
   attribute instead, leaving the national view intact. No reimplementation.
2. **The evolution chart indexed rates to base 100** — meaningless, and
   visibly wrong: an unemployment rate rebased to 100 sat at 400 on the axis
   beside real levels, reading as a series that had quadrupled. Now restricted
   to levels (counts, euro amounts); rates are excluded.
3. **Series with different periods were plotted against each other.**
   `charts.js` places every series against the *first* series' point
   positions, so an annual series and a quarterly one silently slid out of
   alignment. Now every series on the chart must share the same periods.
4. **…and the first fix for (3) was too strict**, then the second was biased.
   Intersecting *all* candidates gave an empty set (annual never intersects
   quarterly), so the panel drew nothing; seeding from the *longest* series
   picked the quarterly one and excluded both annual series, leaving a
   one-line "comparison". Now every candidate is tried as a seed and the group
   with the most comparable series wins — deterministic, so the same data
   always yields the same chart.
5. **The ranking panel asserted one denominator for every row.** The ranks
   genuinely have different ones — some indicators rank against 565 communes,
   others against 581 (**the pre-2019-merger commune map, which some sources
   still publish on**), others against however many carry a value. The
   subtitle took whichever row came last and stated it for all of them. It now
   states a single total only when every rank really shares one. Worth noting:
   the design's own "581 communes" is not purely aspirational — it is a real
   commune vintage that appears in this pipeline's own percentile data.
6. **Three of the four finance tiles rendered as blank white boxes**, which
   reads as a broken render rather than a deliberate "we do not have this".
   All four now carry their designed label and an em-dash, with the reason
   stated once beneath the row.

## Known limitations

- The CTA reads the real counts from `manifest.json` (565 / 52), not the
  design's aspirational "581 communes / 200+ indicateurs".
- Tabs scroll the page to the thematic-focus grid rather than filtering the
  body to one section; per-section filtering is a data-binding question that
  belongs with Batch 14, not a visual-reproduction batch.
- The "Compare" sidebar picker is presentational here — the real four-way
  peer comparison already exists on `local.html` and is not duplicated.

Next batch: 6 (macro) or 5 (commune explorer); 10 (block renderer) remains the
keystone of the builder track.
