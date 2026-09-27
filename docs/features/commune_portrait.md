# Commune portrait ("Portrait" layout)

Replaces the Batch 4 commune.html layout. Approved by the maintainer 2026-09-26 from the v4c
mockup (`screenshots-review/commune-beta/v4b/index.html`), rebuilt against `origin/develop`'s
real `public/data/**` and shared assets rather than the mockup's own stale data snapshot.

Derived from `screenshots-review/commune-beta/v4-saved/DESIGN_V4.md` (original spec) and
`DESIGN_V4C.md` (the latest round of maintainer feedback). See those files for the full design
rationale; this document records what actually shipped and how it maps onto this pipeline's
real data contracts.

## URL and query parameters

Unchanged: `commune.html?nis=<nis>` (default 92094), `&from=<key>=<value>` for the breadcrumb
back-link (revalidated against `public/data/metadata/geographies.json` and
`public/data/metadata/typology.json` before being trusted -- `parseFrom()`/
`resolveBreadcrumbBack()`, carried over byte-for-byte from the previous layout). Every existing
canonical and legacy URL to this page remains valid (rule 31).

## Structure

1. Shared header, breadcrumb, footer, licence attribution -- verbatim from the previous layout.
2. Hero: photo, name, tagline, a 300px locator map (region/province/neighbours tinted, zoom,
   click to open a commune), the commune switcher, and the header actions (Compare / Share /
   Download).
3. "L'essentiel": headline figures (`metadata/sections.json`'s `headlines`), each with a mini
   sparkline in its chapter's colour and the national position strip.
4. A sticky chapter nav, scroll-spied.
5. One chapter per theme with data (`metadata/sections.json`'s `sections`, in that order), each
   carrying its own hue assigned by its POSITION in that array -- never by an indicator or theme
   id (rule 2/24). Per chapter: a header (title, blurb, "so what" rank sentence), a lead row
   (chart 7 cols + choropleth 5 cols, re-renderable together), a small-multiples grid of the
   chapter's other indicators, a context row, and a footer naming the chapter's sources and
   update dates.
6. "Toutes les données": every indicator, every published period, grouped by theme, sortable by
   opening a history disclosure per row -- ported outright from the previous layout's
   `<details id="allData">` (same ids, same renderer, same five distinct data states), because
   `tests/test_a4_finishing_fixes.py` and `tests/test_map_ui_logic.py::
   test_belgium_refnis_label_never_appears` pin that exact structure.

## Context-row placement (section id -> block)

Attached through one small page-level map from a `sections.json` section id to a block builder
(`CONTEXT_BLOCK_BUILDERS` in commune.html), keyed on section ids only, never indicator ids
(rule 24):

- `demography`: the age-composition donut (config-driven, `sections.json`'s `compositions`),
  the age-pyramid timeline, and the neighbours map+bars.
- `housing`: buyers'-origin flows (`docs/features/commune_flows.md`).
- `households`: the Schools ISE panel (`docs/features/schools_ise.md`).
- every other chapter with data: the neighbours bars for that chapter's headline indicator.

A missing payload degrades to the honest state for that block alone (a hidden card for flows, a
three-state `not-applicable`/`unavailable`/`ready` panel for schools) and never blocks the rest
of the chapter.

## Age-pyramid timeline (this batch's core ask)

`public/data/demography_history/<nis>.json` (PR #267): `bands` (age-band labels) and `years[]`
(one entry per published Statbel year, 2010 onward, each with `period`, `reference_date`,
`male[]`, `female[]`, `coverage{found,expected}`). Falls back to today's single-year
`public/data/demography/<nis>.json` when the history payload is absent or does not match the
current commune -- an existing view with no history file never breaks.

- Controls: a Play/Pause button (`aria-pressed`, keyboard reachable, hidden under
  `prefers-reduced-motion`), a range slider over every year, a large current-year label, and
  year ticks under the slider (first year, roughly every 4 years, last year).
- Play advances about 700ms per year from the current position and loops back to the first year
  once the last is reached.
- The axis maximum is fixed across every year the payload has, so a shape change reads as real
  population change, never a rescaling artefact.
- The earliest year's own bars are drawn as a thin outline behind the current year's bars (a
  `<b class="earliest-outline">`, never an `<i>`, so it never collides with the real bar's own
  `.pyramid-track i` selector), with a legend entry, so the change since the earliest year reads
  at a glance without needing to move the slider.
- An incomplete year (a merged commune whose predecessor rows were missing that year,
  `coverage.found < coverage.expected`) carries a visible note; the payload is never gap-filled.

Implementation carried over unchanged from the previous layout's `renderAgeSex()`/
`drawPyramid()`/`renderPyramidYear()`/`pyramidStartPlaying()`/`pyramidStopPlaying()`/
`wirePyramidHistoryControls()` (PR #275) -- same ids (`#pyramidPanel`, `#pyramidChart`,
`#pyramidHistory`, `#pyramidPlay`, `#pyramidYear`, `#pyramidYearOut`), same i18n keys
(`cpPyramidPlay`, `cpPyramidPause`, `cpPyramidYearLabel`, `cpPyramidHistorySub`,
`cpPyramidIncomplete`), same tests (`tests/test_age_pyramid_slider.py`). The only genuinely new
pieces are the earliest-year outline layer and the tick row.

## Shared assets touched

- `assets/commune_map.js`: adds `palette`, `divergingPalette`, `nodataColour`, `strokeColour`
  constructor options and a `setPalette()` method. Every existing caller that passes no options
  keeps drawing exactly as before (`var(--ramp-N)`, no diverging path, `var(--nodata)`, the CSS
  stylesheet's own stroke rule) -- verified in `tests/test_commune_map_palette_options.py`. The
  `tickLabel()` precision fix from the design round is deliberately NOT ported: this page
  corrects its own legend ticks locally (`fixLegendTicks`/`fixLegendCurrency`) rather than
  changing shared behaviour every other map page would also inherit.
- `assets/i18n.js`: every `v4*` key the page uses, plus `cpNeighboursValue`, added in fr/nl/en,
  merged alongside the existing keys (nothing removed or reworded).

## Comparable communes (PR C, feat/commune-comparables)

Adds the "comparable communes" layer to every non-additive chapter's lead chart and small
multiples, from `public/data/peers/<nis>.json` (docs/features/peer_model.md) -- the page
computes no statistic; every median, position and deviation shown is read straight from that
file (rule 4/27).

- **The switch.** A per-chapter `<fieldset class="sim-toggle">` (Hide / Same region / All of
  Belgium), default "Same region" (maintainer decision, 2026-09-27). Persisted to
  `localStorage.belpulse-similar`; every chapter's switch stays in sync without a rebuild, so
  clicking one never drops keyboard focus elsewhere on the page.
- **Grey lines.** Line-mode leads only, drawn from every member of the active list whose profile
  has loaded, behind the commune's own line (`assets/belpulse/similar.js`'s `peerRuns`, reusing
  the chart's own `medianStep` converted to pixel space). Loaded lazily (IntersectionObserver,
  300px rootMargin) and cached module-wide across language/list switches
  (`simProfiles`/`loadSimProfiles`).
- **The % badge.** `.sim-badge`, a full-width row under the lead's header: "X% above/below the
  median of comparable communes (list)", or one of six withheld states (no_pct_zero/negative/
  other/config, few_peers, stale, generic, mismatch) -- never a computed value, never "average",
  never green/red or better/worse wording.
- **Tiles.** `.mtile-sim` under `.mtile-period` on every non-additive small-multiple, updated in
  place on a list switch (never a tile-grid rebuild).
- **"Quelles communes ?"** (`.sim-which`, `simWhich`/`simWhichNote` -- the page never uses the
  word "peer") -- every member of the active list, in rank order, each linking to `?nis=<nis>`;
  a peer cell distinguishes suppressed, na, "no figure for this period" and "not loaded" as four
  states (rule 26), never collapsed. `comparables.html?nis=<nis>` is not linked from this PR
  (out of scope; the Block M session's own page covers it).
- **Hover/touch/keyboard.** `nearestPeer` (assets/belpulse/similar.js) picks the closest peer
  point within tolerance and closer than the commune's own point; ArrowDown/ArrowUp cycle
  commune -> peers by value -> commune; Enter/click opens the hot peer; touch never navigates.
- **Naming.** "peer" already means province mates elsewhere on this page (`peerGeographies`,
  `buildPeerStrip`, `v4PeerChartTitle`) -- every new class, id and function here is `sim-`/
  `simXxx` only, checked by `tests/test_similar_logic.py`'s static word scan.

Not built in this PR (see the build spec's "Explicitly not done"): no peer-median line over
time, no median diamond, no points difference, no similarity score, no lines in sparklines or on
additive indicators, no map overlays, no red/green or better/worse wording.

Depends on PR A (`peer_model.md`) having merged and a daily run having published `peers`,
`min_peers_with_value` and `withheld` on `public/data/peers/<nis>.json`, and on A4's
`peer_deviation: none` override (POPULATION_CHANGE_5Y, POPULATION_CAGR_10Y,
MUN_EXPENDITURE_GROWTH_1Y, MUN_REVENUE_GROWTH_1Y) having been exported into
`metadata/indicators.json` -- until then this PR's CI cannot pass and its browser tests for
those four indicators route the flag onto the fixture directly (see
`tests/test_commune_comparable_browser.py`'s `test_92094_population_change_5y_has_no_pct_line`).

## Rules kept

No indicator id appears anywhere in this file (rule 24, `tests/test_map_ui_logic.py::
test_no_page_names_an_indicator`, and this batch's own `tests/test_commune_portrait_static.py`).
No figure, commune or indicator is hand-typed (rule 36) -- every value comes from
`public/data/**`. Source, unit, period, status and freshness come from metadata only (rule 28).
Missing, unavailable, suppressed, not-applicable and an explicit zero stay five distinct states
(rule 26) -- strips and maps skip `na`/`suppressed` cells and never show them as values. The
only browser arithmetic is the province-peer rank count and a count's share of its province
total (rule 27) -- no ratio is ever averaged across communes. The shared map engine is reused,
extended only through instance-level options (rule 29).
