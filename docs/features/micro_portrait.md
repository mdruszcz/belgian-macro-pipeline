# Micro Portrait

Requested 2026-09-28: same design as the macro page.

Copies macro.html's own Portrait redesign (docs/features/macro_portrait.md, itself a copy of
commune.html's shipped design) onto micro.html. Reuses commune.html's hero typography, topic
pills, essentials-band styling, and SVG line-chart renderer
(assets/belpulse/macro_portrait_charts.js, a factory copied from commune.html's own chart code --
no indicator id or figure lives in it, only the language/translation hooks micro.html supplies).
Keeps the existing national/aggregates payloads, multilingual labels, seven chapters, history/hash
navigation, and unavailable states. Styling is shared with macro.html: both pages link
assets/belpulse/macro_portrait.css, whose classes that name no chapter-specific behaviour (hero,
pill strip, essentials band, chart cards, portrait-metrics tiles, chapter numbering) are reused
byte-for-byte from the `.macro`-scoped block, mirrored under a new `.micro`-scoped block appended
to the same file. Nothing under `.macro` was touched, and nothing under `.micro` can reach
macro.html (verified: macro.html's own screenshots and tests, taken before and after this batch,
are unchanged).

The redesign moved from a one-panel-visible-at-a-time layout (panels.js toggling `hidden`, a
`<select id="panelPick">` standing in for the sidebar on phones) to macro.html's own shipped
pattern: every chapter scrolls on one page, and a sticky horizontal pill strip (still
`.bp-sidebar`/`.bp-sidebar-nav`) is the nav at every width -- no separate phone picker, matching
macro.html and commune.html, neither of which has one either. panels.js is no longer loaded by
micro.html. The scroll-spy that keeps `aria-current` on the right pill is macro.html's
`updateCurrentChapter` (precomputed, clamped "claim points" compared directly against scroll
position), copied verbatim rather than the IntersectionObserver band the old code never used --
this page has the same shape of short trailing chapter ("Entreprises", every card unavailable)
that made the band approach fail for macro.html's "Finances publiques" and "Europe".

## What changed technically

- Every time-series chart (the KPI band's sparklines, the "Key indicators"/chapter list tiles, the
  housing-market history) moved from `<canvas>` (assets/belpulse/charts.js) to the SVG renderer in
  assets/belpulse/macro_portrait_charts.js -- the same factory macro.html uses, `portrait.line()` /
  `portrait.spark()`, wired through the page's own `drawPortraitChart()`.
- Two charts stayed on canvas, deliberately: the province/Belgium ranking bar chart
  (`#compareCanvas`, `BPCharts.drawRanking`) and the choropleth map (assets/commune_map.js, rule
  29). Neither has an SVG Portrait equivalent to reuse -- macro_portrait_charts.js's factory only
  draws a line/spark/bar-of-one-series time series, not a ranked-geography bar chart or a
  choropleth -- and this batch did not build a third chart renderer to replace two working ones.
  The map keeps reusing the shared engine exactly as before; no second map implementation.
- The housing-market history chart (`config/micro_sections.yaml`'s `history.chart: bar`, previously
  `BPCharts.drawBar` on canvas) now draws as a line, through the same `drawPortraitChart()` every
  other lead chart on the page uses. `history.chart` no longer changes anything: a config hint from
  the old canvas renderer has no SVG bar-chart-over-time equivalent to reuse (commune.html's own
  Portrait design never drew a time series as bars either), and a line reads the same annual
  sales-transaction trend honestly, with the same axis/tooltip/data-table behaviour as every other
  chart on the page.
- The household tile grid (2x2, Population chapter), the housing-stock/income/employment/
  demographics list cards, the region table (Logement) and the province comparison/map
  (Comparaisons) keep their existing data bindings and generic renderers (`renderListInto`,
  `buildEntry`, `comparisonRows`, `regionalHousingRows`) unchanged in logic -- only their markup
  moved from a one-at-a-time `<section data-panel hidden>` to an always-visible Portrait chapter.

## Bugs found and fixed while building this

1. The `.macro`-scoped composition rules in `assets/belpulse/macro_portrait.css` (hero grid, pill
   strip, essentials band, chart-card border/dash, portrait-metrics grid) only ever applied to
   `body.macro` -- linking the stylesheet from micro.html (`body.micro`) alone left the page
   effectively unstyled and overflowing horizontally at 390px (measured: `scrollWidth` 602px in a
   390px viewport). Fixed by mirroring a `.micro`-scoped block, parallel to `.macro`'s, at the end
   of the same file.
2. The choropleth legend's tick labels (assets/commune_map.css, the shared engine) extended a few
   pixels past the card edge at 390px -- the same overflow the old panel-based micro.html clipped
   with its own `.map-card .legend, .map-card .ticks{overflow-x:hidden}` rule. Reapplied as
   `.micro .leadmap-legend .ticks{overflow-x:hidden}` inside the same `@media (max-width:640px)`
   block, page-level only, never touching the shared engine's own CSS.
3. `.row-3` is reused by two rows with a different card count on this page -- Overview's key+news
   (2 cards) and Entreprises' three unavailable cards (3 cards) -- and a fixed-column-count track
   correctly fit one and squeezed the other, leaving Entreprises' third card wrapping alone onto
   its own row at 1440px. Fixed with `repeat(auto-fit, minmax(300px, 1fr))`, which gives each row
   the column count that actually fits its own children.

## Known, not fixed (out of scope)

The comparison ranking chart's value labels (`BPCharts.drawRanking`, canvas, unmodified by this
batch) can print a few pixels short of a wide formatted number ("36,669." instead of "36,669.4")
at 390px -- a pre-existing limit of that renderer's fixed-offset label placement, not something
this redesign introduced or regressed. Fixing it would mean editing shared, untouched
assets/belpulse/charts.js, outside this batch's scope (touching a file other pages also load, for
a canvas chart type this batch deliberately did not rebuild in SVG).

No per-chapter intro/description text exists anywhere in config/micro_sections.yaml,
public/data/metadata/micro_sections.json, or the i18n tables -- chapter headers stay number + title
only, per rule 36 (never hand-type copy that has no source), matching macro.html's own Portrait.

## Verification

Verified desktop (1440px) and phone (390px) layouts, three languages (en/fr/nl) and three themes
(light/dark/paper), zero console errors, no horizontal overflow at any width, every chapter's KPI/
list/tile/table/map/comparison renders real values from the existing payloads, and macro.html's own
screenshots and test suite are unchanged before and after this batch's CSS addition.
