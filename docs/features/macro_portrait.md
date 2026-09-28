# Macro Portrait

Requested 2026-09-27: copy the shipped commune Portrait design onto macro.html.

Reuse commune.html's hero typography, topic pills, essentials-band styling,
and SVG line-chart renderer (assets/belpulse/macro_portrait_charts.js, a
factory copied from commune.html's own chart code -- no indicator id or
figure lives in it, only the language/translation hooks macro.html supplies).
Keep the existing national payloads, multilingual labels, seven panels,
history/hash navigation, and unavailable states. The existing skyline fills
the visual slot; no new image source is introduced. The title and freshness
remain visible above every chapter. Styling is scoped in
assets/belpulse/macro_portrait.css so the commune page is unaffected.

The redesign moved from a one-panel-visible-at-a-time layout (panels.js
toggling `hidden`, a `<select id="panelPick">` standing in for the sidebar
on phones) to commune.html's own shipped pattern: every chapter scrolls on
one page, and a sticky horizontal pill strip (still `.bp-sidebar` /
`.bp-sidebar-nav`) is the nav at every width -- no separate phone picker,
matching commune.html, which has none either. panels.js is no longer
loaded by macro.html. Charts moved from `<canvas>` (assets/belpulse/
charts.js) to the SVG renderer above; there is no `<canvas>` left anywhere
in the page's own chart markup.

Two real bugs found and fixed while building this (not just test updates):
1. The essentials band's value+unit (e.g. "87,7 2021=100") could overflow
   into the next card at some widths. Fixed with macro.html's
   `shrinkEssentielValuesToFit()`, copied verbatim from commune.html's own
   function of the same name -- shrinks the value's font size until it
   fits, down to a 14px floor, then lets `overflow:hidden` clip cleanly
   rather than spill.
2. The pill strip's scroll-spy (which pill is `aria-current`) used an
   IntersectionObserver band, which has a dead zone for a short trailing
   chapter: "Finances publiques" and "Europe" could never become current,
   because native anchor-scroll cannot bring their heading far enough up
   the page to enter the band -- confirmed live, not a guess. Replaced
   with `updateCurrentChapter()`, a direct comparison of each chapter's
   position (via precomputed, clamped "claim points") against the real
   scroll position; also works around a pre-existing, out-of-scope defect
   in the closed Europe country-picker `<details>` (assets/belpulse/
   europe_map.js, untouched by this batch), which keeps its panel content
   in normal document flow even while collapsed on the Chromium build this
   repo's Playwright pins, inflating `document.*.scrollHeight` by
   thousands of pixels past what the page can actually be scrolled to.
   A related fix: native anchor-scroll (both the initial page load and a
   later pill click) could land short of a chapter's real position on a
   slow connection or near the end of a long page; `afterLayoutSettles()`
   re-applies the same anchor once layout has actually settled, whether
   that is the very first `load` or a `hashchange` from a click.

No per-panel intro/description text exists anywhere in config/
national_sections.yaml, public/data/metadata/national_sections.json, or the
i18n tables -- the design mockup shows one line of chapter intro copy under
each heading, but nothing here authors that text: chapter headers stay
number + title only, per rule 36 (never hand-type copy that has no source).
Likewise no headline figure sits beside a chapter title: commune.html's own
version of that slot is a peer-rank ("9e / 565"), a concept national
indicators have no equivalent of.

Verify desktop and phone layouts, three languages and themes, populated
KPIs, no essentials-band overflow, and every pill (including the two short
trailing chapters) correctly becoming `aria-current` on both a click and a
deep link. Existing panel contracts continue to run on CI.
