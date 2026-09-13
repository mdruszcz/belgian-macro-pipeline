# Batch 8b — the map page restyle

`map.html` moves onto the design system and becomes a one-screen map explorer: the map fills the
viewport, the controls sit beside it, and an area picker, five palettes, a compact legend and a
distance scale are added. The map engine gains one additive hook and is otherwise untouched;
`assets/commune_map.css`, which seven pages share, is not touched at all. A new `sources.html`
carries the full licence text the one-screen layout has no room for. This completes Batch 8; 8a was
the data explorer.

## What the roadmap asked

> restyle map.html without replacing its map engine.

## In place, not as a preview

Unlike 8a, this one edits the live page. The distinction is deliberate: 8a needed a new data
architecture and a new URL, so a preview beside the live page was the safe shape. 8b is
presentational and additive. The route, the data, the indicator picker, the `?indicator=` URL
parameter, the language and theme switches and every value on screen are unchanged, so a preview
would have meant two maps drawing the same numbers and a cutover to schedule for no benefit. The
licence block is the one thing that moved, and it moved to a page of its own rather than away.

`map.html` is indexed and in the sitemap. It still is: no `noindex` was added.

## The trap this batch existed to walk into

`assets/commune_map.css` — the shared choropleth stylesheet, used by seven pages — reads eleven
custom properties it does not define: `--surface`, `--border`, `--border-strong`, `--text`,
`--text-muted`, `--text-faint`, `--accent`, `--accent-soft`, `--accent-ink`, `--warn`,
`--warn-soft`. They come from the old palette each hand-built page declares for itself.

So a page that switches to `--bp-*` tokens and stops declaring the old names does not merely look
different: the tooltip loses its background and border, the zoom buttons lose theirs, and the
legend note loses its colour. The declarations become invalid at computed-value time and fall back
to their initial values.

`map.html` now declares **an alias block**: every one of the eleven names, once, pointing at its
`--bp-*` equivalent. A custom property is a live reference, so one flat block is enough — it
re-resolves when the theme changes, with no duplicated dark-mode copy. `assets/commune_map.css` is
not touched, which matters because editing it is editing `communes.html`, `home.html`,
`home2.html`, `commune.html`, `micro.html`, `profiles.html` and three `preview/` builds at once.

**The same trap is already live on three pages and is not fixed here.** `micro.html`,
`profiles.html` and `home2.html` all link `commune_map.css` and define none of those names. On
`micro.html` and `profiles.html` the tooltip additionally carries `class="tip"` where the
stylesheet styles `.map-tip`, and the markup sets the `hidden` **attribute** while the engine
shows the element by removing a `map-hidden` **class** — so the tooltip is filled with the right
text on hover and never becomes visible at all. Verified in Chromium. That is a separate PR
against those pages (rule 10); it is recorded in 8a's report and its PR body as well.

## What else changed

**The page gained the site navigation.** `src/pages/shell.py:109` lists `/map.html` in the nav
every block-built page renders, but `map.html` itself rendered no nav — a reader arriving on it had
no way back except the three links buried in its lead paragraph. It now carries the same top bar as
the other redesigned pages, with `aria-current="page"` on its own entry. The link list is repeated
in the markup rather than generated, because this page is hand-built and stays hand-built.

Two nav strings did not exist in `assets/i18n.js` and were added in all three languages:
`navAllData` and `navAbout`, worded as `shell.py` words them.

**Two responsive defects, both found by rendering.**

1. **The top bar took the whole page sideways at 390 px.** `layout.css` makes `.bp-topbar` itself
   the flex row, so the centring `.wrap` inside it is a flex *item* — and a flex item's base size
   is its max-content width, measured at 602 px inside a 390 px screen. `flex:1` plus
   `min-width:0` lets it shrink; the nav and the switches then wrap inside it.
2. **The legend is 448 px wide and cannot be narrowed in CSS.** `MapUI.SWATCH_PX` is 64, ticks are
   positioned at multiples of it, and `ticks.style.width` is written inline by the engine — so
   restyling `.swatches div` would slide every colour band away from its own tick. The legend
   scrolls inside itself instead, which is the answer `communes.html` already gives. Page-local
   rule, for the shared-stylesheet reason above.

## The second pass: the page became an application, not a document

After the restyle was working, the maintainer asked for the layout a map explorer actually wants,
against a reference screenshot of another product. Five things, and the last of them forced a
licence decision.

**The map fills the screen and the controls sit beside it, on the right.** The top bar, the map row
and the footer are one flex column of exactly one viewport, so nothing hardcodes the bar's height.
`dvh`, not `vh`: on a phone the browser chrome counts as viewport until you scroll, and `vh` would
put the bottom of the map underneath it. Below 900 px the split stops being a split — the map takes
a fixed slice and the controls follow in normal flow, because a header, a map, five controls and a
footer cannot usefully share 844 px.

**A first attempt collapsed the map to 150 px on a laptop** and the cause is worth recording: the
whole page was one full-height flex column, and the licence text at the bottom made the column
taller than the viewport, so the map row had no free space to grow into. Only the header, the map
and the footer are inside the one-viewport box now.

**An area of analysis picker — Belgium, three regions, ten provinces.** No new map code: the
component already had `setVisible(Set)` and `frame(list)`. What matters is the legend, which
`paint()` re-bands from the visible subset only, so picking Limburg re-ranks the 38 communes within
Limburg instead of keeping the national bands — and the component's own note already says so in
words. Membership is the ancestor walk through `metadata/geographies.json` (commune → arrondissement
→ province → region), never a NIS prefix (rules 3 and 25). The chosen area goes into the URL beside
the indicator, with `replaceState`.

**Five choropleth palettes, defaulting to blue-through-yellow-to-red.** The component paints with
`var(--ramp-0..6)`, which is a live reference, so redefining those seven names recolours the map
with no redraw and no refetch — the palettes are pure CSS, scoped to a `data-palette` attribute on
this page's root, and `commune_map.css`'s own ramp (shared by seven pages) is untouched. The
default diverging scale is **deliberately identical in both themes**, unlike the sequential ramps,
which `commune_map.css` inverts in dark mode because pale reads as "more" on a dark ground: flipping
a diverging scale would swap blue and red and with them the meaning of every colour.

One caveat is recorded in the CSS and worth repeating: a blue-to-red scale reads as a verdict, and
`preferred_direction` is `contextual` for most municipal indicators. Red is not bad news on a
population map. The note beside the controls says which of the three cases a given indicator is.

**A compact legend, read down instead of across.** The component's colour bar is 7 × 64 = 448 px by
construction, with ticks placed inline in pixels, so it can never be narrower than that. The card
now lists one row per class — swatch beside the range it covers — in a 300 px card. Those rows are
built **by the component**, in `_drawLegend`, from the very breaks that painted the map: an
additive `legendRows` element that every other caller simply does not pass. The alternative was for
the page to re-run `MapUI.classify` on its own copy of the numbers, which is a second implementation
of the one thing a shared component exists to prevent. The card's title is the indicator's own name,
not its unit — most are declared `count`, which printed literally as "count".

**A distance scale bar.** It needs no projection maths: x is longitude times cos(mean latitude) and
a degree of longitude is 111.32 × cos(latitude) km, so the cosines cancel and one viewBox unit of
width is always ~111 km. It reads `state.map.view` and watches the SVG's `viewBox` attribute with a
`MutationObserver`, because the component emits no "view changed" event and adding one would be a
change to a file seven pages share.

## The licence decision, stated plainly

The page is now one screen with no scroll. The several hundred words of licence text that sat under
the map do not fit, so **`sources.html` is new** and carries the complete notice for every source —
Statbel boundaries, the canonical values block, ONEM, the federal police, Steunpunt Werk —
unabbreviated, with the date-of-last-update slot filled from the published indicator index. It is
registered, indexable and in the sitemap on purpose: a licence page search engines cannot reach is
harder to find than the figures it covers.

`map.html` keeps a visible credit in its own markup — the source named and linked, the licence named
and linked, and a link to the full text — **written out rather than assembled by script**, so a
reader without JavaScript still gets one. The script refines the source name to whichever source the
indicator on screen actually came from.

**This changed a rule the test suite enforced.** Before it, every page showing a Statbel figure
carried the complete notice in its own markup, and `tests/test_statbel_attribution.py` asserted
exactly that. Rather than deleting the assertions, the file now names two classes of page:
`MUNICIPAL_PAGES`, which carry the whole notice and now includes `sources.html`, and
`DELEGATING_PAGES`, which show figures but delegate the text — with three new tests requiring the
credit in markup, the licence named and linked, and the full notice linked by name. The change of
policy is written into that file so the next person meets it as a decision rather than as a gap.

## Two test docstrings corrected

`tests/pages/test_map_conversion.py::test_the_hand_built_map_is_untouched` and its twin in
`tests/pages/test_trilingual_export.py` run `git diff --stat -- map.html` and require it to be
empty. That compares the **working tree against HEAD**, so what they forbid is an *uncommitted*
edit made while working on the conversion, not change as such — they pass once a deliberate change
is committed. Their docstrings said "map.html stays live and byte-identical" and "nothing about
this conversion is allowed to change the page readers are using today", which was true of Batch 15
and is not true of this batch. Both now say what they actually guard, and note that the block-built
`/preview/map.html` is expected to differ in chrome until it is restyled too. The markup contract
the component depends on — the class names asserted elsewhere in that same file — is what has to
stay identical between the two, and does.

## What was deliberately left alone

`assets/commune_map.js` and `assets/commune_map.css` (rule 29, and both are shared). The
projection, zoom, tooltip content, legend banding, no-data handling and click-through are the
engine's and were not touched. The `?indicator=` parameter still uses `replaceState`, never
`pushState`. The values-attribution block is byte-identical to `communes.html`'s, as
`tests/test_statbel_attribution.py` requires. The `data-t="mapTitle">Commune Map<` literal that
`tests/test_i18n.py` pins is unchanged. `/preview/map.html` and its two translations are untouched.

## Verification

Chromium at 1400, 820 and 390 px in both themes: zero console errors, zero horizontal overflow,
565 paths all filled, seven legend swatches with the tick strip still at 7 × 64 = 448 px, seven nav
links, 69 indicators in the picker, and — the check that proves the alias block works — hovering a
commune shows a tooltip with a real surface background in both themes.
