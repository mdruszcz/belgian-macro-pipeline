# Batch 8a — the data explorer

`explorer.html`, a noindexed preview beside the live `all_data.html`, plus the sharded payloads
that make the full dataset browsable. The `map.html` restyle is the other half of Batch 8 and is
not in this batch.

## What the roadmap asked

> Redesign all_data.html with search/filters/table/chart/map modes and virtualization.
> Why: The explorer must stay usable at full data scale and never disagree with a download.

## What was measured first

`all_data.html` today fetches one file, `data/belgian_macro_export.csv` — 149 KB, 1,402 rows,
19 **national** indicators — and writes all of them into the DOM with a single `innerHTML`
assignment on every keystroke. It carries **no municipal data at all**. So "usable at full data
scale" is not a performance tweak on an existing page: the municipal half of this pipeline,
178,128 rows across 68 indicators, has never been browsable anywhere on the site.

That also settles why a map mode belongs here. A national series has one geography; a choropleth
of it would be an invented map. The map mode exists because the explorer now reaches commune data.

## What shipped

**A preview page, not a replacement.** `explorer.html` sits beside `all_data.html`, noindexed and
out of every sitemap, linked from nowhere — the pattern `home2` beside `index`, `macro` beside
`dashboard`, `micro` beside `all_data` and `profiles` beside `communes` all follow, and which
`src/site/routes.py` records in each of those pages' own reason strings. The live page keeps its
CSV fetch, its markup and its behaviour. The cutover is a separate, later step.

**Sharded payloads, built from the downloads.** `scripts/export_explorer_payloads.py` writes
`public/data/explorer/index.json` plus one file per indicator: 68 municipal, 19 national, 5.4 MB
in total, median 46 KB, largest 394 KB. The municipal shards are built from
`data/communes_history.csv` and the national ones from `data/belgian_macro_export.csv` — the two
files the site actually offers for download. Not from the database, and deliberately not from
`communes_history_full.csv`, which is gitignored and which no reader can obtain. The correspondence
is asserted in both directions by `tests/test_export_explorer_payloads.py`: no cell on the page
that the download lacks, no row in the download that the payload dropped.

That is the only construction that makes "never disagree with a download" checkable rather than
aspirational. It also makes the scale problem disappear: the browser fetches one indicator.

**A windowed table that does not lie about itself.** Only the visible rows plus an overscan margin
are ever in the DOM. `aria-rowcount` on the table and `aria-rowindex` on each rendered row report
the true position in the full set, the status line says "36 of 14,748 rows match" rather than the
count it happened to render, and the scroll region is keyboard-operable. A windowed table that
reported only what it rendered would tell a screen reader there are forty rows when there are
fourteen thousand.

**Three modes**: table, chart (`assets/belpulse/charts.js`) and map (`MapUI.CommuneMap`, rule 29 —
the shared engine, not a second implementation). The map mode is offered only for municipal
indicators; for a national one it says why instead of drawing an empty country.

**URL state.** `?scope=&indicator=&geo=&from=&to=&mode=`, written with `history.replaceState` and
never `pushState`, following `map.html`. A researcher's use case is sending someone a link to a
slice; every keystroke pushing a history entry would make the back button useless.

## Changes outside the page

**A published download changed.** `scripts/export_canonical_csv.py` mapped only `final` → `A` and
`provisional` → `P`, sending every other status to an empty cell. Nine rows in
`belgian_macro_export.csv` — all 2009 annual figures the database records as `revised` — therefore
reached readers with no status at all, indistinguishable from a row about which nothing is known.
The exporter's own docstring said the shortcut existed only because "the frontend has no dedicated
visual for estimate/revised/suppressed/na yet". This batch builds that visual, so the precondition
is gone and the mapping now covers all six states with the same letters `communes.html` already
uses for municipal data. The two vocabularies could otherwise have drifted apart.

**A dead control on the live page was fixed.** `all_data.html`'s theme switcher had never worked:
it selected `document.querySelector('.theme-toggle')`, which matches the **language** group first
because that group carries the same class and comes earlier in the document, so the button list was
empty and no click handler was ever attached. The saved theme still applied on load — the pre-paint
script in `<head>` does that — which is why the failure was invisible and why the six assertions in
`tests/test_theme_toggle.py` pass either way: they check markup shape, not behaviour. Fixed the way
`map.html` has always done it, by selecting on the attribute.

## Two defects found and deliberately NOT fixed here

**1. The two national downloads disagree with each other.** `README.md` offers
`data/belgian_macro_export.csv` and `data/belgian_macro_export.json` as the same data. They are
not: 1,402 rows and 19 indicator codes against 885 rows and 17, different agency labels
(`Eurostat` against `Eurostat/DBnomics`), and different status letters. The CSV is overwritten by
`scripts/export_canonical_csv.py` while the JSON is still written by the legacy path at
`belgian_macro_db.py:404`; nothing in `tests/` compares them. This is the same class of problem
this batch exists to prevent, but fixing it means touching the legacy exporter and is a decision
for the maintainer, not a side effect of a page build.

**2. The map tooltip never appears on `micro.html` or `profiles.html`.** Both pass a tooltip
element to the shared map engine as `<div class="tip" id="..." hidden>`. The stylesheet styles
`.map-tip`, not `.tip`, and the engine shows and hides the element by toggling the `map-hidden`
**class** while the markup sets the `hidden` **attribute**, which nothing ever removes. Verified in
Chromium: hovering a commune on `micro.html` fills the element with the right text
("Avelgem · 39 788 · 2023 · derived") and it stays invisible, with no background, border or
padding. The same markup is in `profiles.html`. Both pages shipped in the last three merges. It is
a small fix and it belongs in its own PR against those pages, not in this one (rule 10).

Related and worth recording: `assets/commune_map.css` reads eleven custom properties
(`--surface`, `--border-strong`, `--text`, `--accent-ink`, `--text-faint`, `--warn`, `--warn-soft`
and four more) that it does not define, because it was written for the old palette. No redesigned
page defines them. `explorer.html` aliases all eleven onto their `--bp-*` equivalents in one block;
`micro.html`, `profiles.html` and `home2.html` do not, so every map chrome rule on those pages
resolves to its initial value.

## Defects found by rendering the page, not by reading it

Both were invisible to every static test and were fixed in this batch.

1. **The indicator picker took the whole page sideways.** A `<select>` sizes itself to its longest
   option, and the indicator names run to sixty characters: measured at 573 px inside a 390 px
   viewport. `min-width:0` plus `max-width:100%` on the control.
2. **The top bar did not wrap at tablet width.** Six nav links plus the wordmark and the language
   picker need about 880 px of min-content width; at 820 px they ran past the right edge rather
   than wrapping, giving the whole page 60 px of horizontal scroll. The nav takes its own line and
   scrolls inside itself below 1000 px, which is what `profiles.html` already does at its own
   breakpoint.

## Verification

- The exporter run twice produces byte-identical output (rule 35), checked by hashing all 87 files.
- Chromium at 1400, 820 and 390 px in both themes, in all three modes: zero console errors, zero
  horizontal overflow, 565 map paths, the chart drawing, and the windowed table scrolling to the
  last row (rendered `aria-rowindex` reaches 5,086 of a declared 5,086).
- Three figures hand-checked from the page against the download with `grep`:

| indicator | geography | period | in the CSV | on the page |
|---|---|---|---|---|
| `AVG_NET_TAXABLE_INCOME` | Namur (92094) | 2023 | `37748.548…` / derived | €37,749 · calculated |
| `MEDIAN_HOUSE_PRICE` | Aartselaar (11001) | 2026-Q1 | `445000.0` / P | €445,000 · provisional |
| `GDP_ANNUAL_CY` | Belgium | 2009 | `-1.9` / R | −1.9 pp contribution · revised |

The third is the status fix working end to end: that cell was empty in the CSV before this batch.

## Known limitations

- The page is a preview. `all_data.html` remains the canonical, indexed explorer.
- The chart mode draws one geography at a time. Comparing several on one chart needs the peer model
  and a selection UI, neither of which exists yet.
- `AVERAGE_HOUSEHOLD_SIZE` and the other Census 2021 indicators carry one period, so their chart is
  a single point. The page says so rather than drawing a line through one value.
- No commune-level data exists for the national indicators and no national aggregate exists for a
  median, so the two scopes are not interchangeable and the page does not pretend they are.
