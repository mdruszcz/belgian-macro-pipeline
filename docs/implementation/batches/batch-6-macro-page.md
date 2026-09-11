# Batch 6 — the macroeconomics page (`macro.html`)

```
Batch:                 6 — macroeconomics reproduction (docs/steps L566-570)
Base commit:           c1a9c6a8 (origin/feat/commune-loop)
Final commit:          see branch feat/macro-page
Files changed:         macro.html (new), config/national_sections.yaml (new),
                       tests/test_macro.py (new), scripts/export_site_payloads.py,
                       assets/i18n.js, assets/commune_map.js, src/site/routes.py,
                       src/pages/semantics.py, tests/test_map_ui_logic.py,
                       tests/test_home2.py (lint only)
Requirements completed: analytical shell, KPI row, GDP history, key-indicator panel,
                       contributions breakdown, the four cards that cannot be filled,
                       configuration-driven throughout, national data logic unchanged
Deferred requirements: international comparison (no data), public-finance counters
                       (no data — and never simulated), economic map (no commune GDP),
                       news (no article store)
Data-contract impact:  national.json entries gain `names{en,fr,nl}`, `direction` and
                       `decimals`; new metadata/national_sections.json. Additive.
Commands executed:     see "Verification"
Tests passed:          12 new; 2364 in the page/site/i18n suites; full suite below
Screenshots produced:  1400 / 820 / 390, light and dark (not committed — rule 12)
Performance results:   first paint draws from four JSON fetches; no page errors
Reviewer findings:     pending
Known limitations:     below
Rollback procedure:    delete macro.html and its two config/test files; the exporter
                       change is independent and additive
Next batch:            Batch C — the finance section on the commune profile
```

## What this batch does

Builds `macro.html` from the supplied design (`assets/belpulse/macro page.png`,
`docs/design-references/macro.md`) as a noindexed preview beside `dashboard.html`, on
Batch 1's tokens and Batch 2's components.

**The page contains no indicator id and no figure.** The layout is
`config/national_sections.yaml`, published as
`public/data/metadata/national_sections.json`; the values, names, units, sources, grades and
retrieval dates come from `public/data/national.json` and `metadata/sources.json`. Adding a
series to this page is a config change — the property docs/steps' 50% gate asks for, and what
`tests/test_macro.py` actually enforces.

## Where it departs from the design, and why

Each departure is a figure the design shows that this pipeline does not hold. None is a layout
choice.

- **Four of the six KPI cards.** Population, the employment rate, the fiscal balance and public
  debt have no national series here. The row shows the six series that exist, and a line under
  it names the four that do not.
- **"Comparaison internationale".** The five foreign GDP configs hold zero observations (a
  pre-existing DBnomics timeout, recorded under Block H); Poland is not configured at all. Built
  at its designed size in the `unavailable` state, with that exact reason on the card.
- **"Finances publiques (compteurs simulés)".** There is no national revenue, expenditure,
  balance or debt series to extrapolate from. The design's counters are labelled "simulés"; a
  simulation from *nothing* is an invention (rule 36), so the card says so instead. The page
  contains no timer of any kind, and a test asserts it.
- **"Carte économique".** Belgium publishes no GDP below the regional level. The card says so
  and links to the interactive map of the municipal indicators that do exist.
- **"Actualités".** No article store exists; nothing here is written by hand.
- **The sidebar** lists this page's own sections rather than the design's eleven themes — an
  anchor to a section that does not exist is a broken promise, and a test checks every anchor
  resolves. The design's "Mon espace" button is omitted: no accounts before the 50% milestone.

## Two changes outside the page

1. **`national.json` was the site's one monolingual payload.** Its entries carried a single
   English `name`, which is why home2's national cards read English in French and Dutch — a gap
   `known-risks.md` has recorded since 2026-09-07. National entries now carry
   `names{en,fr,nl}` from the `indicators` table, plus `direction` and `decimals`, so a KPI's
   change can be coloured by the indicator's OWN preferred direction (a falling unemployment
   rate is favourable; falling growth is not) and rounded as published — metadata, never typed
   into the page (rule 28).
2. **`MapUI.formatValue` now honours a declared `decimals` as a minimum as well as a maximum.**
   `src/pages/resolve.py` has always padded; the JavaScript mirror did not, so a GDP growth of
   exactly 1.0 % printed as a bare "1" beside "0,5 %" and "2,2 %". The two mirrors now agree.
   With no declared decimals the old guess stands.

## Verification

- `ruff check .` clean; `black --check` clean.
- `tests/test_macro.py` — 12 tests: no indicator id anywhere; none of the design's eleven
  invented figures; no simulated counter; no third-party asset; every series the layout names
  exists in the payload; a slot for every unavailable card; every label trilingual; every anchor
  resolves; every string key in all three languages; English left in the markup; registered,
  noindexed, absent from every sitemap; every link and script resolves.
- Playwright, Chromium, at 1400 / 820 / 390 in both themes: **zero page errors, zero console
  errors, no horizontal overflow at any width**. Read back from the live DOM, not from the
  source: six KPI cards with real values and periods, the GDP bar chart over twenty years with
  the contraction years drawn in the accent colour, six key indicators, seven contributions
  summing to +1,0 pp against the published annual growth of +1,0 pp, four unavailable cards each
  carrying its reason, the sources line built from the sources actually shown, and the build date
  from `manifest.json`.
- Exported the payloads twice and diffed: `national.json` and `national_sections.json` identical
  (rule 35).

Two fixes came out of looking at the render rather than the code: the top bar pushed the page
sideways between 769 and 1000 px (the language select sat at x=959 in an 820-wide viewport), and
on a phone the navy sidebar filled the entire first screen before a single figure — it is now a
horizontal strip of links below the one-column breakpoint.

## Known limitations

1. **`tests/test_home2.py` needed a one-line import-order fix** to make `ruff check .` pass at
   all on this branch; it arrived with the home2 commit. Recorded rather than silently absorbed.
2. **The page is a preview.** `/macro.html` is registered, noindexed and out of every sitemap;
   `dashboard.html` is untouched and still what a visitor gets.
3. **No commune map on this page**, so it publishes no municipal figure and is deliberately not
   added to `MUNICIPAL_PAGES` in the attribution tests. The moment a municipal figure appears
   here, that changes.
4. **The KPI row's sixth card is a confidence balance**, not one of the design's six figures —
   it is what the pipeline has. The note under the row is the honest version of the difference.
