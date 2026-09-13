# Batch 7 — the microeconomics page (`micro.html`)

```
Batch:                 7 — microeconomics page (docs/steps L570-573)
Base commit:           c9cd9a96 (origin/develop)
Final commit:          see branch feat/batch-7-micro-page
Files changed:         micro.html (new), config/micro_sections.yaml (new),
                       tests/test_micro.py (new), scripts/export_site_payloads.py,
                       tests/test_export_site_payloads.py, assets/i18n.js,
                       src/site/routes.py, src/pages/semantics.py,
                       tests/test_statbel_attribution.py,
                       docs/features/site_payloads.md, docs/features/page_builder.md,
                       public/data/aggregates.json (new),
                       public/data/metadata/micro_sections.json (new),
                       public/data/communes/*.json, public/data/manifest.json
                       (regenerated, see "One change outside the page" below)
Requirements completed: analytical shell (light variant), KPI row, key-indicator list,
                       household tile grid, territorial comparison (provinces + Belgium),
                       choropleth map with a restricted indicator picker, housing-market
                       history chart, four honestly-unavailable cards, explore-communes
                       card, configuration-driven throughout
Deferred requirements: business fabric / enterprise dynamics (one snapshot, no trend),
                       sectoral structure (no NACE series), local entrepreneurship (no
                       trend), microeconomic news (no article store)
Data-contract impact:  new public/data/aggregates.json (province/region/country
                       cross-section) and metadata/micro_sections.json; `_read_aggregates`
                       now also carries `nis_code`, which ripples (additively) into every
                       commune payload's existing `comparison.{level}` objects. Additive.
Commands executed:     see "Verification"
Tests passed:          13 new in test_micro.py, 6 new in test_export_site_payloads.py;
                       full suite below
Screenshots produced:  1400 / 820 / 390, light and dark (not committed — rule 12)
Performance results:   first paint draws from six JSON fetches plus the boundary geojson;
                       no page errors
Reviewer findings:     pending
Known limitations:     below
Rollback procedure:    delete micro.html and its two config/test files; the exporter
                       change is independent and additive; the commune-payload nis_code
                       addition can be reverted by dropping one field from
                       `_read_aggregates()` and re-exporting
Next batch:            per docs/steps
```

## What this batch does

Builds `micro.html` to the same analytical shell as Batch 6's `macro.html`, in the LIGHT
variant (`docs/features/design_system.md`: macro is navy, micro is light), from
`docs/design-references/micro.md` (a written transcription — there is no image for this page).

**The page contains no indicator id and no figure.** The layout is
`config/micro_sections.yaml`, published as `public/data/metadata/micro_sections.json`; values,
names, units, sources, grades and retrieval dates come from `public/data/national.json` and
`public/data/aggregates.json` (new this batch) via `metadata/sources.json` and
`metadata/indicators.json`. Adding a series to this page is a config change.

**Two payloads feed it, not one.** Unlike macro.html, this page's series come from two
universes: `national.json` (a handful of micro-adjacent national series — the ONEM-insured
unemployment rate is the KPI-row example) and `aggregates.json`'s `be:country` row (municipal
indicators aggregated from the ground up, per `docs/features/comparison.md` — summed and
recomputed, never averaged, never population-weighted). `buildEntry()` in the page's own script
is the one place that resolves a code against both; the exporter's `_check_micro_sections`
refuses a layout naming a code in neither.

## Where it departs from the design, and why

Each departure is a figure the design shows that this pipeline does not hold. None is a layout
choice.

- **All six KPI cards.** None of the design's six figures (active enterprises, a
  business-creation rate, a household median income, an average gross wage, a national
  unemployment rate, a national median house price) has a series here. The row shows six real,
  dated figures instead — local business units, mean net taxable income per tax return, the
  ONEM-insured unemployment rate, average household size, home-sales transactions and the
  working-age claimant rate — and `kpisNote` states plainly which of the design's figures do not
  exist in this pipeline.
- **"Tissu entrepreneurial" (business fabric).** `LOCAL_UNITS_BY_COMMUNE` is a single 2023-Q4
  snapshot with no prior period, so no trend, no creation count and no bankruptcy count exists.
  Built at its designed (card-wide) size, unavailable, with that reason.
- **"Structure sectorielle".** No NACE sector breakdown exists at any geography.
- **"Entrepreneuriat local".** The same one-period snapshot cannot support the design's three
  trend rows.
- **"Actualités microéconomiques".** No article store exists; nothing here is written by hand.
- **"Marché du logement" is transaction volume, not a price.** The design's dropdown implies a
  house-price series ("Prix d'achat"); the one national housing series this pipeline holds is
  `HOUSE_SALES_TRANSACTIONS`, and the chart's label and note say so explicitly.
- **The sidebar** lists this page's own seven sections, not the design's eleven themes.
- **The dropdown-driven chart/map** resolves the open question `micro.md` itself raised: the
  comparison and map `<select>`s switch ONLY among the codes `config/micro_sections.yaml`
  configures, at view time, never an arbitrary indicator (rule 23).

## One change outside the page

`_read_aggregates()` (used by both the pre-existing commune-comparison feature and this batch's
new `aggregates.json`) now carries `nis_code` per row, so the new payload can publish it
alongside `level` and `name`. That field flows, additively, into every commune payload's
existing `indicators.*.comparison.{province,region,country}` objects too — confirmed by diffing
a full re-export against the current `develop` snapshot: every one of the 565 changed files
gained only `nis_code` keys already-present sibling objects, no value changed. `manifest.json`'s
diff is its own build timestamp and git commit, as on every run.

Regenerating also required rebuilding the gitignored `data/communes_history_full.csv` from the
committed database — the copy on this machine predated the already-merged `UNEMPLOYMENT_RATE_INSURED`
data (PRs #138/#139), which is why the first export run refused `config/local_sections.yaml`
(a pre-existing, unrelated config) until that CSV was regenerated. `data/communes_history.csv`,
`data/aggregates.csv` and the database itself were untouched.

## Verification

- `ruff check .` clean; `black --check` clean (on changed Python files).
- `tests/test_micro.py` — 13 tests: no indicator id anywhere (checked against national.json,
  aggregates.json and metadata/indicators.json together); none of the design's invented figures;
  no simulated counter; no third-party *resource* (script/stylesheet/image) outside
  fonts.googleapis.com — narrower than macro's identical-named check by design, since this page
  correctly carries real `<a href>` attribution links macro.html has none of; every series the
  layout names exists in the union universe; a slot for every unavailable card and no orphans
  either way; every label trilingual; every sidebar anchor resolves; every translation key exists
  in all three languages; English left in the markup; registered, noindexed, absent from every
  sitemap; every link and script resolves.
- `tests/test_export_site_payloads.py` — 6 new tests: `aggregates.json` excludes arrondissement;
  one hand-checked value plus coverage at each of country/region/province matches the source CSV
  row exactly; byte-identical across two runs; `_check_micro_sections` refuses an unknown code
  and accepts a code from either universe; `_aggregates_payload`'s reshape is a pure function of
  its input.
- Fast tier: `pytest tests/ -q -m "not browser and not generated_site and not slow"` —
  **1613 passed, 5 failed, 19 skipped**. Full tier: `pytest tests/ -q` — **3660 passed, 6 failed,
  20 skipped in 452s**. All 6 failures are pre-existing on this Windows machine and untouched by
  this branch (`git diff origin/develop -- tests/builder/ src/builder/ tests/security/` is empty):
  `test_builder_api.py::test_state_payload_too_large`,
  `test_builder_paths.py::test_symlink_planted_inside_a_page_directory_cannot_redirect_a_write_outside`
  and `test_builder_transaction.py::test_atomic_write_text_crash_before_replace_leaves_destination_untouched`
  and `test_builder_service_hardening.py::test_symlink_escape_via_http_save_is_refused` are the
  four named in this batch's handoff; `test_builder_shell_e2e.py::test_413_payload_too_large_is_stated_plainly`
  and `::test_server_unreachable_is_stated_plainly_not_as_a_blank_failure` are two more instances
  of the identical documented failure classes (a `ConnectionAbortedError`/`WinError 10053` socket
  abort on an oversized-payload test, and a `WinError 1314` symlink-privilege error) — they only
  appear in the full tier because they carry the `browser` marker the fast run excludes.
- Playwright, Chromium, at 1400 / 820 / 390 in both themes: **zero console/page errors, no
  horizontal overflow at any width, 565 coloured map paths, the comparison chart resolving 11
  rows (10 provinces + Belgium) with Belgium included, every one of the 4 unavailable cards
  showing its reason.**
- Exported the payloads twice and diffed: `aggregates.json`, `micro_sections.json` and every
  sampled commune/indicator payload byte-identical (rule 35); only `manifest.json`'s timestamp
  and git-commit fields differed, as expected.
- Hand-checked against `data/aggregates.csv` directly, then cross-checked against what the live
  page actually renders (or, for the one figure the page never prints as DOM text, against the
  exact payload object the page's own script fetches):
  - Belgium, `AVG_NET_TAXABLE_INCOME`, 2023: CSV value `40125.69795611309` (mean per tax return;
    coverage 581/581) — the KPI card renders **"€40 125,7"** (1 declared decimal).
  - Belgium, `HOUSEHOLDS_PRIVATE`, 2021: CSV value `5024851.0` (coverage 581/581) — the household
    tile renders **"5 024 851"**, exact.
  - Antwerp province, `UNEMPLOYMENT_RATE_COM`, 2021: CSV row `6.8440169766766825`, coverage
    69/69/100.0 — `aggregates.json`'s `indicators.UNEMPLOYMENT_RATE_COM["be:prov:10000"].periods["2021"]`,
    read live from the running page, is `{value: 6.8440169766766825, coverage: {n: 69, of: 69,
    pct: 100}}`, exact. (Antwerp is not printed as page text on its own — the comparison chart
    draws its 11 bars on a canvas — so this one is verified against the payload the page itself
    fetched rather than OCR'd off a canvas; the chart's row count and Belgium-highlight were
    separately confirmed live, above.)

### Two defects found only by looking at the render, not the code

Both are page-composition CSS fixes (in `micro.html`'s own `<style>`, never in a shared file),
found because the design has seven sidebar labels noticeably longer than macro.html's and a real
choropleth legend that macro.html never has to place in a narrow column:

1. **A CSS grid-blowout pushed the whole page sideways at 820 px.** `layout.css`'s
   `.bp-body--analytical{grid-template-columns:1fr}` (the ≤1024 px rule) still sizes the track to
   its child's min-content, and a horizontal `.bp-sidebar-nav` of seven no-wrap links
   ("Territorial comparisons", "Microeconomic map"…) has a min-content width over 1100 px.
   `minmax(0,1fr)` on the track (plus `min-width:0` on the flex chain), declared in this page's
   own stylesheet, fixes it without touching `layout.css` or any other page.
2. **The choropleth's legend and its indicator `<select>` overflowed a narrow grid column.** The
   map card only had one of two (later one of three) row columns until the ≤1024 px breakpoint;
   given `card-wide` like the comparison card already had, plus stacking the select under the
   title and shrinking the legend's swatch width at ≤768 px (again page-local CSS), it fits at
   390 px with the tick labels clipped rather than overflowing.

## Known limitations

1. **The page is a preview.** `/micro.html` is registered, noindexed and out of every sitemap;
   nothing links to it from a live page (rule out of scope, honoured).
2. **The map's indicator list is short by design** — four codes with full municipal coverage,
   not the whole 69-indicator index `map.html` offers — because rule 23 forbids an arbitrary
   indicator switch inside a page definition.
3. **The comparison chart drops a province rather than showing it short** if that province lacks
   a value at the period every other row shares; `data/aggregates.csv` already suppresses any
   aggregate under 90% coverage upstream, so in practice this has not been observed for the five
   configured indicators.
