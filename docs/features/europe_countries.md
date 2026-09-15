# Feature: Europe panel -- country view and country comparison charts

Status: in-progress
Issue: (maintainer direction, 2026-09-15 -- no issue number yet)
Branch: feat/europe-countries

## Problem

The Europe panel's NUTS 2 choropleth (batch B3, `assets/belpulse/europe_map.js`) only ever shows
regions. Belgium's neighbours and the rest of Europe cannot be compared country to country, and the
"Comparaison internationale" card has stood as `config/national_sections.yaml`'s `unavailable` slot
since the international pilot (`docs/features/international.md`) explicitly excluded any page. The
maintainer's decision on 2026-09-15 lifts that exclusion for the five pilot indicators, and asks for
two things: a country view on the map, and picking countries to draw comparison charts.

## Goal

- A "Régions / Pays" toggle on the Europe panel's map. Country mode paints a NUTS 0 choropleth for
  one indicator/period at a time, exactly like region mode does for NUTS 2.
- Clicking a country (map or an accessible picker list) selects it; up to 8 selected countries draw
  the comparison charts below the map, replacing the `international` card's `unavailable` state.
- Two new indicators, fetched from Eurostat's own country-level cells of the two datasets the NUTS 2
  batch already reads (`nama_10r_2gdp`, `demo_r_pjanaggr3`): `GDP_PC_PPS_COUNTRY`,
  `POPULATION_COUNTRY`. Eurostat's own country figures, never summed or averaged from the NUTS 2
  rows (CLAUDE.md aggregation rules).
- The international pilot's five indicators become visible for the first time (`UNEMPLOYMENT_RATE_EUROPE`,
  `HICP_ANNUAL_RATE_EUROPE`, `GOV_DEBT_EUROPE`, `CONSUMER_CONFIDENCE_EUROPE`, `GDP_VOLUME_EUROPE`).

## Non-goals

- No change to the NUTS 2 payloads, formulas, or region mode's behaviour -- it must work exactly as
  before.
- No averaging or summing of anything in the browser. No new data source. No framework. No
  browser-side Eurostat fetch.
- No annual NUTS 0 unemployment series -- country mode reuses the pilot's monthly
  `UNEMPLOYMENT_RATE_EUROPE`; region and country unemployment are different Eurostat
  surveys/frequencies and the UI says so.
- AMECO / PR 2 is untouched.

## Proposed approach

### Data

Two new indicators, `config/indicators/GDP_PC_PPS_COUNTRY.yaml` / `POPULATION_COUNTRY.yaml`,
`geo_levels: [national]`, `fetch.geographies: allowlist` (the same multi-geo shape
`is_multi_geo()` already recognises), added to `config/stores.yaml`'s existing `international`
store and fetched by the existing `scripts/sync_international.py` (already wired into `make fetch`
and the daily run -- no new daily-path wiring needed).

Both datasets mix every NUTS level (0-3) into one `geo` dimension -- the same problem
`scripts/sync_nuts2.py` already solves for the NUTS 2 batch with `geo_filter=is_nuts2_code`. A new
predicate, `src/geography/international.py`'s `is_country_level_code()`, filters to NUTS 0-shaped
codes (a real country is always exactly 2 letters) or an underscore-joined EU/EFTA aggregate
spelling (`EU27_2020`), applied ONLY to these two datasets
(`sync_international.py`'s `MIXED_NUTS_LEVEL_DATASETS`) -- never universally, because the five
original pilot indicators' own aggregate code, `EA21`, is shaped exactly like a NUTS 1/2/3
subdivision code and would be wrongly filtered out if this ran for every dataset.

Two live findings, both handled the same way `international_excluded.csv` already handles
superseded aggregates: `demo_r_pjanaggr3` also carries `EU27_2007` (superseded EU27 composition)
and `DE_TOT` (Germany's whole-country total under an alternate code, same figure as `DE`) -- both
added to `international_excluded.csv` with reasons. Neither dataset carries a euro-area code at
all (confirmed against a live fetch, 2026-09-15) -- **contradicts this handoff's original
assumption** that a euro-area reference line would exist for GDP per capita and population; it does
not, and the comparison chart's EU27_2020 reference line has no euro-area counterpart for those two
indicators.

Real data fetched and committed (2026-09-15, `data/international/GDP_PC_PPS_COUNTRY.csv`,
`POPULATION_COUNTRY.csv`, verified against this handoff's live-checked figures):

| geo | indicator | period | value | status |
|---|---|---|---|---|
| BE | GDP_PC_PPS_COUNTRY | 2023 | 45400 | final |
| DE | GDP_PC_PPS_COUNTRY | 2023 | 45100 | provisional |
| EU27_2020 | GDP_PC_PPS_COUNTRY | 2023 | 38400 | final |
| BE | POPULATION_COUNTRY | 2025 | 11,883,495 | final |

The reload of the five pre-existing `international` store indicators was byte-identical (0 new
vintages for those five out of the 28,004 rows fetched; only the 2,133 rows belonging to the two new
indicators were new) -- the pre-existing store CSVs are untouched (verified with `git status`).

### Geometry

A NUTS 0 (whole-country) TopoJSON file, same publisher and vintage as the NUTS 2 batch's own
geometry, self-hosted at `public/data/geo/nuts0/2024/0.json` (294,569 bytes, sha256
`43ac926df21bcad84ef42d9d9d56dc6ab7b4c99372700c61790708480f4b611d`). `raw.githubusercontent.com`
did not respond on this network; the `ec.europa.eu` GISCO cache mirror of the same file did (see
`public/data/geo/nuts0/ATTRIBUTION.md`). 39 country outlines; the licence allowlist has 40 pilot
countries, so Georgia (`GE`) and Moldova (`MD`) are `no_outline` (documented, not silently dropped);
Kosovo (`XK`) has an outline but stays licence-excluded, same as every other payload.

### Payloads

`public/data/europe/countries/index.json` + one `{ID}.json` per indicator
(`scripts/export_europe_countries.py`, modelled on `scripts/export_europe_nuts2.py`): names,
unit, frequency, source block, periods, latest period, `values[period][code] = {v, s}` keyed by the
Eurostat 2-letter country code (matching the geometry's own `properties.id`), `class_breaks` per
period (map indicators only -- reusing `export_europe_nuts2.py`'s own `_quantile_breaks`), a
`reference_lines` block carrying whichever aggregate codes (`EU27_2020`, `EA21`) the indicator's
own dataset actually has, `excluded_by_licence`, `no_outline`.

Six indicators are painted on the map AND drawn in the comparison charts: `GDP_PC_PPS_COUNTRY`,
`POPULATION_COUNTRY`, `UNEMPLOYMENT_RATE_EUROPE`, `HICP_ANNUAL_RATE_EUROPE`, `GOV_DEBT_EUROPE`,
`CONSUMER_CONFIDENCE_EUROPE` -- each is a rate, level-per-capita or index directly comparable
country to country, so nothing about the handoff excludes them from the map the way it explicitly
excludes GDP volume. **This reading (all six map-eligible, not just the two new indicators) is an
assumption made during implementation** -- flagged here per CLAUDE.md's "when you are unsure" rule,
not guessed silently.

`GDP_VOLUME_EUROPE` is chart-only: a level in million EUR, not comparable across countries on one
choropleth scale. Published as an index (average of the real 2015 quarters = 100), computed in
`export_europe_countries.py`'s `_rebase_to_2015_index` (Python, never the browser -- CLAUDE.md
rules 4/6), unit-tested with hand-computed values (rule 5). A country missing any of its four 2015
quarters gets `"s": "na"` for its entire series, never rebased on a different year. Carries an
`adapted` notice reusing `src/exporters/provenance.py`'s existing grade-B text (international.md
point 4), rather than inventing new copy.

Five states never collapse (rule 26): a real value keeps its own status; a source-suppressed/na cell
keeps `v: null` with that real status; an allowlisted country this indicator's data does not cover
is `v: null, s: "missing"`; a licence-excluded country (`XK`) has no entry in `values` at all, only
in `excluded_by_licence`. Verified with `tests/test_export_europe_countries.py`.

Wired into the same `make exports` line `export_europe_nuts2.py` already has (no `$(DB)`
dependency -- reads only the committed CSV store and geometry, so it is safe whether or not `make
fetch` ran). `scripts/sync_international.py` is already part of `make fetch` and the daily
Dagster run, so these two indicators do not need separate daily-path wiring; only the store list and
the exporter step changed.

### UI (macro.html, assets/belpulse/europe_map.js/.css, assets/belpulse/charts.js)

Region/country toggle drives which `index.json` the map reads. Country mode: click a country to
toggle its selection (visible outline), a keyboard-usable picker (chip list with search/filter) for
every allowlisted country regardless of map size, default selection Belgium, capped at 8 with a
message when hit. No indicator id appears in `macro.html` or `europe_map.js` -- both read the
indicator list from the payload index (CLAUDE.md rules 2/24).

The comparison card draws one small-multiple line chart per country indicator (7: the six map
indicators plus the GDP volume index), one line per selected country, a consistent colour per
country across all seven charts, `charts.js`'s existing multi-series support extended rather than a
second chart library. EU27_2020/EA21 shown as togglable reference lines wherever the indicator's own
`reference_lines` block has them. Estimates/provisional values visually distinguished per
`charts.js`'s existing convention; a missing period breaks the line, never draws as zero.

`config/national_sections.yaml`'s `international` `unavailable` entry and the `europe` section's
`empty_reason` are removed now that the card is built; `#international` stays a valid anchor (rule
31).

## Data / schema changes

Two new `config/indicators/*.yaml` (both `source_id: eurostat`, multi-geo fetch). Two rows added to
`config/stores.yaml`'s existing `international` store. Two rows added to
`config/geography/international_excluded.csv` (`EU27_2007`, `DE_TOT`). One new predicate,
`is_country_level_code()`, in `src/geography/international.py`. No change to `observations`,
`indicators`, `geographies` columns; no change to the NUTS 2 batch's own configs or payloads.

## New data sources

None. Same `eurostat` source row the pilot and NUTS 2 batch already use (`docs/data_catalog.md`);
the two new datasets (`nama_10r_2gdp`, `demo_r_pjanaggr3`) are already approved (the NUTS 2 batch
reads the same two datasets).

## Tests

- `tests/test_export_europe_countries.py`: payload shape, five states, class breaks, the rebase
  math (hand-computed, including the missing-quarter `na` case and the zero-base guard), the
  geometry/allowlist cross-check refusal, byte-identical rebuild, real BE/DE/EU27_2020 figures
  against the live-checked numbers above.
- `tests/test_europe_panel.py` extended: toggle, country click-select, picker, the 8-country cap,
  comparison charts rendering N lines, the no-indicator-id regex test, the same-origin-only network
  test (the NUTS 0 geometry must be reachable through the same fetch-shim mechanism, no new remote
  host).
- Geography: `is_country_level_code()` unit tests (2-letter codes, underscore aggregates, a
  NUTS-subdivision-shaped code correctly rejected, `EA21` correctly NOT filtered when the filter is
  not applied).
- `config/national_sections.yaml` export check: `international` no longer `unavailable`, `#international`
  anchor still resolves.
- Run with the maintainer's standing instruction: targeted tests for changed files; CI runs the full
  suite.

## Assumptions and open questions

1. **Which indicators paint the map.** The handoff names `GDP_PC_PPS_COUNTRY`/`POPULATION_COUNTRY`
   explicitly and confirms `UNEMPLOYMENT_RATE_EUROPE` for the map; it does not explicitly say
   whether `HICP_ANNUAL_RATE_EUROPE`/`GOV_DEBT_EUROPE`/`CONSUMER_CONFIDENCE_EUROPE` are map-eligible
   too. Implemented as: all six are map-eligible (each is directly comparable country to country;
   only the GDP level fails that test). Flagged for the maintainer to confirm or correct.
2. **No euro-area reference line for GDP per capita or population** -- `nama_10r_2gdp` and
   `demo_r_pjanaggr3` do not carry a euro-area aggregate at all (live-checked, 2026-09-15), only
   `EU27_2020`. This contradicts the handoff's assumption that both aggregates would be available
   for every indicator.
3. Georgia/Moldova have no NUTS 0 outline in this Nuts2json 2024 vintage at all (not merely
   unpublished at this URL, as the NUTS 2 batch's overseas-territory insets were) -- they remain
   selectable in the picker and appear in comparison charts, just never paint on the map.

## Rollout / risks

- The five newly-visible pilot indicators inherit whatever real coverage gaps Eurostat itself has
  per country (see the "allowlisted but absent" lists in the sync output) -- rendered as `missing`,
  never blank.
- Rollback: remove the two new indicator configs and store entries, the country payloads and the
  NUTS 0 geometry, and restore `national_sections.yaml`'s `unavailable`/`empty_reason` entries. No
  change to the NUTS 2 batch or any Belgian table/export.

## Amendment, 2026-09-15 -- maintainer-requested polish batch

Seven follow-up fixes to the Europe panel, all on `macro.html#europe`, requested directly by the
maintainer (not a new spec document -- a fast-turnaround polish pass on the batch above and on
Europe NUTS 2, `docs/features/europe_nuts2.md`). `assets/belpulse/europe_map.js`,
`assets/belpulse/europe_map.css`, `assets/belpulse/charts.js`, `assets/i18n.js`,
`scripts/export_europe_countries.py`, `scripts/export_europe_nuts2.py`.

1. **Africa/Middle East hidden from the map.** eurostat-map's own `filterGeometriesFunction` hook
   drops a fixed denylist (Libya, Egypt, Israel, Palestine, Jordan, Lebanon, Syria, Saudi Arabia,
   Kuwait, Iraq, Iran, Algeria, Tunisia, Western Sahara, Morocco) from the `cntrg` background-fill
   layer, in both map modes. `cntbn` (the background-country BORDER-LINE layer) carries no per-
   country ISO2 id in this topology, only numeric segment ids with `eu`/`efta`/`cc`/`oth`/`co`
   flags shared with Russia/Belarus/Ukraine/the Balkans -- it is deliberately left unfiltered, a
   known, documented limitation rather than a risk of dropping a legitimate border. Never touches
   `nutsrg`/`nutsbn` (the 39 licensed countries, Turkey included) or either geometry file itself.
2. **Region `<select>` and the new region picker (point 3) are grouped by country**, `<optgroup>`
   for the select and a labelled chip group for the picker, both keyed on a NUTS code's own first
   two characters. Country display names come from `public/data/europe/countries/index.json`,
   loaded once at panel init (not lazily on first switch to country mode) so grouping works even
   when the panel opens straight into Région mode.
3. **Region-mode selection and comparison charts.** `state.region.selected` (cap 8, same shape as
   `state.country.selected`) mirrors country mode's map-click/checkbox-picker/cap-message model
   exactly. Starts EMPTY -- no single Belgian region is an obvious default the way `['BE']` is for
   country mode. "Comparaison internationale" is now MODE-SCOPED: region mode shows the 3 NUTS2
   indicators (`public/data/europe/nuts2/*.json`), country mode is unchanged (7 country indicators).
   NUTS2 payloads carry no `reference_lines` key at all, so region mode omits the EU27/euro-area
   checkboxes entirely rather than fabricate a non-existent aggregate (rule 26). A small read-side
   normalizer (`payload.periods || payload.years`) lets one shared card-rendering function serve
   both payload shapes without renaming either exporter's existing field names.
4. **"Croissance annuelle" (year-on-year growth) toggle**, level series only --
   `GDP_PC_PPS_COUNTRY`, `POPULATION_COUNTRY`, `GDP_VOLUME_EUROPE`, `GDP_PC_PPS_NUTS2`,
   `POPULATION_NUTS2`. Computed in Python at export time (rule 4) via
   `src.analytics.derived.growth_rate`/`shift_period_years` (already handles both annual and
   quarterly periods correctly -- reused directly, not reimplemented), added as a `yoy` field
   alongside each period's `{v, s}`, rounded to 1 decimal at export. A top-level `has_yoy` boolean
   marks which payloads carry it (true for the 5 above, false elsewhere, present on blocked-shape
   payloads too for uniformity) so the UI never has to know an indicator id. The toggle is per-card,
   only appended when `has_yoy` is true; while it is on, that card's EU27/EA21 reference line (when
   otherwise applicable) is hidden entirely rather than plotted next to a percent-growth line on the
   same axis.
5. **No per-point markers on comparison-chart lines.** `assets/belpulse/charts.js`'s `drawLine` gets
   a new `markers` option (default `true`, so every other caller -- macro.html's other history
   panels, the single-geography detail chart in europe_map.js -- is unaffected); only the country
   and region comparison renderers pass `markers:false`.
6. **Palette picker**, reusing `map.html`'s exact 5 names/hex values and its already-trilingual
   `mapPaletteLabel`/`mapPalette_*` i18n keys (no duplicates added). A real cascade-fight risk,
   flagged in advance: `europe_map.js`'s existing `applyRampTokens()` already documents that a
   `.bp-europe-map`-scoped CSS rule for `--ramp-*` loses to `assets/commune_map.css`'s own
   `:root[data-theme][data-palette]` rule on a page that loads both stylesheets -- exactly why
   today's single ramp is applied as an inline style, not CSS. The picker extends that SAME inline
   mechanism for all 5 palettes; the `[data-palette]` CSS block `europe_map.css` also carries is
   decorative/inspectable only. Own localStorage key (`belpulse-europe-palette`), independent from
   the commune map's own (`belpulse-map-palette`) -- deliberate, not a bug. Default `'default'`
   (today's existing blue ramp), not map.html's own `'bluered'` default, so a reader who never
   touches the picker sees no change.
7. A real bug found by screenshot review, not by the automated tests as first written: hiding the
   EU27/EA21 reference checkboxes in region mode (`el.hidden = true`) did not actually hide them on
   screen -- an author CSS rule (`.bp-europe-compare__refs{display:flex}`) of equal specificity to
   the browser's own `[hidden]{display:none}` default was winning the cascade, the same class of
   bug `.bp-europe-map__mode-section[hidden]` already had to guard against earlier in this file.
   Fixed with the same `[hidden]{display:none}` override; the test that had only checked the
   `el.hidden` DOM property (not the computed style) was strengthened to check
   `getComputedStyle(el).display` instead, so this class of bug fails the suite next time.

Tests: two hand-computed unit tests (one per exporter, `growth_rate`'s own hand-computed tests
already live in `tests/test_derived.py` -- these only prove the field is wired onto the right cell)
plus one extended real-browser pass through `tests/test_europe_panel.py` (24 tests total in that
file after this batch). Kept deliberately minimal, per the maintainer's explicit "go fast" scope for
this batch -- not a new large suite.
  change to the NUTS 2 batch or any Belgian table/export.

## Amendment, 2026-09-15 (later) -- every country indicator on the panel, with a chart filter

Maintainer's ask, verbatim: "Brancher sur le panneau Europe en mode Pays, 25 indicateurs
possiblement visibles en dessous mais avec un filtre pour choisir." Follows the Eurostat
additional domains batch (docs/data_catalog.md, 2026-09-15), which had loaded 18 country-level
indicators into the `international` store without publishing them anywhere.

- `scripts/export_europe_countries.py` now publishes 24 indicators (was 7): the original 7 plus 17
  of the 18. **Map (20):** the original 6 + public finance (balance, tax receipts, health
  expenditure, quarterly debt), employment (LFS), social inequalities (Gini, poverty, AROPE),
  energy (renewable share, GHG emissions), demography (life expectancy, population growth rate),
  innovation (R&D expenditure, R&D personnel). **Chart-only (4):** GDP volume (rebased, as before)
  plus total value added, exports and imports of goods and services -- levels in million euro,
  never painted, for the same reason the maintainer approved for GDP volume; drawn as the levels
  Eurostat publishes, not rebased, with the growth toggle as the cross-country reading.
- **Left out on purpose:** POPULATION_EUROPE (demo_pjan) -- it duplicates POPULATION_COUNTRY
  (the NUTS 0 rows of demo_r_pjanaggr3, already on the map) with a slightly different vintage;
  two "Population" entries would read as a mistake. It stays in the store.
- Growth toggle (`has_yoy`) extended to the new level series: employment (LFS), GHG emissions,
  R&D personnel, value added, exports, imports. Not to the rates/shares/balances, not to life
  expectancy.
- **The filter.** The comparison card gets a chip per indicator of the active mode plus All /
  None. It starts on the seven charts the card showed before (`compare_default: true` on the
  index entries -- a data binding, not an id list in the renderer, rules 2/24); the other 17 are
  one tick away. Region mode gets the same chips over its 6 indicators, all ticked by default. A
  reader's ticks last the page, not longer (no storage).
- Units the new indicators introduced (`thousand_persons`, `kt_co2eq`, `fte`, `per_mille`,
  `years`, `meur_clv2010`, `index_0_100`) are read trilingually in `assets/commune_map.js`'s
  shared unit vocabulary, so no raw unit code reaches a reader (rule 7).
- Found on the way: the rebased branch of the exporter filled each period's missing countries
  by iterating a *set*, whose order differs per process -- the committed GDP_VOLUME_EUROPE.json
  and a fresh rebuild disagreed on the position of one key with no data behind it. Now sorted,
  like the map branch already was (rule 35).
- Verified in a real browser (tests/test_europe_panel.py, 22 passed): 20 entries in the Pays
  map's indicator select, 7 cards by default, All -> 24, unticking one chip removes exactly that
  card; no page errors. Screenshot: screenshots-review/europe-countries/08-*.png.

## Amendment 2026-09-15 (layout): map left, controls right

Maintainer request on the live page. Layout only: no payload, formula or indicator changes.

- The macro page's header stays frozen at the top on a desktop screen, and the section menu is
  pinned flush to the left edge at full height. The breadcrumb and the commune search box are
  gone from this page (commune search is still on home2 and profiles).
- The Europe panel has no visible title or intro above the map. It is two columns: the map
  (about 65% wide, 75% of the screen tall) and a rail on the right (about 35%).
- Inside the map: the colour legend as a small box in the bottom-left corner (the unit heads it);
  the source as one short "Source: Eurostat" link to Eurostat's page for that dataset, plus the
  boundary credit, in the bottom-right corner. Dataset code, retrieval date and geography vintage stay
  on the link's hover text, on every chart card and on the detail card.
- The rail, top to bottom: the Régions/Pays toggle and palette, indicator and period, the region
  or country picker as a closed menu, the detail card for a clicked region or country (with a close button), the "charts shown" filter as a closed menu, the EU27 /
  euro-area reference toggles, then the first three comparison charts.
- Every other comparison chart sits in the grid under the map; that section hides itself when the
  rail already holds them all.
