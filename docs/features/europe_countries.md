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
