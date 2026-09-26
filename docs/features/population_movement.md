# Feature: Population movement (Statbel, annual, automatic)

Status: implemented
Catalogue row: docs/data_catalog.md, "Wave 3 -- Statbel population movement" (approved,
maintainer, 2026-09-23)
Branch: feat/ns3-population-movement

## Source

Statbel's population-movement workbook, one sheet per year, 1992 through 2025 as of
2026-09-23 (measured). Theme page:
`https://statbel.fgov.be/fr/themes/population/mouvement-de-la-population`. `source_id:
statbel` (existing `config/sources/statbel.yaml`, untouched by this batch). Used to be
reachable live from this pipeline's network context in CI. **Not daily any more as of
2026-09-23 -- see below.**

## Not daily any more (2026-09-23)

Diagnosed on the scheduled run 2026-09-23T18:09: statbel.fgov.be answers every request
from a GitHub Actions runner with a CAPTCHA challenge page (HTTP 200, text/html, ~46 KB,
"This question is for testing whether you are a human visitor... What code is in the
image?", carrying a support ID) instead of the theme page or the workbook. A probe run
confirmed this is runner-specific -- the same URLs still return the real page/file from a
maintainer's own machine. This pipeline does not solve or evade CAPTCHAs.

`orchestration/commands.py`'s `population_movement_observations` Command now carries no
`workflow_step`, so it is absent from `TRACKED` and from the daily `fetch_sources` job. The
committed store (`config/stores.yaml` `population_movement`, `mode: in_db`) is unchanged
and keeps flowing into every export; only the automatic refresh stopped.

**How to refresh**, from a machine that still passes the CAPTCHA:
```
python scripts/sync_population_movement.py --db data/belgian_macro.db
```
or, if even that machine gets challenged, open the theme page in a browser (which passes
the CAPTCHA interactively), follow its workbook link, save it, then:
```
python scripts/sync_population_movement.py --db data/belgian_macro.db \
    --from-file mouvement-de-la-population.xlsx
```
`--from-file` runs the exact same parse/resolve/transition-exclusion path as the live
fetch. The indicators' `max_age_days` and the `staleness` validation rule are what flag
when a refresh of this data is actually due. `statbel.yaml`'s `fetch_window_days`/
`fetch_silence` check is keyed on the shared `statbel` source_id, not per-indicator -- it
still passes because `statbel_local_units` (Bestat) stays in the daily gate and keeps
fetching that source_id every day.

## Link discovery

The theme page links one `.xlsx` whose path contains `pop1992-mov` (the workbook's own
filename stem, stable across the URL-encoded space in the page's own section title).
`src/fetchers/population_movement.discover_xlsx_url()` reads the href off the page's HTML
with a plain regex -- the same discipline `scripts/sync_bankruptcies.py` uses for its own
landing page. Zero matches, or more than one, raises `PopulationMovementLinkNotFoundError`
-- never a cached or hard-coded URL fallback (CLAUDE.md rule 13).

## Sheets and columns

One sheet per year, named by the bare year ("2025", "2024", ...). Header spans rows 2-4
(merged cells); data starts at row 5. `_parse` validates four column labels by name, not
position, checked together with the row-2/row-3 group labels sitting above them so a
Statbel reorder inside a group (e.g. swapping ENTREES/SORTIES) is caught even where the
leaf label alone would not move:

- D / NAISSANCES -- live births
- E / DECES -- deaths
- I / SOLDE, under G-I "MOUVEMENT MIGRATOIRE INTERNE" (ENTREES/SORTIES/SOLDE) -- net internal
  migration
- P / SOLDE, under J-P "MOUVEMENT MIGRATOIRE INTERNATIONAL" -- net international migration

A header that has moved is refused, never silently re-indexed (CLAUDE.md rule 13). Row 2's
group labels are written only at each merge's own origin column by openpyxl in read-only
mode (a merged value is not fanned out across its whole span), so `_check_header` reads them
at that origin column, not at the data column underneath.

The four SOLDE/NAISSANCES/DECES columns are the publisher's own totals, taken as published.
Nothing in this batch recomputes or derives a value from them (CLAUDE.md rule 6) -- in
particular, the two SOLDE columns are Statbel's own net figures (arrivals minus departures),
not something this pipeline sums from separate ENTREES/SORTIES columns, because doing that
would make ENTREES/SORTIES/SOLDE inconsistency in the source file invisible instead of
loudly wrong.

## Geography: resolved per row, at the row's OWN period

Every (indicator, nis, sheet year) cell resolves via `resolve_geo(conn, nis, sheet_year)` --
the ordinary rule (CLAUDE.md rule 3), the commune map as of 1 January of the sheet's own
year, not today's map and not the following year's.

An earlier version of this sync resolved at `sheet_year + 1`, reasoning from column W's own
label ("POPULATION AU 31 DECEMBRE (SOIT AU 1/1 DE L'ANNEE SUIVANTE)", population at 31
December i.e. 1 January of the FOLLOWING year) that the whole sheet's commune grid was dated
to the following year. That reading was rejected after an independent audit of PR #233
traced its effect downstream: `scripts/export_aggregates_csv.py`'s `_universe_resolver`
classifies an indicator as "pinned" to one fixed vintage the moment any period's geo_id set
is not a subset of the calendar map for that SAME period. Resolving 2018's rows against the
2019 map put post-merger codes onto their pre-merger sheet year, so every period's geo_id set
failed that subset check and all four indicators were detected as pinned to a union universe
of 620 communes -- a commune count that has never existed at any point in Belgian geography.
Every Belgium/region/province aggregate was then measured against 620 instead of the real,
period-correct denominator (589 in 1992, 581 in 2018-2023, 565 from 2025), which wrongly
suppressed roughly 1,660-1,870 aggregate cells built from otherwise-complete data (see the PR
body for the exact measured count) -- for example Flanders' 2024 BIRTHS aggregate, complete
at 273 of the 300 communes covering it (91%), was suppressed entirely rather than published,
because the pinned-620 denominator made 273/620 read as 44% coverage, below the 90% floor.

## Transition sheets: forward-mapped codes, excluded loudly

Statbel publishes each merger's transition year already carrying the POST-merger codes, with
no row at all for the pre-merger predecessor codes. Measured directly against the real file
and the geographies table (2026-09-23):

- Sheet "2018" already carries the eighteen commune codes the 2019-01-01 merger created
  (12041, 44083-85, 45068, 51067-69, 55085-86, 57096-97, 58001-04, 72042-43), and none of
  their pre-merger predecessor codes (e.g. 55010, old Enghien) appear in that sheet at all.
- Sheet "2024" already carries 82039 (Bastogne+Bertogne, valid_from 2024-12-02, inside that
  sheet's own calendar year) plus the twelve other 2025-01-01 merger codes (23106, 37021-22,
  44086-88, 46029-30, 71071-72, 73110-11) -- thirteen codes total -- again with no
  predecessor rows.

Resolved at their own sheet year (the fix above), none of these thirty-one rows resolves: the
commune did not legally exist yet at that sheet year's 1 January, and there is no predecessor
row to fall back to, because the sheet simply does not carry one. This is the missing state
(CLAUDE.md rule 26), not a zero and not an error -- on the year's own published map, that
commune's births/deaths/migration figures genuinely do not exist as a single row; they are
split across a predecessor the sheet does not name and a successor that does not yet exist.

`scripts/sync_population_movement.py` drops these rows, but never silently: each excluded
`(sheet_year, nis)` pair is counted, the codes are printed in the run's own report, and the
full set is checked against `_EXPECTED_TRANSITION_EXCLUSIONS`, a set declared in the script
from the measurement above. A code that fails to resolve and is NOT in that declared set --
in any sheet, including one not listed here at all -- fails the whole run (CLAUDE.md rule 13:
nothing is ever dropped silently). A future merger year will need its own entry added to
`_EXPECTED_TRANSITION_EXCLUSIONS` before its sheet can load; the run refuses rather than
guessing which codes are expected to be missing.

A non-municipal code (Belgium, region, province, arrondissement) is classified by the NIS
code alone, independent of sheet year -- a code's level never changes across time -- rather
than by the row valid at that sheet's own year, because a handful of arrondissement codes
(e.g. 58000) are themselves new as of the same mergers and so have no geography row at all in
an earlier sheet; resolving those "as of sheet year" would wrongly route them into the
transition-exclusion guard instead of the ordinary non-municipal row-filtering trap they
actually are (see "Row-filtering trap" below).

## Row-filtering trap

Do not select commune rows by "5-digit code not ending in 000" -- that also matches
20001/20002 (the two Brabant provinces). `_parse` does not filter at all; every row with a
5-digit numeric CODE INS is emitted with its raw code, unfiltered.
`scripts/sync_population_movement.py` does the real filtering, by checking each code's level
in the geographies table: a code that is not `municipality` (Belgium, region, province,
arrondissement) is dropped, expected and silent -- not an error, not counted as unresolved,
not a transition exclusion.

## Units: count vs. balance

`BIRTHS` and `DEATHS` are `unit: count` -- always non-negative, and subject to CLAUDE.md's
`counts_non_negative` validation rule as normal.

`INTERNAL_MIGRATION_NET` and `INTERNATIONAL_MIGRATION_NET` are `unit: balance` -- the
existing unit for a signed, additive figure (already used by e.g. `BUSINESS_CONFIDENCE`).
Both migration SOLDE columns are routinely negative (net outflow) in the real data: net
internal migration alone carries a negative value in 6,430 of the 79,596 rows this sync
writes from the real file, and net international migration in 5,327. `counts_non_negative`
filters on `unit = 'count'`, so `unit: balance` exempts these two indicators from that rule
without weakening the rule itself or touching `src/validation/rules.py` -- a negative
`BIRTHS` or `DEATHS` row still fails validation exactly as before.

## Aggregation

All four indicators are additive counts (`is_additive=1`, `aggregation_method='sum'`),
summed the ordinary way to province/region/Belgium over the geographies that existed in that
period -- the existing aggregation engine does this unmodified; nothing in this batch touches
`scripts/export_aggregates_csv.py`, the aggregation engine, or `resolve_geo()` itself (all
three are ADR-gated per CLAUDE.md rule 19). The per-row, own-period resolution above is what
keeps each indicator's per-period geo_id set a genuine subset of the calendar map, so the
existing coverage-denominator logic (`_universe_resolver`) measures real coverage instead of
detecting a false "pinned" vintage.

## Duplicate guard

A repeated `(indicator_id, nis, sheet_year)` triple within the workbook raises rather than
silently keeping the last value -- mirrors `src/fetchers/walstat.py`'s own duplicate guard.
Not observed in the real file (verified: 622 codes, 622 distinct, in the largest sheet,
2025), but the workbook is out of this pipeline's control and a future republish could
introduce one.

## Rates: Decided, ADR 0014

Per-1,000 birth/death/migration rates needed `src/analytics/derived.py`, gated behind an ADR
per CLAUDE.md rule 19 (touches the analytical-formula surface). Decided in
`docs/decisions/0014-rates-per-1000-residents.md`: `per_thousand(value, population)`, one
denominator (POPULATION_BY_COMMUNE on 1 January of the SAME year), coverage 2017-2025 only
(null, never 0, for the 18 communes merged in 2019's 2018 gap and the 13 communes merged in
2025's 2024 gap), aggregated by summing both sides and recomputing, never by averaging a
commune rate.

## Store

`config/stores.yaml`'s `population_movement` entry: `mode: in_db`, `source_id: statbel`, all
four indicators in one CSV (not `one_csv_per_indicator`) at
`data/population_movement_observations.csv` -- measured well under the 25 MB commit limit
(see the file's own committed size for the current figure; re-measured after each
regeneration, most recently after the per-own-period resolution fix in this batch).
`reference_rows` points at `scripts/sync_population_movement.py --reference-rows-only`. The
committed data was produced by running the real sync against the live Statbel file once and
offloading, the same way the bankruptcies and WalStat stores' first committed CSVs were
produced.

## Out of scope (this batch)

Per-1,000 rates (see above). Commune-page surfacing (`config/local_sections.yaml`). Any
change to `scripts/export_aggregates_csv.py`, the aggregation engine, `resolve_geo()`, the
schema, `src/validation/rules.py`, or `config/sources/statbel.yaml`.
