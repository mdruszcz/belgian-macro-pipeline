# Feature: /local interface (Block K)

Status: built (MVP)
Issue: (Block K — /local interface, docs/steps)

## What this is for

The public-facing per-commune page: a bourgmestre or directeur financier opens a link on a phone in
a meeting and sees their commune's own numbers in under a second. Block J already produces the data
(`public/data/communes/{nis}.json`, `public/data/metadata/geographies.json`); this block is the page
that reads it.

## Grounded in what the pipeline actually publishes today, not the roadmap's aspiration

The roadmap's own sketch for the header ("Population, 5Y growth, median income, unemployment,
businesses, housing price") assumes six figures. Checking `data/communes_history.csv` and
`data/metadata/indicators.json` against that list before building anything:

| Roadmap figure | Available? | Real source |
|---|---|---|
| Population | yes | `POPULATION_BY_COMMUNE` |
| 5Y growth | yes | `POPULATION_CHANGE_5Y` (derived) |
| Median income | partial | `AVG_NET_TAXABLE_INCOME` is a **mean**, not a median — no municipal median exists (Block F's fiscal-income note: the commune file has no median column, and the sector file's median cannot be aggregated) |
| Unemployment | **no** | `UNEMPLOYMENT_RATE` is national-only (`be:country`); no municipal unemployment series exists in this pipeline |
| Businesses | yes, but single-period | `LOCAL_UNITS_BY_COMMUNE` — one quarter (2023-Q4) only, per `statbel_adapter.md`'s "only the latest quarter is available" |
| Housing price | **no** | Housing is `[DEFERRED]` in Block F — no dataset was ever selected in Block E |

**Decision: ship four of six honestly rather than fabricate or omit silently.** The header shows
Population, 5Y growth, Avg. net taxable income (labelled "average", never "median"), and Local
business units, each captioned with its real reference period. Unemployment and housing price render
through the same `.no-data` component the rest of the page uses for genuinely absent sections, with
the caption explaining why ("not collected at municipal level" / "housing data not yet collected"),
rather than a blank space that reads as a bug.

## Fixed section structure

Per the roadmap ("fixed structure across all communes makes the product feel systematic"), every
commune page renders the same eight sections in the same order, whether or not that commune has data
for them:

| Section | Data used | State today |
|---|---|---|
| Overview | the four header figures again, plus commune ancestry (region/province/arrondissement) | live |
| Demography | `POPULATION_BY_COMMUNE` history chart, three age bands (`POPULATION_AGE_0_14/15_64/65_PLUS`), `DEPENDENCY_RATIO` | live |
| Income | `AVG_NET_TAXABLE_INCOME` history, `FISCAL_TOT_NET_TAXABLE_INC`, `FISCAL_TOT_MUNICIP_TAXES`, `FISCAL_TOT_TAXES`, `FISCAL_NBR_NON_ZERO_INC` | live |
| Employment | — | `.no-data`: "Municipal-level employment data is not collected by this pipeline" |
| Business | `LOCAL_UNITS_BY_COMMUNE` | live, single period, captioned as such |
| Housing | — | `.no-data`: "Housing data collection is deferred (see Block F)" |
| Mobility | — | `.no-data`: "Mobility data collection is deferred (see Block F)" |
| Finances | — | `.no-data`: "Coming in Phase II (Block O — Walloon municipal finance)", per the roadmap's own instruction to render this as "coming soon" until Phase II |

A commune with zero rows in a section that CAN have data (e.g. a 2025-merger successor with no
fiscal history, per `fiscal_income.md`) gets the same `.no-data` component as a section with no
dataset at all — the component does not distinguish "no dataset exists" from "this commune has none
of an existing dataset"; both read to a visitor as "nothing to show here," which is the honest
message either way.

## Search and routing

`metadata/geographies.json` (622 rows, Block J) is the whole search index: client-side substring
match over `name.en/fr/nl` and `nis_code`, plus a region/province/arrondissement browse tree built
from `parent_geo_id`. No server, no build step — this is what makes the search instant on a phone.

Routing is a query parameter, `local.html?nis=11002`, not `/local/{nis}` yet. Permanent static
per-commune URLs are Block L's own `[BUILD]` step ("statically generated... this is the entire basis
of the SEO moat"); building static generation before Block L's URL/SEO design exists would mean
redoing it. The query-param version is fully functional today and trivially becomes a build-time
template later — the page's JS already separates "read `nis` from wherever" from "render this
commune," so swapping the source of `nis` is a one-line change when Block L lands.

## Reuse, not a framework rewrite

Per the roadmap's explicit ban on a frontend rewrite before 50%: `local.html` reuses
`communes.html`'s CSS custom-property token system (`--bg`, `--surface`, `--text`, `--accent`, IBM
Plex Sans/Mono/Spectral) rather than inventing a second design language, and adapts
`dashboard.html`'s canvas line-chart renderer (`_rc`) into a single shared `drawSeries()` function
used by every history chart on the page, rather than one chart implementation per section.

## The `.no-data` component

One component, one visual treatment, used for every kind of absence on the page: a section with no
dataset, a section whose dataset has no rows for this specific commune, and a chart with fewer than
two points (nothing to draw a line through). Always states *why* the data is missing, never just
that it is — "not collected" and "coming in Phase II" are different situations and a bourgmestre
should not have to guess which.

## Tests

No browser is available in this environment (as in prior blocks); verification here follows the same
method used for Block G/I — Node emulating the pure-JS logic paths (search matching, header
figure selection, section-availability decisions) against fixture payloads, plus a manual
`python -m http.server` + `curl` smoke check that the page, its fetches, and a real commune payload
all resolve. `tests/test_local_ui_logic.py` shells out to `node` to run those same logic paths
against saved fixture JSON, so a future regression in the matching or availability logic fails in CI
without needing a browser.

## What is deliberately NOT built here

- **Permanent static `/local/{nis}` routes and SEO markup.** Block L's job, named above.
- **Comparison, percentiles-in-context, peer figures.** Block L again.
- **"5 things to know" ranking.** Block V, needs the signals engine (Block Q) that does not exist yet.
- **Full FR/NL/EN interface chrome.** Block X. Commune *names* are already trilingual (search works
  in all three); indicator *labels* and UI strings are English only until Block X's translation
  pipeline exists.
