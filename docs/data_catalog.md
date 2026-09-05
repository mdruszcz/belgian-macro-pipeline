# Data Catalog

Per `CLAUDE.md` rule 8: no new data source may be added without a row here, approved by the
maintainer. That rule *is* Block E's anti-scope-creep step, word for word — already in place
before this block existed, not added because of it.

## Scoring a new candidate (Block E)

Before a candidate dataset is approved and added below:

```
priority = (decision_value × comparability × coverage × depth) ÷ maintenance_cost
```

- **decision_value** — what a directeur financier would actually pay for this, or use to decide
  something. Not a general "interesting data" score. **Only the maintainer can fill this in** —
  it depends on the client conversations in Block Z, not on anything visible from the dataset
  itself.
- **comparability** — can it be compared across communes/regions on equal terms (same
  methodology, same period), or is it apples-to-oranges by construction?
- **coverage** — fraction of Belgium's 565 communes actually covered, not just claimed.
- **depth** — years of usable history. A dataset with 18 months of data cannot support a
  five-year trend claim, however good its other scores are.
- **maintenance_cost** — parsing fragility (stable API vs. hand-formatted spreadsheet), update
  cadence relative to how often the pipeline runs, and any licence-compliance overhead (e.g. an
  attribution requirement per Statbel's CC BY 4.0 terms below).

**Refusal criteria** — deliberately left for the maintainer to write, not drafted here. Per the
roadmap's own reasoning for this exact step, decision_value depends on conversations only the
maintainer has had; refusal criteria are the negative form of the same judgment (what disqualifies
a dataset regardless of score — e.g. a licence that forbids commercial reuse, or a publisher with
no update history). Filling this in is `docs/steps`' `[H] Write the catalogue header` step.

**Discipline check (CONTROL E):** the *approved* table below must have exactly 10 rows once
Block E is complete, not 11. Rejected candidates are marked `DEFERRED` in the candidates table,
never deleted — so a dataset already considered and declined doesn't get re-researched in three
months by an agent with no memory of the first pass.

## Approved sources

These five are already in production use; rows here formalize existing fetches, not new
approvals under the scoring process above (predating this catalog, `statbel_geography` added in
Block C). Licence terms for the four macro sources are marked TODO pending separate verification
— flagged, not blocking, since nothing new is being introduced.

| source_id | Agency | Adapter | Base URL | Licence | Cadence |
|---|---|---|---|---|---|
| `nbb` | National Bank of Belgium | `nbb` (SDMX) | `nsidisseminate-stat.nbb.be/rest/data/BE2` | TODO | daily |
| `dbnomics_eurostat` | Eurostat (via DBnomics) | `dbnomics` | `api.db.nomics.world/v22/series/Eurostat` | TODO | daily |
| `dbnomics_ameco` | AMECO/EC (via DBnomics) | `dbnomics` | `api.db.nomics.world/v22/series/AMECO` | TODO | daily |
| `fpb` | Federal Planning Bureau | `fpb` (XLSX) | `plan.be` | TODO | quarterly |
| `statbel_geography` | Statbel (Directorate-general Statistics) | manual download → `scripts/derive_geography_csv.py` | statbel.fgov.be open-data portal | **CC BY 4.0** — commercial reuse permitted, attribution required | ad hoc (on Belgian administrative reorganizations) |

## `statbel_geography` — reference geography

Added in Block C. This is **reference data, not an observation source**: it populates
`geographies` (the country, 3 regions, 10 provinces, 43 arrondissements, 565 communes, and 55
historical predecessor communes) and carries no indicator values.

Files, downloaded by the maintainer on 2026-09-05 and held under `data/raw/statbel/`
(gitignored — see `docs/features/geography.md`):

| File | Contents |
|---|---|
| `Nis9_Nis6_refnis_names_01012026.xlsx` | Statistical sectors with the full explicit hierarchy and NUTS codes, valid 2026-01-01 |
| `REFNIS_DEFINITIEF.csv` | Administrative entities, pre-2019 (589 communes) |
| `REFNIS_2019.csv` | Administrative entities, post-2019 wave (581 communes) |
| `REFNIS_2025.csv` | Administrative entities, post-2025 wave (565 communes) |
| `Conversion Postal code_Refnis code_va01012025.xlsx` | Postal code → commune; retained, not yet used |

### Licence — CC BY 4.0

Confirmed by the maintainer from Statbel's *Conditions générales d'utilisation* on 2026-09-05.
Statbel publishes data it owns under **Creative Commons Attribution 4.0**.

**Commercial reuse is explicitly permitted.** Both rights are granted "pour toute utilisation,
y compris commerciale":

- *Partager* — copy, distribute and communicate the material in any medium or format.
- *Adapter* — remix, transform and build upon it.

This settles the question the roadmap raised for Block E ("a dataset you cannot resell is worse
than useless once it is embedded in a paid report"). Selling analysis built on this geography is
within the licence.

#### Obligations this places on us

These are conditions, not suggestions — clause 6 terminates the licence automatically if they
are breached.

1. **Attribution** (5.1). Credit as `Source : Statbel` or
   `Source Statbel : (Direction générale Statistique - Statistics Belgium)`.
2. **Link to the licence** (5.1) — https://creativecommons.org/licenses/by/4.0/
3. **State that changes were made** (5.1). We do modify: the raw files are reshaped into
   `config/geography/*.csv`, codes are re-keyed to internal `geo_id`s, validity windows are
   derived by diffing vintages, and a merger crosswalk is computed. The attribution must say so,
   not merely name the source.
4. **No implied endorsement** (5.2). Nothing may suggest Statbel backs this product or approves
   how its data is used.
5. **Unofficial translations must be labelled** (3). Translations made on one's own initiative
   must be marked as such and cite the original source. **This applies to `name_en`**: no Statbel
   file supplies English, so every English name here is either an unofficial exonym from
   `config/geography/name_en_exonyms.csv` or the entity's own official-language name. Any surface
   showing English geography names must not present them as official Statbel labels.
6. **Third-party data is out of scope** (2). The licence covers Statbel's own data only. The
   NUTS codes carried in `geographies.csv` originate with Eurostat and are governed separately —
   they are currently captured but not published.

Governed by Belgian law (clause 7); granted for an indefinite term (clause 6).

**Not yet done:** points 1–4 are obligations on *published output*. The dashboard and any
exported payload that carries municipal geography needs the attribution string, the licence link
and the "modified" notice. Nothing municipal is published yet, so this is not currently in
breach — but it must land before the first commune-level page goes live. Tracked as a
Block K/J item.

## Candidate municipal-level datasets (Block E `[SPEC]`)

Facts only, no scores — `decision_value` and `difficulty` are the maintainer's own columns to
fill in (see the formula above), and `status` starts blank for the same reason. Every candidate
below was checked this session; confidence varies and is stated honestly rather than presented as
uniform. **Verified** = a page was actually opened and its content read. **Corroborated** = an
independent web search returned the same specific facts as a first pass, without opening the
page. **Snippet-only** = a search result named the page and gave a plausible detail, but nothing
here confirms the page still says that — treat these as leads, not facts, until someone opens
the URL directly. `statbel.fgov.be` and `data.gov.be` refused every direct fetch attempt this
session (silent failures, both from an agent and from a second, independent attempt) — that
reads as bot-blocking, not as the pages not existing, since search kept returning the same
specific, plausibly-real titles and URLs consistently across multiple queries.

| # | Dataset | Publisher | Geography | History | Update freq. | Licence | Format | Confidence | Status |
|---|---|---|---|---|---|---|---|---|---|
| 1 | [Fiscal statistics on income](https://statbel.fgov.be/en/open-data/fiscal-statistics-income) | Statbel | Commune (confirmed as a real dimension in the Bestat datasource — see below — but every standard/pre-built view found stops at province level) | 2005–2023 (open-data page); 2017–2019 confirmed directly via Bestat standard view (`850933c9`, province-level) | Annual | CC BY 4.0 | XLSX (open-data page); JSON via Bestat API (see below) | Corroborated + Bestat datasource verified directly | |
| 1b | Same dataset via Bestat API — datasource `IM_SOC_PSNL_INC_TAX_MUNTY` | Statbel (Bestat) | **Commune is a real dimension of this datasource** (name: "par commune de résidence") but no standard view exposing it was found — see "Statbel Bestat API" note below | Datasource "last data update" timestamp confirms it is live-maintained (Nov 2026) | Unconfirmed at commune level; region-level standard view covers 2017–2019 only | Not stated per-datasource; presumed CC BY 4.0 (Statbel's general policy) | JSON, CSV, XML, XLS, HTML, PDF via `https://bestat.statbel.fgov.be/bestat/api/views/{id}/result/{FORMAT}` | Verified directly (datasource id `b394aa82-5045-4483-9e79-cd5344651791`, fetched and read) | |
| 2 | [Fiscal statistics on income by statistical sector](https://statbel.fgov.be/en/open-data/fiscal-statistics-income-statistical-sector) | Statbel | Statistical sector (sub-commune) | 2005–2023 | Annual | CC BY 4.0 | XLSX | Snippet-only | |
| 3 | [Population by place of residence, nationality, marital status, age and sex](https://statbel.fgov.be/en/open-data/population-place-residence-nationality-marital-status-age-and-sex-12) | Statbel | Commune (aggregable to arrondissement/province/region) | Annual snapshot (1 Jan); exact earliest year not confirmed | Annual | CC BY 4.0 (site default, not independently confirmed on this page) | CSV | Snippet-only | |
| 3b | Census 2011+2021 population/household/housing indicators via Bestat — datasource `IM_SOC_GEO_IND_CENSUS_2021` | Statbel (Bestat) | Standard views confirmed at **province level only** (e.g. "Total population" view returns 28 rows: region × province × 2 census years) despite dozens of commune-sounding indicator names | 2011 and 2021 census years, confirmed directly | Static (census-based, not continuously updated) | Unconfirmed; presumed CC BY 4.0 | JSON etc. (Bestat API) | Verified directly (datasource id `e957ac31-44a2-4718-8469-10470d3c41d9`, one standard view fetched and read) | |
| 4 | [Sales of real estate according to nature of property](https://statbel.fgov.be/en/open-data/sales-real-estate-belgium-according-nature-property-land-register) | Statbel | **Unconfirmed at commune level** — Statbel's public bulletins for this series are region/province aggregates; whether the open-data file itself goes to commune level was not verified | Unconfirmed | Quarterly (per Statbel's general release cadence) | Presumed CC BY 4.0 | Unconfirmed | Snippet-only | |
| 5 | [Cadastral statistics of the building stock](https://statbel.fgov.be/en/open-data/cadastral-statistics-building-stock) | Statbel | Commune (stated: buildings in Belgium as of 1 Jan of the reference year) | At least one reference year confirmed to exist (2024) | Annual | Unconfirmed on page | Unconfirmed (likely CSV) | Snippet-only | |
| 6 | Building permits statistics | Statbel | Region/province confirmed via press releases; commune-level open-data availability **not verified** | Monthly figures referenced in recent press bulletins | Monthly | Unconfirmed | Unconfirmed | Snippet-only, no confirmed open-data page found (only a thematic page) | |
| 7 | Statistics on establishment units (business/enterprise) | Statbel | Commune, but counts are **banded/masked for confidentiality**, not exact — worth weighing against `comparability` in the formula above | Unconfirmed | Annual (VAT-registered units) | Unconfirmed | Unconfirmed | Snippet-only | |
| 7b | Local units (établissements) by commune via Bestat — datasource `IM_EAF_LCL_UNIT_POP` | Statbel (Bestat) | **True commune-level confirmed by directly fetching the data**: 566 rows, one per commune, exact (not banded) counts, e.g. Aartselaar 2232, Antwerp 66,381 | Datasource covers "since 2015 per quarter" but the commune-level standard view returns only the latest quarter (Q4 2023); a separate region-level standard view (`e21e18c6`) has full 2015–present quarterly depth. **Getting both commune granularity and full depth needs a custom cross-tab, not a standard view** — see note below | Quarterly (per datasource description) | Unconfirmed; presumed CC BY 4.0 | JSON (confirmed), + CSV/XML/XLS/HTML/PDF per the API structure | **Verified directly** — fetched real data, real commune names matching this repo's own `geographies.csv`, real counts | |
| 8 | [WalStat portal](https://walstat.iweps.be/walstat-accueil.php) — 19 themes incl. "Pouvoirs locaux" (local governance) | IWEPS (Wallonia) | Quartier / commune / arrondissement / province / bassin | Not stated on the pages opened; needs a catalogue-level query | Not stated | **CC0 for the data, CC BY-SA for maps** — a genuine two-licence split confirmed independently, not a page error | CSV, JSON | Verified (portal opened directly) + corroborated (licence split confirmed by a second, independent search) | |
| 9 | [WalStat open-data catalogue (DCAT-AP)](https://opendata.iweps.be/statdcat-ap/walstat) | IWEPS (Wallonia) | Same as above | Not stated; catalogue updated twice yearly (end of June, end of December) per iweps.be | Semi-annual catalogue refresh | CC0 (data) | RDF/XML catalogue → CSV/JSON | Verified (catalogue page opened directly) | |
| 10 | [IBSA — List of Belgian Municipalities in Urban Regions](https://ibsa.brussels/opendata) | IBSA (Brussels) | Belgium-wide | 2021–2025 | Last updated 25 June 2026 (per page) | CC BY 4.0 | XLSX, CSV (+ codebook) | Verified (page opened directly) | |
| 11 | [IBSA — Brussels Municipal Demographic Projections](https://ibsa.brussels/opendata) | IBSA (Brussels) | Brussels-Capital Region, municipal | Projections 2026–2035 | Last updated 26 March 2026 (per page) | CC BY 4.0 | XLSX, CSV (+ codebook) | Verified (page opened directly) | |
| 12 | ABB / "Financieel profiel van het lokaal bestuur" (BBC financial reporting) | ABB / Flemish government (`vlaanderen.be/lokaal-bestuur`) | Municipality + OCMW + the 10 Antwerp districts | Not confirmed — the actual dataset download page was not reached (redirects led to a general landing page) | Quarterly submissions feed the underlying BBC system, per its own description | Unconfirmed | Interactive tool; underlying data format unconfirmed | Snippet-only, page not reached | |
| 13 | ["De financiële toestand van de Vlaamse gemeenten"](https://publicaties.vlaanderen.be/view-file/78642) (annual analysis of Flemish municipal accounts) | ABB / Flemish government | Municipality | 2024 annual accounts (latest edition found) | Annual | Unconfirmed | **PDF report, not structured data** — a real cost against `maintenance_cost` in the formula above | Snippet-only | |
| 14 | ["Jouw gemeente in cijfers" / Gemeente-Stadsmonitor](https://gemeentemonitor.vlaanderen.be/) | Statistiek Vlaanderen | Municipality (~200 indicators, ~70 from a resident survey per search snippets) | Unconfirmed | Unconfirmed | Unconfirmed | Page did not return usable content this session; needs a direct visit | Snippet-only | |
| 15 | [ODWB — Open Data Wallonie-Bruxelles](https://www.odwb.be/pages/home/) | Agence du Numérique (Walloon Region + French Community) | Confirmed to include a "Données locales" / commune-level section, exact datasets not enumerated | Unconfirmed | Unconfirmed | Not stated on the homepage; needs a dataset-level check | Unconfirmed | Verified portal exists and structure (homepage opened directly); individual dataset details not checked | |
| 16 | [data.gov.be](https://data.gov.be/en/documentation/licenses) (federal aggregator, ~10,000 datasets across 14 categories per search snippets) | Federal Belgian government | Aggregates federal + some regional/local; explicitly *not* a one-stop shop — its own docs point out to regional portals | N/A (aggregator) | N/A | Default **CC0**, "comply or explain" — a department may instead choose CC BY 4.0 / CC BY-SA 4.0 / CC BY-NC 4.0 / CC BY-ND 4.0 (per search snippet only, could not open the licence page directly to confirm) | Varies by dataset | Snippet-only, page unreachable | |

### Statbel Bestat API — real, verified, directly queryable

The maintainer supplied the structure of Statbel's actual data API (`bestat.statbel.fgov.be`,
distinct from `statbel.fgov.be` — a different subdomain that did **not** refuse fetches the way
the main site did this session). It works exactly as described, verified by directly fetching
real data, not just reading documentation:

- `GET /bestat/api/datasources/` — lists every datasource (182 found). Each has a stable UUID,
  bilingual/trilingual description, and a last-update timestamp.
- `GET /bestat/api/views/` — lists 1,341 pre-built ("standard") views, each tied to one
  `dataSourceId`, with a human name in one locale.
- `GET /bestat/api/views/{id}/result/{FORMAT}` (`FORMAT` = JSON/CSV/XML/XLS/HTML/PDF) — the
  actual export. This is the same shape as NBB's SDMX-CSV and DBnomics' JSON endpoints already
  integrated in `src/fetchers/` — a `StatbelSource(TimeSeriesSource)` adapter (Block D's
  interface) is a plausible, concrete fit once Block E selects specific datasets.

**The important, non-obvious finding**: a datasource's description naming "commune" as a
dimension does not mean a *standard* view exposes it at that granularity. Confirmed by directly
comparing two standard views built from the same `IM_EAF_LCL_UNIT_POP` datasource:

- One standard view returns true commune-level data (566 rows, exact counts) — but only the
  **single latest quarter**.
- Another standard view has the **full 2015–present quarterly time series** — but only at
  **region level** (3 rows per quarter).

The same pattern held for fiscal income and census population: the underlying datasource
supports a commune dimension, but every standard view actually found stops at province level.
**Getting both commune granularity and multi-year depth from Bestat requires building a custom
cross-tabulation** via the Bestat web UI (`https://bestat.statbel.fgov.be`) and capturing that
view's own ID from the resulting URL — precisely what the maintainer's own note described for
"customised views." This is a concrete, bounded, one-time manual step per dataset (the same shape
as Block C's manual Statbel geography download), not a blocker, but it means the `maintenance_cost`
term in the scoring formula above should account for it: someone has to actually build and record
each needed view before `StatbelSource` can fetch from it.

Not checked this session: whether Bestat's API has its own licence/terms page distinct from
Statbel's general CC BY 4.0 statement (no licence field appears on a view or datasource's own
JSON metadata) — worth a direct check before relying on it for the 10 selected sources.

Not investigated this session, and not implied to be worse candidates for it — simply outside the
7 categories the research pass covered: **datastore.brussels** (the Brussels-region aggregator
IBSA's own page points to for more datasets) and **opendata.brussels.be** (City of Brussels'
own portal, distinct from the regional one). Worth a follow-up pass if the maintainer wants
Brussels coverage beyond IBSA's two current datasets.
