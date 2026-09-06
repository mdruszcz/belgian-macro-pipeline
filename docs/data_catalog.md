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

## Selected 10 (Block E, 2026-09-06)

Chosen by the maintainer via a multi-round selection interview (facts presented per candidate,
no scores computed by the agent) rather than by filling in numeric `decision_value`/`difficulty`
columns directly — the roadmap's underlying goal (a defensible, human-made priority call) is
met either way. **CONTROL E check: exactly 10.** ✅

| # | Dataset | Full row | Licence status |
|---|---|---|---|
| 1 | Fiscal income by commune | rows 1, 1b | ✅ **Verified 2026-09-06 from Statbel's own licence document** — commercial reuse granted. Not CC BY 4.0; see "Statbel licence" below |
| 2 | Fiscal income by statistical sector | row 2 | ✅ **Verified** — same Statbel licence, see below |
| 3 | Population by commune | rows 3, 3b | ✅ **Verified** — same Statbel licence, see below |
| 4 | Business/enterprise units by commune | rows 7, 7b | ✅ **Verified** — same Statbel licence, see below. This is the one dataset already ingested (`StatbelSource`, PR #39) |
| 5 | WalStat (IWEPS) | rows 8, 9 | **Verified from IWEPS' own FAQ, maintainer-supplied 2026-09-06** — CC0 (data) / CC BY-SA (maps). See below for the exact text and a liability nuance worth knowing before this ships. |
| 6 | IBSA — municipalities in urban regions | row 10 | **Verified from IBSA's own licence statement, maintainer-supplied 2026-09-06** — CC BY 4.0, attribution required. See below for the exact text. |
| 7 | IBSA — Brussels demographic projections | row 11 | **Verified from IBSA's own licence statement, maintainer-supplied 2026-09-06** — CC BY 4.0, attribution required. Same source as row 6. |
| 8 | Gemeente-Stadsmonitor | row 14 | **Not verified.** Site reachable 2026-09-06 but its FAQ page yielded no licence text. Flemish default is *Modellicentie Gratis Hergebruik v1.0* — a default, not this platform's stated terms |
| 9 | ODWB (Wallonie-Bruxelles) | row 15 | ⚠️ **UNRESOLVED — category error.** This is a portal, not a dataset: 1289 datasets under 22 different licences, incl. non-commercial ones. Cannot be cleared as a unit. See "Portals cannot be licence-cleared" below |
| 10 | opendata.brussels.be | row 17 (new) | ⚠️ **UNRESOLVED — category error.** Same: a portal, 208 datasets under 7 licences, incl. NC and ND. See below |

**`[H] Verify commercial-reuse permission for each of the 10` is NOT done — 7 of 10 confirmed.**
All 7 are confirmed from the publisher's own stated licence text, maintainer-supplied, not from a
search snippet or a page someone skimmed: WalStat's FAQ, IBSA's licence statement (both quoted in
full below), and Statbel's own licence PDF, now committed at
[`docs/licences/statbel_open_data_licence_2015-10-22.pdf`](licences/statbel_open_data_licence_2015-10-22.pdf)
and quoted below. **The maintainer's rule stands: do not build adapters for data you cannot
resell.** The remaining 3 are Gemeente-Stadsmonitor (genuinely unverified) and the two portal
rows, which are a different problem entirely — see below.

**Deferred, not deleted:** rows 4 (real estate sales), 5 (cadastral building stock), 6 (building
permits), 12–13 (ABB municipal finance — both the interactive tool and the PDF report), and 16
(data.gov.be). `datastore.brussels` (Brussels-region aggregator, see below) was never formally
offered as a candidate and stays unresolved rather than silently deferred.

## Census indicators — 2021 wanted, 2011 available (approved 2026-09-06, revised same day)

**Status: approved in principle, blocked on a hand-download. Do not build the 2011-only adapter.**

### What happened

The Bestat API turned out to hold 38 commune-level indicators that no view name advertises — the
views are called *"Communes dont le taux de chômage des 15-64 ans est le plus élevé"*, which reads
like a top-ten extract but returns **all 589 communes**. Measured: 590 rows, 589 naming a commune,
values from 1.76% to 24.06% (Saint-Josse-ten-Noode), median 6.0%.

That was approved as an eleventh dataset, and then the maintainer asked the right question: *nothing
more recent than 2011?* Checking rather than defending it changed the decision.

### What the API actually has, measured

Every municipal-named view in all 1,341 was probed. Only **three** datasources return commune-level
rows at all:

| Datasource | Communes | Periods |
|---|---|---|
| `IM_SOC_GEO_IND_CENSUS` | 589 | **2011 only** |
| `IM_EAF_LCL_UNIT_POP` | 565 | 2023-Q4 (already loaded) |
| `IM_EAF_PROP_TRANS_PRCL_EXT` (building-land prices) | 589 | 1992, 2000, 2005, **stops 2014** |

Everything Statbel updated recently — June 2026 property prices, VAT turnover, population by marital
status — is national or regional. The house and apartment price views whose names promise *"par
commune"* return 48 region rows.

**`IM_SOC_GEO_IND_CENSUS_2021` and `IM_SOC_GEO_NUC_CENSUS_2021` do exist**, updated 2024-04 and
2025-02, carrying *both* 2011 and 2021. All 174 of their views were probed across four locales:
every one stops at **province or arrondissement**. None reaches commune.

### Census 2021 at commune level exists, off-API

Statbel published **~140 Census 2021 open datasets** under CC BY 4.0 (commercial reuse permitted,
same licence family already cleared here). Confirmed commune-level titles include:

- *Census 2021 — Population selon : Lieu de résidence (Commune), sexe et pays de citoyenneté*
- *Census 2021 — Locaux d'habitation selon : Lieu de résidence (Commune) et Type de local d'habitation*

Index: `statbel.fgov.be/fr/open-data/consultez-tous-les-open-data-du-census-2021`

They are on `statbel.fgov.be`, which automation cannot read. Note a likely refinement to
[manual_sources.md](features/manual_sources.md)'s account: that document calls it "a
connection-level block", and `curl` does fail outright — but search engines report the same URLs
serving **CAPTCHA verification pages**, which points at edge bot-protection rather than a network
block. `data.gov.be`, which mirrors Statbel, is equally unreachable from here. Either way the
consequence is unchanged: **a human with a browser can download these; CI cannot.**

### The decision

Census 2021 is worth having and 2011 alone is not. Publishing 38 fifteen-year-old indicators to
clear a 50-indicator gate would make the product worse, not better — a directeur financier reads the
vintage first. The 2011 API set stays **approved but unbuilt**, as the fallback if the 2021 download
never happens, and only ever alongside its 2021 counterpart for the ten-year change.

**Owed by the maintainer:** the Census 2021 commune-level files, hand-downloaded, exactly as
population (`TF_SOC_POP_STRUCT`) and fiscal income (`TF_PSNL_INC_TAX_MUNTY`) already were. Those two
are the pipeline's only current municipal series, and they are current *because* they were fetched
by hand.

### Also established

Housing prices are **not** obtainable at commune level from the API, so the `/local` housing headline
stays unavailable and real estate sales stays DEFERRED from Block E.

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

### Licence — CC BY 4.0 (see also the 2015 open-data licence below — two documents, both from Statbel)

> ⚠️ **Two Statbel licence documents exist and they do not say the same thing.** This section
> records the *Conditions générales d'utilisation* (CC BY 4.0), confirmed by the maintainer
> 2026-09-05. A second document — the *Licentie open data* of 22 October 2015, committed at
> [`licences/statbel_open_data_licence_2015-10-22.pdf`](licences/statbel_open_data_licence_2015-10-22.pdf)
> — is a bespoke federal licence that never mentions CC BY 4.0 and carries a **different
> attribution obligation**. See "Statbel licence — the second document" below. **Both grant
> commercial reuse**, so the commercial question is settled either way; the difference is in what
> we must print. Until Statbel is asked which supersedes, satisfy the **union** of both
> obligation sets — that is compliant under either reading.

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

**Done 2026-09-06.** Points 1–5 are obligations on *published output*, and they became live the
moment commune-level data was published — which had already happened: `communes.html` shipped
before this was noticed, and `local.html` followed in Block K, both publishing Statbel-derived
municipal figures with no attribution at all. **The repo was in breach**, briefly, on the reading
that clause 6 terminates the grant automatically.

Both pages now carry an `.attribution` block with the source credit, the licence link, an
explicit "changes were made" notice naming what was changed, the date of last update, the
no-endorsement disclaimer, and the unofficial-English-names statement. `tests/test_statbel_attribution.py`
asserts each obligation against the published HTML, page by page — verified to fail (7 of 7 for
that page) when the block is removed, so it is a real guard rather than a note. A new page that
starts rendering commune data must be added to that test's `MUNICIPAL_PAGES` list.

Point 6 (third-party data) is unaffected: the Eurostat NUTS codes in `geographies.csv` are still
captured but not published.

### Statbel licence — the second document (2015), maintainer-supplied 2026-09-06

Verified from the publisher's own document, committed at
[`licences/statbel_open_data_licence_2015-10-22.pdf`](licences/statbel_open_data_licence_2015-10-22.pdf):
*"Licentie open data — Algemene Directie Statistiek - Statistics Belgium"*, 22 October 2015.
This is the licence that clears **rows 1–4 of the Selected 10** (all four Statbel datasets).

**Commercial reuse is granted in as many words** — it is listed as one of the freedoms, not
merely tolerated:

> de "informatie" commercieel benutten, bijvoorbeeld door ze met andere "gegevens" te combineren,
> of door ze in je eigen product of applicatie te gebruiken.

*(use the information commercially, for example by combining it with other data, or by using it
in your own product or application.)*

The grant is **personal, non-exclusive, free of charge, worldwide, and of unlimited duration**,
and expressly covers reproducing, publishing, redistributing, adapting, extracting, transforming
and **deriving data** (`gegevens af te leiden`). The reuser **retains all intellectual property
rights in the product they build** on it — directly relevant to selling analysis built on this.

#### Where it differs from CC BY 4.0

1. **It never claims to be CC BY 4.0.** It declares itself designed to be compatible with any
   licence requiring attribution, naming **OGL (UK)**, **CC-BY 2.0**, and **ODC-BY**.
2. **Attribution must carry the date of last update** — `de bron (ten minste de naam van de
   "producent") en de datum van de laatste bijwerking`. CC BY 4.0 requires no such date. A
   hyperlink to the information may satisfy this, provided it genuinely evidences authorship.
3. **No "changes were made" notice is required** by this document — that obligation comes from
   the CC BY 4.0 text, not this one.
4. **No implied endorsement** — the attribution must not lend the reuse any official character,
   nor imply recognition by the producer or any other public body. (Same as CC BY 4.0 §5.2.)
5. **Must not mislead** third parties as to the content of the information, its source, or its
   update date. This binds Block F's open `[REVIEW] label honesty` step: a mislabelled indicator
   is a licence breach here, not only a product-quality problem.
6. **No warranty** — no guarantee the information is free of defects or continuously available,
   and no producer liability for loss or damage to third parties from the reuse.
7. **Terminates automatically on non-compliance**, and is governed by Belgian law, with recourse
   to the *Commissie voor de toegang tot en het hergebruik van bestuursdocumenten*.

#### What this obliges us to build

Point 2 is the one with teeth. **Every published Statbel-derived figure must show the date that
figure was last updated** — and the licence ends automatically if it does not. That converts
Block X's *"Freshness badge on every metric — source name, reference year and retrieval date"*
from a trust-building nicety into a **licence condition**. The `observations` table already
stores `vintage` and `created_at` per row, so the data needed is present.

**Partly done 2026-09-06.** The date now reaches the published output, per figure, on both
municipal pages:

- `export_site_payloads.py` carries the source retrieval date per indicator into every payload as
  `updated` (the *latest* date seen for that indicator, not the last row read — asserted by test).
- `local.html` prints it on every headline card and every fact tile (`as of 2026 · updated
  2026-09-05`), and in the attribution block as the newest date across the commune's data.
- `communes.html` prints it in the attribution block and in its "Last fetched" figure.

**A derived indicator deliberately shows no date and says `derived` instead.** It was computed,
not fetched, and has no retrieval date of its own; borrowing its inputs' date would mislead a
reader about the update date, which point 5 of this same licence forbids as squarely as point 2
requires showing one. Today that affects `AVG_NET_TAXABLE_INCOME`, `DEPENDENCY_RATIO`,
`POPULATION_CHANGE_5Y`, `POPULATION_CAGR_10Y` and `POPULATION_PERCENTILE`.

**Still open:** whether "derived" is a sufficient disclosure for a derived figure under point 2,
or whether such a figure must also expose the retrieval dates of its inputs. Block X's freshness
badge is where that would land. `all_data.html` and `dashboard.html` are unaffected — national
sources only, no Statbel data.

#### Open question for the maintainer

Which document governs — the 2015 licence, or the CC BY 4.0 in the *Conditions générales
d'utilisation*? Statbel may have moved to CC BY 4.0 since 2015 without withdrawing the PDF, or
may apply the PDF to bulk open-data files and CC BY 4.0 to the website. Worth one email to
`statbel.opendata@economie.fgov.be` (the address on the PDF) before anything is sold. Until then
we satisfy both.

### Portals cannot be licence-cleared — rows 9 and 10 are a category error

Established 2026-09-06 by querying both portals' own catalogue APIs directly (both reachable
from CI; `statbel.fgov.be` and `data.gov.be` are not).

**ODWB (`www.odwb.be`) — 1289 datasets, 22 distinct licences:**

| Count | Licence |
|---|---|
| 653 | CC BY |
| 270 | Creative Commons — CC0 |
| 102 | CC-BY |
| 93 | Open licentie CC0 — Universeel |
| **19** | **CC BY-NC 4.0** — non-commercial, unusable for us |
| 17 + 1 | *Licence non spécifiée* / *License Not Specified* |
| 8 | Belgian NGI — ngi-standard-open |
| 6 + 1 + 1 + 1 | Service public de Wallonie — custom terms (4 variants) |
| 5 | CC-by 4.0 |
| 3 | Licence Personnalisée (see reference link) |
| **3** | **other-closed** |
| 2 | CC 0 · CC BY-SA · CC-0 · Open Government Licence v3.0 · other-open |
| 1 | CC BY-SA 4.0 · **CC-BY-NC-ND** |

**opendata.brussels.be — 208 datasets, 7 licences:** CC0 1.0 (107), CC BY 4.0 (85),
"Statbel Open Data" (11), **CC BY-NC 4.0 (3)**, **CC BY-ND 4.0 (1)**, Custom (1).

Neither portal has a portal-wide licence, and **both contain non-commercial and no-derivatives
datasets**. A row that names the portal can therefore never be marked "commercial reuse
permitted" — the answer depends entirely on which dataset is pulled. These two rows were selected
in Block E as if they were datasets; they are aggregators.

Marked **UNRESOLVED** rather than re-scoped, deferred, or quietly deleted: Block E selection is
the maintainer's call (`CLAUDE.md` rule 8), and the options — narrow each row to named datasets,
replace them, or drop to 8 and break CONTROL E's "10 items, not 11" — are commercial judgments,
not technical ones. **No adapter may be built against either until this resolves.**

Side finding worth keeping: `opendata.brussels.be` labels 11 of its datasets `"Statbel Open
Data"`, independently corroborating that Statbel's open-data licence is a distinct named licence
rather than a plain CC BY 4.0 grant.

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
| 1 | [Fiscal statistics on income](https://statbel.fgov.be/en/open-data/fiscal-statistics-income) | Statbel | Commune (confirmed as a real dimension in the Bestat datasource — see below — but every standard/pre-built view found stops at province level) | 2005–2023 (open-data page); 2017–2019 confirmed directly via Bestat standard view (`850933c9`, province-level) | Annual | CC BY 4.0 | XLSX (open-data page); JSON via Bestat API (see below) | Corroborated + Bestat datasource verified directly | SELECTED |
| 1b | Same dataset via Bestat API — datasource `IM_SOC_PSNL_INC_TAX_MUNTY` | Statbel (Bestat) | **Commune is a real dimension of this datasource** (name: "par commune de résidence") but no standard view exposing it was found — see "Statbel Bestat API" note below | Datasource "last data update" timestamp confirms it is live-maintained (Nov 2026) | Unconfirmed at commune level; region-level standard view covers 2017–2019 only | Not stated per-datasource; presumed CC BY 4.0 (Statbel's general policy) | JSON, CSV, XML, XLS, HTML, PDF via `https://bestat.statbel.fgov.be/bestat/api/views/{id}/result/{FORMAT}` | Verified directly (datasource id `b394aa82-5045-4483-9e79-cd5344651791`, fetched and read) | SELECTED |
| 2 | [Fiscal statistics on income by statistical sector](https://statbel.fgov.be/en/open-data/fiscal-statistics-income-statistical-sector) | Statbel | Statistical sector (sub-commune) | 2005–2023 | Annual | CC BY 4.0 | XLSX | Snippet-only | SELECTED |
| 3 | [Population by place of residence, nationality, marital status, age and sex](https://statbel.fgov.be/en/open-data/population-place-residence-nationality-marital-status-age-and-sex-12) | Statbel | Commune (aggregable to arrondissement/province/region) | Annual snapshot (1 Jan); exact earliest year not confirmed | Annual | CC BY 4.0 (site default, not independently confirmed on this page) | CSV | Snippet-only | SELECTED |
| 3b | Census 2011+2021 population/household/housing indicators via Bestat — datasource `IM_SOC_GEO_IND_CENSUS_2021` | Statbel (Bestat) | Standard views confirmed at **province level only** (e.g. "Total population" view returns 28 rows: region × province × 2 census years) despite dozens of commune-sounding indicator names | 2011 and 2021 census years, confirmed directly | Static (census-based, not continuously updated) | Unconfirmed; presumed CC BY 4.0 | JSON etc. (Bestat API) | Verified directly (datasource id `e957ac31-44a2-4718-8469-10470d3c41d9`, one standard view fetched and read) | SELECTED |
| 4 | [Sales of real estate according to nature of property](https://statbel.fgov.be/en/open-data/sales-real-estate-belgium-according-nature-property-land-register) | Statbel | **Unconfirmed at commune level** — Statbel's public bulletins for this series are region/province aggregates; whether the open-data file itself goes to commune level was not verified | Unconfirmed | Quarterly (per Statbel's general release cadence) | Presumed CC BY 4.0 | Unconfirmed | Snippet-only | DEFERRED |
| 5 | [Cadastral statistics of the building stock](https://statbel.fgov.be/en/open-data/cadastral-statistics-building-stock) | Statbel | Commune (stated: buildings in Belgium as of 1 Jan of the reference year) | At least one reference year confirmed to exist (2024) | Annual | Unconfirmed on page | Unconfirmed (likely CSV) | Snippet-only | DEFERRED |
| 6 | Building permits statistics | Statbel | Region/province confirmed via press releases; commune-level open-data availability **not verified** | Monthly figures referenced in recent press bulletins | Monthly | Unconfirmed | Unconfirmed | Snippet-only, no confirmed open-data page found (only a thematic page) | DEFERRED |
| 7 | Statistics on establishment units (business/enterprise) | Statbel | Commune, but counts are **banded/masked for confidentiality**, not exact — worth weighing against `comparability` in the formula above | Unconfirmed | Annual (VAT-registered units) | Unconfirmed | Unconfirmed | Snippet-only | SELECTED |
| 7b | Local units (établissements) by commune via Bestat — datasource `IM_EAF_LCL_UNIT_POP` | Statbel (Bestat) | **True commune-level confirmed by directly fetching the data**: 566 rows, one per commune, exact (not banded) counts, e.g. Aartselaar 2232, Antwerp 66,381 | Datasource covers "since 2015 per quarter" but the commune-level standard view returns only the latest quarter (Q4 2023); a separate region-level standard view (`e21e18c6`) has full 2015–present quarterly depth. **Getting both commune granularity and full depth needs a custom cross-tab, not a standard view** — see note below | Quarterly (per datasource description) | Unconfirmed; presumed CC BY 4.0 | JSON (confirmed), + CSV/XML/XLS/HTML/PDF per the API structure | **Verified directly** — fetched real data, real commune names matching this repo's own `geographies.csv`, real counts | SELECTED |
| 8 | [WalStat portal](https://walstat.iweps.be/walstat-accueil.php) — 19 themes incl. "Pouvoirs locaux" (local governance) | IWEPS (Wallonia) | Quartier / commune / arrondissement / province / bassin | Not stated on the pages opened; needs a catalogue-level query | Not stated | **CC0 for the data, CC BY-SA for maps** — confirmed from IWEPS' own FAQ text, maintainer-supplied 2026-09-06 (see the "WalStat licence" note below) | CSV, JSON | **Verified from the primary source** — this is IWEPS' own stated policy, not a search corroboration | SELECTED |
| 9 | [WalStat open-data catalogue (DCAT-AP)](https://opendata.iweps.be/statdcat-ap/walstat) | IWEPS (Wallonia) | Same as above | Not stated; catalogue updated twice yearly (end of June, end of December) per iweps.be | Semi-annual catalogue refresh | CC0 (data) | RDF/XML catalogue → CSV/JSON | Verified (catalogue page opened directly) | SELECTED |
| 10 | [IBSA — List of Belgian Municipalities in Urban Regions](https://ibsa.brussels/opendata) | IBSA (Brussels) | Belgium-wide | 2021–2025 | Last updated 25 June 2026 (per page) | CC BY 4.0 | XLSX, CSV (+ codebook) | **Verified from IBSA's own licence statement**, maintainer-supplied 2026-09-06 (see "IBSA licence" note below) | SELECTED |
| 11 | [IBSA — Brussels Municipal Demographic Projections](https://ibsa.brussels/opendata) | IBSA (Brussels) | Brussels-Capital Region, municipal | Projections 2026–2035 | Last updated 26 March 2026 (per page) | CC BY 4.0 | XLSX, CSV (+ codebook) | **Verified from IBSA's own licence statement**, maintainer-supplied 2026-09-06 (same source as row 10) | SELECTED |
| 12 | ABB / "Financieel profiel van het lokaal bestuur" (BBC financial reporting) | ABB / Flemish government (`vlaanderen.be/lokaal-bestuur`) | Municipality + OCMW + the 10 Antwerp districts | Not confirmed — the actual dataset download page was not reached (redirects led to a general landing page) | Quarterly submissions feed the underlying BBC system, per its own description | Unconfirmed | Interactive tool; underlying data format unconfirmed | Snippet-only, page not reached | DEFERRED |
| 13 | ["De financiële toestand van de Vlaamse gemeenten"](https://publicaties.vlaanderen.be/view-file/78642) (annual analysis of Flemish municipal accounts) | ABB / Flemish government | Municipality | 2024 annual accounts (latest edition found) | Annual | Unconfirmed | **PDF report, not structured data** — a real cost against `maintenance_cost` in the formula above | Snippet-only | DEFERRED |
| 14 | ["Jouw gemeente in cijfers" / Gemeente-Stadsmonitor](https://gemeentemonitor.vlaanderen.be/) | Statistiek Vlaanderen | Municipality (~200 indicators, ~70 from a resident survey per search snippets) | Unconfirmed | Unconfirmed | Unconfirmed | Page did not return usable content this session; needs a direct visit | Snippet-only | SELECTED |
| 15 | [ODWB — Open Data Wallonie-Bruxelles](https://www.odwb.be/pages/home/) | Agence du Numérique (Walloon Region + French Community) | Confirmed to include a "Données locales" / commune-level section, exact datasets not enumerated | Unconfirmed | Unconfirmed | Not stated on the homepage; needs a dataset-level check | Unconfirmed | Verified portal exists and structure (homepage opened directly); individual dataset details not checked | SELECTED |
| 16 | [data.gov.be](https://data.gov.be/en/documentation/licenses) (federal aggregator, ~10,000 datasets across 14 categories per search snippets) | Federal Belgian government | Aggregates federal + some regional/local; explicitly *not* a one-stop shop — its own docs point out to regional portals | N/A (aggregator) | N/A | Default **CC0**, "comply or explain" — a department may instead choose CC BY 4.0 / CC BY-SA 4.0 / CC BY-NC 4.0 / CC BY-ND 4.0 (per search snippet only, could not open the licence page directly to confirm) | Varies by dataset | Snippet-only, page unreachable | DEFERRED |
| 17 | [opendata.brussels.be](https://opendata.brussels.be/) — a portal, not yet a chosen dataset within it | City of Brussels | 208 datasets, spans whatever geography levels its individual datasets use — not enumerated | Unconfirmed at dataset level | Unconfirmed at dataset level | Not stated at portal level; per-dataset | Standard OpenDataSoft Explore API v2.1, JSON/CSV | Verified (portal + working search API confirmed directly) | SELECTED — **a specific dataset within it still needs to be picked** before this can feed an adapter |

### IBSA licence — confirmed from IBSA's own statement

The maintainer supplied IBSA's own licence text directly. Quoted in full:

> *"Les Open Data publiées par l'IBSA sont soumises à la licence Creative Commons Attribution 4.0
> (CC BY 4.0). Les données peuvent être utilisées gratuitement moyennant mention de la source."*

Straightforward and matches what was already found by opening `ibsa.brussels/opendata` directly:
**CC BY 4.0, free use with attribution required** — no share-alike clause, no non-commercial
restriction, no ambiguity between a data licence and a separate map licence (unlike WalStat,
below). Applies to both selected IBSA datasets (rows 10 and 11), since they're published under
the same portal-wide policy. The only obligation is attribution — "mention de la source" — which
should follow the same pattern already established for `statbel_geography`'s CC BY 4.0 terms
above: credit IBSA, and if the data is reshaped before publishing (as it will be, going through
the canonical schema), say so.

### WalStat licence — confirmed from IWEPS' own FAQ

The maintainer supplied the exact FAQ text (`walstat.iweps.be`), confirming what two independent
searches had corroborated but neither had actually read from the primary source. Quoted directly:

> *"WalStat s'inscrit dans la mouvance de l'Open Data. [...] Nous avons décidé d'utiliser 2 types
> de licences des Creative Commons : Pour les cartes [...] nous appliquons le CC BY SA [...].
> Pour les données, nous appliquons le CC0 : la personne qui ré-utilise les données est libre et
> responsable de ce qu'elle en fait (croisements, interprétations, représentations…). L'IWEPS ne
> peut, en aucun cas être tenu responsable d'une mauvaise utilisation secondaire des données
> publiées."*

Two things worth acting on:

1. **The data itself is CC0** — no attribution requirement, no share-alike obligation, unrestricted
   commercial reuse. This is the most permissive licence of any of the 10 selected datasets.
2. **Maps are CC BY-SA** — attribution to IWEPS required, and anything built from their maps must
   be shared under the same licence. If BelPulse ever reuses a WalStat *map* (not just the
   underlying data) in a paid product, that output inherits a share-alike obligation the CC0 data
   does not carry — worth keeping distinct in whatever tracks licence provenance per output.
3. **IWEPS explicitly disclaims responsibility for secondary use** — "en aucun cas être tenu
   responsable d'une mauvaise utilisation secondaire." This is not a licence restriction (CC0
   already carries no warranty), but it is IWEPS stating plainly that any interpretation, ranking,
   or benchmark BelPulse derives from this data is BelPulse's own responsibility, not something
   IWEPS backs. Worth carrying into whatever "what this score is not" disclaimer language Block P
   (Financial health scoring) eventually needs, since WalStat is a plausible ingredient there.

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

**Confirmed (follow-up pass): the `/result/{FORMAT}` export does not accept query parameters to
reshape a standard view's dimensions** — appending `?dimension=Commune` to the region-level local
units view returned the identical region-level shape, unchanged. This closes the question left
open above: there is no API shortcut around building a custom cross-tabulation through the
Bestat web UI when a standard view doesn't already have the geography/depth combination needed.

### Brussels aggregator portals (follow-up pass)

Previously flagged as not investigated; both are now confirmed real and queryable, closing that
gap:

- **[opendata.brussels.be](https://opendata.brussels.be/)** (City of Brussels) — a real,
  standard **OpenDataSoft** portal, confirmed via its Explore API v2.1
  (`/api/explore/v2.1/catalog/datasets`): **208 datasets**, searchable
  (`?q=commune` returns real results). Same API family as many other Belgian open-data portals,
  so the same fetch pattern could serve multiple sources if this is selected. Licence and
  update frequency are per-dataset, not checked at the catalogue level this pass. **Selected as
  one of the 10** (row 17 above) — a specific dataset within it still needs to be picked.
- **[datastore.brussels](https://datastore.brussels/)** (Brussels-Capital Region aggregator,
  the one IBSA's own page points to) — confirmed to be a **single-page application**; every path
  tried under `/web/...` returns the same client-rendered HTML shell rather than JSON, including
  a guessed CKAN-style `/web/api/action/package_search` endpoint. The real data API exists
  somewhere behind this frontend but its path was not found this pass — needs either browser
  dev-tools inspection of a real page load, or the maintainer already knowing the endpoint. **Not
  selected** — it was never formally offered as a candidate in the selection round, so it stays
  unresolved rather than counted as either chosen or deferred.

Both are now "verified to exist and be worth a real look," upgraded from "not investigated" —
neither has been evaluated as a candidate dataset itself yet, since that requires picking a
specific dataset within each, which is exactly the enumeration Block E's `[H]` scoring step
still needs from the maintainer.
