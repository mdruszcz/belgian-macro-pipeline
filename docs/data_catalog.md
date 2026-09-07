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

They are on `statbel.fgov.be`, which automation cannot read.

**Correction, 2026-09-06.** This section previously speculated the block was "likely bot-protection"
rather than a network block, reasoning from search engines reporting CAPTCHA pages at the same URLs.
Re-checked directly with `curl -v`: DNS resolves cleanly on both address families, then the **TCP
handshake itself times out** — not an HTTP-level CAPTCHA response, a connection that never
completes. `manual_sources.md`'s original "connection-level block" was correct; the CAPTCHA a search
engine sees is a separate defence Statbel runs for crawlers, encountered only once a connection
succeeds, which this pipeline's network context never reaches. `data.gov.be`, which mirrors Statbel,
is equally unreachable. The consequence was never in question either way: **a human with a browser
can download these; CI cannot.**

**Statbel's own file-naming convention**, maintainer-supplied: pre-built table exports follow
`.../Census2021/T01_CAS_AGE_COM_FR.XLSX` — table number, theme (`CAS`=civil status, `EDU`=education,
`ACT`=activity/labour, `MIG`=migration, …), geography (`BE`/`REG`/`PROV`/`ARR`/`COM`/`SEC`), language.
A commune-level French file is `*_COM_FR.XLSX`. The ten files already loaded below are a *different*
export family — Statbel's bulk "hypercube" dumps, `TF_CENSUS_2021_HCnn_m.xlsx` — not this
per-table naming. Both are current Census 2021 data; see
[manual_sources.md](features/manual_sources.md) for the full breakdown.

### LOADED 2026-09-06 — unemployment, found inside a file already downloaded

The `ACT` file this catalogue said was owed turned out to already be sitting in
`data/raw/statbel/census2021/`. The maintainer's file-naming note called `CAS` "civil status",
guessing from the abbreviation; checking the actual file (`T01_CAS_AGE_BE_NL.XLSX`, one of the ten
already downloaded) shows CAS means *arbeidsmarktsituatie* — labour-market situation — and its
15-64 sheet is exactly the `ACT`-equivalent table thought to be missing: labour force, employed,
unemployed, inactive, all at commune level. Despite the "BE" in the filename (Statbel's convention
for "national report", not "national-only geography"), the sheet drills to 583 NIS-6 codes.

Verified against Belgium's own 2021 total before trusting any commune row: 462,991 unemployed of
5,376,113 in the labour force = **8.61%**, the right order of magnitude for a pandemic-affected
year. `CAS_LABOUR_FORCE` / `CAS_EMPLOYED` / `CAS_UNEMPLOYED` / `CAS_INACTIVE` loaded as raw counts;
`UNEMPLOYMENT_RATE_COM` derived (named `_COM` because `UNEMPLOYMENT_RATE` already exists as a
*national* NBB indicator with a different definition and cadence — a real naming collision, caught
before it happened).

**A real bug found while loading it:** the sheet reports every geography level in one table —
country, region, province, arrondissement *and* commune — and Flemish/Walloon Brabant's split
province codes (`20001`, `20002`) do not end in `000`, so a first attempt at a digit-pattern filter
missed them. 57 non-commune rows landed silently under a commune-only indicator on the first run,
caught by inspecting the loaded row count (638, not the expected ~581) rather than by a crash. Fixed
by filtering against the geographies table's actual commune codes, not a pattern.

**Still owed:** `EDU` (education), not among the files downloaded.

### LOADED 2026-09-06 — house prices, found in the same download

`immo_by_municipality_2010-2019.xlsx`, downloaded alongside the census workbooks, holds real
commune-level house-sale transactions and prices, 2010–2017, four property types. This directly
supersedes the earlier "not available at commune level from the source" finding — that finding was
about the Bestat *API*; this is a separate hand-supplied bulk file the API probe never saw.

Scoped to ordinary houses only (`gewone woonhuizen`) for now — best commune coverage of the four
types (588–589 of 589 per year, against 527–546 for apartments). `HOUSE_SALES_TRANSACTIONS` and
`HOUSE_SALES_TOTAL_PRICE` stored as additive totals; `AVG_HOUSE_PRICE` derived
(`total_price / transactions`), never Statbel's own per-row mean averaged across communes — the
exact mistake [ADR 0003](decisions/0003-aggregation-rule.md) exists to prevent.

National average verified against the file's own totals: **€181,040 (2010) → €218,722 (2017)**,
matching published Belgian house-price trends for that period. Apartments, villas and building land
are real data in the same file, correctly available, and not loaded by this first pass.

### UPDATED 2026-09-06 — richer quarterly file replaces the 2010–2019 one

The maintainer supplied `FR_immo_statbel_trimestre_par_commune.xlsx`, a much larger Statbel
export: **quarterly, 2010–2026 so far** (against the old file's 2010–2019 annual), covering every
house type except apartments in one column block (closed, semi-closed and open/detached houses
combined), plus a **median** price and its quartiles. `scripts/sync_realestate.py` rewritten to
read it; `immo_by_municipality_2010-2019.xlsx` is no longer read by anything.

**The trade this makes, plainly.** The old file gave a `MS_TOTAL_PRICE`, additive, from which a
true mean was recomputed downstream (`AVG_HOUSE_PRICE`, `mean_from_total`, CLAUDE.md rule 6). This
file gives only a **median** — which cannot be reconstructed from parts, the same fact
`fiscal_income.md` already records for a different Statbel file. So `AVG_HOUSE_PRICE` and
`HOUSE_SALES_TOTAL_PRICE` are **retired**, and `MEDIAN_HOUSE_PRICE` (quarterly, stored directly,
`is_additive = 0`, `aggregation_method = 'not_applicable'`) replaces `AVG_HOUSE_PRICE` as the
`/local` housing headline. Wider coverage and far more current, at the cost of the province/region
comparison a true mean allowed and this pipeline no longer has the components to recompute.

**`HOUSE_SALES_TRANSACTIONS`'s scope changed too, measured not assumed.** The old indicator counted
`gewone woonhuizen` (ordinary houses: closed + semi-closed only). The new file's category —
"toutes les maisons ... excl. appartements" — additionally includes open/detached houses. Aartselaar
2017: old file 108, new file's closed+semi-closed subset alone 113 (a plausible late-registration
revision, not investigated further), new file's full "all houses" scope 146. The ~35% step in this
indicator's own history at the file-refresh boundary is that scope change, not a market move —
recorded in the indicator's own description so a reader does not mistake one for the other.

**Same fixed-geography pattern as ONEM and police.be, proven the same way.** All 565 `refnis` codes
are identical across 2010 and 2024 alike; Kruisem (NIS 45068, created by the 2019 merger) carries a
real value in the file's own 2010 rows. Resolved against a single pinned period, `"2025"` —
deliberately the OPPOSITE choice from police.be's `"2024"` pin, because this file's 565-code set
matches the map *after* the 2025 mergers, not before it.

**The 2026 part-year problem, handled the same way ONEM's was.** 2026 has only a Q1 row so far.
Summing it and calling the result "2026" would understate the true annual total by roughly
three-quarters — `HOUSE_SALES_TRANSACTIONS` therefore only loads a year once all four of its
quarters are present, so 2026 is simply absent until Q2–Q4 arrive rather than published wrong.
`MEDIAN_HOUSE_PRICE` has no such problem — a quarter's own median is complete on its own terms —
so it loads through 2026-Q1, marked provisional like every source's most-recent period.

**Reference-row correction pattern reused from police.be.** `sync_realestate.py`'s
`_ensure_reference_rows` now uses `ON CONFLICT ... DO UPDATE`, not `INSERT OR IGNORE`, for the same
reason: a config correction after the first load must actually reach an already-loaded database.

### LOADED 2026-09-06 — Census 2021, hand-downloaded

The maintainer downloaded ten Census 2021 workbooks into `data/raw/statbel/census2021/`
(gitignored). `scripts/sync_census2021.py` loads them into `data/census2021_observations.csv`:
**13 counts × 581 communes, 7,552 observations at period 2021**, plus six derived shares.

| Stored counts | Derived shares |
|---|---|
| `POP_FOREIGN_NATIONALS`, `POP_NON_EU_NATIONALS`, `POP_BORN_ABROAD`, `POP_FEMALE`, `POP_MARRIED` | `SHARE_FOREIGN_NATIONALS`, `SHARE_BORN_ABROAD` |
| `HOUSEHOLDS_PRIVATE`, `HOUSEHOLDS_SINGLE_PERSON` | `SHARE_SINGLE_PERSON_HOUSEHOLDS`, `AVERAGE_HOUSEHOLD_SIZE` |
| `FAMILY_NUCLEI`, `FAMILY_NUCLEI_SINGLE_PARENT` | `SHARE_SINGLE_PARENT_FAMILIES` |
| `DWELLINGS_TOTAL`, `DWELLINGS_OCCUPIED`, `DWELLINGS_VACANT`, `DWELLINGS_IN_SINGLE_UNIT_BUILDING` | `SHARE_DWELLINGS_UNOCCUPIED` |

**Indicators per commune went from 14 to 32.** Belgium-level results, recomputed from summed
components: average household size **2.29**, one-person households **35.39%**, foreign nationals
**12.35%**, born abroad **17.68%**, single-parent families **16.05%**.

**The check that anchors it.** Census 2021's population summed per commune equals the pipeline's
existing `TF_SOC_POP_STRUCT` series for 2021 **exactly — 11,521,238 people, all 581 communes
matching commune by commune**. Two unrelated Statbel products agreeing to the person is what fixes
the reference date (1 January 2021), validates the NIS mapping, and independently corroborates the
population series already published. Asserted in `tests/test_census2021.py`, not just described.

**Two label decisions worth recording**, both caught by checking a figure that looked wrong:

- The first version published a **"dwelling vacancy rate" of 24–29%**. The counts were right, the
  framing was not. The census calls a dwelling unoccupied if nobody was registered there on 1
  January, which sweeps in second homes, renovations and dwellings between tenants — 14.45%
  nationally, against low single digits in Flemish administrative vacancy registers. Renamed
  `SHARE_DWELLINGS_UNOCCUPIED`, denominator corrected from occupied-only to the whole stock, and the
  config states plainly that the two measures are not comparable.
- **Herstappe has no `POP_NON_EU_NATIONALS` row.** These files carry no explicit zeros anywhere
  (verified: zero rows with value 0), so an absent slice is either a true zero or a suppressed small
  count and the file cannot distinguish them. Left absent rather than written as 0 — the
  "suppression looks like zero" failure `data_model.md` warns about.

**Not obtained:** employment, unemployment and education level. Those tables were not among the ten
downloaded, so the `/local` unemployment headline is still unavailable.

### The decision on the 2011 set

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
## ONEM/RVA — commune-level unemployment, LIVE since 2026-09-06 (12th dataset)

Approved by the maintainer 2026-09-06, who supplied both the direct download URLs and the licence
text below. **The pipeline's first and only automated municipal source.** Every other one is a hand
download because `statbel.fgov.be` is unreachable from GitHub's runners; `onem.be` is not, so these
observations go into the daily database rather than a committed CSV — `docs/decisions/0002-split-committed-stores.md`
splits the stores on exactly that question. Loaded by `scripts/sync_onem.py`, wired into
`daily_fetch.yml`.

| Field | Value |
|---|---|
| Publisher | ONEM / RVA (Office National de l'Emploi / Rijksdienst voor Arbeidsvoorziening) — Belgium's federal unemployment office |
| Base URL | `onem.be/sites/default/files/assets/statistiques/113/` |
| Format | Legacy BIFF `.xls` (Composite Document File V2 — hence `xlrd`, which reads it; `openpyxl` cannot open it at all) |
| Geography | Commune, named in FRENCH TEXT with no NIS code — resolved by name, not by code |
| Period coverage | One sheet per year, 2017–2026 |
| Cadence | Refreshed in place, roughly monthly (the files fetched 2026-09-06 were last saved 2026-08-05 by a named ONEM employee) |
| Volume | 37,855 observations: 7 indicators × 565 communes × 10 years, less the current year's euro sheets |

**Licence** (maintainer-supplied, ONEM's own reuse conditions page), quoted in full because it
differs from every other source's terms here — no CC BY 4.0 wrapper, its own attribution
requirement:

> Sans préjudice des droits de propriété intellectuelle de l'ONEM, les informations publiées sur ce
> site internet sont libres de droits. Elles peuvent être réutilisées sans condition à des fins
> privées, associatives, scientifiques et commerciales, dans le respect des droits de propriété
> intellectuelle de l'ONEM. Les personnes qui réutilisent ces informations, en mentionneront la
> source et indiqueront la date des informations utilisées.

**Commercial reuse is explicitly permitted** ("à des fins ... commerciales"). Obligations: credit
the source, and state the date of the information used. Both are now discharged on the published
pages and asserted by `tests/test_statbel_attribution.py`, which carries a separate ONEM section —
separate because ONEM is **not** CC BY 4.0, and the page must say so explicitly rather than let a
reader assume the neighbouring CC BY notice covers it. There is no "changes were made" clause and no
no-endorsement clause in this text, unlike Statbel's; neither is asserted for ONEM.

### What the files actually are

Read off the real files, not inferred. This environment cannot reach `onem.be` at all (DNS resolves,
then the TCP handshake times out — the identical signature `statbel.fgov.be` gives), so the files
were obtained the only way available: run `34054441348` proved a GitHub Actions runner reaches
`onem.be`, and run `34056982618` uploaded all six as a workflow artifact so they could be downloaded
and opened.

Layout, identical in all six files: row 0 the agency, row 1 the dataset, **row 3 the unit**, row 5
the column headers, rows 6+ the data — 618 rows, being 565 communes, 42 `arr.*` subtotals, 10
`prov.*` subtotals and one `Région Bruxelles-Capitale` row. Only commune rows are loaded; the
subtotals are this pipeline's own job (`src/analytics/aggregate.py`, ADR 0003) and storing them too
would be a second source of truth for one fact.

**The `UP`/`M` suffixes are now decoded, and not by guessing.** An earlier note in this repo recorded
them as "likely two report granularities". Row 3 says otherwise:

| Suffix | Row 3 declares | Meaning |
|---|---|---|
| `M` | `Montants - Total` | Euros paid, summed over the year |
| `UP` | `Unités physiques - Moyenne annuelle` | People, averaged over the year's months |

Each dataset in `sync_onem.py` declares the unit string it expects and the loader **refuses a file
whose row 3 disagrees**. Guessing at an abbreviation is what produced the Census 2021 `CAS` bug, so
if ONEM ever swaps a suffix's meaning that is a crash, not a silent switch from people to euros.

### Indicators loaded

Seven, all additive, plus one derived. **Only total columns are loaded, and the reason is measured:**
ONEM masks small counts as the literal string `<10`, and in the `UP` files that masking is pervasive
in the fine breakdowns — 513 of 618 rows for CCI-NDE voluntary part-timers, 519 for unpaid teaching
periods — while almost absent from the totals (1 row for CCI-DE). A column masked for four communes
in five carries no usable commune-level signal. The `M` files are not masked at all: euros are not
disclosive.

| Indicator | Source column | Unit |
|---|---|---|
| `UNEMPLOYED_JOBSEEKERS` | `CCI_..._UP`, column `CCI-DE` | people (annual average) |
| `UNEMPLOYMENT_BENEFIT_RECIPIENTS` | `CCI_..._UP`, column `Total` | people (annual average) |
| `TEMP_UNEMPLOYED` | `CT_..._UP`, column `Total` | people (annual average) |
| `PART_TIME_BENEFIT_RECIPIENTS` | `TTP_..._UP`, column `Total` | people (annual average) |
| `UNEMPLOYMENT_BENEFIT_PAID` | `CCI_..._M`, column `Total` | EUR |
| `TEMP_UNEMPLOYMENT_BENEFIT_PAID` | `CT_..._M`, column `Total` | EUR |
| `ACTIVATION_MEASURES_PAID` | `EMPL_..._M`, column `Total` | EUR |
| `SHARE_POP_ON_UNEMPLOYMENT_BENEFIT` | derived | percent |

`UNEMPLOYED_JOBSEEKERS` is the headline and the pipeline's most current commune-level labour-market
series. It **supersedes Census 2021's `UNEMPLOYMENT_RATE_COM` for currency but not for definition**,
and the two are kept separate rather than merged: ONEM counts *benefit claimants*, so it excludes
jobseekers without entitlement, while the census figure is the EU Labour Force Survey definition
measured against the labour force. The derived share is over TOTAL POPULATION, not the labour force,
because ONEM publishes no local labour-force denominator — so it is deliberately **not** called an
unemployment rate anywhere in the config or the UI.

### Verified against ONEM's own published subtotals

The strongest check available, and it uses the figures this pipeline deliberately does *not* load:
summing the loaded commune rows must reproduce the province/region subtotals printed in the same
sheet. Every year reconciles, and every residual is explained:

| Year | Loaded commune sum | ONEM's own subtotals | Gap |
|---|---|---|---|
| 2017 | 373,699.9 | 373,700.9 | −1.00 |
| 2023 | 284,785.2 | 284,786.1 | −0.92 |
| 2024 | 284,859.0 | 284,859.2 | −0.17 |
| 2026 | 279,659.0 | 279,668.5 | −9.50 |

Each gap is **exactly one masked commune** — Herstappe (75 residents) in every year but 2026, where
it is Horebeke — and each is under the 10-person bound the `<10` marker implies. The national totals
also match ONEM's published figures directly: 373,701 in 2017 falling to 284,786 in 2023.

### Three decisions that keep a plausible-looking wrong number off the page

1. **A masked cell is not a zero.** `<10` is stored with `status = 'suppressed'` and a NULL value —
   the one case `migrations/001_core_schema.sql`'s CHECK constraint permits a NULL for. Zero would
   understate every aggregate built on it; skipping the row would make the commune
   indistinguishable from one ONEM does not cover. This is the pipeline's first use of that status,
   which is why `STATUS_TO_LETTER` in `export_communes_csv.py` gained `S` (and `R`/`E`/`N`, which
   were reachable and falling through unmapped), and why `communes.html`'s status pill now matches
   the letters the exporter actually writes rather than the word `suppressed`, which never fired.
   Consequence, visible and correct: `PART_TIME_BENEFIT_RECIPIENTS` is masked for 132 of 618 rows,
   so its national and regional aggregates fall below the 90% coverage floor and **are withheld
   entirely** rather than published from four communes in five.
2. **An explicit `0.0` is a real zero.** ONEM writes both, and they mean different things. There is
   exactly one in ten years of the CCI-DE column: Herstappe in 2026. The loader keeps that
   distinction.
3. **A part-year TOTAL is not an annual total.** The current year is **skipped for the euro files**.
   Antwerp's full unemployment benefit reads EUR 261.0m for 2025 and EUR 44.3m for 2026 — about two
   months' worth. Published as the latest figure that is an 83% collapse that never happened, and no
   status letter repairs a number a reader has already misread. A part-year *average* is a different
   matter and IS loaded, marked provisional: Antwerp's claimant count moves 17,328 → 18,207 across
   that boundary rather than falling off a cliff.

### One property worth knowing before comparing ONEM to anything else

**ONEM backcasts today's commune map onto all ten years.** All sheets carry the identical 566 labels,
so its 2017 rows are expressed on the 2025 geography — Hasselt's 2017 figure covers the merged
territory, not the Hasselt that existed in 2017. That makes it a pinned-vintage source in exactly
the sense `scripts/export_aggregates_csv.py` already detects for fiscal income (it picked up all six
ONEM indicators automatically), and it is why names resolve against *current* municipalities rather
than period by period.

The visible consequence: `SHARE_POP_ON_UNEMPLOYMENT_BENEFIT` is absent for 31 communes in their
pre-merger years, because population is recorded on the map that actually existed each year and ONEM
is not. The derived engine correctly declines to divide across mismatched boundaries rather than
producing a ratio of two different territories. Any comparison of an ONEM figure against a
non-pinned source for a pre-2025 year carries the same caveat.

### Ambiguity that had to be resolved rather than papered over

Two current communes carry the French name **Saint-Nicolas** — 46021 in East Flanders and 62093 in
Liège — and ONEM lists both, using the Dutch `Sint-Niklaas` for the Flemish one. A name index built
with `setdefault` maps *both* labels onto whichever row came first and never reaches the other. It
surfaced as a vintage collision on the first real run, but in a single-file load it would instead
have doubled one commune's figure and dropped the other's silently. `CommuneIndex` therefore
**detects** ambiguity and resolves it against the arrondissement the file itself states — reading
ONEM's `arr.*` rows, which *close* each block rather than heading it. Same compound
(name, arrondissement) match `src/fetchers/statbel.py` needs, for the same pair, for the same reason.

Name folding also has to handle Statbel's typographic apostrophe (U+2019) against ONEM's ASCII
quote. Separators are replaced **before** the accent fold, not after: folding first deletes a
non-ASCII apostrophe outright and turns `Braine-l'Alleud` into `braine lalleud`. That ordering left
exactly three communes unmatched — Braine-l'Alleud, Fontaine-l'Evêque, Mont-de-l'Enclus — and the
loader refuses a partial load rather than dropping them.
## police.be — four crime-rate indicators, APPROVED 2026-09-06, 14th dataset

Raised by the maintainer 2026-09-06 with a working request captured from their own browser, then
**approved 2026-09-06** on the licence text below, which the maintainer found and confirmed is
acceptable while they separately email the federal police for written confirmation. If that
confirmation narrows or contradicts the reading below, this row and every indicator sourced from it
must be revisited.

**Licence, quoted in full (maintainer-supplied 2026-09-06):**

> Lors de l'utilisation de ces statistiques, il est demandé de toujours indiquer correctement la
> source des données : Police fédérale - Direction de l'information policière et des moyens ICT.

**Thinner than every other source here.** Statbel's CC BY 4.0 and ONEM's own conditions both state
commercial reuse is permitted; this text does not say that, and does not say the opposite either —
it is silent on everything except attribution. Approved on that silence, not on an explicit grant:
credit the source correctly, exactly as stated, and nothing else is asserted. `.attribution` on
`communes.html` and `local.html` (all three languages) states this plainly rather than implying CC
BY or a stated commercial-reuse grant covers it — `tests/test_statbel_attribution.py` has a section
asserting the disclaimer is present.

**The maintainer separately found that each rate's DENOMINATOR is published by SPF Economie/Statbel
under CC BY 4.0** (vehicle fleet, private households, building stock — see the per-indicator table
below). That governs the denominator DATA, not the RATE police.be itself publishes and this pipeline
actually consumes: the "z" value in each file is police.be's own combination of a police-recorded
count and that denominator, and reusing police.be's published output is still governed by police.be's
own condition above, not inherited from whichever government dataset it cites as an input — the same
way a report citing Eurostat as a source does not put the whole report under Eurostat's licence. Kept
as attribution-only, pending the maintainer's confirmation of whether this reading matches what they
found.

**MANUAL ONLY, and for a different reason than Statbel or ONEM.** `onem.be` and `statbel.fgov.be`
fail at the TCP handshake; `police.be` responds, but with an HTTP 403 "Maintenance" page and no
session cookie, to every request both this pipeline's own network context AND a real GitHub Actions
runner send (`scripts/fetch_police_raw.py`, run confirmed). The maintainer fetched every file used
here from their own browser, where the same requests return 200 — that gap between "reachable by an
ordinary browser" and "reachable by anything this pipeline runs" is exactly why this stays a manual
source rather than an automated one, the same shape as Census 2021 and real-estate.

**Four categories, one directory each under `data/raw/police/`, one file per period named literally
by its year** (`cambriolage/2024`, `vol de voiture/2025`, …). Loaded by `scripts/sync_police.py`,
committed at `data/police_observations.csv`. `cambriolage` has a real multi-year history
(2000, 2017–2025); the other three have one file each (2025 only, as of this writing).

| Indicator | Category (fr) | Denominator (source: SPF Economie) |
|---|---|---|
| `HOUSE_BURGLARIES_PER_10K` | cambriolage dans habitation | dwellings (Parc de bâtiments) |
| `CAR_THEFT_PER_10K` | vol de voiture | cars (Parc de véhicules à moteur) |
| `THEFT_FROM_VEHICLE_PER_10K` | vol dans ou sur un véhicule | vehicles (Parc de véhicules à moteur) |
| `DOMESTIC_VIOLENCE_PER_10K` | violence intrafamiliale (VIF) | households (Ménages privés) |

**`HOUSE_BURGLARIES_PER_10K`'s denominator was wrong on first load, and is corrected here.** An
earlier version of this indicator called it "per 10,000 inhabitants", taken from the maintainer's
own filename for the first file supplied (`...par 10000hab.txt`). That was the downloader's own
shorthand, not police.be's methodology — once the real citation surfaced, the unit and every
description were corrected to per-dwelling. `scripts/sync_police.py`'s reference-row upsert had to
change too: it originally used `INSERT OR IGNORE`, which would have left the wrong name sitting in
an already-loaded database forever, since nothing else ever touches that row. Now
`ON CONFLICT ... DO UPDATE`, matching `scripts/sync_to_canonical.py`'s own pattern (the same latent
gap still exists in `sync_realestate.py`/`sync_census2021.py`, not fixed here, out of scope for this
source).

**Every file in every category shares one fixed geography — 587 `geo_code`s, identical set across
every category and every year, including "cambriolage/2000".** Proven, not assumed: Kruisem (NIS
45068) was FORMED by the 2019 merger wave and did not exist as that code before 2019-01-01, yet it
carries a real, distinct value in the `cambriolage/2000` file. police.be's own historical tool
backcasts its current (pre-2025-merger) municipal grid onto every year it shows, the same move ONEM
makes for its own history. So every row of every file, regardless of the year in its filename, is
resolved against ONE FIXED PERIOD (`2024`, the last day before the 2025 mergers) — not
`resolve_geo(nis, that file's own year)`, which raises for merger-created communes in a pre-merger
year and would be wrong regardless, since the "z" value already reflects police.be's own
current-grid attribution, not a true historical one. The 13 communes created by the 2025 mergers
have no value from this source, in any year, in any category.

**Six of the 587 `geo_code`s are not real geography, under any period** — the negative placeholders
`-1`/`-3`/`-4` (a residual/unknown bucket in the source's own export) plus three positive codes
(`21020`, `23095`, `31999`) matching no geography row, current or historical. All six are always
paired with `z: 0` in every file checked, and are skipped as a GENERAL rule (any code unresolvable
at the pinned period, carrying a value of exactly 0), not a hardcoded list — a nonzero value on an
unresolvable code would still raise. 581 of every 587-row file resolve.

**Status: every year is `'final'` except the MOST RECENT year in each category's own file set, which
is `'provisional'`.** For `cambriolage`, this is now measured rather than guessed: national totals
across the real 2000/2017–2024 series run 35,138–46,000-ish per year with ordinary year-to-year
variation, and 2025's total (35,138) sits inside that range rather than reading like a
two-months-only partial total the way ONEM's genuinely partial euro files do (falling to ~17% of a
full year). That is evidence 2025 may already be a complete period, not proof — nothing in any file
states whether "2025" means a completed calendar year or a still-open rolling window, so
`'provisional'` records the remaining uncertainty rather than asserting a finality nobody has
confirmed. Both this and the fixed-geography caveat above are stated in the published `.attribution`
text on `communes.html` and `local.html`, not left as a code comment only.

**All four indicators are not aggregatable, on purpose.** Every one is a rate with no underlying
count in the source to derive it from — CLAUDE.md rule 6 governs deriving a ratio FROM stored
additive components, and there are none here to derive it from. `is_additive = 0`,
`aggregation_method = 'not_applicable'` for all four; `export_aggregates_csv.py`'s
`methods_from_metadata()` refuses them, so they show at commune level only, with no
province/region/Belgium row manufactured by averaging a rate across communes
(`docs/decisions/0003-aggregation-rule.md`). They still rank correctly (percentile/rank need only a
value comparison, not summability) — verified on Kruisem, a 2019-merger commune: all four indicators
resolve and rank without error.

**`DOMESTIC_VIOLENCE_PER_10K` is a recorded-incident rate, not a prevalence estimate**, flagged in
its own indicator description: domestic violence is known to be substantially and unevenly
under-reported, so a commune difference may reflect reporting practice as much as the true
underlying rate.

**Explicitly rejected approach, unchanged from the earlier note on this source.** The maintainer's
original script pasted a session cookie copied out of a browser (their own note: expires in 1–2
hours) and paced requests with a 2-second sleep described as preventing "the federal police WAF from
IP-banning you". Neither is built here, for the same reasons as before: a hand-pasted expiring
cookie is not automation, and pacing chosen to stay under a security control's ban threshold is
evasion of that control regardless of how public the data is.

**Not yet done, recorded so it is not silently skipped:**
1. Written confirmation from the federal police — the maintainer is emailing separately. If it
   contradicts the reading above (including the FPS-Economy-denominator question above), this row
   and all four indicators must be revisited.
2. Whether the three single-year categories (car theft, vehicle theft, domestic violence) will gain
   a historical series the way `cambriolage` has. Adding one is dropping a year-named file into that
   category's directory — no code change needed.
3. The wider `criminality_table` JSON endpoint this row originally investigated (year=2016…2025,
   monthly, by NIS with an undecoded `_4` suffix) is UNCHANGED from the earlier note: still
   unreachable from any network this pipeline controls, still undecoded, still not loaded. Only the
   four rate snapshots the maintainer fetched by hand are live.
4. Whether next year's equivalent files will use the current commune map, closing the 13-commune
   gap, or continue lagging it. Not knowable until a second year's file exists to compare.

## Federal subsidy register — ASSESSED AND DECLINED 2026-09-06

`Registre_des_subventions_{2023,2024,2025}.csv`, supplied by the maintainer. Belgium's federal
subsidy register: one row per individual grant payment, ~25,000 rows across three years, with
beneficiary, address, enterprise number, paying department, budget line and amount. Real open data,
correctly structured, and **not loaded** — recorded here so it is not re-researched later (Block E's
deferred-not-deleted rule).

**Why not, in one line:** it is a register of payments to named organisations, not a commune-level
statistic, and both routes to making it one produce a wrong number.

**Route 1 — the 129 rows that name a commune directly** (`COMMUNES Commune d'Amay`). These genuinely
are money to that commune, but they cover only 126–133 of 565 communes depending on the year, total
€47–77m against a €195–210bn register, and a commune's absence means "no grant of this recorded
type this year", not "no federal money" — the absence-looks-like-zero failure `data_model.md` warns
about. The source also **truncates beneficiary names** at ~30 characters (`Molenbeek-`,
`Chapelle-lez-`, `Court-Saint-`), so 15 of 129 need guessing, and guessing which commune is credited
with money is the wrong place to guess.

**Route 2 — parse the postcode out of the beneficiary address and aggregate** (the maintainer's own
suggestion, and mechanically the better one). It works: 95.3% of rows carry a parseable 4-digit
postcode, Statbel's own `Conversion Postal code_Refnis code` file resolves 1,146 of 1,149 postcodes
to exactly one commune (only 1040, 1050 and 1804 are ambiguous), and it attributes 8,820 rows across
562 of 565 communes. **The result is nonetheless wrong**, measured rather than argued: it makes
Saint-Gilles — 50,000 residents — the recipient of **44.9% of all federal subsidy in Belgium**
(€29.4bn), with four Brussels communes taking 92% between them. The cause is that the address is the
beneficiary's REGISTERED office: postcode 1060 hosts the Federal Pensions Service, so its €13.9bn
national budget lands on Saint-Gilles; 1210 carries a €9.4bn transfer to the Flemish Region; 1080
carries €4.7bn to the French Community. The method measures where national institutions are
registered, not where money is spent, and would look entirely plausible on a page.

**What it could honestly support, if ever wanted:** a NATIONAL series (federal subsidy by department
and category), which is clean and correct but does not fit the municipal product; or a commune
indicator labelled explicitly "grants recorded in the federal subsidy register", with absence
stated to mean "none recorded". Neither was judged worth the misreading risk.

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

### Boundary geometry — LOADED 2026-09-07, the first geometry in the pipeline

Block C deliberately stored **no geometry**: the `geographies` table carries names, codes and
parentage only, and the note there said there was nothing to build until Block X needed
boundaries. Block X needs them, so this is that.

**Source file**, hand-downloaded by the maintainer 2026-09-07, held under
`data/raw/statbel/sectors/` (gitignored — it is 227 MB):

| File | Contents |
|---|---|
| `sh_statbel_statistical_sectors_3812_20260101.geojson` | 20,781 statistical sectors valid 2026-01-01, each carrying its commune/district/province/region NIS code |
| `..._readme_{en,fr,nl,de}.doc` | Field definitions, reference system and accuracy |
| `Licence open data_{FR,NL}.pdf` | The reuse licence shipped with the data |

**The licence is one already cleared.** The NL PDF in the download is *byte-identical*
(SHA-256 `aaf6847a…`) to
[`licences/statbel_open_data_licence_2015-10-22.pdf`](licences/statbel_open_data_licence_2015-10-22.pdf),
the *Licentie open data* of 22 October 2015 recorded below. The FR PDF is the French text of the
same document, now committed alongside it as
[`licences/statbel_open_data_licence_2015-10-22_fr.pdf`](licences/statbel_open_data_licence_2015-10-22_fr.pdf).
So no new licence question arises: commercial reuse and derived works are granted, and the
obligation is to name the producer and the date of last update — which the map page does.

**What the file says about itself**, read from its own readme rather than assumed:

- Reference system: **Belgian Lambert 2008, EPSG:3812** — projected metres, not longitude/latitude.
- Accuracy: **1:10,000**.
- Boundary version: the 2026 one, differing from 2025 "due to improved accuracy of the statistical
  sector boundaries" rather than any change of commune.

**What is built from it** — `scripts/build_commune_boundaries.py` → `data/geo/communes.geojson`
(1.24 MB, ~320 KB gzipped), committed:

1. The 20,781 sectors are dissolved into the **565 communes**, grouping on `cd_munty_refnis`.
   All 565 resolve through `resolve_geo()`; none is unknown.
2. Borders are simplified to a **50 m tolerance over a shared-arc topology**, not per polygon.
   Simplifying each commune separately moves each shared border twice, in two directions, opening
   visible gaps and overlaps between neighbours. Measured: 55.2 MB dissolved → 1.9 MB at 25 m,
   1.24 MB at 50 m, 0.74 MB at 100 m.
3. Coordinates are converted to WGS84 lon/lat, **reading EPSG:3812 from the file's own `crs`
   member**. Statbel ships boundary layers in both 31370 and 3812; the two are about a kilometre
   apart, and the wrong choice produces a map that looks entirely normal and is in the wrong
   place. The script refuses to run on a file that declares no CRS rather than guess it
   (CLAUDE.md rule 13).

**Three things this is not.** It carries no indicator values, so it is reference data like the
rest of `statbel_geography` and not an observation source. Its simplified outlines are **not**
accurate to the source's 1:10,000 and must never be used to locate a boundary on the ground —
stated on the page itself. And it is deliberately kept out of `requirements.txt` and `make all`:
rebuilding needs `shapely`, `topojson` and `pyproj` (`requirements-geo.txt`) plus the 227 MB
source, so the *output* is committed and CI reads it as data. `make boundaries` rebuilds it, and
should be run only when Statbel publishes a new boundary vintage — the script refuses any file
not stamped `2026-01-01`, so a new vintage is a loud failure rather than a silent change of map.

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
