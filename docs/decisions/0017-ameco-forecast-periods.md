# ADR 0017 — AMECO forecast years are stored as `final`

Date: 2026-10-04
Status: **PROPOSED — awaiting maintainer approval.** Nothing in `src/fetchers/`,
`scripts/sync_to_canonical.py` or `src/validation/` changes until he approves this record.
Required by: CLAUDE.md rules 6, 13, 19, 26, 35. Sibling record:
`docs/decisions/0018-stopped-inflation-series.md`. Page-side half of the fix:
`docs/features/site_clarity.md`, batch 1.

## Context

Ten stored values are European Commission forecasts carrying `"status": "final"`:
`LABOUR_COST_BE`, `LABOUR_COST_DE`, `LABOUR_COST_EA`, `LABOUR_COST_FR`, `LABOUR_COST_NL`, each for
2026 and 2027. One of the five reaches the public site.

**How a forecast becomes a measurement.** `config/sources/dbnomics_ameco.yaml:4` is the only source
with `adapter: dbnomics`, and the only configs using it are the five `LABOUR_COST_*.yaml`
(`/PLCD/{BEL,DEU,EA20,FRA,NLD}.3.1.99.0.PLCD`, annual, rebased to 2010). `src/fetchers/dbnomics.py`
reads only `period` and `value`, then — line 57 — stamps `"obs_status": "A"` on **every** row
unconditionally. Verified by reading the file: there is no branch. `tests/test_dbnomics_source.py:26-45`
pins that behaviour. `scripts/sync_to_canonical.py:159` passes it through `src/fetchers/sdmx_status.py:25`,
which maps `A` → `final`, and the row lands in `observations` with a period end of 31 December.
100 rows are stored (5 series x 2008–2027), all `final`.

**What reaches a reader.** `scripts/export_canonical_csv.py:97-98` keeps `be:country` only, so only
the Belgian series is published:
`public/data/national.json` carries `LABOUR_COST_BE` 2026 = 142.26 and 2027 = 145.02, both `final`
(verified by reading the payload), and `public/data/explorer/national/LABOUR_COST_BE.json` carries
`[145.02,"A"]` with `period_max` 2027. A headless render of `macro.html` shows two tiles reading
`Labour cost index · 145.0 · 2027 · AMECO (European Commission) · (B) Restated · retrieved
2026-09-05` — with no status word anywhere. The four foreign series sit in the committed database
only.

This is the rule 6 failure in its plainest form: a projection sitting in `observations` as if it
were source data. It is also undetectable by the reader, because provenance grade D ("Forecast") is
defined but never assigned — `src/exporters/provenance.py:230,238`, and `tests/test_provenance.py:235`
asserts the grade is empty.

**Neither source flags the forecast.** This is the whole difficulty.
`https://api.db.nomics.world/v22/series/AMECO/PLCD/BEL.3.1.99.0.PLCD?observations=true` returns
`@frequency, dataset_code, dataset_name, dimensions, indexed_at, period, period_start_day,
provider_code, series_code, series_name, value` — and no `observations_attributes` on any of the five
series, where a Eurostat series through the same API carries `OBS_FLAG`. There is no last-actual
marker. The only date in the response is `indexed_at: 2026-05-22T01:32Z`, the day after AMECO's
21/05/2026 release. AMECO's own bulk file (`ameco7.zip`, 2 June 2026) is
`CODE;COUNTRY;SUB-CHAPTER;TITLE;UNIT;1960…2027` with numbers or `NA` and no flag either — so the
planned direct `ameco` adapter (`docs/data_catalog.md:1142-1143`) inherits exactly the same gap.

**The split has to come from the release date.** AMECO's Reference Metadata (26 November 2025) states
the rule: *"The most recent available two years (Spring forecast) or three years (Autumn forecast) in
the AMECO database are forecasts"*, updated "usually… in mid-May and mid-November", not in between.
Today, after the May 2026 release, 2026–2027 are forecasts and 2025 is the last outturn. From
mid-November 2026, it will be 2026–2028.

**A blanket rule would be worse than the bug.** A sweep of 1,779 database rows and 945,711 CSV rows
across all 16 stores found every current row whose period ends after its own vintage or after
2026-10-04 with a status other than `estimate`:

| Store | Rows | Status | What they are |
|---|---|---|---|
| `dbnomics_ameco` | 10 | `final` | **the bug.** The five 2027 rows are the only rows anywhere whose period had not even started |
| `statbel` population | 2,260 | `final` | correct — population *on 1 January* 2026 |
| `spf_finances` | 6,711 + 69 | `final` / `suppressed` | correct — 1-January snapshots, 11 indicators, plus the IPP rate for tax year 2026 |
| `eurostat` consumer confidence | 34 | `final` | correct — September surveys |
| `nbb` `CONSUMER_CONFIDENCE` | 1 | `final` | correct — the September survey |
| `onem` 2026 | 2,653 + 173 | `provisional` / `suppressed` | correct, and deliberately provisional (`scripts/sync_onem.py:298-307`). The 2,653 is **2,087** rows from `onem` plus **566** from `onem_rates`, and those 566 are `UNEMPLOYMENT_RATE_INSURED` 2026 for the 565 communes **plus the `be:country` row = 5.05** — the one named below as a figure the page-side rule must not hide. Counting communes only gives 565 and understates the row by one |

So "an open period cannot be `final`" fires on 9,016 rows, 10 of which are the defect. The other
9,006 are right. No `estimate` row has an open period at all (1,388 exist; every one is a Eurostat
flag on a past period). Everything else in the pipeline is already correct: the FPB consensus goes
to the `forecasts` table (`belgian_macro_db.py:153-161,428`, 78 rows) and never to `observations`;
the `FC_*.yaml` configs are `frequency: F` and skipped by the fetch (`belgian_macro_db.py:52-72`);
Eurostat's `f` flag fails loudly (`src/fetchers/eurostat.py:65-67`). AMECO is the one hole.

## Options

**(a) Stamp `E` (→ `estimate`) on the forecast years.** Smallest change: `src/fetchers/dbnomics.py:57`,
`belgian_macro_db.py:412-416`, and the pinned test. Downstream already copes — `sdmx_status.py:27`
maps it, `src/db/vintages.py:67` writes a new vintage on a status change so the stores self-heal
with no migration, and the pages already word it (`macro.html:449,569`, `explorer.html:468`).
Against it: a reader still sees `145.0 · 2027` as the headline with "estimate" in small print,
because `latestNumeric` ignores status — so the page-side fix is needed anyway. `estimate` acquires
a second meaning (today it means a source's own estimate flag on a past period). And if the split is
taken from the *fetch* date rather than the release date, then between 1 January and the mid-May
release the just-ended year is still only the autumn forecast, yet would flip to `final`.

**(b) Route the forecast years to the `forecasts` table.** Honest about what they are. Against it:
it touches the one-list `TimeSeriesSource` contract (`src/fetchers/base.py:216-220`) and `fetch_all`;
`forecasts` has no geography, unit or history (its PK is institution/indicator/year), so the four
foreign series have nowhere to go; a published download changes shape; and it needs an **explicit
retirement** of 10 canonical and 10 legacy rows, because legacy rows are never deleted
(`belgian_macro_db.py:187-199`) and `scripts/sync_to_canonical.py:152-155` re-reads all of them, so
they would stay `final` forever otherwise. It would also settle ADR 0001's open question as a side
effect, which is a decision, not a cleanup.

**(c) A validation rule.** `src/validation/rules.py`, with `_period_end` at `:562`, blocking the
daily build. As worded it fires on all 9,016 rows above — against the validation layer's own stated
principle (`rules.py:8-15`: a rule that fires on correct data is a broken rule). To work it needs
each indicator to declare what its period *means* — a flow, a 1-January stock, a survey, a rate set
in advance — and `docs/features/indicator_config.schema.json` has no such field and is
`additionalProperties: false`. It also cannot see the January-to-May case, and on its own it only
turns the build red; it does not make the number right.

**(d) A release-date split in the adapter, plus (c) as a backstop.** Reproduce AMECO's own published
rule: anchor on the release date and treat the last two years (Spring) or three (Autumn) as
forecasts.

## Recommendation

**(d), with the forecast years kept OUT of `observations`.**

1. **The adapter computes the last outturn year from the release date**, following AMECO's own
   documented rule, and stores only periods up to and including it. Anchor on `indexed_at` where it
   is the only date available, and **raise** when it is missing rather than defaulting to "everything
   is final". Ideally this is written once, in the direct `ameco` adapter when that is built, rather
   than twice.
2. **The forecast rows do not enter `observations` at all.** They are counted and logged — never
   silently dropped (rule 13) — and the count is asserted by a test, so a year where AMECO's rule
   changes shows up as a failure rather than as rows quietly appearing or disappearing.
3. **Routing them into `forecasts` is a separate, later decision**, not part of this one. It changes
   a published download's shape and has nowhere to put geography or unit, and nothing on the site
   needs the forecast years today.
4. **The backstop is (c), but only after indicators can declare what their period means.** That is a
   schema addition (`period_meaning`: flow | stock_1jan | survey | rate_set_in_advance), a
   separate PR, and it is what makes a blocking rule possible without firing on 9,006 correct rows.

**Why not (a).** It is the smaller change and it is defensible, but it leaves a Commission forecast
inside `observations` — the table whose contract is "what was measured" — and it needs the identical
page-side work anyway. If the maintainer prefers (a) because it is reversible and touches less, that
is a reasonable trade and this record should be amended to say so rather than quietly reinterpreted.

**What this does to the page.** Under (d) the tile reads `139.0 · 2025`, the latest outturn, and the
forecast leaves the site. Under (a) it reads `145.0 · 2027` with "estimate" beside it.

## What is NOT changed until he approves

- `src/fetchers/dbnomics.py` — unchanged, including the unconditional `"obs_status": "A"`.
- `src/fetchers/sdmx_status.py`, `scripts/sync_to_canonical.py`, `belgian_macro_db.py` — unchanged.
- `tests/test_dbnomics_source.py:26-45` — still pins today's behaviour.
- The 100 stored rows and `data/belgian_macro_export.csv` — unchanged; they keep saying `final`.
- `docs/features/indicator_config.schema.json` — no `period_meaning` field.

**What happens meanwhile.** `docs/features/site_clarity.md` batch 1 stops the forecast years being
shown as current values on the pages, using a narrow page-side rule (a `final` whole-year period
past its own `updated` date is not a current value). That is a display fix. The stored status stays
wrong, the published CSV keeps saying `final`, and the PR body says so.

## Consequences

- A reader stops seeing a 2027 projection presented as a measured 2027 figure. That is the point.
- `LABOUR_COST_BE`'s headline becomes 2025 instead of 2027 — a visibly "older" figure, which is
  correct and will look like a regression to anyone who does not know why.
- Two years of every `LABOUR_COST_*` series leave `observations`. No page, payload or test reads
  them today, verified; an external user who downloaded `data/belgian_macro_export.csv` previously
  will see two fewer rows for that series.
- Every AMECO release changes where the split falls (mid-May, mid-November), so the adapter's
  behaviour changes twice a year by design. A test must pin both sides of a release.
- The `ameco` adapter in `docs/data_catalog.md:1142-1143` inherits this rule when it is built, rather
  than re-deriving it.

## Tests that would guard it

- A fixture with `indexed_at` just before and just after a May release, asserting which years are
  stored and which are excluded, with hand-written expected year lists.
- A fixture with `indexed_at` missing: the fetch raises, and the message names the series.
- An autumn-release fixture: three forecast years excluded, not two.
- A store test: no `dbnomics_ameco` row has a period ending after its series' last outturn year.
- A count test: the number of excluded rows per run equals the expected number and is logged.
- The existing `make all` byte-identical rebuild tests stay green (rule 35).
- `tests/test_dbnomics_source.py` is rewritten rather than deleted, with the reason in its docstring.

## Also noticed, not part of this decision

- **`src/fetchers/dbnomics.py:58-59` silently skips an unparseable value.** Against rule 13.
  Recorded in `docs/implementation/known-risks.md`; not fixed here.
- **AMECO `PLCD` is mislabelled.** It is "Nominal unit labour costs (ratio of compensation per
  employee to real GDP per person employed)"; `config/indicators/LABOUR_COST_BE.yaml:20,26` calls it
  "Labour Cost Index (LCI) — Nominal hourly costs" and "Nominal compensation per employee". Display
  text only, needs no ADR, and is fixed in `docs/features/site_clarity.md` batch 1.
