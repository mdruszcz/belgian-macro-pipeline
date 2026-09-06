# Review: unit consistency, definitional breaks and label honesty

Date: 2026-09-06
Scope: Block F `[REVIEW]` step. Nominally the five municipal indicators; the prescribed
unit-consistency sweep also surfaced defects in national indicators, reported here rather than
ignored for being outside the block.

> "The subtle-but-wrong number is your main product risk. Labels are where it enters."
> — `docs/steps`, Block F

Method: every claim in an indicator's config was checked against the actual data or the source's own
wording, not accepted as written. Ground truth for the Eurostat finding came from DBnomics directly.

## Verified correct — no action

| Claim | How it was checked | Result |
|---|---|---|
| `CD_AGE` is single years 0–100, 100 top-coded | Parsed the real 2026 file | 101 distinct values, contiguous 0..100 ✓ |
| Age bands sum to the population total | Recomputed over the committed store | 0 mismatches in **6,383** commune-year cells ✓ |
| Population figures are plausible | Range over the store | 75 (Herstappe) … 565,615 (Antwerp) ✓ |
| Municipal metadata is coherent | `indicators` table | all `is_additive=1`, `sum`, `decimals=0` ✓ |
| No `preferred_direction` inversions | All 35 configs | `UNEMPLOYMENT_RATE` is `lower_is_better` ✓ |
| FR/NL/EN municipal labels agree with the source | Bestat's own datasource description | "Number of local units" ↔ *unités d'établissements* ↔ *vestigingseenheden* ✓ |
| `LOCAL_UNITS` 2023-Q4 is genuinely current | Queried the live standard view | still serves only "4ème trimestre 2023" ✓ |

## Findings fixed

**F1 — the age-band configs claimed a runtime assertion that does not exist.** All three said
"summing the three bands reproduces `POPULATION_BY_COMMUNE` exactly; `scripts/sync_population.py`
asserts that rather than assuming it." It does not assert it. `sync_population.py:211` *derives* the
total as `sum(bands.values())`, so the two agree by construction and the invariant is unfalsifiable
at runtime. The claim overstated the verification actually performed — precisely the failure class
this review exists to catch. Corrected to describe what is true by construction, and what the real
runtime guard is: any population outside every band raises rather than vanishing from both the bands
and the total.

**F2 — six indicators named themselves "MEUR" while holding an index.** `EUROSTAT_GDP_Q_MEUR` and its
five country variants take Eurostat's `CLV10_MEUR` series (chain-linked volumes, *million euro*) and
rebase it so the 2010 average is 100 (`src/fetchers/eurostat.py:51-64`). Ground truth from DBnomics
for 2025-Q3 is **112,599.6 M€**; we publish **123.99**, a constant ratio of 1/908.18 across periods,
confirming a deliberate rebase. The `unit` field correctly said `index_2010`, but the ID and the
displayed name asserted millions of euro — and **91 rows were live in the published dashboard CSV**
carrying that name. Display names corrected to state what the number is.

**F3 — twelve indicators had no real label at all.** Their `name.en/fr/nl` were all three set to the
raw indicator ID, so the dashboard showed `EUROSTAT_GDP_Q_MEUR` and there was no French or Dutch
whatsoever. Root cause: `sync_to_canonical.py` wrote `meta["name"]` (from the legacy `SOURCES` dict)
into all three language columns, ignoring `config/indicators/*.yaml`. All twelve given accurate
trilingual names.

**F4 — config edits never reached the database.** Every writer used `INSERT OR IGNORE`, so once an
`indicators` row existed its metadata was frozen; correcting a name in config changed nothing
downstream. This directly contradicts Block B's premise that config is the source of truth.
`sync_to_canonical.py` now upserts display metadata from config on conflict.

**F5 — a self-inflicted regression, caught before merge.** The corrected names contain commas
("GDP volume index, Belgium (2010=100)"), and both exporters wrote CSV with hand-rolled f-strings and
no quoting. Regenerating broke **287 rows** of the published export by shifting every column after
the name. Both exporters now use `csv.writer`; `tests/test_export_canonical_csv.py` has a regression
test. The hand-rolled writers were a latent bug all along — safe only because no value had yet
contained a comma.

**F6 — `LOCAL_UNITS_BY_COMMUNE`'s description was incomplete in two ways.** It said "registered",
which the source does not say (Statbel's own wording is just "Number of local units"), and it omitted
that the figure is summed across **all** NACE 2008 economic activities — where the population config
does document its analogous summing. It also now states plainly that this is a single 2023-Q4
snapshot that will not move until Statbel republishes, rather than implying a live quarterly series.
That matters for honesty and because the Statbel licence requires showing the date of last update.

## Finding reported, deliberately not fixed

**F7 — the same concept carries different `preferred_direction` across country variants.**
`EUROSTAT_GDP_Q_MEUR` is `higher_is_better` for Belgium but `neutral` for DE/EA/ES/FR/NL;
`LABOUR_COST_BE` is `contextual` while its variants are `neutral`; `EC_CONS_CONF_BE` is
`higher_is_better` while `EC_CONS_CONF_EU` is `neutral`. This may be deliberate — the non-Belgian
series exist as comparison context and arguably should not carry a directional arrow — but it is
undocumented, and Block L's percentile and comparison work will read this field. Left as a maintainer
decision rather than silently harmonised.

**Renaming the misleading indicator IDs themselves** (`EUROSTAT_GDP_Q_MEUR` → something honest) is
also not done here. `indicator_id` is part of the `observations` primary key and is referenced by the
published exports, so it is a migration, not a label fix.

## Residual gap

`INSERT OR IGNORE INTO indicators` remains in `sync_statbel.py`, `sync_population.py`,
`load_observations_csv.py` and `port_existing_indicators.py`. Only the daily national path was fixed
(F4). Those four write indicators whose configs have not needed correcting, so no stale label exists
today, but the same trap would recur if one did.
