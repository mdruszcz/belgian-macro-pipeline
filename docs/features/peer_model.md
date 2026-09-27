# Feature: Peer Model v1 — ten most structurally similar communes

Status: **specification only.** Nothing in this document is implemented. This is roadmap
Block M's first step ("It has to be a document before it is an algorithm"), and the very next
step is the maintainer personally approving the variable list below, "crossing out anything
he cannot justify to a directeur financier in one sentence" — so §"The variable list" is
written for that approval, not for a builder. See [ADR 0015](../decisions/0015-peer-model-v1.md)
for the decision record and a worked example computed from real committed data.

## What this is

For every one of today's 565 Belgian communes, its ten most structurally similar communes —
similar in population, age structure, income, unemployment, nationality mix, household size,
enterprise density and property tax base, never in outcomes the platform judges. Two peer
lists come out of the same model:

- **`national`** — pool = the other 564 communes.
- **`region`** — pool = the other communes of the same region. Measured 2026-09-26 from
  `config/geography/geographies.csv` (municipalities with an empty `valid_to`, region resolved
  through `parent_geo_id` the same way `scripts/export_commune_typology.py`'s
  `municipalities_valid_on` / `region_of` helpers do): **Flanders 285, Wallonia 261,
  Brussels 19** (pool sizes for `region` peers are one fewer: 284 / 260 / 18). This corrects the
  285/261/19 split against the number many people carry in their head from the old 300/262/19
  round figures; 565 have never split that way in this pipeline's own geography table, and the
  same 285/261/19 split is independently recorded in `docs/features/comparison.md`'s percentile
  work (2026-09-06), so this is a second, independent measurement agreeing with the first, not
  a single unchecked count.

"Comparable" means **structurally similar on the variables listed below, and nothing else.**
The model makes no claim about which commune is better run, better off, or a better place to
live. See "What this model does NOT claim" at the end.

## The variable list — for the maintainer's approval

Eleven candidates, each measured against the committed data on 2026-09-26 (non-null count for
the 565 communes of today, at the stated period). For each: what it is, where it comes from,
the one-sentence justification a directeur financier would accept, and any caveat. The
maintainer strikes any line he cannot justify that way; the model is built only from what
survives.

| # | Variable | Source id / definition | Period | Coverage /565 | Transform | Justification (one sentence) | Caveat |
|---|---|---|---|---|---|---|---|
| 1 | Population | `POPULATION_BY_COMMUNE` | 2026 | 565 | `log` | Two communes of wildly different size are not structurally comparable no matter how alike their ratios are. | Logged because commune size spans two orders of magnitude (Herstappe ~525 to Antwerp ~565,615) and untransformed population would dominate every distance. |
| 2 | Population density | `POPULATION_BY_COMMUNE` 2026 ÷ area (km²), area from a new `config/geography/commune_area_km2.csv` derived once from the Statbel statistical-sector geometry already catalogued (`docs/data_catalog.md`, "Boundary geometry" row) | 2026 (population) / geometry vintage 2026-01-01 (area) | 565 once the area file exists; **0 today** — `geographies.csv` has no `area_km2` column at all | `log` | A rural and an urban commune of the same population are not the same commune to plan for. | Not computable yet. No new data source: the same `sh_statbel_statistical_sectors_3812_20260101.geojson` already used to build `data/geo/communes.geojson` is re-used, dissolved to 565 communes and its polygon area measured in EPSG:3812 (metres, not degrees — the reason `data/geo/communes.geojson` itself is WGS84 for display but area must be measured in the projected file, never after re-projecting to lon/lat). This is new **config**, not a new **source**: no data_catalog.md row is needed beyond the dated note this PR adds to the existing geometry row. |
| 3 | Share aged 65+ | `POPULATION_AGE_65_PLUS` / `POPULATION_BY_COMMUNE` | 2026 | 565 | none (already a percentage) | An ageing commune and a young commune face different service and revenue structures; this is demography, not judgement. | None. |
| 4 | Share aged 0–14 | `POPULATION_AGE_0_14` / `POPULATION_BY_COMMUNE` | 2026 | 565 | none | Same reasoning as #3, the other end of the dependency structure. | None. |
| 5 | Population change over 5 years | `POPULATION_CHANGE_5Y` (derived, `five_year_change` on `POPULATION_BY_COMMUNE`) | 2026 | 565 | none (already percent) | A growing and a shrinking commune have different pressures (schools and housing vs. tax-base erosion) regardless of current size. | Uses the maintainer's 2026-09-16 "growth on current territory" decision (`docs/features/geography.md` §"Growth on current territory", L219-224): a merged commune's pre-merger years are the summed predecessor counts on today's territory, never averaged or estimated, via `src/analytics/backaggregate.py`. |
| 6 | Average net taxable income per tax return | `AVG_NET_TAXABLE_INCOME` (derived, `mean_from_total` on `FISCAL_TOT_NET_TAXABLE_INC` / `FISCAL_NBR_NON_ZERO_INC`) | 2023 (income year) | 565 | none (already EUR/return) | Income structure is the single most requested "compare us to..." axis for a finance department. | Per tax return, not per inhabitant — a return can cover a couple, and residents with no taxable income are excluded from the count entirely, so this sits well above any income-per-head figure. This is also a headline indicator elsewhere on the site: see "Circularity" below. |
| 7 | Insured unemployment rate | `UNEMPLOYMENT_RATE_INSURED` | 2026 (provisional — 2026 is a running-year average of the months published so far) | 565 | none (already percent) | Labour-market structure is a standard peer-grouping axis and this is the only commune-level rate in the pipeline with a real labour-force denominator. | ONEM's own published rate, not recomputed; a March-2026 definitional break (benefit time-limiting) lowers the reading nationally from March 2026 onward and must not be read as a stronger labour market (`docs/decisions/0005-onem-published-rate.md`). Also a headline indicator: see "Circularity" below. |
| 8 | Share of foreign nationals | `SHARE_FOREIGN_NATIONALS` (derived, `share_of_total` on `POP_FOREIGN_NATIONALS` / `POPULATION_BY_COMMUNE`) | 2021 census | 565 | none | Nationality mix is a real structural feature of a commune's population, independent of any income or employment figure. | 2021 census — see "Mixed periods" below. |
| 9 | Average household size | `AVERAGE_HOUSEHOLD_SIZE` (derived, `mean_from_total` on `POPULATION_BY_COMMUNE` / `HOUSEHOLDS_PRIVATE`) | 2021 census | 565 | none (already persons/household) | Household structure (families vs. singles vs. shared housing) shapes housing and service demand independently of income or age. | 2021 census — see "Mixed periods" below. Slightly overstates the true private-household average because the numerator includes collective-household residents the denominator excludes (documented on the indicator itself). |
| 10 | Enterprise density | `LOCAL_UNITS_BY_COMMUNE` 2023-Q4 per 1,000 residents, denominator `POPULATION_BY_COMMUNE` 2023 | 2023-Q4 (units) / 2023 (population) | 565 | `log` | A commune's local economic base (how many establishments per resident) is structural, not a judgement of prosperity. | `LOCAL_UNITS_BY_COMMUNE` is a frozen snapshot — Statbel has not republished this series since 2023-Q4 (verified 2026-09-06, indicator's own description) — so this variable will not move until Statbel does, unlike every other variable in the list. |
| 11 | Property tax base per resident | `MUN_CADASTRAL_INCOME_TOTAL` 2026 / `POPULATION_BY_COMMUNE` 2026 | 2026 | 565 | `log` | The property tax base per resident is exactly the structural fact a directeur financier compares first when sizing up a peer commune's fiscal capacity. | None found; both inputs are complete for all 565 at 2026. |

All eleven measured counts match the 565/565 the model needs, **except #2 (density)**, which is
0/565 today because no area figure exists anywhere in this pipeline yet. Density is written in
because the maintainer asked for it in the roadmap sketch ("population, density, income, age
structure..."), but it is **not buildable in v1 as specified** until the area CSV in row 2's
own cell is built and committed — a separate, later PR (this one is documentation only, per its
own exclusions). If the maintainer does not want to wait for that, striking row 2 ships a
ten-variable v1 immediately with everything else unchanged.

### Excluded, with a one-line reason each

| Variable | Reason |
|---|---|
| `MEDIAN_HOUSE_PRICE` | 388/565 communes have a value in the latest quarter (2026-Q1) — not enough coverage to standardise over all 565 without inventing 177 values. |
| `MUN_LEASE_RENT_MEDIAN_HOUSING` | 554/565 at the latest period (2026-Q2); the remaining 11 are suppressed cells (small-sample privacy), not missing data, and rule 26 forbids treating a suppression as anything but itself. |
| Every WalStat series (`MUN_DEBT_*`, `MUN_EXPENDITURE_*`, `MUN_REVENUE_*`, `BIM_BENEFICIARIES_SHARE`, `GRAPA_RECIPIENTS_SHARE_65_PLUS`, `UNEMPLOYMENT_RATE_BIT`, `PREPAYMENT_METERS_*`) | Wallonia only (`docs/features/walstat_adapter.md`) — no value exists for Flanders' 285 or Brussels' 19 communes, so a national peer model cannot use them at all. |
| Police indicators (`CAR_THEFT_PER_10K`, `HOUSE_BURGLARIES_PER_10K`, `THEFT_FROM_VEHICLE_PER_10K`, `DOMESTIC_VIOLENCE_PER_10K`) | 552/565 communes have a row at all (13 merged communes cannot be reconstructed — `docs/decisions/0012-police-zero-is-not-available.md`), and two of the four have real values in only 483 of those 552 once suppressed z:0 placeholders (status `na`, never a measured zero) are excluded. |
| `BANKRUPTCIES` | Monthly and noisy — a peer model needs a structural, not a volatile, figure; nothing here averages it into something structural without inventing a smoothing choice. |
| Belfius socio-economic typology (`public/data/metadata/typology.json`) | It is a transcribed **classification**, not a measured figure, with an unverified licence (`docs/data_catalog.md` §"Belfius socio-economic typology", ~L73-89). Using it would (a) import someone else's judgement of what makes communes alike into a model the roadmap explicitly wants BelPulse's own, and (b) risk republishing a still-unverified-licence classification as a structural input to a paid product. |
| `MUN_IPP_ADDITIONAL_RATE` (communal income-tax surcharge) | It is a **policy choice** the commune's council sets, not a structural fact about the commune, and it is also a finance-benchmark headline target elsewhere on the roadmap (Block P) — using it to select peers and then benchmarking it against those same peers is circular in a way none of the other variables are (see "Circularity" below, which explains why #6 and #7 are a milder, disclosed version of this same risk and this one is not). |

## Method

### Feature matrix

565 rows × the approved variables (up to 11, or 10 if density is struck or deferred). Ratios
and derived transforms are computed in Python from the published counts/values at the periods
stated in the table above — never read from a second, pre-computed store. `log` means the
natural logarithm (`numpy.log`, base e). **Every logged variable is strictly positive by
construction** (population, density, enterprise density and property tax base per resident are
all counts or sums of non-negative quantities over a resident population that is always > 0
for a currently-existing commune), so `log(0)` cannot arise from real data. If a future variable
addition ever produces a zero or negative value for a logged column, the run refuses rather
than substituting a floor value (CLAUDE.md rule 13 — a source-schema surprise fails loudly).

### Missing values — no imputation in v1

Any missing value for any approved variable, for any of the 565 communes, **refuses the whole
run** (CLAUDE.md rule 13: never silently coerce or drop rows). There is no imputation logic in
v1. This is enforceable today because the measured coverage above is 565/565 for every
variable that is actually buildable (i.e. everything except density, which is excluded from the
model entirely until its own data exists — a struck or deferred row is not a "missing value
inside the model", it is a variable outside the model).

### Merged communes — native vs. reconstructed

Some of today's 565 communes were formed by a 2019 or 2025 merger. For every variable, the
value used is **the value already published for today's commune** — never a value read from a
predecessor and left unreconciled. Two different mechanisms produce that value, and the spec
states which variable uses which so a builder never has to guess:

- **Native to the current territory** (measured directly against today's map, no reconstruction
  needed): population, the two age shares, unemployment, property tax base per resident,
  population change over 5 years (via the backaggregate mechanism cited under variable 5).
- **Reconstructed onto the current territory from a source that predates it**: the 2021 census
  variables (share of foreign nationals, average household size) and the 2023 income variable
  and the 2023-Q4 enterprise-density variable were all published before some of today's
  communes existed in their current form. `docs/features/geography.md`'s backaggregate
  machinery (§"Growth on current territory", same section cited for variable 5) is the general
  mechanism the pipeline already uses for exactly this: a successor's figure for a pre-merger
  period is the summed (for additive counts) or recomputed-from-sums (for ratios) predecessor
  figures, never an average of predecessor ratios and never left blank. The peer model does not
  invent a second reconstruction mechanism — it consumes whatever `AVG_NET_TAXABLE_INCOME`,
  `SHARE_FOREIGN_NATIONALS`, `AVERAGE_HOUSEHOLD_SIZE` and the enterprise-density ratio already
  resolve to for today's commune, which is already reconciled by that machinery.

### Standardisation — exactly scikit-learn's `StandardScaler`, computed with numpy/pandas

This pipeline does not depend on scikit-learn (the roadmap's own sketch for this block says
"plain scikit-learn"; the maintainer's 2026-09-26 decision overrides that sketch: **numpy and
pandas only**, no new dependency). The standardisation is defined to be numerically identical
to what `sklearn.preprocessing.StandardScaler` would produce, so a builder does not have to
choose an interpretation:

```
z = (x - mean) / std
```

- `mean` is the arithmetic mean over the 565 communes for that variable (or 284/260/18 within a
  `region` pool — standardisation is fit once, nationally; the `region` peer list reuses the
  same national z-scores and only restricts the distance search to the regional pool, so a
  commune's z-score does not change between its `national` and `region` output).
- `std` is the **population** standard deviation, i.e. `ddof=0` (divide by N, not N-1) —
  `numpy.std(x, ddof=0)`, not `numpy.std(x, ddof=1)` and not pandas' default `.std()` (which is
  `ddof=1`). This is the one line in this spec most likely to be implemented wrong by habit,
  because pandas' default disagrees with it.
- A column with `std == 0` (every commune has the same value) **refuses** — dividing by zero
  would produce `inf` or `nan` silently propagating into every distance, and CLAUDE.md rule 13
  forbids exactly that kind of silent corruption. No variable in the approved list is expected
  to have zero variance across 565 communes; if one ever does, that is worth investigating, not
  suppressing.
- ADR 0015's worked example computes this by hand for two variables on a real commune pair and
  a unit test proves the `ddof=0` numpy formula matches `StandardScaler.fit_transform` bit for
  bit on a small fixture, closing the "exactly scikit-learn's" claim rather than asserting it.

### Distance, k, ties, self-exclusion

- **Plain Euclidean distance** on the standardised (z-scored) feature vectors. Equal weights —
  no variable is up- or down-weighted relative to another.
- **k = 10.** Every commune gets exactly ten peers in each list (subject to pool size — Brussels'
  `region` pool is only 18, so a Brussels commune's `region` list has at most 18 candidates to
  choose 10 from, which is still ≥ 10 and therefore unaffected; if a future pool ever had fewer
  than 10 members the run must refuse rather than silently return fewer than 10 peers, since
  that is not a case that occurs today and should not be handled by an unstated convention).
- **Ties broken by NIS code, ascending.** Two communes at exactly equal distance are ordered by
  their numeric NIS code so the output is deterministic without depending on row order.
- **A commune is never its own peer** — excluded from its own candidate pool before ranking,
  not filtered out afterward (which could silently shrink the returned list below 10).

### Optional PCA variant

A second variant, same interface, **off by default**: SVD on the standardised matrix, keeping
the fewest principal components whose cumulative explained variance is ≥ 90%, then Euclidean
distance in that reduced space instead of the full standardised space. No scikit-learn — SVD
via `numpy.linalg.svd`. Every output (payload, CSV row, benchmark) carries a `variant` field
(`"standardised"` or `"pca"`) so the two are never silently mixed. This exists to let the
maintainer measure whether the extra sophistication actually changes who is a peer, not because
v1 defaults to it.

### Mixed periods

The feature matrix mixes 2026 counts, a 2023 income year, a 2023-Q4 enterprise snapshot and a
2021 census — up to five years apart for the oldest variable. This is stated plainly rather than
smoothed over: **every model output lists each variable's own period next to its value**, so a
reader can see that "similar household size" means "similar in a figure last measured in 2021,"
not "similar today." No attempt is made to inflate, extrapolate or otherwise align periods —
doing so would be inventing a number CLAUDE.md rule 36 forbids.

### Similarity score for display

Distances are the model's real output; a 0–100 "similarity score" is a **display-only**
convenience computed from the distance, never stored as if it were itself a measured
similarity. Defined as:

```
score = 100 × (1 − d / d_max)
```

where `d_max` is the largest of the ten distances actually returned **in that same list** (not a
global constant across all 565 communes, and computed separately for `national` and `region` —
never the national list's `d_max` reused for the region list, which could send a region score
below 0 whenever a region peer is farther than the farthest national peer) — chosen over a
rank-based score because it preserves *how much* closer the nearest peer is than the tenth,
information a rank alone throws away, and because clamping to each list's own worst-of-ten keeps
the score inside 0–100 without needing a second, arbitrary ceiling constant. Every place this
score is shown states "for display only; the underlying figure is the standardised Euclidean
distance."

### Per-indicator benchmarks (defined here, built later)

For any municipal indicator (not only the eleven feature variables) and any commune:

- **Peer median** = the median value among the peers (up to 10) that have a real value (not
  missing, not suppressed, not `na`) in the same period as the commune's own value.
- **Position** = the commune's rank among itself plus its peers that have a value in that
  period (so "3rd of 8" is possible when only 7 of 10 peers report).
- **`deviation_pct = (value − median) / median × 100`.**
- **Null, not zero, when:** fewer than 7 of the 10 peers have a value in that period (the whole
  benchmark is withheld), or the peer median is 0 or negative (only `deviation_pct` is withheld —
  see below). A withheld benchmark is not a benchmark of zero deviation (CLAUDE.md rule 26).
- **A zero or negative median withholds only the percentage, not the whole entry.** The peer
  median, position and rank ("nth of m") still mean something — a commune's rank among peers is
  well-defined even when the base is zero or negative — but a percentage against a zero or
  negative median is not shown because it reads backwards. Example, national list, `nis=11002`
  (Antwerpen), `INTERNAL_MIGRATION_NET`, 2025 (`public/data/peers/11002.json`): the commune's
  value is −4,085 against a peer median of −85; naive `deviation_pct` arithmetic would compute
  roughly +4700%, which looks like a huge improvement in the wrong direction, so `deviation_pct`
  is null and the entry carries `deviation_withheld: "median_negative"` (or `"median_zero"` when
  the median is exactly 0) instead.
- Suppressed and `na` peers are **excluded from the median's inputs entirely** — never treated
  as zero, which would pull every median toward zero for indicators (like police rates on
  merged communes) with real suppression.
- **This peer median is a ranking statistic about the ten peers, not an aggregate of them.**
  CLAUDE.md's aggregation rule ("ratios recomputed from sums, never averaged across
  geographies") governs how a *province or region total* is built from its constituent
  communes' own additive components — it says nothing about, and does not forbid, taking the
  median of ten already-published commune-level values to answer "where does this commune sit
  relative to its structural peers." These are different questions with different correct
  answers; the peer median must never be labelled as if it answered the first one. Every place
  this appears on a page, the wording is **"median of comparable communes,"** never "average
  of," "total for," or any phrasing that could be read as an aggregate.

### Circularity — any of the eleven selection variables can also be a headline

The flagged set is derived, not hand-maintained: an indicator id is flagged when it IS a
selection variable's own published indicator or IS one of the raw numerators named in that
variable's `description` in `src/analytics/peers.py`'s `VARIABLES` — never a denominator such
as `FISCAL_NBR_NON_ZERO_INC` or `HOUSEHOLDS_PRIVATE`, which the peer is not selected "on" in the
same sense — so adding or changing a variable in `VARIABLES` is the only way to change which
indicators carry the caveat.

`AVG_NET_TAXABLE_INCOME` (variable 6) and `UNEMPLOYMENT_RATE_INSURED` (variable 7) are the two
most obvious cases, but the same risk applies to **any of the eleven selection variables** that
is also published elsewhere as its own indicator — the two age shares, population change over
5 years, share of foreign nationals and average household size are all published commune-level
indicators in their own right, not only inputs to this model. Whenever a peer benchmark is shown
for a commune's own value of *any* of the eleven, the display carries the note **"peers were
chosen partly on this figure"** — the deviation is real and correctly computed, but a reader
should understand that a commune's peers were, in part, chosen for being close on that same
figure, which mechanically compresses how large a deviation among peers can ever look for it.
This is disclosed, not hidden. By contrast, Block P's finance/debt benchmarks (WalStat's
`MUN_DEBT_*`, `MUN_REVENUE_*`, `MUN_EXPENDITURE_*` and the communal income-tax surcharge) select
peers from a model that contains **no finance variable at all** — true by construction, since
none of the eleven candidates above is a finance figure — so those benchmarks carry no such
caveat.

### Stability — a stand-in until a second data vintage exists

Every store behind this model (population, income, census) currently holds **one vintage
only** (checked 2026-09-26 against the `vintage` column of `data/*_observations.csv`), so the
model cannot yet be tested by re-running it against a corrected later vintage of the same data —
the standard way to test whether a model's groupings are an artefact of measurement noise. The
stand-in: recompute the model with each variable shifted one period earlier where an earlier
period exists for that variable (e.g. `POPULATION_BY_COMMUNE` 2025 instead of 2026), and report
the **mean Jaccard overlap** of the national top-10 peer sets between the two runs, across all
565 communes. Proposed acceptance threshold: **mean overlap ≥ 0.5** (on average, at least 5 of
10 peers survive a one-period shift), with **the ten communes with the lowest overlap listed by
name** so a reviewer can look at exactly which peer sets are least stable rather than trusting a
single summary number. This is stated plainly as a stand-in for a real vintage-to-vintage
stability test, valid only until a second vintage of these stores exists.

### Outputs (defined here; built in a later PR)

- `public/data/metadata/peers.json` — `model_version`, `variant`, per-variable metadata (period,
  transform, mean, std), and per commune: raw features, z-scores, `national` top-10 (nis,
  distance, rank) and `region` top-10 (nis, distance, rank).
- `data/peers.csv` — the same national/region peer pairs, flattened, for download.
- `public/data/peers/<nis>.json` — the per-commune benchmark payload (peer median, position,
  deviation per indicator).
- All three: **sorted keys, compact serialisation, no embedded clock or timestamp**, so
  identical inputs keep producing byte-identical output on re-run (CLAUDE.md rule 35, the same
  guarantee the existing exporters give and `make all`'s byte-identical rebuild test checks).

### Versioning

`PEER_MODEL_VERSION = "1.0.0-rc.1"` until the maintainer completes his 15-commune manual review
and a red-team audit finds nothing that changes the variable list or method, at which point it
becomes `"1.0.0"`. The version string is stamped on every row of `peers.csv`, every commune
entry in `peers.json`, and every per-commune benchmark payload. **Any change to the variable
list, a variable's period, or the method (standardisation, distance, k, PCA threshold) bumps
the version.** A page or report generated under one version must never be silently reread as if
generated under another.

## What this model does NOT claim

- **No causality.** A commune's peers being high-income does not mean anything about why the
  commune itself is high- or low-income.
- **No better-or-worse.** The model ranks nothing as a better place to live, invest, or govern.
  It measures structural distance on the listed variables only.
- **No ranking of communes against each other in general.** Peers are a *set*, not a *league
  table*; the position/deviation figures rank a commune only against its own ten peers on one
  indicator at a time, never communes against the full 565.
- **"Comparable" is scoped to exactly these variables.** A commune could be a poor real-world
  comparison on some dimension this model does not measure (a tourism economy, a university
  town, a commuter-belt effect) and still be a valid "peer" under this definition. The model
  says what it measures and nothing more.
