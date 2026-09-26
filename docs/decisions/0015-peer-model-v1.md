# ADR 0015 — Peer model v1: variable list, standardisation and distance metric

Date: 2026-09-26
Status: Proposed — awaiting the maintainer's approval
Required by: roadmap Block M's first step, "[SPEC] Write /docs/features/peer_model.md" — "It
has to be a document before it is an algorithm." Governed by CLAUDE.md rules 5, 6, 13, 19, 26,
35, 36.

## Context

The roadmap's Block M asks for a peer model: for each of the 565 Belgian communes, the ten
communes most structurally similar to it, becoming the basis of every later "versus comparable
communes" feature (per-indicator peer benchmarks, a commune-page block, eventually a paid
finance-benchmark product in Block P). The roadmap's own sketch for this step names a rough
variable list ("population, density, income, age structure, employment, urbanization, housing,
enterprise density, social indicators, tax base") and a method ("plain scikit-learn,
deterministic, seeded") but leaves both undecided at the level a builder could implement
without re-deciding — exactly the gap this record closes.

CLAUDE.md rule 19 requires an ADR before anything touching an analytical formula; a peer model
that will feed benchmarked figures onto public commune pages is squarely that. This record and
its companion spec, `docs/features/peer_model.md`, together are that ADR: the spec is the
document a builder implements from, and this record is the decision trail — why each choice was
made, and the worked arithmetic proving the standardisation is what it claims to be.

The maintainer took several decisions on this on 2026-09-26, recorded as decisions below.

## Decision

**1. Two peer lists from one model — `national` and `region` — with measured, not assumed,
pool sizes.**

A `national` list (pool = the other 564 communes) and a `region` list (pool = the other
communes of the same region) come from the same standardised feature matrix and the same
distances; `region` only restricts which candidates are eligible, it does not refit the
standardisation. Region pool sizes were measured against `config/geography/geographies.csv`
on 2026-09-26 (municipalities with an empty `valid_to`, region resolved by walking
`parent_geo_id` up to a `be:reg:*` row, the same logic
`scripts/export_commune_typology.py`'s `municipalities_valid_on` and `region_of` helpers
already implement): **Flanders 285, Wallonia 261, Brussels-Capital 19** — total 565. This
differs from the round 300/262/19 figure carried informally before this measurement; the
correct split is independently confirmed by `docs/features/comparison.md`'s own 2026-09-06
percentile work, which recorded the identical 285/261/19 split for a different purpose. Two
independent measurements agreeing is why this record states 285/261/19 as fact rather than as
one script's output taken on faith.

**2. numpy/pandas only — no scikit-learn — with the standardisation defined to be numerically
identical to `StandardScaler`.**

The roadmap's own sketch says "plain scikit-learn." The maintainer overrides that on
2026-09-26: this pipeline has no scikit-learn dependency today and none is added for this
model. Instead, the spec defines the standardisation precisely enough that a numpy
implementation is provably equivalent: `z = (x - mean) / std`, `std` computed with `ddof=0`
(population standard deviation, dividing by N) — the exact formula
`sklearn.preprocessing.StandardScaler` uses internally. The distance is plain Euclidean on the
z-scored matrix. A later unit test proves this numpy formula reproduces
`StandardScaler.fit_transform` bit-for-bit on a small hand-built fixture, so "exactly
scikit-learn's StandardScaler" is a tested equivalence, not an assertion.

**3. The variable list — eleven candidates, each independently measured, presented for the
maintainer to keep or strike.**

`docs/features/peer_model.md` §"The variable list" lists all eleven candidates with their
indicator id, exact definition, period, real measured coverage out of 565, transform and a
one-sentence justification. Two coverage numbers differ from an earlier draft and are corrected
here rather than carried forward silently: `LOCAL_UNITS_BY_COMMUNE` is confirmed frozen at
2023-Q4 (565/565, unchanged since) and `MUN_CADASTRAL_INCOME_TOTAL` is confirmed at 2026
(565/565) — both measured directly against `data/communes_history.csv` and
`data/communes_history/spf_agdp_patrimony.csv` on 2026-09-26, matching what was proposed.
**Density (variable 2) cannot be built today**: `config/geography/geographies.csv` carries no
`area_km2` field for any commune, so this variable's coverage is 0/565, not "null for all 565"
as if the field existed and were merely empty. Building it needs one new committed file,
`config/geography/commune_area_km2.csv`, derived once from the Statbel statistical-sector
geometry already catalogued for `data/geo/communes.geojson` (`docs/data_catalog.md`,
"Boundary geometry — LOADED 2026-09-07") — no new data source, because it is the same already-
approved Statbel file, only measured for area instead of dissolved for a map outline. That file
is not built in this PR; it is a prerequisite for shipping variable 2 in v1, not for the spec.

**4. Missing data refuses the run; no imputation in v1.**

Every one of the ten buildable variables (everything except density) measures at 565/565
today, so "refuse on any missing value" (CLAUDE.md rule 13) costs nothing now and prevents a
silent 564-or-fewer model appearing later without anyone deciding that was acceptable.

**5. Equal weights, k = 10, ties by NIS ascending, a commune is never its own peer.**

No variable is weighted more than another. Exactly ten peers per list per commune (subject to
pool size, which is ≥ 18 everywhere today, so this never binds). Ties at equal distance are
broken by NIS code, ascending, for a deterministic order that does not depend on input row
order. A commune's own row is removed from its candidate pool before ranking.

**6. Circularity is disclosed, not hidden, for income and unemployment.**

`AVG_NET_TAXABLE_INCOME` and `UNEMPLOYMENT_RATE_INSURED` are both selection variables and
headline indicators elsewhere on the site. Any peer-benchmark display of either variable's own
deviation carries the note "peers were chosen partly on this figure." Block P's finance/debt
benchmarks select from a model with zero finance variables, so they carry no such note — true
by construction, not by later patching.

**7. The peer median is a ranking statistic, not an aggregate — CLAUDE.md's aggregation rule
does not apply to it.**

CLAUDE.md's rule that "a ratio is recomputed from summed additive components, never averaged
across geographies" governs building a province, region or national **total** from its
constituent communes. Taking the median of ten already-published commune-level values to
answer "where does this commune sit relative to ten structurally similar ones" is a different
question — a ranking among peers, not a total over them — and is not forbidden by that rule.
The spec requires every such figure to be worded "median of comparable communes," never
"average" or "total," so this distinction is visible on the page, not just in this record.

## Worked example — Boechout (NIS 11004) and Oosterzele (NIS 44052)

Read live from the committed data on 2026-09-26, computed here exactly as the spec's standard-
isation formula requires, over all 565 communes of `POPULATION_BY_COMMUNE` 2026 and
`FISCAL_TOT_NET_TAXABLE_INC` / `FISCAL_NBR_NON_ZERO_INC` 2023:

- `data/communes_history/population.csv`: POPULATION_BY_COMMUNE 2026 — Boechout **14,106**,
  Oosterzele **14,150**. POPULATION_AGE_65_PLUS 2026 — Boechout **3,440**, Oosterzele
  **3,054**.
- `data/communes_history/fiscal_income.csv`, 2023: Boechout FISCAL_TOT_NET_TAXABLE_INC =
  **EUR 399,803,426.00**, FISCAL_NBR_NON_ZERO_INC = **8,113** returns; average net taxable
  income per return = 399,803,426.00 / 8,113 = **EUR 49,279.36**. Oosterzele:
  FISCAL_TOT_NET_TAXABLE_INC = **EUR 402,795,389.61**, FISCAL_NBR_NON_ZERO_INC = **8,131**
  returns; average = 402,795,389.61 / 8,131 = **EUR 49,538.24**.

Two communes chosen because they sit almost on top of each other by population (14,106 vs.
14,150, a difference of 44 residents) and by average income (EUR 49,279 vs. EUR 49,538) — a
useful pair for showing the arithmetic precisely because a reader can sanity-check that "close
in the raw numbers" and "close in z-score" agree.

Over the 565 communes, measured mean and population standard deviation (`ddof=0`) for the three
variables shown:

| Variable | Mean (565 communes) | Std, ddof=0 |
|---|---|---|
| `log(POPULATION_BY_COMMUNE)`, 2026 | 9.501990 | 0.896735 |
| Share aged 65+ (%), 2026 | 21.671207 | 3.642863 |
| `AVG_NET_TAXABLE_INCOME`, 2023 | 41,554.4277 | 5,937.0082 |

Boechout's and Oosterzele's own values and z-scores:

| Variable | Boechout value | Boechout z | Oosterzele value | Oosterzele z |
|---|---|---|---|---|
| `log(population)` | ln(14,106) = 9.554356 | (9.554356 − 9.501990) / 0.896735 = **0.05840** | ln(14,150) = 9.557470 | (9.557470 − 9.501990) / 0.896735 = **0.06187** |
| Share aged 65+ | 3,440 / 14,106 × 100 = 24.386786% | (24.386786 − 21.671207) / 3.642863 = **0.74545** | 3,054 / 14,150 × 100 = 21.583039% | (21.583039 − 21.671207) / 3.642863 = **−0.02420** |
| Avg. net taxable income | 49,279.357328 | (49,279.357328 − 41,554.4277) / 5,937.0082 = **1.30115** | 49,538.235101 | (49,538.235101 − 41,554.4277) / 5,937.0082 = **1.34475** |

This pair's distance contribution from just these three variables (the full model would include
all ten or eleven):

```
d² = (0.05840 − 0.06187)² + (0.74545 − (−0.02420))² + (1.30115 − 1.34475)²
   = (−0.00347)² + (0.76965)² + (−0.04360)²
   = 0.0000120 + 0.5924  + 0.0019
   = 0.5943
d  = √0.5943 ≈ 0.7709
```

Boechout and Oosterzele are near-identical in size (z-distance ~0.003) and in income
(z-distance ~0.044), and the pair's whole three-variable distance is driven almost entirely by
the age-structure difference (Boechout is noticeably older, z = 0.745 vs. −0.024, a difference
of 0.770) — exactly the kind of decomposition the standardised-Euclidean method is meant to
make legible, which a raw, unstandardised distance on population, percent and euros could not
show at all (the income figures alone, in raw euros, would swamp both other variables by
orders of magnitude).

## Consequences

- A builder can implement the feature matrix, standardisation and distance calculation from
  `docs/features/peer_model.md` alone once the maintainer has struck or kept each variable —
  every id, period, transform and edge case (missing data, zero-std column, ties, self-
  exclusion, mixed periods) is stated as a number or a named mechanism, not left to judgement.
- Density (variable 2) is written into the spec and this record so the *interface* is decided
  now, but is not buildable until `config/geography/commune_area_km2.csv` exists. Shipping v1
  without it (ten variables) or waiting for it (eleven) is the maintainer's call, not a
  technical constraint either way.
- No new dependency (no scikit-learn) and no new data source (density reuses an already-
  approved geometry file; nothing else in the list needs a new source at all).

## Risks

- **The maintainer may strike enough variables that the model stops discriminating.** Losing
  income or unemployment (the two most requested comparison axes) would leave a demographically
  focused rather than economically focused model; the spec does not gate against this because
  the variable list is explicitly his call, not a threshold this document should enforce.
- **A future builder could implement `std()` with pandas' default `ddof=1`** instead of the
  `ddof=0` this record specifies, silently producing a different, non-`StandardScaler`-
  equivalent result that still runs without error. The required equivalence test (Decision 2)
  is the guard; it must be written before any other peer-model code, not after.
- **The stability stand-in (period-shift Jaccard overlap) is not a real stability test** and
  should not be reported to a client as one — it exists only because every input store has one
  vintage today. `docs/features/peer_model.md` §"Stability" states this limitation plainly; it
  must not be dropped when the section is eventually implemented.
- **A reader could mistake the peer median for an aggregate** (a total or average "for the
  peer group") rather than a ranking statistic about ten specific communes. Decision 7's
  required wording ("median of comparable communes") is the mitigation; it must appear on every
  benchmark display, not only in this record.
