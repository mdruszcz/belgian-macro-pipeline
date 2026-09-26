# ADR 0014 — Rates per 1,000 residents: births, deaths, internal and international migration

Date: 2026-09-26
Status: Proposed — awaiting the maintainer's approval
Required by: `docs/features/population_movement.md` §"Rates: out of scope, pending an ADR"
(L187-190). Extends [ADR 0003](0003-aggregation-rule.md) (aggregation rule). Governed by
CLAUDE.md rules 5, 6, 19.

## Context

The maintainer asked on 2026-09-26 for six rates that turn the raw Statbel population-movement
counts and balances already published (BIRTHS, DEATHS, INTERNAL_MIGRATION_IN,
INTERNAL_MIGRATION_OUT, INTERNAL_MIGRATION_NET, INTERNATIONAL_MIGRATION_NET) into per-1,000-
residents rates, so a reader can compare a small commune's births or migration against a large
one's without doing the division themselves. `docs/features/population_movement.md`'s own
"Rates: out of scope, pending an ADR" section named this exact gap when the raw counts were
added on 2026-09-23, and CLAUDE.md rule 19 requires an ADR before any change that touches the
analytical-formula surface (`src/analytics/derived.py`, `src/analytics/engine.py`). This record
is that ADR.

## Decision

**1. Formula: `events / population × 1,000`, one denominator, no invented mid-year figure.**

`per_thousand(value, population)`, next to `per_capita` in `src/analytics/derived.py`: null in
→ null out, `population` null or 0 → null, otherwise `value / population * 1000.0`, no
rounding (rounding happens once, at export, like every other derived function here).
`population` is POPULATION_BY_COMMUNE at THE SAME YEAR as the event count, i.e. residents on
1 January of that year. The derived-indicator engine reads every multi-input function's
arguments at one shared period (`src/analytics/engine.py`'s `_apply`, MULTI_INPUT branch), so
there is no mechanism here for a mid-year average, and none is invented: Statbel's own
published crude rates (birth rate, death rate, net migration rate) use the mid-year
population as their denominator, which this pipeline does not hold as a separate figure. Our
rate can therefore differ slightly from Statbel's own published crude rate for the same
commune and year. That is accepted rather than worked around, because the alternative is
inventing a mid-year population this pipeline has no source for, which CLAUDE.md rule 36
forbids outright, and because one already-published, already-period-correct denominator
(1 January population, the same one the raw counts themselves resolve against) is a smaller and
more auditable departure from Statbel's own number than a fabricated interpolation would be.

**2. Six indicators, one function.**

| id | inputs |
|---|---|
| `BIRTH_RATE_PER_1000` | BIRTHS, POPULATION_BY_COMMUNE |
| `DEATH_RATE_PER_1000` | DEATHS, POPULATION_BY_COMMUNE |
| `INTERNAL_MIGRATION_IN_RATE_PER_1000` | INTERNAL_MIGRATION_IN, POPULATION_BY_COMMUNE |
| `INTERNAL_MIGRATION_OUT_RATE_PER_1000` | INTERNAL_MIGRATION_OUT, POPULATION_BY_COMMUNE |
| `INTERNAL_MIGRATION_NET_RATE_PER_1000` (signed) | INTERNAL_MIGRATION_NET, POPULATION_BY_COMMUNE |
| `INTERNATIONAL_MIGRATION_NET_RATE_PER_1000` (signed) | INTERNATIONAL_MIGRATION_NET, POPULATION_BY_COMMUNE |

The two NET rates carry the sign of their input balance through unchanged: `per_thousand` does
not clamp a negative numerator, so a commune losing more residents to internal migration than
it gains gets a negative rate, exactly as its NET balance is itself negative.

**3. Coverage: only where both inputs exist for the same commune and year → 2017-2025.**

POPULATION_BY_COMMUNE starts in 2017 (committed store, verified 2026-09-26: 2017-2026 with
nothing in 2016); the movement counts start in 1992 but resolve per-row against the commune map
in effect at the start of their own year (`docs/features/population_movement.md` §Geography). A
rate needs both, so it exists only 2017-2025. Within that range, 18 of the 31 communes merged in
2019 have no movement row for 2018 (verified: 565 communes with a BIRTHS row in 2017, 547 in
2018), and 13 of the 31 communes merged in 2025 have no movement row for 2024 (565 in 2023, 552
in 2024) — the transition sheet carries the post-merger codes the following year — so every one
of the six rates is **null**, never 0, for exactly those cells (CLAUDE.md rule 26: a withheld
figure is not a measured zero). The engine already enforces this without extra
code: `compute()` iterates `result.cells(inputs[0])`, so a (geo_id, period) cell absent for the
numerator never produces a row at all.

**4. Aggregation: sum both sides, recompute, never average a commune rate.**

Province, region and Belgium figures are built by summing the numerator (events) and the
denominator (population) separately over the geographies that existed in that period, then
applying `per_thousand` to the two sums — sum(events) / sum(population) × 1,000 — exactly the
existing machinery in `src/analytics/aggregate.py` (`RECOMPUTE` method, driven by the derived
config's own `function` name) and `src/analytics/backaggregate.py` (predecessor reconstruction
across a merger) already apply to `per_capita` and `share_of_total`. `per_thousand` is added to
both modules' `RECOMPUTABLE_FUNCTIONS` sets for exactly this reason: it is arithmetically the
same shape as `per_capita`; only the multiplier differs. A province rate is never the mean of
its communes' own rates, which would let Herstappe's 36 births and Antwerp's 2,900 births count
equally.

**5. Unit, decimals, direction.**

`unit: per_mille`, `decimals: 1`, `preferred_direction: contextual` for all six. `per_mille`
already exists in this pipeline's unit vocabulary (`config/indicators/POPULATION_GROWTH_RATE_EUROPE.yaml`)
and `assets/commune_map.js` already renders it as the literal `‰` glyph with no i18n text
lookup needed, so no display-layer change is required. `contextual` because a high or low
birth, death or migration rate is a fact about a commune, not something a profile can call good
or bad on its own — exactly the same reasoning already applied to the raw BIRTHS, DEATHS,
INTERNAL_MIGRATION_* and INTERNATIONAL_MIGRATION_NET configs.

## Worked example — Boechout (NIS 11004), 2025

Read live from the committed history export, 2026-09-26:

- `data/communes_history/population_movement.csv`: BIRTHS 2025 = **125**, DEATHS 2025 =
  **192**, INTERNAL_MIGRATION_IN 2025 = **838**, INTERNAL_MIGRATION_OUT 2025 = **771**,
  INTERNAL_MIGRATION_NET 2025 = **67**, INTERNATIONAL_MIGRATION_NET 2025 = **26**.
- `data/communes_history/population.csv`: POPULATION_BY_COMMUNE 2025 = **14,084**.

| Rate | Arithmetic | Result |
|---|---|---|
| BIRTH_RATE_PER_1000 | 125 / 14,084 × 1,000 | 8.875319… → **8.9‰** |
| DEATH_RATE_PER_1000 | 192 / 14,084 × 1,000 | 13.632491… → **13.6‰** |
| INTERNAL_MIGRATION_IN_RATE_PER_1000 | 838 / 14,084 × 1,000 | 59.500142… → **59.5‰** |
| INTERNAL_MIGRATION_OUT_RATE_PER_1000 | 771 / 14,084 × 1,000 | 54.742971… → **54.7‰** |
| INTERNAL_MIGRATION_NET_RATE_PER_1000 | 67 / 14,084 × 1,000 | 4.757171… → **4.8‰** |
| INTERNATIONAL_MIGRATION_NET_RATE_PER_1000 | 26 / 14,084 × 1,000 | 1.846066… → **1.8‰** |

Note INTERNAL_MIGRATION_IN_RATE_PER_1000 minus INTERNAL_MIGRATION_OUT_RATE_PER_1000 = 59.500142
− 54.742971 = 4.757171, matching INTERNAL_MIGRATION_NET_RATE_PER_1000 exactly (both sides of the
subtraction are linear in the same denominator, so this holds for every commune and year, not
just Boechout's).

## Consequences

- A builder can implement and test every one of the six rates from this record alone: the
  formula, the null-not-zero coverage rule, the aggregation rule and the unit are all stated as
  numbers.
- The rate can read slightly differently from Statbel's own published crude rate for the same
  commune and year, because the denominators differ (1 January population here, mid-year
  population there). This is stated in the block's own definition text, not hidden.
- No new data source and no new catalogue row: all six inputs are already-catalogued Statbel
  indicators (Wave 3, `docs/data_catalog.md`, approved 2026-09-23).

## Risks

- **A reader could mistake our rate for Statbel's own published crude rate** and flag a
  discrepancy. Decision 1's plain statement of the denominator difference, repeated in each
  config's `definition`, is the mitigation.
- **The null gap for the 18 communes merged in 2019 (2018) and the 13 merged in 2025 (2024)
  could look like a bug** rather than the documented consequence of the transition-sheet timing
  already described in `docs/features/population_movement.md`. Tests assert null, not 0, for
  exactly these cells so a future edit cannot quietly convert one into the other.
- **A future aggregate consumer could average commune rates instead of recomputing from sums**,
  reintroducing the exact error ADR 0003 exists to prevent. `RECOMPUTABLE_FUNCTIONS` in both
  `aggregate.py` and `backaggregate.py` is the single point of truth for which functions may be
  recomputed at a higher geography; adding `per_thousand` there is the whole mechanism, and it
  is covered by a hand-computed two-commune test.
