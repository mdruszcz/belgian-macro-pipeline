# Feature: derived-indicator engine (Block G)

Status: spec
Issue: (Block G — Derived-indicator engine, docs/steps)
Branch: feat/block-g-spec

## Problem

`src/analytics/` is empty. Every figure in the product today is a raw source observation. The
roadmap's estimate is that ~150 raw indicators become ~400 analytical ones with no additional data
collection — the best value-per-hour in the project — but only if the derivation rules are decided
once, in writing, rather than per-function by whoever writes the next one. Null handling in
particular produces silently inconsistent results across the site when each function invents its own.

## The hard rule

**Derived values are computed, never stored in `observations`.** This is CONTROL G, and it is not a
style preference: once a derived value is persisted as a source observation, a formula fix stops
propagating and the database holds two contradicting truths.

Derived values are computed at **export** time and written into the published payloads
(`data/*_export.csv`, later Block J's per-commune JSON). They never round-trip back into
`observations`, and no derived `indicator_id` ever appears in that table. A test asserts this.

## Non-goals

- Peer-group comparison (Block M), signals and anomaly ranking (Block Q), forecasting (Block R).
  `z_score` and `percentile` here operate on *geographic* peer sets (all communes, or all communes in
  a region), not the structural peer model.
- Any LLM involvement. Every function is deterministic Python with hand-computed unit tests.

---

## What is actually computable today

Written from the real committed data, not from what the roadmap hopes for, because three of these
constraints change what the functions must do.

| Data | Reality |
|---|---|
| `POPULATION_BY_COMMUNE` + 3 age bands | 11 annual periods 2016–2026, municipal |
| `LOCAL_UNITS_BY_COMMUNE` | **exactly one period** (2023-Q4) |
| 30 national indicators | up to 216 periods, but **`geo_id = 'be:country'` only** |
| National population | **does not exist** |

Three consequences the engine must handle rather than assume away:

1. **`LOCAL_UNITS_BY_COMMUNE` has one period**, so `growth_rate`, `cagr`, `five_year_change` and
   `index_base_100` are undefined for it. They must return null, not zero and not an error.
2. **`per_capita` is municipal-only.** There is no national population indicator, so a national
   per-capita request has no denominator and must fail loudly rather than silently pick one.
3. **Percentile and z-score are municipal-only**, because every national indicator has a single
   geography. A peer set of one is not a peer set.

### The peer set changes size by period

The population store holds historical predecessor communes for pre-merger years — correctly, since
`resolve_geo` is period-aware. So the number of communes that existed depends on the year:

| Period | Communes |
|---|---|
| 2016–2018 | 589 |
| 2019–2024 | 581 |
| 2025–2026 | 565 |

**A percentile or z-score must be computed against the communes that existed in that period**, never
against today's 565. Ranking a 2016 value against the 2026 commune set would use a denominator 24
too small and silently exclude real communes. This is precisely the case the roadmap warns about:
"a commune arguing about its rank will notice."

Corollary: a commune that did not exist in a period has no value, and therefore no rank, for that
period. Puurs-Sint-Amands has no 2018 percentile; its predecessors do.

---

## Function catalogue

Ten functions in `src/analytics/derived.py`. Every one is a pure function of its inputs.

| Function | Definition | Output unit |
|---|---|---|
| `growth_rate(x, years=1)` | `(v_t − v_{t−n}) / v_{t−n} × 100` | percent |
| `cagr(x, years)` | `((v_t / v_{t−n})^(1/years) − 1) × 100` | percent per year |
| `five_year_change(x)` | `growth_rate(x, years=5)` | percent |
| `per_capita(x, denominator)` | `v / population`, same geo and period | x's unit per person |
| `share_of_total(x, total)` | `v / v_total × 100` | percent |
| `index_base_100(x, base_period)` | `v_t / v_base × 100` | index |
| `dependency_ratio()` | `(age_0_14 + age_65_plus) / age_15_64 × 100` | percent |
| `z_score(x, scope)` | `(v − mean(peers)) / stdev(peers)` | standard deviations |
| `percentile(x, scope)` | see below | percent |
| `regional_share(x)` | `v_commune / v_region × 100` | percent |

**Periods are specified in years, not in period counts.** `five_year_change` on a quarterly series
must span 20 quarters, not 5. Taking `years` and converting via the indicator's `frequency` removes
a whole class of silent frequency bugs.

### Percentile — the contentious one

**Definition: `percentile = 100 × (below + 0.5 × equal) / N`**, where `below` and `equal` count the
peer set, `N` is its size, and the subject commune is included in `N`.

This is the "percentile rank" a lay reader means by *"we're in the 80th percentile"* — 80% of
communes are below us. It is chosen over interpolated quantiles (numpy's default `linear` method)
deliberately:

- It is explainable in one sentence to a non-analyst, which matters because the audience is municipal
  management, not statisticians.
- It is stable and reproducible from the published data — anyone can recount.
- The `0.5 × equal` term means tied communes get the same rank, which they must; a definition that
  gives two identical values different ranks is indefensible in a meeting.

Rank-based and interpolated percentiles differ by several points across 565 items, so this choice is
recorded here and must be shown on screen next to the figure (Block L requires the reference year and
the peer-set size alongside it).

**Scope is explicit**, never implied: `national` (all communes valid in that period) or `regional`
(all communes in the same region, valid in that period).

### z-score

Population standard deviation (`ddof=0`), not sample. We hold the entire peer set, not a sample from
it. Null when the peer set has fewer than 2 members or zero variance.

### Aggregation across geographies is gated on `is_additive`

`regional_share` and any `total` computed by summing communes require the indicator's `is_additive`
flag. Counts (population, local units) are additive. Indices, rates and per-capita figures are not —
summing GDP indices across regions is meaningless. **Requesting an aggregate of a non-additive
indicator raises**, it does not silently produce a number.

### Per-capita denominators

The denominator is named explicitly in the config, never inferred. It defaults to
`POPULATION_BY_COMMUNE` for municipal indicators and is matched on **the same geography and the same
period** as the numerator. If the denominator is missing for that cell, the result is null — never a
neighbouring year's population, and never a national figure scaled down.

---

## Null propagation and division

One policy, applied by every function, because inconsistency here is invisible and corrosive:

- **Any null input produces a null output.** Never zero, never dropped, never carried forward.
- **Division by zero produces null**, not infinity and not an exception. A commune with zero
  working-age population has no dependency ratio; that is a fact about the commune, not an error.
- **`cagr` requires both endpoints strictly positive.** A non-positive endpoint makes the root
  undefined or complex; return null.
- Null is distinct from `suppressed`. A suppressed source value (Statbel small-cell suppression)
  stays `suppressed` in the status enum and derives to null — it must never surface as 0, which would
  make a quiet commune look like a collapsed one.

## Rounding

- **Intermediate values are never rounded.** Rounding inside a chain compounds; a per-capita figure
  feeding a growth rate must carry full float precision.
- **Rounding happens once, at export**, to the indicator config's `decimals`.
- Defaults when unspecified: 1 decimal for percent-like outputs, 2 for ratios and per-capita, 0 for
  counts.

## Formula grammar

Derived indicators are declared in `config/indicators/derived/*.yaml`, following Block B's rule that
adding analysis is config, not code:

```yaml
id: POPULATION_GROWTH_5Y
name:
  en: Population change over 5 years
  fr: Évolution de la population sur 5 ans
  nl: Bevolkingsverandering over 5 jaar
unit: percent
frequency: A
geo_levels: [municipal]
preferred_direction: contextual
derived:
  function: growth_rate
  inputs: [POPULATION_BY_COMMUNE]
  args:
    years: 5
```

`derived.function` must name a function in the catalogue; `derived.inputs` names indicator ids, which
may themselves be derived. An unknown function or an unknown input id fails validation at CI time,
not at export time — the same JSON Schema gate `config/indicators/*.yaml` already passes through
`scripts/validate_config.py`.

**`preferred_direction` is carried on the derived indicator itself and is never inferred from its
inputs.** Population is `contextual`; population *decline* in a shrinking commune is not automatically
bad. This field drives arrows and phrasing later (Block U), and inferring it is how
"unemployment improved to 12%" gets published.

Percentiles are computed on the **raw value**; `preferred_direction` affects only how the result is
described, never the arithmetic. Inverting the percentile for a `lower_is_better` indicator would
make the published number disagree with the published rank.

## Dependency resolution

Derived-of-derived is expected (a per-capita figure feeding a growth rate). The engine builds a graph
over `derived.inputs`, topologically sorts it, and evaluates in order.

**Cycles raise, naming the full cycle.** Without ordering, results depend on evaluation order and
produce nulls that move when unrelated config changes — the roadmap calls this "maddening to debug",
correctly.

## Reading from two stores

Population lives in `data/population_observations.csv`, not in the database
([ADR 0002](../decisions/0002-split-committed-stores.md)). Every function that touches population —
`per_capita`, `dependency_ratio`, and any municipal percentile — must therefore read from both
stores. The engine takes an already-assembled observation set rather than opening files itself, so
the functions stay pure and testable, and `scripts/export_communes_csv.py`'s existing
`--extra-observations` merge is reused rather than duplicated.

## Tests

Per the roadmap: "Testing a formula against its own implementation proves nothing. Only an
independently computed number is a test."

- Every function gets **hand-computed expected values** on small fixtures, asserted to a fixed
  tolerance. The expected numbers are written out in the test, with the arithmetic shown in a comment.
- Null propagation and division-by-zero are tested per function, not once globally.
- `percentile` is tested against a hand-ranked set including **ties**, since tie handling is the part
  most likely to be wrong.
- The peer-set-by-period rule gets a named test: a 2016 percentile must use 589 communes, a 2026 one
  565.
- Cycle detection gets a test asserting the error names the cycle.
- **CONTROL G gets a test**: no derived `indicator_id` appears in `observations`.

## Open questions for the maintainer

- **`preferred_direction` is inconsistent across country variants** in existing config (GDP is
  `higher_is_better` for Belgium, `neutral` for DE/EA/ES/FR/NL) — reported in
  [the Block F label-honesty review](../reviews/2026-09-06-label-honesty.md) and still undecided. It
  does not block this block, but Block L's comparison work will read that field.
- **Which derived indicators to actually publish first.** This spec defines the machinery; the choice
  of which ~20 derived figures earn a place on a commune page is a commercial judgment, and per
  `CLAUDE.md` that is the maintainer's call, not the agent's.
