# ADR 0003 — Aggregates are summed and recomputed, never averaged or population-weighted

Date: 2026-09-06
Status: accepted
Supersedes: the line *"National aggregates are population-weighted unless the indicator config says
otherwise. Document any exception."* in `claude.md`, replaced on the maintainer's approval. The
schema frozen by [ADR 0001](0001-data-model.md) is unchanged; so is [ADR 0002](0002-split-committed-stores.md).

## Context

Block L needs a province, region and Belgium figure beside every commune figure. `claude.md`
instructed that national aggregates be population-weighted. Before implementing that, the three
candidate methods were measured against the real committed store.

`AVG_NET_TAXABLE_INCOME`, Belgium, 2023, over all 581 communes that existed in that period:

| Method | Result | Error |
|---|---|---|
| Unweighted mean of the 581 commune means | €41,613.37 | **+€1,487.67 (+3.71%)** |
| **Population-weighted** mean of commune means | €40,108.32 | −€17.38 (−0.04%) |
| `Σ FISCAL_TOT_NET_TAXABLE_INC / Σ FISCAL_NBR_NON_ZERO_INC` | **€40,125.70** | — (definitionally correct) |

`DEPENDENCY_RATIO`, Belgium, 2026:

| Method | Result | Error |
|---|---|---|
| Unweighted mean of commune ratios | 59.6256% | **+2.1720 pp** |
| **Population-weighted** | 57.8659% | **+0.4123 pp** |
| `(Σ age_0_14 + Σ age_65_plus) / Σ age_15_64` | **57.4536%** | — (definitionally correct) |

The unweighted mean is badly wrong: it gives Herstappe (78 residents) the same weight as Antwerp
(565,615), and would publish Belgian average income €1,488 too high.

**Population-weighting is also wrong**, by a smaller and far less obvious amount, because population
is not the denominator of either ratio. Average net taxable income is per **tax return**; a
dependency ratio is per **working-age person**. Population-weighting is an approximation that happens
to land close when the true denominator correlates with population, and is quietly off when it does
not — 0.04% for income, 0.41 percentage points for the dependency ratio. Both would be published as
measured national statistics.

## Decision

Aggregates over geographies are built from the ground up:

1. An **additive** indicator (a count or a total, `is_additive = 1`) is **summed**.
2. A **ratio or average** is **recomputed** from those sums, by the same `derived.py` function that
   produced it at commune level. Average income at province level is total province income over
   total province tax returns — never a mean of commune means, weighted or otherwise.
3. Anything that is **neither** — an index, a share, a cross-sectional rank — has no defensible
   aggregate. The engine **raises** rather than returning a number.

**Population-weighting is not implemented at all**, and must not be added. Leaving the capability in
place would mean someone eventually reaches for it believing it is the correct answer, which is
exactly the trap this measurement walked into.

Two supporting rules, both from the same measurement exercise and both now enforced in code:

- **Aggregate over the geographies that existed in that period**, walking each one's own parent
  chain — never over today's communes. Summing today's 565 puts Limburg's 2023 taxable income at
  €16.076bn against a correct €21.278bn: 24.4% short, because Hasselt has no fiscal row after the
  2025 mergers.
- **Every aggregate carries its coverage** (contributing geographies over existing ones) and is
  **suppressed below 90%**. A total 24% short is not a number with a footnote.

## Why this is better than the rule it replaces

`claude.md`'s instruction was right in spirit — do not average averages — but named the wrong
mechanism. Recomputation from components is strictly better wherever the components exist, and they
exist everywhere in this pipeline **by design**: Block F deliberately stores totals rather than
means, with `FISCAL_TOT_NET_TAXABLE_INC` and `FISCAL_NBR_NON_ZERO_INC` as separate indicators,
precisely so the average can be rebuilt at any level. The fiscal-income config says so in as many
words: *"Stored as a TOTAL rather than as an average on purpose: a total is additive and can be
re-aggregated when communes merge, an average cannot."*

It is also cheaper to defend. "We add up the income and divide by the number of returns" survives a
sceptical directeur financier; "we take a population-weighted mean of commune averages" invites the
question of why population, and has no good answer.

## Consequences

- `claude.md`'s definitions section now states the sum-then-recompute rule, the period-aware rule and
  the coverage rule. Any future agent reads those three instead of the population-weighting line.
- A new indicator aggregates correctly the day it lands, from `is_additive` plus its derived config —
  no hardcoded list to maintain (`methods_from_metadata`).
- An indicator that genuinely cannot be aggregated fails loudly. That is the intended behaviour, not
  a gap: `POPULATION_PERCENTILE` and the growth rates are refused here and computed elsewhere.
- The 90% coverage threshold is a judgement, not a measurement. It suppresses nothing on today's
  data, and 95% would also suppress nothing. Recorded as an open question in
  [comparison.md](../features/comparison.md).

## What would reverse this

A source that publishes only a ratio, with no additive components to rebuild it from. Then an
aggregate would have to be weighted by something, and the weight would need to be that ratio's own
denominator — declared in the indicator's config, not assumed to be population. No such source is in
the approved catalogue today.
