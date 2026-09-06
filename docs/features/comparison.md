# Feature: comparison, aggregation and percentiles (Block L)

Status: spec
Issue: (Block L — Comparison, percentiles, URLs, docs/steps)
Branch: spec/block-l-comparison

## What this is for

`€31,842` means nothing on its own. This block adds the context that turns a commune's figure into
information: the same metric for its province, its region and Belgium; a free picker for up to four
peer communes; and a percentile with its universe stated. It also gives every commune a permanent,
indexable URL, which is the basis of the SEO position the roadmap depends on.

## Everything below was measured on the real committed data first

Three of the roadmap's one-line sketches for this block do not survive contact with the data. Each
correction below is a measurement, not an opinion.

---

### Correction 1 — the `[REVIEW]` step's question is a false choice

The roadmap asks the reviewer to *"resolve unweighted commune average versus population-weighted
national figure"*. Measured on `AVG_NET_TAXABLE_INCOME` for Belgium, 2023, three ways:

| Method | Result | Error |
|---|---|---|
| A. Unweighted mean of the 552 commune means | €41,507.47 | **+€1,540.13 (+3.85%)** |
| B. Population-weighted mean of commune means | €39,950.17 | −€17.17 (−0.04%) |
| C. `sum(FISCAL_TOT_NET_TAXABLE_INC) / sum(FISCAL_NBR_NON_ZERO_INC)` | **€39,967.34** | — (definitionally correct) |

And on `DEPENDENCY_RATIO` for Belgium, 2026:

| Method | Result | Error |
|---|---|---|
| A. Unweighted mean of commune ratios | 59.6256% | **+2.1720 pp** |
| B. Population-weighted | 57.8659% | **+0.4123 pp** |
| C. `(Σ age_0_14 + Σ age_65_plus) / Σ age_15_64` | **57.4536%** | — (definitionally correct) |

**Neither of the roadmap's two options is right.** The unweighted average is badly wrong — it would
publish Belgian average income €1,540 too high, because it gives Herstappe (75 residents) the same
weight as Antwerp (565,615). But population-weighting is *also* wrong, by a smaller and less obvious
amount, because population is not the denominator of either ratio: average income is per **tax
return**, and a dependency ratio is per **working-age person**. Population-weighting is an
approximation that happens to be close when the true denominator correlates with population, and is
quietly off when it does not.

**Decision: a ratio is never averaged across geographies. It is recomputed from the summed additive
components at the target level.** Method C, always. Population-weighting is not implemented at all,
so nobody can reach for it later believing it is the correct answer.

This overrides CLAUDE.md's *"National aggregates are population-weighted unless the indicator config
says otherwise"* for every ratio currently in the pipeline. That line is right in spirit — do not
average averages — but names the wrong mechanism; recomputation from components is strictly better
wherever the components exist, which is everywhere here by design (Block F stores totals, not means,
precisely so this is possible). **An ADR is owed for this**, per CLAUDE.md's definitions section.

### Correction 2 — summing today's 565 communes silently understates every aggregate

`communes_history.csv` restricts output to the 565 *current* communes. 13 of them have no 2023 fiscal
row, because the Statbel fiscal file predates the 2025 merger wave (`fiscal_income.md`) — and those
13 include **Hasselt**, the capital of Limburg. Aggregating what the export shows produces:

| Province | Correct (the period's own communes) | Naive (today's communes only) | Error |
|---|---|---|---|
| Limburg | €21.278 bn | €16.076 bn | **−24.4%** (€5.2 bn missing) |
| East Flanders | €39.875 bn | €34.188 bn | **−14.3%** (€5.7 bn missing) |
| Luxembourg | €6.759 bn | €6.313 bn | −6.6% |
| West Flanders | €29.970 bn | €28.719 bn | −4.2% |
| Flemish Brabant | €32.236 bn | €31.542 bn | −2.2% |
| Antwerp | €46.890 bn | €46.154 bn | −1.6% |

Every naive figure looks entirely plausible. A directeur financier in Limburg would be benchmarked
against a province total missing a quarter of its tax base, with nothing on screen to suggest it.

**Decision: aggregate over the geographies that held a value in that period, walking each one's own
parent chain — never over today's communes.** This is the identical rule Block G established for
percentile peer sets, and the identical trap that block already hit once. Verified: feeding the
aggregator the unrestricted store maps all 581 geo_ids of the 2019 fiscal vintage to a province with
**zero unmappable**, restoring Limburg to 42 contributing communes (from 34) and East Flanders to 60
(from 50). Historical predecessor communes are in `geographies` with `valid_to` set, so the parent
walk resolves them correctly with no special-casing.

Mechanically this is the same two-stage shape `export_communes_history_csv.py` already uses:
**compute wide** (unrestricted set, including merged-away geo_ids), **display narrow** (only current
geographies reach the output).

**Note on how the error behaves:** a *total* is catastrophically wrong under missing coverage
(−24.4%), but the *ratio* built from it is only mildly wrong (Limburg average income −1.10%, East
Flanders −1.36%) because numerator and denominator lose the same communes. That asymmetry is why
coverage must be reported rather than inferred from whether a number looks sane.

### Correction 3 — a percentile over 19 communes implies precision it does not have

Measured peer-set sizes: Belgium 565, Flanders 285, Wallonia 261, **Brussels-Capital Region 19**
(and 552 for Belgium in 2024, since the peer set is per period). Over 19 items, one rank step is
**5.26 percentile points** and the only attainable values are 2.6, 7.9, 13.2, 18.4, … Publishing
"73.7th percentile in your region" from 19 observations reads as measured precision and is not.

**Decision: a percentile requires a peer set of at least 30. Below that the component states the
rank instead** — "4th of 19 in Brussels-Capital Region" — which is exactly as informative and makes
no false claim. Regional percentiles are therefore published for Flanders and Wallonia and
deliberately not for Brussels.

---

## The comparison set

For any commune, indicator and period, up to four reference values:

| Scope | Definition | Available today |
|---|---|---|
| Commune | the value itself | yes |
| Arrondissement | not published | **no** — deliberately omitted; 43 arrondissements averaging 13 communes each would put most below the 30-item floor for percentiles and add a level users do not ask for |
| Province | aggregate over the province's communes in that period | yes, except Brussels (its 19 communes have no province — `geography.md` Q3) |
| Region | aggregate over the region's communes in that period | yes |
| Belgium | aggregate over all communes in that period | yes |

Brussels communes show three scopes, not four. The comparison component renders whatever scopes
resolve rather than assuming a fixed depth — the same tolerance `_ancestor_names()` already applies.

## Aggregation rules, per indicator

Driven by config, not by a hardcoded list:

- **Additive** (`is_additive = 1`): sum. All nine raw municipal indicators qualify — they are counts
  and totals, by Block F's deliberate design.
- **Ratio / derived**: recompute from the aggregated components via the same `derived.py` function
  the commune-level figure uses. `AVG_NET_TAXABLE_INCOME`, `DEPENDENCY_RATIO`.
- **Cross-sectional** (`percentile`, `z_score`): not aggregated at all. A province's percentile
  against communes is a category error; the province gets a rank among provinces or nothing.
- **Period-relative** (`growth_rate`, `cagr`, `five_year_change`): computed from the aggregate's own
  history — aggregate first, then difference. Differencing first and then averaging is the same
  average-of-averages error as Correction 1.
- **Non-additive and non-recomputable**: refuse. `regional_share` and any index has no defensible
  aggregate; the engine raises rather than returning a number (Block G already enforces this).

## Missing-data handling

Every aggregate ships with its coverage, and coverage gates publication:

```json
{"value": 21278000000, "period": "2023", "coverage": {"n": 42, "of": 42, "pct": 100.0}}
```

- `n` = geographies that contributed; `of` = geographies that existed in that period.
- **Below 90% coverage the aggregate is suppressed**, not published with a caveat: a total 24% short
  is not a number with a footnote, it is a wrong number. The payload omits it and the page renders
  the existing `.no-data` component.
- At 90–99% coverage the value publishes with its coverage shown on screen.
- Coverage is computed against the period's own geographies, so a merger year does not read as a
  coverage collapse.

## The four-commune picker

Up to four comparison communes, lazy-loading `communes/{nis}.json` on selection — the payloads are
8–9.5 KB, so four is ~38 KB, well inside the Block J budget. Selection is encoded in the URL
(`?nis=11002&vs=11001,21015`) so a comparison is shareable; the roadmap's own reasoning is that a
shared link circulates inside the administration for free.

Four is the roadmap's number and it is also the practical limit for a legible mobile table.

## Permanent URLs

| Route | Generated from |
|---|---|
| `/local/{nis}/index.html` | one page per current commune — 565 files |
| `/local/{nis}/{indicator}/index.html` | one page per (commune, indicator) that has data |

Statically generated at build time from the same payloads the client fetches, so there is no second
rendering path to drift. Directory-plus-`index.html` rather than `{nis}.html` because it gives clean
`/local/11002` URLs on GitHub Pages with no server rewrite rules.

**File-count check before committing to this:** 565 commune pages plus, at today's 14 municipal
indicators, up to 7,910 indicator pages — 8,475 files per build. That is a real number to confirm
against Actions and Pages limits before the indicator route is built, so **the commune route ships
first and the indicator route is gated on that check**. At the roadmap's 200-indicator target the
naive product is 113,000 files, which is not viable as one directory per pair; the indicator route
will need either a subset rule (only indicators with real history) or a different URL shape. Decided
now, not discovered at 200 indicators.

**Thin pages are banned** (roadmap Block AD, brought forward here because it is cheaper to never
generate them): no data, no page. A commune-indicator pair with no observations gets no route, and
therefore no URL for Google to index and demote the domain for.

## Per-page metadata

Templated from indicator and geography metadata, never hand-written per page: `<title>`,
`<meta name="description">`, `<link rel="canonical">`, Open Graph tags, and a JSON-LD
[`Dataset`](https://schema.org/Dataset) block carrying `name`, `description`, `temporalCoverage`,
`spatialCoverage`, `creator` (the source agency), `license` and `dateModified`.

`dateModified` and `license` are not optional extras here: they are the same Statbel licence
obligations the pages already satisfy visually (`data_catalog.md`), expressed in structured form.
Every generated page carries the `.attribution` block too — a static page is published output like
any other.

## Tests

- **Correction 1 pinned numerically**: an aggregate ratio must equal the recompute-from-components
  figure and must *differ* from both the unweighted and population-weighted means, using the real
  measured values above (€39,967.34 vs €41,507.47 vs €39,950.17). A test that only checked "close
  to the right answer" would pass on the population-weighted figure, which is wrong by €17.
- **Correction 2 pinned**: a fixture with a merged-away predecessor holding the only value for a
  period must produce the complete province total, and the predecessor must not appear as its own
  output row. Directly mirrors Block G's existing peer-set test.
- **Coverage gate**: an aggregate at 89% coverage is absent from the payload; at 91% it is present
  with its coverage attached.
- **Refusal**: aggregating a non-additive, non-recomputable indicator raises rather than returning a
  number.
- **Percentile floor**: a 19-member peer set yields a rank, not a percentile.
- **URL stability**: the route for a given NIS is byte-identical across two consecutive builds apart
  from the build stamp — the `[H]` step's "survives a rebuild" property, as a test.

## Open questions for the maintainer

- **The ADR owed by Correction 1.** CLAUDE.md says national aggregates are population-weighted;
  measurement says recompute from components. The rule needs amending, and only the maintainer
  should amend CLAUDE.md.
- **The 90% coverage threshold is a judgement, not a measurement.** It suppresses Limburg's 2023
  fiscal total under the naive method and publishes everything under the correct one. A stricter 95%
  would suppress nothing today either. Worth a deliberate choice rather than inheriting this default.
- **The indicator-route file count** (8,475 today, ~113,000 at target). Confirm the platform limits
  before that route is built, and decide the subset rule.
- **Arrondissement omitted from the comparison set** — 43 units averaging 13 communes. Included only
  if a real user asks for it.
