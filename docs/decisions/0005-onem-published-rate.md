# ADR 0005 — A publisher's own ratio may be loaded as source data, and then has no aggregate

Date: 2026-09-12
Status: accepted
Extends: [ADR 0003](0003-aggregation-rule.md), which says how an aggregate is built. This record says
what to do when it cannot be. [ADR 0002](0002-split-committed-stores.md) decides which store the
data lands in; nothing here changes that rule, only applies it.

## Context

Until 2026-09-12 this pipeline had no current commune-level unemployment rate with a real
labour-force denominator, and the reason recorded in `docs/data_catalog.md` was that no publisher
offered one locally. That reason was wrong. ONEM publishes exactly such a rate, monthly, for all 565
communes, back to January 2017, and computes it themselves:

> Le taux de chômage résulte de la division du nombre de CCI demandeurs d'emploi par le nombre
> d'assurés contre le chômage.
> Source: calculs ONEM sur base des données de l'ONEM, de l'ONSS et de l'INAMI.

The numerator is `UNEMPLOYED_JOBSEEKERS`, already in the store. The denominator — persons insured
against unemployment, assembled from ONEM, ONSS and INAMI registers — is not published in any form
and could not be reconstructed here.

That combination is new for this repository. Every ratio published so far has been one of two kinds:
computed by us from two stored counts (`DEPENDENCY_RATIO`, `AVG_NET_TAXABLE_INCOME`), or a rate from
a source that also gives us its parts. This one is a quotient whose parts we will never hold.

Three questions had to be answered before it could be published at all.

## Decision 1 — loading a publisher's ratio as source data does not breach rules 4 or 6

It is loaded, as an ordinary observation, with `status` from the file's own coverage.

CLAUDE.md rule 4 forbids an LLM calculating a statistic, and rule 6 forbids writing a *derived*
value into `observations` as if it were source data. Neither is engaged here. Rule 4 is about who
does the arithmetic: ONEM does, on registers we cannot see, and nothing in this pipeline divides
anything. Rule 6 is about **our** derived values — a figure this repository computed from stored
inputs, which must live in the derived layer where its formula and inputs are declared and testable.
A published statistic is not that. It is the same category as `UNEMPLOYMENT_RATE_COM` (Statbel's
census rate) or `ADMIN_UNEMPLOYMENT_RATE_COM` (Steunpunt Werk's administrative rate): a number the
publisher stands behind, loaded as it stands, attributed to them.

The distinction worth writing down is **provenance, not arithmetic**. A ratio belongs in the derived
layer when we own the formula, and in `observations` when someone else does. Mixing those up in
either direction is the actual error: a source rate recomputed by us would silently diverge from
what the publisher prints, and a rate of ours passed off as source data would lose the formula a
reader needs to interpret it.

## Decision 2 — an unpublished denominator means NO aggregate, not a substitute one

Both indicators carry `aggregation_method = not_applicable` and `is_additive = 0`. No province,
region or arrondissement figure is produced.

ADR 0003 requires a ratio's aggregate to be **recomputed** from the summed numerator and the summed
denominator, using the same formula as at commune level. Here the denominator does not exist in the
store, so that computation is impossible — not inconvenient, impossible.

The schema offers `population_weighted` as the other option and it must not be used. CLAUDE.md
already forbids it outright, and the specific reason applies with full force: the whole population is
not this rate's denominator. Insured persons are. Weighting a rate by a quantity that is not its
denominator is the error ADR 0003 was written to stop, measured there at +0.41 pp on the dependency
ratio and +2.17 pp unweighted.

ONEM does publish its own aggregates, at levels 2 to 4 of the same file, and **those are also
skipped** — for a second, independent reason. ONEM's zones are not this repository's zones. Level 2
splits `Région wallonne à l'excl. de la Com. germ.` (55) from `Com. germanophone` (56), whereas
`be:reg:03000` here is Wallonia *including* the German-speaking communes. Loading 55 under our
Wallonia would publish a figure for a different territory under our name, which is worse than
publishing none. Level 3 uses codes `0` and `29` that are not province NIS codes, so the mapping
would have to be guessed, and CLAUDE.md is explicit that Belgian administrative semantics are not
to be guessed at.

Belgium (level 1, zone 99) is unambiguous and **is** loaded. That is the one aggregate the file can
contribute without a mapping decision.

**Refusing an aggregate is a correct answer here, not a gap to be filled later.** ADR 0003 already
says so for indices and shares: *"anything that is neither has no defensible aggregate. Refuse, do
not invent one."* This extends that to any ratio whose denominator the publisher withholds. If ONEM
ever publishes the insured-persons count, the aggregate becomes computable and this decision should
be revisited — that is the only thing that would change it.

## Decision 3 — a definitional break is carried and labelled, never smoothed

Belgium's rate sits between 6.26 % and 6.85 % from 2023 through February 2026, then reads 5.47 in
March and 4.41 in April. The fall appears in 546 of 565 communes. Time-limiting unemployment benefit
removes people from the CCI-DE numerator whether or not they find work, so the movement is
administrative, not economic.

Three options were available: drop the affected periods, splice the series, or publish it with the
break stated. **The third is chosen.** Dropping real published data would make this repository
disagree with ONEM about what ONEM published. Splicing would require us to model the reform's effect,
which is exactly the Belgian administrative semantics CLAUDE.md says to escalate rather than invent.

So the break is stated in both indicator descriptions in all three languages, asserted by a test
against the real file, and shown on any page that draws the series. The maintainer's instruction on
2026-09-12 was to publish both the annual level and the monthly detail precisely so the reform's
effect is visible — *"on affiche les deux, il est important de voir les effets de la fin des
allocations"* — which makes the break a feature of the publication, not a caveat buried in a config.

**A consequence worth naming:** the year-on-year change on this series is not interpretable across
March 2026 and must not be rendered as a trend arrow for 2026 without the break beside it. A reader
shown "Namur 6.74 %, down 3.9 points" concludes something false about Namur.

## Consequences

- Two indicators, `UNEMPLOYMENT_RATE_INSURED` (A) and `UNEMPLOYMENT_RATE_INSURED_MONTHLY` (M), both
  `source_id: onem`, loaded by `scripts/sync_onem_rates.py`.
- **Five unemployment measures now coexist** and must stay distinguishable: this rate, the Census
  2021 register rate, the Steunpunt Werk administrative rate, the ILO/LFS rate for Wallonia, and the
  claimant share of the 15-64 population. `tests/test_unemployment_rates.py` holds them apart.
- **Two size limits, both recorded because they are compromises, not design.** The monthly series
  stores its last 18 months rather than all 114: the full history is 46 % of every observation in
  the repository and takes the committed `data/belgian_macro.db` from 34 MB to 63 MB, against a
  25 MB rule it already broke. The annual series is untrimmed. Separately, the monthly series stays
  out of the committed `data/communes_history.csv` (already 35 MB) while still reaching the
  gitignored `--all-periods` file that feeds the payloads, so the commune page has the whole stored
  series. Neither filter changes any output that shipped before: no municipal indicator was monthly.
- **The committed database is over the size rule and this change made it worse**, 34 MB to 42.6 MB.
  That is flagged for the maintainer, not resolved here: the structural fix is to stop committing a
  SQLite file that `daily_fetch.yml` rewrites daily, which is a separate decision.
- Geography resolution is against **current** municipalities, not period-accurate: ONEM restates all
  124 periods on today's 565-commune map, and 31 of those codes did not exist in January 2017. This
  is the same pinned-vintage treatment `scripts/sync_onem.py` already applies to the same
  publisher's Excel tables, and it is a property of the source, not a shortcut.
- The `onem` source row is now written by two scripts. Both use `INSERT OR IGNORE`, so run order
  would decide the licence text shown beside a figure if they ever diverged;
  `tests/test_sync_onem_rates.py` runs both in both orders and asserts the row is identical.
