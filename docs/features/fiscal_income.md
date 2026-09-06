# Feature: fiscal income by commune

Status: implemented
Issue: (Block F — Statbel adapter, docs/steps; catalogue rows 1/1b)
Branch: feat/fiscal-income

## What this adds

Personal income tax statistics per commune, 2005–2023, from Statbel's
`TF_PSNL_INC_TAX_MUNTY.xlsx`. Four stored measures and one published average:

| Indicator | Unit | Source column |
|---|---|---|
| `FISCAL_TOT_NET_TAXABLE_INC` | eur | `MS_TOT_NET_TAXABLE_INC` |
| `FISCAL_NBR_NON_ZERO_INC` | count | `MS_NBR_NON_ZERO_INC` |
| `FISCAL_TOT_TAXES` | eur | `MS_TOT_TAXES` |
| `FISCAL_TOT_MUNICIP_TAXES` | eur | `MS_TOT_MUNICIP_TAXES` |
| `AVG_NET_TAXABLE_INCOME` | eur | **derived**, not stored |

`FISCAL_TOT_MUNICIP_TAXES` is the commercially important one: the additional municipal levy on
personal income tax is, for most Belgian communes, the single largest own-source revenue line.

## Why the commune file and not the sector file

Statbel publishes the same statistics by statistical sector
(`TF_PSNL_INC_TAX_SECTOR.xlsx`, 378,416 rows, 20,156 sectors). It is **not** the source here, for
two measured reasons.

**Suppression makes aggregation dishonest.** 15.9% of sectors are suppressed in 2023, concentrated
in small ones. Herstappe (73028) has *both* its sectors suppressed — all 41 of its declarations.
Rolling sectors up to communes would publish **€0** for it. In the commune file Herstappe is
complete: 41 returns, €1,849,504 total, €45,110 average. That is precisely the failure
`data_model.md` warns about — *"if suppression looks like zero, you will publish 'median income €0'
for a small commune"* — with a name attached. Four more communes hide 23–29% of their declarations
in suppressed sectors (Spiere-Helkijn, Braives, Ardooie, Faimes).

**A median cannot be reconstructed from parts.** The sector file's headline measure is a median, and
no arithmetic turns sector medians into a commune median. The commune file publishes totals and
counts, which *can* be combined — which is why they are what gets stored.

The sector file remains the right source for genuinely sub-communal work. That needs a `sector`
geography level this pipeline does not have (20,156 rows), and a ~35 MB store.

## Two traps in the file itself

### The geography is a fixed vintage, not the boundaries of the time

The set of 581 commune codes is **identical in every year from 2005 to 2023**, and resolves 581/581
only at a reference period of 2019–2024. At 2005 it resolves 563/581: the missing 18 are 2019-wave
creations (Puurs-Sint-Amands, Deinze, Aalter, Lievegem, Kruisem, Enghien, …) that did not exist then.

Statbel has back-cast the 2019 structure across the whole history, so a 2005 row labelled `12041`
means *"the territory of today's Puurs-Sint-Amands in 2005"*, not an entity that existed in 2005.

This is the **opposite** of the population files, which really are per-year snapshots and are
correctly resolved per period. Resolving each row at its own period here silently dropped **252 rows
(2.3%)** — exactly the mismapping Block C's audit exists to catch. The loader therefore resolves at
a fixed `GEOGRAPHY_REFERENCE_PERIOD` and **refuses to load at all** if any code fails to resolve
there: a partial fiscal panel looks complete, which is worse than no panel.

Consequence: the file predates the 2025 merger wave, so 29 of its communes resolve to entities since
merged away, and **13 successor communes have no fiscal row**. They are left empty rather than
back-filled — summing predecessors would be defensible for these additive measures but it is
publishing a figure Statbel did not, which is the maintainer's call, not the loader's.

### Suppressed and zero are both present, and are not the same

Statbel marks a withheld cell with a literal asterisk, so a naive numeric read does not fail — it
puts a string where a number belongs. Those become `value NULL, status 'suppressed'`, never `0`.

And a `0` here is often **real**: `MS_TOT_MUNICIP_TAXES` is exactly 0 for Knokke-Heist and Koksijde
in all 19 years and De Panne from 2007, because those communes levy no municipal surcharge at all,
funding themselves from second-home and tourism taxes. Conflating the two in either direction
publishes a false statement about a real place.

None of the four measures loaded here is suppressed anywhere in 2005–2023. The handling exists
because six other columns of the same file are, and because a future year could be.

## Why the average is derived, not stored

`AVG_NET_TAXABLE_INCOME` is computed on read from the total and the count via `mean_from_total`.
Storing the mean would be cheaper-looking and wrong: **a total is additive and survives a commune
merger by summation; a mean is not and does not.** Belgium merged 29 communes into 13 in 2025 alone.
Keeping the pair means a successor's average is the summed total over the summed count — the correct
figure, and *not* the average of its predecessors' averages.

It is an average **per tax return**, not per inhabitant: one return can cover a couple, and residents
with no taxable income are excluded from the denominator. It therefore sits well above any
income-per-head measure and the two must not be compared. Sanity check against known reality — the
2023 extremes are Attert (€63,679), Oud-Heverlee (€63,490) and Sint-Martens-Latem (€62,091) at the
top; Saint-Josse-ten-Noode (€25,000), Molenbeek-Saint-Jean (€27,668) and Anderlecht (€28,149) at the
bottom.

## What was deliberately left out

The file has 49 columns. Four are loaded. Notably **not** loaded:

- `MS_TOT_RESIDENTS` — a resident count from the tax file, which is *not* the population-register
  figure already stored as `POPULATION_BY_COMMUNE`. Two different definitions of "how many people
  live here" in one dataset is how a per-capita figure quietly becomes meaningless.
- The income-type breakdowns (real estate, movable assets, various, professional) and the
  deduction/assessment detail. These are where the file's suppression actually lives, and none has a
  named consumer yet. Adding one is a config plus a line in `COLUMN_TO_INDICATOR`.

## Where it runs

Manual only, like population, and for the same reason: `statbel.fgov.be` is unreachable from CI.

```bash
python -m src.db.migrate --db data/local/manual.db
python scripts/load_geography.py --db data/local/manual.db
python scripts/sync_fiscal_income.py --db data/local/manual.db
python scripts/export_observations_csv.py --db data/local/manual.db \
  --indicators FISCAL_TOT_NET_TAXABLE_INC,FISCAL_NBR_NON_ZERO_INC,FISCAL_TOT_TAXES,FISCAL_TOT_MUNICIP_TAXES \
  --out data/fiscal_income_observations.csv
```

The committed store is `data/fiscal_income_observations.csv` (6.5 MB, 44,156 rows), per
[ADR 0002](../decisions/0002-split-committed-stores.md).

The daily workflow does **not** load it, but does run
`sync_fiscal_income.py --reference-rows-only`, which inserts the `sources`/`indicators` rows and
nothing else. That keeps one metadata path: the observations live in the CSV, their name and unit
come from the `indicators` table, exactly as for population.
