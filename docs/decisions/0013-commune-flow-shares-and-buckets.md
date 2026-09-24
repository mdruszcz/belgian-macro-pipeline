# ADR 0013 — Buyers'-origin shares: what a cell counts, the denominator, and the five buckets

Date: 2026-09-24
Status: Proposed — awaiting the maintainer's approval
Required by: `docs/features/commune_flows.md` §2 ("Data rules"), which names this record as a
precondition for any code. Extends [ADR 0003](0003-aggregation-rule.md) (aggregates are built from
the ground up or refused) and [ADR 0012](0012-police-zero-is-not-available.md) (a zero is a state,
not automatically a measurement). Governed by CLAUDE.md rules 3/25 (NIS resolved through
`resolve_geo`), 5 (derived statistics need hand-computed tests), 13 (a schema surprise fails
loudly), 26 (five distinct states), 35 (byte-identical rebuilds) and 36 (no hand-typed figures).

## Context

The maintainer approved publishing SPF Finances 52.01.21 ("Origine des acheteurs — personnes
physiques") on 2026-09-24, labelled as purchases by the buyers' home commune and never as a
headcount of buyers. Every share we publish from it is a derived statistic and the bucket
definitions encode a geography judgement, so both need deciding before code exists.

The spec inferred what a `BuyerFrom<NIS>` cell means from the fractional values in the file. The
SPF's own product specification (`BuyersOriginNaturalPersons_20251231_TechSpec_FR.pdf`, shipped
inside the dataset zip, §4.1.1.1.6 ff.) states it directly. Translated, for every one of the 813
origin columns:

> "Number of parcels of the nature concerned and situated in the object described of which **at
> least one buyer is domiciled in** \<origin\>. If a real right is exercised in undivided
> co-ownership over a parcel, then the parcel is **counted pro rata to the acquired share**."

And §1.4, on the dataset as a whole:

> "Origin of buyers — natural persons is the dataset describing the origin of buyers (natural
> persons) of immovable property situated in Belgium **according to their commune of domicile**
> for residents and according to their country or territory of domicile for non-residents
> (**domicile at the date of the deed**). […] The number of parcels **takes account of the shares
> actually acquired**."

So a cell is **parcels, apportioned by each buyer's acquired undivided share** — not
transactions, not persons, not a headcount. The spec's inference was right in substance; the
TechSpec adds three things it did not have: the unit is the *parcel* (a cadastral parcel belongs
to exactly one commune, §1.2), the reference is *domicile at the date of the deed* (not today's
address), and the pro-rata weighting is the SPF's own stated rule rather than something we
deduced from decimals.

The TechSpec also settles the residual. The dataset covers natural persons only; a parcel bought
by a legal person contributes to `ParcelsNumber` and to no origin column. `ParcelsNumber` has no
attribute definition of its own in the TechSpec (it appears only as "the total number of
parcels" in §1.4), and measured on the real 2025 file it is an integer while the origin cells are
fractional. It is therefore a different quantity, not a denominator for these shares.

**One place the TechSpec contradicts the origin lookup, and it must not be silently reconciled.**
`OriginTable.csv` labels `000` as "Indéterminé / Onbepaald" and `999` as "Inconnu / Onbekend".
The TechSpec's attribute definitions are the other way round: §4.1.1.1.817 defines `BuyerFrom999`
as "at least one buyer whose **country** of domicile is undetermined", and §4.1.1.1.818 defines
`BuyerFrom000` as "at least one buyer with **domicile information not available** (deceased
persons, special holders or incomplete files)". Both are unknown-origin states, so no published
figure changes either way, but the two files disagree about which label belongs to which code and
neither is authoritative over the other. Consequence: we publish neither label. Both codes go to
one bucket named "origin unknown" in our own words.

## Decision

**1. Wording. One label, three languages, fixed here.**

- en: "purchases, by the buyers' home commune"
- fr: "achats, selon la commune des acheteurs"
- nl: "aankopen, volgens de woongemeente van de kopers"

Plus a fixed note, in all three, that a parcel bought jointly is split pro rata between the
buyers' communes, which is why figures are not whole numbers. The strings "number of buyers",
"buyers", "aantal kopers" and "nombre d'acheteurs" must not appear as the figure's label
anywhere. We do not claim migration, and we do not claim persons.

**2. Denominator. The row's own summed origin cells at `ParcelNature=TOTAL`.**

For destination commune *d* in period *p*, read the single row `(NISCode=d, ParcelNature=TOTAL)`
and let

    D(d) = Σ over all 813 origin columns of that row's cell value

Every published share has `D(d)` as its denominator. `ParcelsNumber` is **never** a denominator.
Measured on the real 2025 file: `D` is 97.39% of `ParcelsNumber` at the median commune, ranges
72.78% (Ouffet) to **100.16% (Gesves, 187.30 against 187 parcels)**, and the national total is
1.65% abroad. Gesves matters: coverage is **not** bounded by 100%, because a parcel with buyers
domiciled in two communes contributes to two columns while the pro-rata shares need not recompose
to exactly one. Any code or test asserting `D ≤ ParcelsNumber` is wrong and will fail on real
data.

**3. Coverage is published beside the shares but does not gate them.**

    coverage(d) = D(d) / ParcelsNumber(d)

published as a ratio with its own label ("share of parcels here with an identified natural-person
buyer"), rounded to one decimal place. It is **not** suppressed below 90% and **not** capped at
100%. This departs from the spec's outline and from ADR 0003's 90% gate deliberately: that gate
exists for an aggregate assembled from geographies that may be missing, where a short total is a
wrong number. Here nothing is missing — every cell is present (measured: 0 blank cells in
459,345 checked), the shares are internally complete by construction (they sum to `D` exactly,
verified for all 565 destinations), and a low coverage means only that more of that commune's
parcels went to legal persons. Suppressing at 90% would delete 15 real communes' shares (Ouffet,
Messines, Manage, Visé, Zuienkerke, Waasmunster, Oud-Heverlee, Donceel, Berlare,
Sint-Martens-Latem, Zonnebeke, Poperinge, Baerle-Duc, Chaudfontaine, Erquelinnes) whose shares
are correct. Coverage is shown so a reader can see how much of the commune's activity the
breakdown speaks for.

**4. The five buckets, plus unknown, defined on the ORIGIN commune's geography at the period.**

Every non-zero origin cell lands in exactly one bucket. Membership is decided by resolving the
**origin** NIS through `resolve_geo(nis, period)` at the row's own period and walking
`parent_geo_id` to the arrondissement and region, never by a hardcoded list and never by the
destination's geography alone:

1. `same_commune` — origin `geo_id` equals the destination's `geo_id`.
2. `rest_of_arrondissement` — same arrondissement ancestor, different commune.
3. `rest_of_region` — same region ancestor, different arrondissement.
4. `other_regions` — a Belgian commune in a different region.
5. `abroad` — a non-NIS origin code (the 248 country/territory codes in `OriginTable.csv`).
6. `origin_unknown` — codes `000` and `999` only (see the label contradiction above). This is a
   **sixth, separately named bucket**, never folded into `abroad` and never dropped. Measured: 185
   of 565 communes have a non-zero value here, so dropping it would silently move real volume.

The six are exhaustive and disjoint, so they sum to `D(d)` exactly and their shares sum to 100%.
Verified on all 565 destinations of the real 2025 file: zero mismatches. There is therefore **no
separate residual line** for the first release — the residual the maintainer asked for is
`origin_unknown` plus the coverage figure, and the six buckets already close to 100%. An origin
NIS that fails to resolve raises and stops the run (rule 13); it is never bucketed as unknown.

**5. Top 8 named origins, deterministic tie-break.**

Rank the named Belgian origins (buckets 1–4, excluding `abroad` and `origin_unknown`) by cell
value descending; break every tie by **origin NIS code ascending**. Take the first 8. Ties are
real, not theoretical: Boechout's 2025 row has Schilde (11039) and Geraardsbergen (41018) both at
exactly 10, straddling positions 7 and 8. Sorting on the full decimal value as a `Decimal`, never
a rounded or float value, and on the NIS as a zero-padded string, is what makes rule 35's
byte-identical rebuild hold.

**6. Rounding. Compute in `Decimal`, round once, at the end, half-up, to one decimal place.**

Shares are stored and published to one decimal place. Because the six buckets are computed from
exact cell values and only then rounded, the rounded shares may sum to 99.9% or 100.1%; that is
accepted and must not be "fixed" by adjusting the largest bucket, which would make one published
number not equal its own formula. The block states the buckets sum to 100% before rounding. The
store keeps the unrounded cell sums so rounding is a presentation step only.

**7. A destination with zero purchases is a stated state, not 0%.**

If `D(d) = 0`, no share is published for that commune — not `0%` for six buckets, which would
assert a distribution that does not exist. The payload carries an explicit state
`no_purchases_recorded` and the block renders a sentence saying no parcel in that commune was
bought by a natural person in that year. `ParcelsNumber = 0` alongside it is a **measured zero**
(rule 26) and renders "0", never "–". This is a contingency, not a present case: the spec's
example, Herstappe, is **not** all-zero in the 2025 file — measured, it has `ParcelsNumber = 1`
and one origin cell of 1.0, its own commune, 100%. The spec's all-zero observation was made on
the 2023 file and does not hold for the year we are publishing.

**8. Aggregation. Sum the cells, recompute the shares. Never average a share.**

A province, region or Belgium figure is built by summing the underlying origin cells of its
member destinations and recomputing every share and the coverage from those sums, exactly as at
commune level (CLAUDE.md "Definitions"). The zip ships the SPF's own
`ProvincialWide…`/`RegionalWide…`/`NationalWide…` members; where we publish an aggregate we
prefer those rows over our own sum, and a test asserts our sum agrees with the SPF's row. Note
the buckets change meaning at aggregate level ("same commune" is not "same province"), so a
province-level bucket breakdown is **refused** in this ADR; only the aggregate's named origins
and its coverage are defensible. "Top origin commune for a province" stays refused, per the spec.

**9. Merged communes. Latent for this release, and that is stated, not assumed.**

First release is the latest year only (maintainer decision, 2026-09-24). Measured on the real
2025 file: 565 destinations and all 565 five-digit origin codes resolve live at 2025-12-31
against `config/geography/geographies.csv` — the SPF has moved to the post-merger grid on **both**
axes. So no predecessor-summing is needed for 2025 and none is implemented. The rule for when a
second year is added is fixed here so it is not re-decided later: sum predecessors on **both**
axes into the successor's `geo_id`, using `resolve_geo` at that year's own period, and fail loudly
on any code that does not resolve. The 2023 file's 581 destinations are the case that will
exercise it.

## Worked example — Boechout (NIS 11004), 2025, `ParcelNature=TOTAL`

Read live from
`MunicipalityWideBuyersOriginNaturalPersons_20251231.csv` on 2026-09-24. Builder tests reuse these
figures as their hand-computed expected values (rule 5); none of them is typed by hand from
nowhere (rule 36).

`ParcelsNumber` = 423. Non-zero origin cells: 39 (38 Belgian communes, 1 foreign, 0 unknown).

Denominator `D` = **413.995833337** (the sum of all 813 origin cells of that one row).
Coverage = 413.995833337 / 423 = **97.9%**.

Top 8 named origins (value desc, NIS asc tie-break):

| # | NIS | Origin | Cell value | Share of `D` |
|---|-----|--------|-----------:|-------------:|
| 1 | 11004 | Boechout | 151.15416667 | 36.5% |
| 2 | 11002 | Antwerpen | 79.6 | 19.2% |
| 3 | 11029 | Mortsel | 46.858333333 | 11.3% |
| 4 | 11021 | Hove | 22.166666667 | 5.4% |
| 5 | 12021 | Lier | 17 | 4.1% |
| 6 | 11013 | Edegem | 12.186666667 | 2.9% |
| 7 | 11039 | Schilde | 10 | 2.4% |
| 8 | 41018 | Geraardsbergen | 10 | 2.4% |

Positions 7 and 8 are the tie-break: both cells are exactly 10, and 11039 precedes 41018 because
the NIS code sorts lower.

Buckets (Boechout is `be:mun:11004`, arrondissement `be:arr:11000`, region `be:reg:02000`):

| Bucket | Sum of cells | Share of `D` |
|--------|-------------:|-------------:|
| same_commune | 151.15416667 | 36.5% |
| rest_of_arrondissement | 211.291666667 | 51.0% |
| rest_of_region | 50.5 | 12.2% |
| other_regions | 0.05 | 0.0% |
| abroad | 1 | 0.2% |
| origin_unknown | 0 | 0.0% |
| **total** | **413.995833337** | **100.0%** |

The bucket sums equal `D` exactly, which is the invariant a test should assert for every commune,
not just this one. Note `other_regions` = 0.05 — a real non-zero value that rounds to 0.0%; the
block must not render it as "nothing", and this is why the store keeps the unrounded sum.

Second example, for the single-origin edge: **Herstappe (73028)**, `ParcelsNumber` = 1,
`D` = 1, coverage 100.0%, one named origin (Herstappe itself, 1.0), `same_commune` 100.0%, the
other five buckets 0.

## Consequences

- A builder can implement and test every published figure from this record alone: the denominator,
  the six buckets, the tie-break, the rounding, the zero state and the aggregate rule are all
  stated as formulas.
- `OriginTable.csv` must be read as **latin-1** (confirmed: country names are mojibake under
  UTF-8) and as **semicolon**-delimited, like the data members.
- Coverage is published without a suppression gate, which is a deliberate departure from the
  spec's §2 outline. If the maintainer prefers the 90% gate, 15 named communes lose their
  breakdown and this record needs amending before PR 1 lands.
- The `000`/`999` label contradiction is recorded rather than resolved. If it ever matters beyond
  labelling, it is a question for `datadelivery@minfin.fed.be`, not something to infer.

## Risks

- **The SPF could move back to a pre-merger grid in a future year**, as police.be did (ADR 0012's
  first risk). Decision 9's "fail loudly on an unresolvable code" is the intended behaviour, not a
  bug to work around.
- **A reader will still read "purchases by the buyers' home commune" as a headcount of people.**
  Decision 1's wording and the pro-rata note are the whole mitigation; there is no better one, and
  it is the reason the maintainer's approval was needed before publishing at all.
- **Coverage above 100% will look like a bug to a future editor.** It is documented here and in
  Decision 2 with the real case (Gesves) precisely so nobody "fixes" it by clamping.
