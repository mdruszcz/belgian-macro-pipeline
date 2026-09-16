# ADR 0012 — A police.be rate of exactly zero is "not available", never a measured zero

Date: 2026-09-16 (amended 2026-09-16, same day — see "Amendment" below)
Status: accepted (Decision 1 superseded by the Amendment; Decisions 2 and 3 unchanged)
Extends: [ADR 0003](0003-aggregation-rule.md) (aggregates are refused, never invented) and the
docstring of `scripts/sync_police.py` (docs/data_catalog.md's police.be row), which already
established the zero-placeholder convention this record generalises. CLAUDE.md rule 26 (missing,
unavailable, suppressed, not-applicable and an explicit zero are five distinct states) and rule 13
(a source schema surprise fails loudly, never silently coerces).

## Context

Maintainer direction, 2026-09-16: "Hainaut is missing data, not 0 for car theft."

Measured on the committed `data/police_observations.csv` before this change: `CAR_THEFT_PER_10K`
had 581 rows for 2025, status `provisional`. Every one of Hainaut's 69 communes — Charleroi
included — carried a value of exactly `0.0`. A province of that size recording zero car thefts in
a year is not a real outcome; it is what the source calls a placeholder. Across all four police
indicators (`HOUSE_BURGLARIES_PER_10K`, `CAR_THEFT_PER_10K`, `THEFT_FROM_VEHICLE_PER_10K`,
`DOMESTIC_VIOLENCE_PER_10K`), 348 rows carried this same exact `0.0`, all status `provisional`.

`scripts/sync_police.py`'s own module docstring already documented why: police.be's export pairs a
placeholder row with `z: 0` (proven there for the six `geo_code`s that name no real geography at
all — three negative IDs and three garbage-looking codes, always zero). The loader already honoured
that convention for a code that resolves to **no** geography. It did not honour it for a code that
**does** resolve to a real commune: the same `z: 0` was written straight through as a measured
value. The source's own "nothing to report" marker became, on this pipeline's side, "zero car
thefts, confirmed" — collapsing two of CLAUDE.md's five distinct states (missing vs. an explicit
zero) into one, and doing it silently.

Separately, red-team finding P1-3 identified a second, independent defect in the same loader: the
27 communes dissolved on 2025-01-01 (plus two more, Bastenaken-old and Bertogne, dissolved
2024-12-02) still carried 2025 police rows. Borsbeek (11007) `HOUSE_BURGLARIES_PER_10K` went 61.47
(2024) to 0.0 (2025) — it did not fall to zero, it ceased to exist as that geo_id before 2025
began. Both defects share a root cause (the loader trusted the raw `z` value and the raw
`geo_code`'s resolvability without asking whether either was honest for the period in question),
so this record settles both.

## Decision 1 (ORIGINAL, SUPERSEDED BY THE AMENDMENT BELOW) — `z == 0` on a resolvable commune is
loaded as `na`, value NULL, for every commune, every indicator, every year

Not a Hainaut-only fix, not a car-theft-only fix, and not a province-level or magnitude heuristic.
The file gives no way, for ANY commune, to distinguish a placeholder zero from a genuine one — the
`z` field carries no flag, no separate placeholder marker, nothing but the number itself. Any rule
that tried to guess ("zero is suspicious above N population", "zero is fine for a small commune")
would be exactly the kind of Belgian-administrative-semantics guess CLAUDE.md says to escalate
rather than invent, applied here to a statistical semantics guess instead. So the rule is uniform:
every `z == 0` on a real, resolvable commune becomes `status='na'`, `value=NULL` — the schema's own
shape for "not available" (`CHECK (value IS NOT NULL OR status IN ('suppressed','na'))`).

**The accepted loss, stated plainly.** A small commune that genuinely had zero car thefts in a real
year now shows as "not available" rather than 0. That loss is real and is not being minimised. It
is preferred to the alternative on the table, which was letting an ambiguous marker stand as a
confirmed zero for every commune it touches — in the worst measured case, a whole province at once.
An honest "we don't know" is recoverable (the maintainer can pursue written confirmation from the
federal police, already in progress per docs/data_catalog.md); a silently wrong zero published to
565 commune pages is not something a reader can be expected to catch.

The unresolvable-and-zero rule already in the loader (six garbage `geo_code`s, always paired with
`z: 0`, skipped entirely rather than written) is unchanged: it answers a different question
("is this even a geography") and is not touched by this decision.

**This uniform rule turned out to be wrong, and was corrected the same day — see "Amendment
2026-09-16" below. It is kept here, struck through in spirit but not in text, because the record
should show what was tried and why it was rejected, not just the final answer.**

## Decision 2 — a commune dissolved before the row's own period gets no row at all, not `na`

`na` means "this commune existed and we don't have a trustworthy reading for it." A commune that no
longer existed at the start of the period in question is a different state entirely — there was
nothing to measure — so it gets **no row**, decided from `geographies.valid_to` (never a date
literal) against the period's own start date, not the `PINNED_PERIOD` (2024) `sync_police.py`
already uses to resolve which `geo_id` a `geo_code` names.

Those two questions are genuinely separate and can disagree, which is why this needed its own rule
rather than folding into the existing pin:

- **Resolution** ("which `geo_id` does this NIS code name") is pinned at 2024 because police.be
  backcasts its current (pre-2025-merger) municipal grid onto every year it publishes, proven in
  the loader's own tests by Kruisem (2019-merger-created) carrying a value in the source's "2000"
  file.
- **Existence for this row's period** is not pinned to anything; it is asked fresh per row, using
  the file's own year.

The sharpest case measured: Bastenaken-old (82003) and Bertogne (82005) both dissolved on
2024-12-02. Both still resolve at the 2024 pin (they were alive on 2024-01-01), so a naive "does it
resolve" check would keep them. But `2024-12-02 <= 2025-01-01` (the exclusive `valid_to` boundary),
so a 2025 row for either must be dropped. Both had `z: 0` in the real file, so before this ADR they
were two more rows of the manufactured-zero defect above; the drop rule applies regardless of
value, because CLAUDE.md rule 26 says a commune that did not exist is not the same "missing" as one
that existed and gave a placeholder.

## Decision 3 — no reconstruction is attempted for the 13 merger-created 2025 communes

The maintainer confirmed police figures for the 13 communes created in the 2025 mergers (23106,
37021, 37022, 44086, 44087, 44088, 46029, 46030, 71071, 71072, 73110, 73111, 82039) cannot be
reconstructed from this source, and none of the changes here attempt it. Measured: the real raw
file as fetched 2026-09-06 contains none of these 13 codes at all — certain, not inferred, because
`sync()` raises `SystemExit("Refusing to load a partial series")` on any code that fails to resolve
at the 2024 pin with a nonzero value, and the real sync succeeds. police.be expressed all of 2025 on
its old, pre-merger 587-code grid, the same backcasting behaviour the loader's docstring already
documents for every other year.

Consequence, stated because it is easy to miss: Bastogne's post-merger territory (82039) has **no**
2025 police figure at all, for any of the four indicators, after this change — not `na`, no row,
because 82039 does not resolve at the 2024 pin (it did not exist then) and so is treated the same as
any other code the fixed grid does not recognise. This is the honest state: the source published
nothing measurable for that territory in 2025, and Decision 1 does not manufacture a reading for
one just because its two predecessors happened to have (placeholder) rows.

## Amendment, 2026-09-16 — Decision 1 replaced with a PROVINCE-WIDE rule

Same day, after Decision 1 shipped (PR #215) and was measured against the corrected store, the
maintainer's own reaction was: **"you removed all the zero instead of just hainaut like i asked."**
He was right, and the data agrees with him.

**What Decision 1 got wrong.** Its stated goal was narrower than what it did: the actual direction
was "Hainaut is missing data, not 0 for car theft" — a claim about one province, two indicators. The
uniform rule that shipped instead converted EVERY `z == 0` anywhere in the file to `na`, including
in provinces where the zero was real. Measured on the pre-#215 store
(`git show 66468d61:data/police_observations.csv`, the last commit before Decision 1's uniform rule):

- **Hainaut** (the actual problem): `HOUSE_BURGLARIES_PER_10K` 0/69 communes zero,
  `DOMESTIC_VIOLENCE_PER_10K` 0/69 zero — real data throughout, e.g. Charleroi 109.62 and 173.2.
  `CAR_THEFT_PER_10K` 69/69 zero, `THEFT_FROM_VEHICLE_PER_10K` 69/69 zero — a genuine province-wide
  non-report on exactly those two indicators, exactly the shape the maintainer described.
- **Every other province**: a normal scatter of zero shares per indicator (0-25%, e.g. Flandre
  occidentale car theft 16/64 communes, Liège 13/84) sitting alongside real burglary and violence
  rates in the SAME small communes (median population 7,323 among the 64 non-Hainaut car-theft-zero
  communes, max 21,546 Koksijde). These are ordinary "no car theft this year" outcomes for a small
  commune, not placeholders — and Decision 1's uniform rule erased every one of them as `na`.

**The corrected rule.** A zero is a placeholder only when EVERY live, resolvable commune of the same
PROVINCE has a zero for that indicator in that period — a whole-province zero is the source's own
non-report, not dozens of communes independently having a zero year. Computed fresh per
(indicator, period, province) from the data itself, not a hardcoded "Hainaut" list, so:

- a future province-wide gap in any indicator is caught the same way, with no code change;
- a province that starts reporting again (even one commune) flips back to measured zeros
  automatically.

Province grouping walks `geographies.parent_geo_id` from a commune up to the ancestor whose
`level = 'province'` (`scripts/sync_police.py`'s new `_province_group()`, reused unchanged by
`correct_police_zero_placeholders.py` so the two scripts can never define "province" differently —
same discipline as the shared `_dissolved_before()` rule already had). The 19 Brussels communes have
no province ancestor (their arrondissement's parent is the region directly), so they group by
region instead; a Brussels-wide non-report would be caught the same way, since Brussels has only one
region regardless.

**Accepted residual risk, stated plainly.** A province where literally every one of its communes
had a genuine zero of some indicator in some year would still read as `na`, indistinguishable from a
non-report — the same kind of loss Decision 1 accepted, just scoped to the province level instead of
the commune level. This is judged acceptable at Belgium's scale: 565 communes across roughly 10
provinces plus Brussels, four crime-rate indicators. A true province-wide zero for burglary,
domestic violence, car theft or vehicle theft in a single year is exceptionally unlikely to occur
by chance for any real province; if it ever does, the cost is one wrongly-`na` (province, indicator,
year) cell, not a systematic bias across the dataset the way the original uniform rule was. Revisit
only if the federal police's written confirmation (in progress, docs/data_catalog.md) describes the
export at a level of detail that distinguishes a placeholder from a true zero directly.

**Correction script re-derives from the PRE-#215 store, not the current one.** Decision 1's rewrite
already destroyed the original `z` values for every cell it touched (NULL has no memory of what it
replaced), so `correct_police_zero_placeholders.py` cannot recompute the new rule from today's
`data/police_observations.csv` — it must start over from the real values at commit `66468d61`
(immediately before #215) and reapply both rules (province-wide zero, then dissolution) from
scratch. This is the one instance where "re-derive from the last-known-good state, not the current
one" was itself necessary to satisfy CLAUDE.md rule 1's usual pattern of building a migration on top
of the current store.

## Consequences

- `scripts/sync_police.py` gains `_dissolved_before()` (unchanged from the original decision) and
  `_province_group()` (new, replacing the uniform `z == 0 -> na` branch with the province-wide test
  in `sync()`). Documented at length in the module's own docstring rather than only here, because a
  future editor reading that file needs the reasoning next to the code it governs.
- `scripts/correct_police_zero_placeholders.py` is new: the migration half of CLAUDE.md rule 1 (data
  changes only through adapters and migrations, never by hand). The raw files under
  `data/raw/police/` are hand-fetched by the maintainer and are not present on every machine, so the
  adapter fix alone cannot re-run against them everywhere; this script applies the identical rule to
  the already-committed `data/police_observations.csv`, always re-deriving from the pinned pre-#215
  git blob (see the Amendment above). Tested to agree, row for row, with the fixed adapter run on
  equivalent synthetic raw JSON — so the next real re-sync from raw reproduces the corrected store
  exactly.
- Measured effect of running the (amended, province-wide) correction on the real pre-#215 store:
  7,553 rows read. 116 dropped for a dissolved commune (unchanged from the original decision: the 27
  communes dissolved 2025-01-01 plus Bastenaken-old and Bertogne, dissolved 2024-12-02, each carrying
  a 2025 row for all four indicators). Of the remaining 7,437 live rows, 138 became `na`/NULL — ALL
  138 in Hainaut, split exactly `CAR_THEFT_PER_10K` 69 and `THEFT_FROM_VEHICLE_PER_10K` 69, matching
  the province-wide non-report the maintainer identified and unchanged in count from Decision 1's
  own measurement for those two Hainaut cells. The other 94 cells that Decision 1 had converted to
  `na` (348 total zero-valued resolvable rows in the pre-#215 store, minus 116 dropped-dissolved,
  minus these 138 = 94) are restored as measured `0.0` with their original status, spread across the
  non-Hainaut provinces exactly as the "every other province has real zeros" evidence above predicts.
- No aggregate is affected: all four indicators are already `is_additive=0`,
  `aggregation_method=not_applicable` (ADR 0003's "refuse, do not invent" already applied here), so
  `export_aggregates_csv.py` was already skipping them at every level above the commune.
- Percentiles: an `na` cell drops out of its peer set the same way a `suppressed` cell already does,
  because both `export_aggregates_csv.py`'s and `export_percentiles_csv.py`'s own observation
  readers filter on `value IS NOT NULL` (the database path) / `row["value"] != ""` (the extra_csv
  path) before a row is ever added to the peer set — no new filtering logic was needed, only the
  status generalisation above. Verified locally (not committed — see the PR body): Koksijde's
  `CAR_THEFT_PER_10K` percentile exists after this amendment and did not under Decision 1's uniform
  rule (its 0.0 is now measured, not `na`); Charleroi's still does not, since Hainaut's non-report is
  unchanged.
- `communes.html`'s existing `statusPill()` already renders status letter `N` as "n/a" (it was built
  for `suppressed`/`na` generically, not police-specific), so no page template changed.
  `export_explorer_payloads.py`'s existing `STATUS_TO_LETTER`/`_cell()` already accept `na` and
  render `[null, "N"]`; both were already correct for this status before this ADR, because ONEM's
  published-rate work (ADR 0005) and the merger back-aggregation work exercised the same status
  machinery first. This decision is a data-correctness fix, not a rendering-pipeline change.

## Risks

- **If the maintainer re-fetches and police.be has moved its 2025 export to the new 565-code
  (post-merger) grid, the sync will refuse loudly, and that is the expected, correct failure — not
  a bug to silently work around.** `PINNED_PERIOD` is hardcoded to `"2024"`; a file carrying any of
  the 13 2025-created NIS codes would fail to resolve at that pin, and `sync()` already raises
  `SystemExit("Refusing to load a partial series")` on any unresolvable code with a nonzero value
  (CLAUDE.md rule 13). This ADR deliberately does NOT pre-emptively change `PINNED_PERIOD` or the
  resolution logic to anticipate that — there is nothing to fix yet, and guessing at the shape of a
  grid change before it happens risks exactly the kind of invented semantics this pipeline exists to
  avoid. When it does happen, the fix is a separate decision: either per-file (per-year) geography
  resolution instead of one fixed pin, or a new pin once the source's new backcasting behaviour (if
  any) is established — and Decision 3's backcast claim above would need re-checking against
  whatever the new file actually does, not assumed to still hold.
- The genuine-zero loss is now scoped to the province level (see the Amendment's "accepted residual
  risk"), not the commune level, and has no mitigation inside this pipeline beyond that scoping. If
  the federal police's written confirmation (in progress, docs/data_catalog.md) ever describes the
  source data at a level that distinguishes a placeholder from a true zero, this decision should be
  revisited — that is the only thing that would change it.
- This migration touches only `data/police_observations.csv`. `public/data/**` and
  `data/communes_history.csv` are not regenerated or committed by this change; they carry the old,
  wrong zeros until the next full export run (see the PR body).
