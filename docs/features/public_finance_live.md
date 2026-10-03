# Feature: Public-finance live counters

Status: in-progress (PR 1 of 2 -- data + engine + payload; no page/UI change)
Issue: public-finance feature, maintainer request
Branch: feat/public-finance-data

## Problem

`macro.html`'s "Finances publiques (compteurs simulés)" card has stood refused since
Batch 6 (`docs/implementation/batches/batch-6-macro-page.md`): there was no national
revenue, expenditure, balance or debt series to extrapolate from, and "a simulation
from nothing is an invention" (CLAUDE.md rule 36). The design still asks for labelled
counters that visibly tick, once a second, for population, debt, deficit, revenue and
spending (with their parts) -- a product the maintainer wants built now that real
series exist.

## Goal

A later PR's browser code can show five ticking counters -- population, debt, deficit,
revenue, spending (the last two with named parts) -- each evaluating a precomputed
straight line (`v0 + rate_per_ms * (t - start_ms)`) with no arithmetic of its own. Every
number behind that line is computed in Python, from real, loaded Eurostat/Statbel data,
and is reproducible: identical inputs produce byte-identical output
(`public/data/live_counters.json`).

## Non-goals (this PR)

- No page or UI change. `live_counters.js`, the macro.html/home2.html card wiring, and
  any guard-test rewrite are PR 2.
- No births/deaths/migration counters, no FPB forecast figures.
- Not a forecast: the projection is a 3-year trend extrapolation, clearly labelled as a
  simulation (see the ADR). It is not held to forecast accuracy, and must never be
  presented as one.

## Proposed approach

**26 new Eurostat indicators** (`config/indicators/GOV_*_BE.yaml`, approved by the
maintainer 2026-10-03, `docs/data_catalog.md`): Belgian general-government revenue,
expenditure, balance and debt (annual and quarterly), seven tax/social-contribution
components (the 7th, D.995, added in the fix round to fix a revenue-breakdown
misallocation -- see "Assumptions and open questions" below), and twelve COFOG
expenditure functions. Single-country fetches (`geo: BE`),
same `eurostat` source/adapter/licence as the existing Europe rows -- no new licence
decision, no new store (loaded into the existing `international` store).

**A pure computation module**, `src/analytics/live_counters.py`: year boundaries at a
fixed +01:00 offset, a 3-year trend growth rate (`decimal`, rounded to 10 dp, reused for
every projection), flow/difference/stock segment builders, and a named-parts breakdown
with an exact remainder (clamped and renormalised within a tolerance, refused beyond
it). No I/O, no wall clock -- see the module's own docstring and
`docs/decisions/0016-simulated-live-counters.md` for the full method and its failure
policy.

**One exporter**, `scripts/export_live_counters.py`: reads the already-published
`public/data/national.json`, `aggregates.json` and `metadata/indicators.json` (never
SQLite, never a raw source file -- rule 20) plus `config/live_counters.yaml`, and writes
`public/data/live_counters.json`. All arithmetic happens here and in the engine; the
browser (PR 2) only evaluates the precomputed segments.

## Data / schema changes

- 26 new rows in `config/indicators/*.yaml`, registered in `config/stores.yaml`'s
  `international` store. No canonical schema change (rule 18).
- New unit `meur` (a million-EUR level at current prices, distinct from
  `meur_clv2010`'s 2010-volumes measure) -- `assets/commune_map.js` / `assets/i18n.js`
  (en/fr/nl).
- New config `config/live_counters.yaml`, validated by
  `docs/features/live_counters.schema.json` plus the cross-field checks in
  `src/validation/live_counters_config.py`.
- New payload `public/data/live_counters.json` -- see the `docs/features/
  site_payloads.md` entry added alongside this one. Additive; no existing payload's
  shape changes.

## New data sources

No new source. All 26 series use the `eurostat` source already approved
(`docs/data_catalog.md`'s "Approved sources" table); the new catalogue section
("Belgian general government finance in euros") records the 26 series themselves (the 26th, GOV_TAX_UNCOLLECTED_BE, added in the fix round),
approved by the maintainer 2026-10-03.

## Tests

- `tests/test_live_counters.py` -- the engine, pure unit tests, hand-computed expected
  values with the arithmetic in each comment (year boundaries, trend growth, the 2026
  revenue/expenditure/deficit projection, flow/stock/population segment examples from
  the batch spec, the two real breakdowns, every `Unavailable`/`LiveCounterError`
  branch).
- `tests/test_live_counters_config.py` -- schema + cross-field validation of
  `config/live_counters.yaml` (duplicate ids, unknown placement references, malformed
  remainders, missing languages).
- `tests/test_export_live_counters.py` -- the exporter against
  `tests/fixtures/live_counters/` (trimmed slices of this PR's own real 2026-10-03
  published payloads): byte-identical rebuild, no wall-clock dependency, config
  refusals, data conditions degrading to `"unavailable"` rather than raising, and that
  `live_counters_payloads` is in the daily export job's asset selection.
- Existing `tests/test_stores.py`, `tests/test_orchestration.py`,
  `tests/test_map_ui_logic.py` / `test_resolve_format_value.py` re-run unchanged and
  green with the 26 new configs and the `meur` unit present.

Run with `pytest tests/test_live_counters.py tests/test_live_counters_config.py
tests/test_export_live_counters.py -q`, plus `ruff check` / `black --check` on every
changed file.

## Assumptions and open questions

- **Wording not yet confirmed by the maintainer**: the French/Dutch translations of the
  26 indicators' `name`/`definition` fields, and the three remainder parts' labels
  (`other_taxes`, `non_tax_revenue`, `other_functions`) -- written by this batch from
  the English description only (rule 40), in the same style as the existing
  `GOV_DEBT_EUROPE`-family configs, never invented methodology. Listed in full in this
  PR's own implementation report.
- The debt counter's displayed label ("deficit" vs "surplus" by sign) is left to PR 2;
  this PR's config gives it a neutral label ("Budget balance").
- Population's own coverage/anchor is refreshed by hand (rule 38, Statbel), so the 1
  January anchor moves only when the maintainer next loads it -- not on every daily run.
- **D.995 fix (resolved in the fix round, 2026-10-03).** An independent audit (P2-1)
  found that `GOV_TAX_SSC_TOTAL_BE` nets Eurostat's `D995` out (capital transfer for tax
  assessed but unlikely to be collected, 895.2 M EUR for 2025) while `GOV_REVENUE_BE`
  (`TR`) does not, so `non_tax_revenue = TR - taxag_total` was silently folding that
  write-off in as if it were ordinary non-tax revenue. The maintainer approved a 26th
  series, `GOV_TAX_UNCOLLECTED_BE` (`D995` itself), the same day; `other_taxes`'s own
  `remainder_of` now carries a `plus: [GOV_TAX_UNCOLLECTED_BE]` (config/live_counters.yaml,
  validated by src/validation/live_counters_config.py) so its `whole` is the GROSS tax
  total (taxag_total + D995), and `non_tax_revenue` is now `TR - gross_total` with no
  residual folded in. Full arithmetic in `docs/data_catalog.md`'s public-finance section.
- **`deficit`'s own `basis`** now names the two flow counters (spending/revenue, i.e.
  TE/TR) it is computed from, each tagged `role: "minuend"`/`"subtrahend"`. **`debt`'s
  `basis`** names its anchor series (`role: "anchor"`); when the anchor year is already
  official, it also names the TR/TE pair setting that year's own pace (`role:
  "pace_minuend"`/`"pace_subtrahend"`); for every pace year beyond `latest_year` (today,
  that is every year -- the 2026-Q1 anchor is already past the 2025 latest_year), the
  pace is `deficit`'s own PROJECTED segment, cited structurally by counter id
  (`{"counter": "deficit", "role": "pace_counter"}`) rather than re-citing TR/TE
  directly, which would overstate a trend extrapolation's precision -- a reader follows
  `deficit`'s own basis one level down for ITS TR/TE citation. Both were empty/
  anchor-only gaps the audit flagged (P3), now filled so PR 2's UI always has a source
  citation to show.
- **The top-level `valid_from_ms`/`valid_until_ms` are only an envelope** (min/max
  across every counter's own segment bounds), not each counter's own window -- `debt`
  in particular starts at its own quarterly anchor, not the envelope's 1 January. PR 2's
  browser code must read each counter's own `segments[0].start_ms`/`segments[-1].end_ms`.
  Full detail in `docs/features/site_payloads.md`'s `live_counters.json` entry.

## Rollout / risks

- A simulated counter must never look like an official forecast. The ADR records the
  maintainer's own method choice and the exact wording constraint ("simulation — trend
  extrapolation, not a forecast"; never a grade-D "Forecast" label) that PR 2's UI must
  carry.
- The projection widens meaningfully past the latest official figures within two years
  (2026's projected deficit is roughly 4bn wider than the 2025 official one) -- labelled
  prominently so a reader does not mistake it for the real, settled number.
- A broken breakdown or counter never blocks the whole daily export: the failure policy
  degrades to `"unavailable"` with a reason, not a build failure, for every DATA
  condition (never for a config/schema one).
- This PR produces no page; nothing on the public site changes until PR 2 ships.
