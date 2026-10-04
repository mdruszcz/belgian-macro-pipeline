# Feature: Public-finance live counters

Status: PR 2 of 2 open for review (data + engine + payload + the live UI on
macro.html/home2.html -- see "PR 2: the live UI" below). Not yet merged;
docs/steps stays unticked until the daily export is verified live.
Issue: #309 (public-finance feature, maintainer request)
Branch: feat/public-finance-data (PR 1), feat/309-public-finance-live-ui (PR 2)

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
  any guard-test rewrite are PR 2 (now built -- see "PR 2: the live UI" below).
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
- Issue #309: each breakdown part may carry an optional, config-driven `short_label`
  (`en`/`fr`/`nl`, all three or none -- `docs/features/live_counters.schema.json`'s
  `trilingual_label`) for a narrower legend/chip UI slot than the part's own full
  `names`/`label`; `placements.home_strip` now lists all five counters (population,
  debt, deficit, revenue, spending), not just debt/deficit. Data-only change; no PR 2
  page/UI wiring yet.

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
- **`deficit` is computed as TE − TR (GOV_EXPENDITURE_BE − GOV_REVENUE_BE), not read
  from Eurostat's own published balance series (`GOV_BALANCE_MEUR_BE`, na_item `B9`).**
  For 2025 that gives 347,956.3 − 314,736.4 = 33,219.9 (a deficit of that size), against
  B9's own published −33,220.7 for the same year (docs/data_catalog.md) -- an 0.8 M EUR
  gap, inside Eurostat's own stated rounding for the two releases, not a bug in this
  engine. The counter is deliberately computed this way (the `minuend`/`subtrahend` config
  in `config/live_counters.yaml`) so a reader can trace it to the same two flow counters
  (`revenue`/`spending`) already ticking above it, rather than to a third series with no
  visible counter of its own. Only PROJECTED years (beyond `latest_year`) are ever
  published as `deficit` segments -- the 2025 TE/TR values above feed the trend and
  appear in `deficit`'s own `basis`, but 2025 itself is never shown as a `deficit`
  segment, so this 0.8 M EUR gap against B9 never reaches the published payload as a
  visible figure.
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

## PR 2: the live UI (issue #309, branch feat/309-public-finance-live-ui)

Built from an approved, out-of-repo design mockup (four review rounds; see that PR's own
report for the full before/after). Adds the UI this PR's engine/payload had no page for:
macro.html's `#finances-publiques` chapter and home2.html's `#financeStrip`, via one new
generic, page-agnostic engine, `assets/belpulse/live_counters.js`
(`window.BPLiveCounters`), loaded by both pages and by no other (`tests/test_micro.py`,
`tests/test_map_ui_logic.py` both assert this).

**Config, not code (rules 2/20/24/28).** The mockup's own
`finances_publiques_sections.json` (official-figure cards + `anchor_indicator`, the five
official charts, the `simulated` block naming the strip placement and breakdown list)
moved into a `finances_publiques` block inside `config/national_sections.yaml`, passed
through unmodified structure by `scripts/export_site_payloads.py`'s existing
`_national_sections()`/`_check_national_sections()` into the SAME generated file every
other macro.html chapter already reads, `public/data/metadata/national_sections.json`.
`_check_national_sections()` now also refuses (exit non-zero): an `indicator` not present
in `public/data/national.json`'s own indicator set, a `counter`/`strip_placement` name not
declared in `config/live_counters.yaml`, and a trilingual label missing a language --
`tests/test_export_site_payloads.py`'s `test_finances_publiques_refuses_*` tests exercise
all three refusals directly, and `test_committed_national_sections_json_matches_the_real_config`
proves the committed JSON is the exporter's own output against the real, committed
config and `national.json`, not hand-edited to look right. The `unavailable` list's old
`public_finance` entry (and its now-false `kpis_note` claiming these series "do not
exist") is gone; `legacy_anchors: [public-finance]` still resolves to the same chapter.

**States, never collapsed (rule 26).** Each simulated region (the strip, the two
breakdown cards, home2's strip) renders one of: `not-started` (before its first segment),
`running` (ticking, with a `data-raw` float), `expired` (past the payload's own last
segment -- a DIFFERENT headline from "unavailable", since a simulation that ran and
stopped is a different fact from one that never had data), or `unavailable` (missing
config, missing/invalid payload fetch, or -- for a breakdown -- the payload's own stated
reason). The "Official figures" block is separate and never simulated: read straight from
`national.json`, each figure carrying its own period/status, no badge, no pause.

**Guard tests rewritten to the new policy (ADR docs/decisions/0016-simulated-live-counters.md),
not dodged.** `test_macro.py::test_macro_simulates_nothing` no longer asserts the OLD
policy (no `data-simulated` anywhere); it now asserts the NEW one: every `data-simulated`
region carries a visible `.bp-simulated-badge`, this page's own inline script contains no
`setInterval`/hand-rolled tick loop, and the shared engine it loads ticks on exactly one
`setTimeout` and zero `setInterval`. `test_home2.py`'s finance-strip tests assert the strip
is wired as a simulated region with a badge and a link to
`macro.html#finances-publiques`, and that the fallback sentence is now actually TRUE
(official figures are no longer "not published"; only the live trend can be
unavailable/expired). `test_statbel_attribution.py`'s `DELEGATING_PAGES` gained
macro.html (its own population figure is Statbel-sourced) as a `{page: credit-block
pattern}` map, since macro's credit block is a `<p>`, not map.html's `<footer>`.

**Tests.** `tests/pages/test_shared_components.py` adds Node-run unit tests for
`live_counters.js` (`valueAt`'s five states including a gap between segments;
`sinceOpened` never negative across a year-boundary reset, with the naive
value-difference formula shown to go negative in the same scenario; `mount()`'s four
refusal guards plus one positive control), run the same way `charts.js`/`components.js`
already are -- no DOM needed. `tests/test_public_finance_live_browser.py` is a new
Playwright suite (frozen clock via `page.clock.install`/`pause_at`/`run_for`): every
`data-raw` checked against an independent Python re-statement of `valueAt`'s own formula
against the committed `live_counters.json`, at a frozen instant and after `run_for(5000)`;
pause/resume; `prefers-reduced-motion` starting every counter paused; the year-boundary
reset (flow counters, the since-opened line staying non-negative, the YTD label reading
the new year); the expired state reading a different headline from a missing payload; a
mocked 404 falling back honestly with no badge/pause; French thousands-grouping
(U+202F); no value wrapping/overflow at 390px; and a behavioural guard that snapshots
every leaf element's rendered text, advances the clock 5s, and requires any node whose
text changed to sit inside a badged `[data-simulated="true"]` region -- zero changed
nodes on micro.html.

**Not done in PR 2 / unconfirmed.** The French/Dutch wording for the breakdown
`short_label`s, the home-strip fallback sentences, and the signed-balance strings are
first-draft translations (see the mockup's own LISEZMOI.md "Ce qui reste à décider"),
not yet confirmed by a native reviewer. The Bureau fédéral du Plan link in the chapter's
method `<details>` is a proposed replacement for a dead link, also unconfirmed. Three
pre-existing, sitewide contrast gaps the mockup's own review found (the raw `--th`
chapter-number colour, chart axis text, a dark-theme prose link colour) are out of scope
(rule 10) and unchanged by this PR.
