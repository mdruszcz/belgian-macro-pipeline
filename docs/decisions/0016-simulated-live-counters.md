# ADR 0016 — Simulated public-finance "live counters"

Date: 2026-10-03
Status: Accepted (reverses Batch 6's refusal; the maintainer approved the method, the
25 source series, and the wording constraint on 2026-10-03)
Required by: the maintainer's public-finance feature request. Governed by CLAUDE.md
rules 4, 6, 13, 19, 26, 35, 36, 38, 40, 41.

## Context

Batch 6 (`docs/implementation/batches/batch-6-macro-page.md`) built `macro.html`'s
"Finances publiques (compteurs simulés)" card in its REFUSED state:

> There is no national revenue, expenditure, balance or debt series to extrapolate
> from. The design's counters are labelled "simulés"; a simulation from *nothing* is an
> invention (rule 36), so the card says so instead. The page contains no timer of any
> kind, and a test asserts it.

That refusal was correct at the time: no such series existed in this pipeline. It no
longer holds. This PR loads 25 real Belgian general-government finance series from
Eurostat (`docs/data_catalog.md`, approved by the maintainer 2026-10-03) — revenue,
expenditure, balance, debt (annual and quarterly), six tax components and twelve COFOG
expenditure functions. A simulation built FROM these real, loaded series is no longer
"from nothing"; it is an extrapolation of real, labelled data, which rule 36 does not
forbid. This record is the decision that authorises building it, and the method it
must follow.

## Decision

**1. Method: a 3-year compound trend, not a forecast.** For each flow (revenue,
spending), let L be the latest year both TR and TE exist officially (2025 at the time
this PR loaded real data), and let the trend growth rate be
`g = (v_L / v_(L-3)) ** (1/3) - 1`, computed with `decimal` and rounded to 10 dp. Years
up to L use the official value; years after L use `v_L * (1+g) ** (year - L)`, with
`g` always the ROUNDED rate, never the unrounded ratio, so a reader who takes the
published growth rate and redoes the arithmetic gets exactly this module's own number.
The simulation horizon is `H = L + 2` (two years past the latest official one, never
further).

**This is explicitly NOT a forecast.** Live-loaded 2026-10-03: the 2025 official
deficit (TE − TR) is 33,219.9 million EUR; this method's own 2026 projected deficit is
38,474.1 million EUR — about 4.3 billion EUR wider, purely from compounding the
trailing 3-year growth rates of revenue and spending separately (spending's own trend
is faster than revenue's). **PR 2's UI must label every one of these counters
"simulation — trend extrapolation, not a forecast"** (the maintainer's own wording,
2026-10-03) and must never use this pipeline's grade-D "Forecast" label (that grade is
reserved for FPB's own published forecast, a different thing this method must not be
confused with).

**2. The +01:00 boundary convention.** Every year boundary is 1 January 00:00 at a
FIXED +01:00 (CET) offset, never a DST-aware zoneinfo. This is EXACT for 1 January
(Belgian summer time never starts before March), so every annual flow/stock boundary
this module computes is the real Brussels midnight. A quarterly boundary that falls in
summer time (1 April, 1 July, 1 October) is therefore up to one hour off true Brussels
local time. Accepted: these counters are a labelled simulation ticking once a second,
not a legal or accounting clock, and a one-hour error on a quarter boundary is
invisible at that resolution. `src/analytics/live_counters.py`'s own docstring repeats
this.

**3. The browser evaluates a straight line, nothing else.** Every counter is published
as a list of segments, each `{start_ms, end_ms, v0, v1, rate_per_ms}`. PR 2's browser
code (not built in this PR) may only evaluate
`v0 + rate_per_ms * (t - start_ms)` for whichever segment contains `t`. ALL arithmetic
— the trend rate, every projected value, every breakdown share, every segment boundary
— happens in Python, in `src/analytics/live_counters.py`, never in JavaScript (CLAUDE.md
rules 4/6). This is also why the engine is pure (no I/O, no wall clock): identical
inputs must keep producing byte-identical segments (rule 35), and a function that reads
the clock could not promise that.

**4. Failure policy: two different problems, two different responses (rule 13).** A
config or schema problem — an unknown unit, a malformed period string, segments whose
boundaries do not line up — is certainly wrong and raises `LiveCounterError`, failing
the whole export loudly. A DATA condition — the trend's base year is missing, a
breakdown's remainder is too negative to clamp (beyond `remainder_tolerance_meur`),
population coverage is short of 100%, a debt anchor sits beyond the horizon — is not a
bug, just something this run cannot show: it is published with `"state":
"unavailable"` and a reason, never raised. **A simulated widget must never block the
daily site update** — the entire reason this is a "never raise" condition rather than a
"fail the build" one, unlike almost every other rule-13 case in this pipeline.

**5. Population is refreshed by hand (rule 38).** `POPULATION_BY_COMMUNE`'s own
Statbel source is not part of the daily automatic fetch; its 1 January anchor for this
counter therefore moves only when the maintainer next loads a new year of that series,
not on every daily run. The population counter's own coverage check (both the latest
and the prior year's aggregate must show 100% coverage) is what stops this counter
anchoring on an undercounted base year if that ever happens.

**6. Counters jump on each official release, by design.** The debt counter re-anchors
on whichever of `GOV_DEBT_MEUR_BE` (annual EDP) or `GOV_DEBT_Q_MEUR_BE` (quarterly) has
the later period end; Eurostat publishes the quarterly figure roughly quarterly and the
annual EDP figure around the October EDP notification (historically ~22 October). Each
new anchor is a real, published number replacing a simulated one — the counter will
visibly jump (up or down) the day the daily export picks up a new anchor. This is
correct behaviour, not a bug: the simulation is always a bridge between two real
points, never a smoothed line across them.

## Consequences

- The public-finance card can now be built with real, labelled counters instead of the
  "no data" refusal state Batch 6 shipped. PR 2 (not part of this record) builds the
  page; this record only authorises the method and governs the wording.
- `src/analytics/live_counters.py` is a new file under `src/analytics/` — ordinarily
  requiring its own ADR before being touched at all (CLAUDE.md rule 19). This record
  and `docs/features/public_finance_live.md` together are that ADR, written in the
  same PR as the implementation, the same pairing `docs/decisions/0015-peer-model-v1.md`
  used for the peer model.
- Every later PR that reads `public/data/live_counters.json` must keep the "simulation,
  not a forecast" label and the grade-D exclusion from Decision 1 — this is a product
  requirement from the maintainer, not merely a style preference, and a UI change that
  drops it would re-create exactly the confusion Batch 6's refusal was written to avoid.

## Risks

- **A reader could mistake the projected years for official figures.** The wording
  constraint in Decision 1 exists specifically to prevent this; a future UI change that
  removes or waters down that label reintroduces the risk this record exists to avoid.
- **The trend method can diverge sharply from reality within the 2-year horizon** if
  revenue and spending's trailing 3-year growth rates move apart (as they already have:
  spending's trend is faster than revenue's, widening the simulated deficit well past
  the latest official one). This is expected and accepted — it is why the horizon is
  capped at 2 years and the label says "not a forecast" — but it means the simulated
  deficit/debt numbers should never be quoted as this pipeline's own view of Belgium's
  fiscal trajectory.
- **A breakdown silently going "unavailable"** (e.g. a future year where a named COFOG
  part overshoots its own total beyond tolerance) removes that one chart from the page
  without blocking anything else — correct per Decision 4, but it means a maintainer
  checking "why did the spending breakdown disappear" needs to read the exporter's own
  WARN line, not just the published JSON.
