# Feature: validation layer (Block H)

Status: spec
Issue: (Block H — Validation layer, docs/steps)
Branch: feat/block-h-spec

## Problem

Nothing currently stops a wrong number reaching the public site. This session alone produced two
that got through: an indicator publishing a 2010-based index under a name asserting *millions of
euro* (91 rows live in the dashboard CSV), and a CSV writer that shifted every column after a name
containing a comma (287 rows). Both were caught by a human reading carefully. That does not scale.

## The survivability principle

The roadmap states the failure mode plainly: *"If every rule fails the build, you will disable
validation within a week."* So every rule below carries a severity, and the bar for `fail` is
deliberately high:

- **`fail`** — blocks the commit and the deploy. Reserved for things that are *certainly* wrong:
  broken referential integrity, a duplicate primary key, a value that cannot physically exist.
- **`warn`** — recorded and printed, does not block. For things that are *probably* wrong, or that
  are legitimately wrong sometimes.

A rule that would fire today on correct data is a `warn`, not a `fail`. That is not squeamishness;
it is the only way the layer is still switched on in six months.

## Baseline, measured today

Every proposed rule was run against the real committed stores before being written down:

| Check | Result today |
|---|---|
| `PRAGMA foreign_key_check` | clean |
| observations with no matching indicator / geography / source | 0 / 0 / 0 |
| period string vs the indicator's declared frequency | 0 mismatches |
| duplicate `is_latest` per (indicator, geo, period) | 0 |
| negative counts, percentages outside ±100, null values | 0 / 0 / 0 |

So the baseline is clean, and any future `fail` is a genuine regression rather than pre-existing
debt being surfaced. (The `fetch_runs`/`fpb` orphan reported earlier in this session has since been
resolved by the daily run.)

---

## Four corrections the data forced

The roadmap sketches these rules in one line each. Running them against the real data showed three
of those lines would not work as written, and surfaced a live bug.

### 1. `fetch_runs` does not see failures — a live bug

The roadmap's volume rules assume `fetch_runs` records what happened. It does not:

- `fetch_runs`: **162 rows, every one `ok`.**
- `legacy_fetch_log`: **83 `ERROR` rows** — DBnomics read timeouts.

The canonical table only covers adapters refactored in Block D; `belgian_macro_db.py` still logs the
legacy path to its own table. **A validation layer reading only `fetch_runs` would report all-clear
on a day when five indicators failed to fetch.**

And they did fail. Those timeouts are why `EUROSTAT_GDP_Q_MEUR_DE/_EA/_ES/_FR/_NL` and
`LABOUR_COST_DE/_EA/_FR/_NL` have configs but **zero observations** — a fact noticed during the
Block F label-honesty review without its cause being known. It is this.

**Rule:** validation reads both logs until the legacy path is retired, and `fail`s if either records
an error for a source expected to succeed.

### 2. Volume rules cannot use `rows_written`

The roadmap proposes failing on a *"row-count drop > 10%"*. Measured over the last runs:

| Source | `rows_written` on recent successful runs |
|---|---|
| `nbb` | `0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0` |
| `statbel` | `565, 565, 19149, 6383, 565, 565` |

Both make the rule meaningless. NBB writes **zero rows on a normal day** — that is
insert-only-on-change working correctly, not a fault, so a "count dropped" rule fires constantly and
a "count is zero" rule fires daily. Statbel's counts swing by 30× because a single number mixes
different indicators loaded by different scripts.

**Rule:** volume is measured on the **store**, not the run — the count of `is_latest = 1` rows per
**indicator**. That number is stable (`LOCAL_UNITS_BY_COMMUNE` 565, `EC_CONS_CONF_BE` 216,
`CONSUMER_CONFIDENCE` 200), so a drop in it is genuinely alarming. Per indicator, not per source,
because per-source totals mix unrelated datasets.

### 3. Staleness needs a per-indicator allowance

Age of the latest period, today:

| Indicator | Latest | Age |
|---|---|---|
| `LOCAL_UNITS_BY_COMMUNE` | 2023-Q4 | **1,010 days** |
| `EUROSTAT_GDP_Q_MEUR` | 2025-Q3 | 370 days |
| `HICP` | 2025-12 | 279 days |

A blanket "latest period older than expected" rule fires immediately on `LOCAL_UNITS_BY_COMMUNE` —
and that indicator is **correctly** stale: Statbel's standard view is pinned to 2023-Q4, verified
against the live API. A rule that fails the build every day on correct data is the exact thing that
gets validation switched off.

**Rule:** staleness is a `warn`, with an optional `max_age_days` per indicator config. Absent that,
the default allowance is generous (two publication intervals). `LOCAL_UNITS_BY_COMMUNE` gets an
explicit allowance documenting *why* it is frozen, so the warning means something when it changes.

### 4. Unit-vs-label mismatch deserves its own rule

The `EUROSTAT_GDP_Q_MEUR` defect — a name asserting millions of euro over values that are an index —
was invisible to every rule the roadmap lists. It is not a range error (the values are plausible),
not structural, not referential.

**Rule:** `warn` when an indicator's `unit` and its display `name` disagree on the obvious markers
(a name containing a currency word while the unit is an index, or vice versa). Heuristic, hence
`warn` — but it would have caught a defect that reached production.

---

## Rule catalogue

### Referential — severity `fail`

Broken references mean numbers attached to nothing, the automated form of Block C's mismapping risk.

| Rule | Check |
|---|---|
| `fk_integrity` | `PRAGMA foreign_key_check` returns nothing |
| `observation_has_indicator` | every `observations.indicator_id` exists in `indicators` |
| `observation_has_geography` | every `observations.geo_id` exists in `geographies` |
| `indicator_has_source` | every `indicators.source_id` exists in `sources` |
| `derived_not_stored` | no derived indicator id appears in `observations` (CONTROL G) |

### Structural — severity `fail`

| Rule | Check |
|---|---|
| `unique_latest` | at most one `is_latest = 1` row per (indicator, geo, period) |
| `period_matches_frequency` | `A` → `YYYY`, `Q` → `YYYY-Qn`, `M` → `YYYY-MM` |
| `export_parses` | every published CSV parses to a constant field count per row |

`export_parses` exists because the 287-row comma bug produced a file that was still valid UTF-8, had
the right number of lines, and was wrong. Only parsing it catches that.

### Range — severity `fail`

| Rule | Check |
|---|---|
| `counts_non_negative` | `unit = count` implies `value >= 0` |
| `percent_bounded` | `unit` starting `percent` implies `-100 <= value <= 100` |
| `no_null_values` | `observations.value` is never null (absence is an absent row) |

Catches the classic unit error — a rate stored as `0.052` where its siblings use `5.2` — which is
otherwise invisible until a chart looks flat.

### Volume — severity `warn`, except `row_collapse`

| Rule | Severity | Check |
|---|---|---|
| `row_collapse` | **`fail`** | `is_latest` count for an indicator drops by more than 10% since the last run |
| `indicator_disappeared` | **`fail`** | an indicator that had observations now has none |
| `staleness` | `warn` | latest period older than the indicator's allowance |
| `fetch_error` | `fail` | either fetch log records an error for a source expected to succeed |

`row_collapse` is the one CONTROL H names as the scenario most likely to publish garbage — "17,000
rows yesterday, 436 today" — so it blocks even though it is a volume rule.

### Labelling — severity `warn`

| Rule | Check |
|---|---|
| `unit_name_agreement` | display name and `unit` do not contradict on currency/index/percent markers |
| `has_trilingual_name` | `name.en/fr/nl` all present and none equal to the raw indicator id |

`has_trilingual_name` would have caught the twelve placeholder-named indicators found in the Block F
review.

## Persisting counts

`fetch_runs` gains `previous_count`, `new_count` and `delta` (migration `003`), so thresholds come
from observed history rather than being guessed once and never revisited. There are already 162 runs
of history to calibrate against.

## Where it runs

A `validate` step in `daily_fetch.yml` **between the sync steps and the export steps**. Non-zero
exit stops the job, so a `fail` prevents both the export and the commit — validation that only logs
is decoration, and the value is entirely in the blocking.

It also runs in `ci.yml` against the committed stores, so a bad hand-edited CSV is caught at PR time.

## Tests

- Each rule gets a test that constructs a violating fixture and asserts the rule fires — and one
  asserting it does **not** fire on the clean baseline, because a rule that always fires is worse
  than no rule.
- `[H]` step: deliberately corrupt a fixture, push, and confirm the Action fails. That verifies the
  *wiring*, which is where most validation failures actually live.
- CONTROL H: simulate a 95% row drop and prove the build breaks.

## Open questions for the maintainer

- **Should the legacy fetch path be retired rather than validated?** Reading two logs is a
  workaround for `belgian_macro_db.py` not having moved onto the Block D interface. Retiring it is
  the real fix, but it is a bigger job than this block.
- **The five country-variant indicators have configs and no data**, because of the DBnomics timeouts
  above. Fix the fetch, or delete the configs? Leaving them is the one option that keeps the
  `indicator_disappeared` rule permanently noisy.
