# Feature: vintages and as-of queries (Block I)

Status: spec
Issue: (Block I — Vintages, docs/steps)
Branch: spec/block-i-vintages

## What this block is actually for

One sentence, because it is the commercial argument: **a client must be able to ask what the number
was on the day they quoted it.** A municipality that published "unemployment 7.2%" in a March
council report and finds the same page saying 7.4% in December has not found a revision, they have
found a supplier who cannot reproduce their own figures.

## What already exists

The roadmap's `[BUILD] insert rather than overwrite on change` is **largely already built**, as a
side effect of Blocks D and F rather than as a decision. All three writers implement it:

| Writer | Compares | Writes on change | Collision check |
|---|---|---|---|
| `scripts/sync_to_canonical.py` | `value` **and** `status` | new row, old flipped `is_latest = 0` | raises on `rowcount != 1` |
| `scripts/sync_statbel.py` | `value` **and** `status` | same | same |
| `scripts/sync_population.py` | `value` **only** | same | same |

`vintage` is already a full ISO-8601 timestamp, not a bare date, and that was itself a bug fix: two
syncs on the same calendar day where a value legitimately changed would collide on the primary key
`(indicator_id, geo_id, period, vintage)`, the `is_latest = 0` flip would succeed, the `INSERT OR
IGNORE` would silently no-op, and the cell would be left with **zero** `is_latest = 1` rows.

So this block is mostly not "build the mechanism". It is: fix what measurement shows is wrong with
it, and build the query layer that makes it worth having.

## Measured today

| Fact | Value |
|---|---|
| observations | 1,939 |
| rows with `is_latest = 1` | **1,939** |
| rows with `is_latest = 0` | **0** |
| distinct vintages | 3, all dated 2026-09-05 |
| revisions ever recorded | **none** |

**No revision has ever been written.** The 2026-09-06 daily run confirms it from the other side —
its log reads `Checked 1374 observations, 0 new vintage(s) written` and `Fetched 565 observations,
0 new vintage(s) written`. Every value fetched that day was identical to the one already stored.

That is the single most important number in this document, and it cuts both ways. The storage
argument for insert-only-on-change is settled: the marginal cost of keeping revision history is
approximately **zero**, because approximately nothing is ever revised. But it also means **the
revision path has never executed against real data.** It is correct by inspection and by unit test,
not by observation. Tests are therefore not optional polish here; they are the only evidence there
will be.

---

## Three corrections the data forced

### 1. `sync_population.py` will silently lose a suppression

Two of the three writers compare `value` **and** `status`. `sync_population.py` compares `value`
alone:

```python
current = conn.execute("SELECT value FROM observations WHERE ... AND is_latest = 1", ...)
if current is not None and current[0] == value:
    return 0  # unchanged -- no new vintage
```

Today this cannot fire, because that script hardcodes `'final'` on every row it writes, so status
never varies. It is a **latent** defect, not a live one — and the thing that makes it latent is
exactly what is scheduled to change. `docs/features/data_model.md` already names the case: *"Statbel
suppresses small-cell values. If suppression looks like zero, you will publish 'median income €0'
for a small commune."* The day suppression handling lands, a commune moving `final → suppressed`
at an unchanged number writes no new vintage and the suppression is lost.

**Rule:** one shared implementation of the change predicate, comparing `(value, status)`, used by
all three writers. Three hand-copied versions of a rule is how the versions diverge; this one
already has.

### 2. `retrieved_at` would be a third name for a column that exists twice

The roadmap says *"Add vintage and retrieved_at"*. The table already has both concepts:

- `vintage` — full ISO timestamp, the identity of this version of the cell (part of the PK)
- `created_at` — full ISO timestamp, when the row was inserted

They are set from the same clock in every writer. A third timestamp called `retrieved_at` would
duplicate `created_at` and give a future reader three columns to choose between with no rule for
picking. **Not added.** What is added is documentation of which of the two existing columns means
what, because that is the actual gap.

### 3. `fetch_runs.rows_written` holds two contradictory definitions

Runs 116–162 (the 2026-09-06 daily run) record **3,233 rows written** on a day both sync steps
reported writing **zero** vintages, and on which the observations table did not gain a single row.

The column is not wrong so much as overloaded. Two different writers populate `fetch_runs`:

- `belgian_macro_db.py`'s Block D adapters log **rows upserted into the legacy tables** — run 116
  reads 198 and writes 198, because all 198 legacy rows were upserted.
- `sync_to_canonical.py` / `sync_statbel.py` log **canonical vintages written** — 0, correctly.

Same column, same table, two incompatible meanings depending on who wrote the row. Block H already
rejected `rows_written` as a basis for volume rules on the grounds that its values were unusable;
this is the sharper version of that finding — the values are unusable because the column has no
single definition. Recorded here rather than fixed here: the honest fix is retiring the legacy path,
which is its own piece of work and is already an open question for the maintainer.

---

## The design

### What creates a new vintage

A new vintage is written **if and only if** `(value, status)` differs from the current
`is_latest = 1` row for that `(indicator_id, geo_id, period)`. Nothing else — not a new fetch, not a
new day, not a re-run.

This is the rule that keeps the table from growing 365× faster than it needs to. Stated as
arithmetic on today's store: a vintage per run per cell would add 1,939 rows a day, about 708,000 a
year. The measured figure is 0.

Deliberately *not* triggers for a new vintage:

| Event | Why not |
|---|---|
| A re-run on the same day | Same value, same status — nothing happened |
| `fetch_run_id` differs | Provenance of the fetch, not of the value |
| Floating-point noise below the indicator's `decimals` | Not yet an issue: every comparison to date has been exactly equal. Revisit only if a source starts emitting jitter, and record the measurement then rather than pre-emptively rounding |

### Queries default to latest

`is_latest = 1` stays the default path for every existing reader — both exporters, the dashboard,
the derived-indicator engine. As-of is opt-in. A history feature that changes what the front page
shows has broken the front page.

### `get_observations(as_of=...)`

```python
get_observations(conn, indicator_id=..., geo_id=..., period=..., as_of=None) -> list[Observation]
```

`as_of=None` means `is_latest = 1` — the existing behaviour, on the existing partial index.

`as_of=<timestamp>` means: for each `(indicator_id, geo_id, period)`, the row with the **greatest
vintage ≤ as_of**, and no row at all where every vintage is later than `as_of` (the cell did not yet
exist on that date — reporting nothing is correct, and inventing a first-known value is not).

Two things the implementation must get right, both of which are easy to get subtly wrong and
invisible without a test:

- **`as_of` is compared as a parsed timestamp, not as a string.** Every vintage in the store today
  ends `+00:00` (1,939 of 1,939 verified), so lexical comparison happens to work. A single row
  written with a different offset would silently sort wrong. The same trap was already hit in Block
  H's fetch-log comparison.
- **A bare date must be interpreted as the end of that day.** `as_of='2026-03-15'` naïvely parses to
  midnight and excludes everything published on the 15th — which is the day the client is asking
  about.

### Revision-detection report

`scripts/revisions_report.py --since <date>`: every cell whose value changed, with old value, new
value, both vintages, and the delta. Ordered by absolute relative change, because a 0.1% revision to
GDP and a 40% revision to one commune's population are not equally interesting.

Two audiences, and the second is the reason it earns its place: it is analytically interesting to a
client, *and* it is the early warning that a source changed methodology. A source that revises 900
cells at once has not revised, it has redefined.

## Where it runs

The report is generated by the daily workflow after validation and written to the run log. It does
**not** block: a revision is a legitimate event, and failing the build on one would be the same
mistake as failing on `LOCAL_UNITS_BY_COMMUNE` being three years stale.

A `row_collapse`-style guard already covers the pathological case where a "revision" is actually a
source returning garbage.

## Tests

- A fixture with a known revision, asserted from **both** time perspectives: before the revision the
  as-of query returns the old value, after it the new one, and `is_latest` returns the new one in
  both cases.
- Unchanged value → no new vintage, asserted on the row count rather than on a log line.
- Changed **status** at unchanged value → new vintage. This is correction 1's regression test, and
  it fails today against `sync_population.py`.
- `as_of` earlier than every vintage for a cell → no row, not the earliest row.
- `as_of` as a bare date returns values published later that same day.
- A vintage written with a non-UTC offset still orders correctly.

## Open questions for the maintainer

- **Storage projection for the `[REVIEW]` step.** At 200 indicators × 581 communes × 20 years the
  base is ~2.32M rows before any revisions; at the ~414 bytes/row measured for the population data
  that is ~960 MB, which the committed-store split (ADR 0002) already anticipated. The revision
  multiplier is currently measured at **1.00×** — no revision has ever been recorded — so the
  question is whether to plan against the measured rate or a guessed one.
- **Should `sync_population.py`'s predicate be fixed in this block or in the suppression work?**
  Fixing it here is cheap and closes a latent hole; deferring it keeps this block to query-layer
  work. The defect cannot fire until suppression handling exists either way.
- **Does the legacy path's `fetch_runs` logging get retired rather than documented?** Correction 3
  is the second block in a row to trip over this column.
