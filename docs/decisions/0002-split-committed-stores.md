# ADR 0002 — Split the committed stores by how data can arrive

Date: 2026-09-06
Status: accepted
Supersedes: nothing. Revises an operational assumption stated in
`docs/architecture.md` ("One SQLite file … checked into git and overwritten by every daily run"),
not the schema frozen by [ADR 0001](0001-data-model.md), which is unchanged.

## Context

`data/belgian_macro.db` had reached 12.8 MB and was re-committed by `daily_fetch.yml` **every day**.
Measured breakdown of what was in it:

| | rows | size | fetchable automatically? |
|---|---|---|---|
| National macro + `LOCAL_UNITS_BY_COMMUNE` | 1,939 | 1.05 MB | Yes — live NBB/DBnomics/Bestat APIs |
| `POPULATION_BY_COMMUNE` + 3 age bands | 25,532 | 10.56 MB | **No, and never** |

Population comes from Statbel's `TF_SOC_POP_STRUCT` bulk files. `statbel.fgov.be` is unreachable
from this pipeline's network context — confirmed three separate ways (curl IPv4, curl IPv6, and an
independent fetch path), with DNS resolving fine, so it is a connection-level block rather than DNS
or a 403. The files are downloaded by hand and are ~100 MB each.

So **95% of the bytes were data that can only ever arrive manually, and that had changed three times
in its entire life — yet were re-committed daily** because the other 5% moves. SQLite is binary, so
each daily commit stored a fresh ~1.6 MB object that git cannot delta: roughly 600 MB of history a
year, almost all of it re-storing unchanged population figures.

Two further facts made this urgent rather than cosmetic:

- The `find data/ -size +25000k` guard in `daily_fetch.yml` was already ~50% consumed.
- Indexes were 60% of the file — 7.7 MB of index on 4.2 MB of data, across six indexes on
  `observations` — so the growth multiplier is ~466 bytes per row, not the ~140 bytes the row data
  suggests. At the roadmap's own target scale (581 communes × 200 indicators × 20 years ≈ 2.3M rows)
  the file would be roughly **1.1 GB**, and would cross GitHub's hard 100 MB per-file limit at only
  ~226k rows.

## Decision

Split the committed stores **by how the data can arrive**, not by subject matter and not by
geography level:

1. **`data/belgian_macro.db`** keeps everything that CI can fetch on its own — national macro plus
   `LOCAL_UNITS_BY_COMMUNE`, which is municipal but comes from the reachable Bestat API. Committed
   daily, as before. Now **1.7 MB**.
2. **`data/population_observations.csv`** holds the manual-only store, as **text, not a second
   `.db`**. Deterministically sorted by the observations primary key, so re-exporting unchanged data
   yields an empty diff. Changes only when the maintainer hand-loads a refresh. ~3.6 MB.
3. The queryable manual database becomes a **disposable local build artefact** at `data/local/`,
   gitignored and rebuilt from the CSV by `scripts/load_observations_csv.py`.

CSV rather than a second SQLite file because git deltas text and cannot delta SQLite pages, and
because a population refresh then shows up as readable line diffs in the pull request instead of an
opaque binary blob.

`scripts/export_communes_csv.py` gains `--extra-observations`, merging the CSV store with the
database at export time. Chosen over SQLite `ATTACH`, which appears nowhere in this repo and would
have forced CI to rebuild a database from the CSV purely to query it.

## Consequences

- Daily git churn drops from ~1.6 MB/day to ~0.1 MB/day.
- `data/communes_export.csv` and `data/belgian_macro_export.csv` are **byte-identical** across the
  change — verified by checksum. Only where the rows are stored moved; no published number changed.
- `observations.fetch_run_id` is `NOT NULL` with a foreign key, but a run id is meaningless outside
  the database that produced it. The CSV therefore omits it, and the loader opens a single run row
  marked as a rebuild rather than a real fetch.
- Reference rows in `indicators` for the population indicators **stay in the database** even though
  their observations do not. They are catalogue metadata, they cost a few hundred bytes, and keeping
  them means the commune export has exactly one metadata path for both stores.
- The same two fiscal-income datasets are blocked on the identical cause, so this path takes them
  later without redesign.

### What this does not fix

Index overhead (~60% of the automated file) and the ~466 bytes/row multiplier are untouched. They
stop mattering at present scale because the file is now small, but the *automated* store will hit the
same wall eventually if indicator count grows as the roadmap projects. Options deliberately left for
later, with measurements recorded here so the analysis need not be redone: normalising
`indicator_id`/`geo_id` to integer keys, storing `vintage`/`created_at` as epoch integers (measured:
−1.9 MB at the time), or the Postgres move already planned in Block AB.

### Rejected

- **Dropping the `legacy_*` tables** (0.68 MB). They *look* dead but are live: `belgian_macro_db.py`
  recreates and repopulates them on every daily run, and `scripts/sync_to_canonical.py` reads
  `legacy_observations`. Dropping them would regress on the next run and risk breaking the pipeline.
- **Not committing the manual store at all**, publishing only the derived export. Smallest repo, but
  the canonical population observations would exist only on one machine, with no durable provenance
  and no way for a fresh clone to rebuild them.
- **Raising the 25 MB guard.** Treats the symptom; GitHub hard-fails at 100 MB regardless, and the
  history bloat continues.
