# ADR 0006 — Split the committed stores by volume, and never publish from the committed database alone

Date: 2026-09-13
Status: accepted
Revises: [ADR 0002](0002-split-committed-stores.md)'s split axis. ADR 0002's decision for the
hand-loaded stores stands unchanged; what changes is the rule for data CI fetches itself.

## Context

The daily run of 2026-09-13 ([run 34750548379](https://github.com/mdruszcz/belgian-macro-pipeline/actions/runs/34750548379))
fetched every source, then refused to commit: `data/belgian_macro.db` had reached 42 MB against the
workflow's 39.06 MiB guard. Nothing was saved that day.

ADR 0002 split the stores by **how data can arrive**: hand-downloaded sources to committed CSVs,
anything CI can fetch into the committed SQLite file. Under that rule ONEM (2026-09-06), ONEM's
published rate (2026-09-12) and WalStat (2026-09-11) all correctly went into the database, because
CI fetches them. They took it from 1.7 MB to 39.7 MB: 84,031 of its 85,970 observations.

ADR 0002 had already measured why that could not last — 60 % of the file was index, and SQLite pages
do not delta in git — and the measurements repeated on 2026-09-12: 62 % index (24.5 MB of index on
14.3 MB of data), zero free pages, so `VACUUM` could not help. The binding constraint was never
provenance. It is **volume and churn**.

Two things made the obvious fix (dump the two sources to CSV and delete them from the database)
unsafe on its own:

1. **Vintages need the history in the database while a sync runs.** `src/db/vintages.py` writes a
   new vintage only when `(value, status)` differs from the current `is_latest` row. A sync run
   against a database without ONEM's history would see every cell as new, every day.
2. **Everything downstream read the committed database.** CI's `validate_data.py` defaulted to it;
   stripped, `indicator_disappeared` fires on eighteen indicators, CI goes red, the bot's PR never
   auto-merges, and the next daily run aborts on the still-open PR — permanently. `manual_sources.yml`
   exported from it too, and would have published every commune page without ONEM or WalStat, in
   files that get *smaller*, so no guard would notice.

## Decision

1. **The split axis is volume and churn.** A store goes to a committed CSV when it is big or
   changes often, whoever fetches it. ONEM, ONEM's published rate and WalStat move to
   `data/onem_observations.csv`, `data/onem_rates_observations.csv` and
   `data/walstat_observations.csv`, in ADR 0002's 10-column shape, sorted by primary key. The
   database keeps national macro, `LOCAL_UNITS_BY_COMMUNE`, geography, every reference row,
   the legacy tables, the fetch log and the volume history: 1,939 observations, 1.9 MB.

2. **Two kinds of store, declared once** in `config/stores.yaml` (PR 1): `extra_csv` for the
   hand-loaded ones, merged at export time and never loaded into a database, and `in_db` for the
   three above.

3. **Nothing reads the committed database on its own.** `scripts/build_staging_db.py` assembles
   `data/local/working.db` (gitignored): the committed database plus every `in_db` CSV, with
   vintages and `is_latest` exactly as committed. Every sync, the validation and every export read
   that copy — in the daily run, in `manual_sources.yml`, in CI and in `make all`.
   `validate_data.py` defaults to it and refuses to run when it is absent, rather than falling back.

4. **The committed database is written once, last, by one script.** `scripts/offload_stores.py`
   runs after validation and every export. One row set — the `in_db` stores' declared indicators —
   drives both the dump and the delete. It builds everything in scratch files, checks that nothing
   offloaded is left, that no row already in a committed CSV is missing from the working copy
   (observations are append-only, so a missing one means the copy was not assembled), that foreign
   keys and integrity are clean, and only then moves the files into place. A refusal anywhere leaves
   every committed file as it was. It also refuses when an ONEM or WalStat indicator appears that no
   store declares, so a new series cannot quietly re-grow the database.

5. **The daily workflow runs every sync before validation.** ONEM, ONEM's rate and WalStat used to
   sync after the exports: never validated, and published a day late. The manifest's validation
   status is now derived from the validation step, not typed as `pass`. A day on which any source
   failed still opens its PR but no longer auto-merges it.

## Consequences

- The committed database goes from 39.7 MB to 1.9 MB; three CSVs add 12.8 MB of text, which git
  deltas. A day's ONEM or WalStat change is a readable line diff in the bot's PR.
- Verified on the real data before merging: the working copy assembled from the stripped database
  and the three CSVs holds exactly the 85,970 rows of the old database (every column but
  `fetch_run_id`), all seven exports built from it are byte-identical to those built from the old
  database, and a second assemble-and-offload reproduces the three CSVs byte for byte.
- `fetch_run_id` is not carried in the CSVs (ADR 0002). An ONEM row's link to the run that fetched
  it is lost once it is offloaded; the run itself stays in `fetch_runs`, so `fetch_silence` and
  `fetch_error` are unaffected. The rebuild runs the assemble step opens are dropped by the offload.
- `sqlite_sequence` for `fetch_runs` advances by three each day (the rebuild runs). Harmless.
- The committed database is not byte-stable across runs (`VACUUM` output depends on the SQLite
  version). CLAUDE.md rule 35 applies to the CSVs and the published exports, which are.
- Every run spends ~45 s assembling (measured locally, 84,031 rows).
- `UNEMPLOYMENT_RATE_BIT` is configured under WalStat but had no rows on the day of the cutover. An
  `in_db` store may therefore declare an indicator its CSV does not contain yet;
  `src/stores.py` tolerates exactly that, for `in_db` stores only.

### What this does not fix

- `data/communes_history.csv` is 36.9 MB against the same 39.06 MiB guard, and moving the syncs
  before the exports makes today's new ONEM period land in today's file rather than tomorrow's.
  Freeing the database does nothing for it; it is the next wall.
- `sync_onem_rates.py` still keeps 18 months of the monthly rate. That window existed only because
  of the database's size (ADR 0005); widening it is a data change of its own.

### Rejected

- **Dropping the indexes before commit** (known-risks.md, 2026-09-12): gives 21.96 MB, still a
  binary rewritten daily and still growing with every municipal series.
- **Assembling in place into `data/belgian_macro.db`.** Every interruption would leave a 40 MB
  file half-stripped or half-loaded in the worktree; with the working copy, an interrupted run is a
  no-op by construction.
- **Loading all nine stores into the working database.** The six hand-loaded CSVs would then be
  both in the database and passed as `--extra-observations`, and `export_communes_history_csv.py`
  would take every row twice.
- **PostgreSQL.** At ~86,000 observations the problem is versioning a binary file in git, not
  SQLite's capacity.
