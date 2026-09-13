# Pipeline repair, PR 1 — the store registry and staging machinery

```
Batch:                 PR 1 of the pipeline repair (not a numbered redesign batch)
Base commit:           23439dba (origin/develop)
Branch:                feat/store-registry-staging
Files changed:         config/stores.yaml (new), docs/features/store_registry.schema.json (new),
                       src/stores.py (new), scripts/ensure_reference_rows.py (new),
                       scripts/build_staging_db.py (new), scripts/sync_population.py,
                       scripts/load_observations_csv.py, scripts/export_communes_csv.py,
                       scripts/export_communes_history_csv.py, scripts/export_aggregates_csv.py,
                       scripts/export_percentiles_csv.py, scripts/validate_data.py,
                       src/validation/rules.py, Makefile, .github/workflows/daily_fetch.yml,
                       .github/workflows/manual_sources.yml, README.md,
                       tests/test_committed_stores_are_consistent.py,
                       tests/test_export_aggregates_csv.py,
                       tests/test_observations_csv_roundtrip.py, tests/test_validation_rules.py,
                       tests/test_stores.py (new), tests/test_build_staging_db.py (new)
Requirements completed: config/stores.yaml + src/stores.py (single store declaration,
                       mode: in_db|extra_csv), the four exporters take --stores,
                       scripts/ensure_reference_rows.py, scripts/build_staging_db.py +
                       `make assemble`, the forged-heartbeat fix, sync_population.py's
                       --reference-rows-only hole closed, all required tests
Deferred requirements: wiring `make all`/CI/workflows to the working copy (PR2, deliberately
                       out of scope -- the handoff calls this an atomic cutover)
Data-contract impact:  none. This PR moves no data and changes no published artefact
                       (verified below).
Commands executed:     see "Verification"
Tests passed:          59 new (22 test_stores.py, 4 test_build_staging_db.py, 4 new in
                       test_observations_csv_roundtrip.py, 13 (was 6) in
                       test_committed_stores_are_consistent.py, 2 new in
                       test_validation_rules.py); full suite:
                       `pytest tests/ -q` -> 3782 passed, 7 failed, 20 skipped, all 7
                       failures pre-existing and unrelated (see "Verification")
Reviewer findings:     pending (auditor not yet run)
Known limitations:     below
Rollback procedure:    revert the branch; no data files were moved or deleted, only config,
                       scripts and workflow wiring
Next PR:               PR 2 -- move ONEM and WalStat out of data/belgian_macro.db into
                       committed CSVs as `in_db` stores, and wire `make all`/CI to
                       scripts/build_staging_db.py's working copy
```

## What this batch does

Builds the machinery PR 2 needs to actually move data, without moving any data itself
(CLAUDE.md rule 10). The daily pipeline is broken because `data/belgian_macro.db` is 39.7 MB
against a 39.06 MiB commit guard; the fix is offloading ONEM and WalStat into committed CSVs,
but the six CSVs that already exist that way were spelled out independently in six places,
three of which disagreed. This PR makes there be exactly one list.

### `config/stores.yaml` + `src/stores.py`

One declaration per committed observation store: `path`, `source_id`, `indicators` (the exact
distinct `indicator_id` values in that store's CSV, verified against the real files with
`cut -d, -f1 | sort -u`, not typed from memory), `mode` (`in_db` or `extra_csv`), and
`reference_rows` (a script + args, or `null` with a required `reference_rows_reason`).
Validated against `docs/features/store_registry.schema.json` on load. All six existing stores
are `extra_csv` in this PR — being in both modes would double-count a store, since
`export_communes_history_csv.py` takes every `is_latest=1` DB row *and* every CSV row with no
dedup.

Writing the acceptance test below turned up a **fourth** independent copy of the store list, not
named in the handoff: `tests/test_export_aggregates_csv.py`'s own hardcoded `STORES` tuple had
already dropped `police_observations.csv` (5 of 6), the exact class of drift this PR exists to
close. Fixed the same way as the other three: now reads `extra_csv_paths(DEFAULT_STORES_PATH)`.
`README.md`'s local-usage example was also still showing the old two-flag
`--extra-observations` form; updated to `--stores config/stores.yaml` to match.

`python -m src.stores --verify-indicators` prints the registry and cross-checks every store's
declared indicators against its CSV's real contents.

### The four exporters take `--stores`

Not a flag-printing CLI (rejected in the handoff: a `$(shell ...)` Makefile variable computed
at parse time would silently become empty on any Python error, publishing commune files
missing six sources with exit code 0). Instead each of `export_communes_csv.py`,
`export_communes_history_csv.py`, `export_aggregates_csv.py` and `export_percentiles_csv.py`
gained `--stores` (default `config/stores.yaml`) and reads the registry itself at the point of
use. Precedence, implemented in `src.stores.resolve_extra_observations`: an explicit
`--extra-observations` wins outright and the registry is not consulted — this is what keeps
`tests/test_committed_stores_are_consistent.py`'s subprocess test working unchanged instead of
double-counting every store. Only when no `--extra-observations` is given does `--stores`
supply the `extra_csv` paths. `--stores ''` disables the fallback for a bare export.

The Makefile and both workflows now pass `--stores config/stores.yaml` (or rely on the
default) instead of six repeated `--extra-observations` flags each.

### `scripts/ensure_reference_rows.py`

Replaces the six `--reference-rows-only` calls duplicated across the Makefile's `reference`
target and both workflows. Runs each store's registered script as a subprocess (the six
scripts don't share an argument signature, so `reference_rows.args` in the registry already
*is* the CLI contract) and stops at the first failure.

**The hole found and closed:** `sync_population.py` had no `--reference-rows-only` flag at
all — `make reference` had no population line, and the `POPULATION_*` reference rows existed
only because the database itself was committed. Closed by adding the flag (a small, honest
change mirroring the other five scripts' pattern: insert the reference rows, then return
before touching `data/raw/`), not by marking it null in the registry. Verified: running
`--reference-rows-only` against a fresh DB with no `data/raw/statbel/population/` directory
present succeeds and touches nothing else.

The second hole named in the handoff — only `sync_onem_rates` appears in `Makefile:56`, and
ONEM/WalStat have no registry entry — is real but is PR 2's territory (they are not committed
CSVs yet, they are fetched live into the database). `make reference` keeps that one line
outside the registry loop, commented to say why.

### `scripts/build_staging_db.py` + `make assemble`

Assembles `data/local/working.db`: `rm -f` it (and stale `-shm`/`-wal` siblings) → copy
`data/belgian_macro.db` → migrate → load geography → ensure reference rows → load every
`in_db` store. That set is empty in this PR, and the script is tested specifically for
correctness with zero `in_db` stores (`tests/test_build_staging_db.py`). Not wired into
`make all`, CI or either workflow — that cutover is PR 2's, kept atomic on purpose.

### The forged-heartbeat fix

`load_observations_csv.py` hardcoded `source_id='statbel', adapter='statbel', status='ok'` on
every rebuild of a disposable local database from a committed CSV. `fetch_silence` reads
`MAX(started_at)` per `source_id`; a `sync_population.py` rebuild running daily would forge
Statbel's heartbeat every day and permanently defeat the one rule that exists to catch a
source going quiet.

**Chosen fix:** `--run-source-id`/`--run-adapter` were added (default `adapter='rebuild'`),
and `fetch_silence` and `fetch_error` in `src/validation/rules.py` now both exclude
`adapter = 'rebuild'` from their queries entirely, rather than trying to keep `started_at`
honest. The rejected alternative — setting `started_at` from the CSV's max `created_at` — was
rejected because `created_at` is when the *observation* was recorded, which for population is
years in the past for old rows; that would make every rebuild look like "last fetched years
ago" and trip the very rule this is meant to leave alone. Excluding the adapter is unambiguous:
a rebuild is definitionally not a fetch, under either name. Regression tests for both rules are
in `tests/test_validation_rules.py` (`test_fetch_error_ignores_a_rebuild_run_even_if_the_real_fetch_failed`,
`test_fetch_silence_ignores_a_rebuild_run`), reproducing the exact forged-heartbeat sequence
(a failed real fetch followed by an 'ok' rebuild) and confirming it is still caught.

## Verification

**1. Byte-identical exports.** Hashed `data/communes_export.csv`, `data/communes_history.csv`,
`data/aggregates.csv`, `data/percentiles.csv` produced by the exact commands the Makefile
issues (`--extra-observations` × 6, pre-change) against the same commands post-change
(`--stores config/stores.yaml`, and separately with no flag at all relying on the new default).
All four matched byte-for-byte in every combination tried, and the explicit
`--extra-observations` path (still used by the existing consistency test) was also confirmed
not to double-count once `--stores` defaults alongside it.

`make` is not installed in this Windows environment (`which make` → not found), so this was
done by running the exact commands `make exports` issues, rather than through `make` itself.

**2. `make assemble` (`scripts/build_staging_db.py`) twice.** Also could not run through `make`
directly for the same reason; ran `python scripts/build_staging_db.py` twice via its own
Python entry point against a freshly-migrated source DB. Second run succeeded (no
`IntegrityError`) and the resulting `data/local/working.db` was byte-identical to the first
run's output. Covered by `tests/test_build_staging_db.py::test_assemble_is_idempotent`.

Ran once against the real committed `data/belgian_macro.db` too, and hit a **pre-existing,
unrelated** failure: `src.db.migrate` refuses with a checksum mismatch on
`001_core_schema.sql`. This reproduces identically on unmodified `develop` with none of this
PR's changes (`python -m src.db.migrate --db data/belgian_macro.db` fails the same way against
a clean checkout). Root cause: this machine has `git config core.autocrlf true`, so
`migrations/001_core_schema.sql` checks out with CRLF line endings, and its SHA-256 no longer
matches the checksum recorded in the committed database's `schema_migrations` table (computed
against the LF version). Not touched here — fixing it means either changing `.gitattributes`
or accepting a new migration checksum, both out of scope for a store-registry PR (rule 10) and
neither specific to Windows CI, which presumably checks out LF. Flagged for the maintainer as a
separate, pre-existing risk.

**3. `pytest tests/ -q -p no:cacheprovider`.** `3782 passed, 7 failed, 20 skipped in 562s`.
Six of the seven failures match the handoff's pre-authorized list exactly (three symlink tests,
`WinError 1314` -- Windows requires a privilege this account does not have to create a symlink;
three socket tests, connection-reset errors under `tests/builder/` and `tests/security/`). The
seventh, `tests/builder/test_builder_transaction.py::test_atomic_write_text_crash_before_replace_leaves_destination_untouched`,
was not named in the handoff's list, so it was checked rather than assumed: run in isolation it
fails with an `AssertionError` (the destination file's hash does not match the pre-crash hash
after a monkeypatched `os.replace` is made to raise) -- a Windows file-write-ordering quirk, not
anything this PR touches. Confirmed with `git diff origin/develop --stat -- src/builder/
tests/builder/ tests/security/`, which returns **empty** -- every file these seven tests exercise
is byte-identical to `develop`, so all seven are pre-existing on this machine regardless of this
PR; none is caused by anything in this diff.

**4. `black --check .` and `ruff check .`**, repo-wide: both clean after formatting the new and
changed source/test files (`black` reformatted 3, `ruff --fix` reorganized one import block).

## Assumptions and open questions

- **Which two "holes" to close, per the handoff:** `sync_population.py` got the
  `--reference-rows-only` flag (small, honest addition); ONEM/WalStat stay out of the registry
  entirely (they are not committed CSVs today, so they have nothing to register) rather than
  being added as `mode: in_db` with a null `reference_rows` — adding them as stores before PR 2
  actually moves their data would misrepresent what is committed today.
- **Heartbeat fix:** `adapter='rebuild'` excluded from both `fetch_silence` and `fetch_error`,
  not just `fetch_silence` as the handoff's own phrasing focused on — `fetch_error`'s "most
  recent run per source" query has the identical exposure (a rebuild's manufactured 'ok' would
  mask a real error moments before it), so both were fixed together for one coherent invariant:
  a rebuild is never mistaken for a fetch, anywhere the fetch log is read.
- **`--stores` precedence:** explicit `--extra-observations` wins outright over `--stores`
  rather than the two being merged, specifically so the existing subprocess test (which builds
  its own `--extra-observations` list from the registry) is not double-counted. Documented in
  `src.stores.resolve_extra_observations`'s docstring.
- **The acceptance test's scope:** written as a parametrized check over the specific files that
  used to hand-enumerate the six-store list (Makefile, `daily_fetch.yml`, `validate_data.py`,
  the consistency test, and the four exporters) rather than a literal whole-repository grep.
  A repo-wide grep for `_observations.csv` also matches `README.md`, several `docs/features/*.md`
  pages, and other tests (`test_census2021.py`, `test_fiscal_income.py`, ...) that legitimately
  name *one* store's own CSV to test or document that one source in isolation — not the "the
  list is duplicated N times" defect this PR fixes. Rewriting those would be an out-of-scope
  change under rule 10. `manual_sources.yml` keeps one remaining reference
  (`--csv data/population_observations.csv` in its round-trip-validation step) for the same
  reason: it is testing that one specific store's round trip, not re-declaring the list.

## What this batch did NOT do

- Did not move ONEM, WalStat, or any other data between the database and a CSV. Every existing
  store stays `extra_csv`; `data/belgian_macro.db`'s contents are unchanged.
- Did not wire `make all`, `ci.yml`, `daily_fetch.yml`'s main pipeline, or `manual_sources.yml`'s
  main pipeline to `scripts/build_staging_db.py`'s working copy. `make assemble` exists and is
  tested standalone; nothing else calls it yet.
- Did not touch the commit-size guard, `assets/`, any page, or any exporter's output *format*
  (only how each exporter's `--extra-observations` list is assembled).
- Did not fix the pre-existing CRLF/migration-checksum issue described above.

## Risks noticed

- The CRLF/checksum drift above will also block `make schema`/`make all`'s `schema` target on
  any Windows clone with `core.autocrlf=true`, independent of this PR. Worth a maintainer
  decision (pin `.gitattributes` for `migrations/*.sql` to `-text` or `eol=lf`, or accept it as
  a Windows-only local nuisance since CI runs on Ubuntu).
- `scripts/ensure_reference_rows.py` stops at the first failing store rather than reporting all
  six. That matches CLAUDE.md rule 13's "fail loudly" but differs from `validate_data.py`'s
  "print every violation" philosophy; worth revisiting if a maintainer wants one failure report
  covering all stores at once.
