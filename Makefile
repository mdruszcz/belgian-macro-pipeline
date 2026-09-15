# One-command rebuild -- the 50% gate's "clone -> install -> pipeline -> tests
# -> build runs with one command".
#
# THE SPLIT THAT MATTERS: `make all` rebuilds every published artifact from
# what is COMMITTED, with no network and no secrets, and then runs the tests.
# Fetching from the outside world is a separate target (`make fetch`) because
# it cannot work on a fresh clone: statbel.fgov.be and onem.be are unreachable
# from most networks (docs/features/manual_sources.md), and four of this
# pipeline's municipal sources are hand-downloaded files that live under
# data/raw/ which is gitignored. A `make all` that tried to fetch would fail
# on every fresh clone, which is the opposite of what this target is for.
#
# So: `make all` proves the repository can regenerate its own published output.
# `make fetch` is what CI runs daily to bring in new data.

PYTHON ?= python
# Two databases since the ONEM/WalStat cutover (docs/decisions/0006).
# COMMITTED_DB is the small committed file; nothing in `make all` writes it.
# DB is the disposable working copy `make assemble` builds from it plus every
# in_db store's CSV -- what every sync, the validation and every export read.
# Exporting from the committed file alone would publish pages with ONEM and
# WalStat silently missing.
COMMITTED_DB ?= data/belgian_macro.db
DB           ?= data/local/working.db

# The one declaration of every committed observation store (config/stores.yaml,
# src/stores.py) -- PR1 of the pipeline repair. Replaces the old EXTRA
# variable, which used to list all six manual stores' --extra-observations
# flags by hand here, a SECOND TIME in each of the two workflows, and a
# THIRD, incomplete time in scripts/validate_data.py and
# tests/test_committed_stores_are_consistent.py.
#
# DELIBERATELY A PLAIN STRING, NOT `$(shell ...)`. A $(shell) variable runs at
# Makefile PARSE time, on every invocation including `make help` -- if Python
# were missing or the registry failed to load, EXTRA would silently become
# empty and `make exports` would publish commune files missing six sources
# with exit code 0. Passing the path instead means each exporter reads and
# validates the registry itself, at the point it is actually used, and fails
# loudly if it cannot (CLAUDE.md rule 13).
STORES := config/stores.yaml

# Dagster's run history for the local UI -- gitignored with the rest of
# data/local/. Must be absolute.
DAGSTER_HOME ?= $(CURDIR)/data/local/dagster_home

.PHONY: all install schema reference validate exports pages shell-sync page-documents site-index boundaries builder assemble offload test fetch sync-nuts2 clean help dagster dagster-daily verify-dagster-parity

## all: install deps, assemble the working database from what is committed,
## validate it, regenerate every published export, and run the tests. No
## network, and the committed database is left untouched. This is the gate target.
all: install assemble validate exports test-full
	@echo ""
	@echo "Rebuilt from committed data and tests pass."

## install: python dependencies
install:
	$(PYTHON) -m pip install -q -r requirements.txt

## schema: apply migrations and load the geography reference data into $(DB).
## Both are idempotent and offline -- geography comes from config/geography/,
## not the network. `assemble` already does this; kept for ad hoc use.
schema:
	$(PYTHON) -m src.db.migrate --db $(DB)
	$(PYTHON) scripts/load_geography.py --db $(DB)

## reference: indicator/source metadata rows for the manual sources. Their
## OBSERVATIONS live in committed CSVs, but their name/unit come from the
## indicators table, so the exporters need these rows present. Config only:
## no network, no workbook. Driven by $(STORES) (scripts/ensure_reference_rows.py)
## rather than one hand-written line per store -- this used to be six lines
## here, again in each workflow, and used to have NO line for population
## (sync_population.py had no --reference-rows-only flag until PR1 of the pipeline repair).
##
## ONEM, ONEM's rate and WalStat are registry stores too since the cutover,
## so the one line that used to sit here for sync_onem_rates.py is gone.
## `assemble` already does this; kept for ad hoc use.
reference:
	$(PYTHON) scripts/ensure_reference_rows.py --db $(DB) --stores $(STORES)

## validate: Block H's rules. Fails the build on a data problem, which is the
## entire point of it existing (a validation step that only logs is decoration).
validate:
	$(PYTHON) scripts/validate_data.py --db $(DB)

## exports: every published artifact, in dependency order -- the site payloads
## read the bulk CSVs, and the static pages read the site payloads.
exports:
	$(PYTHON) scripts/export_canonical_csv.py --db $(DB) --out data/belgian_macro_export.csv
	$(PYTHON) scripts/export_communes_csv.py --db $(DB) --out data/communes_export.csv --stores $(STORES)
	# Two passes, on purpose. _full feeds the two internal steps below, whose
	# own contract is the complete history (site_payloads.md); the second
	# pass, with no --all-periods, is the trimmed last-10-years file that
	# actually gets committed and offered as a download -- data/communes_history_full.csv
	# is gitignored so it never reaches the commit-size guard it exists to avoid.
	$(PYTHON) scripts/export_communes_history_csv.py --db $(DB) \
		--out data/communes_history_full.csv --all-periods --stores $(STORES)
	$(PYTHON) scripts/export_communes_history_csv.py --db $(DB) --out data/communes_history.csv --stores $(STORES)
	$(PYTHON) scripts/export_communes_table_json.py \
		--communes-history data/communes_history_full.csv --out data/communes_table.json --db $(DB)
	$(PYTHON) scripts/export_aggregates_csv.py --db $(DB) --out data/aggregates.csv --stores $(STORES)
	$(PYTHON) scripts/export_percentiles_csv.py --db $(DB) --out data/percentiles.csv --stores $(STORES)
	$(PYTHON) -m src.exporters.metadata --out data/metadata/indicators.json
	$(PYTHON) scripts/export_site_payloads.py --db $(DB) \
		--communes-history data/communes_history_full.csv \
		--communes-latest data/communes_export.csv \
		--national data/belgian_macro_export.csv \
		--aggregates data/aggregates.csv \
		--percentiles data/percentiles.csv \
		--out-dir public/data --build-id "$${BUILD_ID:-local}" --validation-status unknown
	$(PYTHON) scripts/export_commune_adjacency.py
	$(PYTHON) scripts/export_commune_typology.py
	# Reads the two CSVs written above -- data/communes_history.csv (the
	# trimmed file that is actually committed and offered for download) and
	# data/belgian_macro_export.csv -- and shards them one file per
	# indicator for explorer.html. Deliberately built from the published
	# downloads rather than from the database, so the page cannot show a
	# figure the download does not have. Must run AFTER both.
	$(PYTHON) scripts/export_explorer_payloads.py
	# Europe NUTS 2 (batch B2). Reads only the committed data/nuts2/*.csv
	# store, config/geography/nuts2.csv and the committed geometry -- no
	# $(DB) involved, so it is safe here regardless of whether sync-nuts2
	# has ever been run (a not-yet-loaded indicator publishes as "blocked",
	# not missing silently).
	$(PYTHON) scripts/export_europe_nuts2.py
	# Europe countries (map country mode + comparison charts,
	# docs/features/europe_countries.md). Same shape as the NUTS 2 step above:
	# reads only the committed data/international/*.csv store and the
	# committed NUTS 0 geometry, no $(DB) involved.
	$(PYTHON) scripts/export_europe_countries.py
	$(MAKE) pages
	$(MAKE) shell-sync
	$(MAKE) page-documents
	$(MAKE) site-index

## pages: the permanent /local/{nis} routes. Separate target because it is the
## slowest step and is often what you want to re-run alone while iterating.
pages:
	$(PYTHON) scripts/export_local_pages.py --db $(DB) \
		--payload-dir public/data --out-dir local --build-id "$${BUILD_ID:-local}"

## shell-sync: write the shared header/footer/bootstrap (src/pages/shell.py)
## into home2.html's and macro.html's delimited bp-shell zones (Batch A1.1,
## docs/features/site_unification.md). Runs BEFORE page-documents: both read
## the same src.pages.shell module, so if that module changed, the two
## hand-edited pilots and every generated page pick up the change in the same
## `make exports` run rather than one of them silently lagging.
shell-sync:
	$(PYTHON) scripts/sync_site_shell.py

## page-documents: build every config/pages/*/published.json into its route,
## in all three languages -- en at the declared route, fr and nl one directory
## deeper (Batch 15c). Only PUBLISHED documents: a draft never reaches the site
## without an explicit publish (invariant 10). Fast, so it runs inside `exports`.
page-documents:
	$(PYTHON) scripts/export_page_documents.py

## site-index: robots.txt and the sitemap index. Runs AFTER pages and
## page-documents, because it refuses to name a sitemap or submit a URL that
## is not on disk yet.
site-index:
	$(PYTHON) scripts/export_site_index.py

## boundaries: rebuild data/geo/communes.geojson from the Statbel statistical-
## sectors file. NOT in `all`: the 227 MB source is a hand-downloaded file under
## gitignored data/raw/, so a fresh clone cannot run this -- the OUTPUT is
## committed instead, which is what map.html actually reads. Re-run this only
## when Statbel publishes a new boundary vintage.
boundaries:
	$(PYTHON) -m pip install -q -r requirements-geo.txt
	$(PYTHON) scripts/build_commune_boundaries.py --db $(DB)

## builder: run the local page builder shell on 127.0.0.1:8787 (Ctrl-C to stop).
## Loopback only, one session token per process, printed as the URL to open.
## Editing saves a draft; nothing becomes public until you publish explicitly,
## and nothing is ever committed for you. Needs the published payloads, so run
## `make exports` first if you have run `make clean`. NOT part of `all`.
builder:
	$(PYTHON) scripts/serve_builder.py

## assemble: build the disposable working database at $(DB) from
## $(COMMITTED_DB) plus every `in_db` store in $(STORES)
## (scripts/build_staging_db.py): copy, migrate, geography, reference rows,
## then ONEM, ONEM's rate and WalStat with their vintages intact. Rebuilt from
## scratch every time, so it is safe to re-run.
assemble:
	$(PYTHON) scripts/build_staging_db.py --source-db $(COMMITTED_DB) --working-db $(DB) --stores $(STORES)

## offload: the reverse of assemble. Dumps every in_db store from $(DB) to its
## committed CSV and writes $(COMMITTED_DB) without those rows
## (scripts/offload_stores.py). Refuses, changing nothing, if a committed row
## would be lost. NOT part of `all` -- only needed after `make fetch`, to
## commit what was fetched. The daily run (and `make dagster-daily`) calls the
## same offload() as the last asset of its Dagster run, committed_stores.
offload:
	$(PYTHON) scripts/offload_stores.py --working-db $(DB) --committed-db $(COMMITTED_DB) --stores $(STORES)

## test: THE EVERYDAY LOOP -- every test that needs neither a browser nor the
## generated site. Sub-minute is the target, because a 14-minute default loop
## is one nobody runs. The tiers are pytest markers (pyproject.toml); the
## browser one is applied automatically by tests/conftest.py.
##
## Sequential, deliberately. -n auto measured 91s -> 41s on 8 cores, but it
## also surfaced two NEW failures nothing else did: test_post_with_no_origin_is_forbidden
## and test_post_with_other_loopback_port_origin_is_forbidden, both
## ConnectionAbortedError. Root cause, confirmed by reading the code rather than
## guessed: tests/builder/*'s `_free_loopback_port()` probes a port by binding to
## 0, reading it back, then CLOSING the probe -- and `BuilderServer` sets
## `allow_reuse_address = True` (src/builder/service.py), so if two xdist workers'
## probes land on the same freed port in that window, BOTH servers bind
## successfully instead of one failing loudly, and requests land on whichever's
## listening socket the OS hands them. pytest-xdist stays a declared dev
## dependency for ad hoc use (`pytest -n auto path/to/file.py`), where the
## collision probability is negligible -- just not wired into the default here
## until the port-selection race is fixed. See known-risks.md.
test:
	$(PYTHON) -m pytest tests/ -q -m "not browser and not generated_site and not slow"

## test-browser: the Playwright suite, one Chromium for the whole session.
test-browser:
	$(PYTHON) -m pytest tests/ -q -m browser

## test-generated-site: the sweeps over local/ and preview/ output, plus
## anything marked slow.
test-generated-site:
	$(PYTHON) -m pytest tests/ -q -m "generated_site or slow"

## test-full: everything -- what `make all` and CI's `test` gate run.
test-full:
	$(PYTHON) -m pytest tests/ -q

## fetch: pull new data from the sources CI can reach, into $(DB). NOT part
## of `all` -- needs the network, and the manual sources need hand-downloaded
## files under data/raw/ that are gitignored. This is what daily_fetch.yml
## runs. Order: `make assemble fetch validate exports offload`.
fetch:
	$(PYTHON) belgian_macro_db.py --db $(DB) --fetch --latest --export csv
	$(PYTHON) scripts/sync_to_canonical.py --db $(DB)
	$(PYTHON) scripts/sync_statbel.py --db $(DB)
	$(PYTHON) scripts/sync_onem.py --db $(DB)
	$(PYTHON) scripts/sync_onem_rates.py --db $(DB)
	$(PYTHON) scripts/sync_walstat.py --db $(DB)
	$(PYTHON) scripts/sync_international.py --db $(DB)

## sync-nuts2: fetch the Europe NUTS 2 batch's three regional Eurostat
## indicators into $(DB) (scripts/sync_nuts2.py). NOT part of `fetch` or
## `all` -- Europe NUTS 2 (batch B2, docs/features/europe_nuts2.md) is
## deliberately not wired into the daily run yet (a follow-up). Run by hand,
## then dump the new rows to their committed CSV and commit it yourself,
## e.g.:
##   make sync-nuts2
##   $(PYTHON) scripts/export_observations_csv.py --db $(DB) \
##       --out data/nuts2/GDP_PC_PPS_NUTS2.csv --indicators GDP_PC_PPS_NUTS2
sync-nuts2:
	$(PYTHON) scripts/sync_nuts2.py --db $(DB)

## dagster: the local Dagster UI at http://localhost:3000 -- the pipeline's
## assets, lineage, checks and freshness (docs/features/orchestration.md).
## Shows LOCAL runs only: production still runs on GitHub Actions, outside
## Dagster. Nothing materialises by itself; the schedule is declared stopped.
dagster:
	$(PYTHON) -m pip install -q -r requirements-dagster.txt
	mkdir -p "$(DAGSTER_HOME)"
	DAGSTER_HOME="$(DAGSTER_HOME)" $(PYTHON) -m dagster dev -m orchestration

## dagster-daily: the daily sequence through Dagster, exactly what
## daily_fetch.yml runs -- assemble, fetch every source, then validate, export
## and offload ALWAYS, even when a source failed. It WRITES $(COMMITTED_DB) and
## the in_db CSVs, like production: the offload is its last asset. Exit 0 all
## green, 3 offloaded with a red source, 1 nothing publishable. Needs the
## network (`$(PYTHON) -m orchestration.daily --without-fetch` does not).
dagster-daily:
	mkdir -p "$(DAGSTER_HOME)"
	DAGSTER_HOME="$(DAGSTER_HOME)" $(PYTHON) -m orchestration.daily

## verify-dagster-parity: rebuild every export and the offload twice from the
## committed HEAD, once with `make assemble exports offload` and once through
## the Dagster coordinator without the fetch, each in its own temporary git
## worktree, and compare every file. Refuses a dirty tree: commit
## first, or it would compare two copies of the previous commit.
verify-dagster-parity:
	$(PYTHON) scripts/verify_dagster_parity.py

## clean: remove generated artifacts that are safe to regenerate. Deliberately
## does NOT touch data/*.csv or the committed database -- those are committed stores, and
## a `clean` that deletes hand-downloaded or committed data would be a trap.
clean:
	rm -rf local/ public/data/communes public/data/indicators
	rm -f public/data/national.json public/data/manifest.json

help:
	@grep -E '^## ' Makefile | sed 's/^## //'
