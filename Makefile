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
DB     ?= data/belgian_macro.db

# Every manual store, merged into the exports at build time (ADR 0002).
EXTRA := --extra-observations data/population_observations.csv \
         --extra-observations data/fiscal_income_observations.csv \
         --extra-observations data/census2021_observations.csv \
         --extra-observations data/realestate_observations.csv \
         --extra-observations data/police_observations.csv

.PHONY: all install schema reference validate exports pages page-documents site-index boundaries builder test fetch clean help

## all: install deps, rebuild the database's own structure, regenerate every
## published export, and run the tests. No network. This is the gate target.
all: install schema reference validate exports test-full
	@echo ""
	@echo "Rebuilt from committed data and tests pass."

## install: python dependencies
install:
	$(PYTHON) -m pip install -q -r requirements.txt

## schema: apply migrations and load the geography reference data. Both are
## idempotent and offline -- geography comes from config/geography/, not the
## network.
schema:
	$(PYTHON) -m src.db.migrate --db $(DB)
	$(PYTHON) scripts/load_geography.py --db $(DB)

## reference: indicator/source metadata rows for the manual sources. Their
## OBSERVATIONS live in committed CSVs, but their name/unit come from the
## indicators table, so the exporters need these rows present. Config only:
## no network, no workbook.
reference:
	$(PYTHON) scripts/sync_fiscal_income.py --db $(DB) --reference-rows-only
	$(PYTHON) scripts/sync_realestate.py    --db $(DB) --reference-rows-only
	$(PYTHON) scripts/sync_census2021.py    --db $(DB) --reference-rows-only
	$(PYTHON) scripts/sync_police.py        --db $(DB) --reference-rows-only

## validate: Block H's rules. Fails the build on a data problem, which is the
## entire point of it existing (a validation step that only logs is decoration).
validate:
	$(PYTHON) scripts/validate_data.py --db $(DB)

## exports: every published artifact, in dependency order -- the site payloads
## read the bulk CSVs, and the static pages read the site payloads.
exports:
	$(PYTHON) scripts/export_canonical_csv.py --db $(DB) --out data/belgian_macro_export.csv
	$(PYTHON) scripts/export_communes_csv.py --db $(DB) --out data/communes_export.csv $(EXTRA)
	# Two passes, on purpose. _full feeds the two internal steps below, whose
	# own contract is the complete history (site_payloads.md); the second
	# pass, with no --all-periods, is the trimmed last-10-years file that
	# actually gets committed and offered as a download -- data/communes_history_full.csv
	# is gitignored so it never reaches the commit-size guard it exists to avoid.
	$(PYTHON) scripts/export_communes_history_csv.py --db $(DB) \
		--out data/communes_history_full.csv --all-periods $(EXTRA)
	$(PYTHON) scripts/export_communes_history_csv.py --db $(DB) --out data/communes_history.csv $(EXTRA)
	$(PYTHON) scripts/export_communes_table_json.py \
		--communes-history data/communes_history_full.csv --out data/communes_table.json --db $(DB)
	$(PYTHON) scripts/export_aggregates_csv.py --db $(DB) --out data/aggregates.csv $(EXTRA)
	$(PYTHON) scripts/export_percentiles_csv.py --db $(DB) --out data/percentiles.csv $(EXTRA)
	$(PYTHON) -m src.exporters.metadata --out data/metadata/indicators.json
	$(PYTHON) scripts/export_site_payloads.py --db $(DB) \
		--communes-history data/communes_history_full.csv \
		--communes-latest data/communes_export.csv \
		--national data/belgian_macro_export.csv \
		--aggregates data/aggregates.csv \
		--percentiles data/percentiles.csv \
		--out-dir public/data --build-id "$${BUILD_ID:-local}" --validation-status unknown
	$(PYTHON) scripts/export_commune_adjacency.py
	$(MAKE) pages
	$(MAKE) page-documents
	$(MAKE) site-index

## pages: the permanent /local/{nis} routes. Separate target because it is the
## slowest step and is often what you want to re-run alone while iterating.
pages:
	$(PYTHON) scripts/export_local_pages.py --db $(DB) \
		--payload-dir public/data --out-dir local --build-id "$${BUILD_ID:-local}"

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

## fetch: pull new data from the sources CI can reach. NOT part of `all` --
## needs the network, and the manual sources need hand-downloaded files under
## data/raw/ that are gitignored. This is what daily_fetch.yml runs.
fetch:
	$(PYTHON) belgian_macro_db.py --fetch --latest --export csv
	$(PYTHON) scripts/sync_to_canonical.py --db $(DB)
	$(PYTHON) scripts/sync_statbel.py --db $(DB)
	$(PYTHON) scripts/sync_onem.py --db $(DB)
	$(PYTHON) scripts/sync_walstat.py --db $(DB)

## clean: remove generated artifacts that are safe to regenerate. Deliberately
## does NOT touch data/*.csv or the database -- those are committed stores, and
## a `clean` that deletes hand-downloaded or committed data would be a trap.
clean:
	rm -rf local/ public/data/communes public/data/indicators
	rm -f public/data/national.json public/data/manifest.json

help:
	@grep -E '^## ' Makefile | sed 's/^## //'
