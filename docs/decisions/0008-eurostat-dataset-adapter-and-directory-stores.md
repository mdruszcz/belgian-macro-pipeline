# ADR 0008 — A dataset-level Eurostat adapter, and a directory store form, for the international pilot

Date: 2026-09-13
Status: accepted
Revises: none directly; builds on [ADR 0002](0002-split-committed-stores.md)'s CSV-store split and
[ADR 0006](0006-stores-split-by-volume.md)'s refusal to grow the committed database.

## Context

`docs/features/international.md` (stage 1, the pilot) asks for five indicators across every
European country Eurostat publishes, run daily for 14 days to measure size and churn before the
maintainer decides on the full ~100-indicator rollout. That is a different shape of fetch from
anything this pipeline had: every existing time-series adapter (`NBBSource`, and the class this
became `DBnomicsSource`, see `docs/features/source_adapter.md`) fetches one already-known
geography per call. The pilot needs one call to return every geography a dataset carries, for up
to ~44 countries per dataset at once.

The same PR also had to migrate the eight pre-existing `dbnomics_eurostat` indicators
(`EUROSTAT_GDP_Q_MEUR*`, `EC_CONS_CONF_BE`/`_EU`) off DBnomics: the maintainer decided 2026-09-13
to fetch Eurostat's own API directly rather than keep DBnomics as a transport, to avoid DBnomics'
own ODbL share-alike obligation on top of Eurostat's licence (`docs/data_catalog.md`). Both series
families end up on one `source_id: eurostat` row, so they had to move together — the offload
already refuses any observation whose source is "offloaded" but whose indicator is not declared in
a store, and splitting the migration across two PRs would have left two Eurostat sources active in
parallel with a broken offload in between.

## Decision

1. **A dataset-level adapter, `EurostatSource` (`src/fetchers/eurostat.py`), not a loop over the
   existing single-series adapter.** It requests Eurostat's JSON-stat 2.0 API once per dataset
   (`.../statistics/1.0/data/{dataset}?format=JSON&lang=EN&{filters}&sinceTimePeriod={since}`),
   walks every `(geo, time)` cell in the returned cube by explicit linear-offset arithmetic, and
   refuses (`FetchError`) any dimension besides `geo`/`time` that turns out not to be pinned to
   exactly one code by the config's `filters`. No paging: Eurostat returns the whole cube in one
   reply (`num_found`/pagination is a DBnomics concept that does not apply to this API — verified
   against a live fetch of all five pilot datasets, 2026-09-13, no pagination header of any kind).
   A new adapter contract, `MultiGeoTimeSeriesSource` (`src/fetchers/base.py`), fixes its shape:
   `{geo, period, value, obs_status}`, one row per cube cell carrying a value or a status flag.
   `singleton_geo()` reshapes its output back to the plain `{period, value, obs_status}` contract
   for the eight single-country indicators, which still go through `belgian_macro_db.fetch_all`'s
   national path.

2. **A directory form for the store registry**, `layout: one_csv_per_indicator`
   (`docs/features/store_registry.schema.json`, default remains `single_csv`). One
   `config/stores.yaml` entry (`international`, `path: data/international/`, `mode: in_db`) names
   every indicator it owns explicitly — the five new pilot indicators and the eight migrated
   ones — and `scripts/load_observations_csv.py` gained `load_many()` to load the whole family in
   one process (one `fetch_runs` "rebuild" row, migrations and geography applied once), rather than
   one registry entry, one subprocess and one rebuild row per file. `offload_stores.py` still dumps
   and deletes exactly the store's declared row set, now per file inside the directory; a
   zero-row indicator gets no CSV at all, because a header-only CSV would break the next day's
   assemble step, which expects either a real file or none.

3. **The country list is decided by the licence, not by geography, and is an explicit allowlist,
   not a hardcoded dict.** `config/geography/international.csv` (allowlisted) and
   `international_excluded.csv` (explicitly refused — UK, Kosovo, US, Japan, and Eurostat's
   superseded or ambiguous aggregate codes) replace the six-entry `COUNTRY_GEOS` dict
   `scripts/port_existing_indicators.py` used to hardcode. A geography in neither file fails the
   fetch outright (CLAUDE.md rule 13) rather than being silently included or dropped. Two
   aggregates carry `scope: pilot` (`EU27_2020`, `EA21`, the current euro area); Eurostat's plain
   `EA` code is kept `scope: legacy`, allowlisted only so the pre-existing
   `EUROSTAT_GDP_Q_MEUR_EA` national series can still resolve it, and is not re-published by the
   pilot as a second, ambiguous-vintage euro-area total beside `EA21`.

## Alternatives rejected

- **A loop over the existing single-series adapter (now `DBnomicsSource`), calling it once per
  country.** Both the old class and its DBnomics-JSON parsing keep `series.docs[0]` only — correct
  when a query is known to return exactly one series, silently wrong pointed at a multi-country
  query, which would keep one country's rows and drop the rest without any error. It would also
  cost one HTTP round trip per country per indicator (up to ~44 per dataset) instead of one per
  dataset.
- **One CSV for the whole international family instead of one per indicator.** `international.md`'s
  own measurement: the five pilot indicators alone are 3.2 MB from 2008, and the full ~100
  indicator rollout is projected at roughly 108 MB with full history — past GitHub's 100 MB hard
  per-file limit, not just the repository's 39.06 MiB CI guard a single growing file already
  tripped once (ADR 0006). One file per indicator keeps every file, at the largest measured so far
  (1.7 MB), far under either limit as the family grows.
- **A second database for international data.** Already settled by ADR 0006: the problem that
  stopped the daily run on 2026-09-13 was a growing committed SQLite file, not where the
  observations come from. A second database would reintroduce exactly that failure mode under a
  different name; the existing `observations` schema, vintages and validation already generalize to
  a new `source_id` without any schema change.

## Consequences

- `config/indicators/*.yaml` gains a third `fetch` shape alongside `{query}` (DBnomics/NBB) and
  `{dataset, filters (geo required), since}` (one country, national path): `{dataset, filters (geo
  forbidden), geographies: allowlist, since}`, discriminated explicitly by
  `is_multi_geo()` (`src/validation/config_schema.py`) rather than by a `store:` key on the
  indicator — the registry (`config/stores.yaml`) stays the one place that says which indicator
  belongs to which store.
- An allowlisted country's rate can be `ok` on a day it is simply absent from a dataset's response
  (e.g. a country with no unemployment series yet) — the run still succeeds, and every such code is
  named in the `fetch_runs` message, rather than a `partial` status that would fail
  `src/validation/rules.py`'s `fetch_error` check on any ordinary day a small country is missing
  from one series. This is a deliberate reading, not a literal implementation of
  `international.md`'s Tests section ("a missing expected geography fails") — recorded here and in
  `docs/features/international.md` as a stated deviation, not a silent one.
- The eight pre-existing Eurostat indicators and the five new pilot indicators now share one
  `sources` row (`eurostat`), corrected in place (`ON CONFLICT ... DO UPDATE`) from its stale
  `adapter: dbnomics` value rather than left stale or duplicated.
- Stage 2 (the ~100-indicator rollout) is a separate approval on stage 1's 14-day measurement —
  this ADR covers only the adapter and store-form decisions the pilot needed, not the scale
  decision itself.
