-- Retire the ten AMECO forecast-year rows that were stored as `final` --
-- ADR 0017 (docs/decisions/0017-ameco-forecast-periods.md, ACCEPTED
-- 2026-10-04), option (d): European Commission forecast years must never
-- sit in `observations` as if they were a measurement (CLAUDE.md rule 6).
--
-- WHAT WAS WRONG. src/fetchers/dbnomics.py stamped obs_status "A" (-> final)
-- on every row unconditionally, with no concept of a forecast year. Ten
-- rows are the European Commission's own forecasts for 2026 and 2027,
-- published through AMECO's May 2026 release, for the five indicators this
-- adapter feeds: LABOUR_COST_BE, LABOUR_COST_DE, LABOUR_COST_EA,
-- LABOUR_COST_FR, LABOUR_COST_NL. Verified directly against the committed
-- database before writing this migration: exactly 10 is_latest=1 rows in
-- `observations` at period 2026 or 2027 for these five indicators (all
-- `final`, all vintage 2026-09-05 or 2026-09-13), and exactly 10 matching
-- rows in `legacy_observations` (all obs_status 'A', fetched 2026-10-03).
--
-- WHY A MIGRATION, NOT JUST THE ADAPTER FIX. The adapter fix (this same
-- commit) stops a FUTURE fetch from writing these years again, but it
-- cannot by itself remove what is already stored -- the exact shape ADR
-- 0017 itself names for a related option: "legacy rows are never deleted
-- (belgian_macro_db.py's upsert_observations is a plain INSERT ... ON
-- CONFLICT DO UPDATE, keyed per period -- nothing in it drops a period that
-- a later fetch simply stops mentioning) and scripts/sync_to_canonical.py
-- re-reads ALL of a code's legacy rows on every run, not only today's, so a
-- stale one is re-synced forever otherwise." Precedent for fixing this in a
-- migration rather than leaving it to the next fetch: migrations/005, which
-- corrected 27 already-stored rows (AMECO labour costs among them, for a
-- different column) that its own writer-script fix could not retroactively
-- repair either.
--
-- BOTH TABLES, because fixing only one resurrects the other's row:
--   1. `observations`: retired (is_latest = 0), never deleted -- the
--      standard "retire before rewrite" pattern already used by
--      scripts/port_existing_indicators.py:271 and
--      scripts/sync_population_movement.py, so the vintage history (what
--      AMECO said, and when) stays intact and `data/belgian_macro_export.csv`
--      (WHERE o.is_latest = 1, scripts/export_canonical_csv.py) simply stops
--      listing these 10 rows on the next export -- the two-fewer-rows
--      consequence ADR 0017 itself names.
--   2. `legacy_observations`: deleted outright, not retired -- this table
--      has no vintage concept (one row per indicator_code+period, always
--      overwritten in place) and no downstream reader besides
--      scripts/sync_to_canonical.py, which re-reads EVERY row for a code on
--      every run. Left in place, src/db/vintages.py's upsert_observation
--      would find no is_latest=1 row for 2026/2027 (just retired above),
--      treat the stale legacy value as new, and insert a FRESH is_latest=1
--      row under today's vintage -- silently undoing step 1 the next time
--      sync_to_canonical.py runs. Deleting it here is what makes the
--      retirement durable rather than cosmetic.
--
-- Data-only: no change to any EXISTING schema, so no `migration-mode`
-- marker and no table recreation.
--
-- THE TWO "CREATE TABLE IF NOT EXISTS" STATEMENTS BELOW ARE DEFENSIVE, NOT A
-- SCHEMA CHANGE: legacy_observations/legacy_indicators are not part of the
-- migrations/ -tracked canonical schema at all -- they are created
-- separately, ad hoc, by belgian_macro_db.py's MacroDatabase._init_schema()
-- (verified by reading it), which every real committed database has always
-- gone through. In that always-true-in-production shape this is a pure
-- no-op. It exists only so this migration also runs cleanly against a bare
-- canonical-only database that was never bootstrapped through
-- MacroDatabase -- exactly the shape tests/test_migrations.py's own
-- `migrated_db` fixture builds to exercise the migration RUNNER in
-- isolation. Column-for-column identical to belgian_macro_db.py's own
-- definition, so there is no second, drifting copy of this table's shape.
UPDATE observations
   SET is_latest = 0
 WHERE indicator_id IN
       ('LABOUR_COST_BE', 'LABOUR_COST_DE', 'LABOUR_COST_EA', 'LABOUR_COST_FR', 'LABOUR_COST_NL')
   AND period IN ('2026', '2027')
   AND is_latest = 1;

CREATE TABLE IF NOT EXISTS legacy_indicators (
    code          TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    frequency     TEXT NOT NULL,
    unit          TEXT NOT NULL,
    source_agency TEXT NOT NULL,
    description   TEXT,
    api_url       TEXT
);

CREATE TABLE IF NOT EXISTS legacy_observations (
    indicator_code TEXT NOT NULL,
    period         TEXT NOT NULL,
    value          REAL NOT NULL,
    obs_status     TEXT,
    fetched_at     TEXT NOT NULL,
    PRIMARY KEY (indicator_code, period),
    FOREIGN KEY (indicator_code) REFERENCES legacy_indicators(code)
);

DELETE FROM legacy_observations
 WHERE indicator_code IN
       ('LABOUR_COST_BE', 'LABOUR_COST_DE', 'LABOUR_COST_EA', 'LABOUR_COST_FR', 'LABOUR_COST_NL')
   AND period IN ('2026', '2027');
