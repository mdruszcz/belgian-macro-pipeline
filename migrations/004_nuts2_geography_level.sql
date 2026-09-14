-- migration-mode: recreate-with-foreign-keys-off
-- Add 'nuts2' to geographies.level's CHECK constraint -- Europe NUTS 2 batch
-- B2 (docs/features/europe_nuts2.md), ADR 0009 decision 1: "`level: nuts2`
-- is a new `geographies.level` value distinct from
-- `country`/`region`/`province`/etc." (docs/decisions/0009-nuts2-regional-
-- geography.md, Accepted 2026-09-14). This is the ADR-approved exception to
-- CLAUDE.md's default "never touch the database schema" rule.
--
-- SQLite has no ALTER TABLE ... ALTER CONSTRAINT: an inline CHECK can only
-- be changed by recreating the table (the official recipe:
-- https://www.sqlite.org/lang_altertable.html, "Making Other Kinds Of Table
-- Schema Changes"). PRAGMA defer_foreign_keys was tried first and measured
-- NOT to work here (2026-09-14): even with it ON, DROPping a table that
-- other tables' foreign keys target leaves SQLite's internal deferred-
-- violation counter nonzero, and COMMIT then fails with "FOREIGN KEY
-- constraint failed" even though PRAGMA foreign_key_check reports zero
-- violations both before and after the DROP+RENAME -- a real SQLite
-- limitation of that pragma for a table-recreation, not a bug in the SQL
-- below.
--
-- THE FIRST LINE OF THIS FILE (verbatim, checked against the raw file, not
-- the comment-stripped statements) is a marker src/db/migrate.py's runner
-- reads to apply this file in "recreate mode": PRAGMA foreign_keys=OFF,
-- the runner's OWN BEGIN/COMMIT around every statement below, PRAGMA
-- foreign_key_check inside that transaction before COMMIT, and PRAGMA
-- foreign_keys=ON restored afterwards unconditionally -- all four owned by
-- the runner, never by this file (PR #174 audit, SHOULD-FIX 1: a version of
-- this file used to manage BEGIN/COMMIT/PRAGMA foreign_keys itself, and
-- three ways that could go wrong on a future migration were proved on temp-
-- database probes -- see src/db/migrate.py's _apply_recreate_mode()
-- docstring and tests/test_migrations.py for the reproductions). This file
-- must therefore contain ONLY the schema-change statements themselves --
-- no BEGIN, COMMIT, ROLLBACK or PRAGMA foreign_keys of its own; the runner
-- refuses the file outright if it finds one.
CREATE TABLE geographies_new (
    geo_id           TEXT PRIMARY KEY,
    nis_code         TEXT,
    level            TEXT NOT NULL
                       CHECK (level IN ('country','region','province','arrondissement','municipality','eu_aggregate','nuts2')),
    name_nl          TEXT NOT NULL,
    name_fr          TEXT NOT NULL,
    name_en          TEXT NOT NULL,
    parent_geo_id    TEXT REFERENCES geographies(geo_id),
    valid_from       TEXT NOT NULL,
    valid_to         TEXT,
    successor_geo_id TEXT REFERENCES geographies(geo_id),
    population       INTEGER,
    area_km2          REAL,
    UNIQUE (nis_code, valid_from),
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

INSERT INTO geographies_new
    (geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id,
     valid_from, valid_to, successor_geo_id, population, area_km2)
    SELECT geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id,
           valid_from, valid_to, successor_geo_id, population, area_km2
    FROM geographies;

DROP TABLE geographies;

ALTER TABLE geographies_new RENAME TO geographies;

-- DROP TABLE removed these along with the old table (migrations/002_indexes.sql).
CREATE INDEX IF NOT EXISTS idx_geo_nis_period ON geographies(nis_code, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_geo_level ON geographies(level);
