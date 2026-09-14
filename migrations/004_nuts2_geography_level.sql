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
-- below. PRAGMA foreign_keys=OFF is the documented fix, but it is ALSO a
-- documented no-op once a transaction is already open -- and
-- src/db/migrate.py's runner always opens one before executing a migration
-- file's statements. This file's first statement (`PRAGMA foreign_keys =
-- OFF`, exactly this text) is a signal run() reads (src/db/migrate.py,
-- _is_self_managed_transaction()) to run this ONE file's statements in
-- autocommit mode instead, so the PRAGMA takes effect; the file manages its
-- own BEGIN/COMMIT around the actual schema change and restores
-- `foreign_keys = ON` at the end. run() verifies PRAGMA foreign_key_check
-- is clean immediately afterwards before recording this migration as
-- applied.
PRAGMA foreign_keys = OFF;

BEGIN TRANSACTION;

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

COMMIT;

PRAGMA foreign_keys = ON;
