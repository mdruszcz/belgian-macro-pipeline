-- Canonical five-table data model per docs/features/data_model.md,
-- frozen by docs/decisions/0001-data-model.md.
--
-- PRAGMA foreign_keys=ON is a connection-level pragma and is deliberately NOT
-- set here -- it does not persist across connections, so it is the runner's
-- job (src/db/migrate.py) to set it on every connection, not this file's.

CREATE TABLE IF NOT EXISTS sources (
    source_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    agency      TEXT NOT NULL,
    adapter     TEXT NOT NULL,
    base_url    TEXT,
    licence     TEXT,
    catalog_ref TEXT NOT NULL,
    cadence     TEXT,
    is_active   INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS geographies (
    geo_id           TEXT PRIMARY KEY,
    nis_code         TEXT,
    level            TEXT NOT NULL
                       CHECK (level IN ('country','region','province','arrondissement','municipality','eu_aggregate')),
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

CREATE TABLE IF NOT EXISTS indicators (
    indicator_id        TEXT PRIMARY KEY,
    source_id           TEXT NOT NULL REFERENCES sources(source_id),
    name_nl             TEXT NOT NULL,
    name_fr             TEXT NOT NULL,
    name_en             TEXT NOT NULL,
    description_nl      TEXT,
    description_fr      TEXT,
    description_en      TEXT,
    frequency           TEXT NOT NULL CHECK (frequency IN ('A','Q','M','D')),
    unit                TEXT NOT NULL,
    preferred_direction TEXT NOT NULL
                          CHECK (preferred_direction IN ('lower_is_better','higher_is_better','neutral','contextual')),
    aggregation_method  TEXT NOT NULL DEFAULT 'population_weighted'
                          CHECK (aggregation_method IN ('population_weighted','sum','unweighted_mean','not_applicable')),
    is_additive         INTEGER NOT NULL,
    decimals            INTEGER NOT NULL DEFAULT 1,
    config_path         TEXT NOT NULL,
    is_active           INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS fetch_runs (
    fetch_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id    TEXT NOT NULL REFERENCES sources(source_id),
    adapter      TEXT NOT NULL,
    started_at   TEXT NOT NULL,
    finished_at  TEXT,
    status       TEXT NOT NULL CHECK (status IN ('ok','partial','error','schema_changed')),
    rows_read    INTEGER NOT NULL DEFAULT 0,
    rows_written INTEGER NOT NULL DEFAULT 0,
    http_status  INTEGER,
    message      TEXT
);

CREATE TABLE IF NOT EXISTS observations (
    indicator_id TEXT NOT NULL REFERENCES indicators(indicator_id),
    geo_id       TEXT NOT NULL REFERENCES geographies(geo_id),
    period       TEXT NOT NULL,
    vintage      TEXT NOT NULL,
    value        REAL,
    status       TEXT NOT NULL
                  CHECK (status IN ('final','provisional','estimate','revised','suppressed','na')),
    period_start TEXT NOT NULL,
    period_end   TEXT NOT NULL,
    is_latest    INTEGER NOT NULL,
    fetch_run_id INTEGER NOT NULL REFERENCES fetch_runs(fetch_run_id),
    created_at   TEXT NOT NULL,
    PRIMARY KEY (indicator_id, geo_id, period, vintage),
    CHECK (value IS NOT NULL OR status IN ('suppressed','na')),
    CHECK (period_end >= period_start)
);
