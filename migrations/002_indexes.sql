-- Indexes for the tables created in 001_core_schema.sql.
-- See docs/features/data_model.md for the query patterns each index serves.

-- geographies: hit on every resolve_geo(nis, period) call -- i.e. every
-- ingested row from every adapter. Without it, resolve_geo is a full table
-- scan per row at ingest time.
CREATE INDEX IF NOT EXISTS idx_geo_nis_period ON geographies(nis_code, valid_from, valid_to);
-- "all municipalities" queries, used by any cross-commune aggregation.
CREATE INDEX IF NOT EXISTS idx_geo_level ON geographies(level);

-- indicators: provenance-audit queries ("all indicators from source X").
CREATE INDEX IF NOT EXISTS idx_ind_source ON indicators(source_id);

-- fetch_runs: staleness detection ("when did source X last succeed").
CREATE INDEX IF NOT EXISTS idx_runs_source_started ON fetch_runs(source_id, started_at DESC);

-- observations: partial indexes scoped to the current belief (is_latest=1),
-- serving the two most common reads -- a single indicator's time series, and
-- a commune's full latest profile across indicators.
CREATE INDEX IF NOT EXISTS idx_obs_series ON observations(indicator_id, geo_id, period_start) WHERE is_latest = 1;
CREATE INDEX IF NOT EXISTS idx_obs_geo_period ON observations(geo_id, period_start) WHERE is_latest = 1;
-- per-run audit/rollback of a bad fetch_run_id.
CREATE INDEX IF NOT EXISTS idx_obs_run ON observations(fetch_run_id);

-- observations: non-partial indexes covering ALL vintages (the two above
-- exclude is_latest=0 rows, so they can't serve full revision-history reads).
-- These are intentionally NOT redundant with the partial indexes above:
-- different column order/leading column, and no WHERE clause.
--   idx_obs_geo_indicator: all vintages of indicator X in commune Y
--     (revision-history views; also a fallback if is_latest bookkeeping is
--     ever suspect, since it doesn't depend on it).
CREATE INDEX IF NOT EXISTS idx_obs_geo_indicator ON observations(geo_id, indicator_id);
--   idx_obs_indicator_period: all vintages of indicator X across periods
--     (single-indicator revision audit). Indexes the raw `period` TEXT
--     column rather than period_start -- valid because within one indicator
--     frequency is fixed, so lexicographic `period` order is correct
--     (see docs/features/data_model.md, Period format rules > Sorting).
CREATE INDEX IF NOT EXISTS idx_obs_indicator_period ON observations(indicator_id, period);
