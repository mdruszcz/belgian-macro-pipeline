-- Block H volume rules: run-to-run history for the "17,000 rows yesterday,
-- 436 today" guard (docs/features/validation.md).
--
-- The spec proposed adding previous_count/new_count/delta to fetch_runs.
-- That does not work, and the reason is measured, not stylistic: volume is
-- meaningful per INDICATOR, not per run. fetch_runs.rows_written is 0 on a
-- normal nbb day (insert-only-on-change working correctly) and swings 565 ->
-- 19149 for statbel because one number mixes datasets loaded by different
-- scripts. Three columns on a run row cannot hold a per-indicator count, so
-- the history gets its own table keyed by indicator.
--
-- Rows are written only after a validation pass succeeds: a failing build
-- must not record the collapsed count as the new normal, or the alarm
-- silences itself on the next run.
CREATE TABLE indicator_volume (
    snapshot_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    taken_at       TEXT NOT NULL,
    indicator_id   TEXT NOT NULL REFERENCES indicators(indicator_id),
    previous_count INTEGER,
    new_count      INTEGER NOT NULL,
    delta          INTEGER
);

CREATE INDEX idx_volume_indicator_taken ON indicator_volume(indicator_id, taken_at DESC);
