"""
The single implementation of "insert a new vintage only on change" -- Block I,
docs/features/vintages.md.

Before this module existed, four writers (sync_to_canonical.py,
sync_statbel.py, sync_population.py, sync_fiscal_income.py) each had their own
copy of this logic, and they had already diverged: three compared
`(value, status)`, one (sync_population.py) compared `value` alone. That one
is a LATENT bug, not a live one -- it cannot fire while that script hardcodes
status='final' on every row -- but the day suppression handling lands, a
commune moving final -> suppressed at an unchanged number would write no new
vintage and the suppression would be lost. "Three hand-copied versions of a
rule is how the versions diverge; this one already has" (vintages.md).

One shared function, imported by all four, closes that gap by construction
rather than by review: there is no second copy left to drift.
"""

import sqlite3


class VintageCollisionError(RuntimeError):
    """A row with this exact (indicator_id, geo_id, period, vintage) already
    exists. Raised rather than silently dropping the new value (CLAUDE.md
    rule 13) -- this is what a bare-date vintage would have produced on two
    same-day syncs where a value legitimately changed, which is why vintage
    is a full timestamp and not a date."""


def upsert_observation(
    conn: sqlite3.Connection,
    *,
    indicator_id: str,
    geo_id: str,
    period: str,
    period_start: str,
    period_end: str,
    value: float | None,
    status: str,
    vintage: str,
    fetch_run_id: int,
    created_at: str | None = None,
) -> int:
    """Insert-only-on-change. Returns 1 if a new vintage was written, 0 if
    the cell is unchanged.

    A new vintage is written IFF `(value, status)` differs from the current
    `is_latest = 1` row for this (indicator_id, geo_id, period) -- nothing
    else. Not a new fetch, not a new day, not a re-run: those change
    provenance (fetch_run_id), not the value, and provenance is not what
    vintage means.

    `created_at` defaults to `vintage` -- the normal case, where a row is
    written the instant it is observed. sync_to_canonical.py's tests pass a
    synthetic `vintage` ("v1", "v2", ...) to control the primary key
    deterministically across repeated calls in the same test; `created_at`
    there is kept as a real wall-clock timestamp regardless, so it stays
    meaningful even when `vintage` is not.
    """
    if created_at is None:
        created_at = vintage
    current = conn.execute(
        """SELECT value, status FROM observations
           WHERE indicator_id = ? AND geo_id = ? AND period = ? AND is_latest = 1""",
        (indicator_id, geo_id, period),
    ).fetchone()
    if current is not None and current[0] == value and current[1] == status:
        return 0  # unchanged -- no new vintage

    if current is not None:
        conn.execute(
            """UPDATE observations SET is_latest = 0
               WHERE indicator_id = ? AND geo_id = ? AND period = ? AND is_latest = 1""",
            (indicator_id, geo_id, period),
        )

    cur = conn.execute(
        """
        INSERT OR IGNORE INTO observations
            (indicator_id, geo_id, period, vintage, value, status,
             period_start, period_end, is_latest, fetch_run_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            indicator_id,
            geo_id,
            period,
            vintage,
            value,
            status,
            period_start,
            period_end,
            fetch_run_id,
            created_at,
        ),
    )
    if cur.rowcount != 1:
        raise VintageCollisionError(
            f"Vintage collision writing {indicator_id}/{geo_id}/{period}/{vintage}: "
            "a row with this exact key already exists. Refusing to silently drop "
            "the new value (CLAUDE.md rule 13)."
        )
    return 1
