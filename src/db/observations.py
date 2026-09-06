"""
get_observations(as_of=...) -- Block I, docs/features/vintages.md.

This is the query layer the vintage mechanism exists to serve: "regenerate a
client's March report identically in December." `as_of=None` is the existing
default everywhere else in this codebase (is_latest = 1); `as_of=<a date>`
walks history instead.

Store-agnostic in the same sense as src/analytics/engine.py: this takes a
connection, not a store name, so it works against the daily-committed
database or a locally rebuilt manual store (docs/features/manual_sources.md)
without knowing which.
"""

import sqlite3
from dataclasses import dataclass
from datetime import datetime, time, timezone


@dataclass(frozen=True)
class Observation:
    indicator_id: str
    geo_id: str
    period: str
    vintage: str
    value: float | None
    status: str
    period_start: str
    period_end: str
    is_latest: bool
    created_at: str


_COLUMNS = (
    "indicator_id",
    "geo_id",
    "period",
    "vintage",
    "value",
    "status",
    "period_start",
    "period_end",
    "is_latest",
    "created_at",
)


def get_observations(
    conn: sqlite3.Connection,
    *,
    indicator_id: str | None = None,
    geo_id: str | None = None,
    period: str | None = None,
    as_of: str | None = None,
) -> list[Observation]:
    """Every observation matching the given filters (any of the three may be
    omitted to widen the query), as of a point in time.

    `as_of=None` (the default): `is_latest = 1` rows only -- the same
    behaviour every existing reader in this codebase already has, on the
    existing partial index. This function changes nothing for a caller that
    never passes `as_of`.

    `as_of=<timestamp or bare date>`: for each distinct
    (indicator_id, geo_id, period) the filters select, the row with the
    GREATEST vintage <= as_of -- and no row at all for a cell whose every
    vintage is later than as_of, because the cell did not exist yet on that
    date and inventing a first-known value would be worse than reporting
    nothing.
    """
    where_sql, params = _where_clause(indicator_id, geo_id, period)
    if as_of is None:
        extra = "is_latest = 1" if not where_sql else f"is_latest = 1 AND {where_sql}"
        rows = conn.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM observations WHERE {extra}", params
        ).fetchall()
        return [_row_to_observation(r) for r in rows]

    as_of_dt = _parse_as_of(as_of)
    sql = f"SELECT {', '.join(_COLUMNS)} FROM observations"
    if where_sql:
        sql += f" WHERE {where_sql}"
    candidates = conn.execute(sql, params).fetchall()

    best: dict[tuple[str, str, str], tuple[datetime, tuple]] = {}
    for row in candidates:
        vintage_dt = parse_vintage(row[_COLUMNS.index("vintage")])
        if vintage_dt > as_of_dt:
            continue
        key = (row[0], row[1], row[2])  # indicator_id, geo_id, period
        current = best.get(key)
        if current is None or vintage_dt > current[0]:
            best[key] = (vintage_dt, row)

    return [_row_to_observation(row) for _dt, row in best.values()]


def _where_clause(
    indicator_id: str | None, geo_id: str | None, period: str | None
) -> tuple[str, list[str]]:
    clauses = []
    params: list[str] = []
    if indicator_id is not None:
        clauses.append("indicator_id = ?")
        params.append(indicator_id)
    if geo_id is not None:
        clauses.append("geo_id = ?")
        params.append(geo_id)
    if period is not None:
        clauses.append("period = ?")
        params.append(period)
    return " AND ".join(clauses), params


def _row_to_observation(row: tuple) -> Observation:
    return Observation(
        indicator_id=row[0],
        geo_id=row[1],
        period=row[2],
        vintage=row[3],
        value=row[4],
        status=row[5],
        period_start=row[6],
        period_end=row[7],
        is_latest=bool(row[8]),
        created_at=row[9],
    )


def parse_vintage(value: str) -> datetime:
    """A vintage is always a full timestamp (never a bare date -- that was
    itself a bug fix, see vintages.md), but may carry any UTC offset. Parsed
    and normalised rather than compared as a string: every vintage in the
    store today happens to end '+00:00', so lexical comparison would work by
    luck, and a single row written with a different offset would silently
    sort wrong."""
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _parse_as_of(value: str) -> datetime:
    """A bare date ('2026-03-15') means the END of that day, not midnight --
    the naive parse would exclude everything published on the very day the
    caller is asking about, which is the day a client's own report used."""
    try:
        naive_date = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return datetime.combine(naive_date, time(23, 59, 59, 999999), tzinfo=timezone.utc)
