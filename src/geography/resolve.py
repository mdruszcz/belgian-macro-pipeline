"""
resolve_geo(nis, period) -- the single entry point every adapter uses to turn a
raw NIS code from a source file into a canonical geo_id (CLAUDE.md rule 3).

Centralizing this is the whole point: a crosswalk correction then propagates
everywhere at once, where per-adapter handling would give eight subtly different
behaviours. Mismapped geography produces plausible numbers attached to the wrong
place, which is the most damaging failure this product can have, so the two
rules below are absolute:

- A NIS code is resolved against the *period's* validity window, not today's.
  A 2015 file about Kruibeke resolves to Kruibeke, which stopped existing in
  2025 -- not to Beveren-Kruibeke-Zwijndrecht, which did not exist in 2015.
- An unknown code raises. It is never coerced, defaulted, or skipped
  (CLAUDE.md rule 13).
"""

import re
import sqlite3

# Period shapes accepted by the canonical model (docs/features/data_model.md).
# resolve_geo takes only the period string, with no frequency argument, so the
# shape has to be self-describing -- which these four are.
_YEAR = re.compile(r"^(\d{4})$")
_QUARTER = re.compile(r"^(\d{4})-Q([1-4])$")
_MONTH = re.compile(r"^(\d{4})-(\d{2})$")
_DAY = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")


class UnknownGeographyError(Exception):
    """A NIS code that no geography row covers for the requested period."""


def period_to_date(period: str) -> str:
    """First day of the period, as YYYY-MM-DD.

    Deliberately shape-based. scripts/port_existing_indicators.py has a
    derive_period_bounds() that does something similar but requires the
    indicator's declared frequency, which a geography lookup does not have.
    Consolidating the two is worthwhile follow-up work, noted rather than done
    here so that a tested function is not refactored as a side effect.
    """
    period = period.strip()
    if match := _YEAR.match(period):
        return f"{match.group(1)}-01-01"
    if match := _QUARTER.match(period):
        month = (int(match.group(2)) - 1) * 3 + 1
        return f"{match.group(1)}-{month:02d}-01"
    if match := _MONTH.match(period):
        return f"{match.group(1)}-{match.group(2)}-01"
    if _DAY.match(period):
        return period
    raise ValueError(
        f"Unrecognized period format {period!r}. Expected YYYY, YYYY-Qn, YYYY-MM "
        "or YYYY-MM-DD (docs/features/data_model.md)."
    )


def resolve_geo(conn: sqlite3.Connection, nis: str, period: str) -> str:
    """The geo_id valid for this NIS code during this period.

    Raises UnknownGeographyError if the code is unknown, or is known but has no
    row covering that period -- e.g. asking about a commune for a year before
    the loaded reference data begins. Returning a best guess there would be
    worse than failing: it would silently attribute figures to an entity that
    did not exist.
    """
    as_of = period_to_date(period)
    row = conn.execute(
        """
        SELECT geo_id FROM geographies
        WHERE nis_code = ?
          AND valid_from <= ?
          AND (valid_to IS NULL OR valid_to > ?)
        """,
        (nis, as_of, as_of),
    ).fetchone()
    if row:
        return row[0]

    known = conn.execute(
        "SELECT geo_id, valid_from, valid_to FROM geographies WHERE nis_code = ? "
        "ORDER BY valid_from",
        (nis,),
    ).fetchall()
    if not known:
        raise UnknownGeographyError(
            f"NIS code {nis!r} is not present in `geographies`. Refusing to guess "
            "(CLAUDE.md rule 13). If this is a real code, the reference data needs "
            "reloading -- see scripts/load_geography.py."
        )
    windows = ", ".join(f"[{start} .. {end or 'open'})" for _, start, end in known)
    raise UnknownGeographyError(
        f"NIS code {nis!r} exists but no row covers {as_of} (from period {period!r}). "
        f"Known validity windows: {windows}."
    )


def resolve_to_current(conn: sqlite3.Connection, geo_id: str) -> str:
    """Follow successor_geo_id to the entity that exists today.

    Lineage chains rather than being one hop -- a commune merged twice needs a
    recursive walk (docs/features/data_model.md Review 1) -- so this loops, with
    a cycle guard, instead of reading a single successor.
    """
    seen = [geo_id]
    current = geo_id
    while True:
        row = conn.execute(
            "SELECT successor_geo_id, valid_to FROM geographies WHERE geo_id = ?", (current,)
        ).fetchone()
        if row is None:
            raise UnknownGeographyError(f"geo_id {current!r} is not present in `geographies`.")
        successor, valid_to = row
        if successor is None:
            if valid_to is not None:
                # Dead end: the entity ended but no successor was recorded --
                # the case for predecessors split across several successors, or
                # whose lineage could not be derived. Returning it would claim
                # a defunct commune is the current one.
                raise UnknownGeographyError(
                    f"{current!r} ceased to exist on {valid_to} and has no recorded successor, "
                    "so it cannot be mapped to a current entity. This is expected for communes "
                    "split across several successors -- see "
                    "config/geography/municipality_crosswalk.csv."
                )
            return current
        if successor in seen:
            raise UnknownGeographyError(
                f"Cycle in geography lineage: {' -> '.join(seen + [successor])}."
            )
        seen.append(successor)
        current = successor
