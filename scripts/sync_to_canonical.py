"""
Daily sync of the legacy schema's just-fetched observations into the
canonical schema, with real vintage/is_latest change-detection.

Unlike the one-off scripts/port_existing_indicators.py (which re-fetches
live and only needs same-day dedup), this runs every day: a value that is
unchanged from the current is_latest row must NOT create a new vintage, per
docs/features/data_model.md's own rule ("a re-fetch returning an unchanged
value must not create a new vintage"). Only a genuinely new or changed value
inserts a new vintage row, flipping the prior latest row's is_latest to 0 in
the same transaction. This is issue #9's core logic.

No network calls: reads what the fetchers already wrote to
legacy_observations/legacy_indicators this run (see
scripts/rename_legacy_tables.py, belgian_macro_db.py's _init_schema).

Scope: the same 16 Belgium-only indicators as port_existing_indicators.py.
The other 10 (non-Belgium) stay on the legacy tables only.
"""

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from port_existing_indicators import (
    BE_COUNTRY_GEO,
    INCLUDED,
    PREFERRED_DIRECTION,
    derive_period_bounds,
    map_obs_status,
    source_id_for,
)

from belgian_macro_db import SOURCES


def _ensure_reference_rows(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id,
             valid_from, valid_to, successor_geo_id, population, area_km2)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        tuple(BE_COUNTRY_GEO.values()),
    )
    for code, meta in SOURCES.items():
        if code not in INCLUDED:
            continue
        agency = meta["source_agency"]
        source_id = source_id_for(agency)
        conn.execute(
            """
            INSERT OR IGNORE INTO sources
                (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
            (
                source_id,
                agency,
                agency,
                meta["type"],
                None,
                None,
                "docs/data_catalog.md (pending)",
                None,
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'population_weighted', 0, 1, ?, 1)
        """,
            (
                code,
                source_id,
                meta["name"],
                meta["name"],
                meta["name"],
                meta.get("description", ""),
                meta["frequency"],
                meta["unit"],
                PREFERRED_DIRECTION[code],
                f"scripts/port_existing_indicators.py::{code}",
            ),
        )
    conn.commit()


def sync(db_path: Path, vintage: str | None = None) -> tuple[int, int]:
    """Returns (rows_checked, rows_changed).

    `vintage` must be a full timestamp, not a bare date: two syncs on the
    same calendar day where a value legitimately changed between them (a
    same-day re-run, a source republishing intraday) would otherwise collide
    on the observations PK (indicator_id, geo_id, period, vintage) -- the
    is_latest=0 flip on the old row would succeed but the new row's INSERT
    OR IGNORE would silently no-op, leaving the cell with zero is_latest=1
    rows. Confirmed directly by reproducing it with date-only vintage.
    """
    now = datetime.now(timezone.utc).isoformat()
    vintage = vintage or now

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_reference_rows(conn)

    checked = 0
    changed = 0
    for code, meta in SOURCES.items():
        if code not in INCLUDED:
            continue

        source_id = source_id_for(meta["source_agency"])
        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            (source_id, meta["type"], now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        rows_written = 0

        legacy_rows = conn.execute(
            "SELECT period, value, obs_status FROM legacy_observations WHERE indicator_code = ?",
            (code,),
        ).fetchall()

        for period, value, raw_obs_status in legacy_rows:
            checked += 1
            status = map_obs_status(raw_obs_status)
            current = conn.execute(
                """SELECT value, status FROM observations
                   WHERE indicator_id = ? AND geo_id = 'be:country' AND period = ? AND is_latest = 1""",
                (code, period),
            ).fetchone()

            if current is not None and current[0] == value and current[1] == status:
                continue  # unchanged -- no new vintage

            if current is not None:
                conn.execute(
                    """UPDATE observations SET is_latest = 0
                       WHERE indicator_id = ? AND geo_id = 'be:country' AND period = ? AND is_latest = 1""",
                    (code, period),
                )

            period_start, period_end = derive_period_bounds(period, meta["frequency"])
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO observations
                    (indicator_id, geo_id, period, vintage, value, status,
                     period_start, period_end, is_latest, fetch_run_id, created_at)
                VALUES (?, 'be:country', ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
                (code, period, vintage, value, status, period_start, period_end, fetch_run_id, now),
            )
            if cur.rowcount != 1:
                # A genuine PK collision slipped through despite the timestamp
                # vintage -- fail loudly rather than silently leave this cell
                # with zero is_latest=1 rows (CLAUDE.md rule 13).
                raise RuntimeError(
                    f"Vintage collision writing {code}/{period}/{vintage}: "
                    "a row with this exact (indicator_id, geo_id, period, vintage) "
                    "already exists. Refusing to silently drop the new value."
                )
            changed += 1
            rows_written += 1

        conn.execute(
            "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? WHERE fetch_run_id = ?",
            (datetime.now(timezone.utc).isoformat(), len(legacy_rows), rows_written, fetch_run_id),
        )

    conn.commit()
    conn.close()
    return checked, changed


def main() -> None:
    ap = argparse.ArgumentParser(description="Sync legacy fetch results into the canonical schema")
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file")
    args = ap.parse_args()
    checked, changed = sync(Path(args.db))
    print(f"Checked {checked} observations, {changed} new vintage(s) written.")


if __name__ == "__main__":
    main()
