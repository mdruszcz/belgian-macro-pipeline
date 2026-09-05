"""
Fetch Statbel municipal-level indicators and write them into the canonical
`observations` table -- Block F, docs/features/statbel_adapter.md.

Unlike sync_to_canonical.py (which reads what fetch_all() already wrote to
legacy_observations, all keyed to geo_id='be:country'), this does its own
live network fetch, because StatbelSource's output is per-commune from the
start -- there is no legacy, single-geo intermediate for it to read back
from. Every commune this writes uses a real geo_id resolved by StatbelSource
itself (config/geography/geographies.csv), never 'be:country'.

Scoped to exactly the indicators in STATBEL_INDICATORS below -- currently
just LOCAL_UNITS_BY_COMMUNE. See docs/features/statbel_adapter.md, Non-goals,
for why fiscal income and population are not included yet.

Reuses the same vintage/is_latest discipline as sync_to_canonical.py (full-
timestamp vintage, insert-only-on-change, hard rowcount check) -- see
docs/features/data_model.md, section Vintage -- just keyed per commune geo_id
instead of always the same national one.
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.fetchers.statbel import StatbelSource  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

# Which config/indicators/*.yaml this script is responsible for. A new
# Statbel dataset is added here (and to config/indicators/) once its own
# geo-resolution and column mapping have been verified -- never by widening
# this list speculatively.
STATBEL_INDICATORS = {"LOCAL_UNITS_BY_COMMUNE"}


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    """INSERT OR IGNORE the sources/indicators rows this script depends on --
    same pattern as sync_to_canonical.py's _ensure_reference_rows, but this
    script owns its own small, fixed indicator set rather than iterating
    every canonical-eligible indicator in SOURCES."""
    conn.execute("""
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('statbel', 'Statbel Bestat API', 'Statbel', 'statbel',
                'https://bestat.statbel.fgov.be/bestat/api',
                'CC BY 4.0 -- maintainer-asserted 2026-09-06', 'docs/data_catalog.md rows 7, 7b',
                'quarterly', 1)
        """)
    for code in STATBEL_INDICATORS:
        ind = indicator_configs[code]
        conn.execute(
            """
            INSERT OR IGNORE INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'statbel', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'sum', 1, 0, ?, 1)
            """,
            (
                code,
                ind["name"]["nl"],
                ind["name"]["fr"],
                ind["name"]["en"],
                ind.get("description", {}).get("en", ""),
                ind["frequency"],
                ind["unit"],
                ind["preferred_direction"],
                f"config/indicators/{code}.yaml",
            ),
        )
    conn.commit()


def sync(db_path: Path) -> tuple[int, int]:
    """Returns (rows_fetched, rows_changed)."""
    indicator_configs, source_configs = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    source_meta = source_configs["statbel"]

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_reference_rows(conn, indicator_configs)

    now = datetime.now(timezone.utc).isoformat()
    fetched = 0
    changed = 0

    for code in STATBEL_INDICATORS:
        ind = indicator_configs[code]
        url = source_meta["base_url"] + ind["fetch"]["query"]

        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            ("statbel", "statbel", now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        source = StatbelSource()
        rows = source.fetch(url, cache_key=code, conn=conn)
        rows_read = source._rows_read_hint(rows) or len(rows)
        fetched += len(rows)

        for row in rows:
            current = conn.execute(
                """SELECT value, status FROM observations
                   WHERE indicator_id = ? AND geo_id = ? AND period = ? AND is_latest = 1""",
                (code, row["geo_id"], row["period"]),
            ).fetchone()
            if current is not None and current[0] == row["value"] and current[1] == row["status"]:
                continue  # unchanged -- no new vintage

            if current is not None:
                conn.execute(
                    """UPDATE observations SET is_latest = 0
                       WHERE indicator_id = ? AND geo_id = ? AND period = ? AND is_latest = 1""",
                    (code, row["geo_id"], row["period"]),
                )

            period_start, period_end = derive_period_bounds(row["period"], ind["frequency"])
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO observations
                    (indicator_id, geo_id, period, vintage, value, status,
                     period_start, period_end, is_latest, fetch_run_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    code,
                    row["geo_id"],
                    row["period"],
                    now,
                    row["value"],
                    row["status"],
                    period_start,
                    period_end,
                    fetch_run_id,
                    now,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Vintage collision writing {code}/{row['geo_id']}/{row['period']}/{now}: "
                    "a row with this exact key already exists. Refusing to silently drop "
                    "the new value (CLAUDE.md rule 13)."
                )
            changed += 1

        conn.execute(
            "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
            "WHERE fetch_run_id = ?",
            (datetime.now(timezone.utc).isoformat(), rows_read, len(rows), fetch_run_id),
        )

    conn.commit()
    conn.close()
    return fetched, changed


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Statbel municipal indicators (Block F)")
    parser.add_argument("--db", required=True, help="Path to the SQLite DB file")
    args = parser.parse_args()
    fetched, changed = sync(Path(args.db))
    print(f"Fetched {fetched} observations, {changed} new vintage(s) written.")


if __name__ == "__main__":
    main()
