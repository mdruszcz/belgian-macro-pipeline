"""
One-time, idempotent rename of the legacy indicators/observations/fetch_log
tables so they no longer collide by name with the canonical schema
(migrations/001_core_schema.sql). forecasts is untouched -- it doesn't
collide and stays out of scope entirely, per docs/decisions/0001-data-model.md.

Kept separate from migrations/*.sql deliberately: the numbered-migration
system is for canonical-schema evolution only, not legacy cleanup.

Safe to call on every startup (see belgian_macro_db.py's _init_schema):
no-ops if the legacy tables were already renamed, and no-ops on a DB that
never had them (a fresh test DB, for instance).
"""

import logging
import sqlite3

log = logging.getLogger("rename_legacy_tables")

# (old_name, new_name, a column that only the OLD shape has)
RENAMES = [
    ("indicators", "legacy_indicators", "code"),
    ("observations", "legacy_observations", "indicator_code"),
    ("fetch_log", "legacy_fetch_log", "indicator_code"),
]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def rename_legacy_tables(conn: sqlite3.Connection) -> None:
    existing = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for old_name, new_name, old_only_column in RENAMES:
        if old_name not in existing:
            continue  # never had a legacy table under this name
        if old_only_column not in _table_columns(conn, old_name):
            continue  # already the canonical shape (or something else entirely) -- don't touch
        log.info(f"Renaming legacy table {old_name} -> {new_name}")
        conn.execute(f"ALTER TABLE {old_name} RENAME TO {new_name}")
    conn.commit()
