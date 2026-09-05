"""
Schema migration runner. No ORM.

Applies numbered .sql files under migrations/ in order, tracked in a
schema_migrations table. Idempotent: re-running is a no-op for migrations
already applied, since the daily workflow may invoke this every day.
"""

import argparse
import hashlib
import logging
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
FILENAME_RE = re.compile(r"^(\d{3})_.+\.sql$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("migrate")


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _ensure_tracking_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            filename   TEXT NOT NULL,
            applied_at TEXT NOT NULL,
            checksum   TEXT NOT NULL
        )
    """)
    conn.commit()


def _checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _split_statements(sql: str) -> list[str]:
    """Split a migration file into individual statements.

    conn.executescript() cannot be used here: it issues an implicit COMMIT
    before running, and each DDL statement within it then auto-commits
    immediately in SQLite's autocommit mode -- a syntax error partway through
    a script leaves the earlier statements permanently applied, not rolled
    back (confirmed directly: a CREATE TABLE before a bad statement survived
    conn.rollback()). Executing statements one at a time inside an explicit
    BEGIN/COMMIT does roll back correctly.

    Comment lines (leading -- ) are stripped before splitting on ';', which
    is safe for this project's DDL: no string literals or trigger bodies
    containing semicolons appear in any migration file.
    """
    lines = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    return [s.strip() for s in cleaned.split(";") if s.strip()]


def discover_migrations(migrations_dir: Path) -> list[tuple[int, Path]]:
    found = []
    for p in sorted(migrations_dir.glob("*.sql")):
        m = FILENAME_RE.match(p.name)
        if not m:
            raise ValueError(f"Migration file does not match NNN_description.sql: {p.name}")
        found.append((int(m.group(1)), p))
    found.sort(key=lambda t: t[0])
    versions = [v for v, _ in found]
    if len(versions) != len(set(versions)):
        raise ValueError(f"Duplicate migration version numbers: {versions}")
    return found


def run(db_path: Path, migrations_dir: Path = MIGRATIONS_DIR) -> None:
    conn = connect(db_path)
    try:
        _ensure_tracking_table(conn)
        applied = {
            row[0]: row[1]
            for row in conn.execute("SELECT version, checksum FROM schema_migrations")
        }
        for version, path in discover_migrations(migrations_dir):
            checksum = _checksum(path)
            if version in applied:
                if applied[version] != checksum:
                    raise RuntimeError(
                        f"{path.name}: checksum mismatch against schema_migrations "
                        f"(recorded {applied[version]}, on-disk {checksum}). "
                        "A migration file must never be edited after it has been applied; "
                        "write a new migration instead."
                    )
                log.info(f"skip    {path.name} (already applied)")
                continue
            log.info(f"apply   {path.name}")
            statements = _split_statements(path.read_text())
            try:
                conn.execute("BEGIN")
                for statement in statements:
                    conn.execute(statement)
                conn.execute(
                    "INSERT INTO schema_migrations (version, filename, applied_at, checksum) "
                    "VALUES (?, ?, ?, ?)",
                    (version, path.name, datetime.now(timezone.utc).isoformat(), checksum),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                log.error(f"FAILED  {path.name}")
                raise
            log.info(f"applied {path.name}")
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Apply pending schema migrations")
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file")
    args = ap.parse_args()
    try:
        run(Path(args.db))
    except Exception as e:
        log.error(f"Migration run aborted: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
