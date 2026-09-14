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


def _is_self_managed_transaction(statements: list[str]) -> bool:
    """True for a migration that must run in autocommit mode rather than
    inside this runner's usual outer BEGIN/COMMIT -- i.e. one whose first
    statement is exactly `PRAGMA foreign_keys = OFF`. Recreating a table
    that other tables' foreign keys target (SQLite has no ALTER TABLE ...
    ALTER CONSTRAINT) needs that PRAGMA around the whole change, and it is a
    documented no-op once a transaction is already open -- see
    migrations/004_nuts2_geography_level.sql's own comment for the full
    story, including why PRAGMA defer_foreign_keys does not substitute for
    it. Such a file owns its own BEGIN/COMMIT and restores `foreign_keys =
    ON` itself; this runner only decides whether to wrap it in a
    transaction of its own, and checks PRAGMA foreign_key_check afterwards
    either way is clean before recording the migration as applied.
    """
    if not statements:
        return False
    return " ".join(statements[0].split()).upper() == "PRAGMA FOREIGN_KEYS = OFF"


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
                if _is_self_managed_transaction(statements):
                    # See migrations/004_nuts2_geography_level.sql's own
                    # comment for the full story: recreating a table other
                    # tables' foreign keys target needs PRAGMA
                    # foreign_keys=OFF around the whole change (SQLite's own
                    # documented recipe), and that pragma is a no-op once a
                    # transaction is already open -- so this file's
                    # statements run directly in autocommit mode, and the
                    # file itself owns the BEGIN/COMMIT around its schema
                    # change (and restores foreign_keys=ON at the end).
                    for statement in statements:
                        conn.execute(statement)
                    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                    if violations:
                        raise RuntimeError(
                            f"{path.name}: PRAGMA foreign_key_check found "
                            f"{len(violations)} violation(s) after a self-managed "
                            f"migration -- refusing to record it as applied: "
                            f"{violations[:5]}"
                        )
                else:
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
