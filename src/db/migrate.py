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

#: A migration file's FIRST LINE, verbatim, opts it into "recreate mode" --
#: see _apply_recreate_mode's docstring for what that mode does and why it
#: exists. Checked against the raw file text, never the comment-stripped
#: statement list, so it is unambiguous and cannot be spoofed by a comment
#: appearing later in the file.
RECREATE_MODE_MARKER = "-- migration-mode: recreate-with-foreign-keys-off"

#: Statements a recreate-mode file must NEVER contain -- see
#: _apply_recreate_mode's docstring for why the runner, not the file, owns
#: all transaction and foreign-key-pragma control in this mode (SHOULD-FIX 1,
#: PR #174 audit: a file trusted to manage its own BEGIN/COMMIT/PRAGMA
#: foreign_keys was proven, on temp-database probes, to leave foreign keys
#: off for a LATER migration if it forgot to restore them, to leave partial
#: DDL applied if it forgot its own BEGIN, and to record itself as applied
#: even when its FK check ran after its own COMMIT and so could not be
#: rolled back). Matched case-insensitively against each split statement's
#: own first word(s).
_FORBIDDEN_IN_RECREATE_MODE = (
    re.compile(r"^BEGIN\b", re.IGNORECASE),
    re.compile(r"^COMMIT\b", re.IGNORECASE),
    re.compile(r"^ROLLBACK\b", re.IGNORECASE),
    re.compile(r"^PRAGMA\s+FOREIGN_KEYS\b", re.IGNORECASE),
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("migrate")


class MigrationError(Exception):
    """A migration file is malformed, or could not be safely applied."""


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


def _is_recreate_mode(raw_text: str) -> bool:
    """True iff `raw_text`'s first line is exactly RECREATE_MODE_MARKER.
    Checked against the RAW file, before comment-stripping -- a marker
    appearing later, inside a descriptive comment, must never opt a file in
    by accident."""
    first_line = raw_text.splitlines()[0].strip() if raw_text.strip() else ""
    return first_line == RECREATE_MODE_MARKER


def _refuse_forbidden_statements(path: Path, statements: list[str]) -> None:
    """A recreate-mode file must contain ONLY the schema-change statements
    themselves -- no BEGIN/COMMIT/ROLLBACK/PRAGMA foreign_keys of its own.
    The runner owns all four; a file that tries to manage them itself is
    refused outright (CLAUDE.md rule 13) rather than silently trusted, which
    is exactly the trust the PR #174 audit proved unsafe."""
    for statement in statements:
        for pattern in _FORBIDDEN_IN_RECREATE_MODE:
            if pattern.match(statement):
                raise MigrationError(
                    f"{path.name}: a recreate-with-foreign-keys-off migration must not "
                    f"contain {statement.split(maxsplit=1)[0]!r} -- the runner owns "
                    "BEGIN/COMMIT and PRAGMA foreign_keys for this mode. Remove it; the "
                    "runner applies both around every statement in this file."
                )


def _apply_recreate_mode(
    conn: sqlite3.Connection, path: Path, version: int, checksum: str, statements: list[str]
) -> None:
    """Runner-owned application of a 'recreate-with-foreign-keys-off'
    migration -- one that recreates a table other tables' foreign keys
    target, because SQLite has no ALTER TABLE ... ALTER CONSTRAINT for an
    inline CHECK (migrations/004_nuts2_geography_level.sql is the first;
    see its own comment for why PRAGMA defer_foreign_keys does not
    substitute for turning foreign_keys off around the whole change).

    THE RUNNER, NOT THE FILE, OWNS EVERYTHING (PR #174 audit, SHOULD-FIX 1
    -- three failure modes proved on temp-database probes when the file was
    trusted instead):
      (a) PRAGMA foreign_keys is forced back ON in a `finally`, unconditionally
          -- a file that forgot to restore it used to leave FK enforcement
          off for every migration and every use of the connection after it.
      (b) The runner opens its own BEGIN before the first statement and this
          is the ONLY transaction boundary -- a file without its own BEGIN
          used to leave a partial CREATE TABLE / DROP INDEX applied and
          uncommitted-but-unrollbackable if a later statement in the same
          file failed.
      (c) PRAGMA foreign_key_check runs INSIDE this transaction, before
          COMMIT -- checking after the file's own COMMIT (the old design)
          meant a real violation could be found but never undone, and the
          migration was still recorded as applied over a broken schema.

    All three probes this fixes must end in the same state: nothing applied,
    nothing recorded in schema_migrations, foreign_keys back ON. See
    tests/test_migrations.py for the reproductions.
    """
    _refuse_forbidden_statements(path, statements)
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        conn.execute("BEGIN")
        for statement in statements:
            conn.execute(statement)
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise MigrationError(
                f"{path.name}: PRAGMA foreign_key_check found {len(violations)} "
                f"violation(s) before COMMIT -- refusing to apply or record this "
                f"migration: {violations[:5]}"
            )
        conn.execute(
            "INSERT INTO schema_migrations (version, filename, applied_at, checksum) "
            "VALUES (?, ?, ?, ?)",
            (version, path.name, datetime.now(timezone.utc).isoformat(), checksum),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        # Unconditional, regardless of whether the transaction above
        # committed or rolled back -- (a) above. PRAGMA foreign_keys is a
        # no-op inside an open transaction, but rollback()/commit() have
        # already ended it by the time this runs, so it takes effect.
        conn.execute("PRAGMA foreign_keys = ON")


def _apply_normal_mode(
    conn: sqlite3.Connection, path: Path, version: int, checksum: str, statements: list[str]
) -> None:
    conn.execute("BEGIN")
    for statement in statements:
        conn.execute(statement)
    conn.execute(
        "INSERT INTO schema_migrations (version, filename, applied_at, checksum) "
        "VALUES (?, ?, ?, ?)",
        (version, path.name, datetime.now(timezone.utc).isoformat(), checksum),
    )
    conn.commit()


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
            raw_text = path.read_text()
            statements = _split_statements(raw_text)
            try:
                if _is_recreate_mode(raw_text):
                    _apply_recreate_mode(conn, path, version, checksum, statements)
                else:
                    _apply_normal_mode(conn, path, version, checksum, statements)
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
