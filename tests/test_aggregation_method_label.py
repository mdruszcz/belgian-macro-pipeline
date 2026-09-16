"""No indicator may claim `population_weighted` as its aggregation method.

CLAUDE.md's aggregation rule forbids population-weighting outright: it was
measured against the correct figure and is wrong for both ratios in this
pipeline, because population is not their denominator. 27 rows nevertheless
carried it, written as a hardcoded literal by two sync scripts, until
migrations/005_population_weighted_label.sql.

These tests exist because the column is vestigial -- nothing under src/ reads
it, so no existing test or output would notice the value coming back. The
risk is not a wrong number today; it is that `aggregation_method` is the
obviously-named field someone implementing an aggregation reaches for, and a
forbidden method recommended by the data is a trap.
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
COMMITTED_DB = REPO_ROOT / "data" / "belgian_macro.db"
FORBIDDEN = "population_weighted"

# The scripts that used to write the literal. Any future writer of this column
# belongs in this list too.
WRITER_SCRIPTS = (
    REPO_ROOT / "scripts" / "port_existing_indicators.py",
    REPO_ROOT / "scripts" / "sync_to_canonical.py",
)


@pytest.mark.skipif(not COMMITTED_DB.exists(), reason="committed database not present")
def test_no_stored_indicator_claims_population_weighting():
    conn = sqlite3.connect(f"file:{COMMITTED_DB}?mode=ro", uri=True)
    try:
        offenders = [
            row[0]
            for row in conn.execute(
                "SELECT indicator_id FROM indicators WHERE aggregation_method = ?",
                (FORBIDDEN,),
            )
        ]
    finally:
        conn.close()
    assert not offenders, (
        f"{len(offenders)} indicator(s) claim the forbidden aggregation method "
        f"{FORBIDDEN!r}: {offenders[:10]}. CLAUDE.md's aggregation rule forbids "
        "population-weighting; use 'not_applicable', or 'sum' where the parts "
        "really are additive."
    )


@pytest.mark.parametrize("script", WRITER_SCRIPTS, ids=lambda p: p.name)
def test_no_script_writes_the_forbidden_label(script):
    """A source check, not a database one: the stored rows are corrected by
    migration 005, but a script still emitting the literal would write it
    straight back on the next from-scratch build."""
    source = script.read_text(encoding="utf-8")
    # The word appears in this file's own explanatory comments, and may appear
    # in the scripts' comments too -- only a quoted SQL literal is a write.
    assert f"'{FORBIDDEN}'" not in source, (
        f"{script.name} still writes the forbidden aggregation method "
        f"{FORBIDDEN!r} as a SQL literal"
    )


def test_the_migration_is_idempotent_and_leaves_other_methods_alone(tmp_path):
    """Applying the migration twice is a no-op the second time, and it must not
    touch 'sum' or 'not_applicable' rows."""
    db = tmp_path / "probe.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE indicators (indicator_id TEXT PRIMARY KEY, aggregation_method TEXT)")
    conn.executemany(
        "INSERT INTO indicators (indicator_id, aggregation_method) VALUES (?, ?)",
        [
            ("WEIGHTED_ONE", FORBIDDEN),
            ("WEIGHTED_TWO", FORBIDDEN),
            ("A_SUM", "sum"),
            ("A_REFUSAL", "not_applicable"),
        ],
    )
    conn.commit()

    sql = (REPO_ROOT / "migrations" / "005_population_weighted_label.sql").read_text(
        encoding="utf-8"
    )
    for _ in range(2):
        conn.executescript(sql)
        conn.commit()

    rows = dict(conn.execute("SELECT indicator_id, aggregation_method FROM indicators"))
    conn.close()
    assert rows == {
        "WEIGHTED_ONE": "not_applicable",
        "WEIGHTED_TWO": "not_applicable",
        "A_SUM": "sum",
        "A_REFUSAL": "not_applicable",
    }


def test_the_migration_changes_no_other_column():
    """Guard against the migration growing beyond its one job. It is a
    data-only migration: exactly one UPDATE, of exactly one column."""
    sql = (REPO_ROOT / "migrations" / "005_population_weighted_label.sql").read_text(
        encoding="utf-8"
    )
    statements = [
        line.strip()
        for line in sql.splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]
    body = " ".join(statements).upper()
    assert body.count("UPDATE") == 1, body
    assert "SET AGGREGATION_METHOD" in body
    for forbidden_verb in ("DROP", "ALTER", "DELETE", "CREATE TABLE", "INSERT"):
        assert forbidden_verb not in body, f"unexpected {forbidden_verb} in migration 005"


def test_the_migration_needs_no_recreate_mode_marker():
    """src/db/migrate.py reads a `migration-mode:` marker on the FIRST line to
    decide whether to recreate tables with foreign keys off. This migration
    changes no schema, so it must not carry one -- claiming recreate mode
    would take the foreign-key-off path for a plain UPDATE."""
    first_line = (
        (REPO_ROOT / "migrations" / "005_population_weighted_label.sql")
        .read_text(encoding="utf-8")
        .splitlines()[0]
    )
    assert "migration-mode:" not in first_line, first_line


def test_migration_filename_matches_the_runners_pattern():
    from src.db.migrate import FILENAME_RE  # noqa: PLC0415

    assert FILENAME_RE.match("005_population_weighted_label.sql")


if __name__ == "__main__":  # pragma: no cover
    sys.exit(subprocess.call([sys.executable, "-m", "pytest", __file__]))
