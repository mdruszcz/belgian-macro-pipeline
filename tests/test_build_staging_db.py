"""Tests for scripts/build_staging_db.py -- the disposable working database
assembly line (PR1 of the pipeline repair; `make assemble`, not wired into
`make all`/CI yet, see the script's own docstring).

Marked slow: each test runs migrate, load_geography and
ensure_reference_rows as real subprocesses against a real (if minimal)
database, which is exactly the sub-minute-loop tradeoff `make test`
excludes (pyproject.toml's `slow` marker).
"""

import shutil
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from build_staging_db import StagingBuildError, build  # noqa: E402

from src.db import migrate  # noqa: E402

pytestmark = pytest.mark.slow


def _fresh_source_db(path: Path) -> Path:
    """A minimal but real source database: migrated schema, no geography or
    observations yet -- build() itself is responsible for loading geography,
    so the source db does not need to carry it."""
    migrate.run(path, migrations_dir=REPO / "migrations")
    return path


def test_assemble_succeeds_with_zero_in_db_stores(tmp_path):
    """THE CASE THIS PR MUST GET RIGHT: config/stores.yaml declares zero
    in_db stores today (all six are extra_csv), so step 6 of the assembly
    line has nothing to load. It must still complete successfully rather
    than erroring on an empty list."""
    source = _fresh_source_db(tmp_path / "source.db")
    working = tmp_path / "local" / "working.db"

    result = build(source_db=source, working_db=working)

    assert result == working
    assert working.is_file()
    conn = sqlite3.connect(str(working))
    # Geography was loaded (step 4).
    assert conn.execute("SELECT COUNT(*) FROM geographies").fetchone()[0] > 0
    # Reference rows for at least one committed store were created (step 5) --
    # FISCAL_TOT_NET_TAXABLE_INC's source_id is 'statbel' per its config.
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM indicators WHERE indicator_id = 'FISCAL_TOT_NET_TAXABLE_INC'"
        ).fetchone()[0]
        == 1
    )
    conn.close()


def test_assemble_is_idempotent(tmp_path):
    """Run twice; the second run must succeed (not IntegrityError on the
    rm'd-and-rebuilt file) and produce byte-identical output."""
    source = _fresh_source_db(tmp_path / "source.db")
    working = tmp_path / "local" / "working.db"

    build(source_db=source, working_db=working)
    first = tmp_path / "run1.db"
    shutil.copy2(working, first)

    build(source_db=source, working_db=working)  # must not raise IntegrityError

    assert working.read_bytes() == first.read_bytes()


def test_assemble_refuses_a_missing_source_db(tmp_path):
    with pytest.raises(StagingBuildError, match="No source database"):
        build(source_db=tmp_path / "nope.db", working_db=tmp_path / "local" / "working.db")


def test_assemble_removes_a_stale_wal_and_shm(tmp_path):
    """A previous run's connection left WAL/SHM siblings behind -- these must
    not survive into the fresh rebuild (they would otherwise let stale pages
    leak into the new file when SQLite next opens it)."""
    source = _fresh_source_db(tmp_path / "source.db")
    working = tmp_path / "local" / "working.db"
    working.parent.mkdir(parents=True, exist_ok=True)
    working.write_bytes(b"not a real db")
    (working.parent / (working.name + "-shm")).write_bytes(b"stale")
    (working.parent / (working.name + "-wal")).write_bytes(b"stale")

    build(source_db=source, working_db=working)

    assert not (working.parent / (working.name + "-shm")).is_file()
    assert not (working.parent / (working.name + "-wal")).is_file()
