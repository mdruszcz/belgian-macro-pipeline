"""Tests for scripts/build_staging_db.py -- the disposable working database
assembly line (`make assemble`; the first step of `make all`, CI and both
workflows since the ONEM/WalStat cutover).

Marked slow: each test runs migrate, load_geography and
ensure_reference_rows as real subprocesses against a real (if minimal)
database, which is exactly the sub-minute-loop tradeoff `make test`
excludes (pyproject.toml's `slow` marker).

Every test uses a small registry of its own rather than config/stores.yaml:
the real one would load ~84,000 ONEM and WalStat rows per test, and the
real-data assemble is exercised once per session by tests/conftest.py's
`working_db` fixture instead.
"""

import csv
import shutil
import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from build_staging_db import StagingBuildError, build  # noqa: E402
from export_observations_csv import COLUMNS  # noqa: E402

from src.db import migrate  # noqa: E402

pytestmark = pytest.mark.slow


def _fresh_source_db(path: Path) -> Path:
    """A minimal but real source database: migrated schema, no geography or
    observations yet -- build() itself is responsible for loading geography,
    so the source db does not need to carry it."""
    migrate.run(path, migrations_dir=REPO / "migrations")
    return path


def _registry(tmp_path: Path, stores: dict) -> Path:
    path = tmp_path / "stores.yaml"
    path.write_text(yaml.safe_dump({"stores": stores}, sort_keys=False), encoding="utf-8")
    return path


def _extra_csv_only(tmp_path: Path) -> Path:
    """One real extra_csv store and no in_db store."""
    return _registry(
        tmp_path,
        {
            "fiscal_income": {
                "path": "data/fiscal_income_observations.csv",
                "source_id": "statbel",
                "mode": "extra_csv",
                "indicators": [
                    "FISCAL_NBR_NON_ZERO_INC",
                    "FISCAL_TOT_MUNICIP_TAXES",
                    "FISCAL_TOT_NET_TAXABLE_INC",
                    "FISCAL_TOT_TAXES",
                ],
                "reference_rows": {
                    "script": "scripts/sync_fiscal_income.py",
                    "args": ["--reference-rows-only"],
                },
            }
        },
    )


def _in_db_store(tmp_path: Path, rows: list[dict]) -> Path:
    csv_path = tmp_path / "walstat_observations.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return _registry(
        tmp_path,
        {
            "walstat": {
                "path": str(csv_path),
                "source_id": "walstat",
                "mode": "in_db",
                "indicators": ["MUN_DEBT_TOTAL_PER_CAPITA"],
                "reference_rows": {
                    "script": "scripts/sync_walstat.py",
                    "args": ["--reference-rows-only"],
                },
            }
        },
    )


def _row(period: str, vintage: str, value: str, is_latest: str, status: str = "final") -> dict:
    return {
        "indicator_id": "MUN_DEBT_TOTAL_PER_CAPITA",
        "geo_id": "be:mun:92094",
        "period": period,
        "vintage": vintage,
        "value": value,
        "status": status,
        "period_start": f"{period}-01-01",
        "period_end": f"{period}-12-31",
        "is_latest": is_latest,
        "created_at": vintage,
    }


def _write_indicator_csv(directory: Path, indicator_id: str, rows: list[dict]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f"{indicator_id}.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def test_assemble_never_loads_an_undeclared_file_in_a_directory_store(tmp_path):
    """Audit SHOULD-FIX 6: a one_csv_per_indicator store must load only its
    DECLARED files (Store.csv_paths()), never every *.csv a glob over the
    directory would find -- an undeclared file sitting beside the declared
    ones (an indicator removed from the registry without deleting its old
    file) must stay unloaded."""
    source = _fresh_source_db(tmp_path / "source.db")
    working = tmp_path / "local" / "working.db"
    store_dir = tmp_path / "international"
    row = _row("2023", "2026-01-01T00:00:00+00:00", "100.5", "1")
    row["indicator_id"] = "GDP_VOLUME_EUROPE"
    _write_indicator_csv(store_dir, "GDP_VOLUME_EUROPE", [row])
    undeclared_row = dict(row, indicator_id="GOV_DEBT_EUROPE")
    _write_indicator_csv(store_dir, "GOV_DEBT_EUROPE", [undeclared_row])  # present, not declared

    registry = _registry(
        tmp_path,
        {
            "international": {
                "path": str(store_dir),
                "source_id": "eurostat",
                "mode": "in_db",
                "layout": "one_csv_per_indicator",
                "indicators": ["GDP_VOLUME_EUROPE"],  # GOV_DEBT_EUROPE NOT declared
                "reference_rows": {"script": "scripts/sync_international.py"},
            }
        },
    )

    build(source_db=source, working_db=working, stores_path=registry)

    conn = sqlite3.connect(str(working))
    try:
        counts = dict(
            conn.execute(
                "SELECT indicator_id, COUNT(*) FROM observations "
                "WHERE indicator_id IN ('GDP_VOLUME_EUROPE', 'GOV_DEBT_EUROPE') GROUP BY 1"
            )
        )
    finally:
        conn.close()
    assert counts == {
        "GDP_VOLUME_EUROPE": 1
    }, "GOV_DEBT_EUROPE's file exists on disk but is not declared -- it must not be loaded"


def test_assemble_succeeds_with_zero_in_db_stores(tmp_path):
    """A registry with nothing to load must still complete rather than
    erroring on an empty list."""
    source = _fresh_source_db(tmp_path / "source.db")
    working = tmp_path / "local" / "working.db"

    result = build(source_db=source, working_db=working, stores_path=_extra_csv_only(tmp_path))

    assert result == working
    assert working.is_file()
    conn = sqlite3.connect(str(working))
    # Geography was loaded (step 4).
    assert conn.execute("SELECT COUNT(*) FROM geographies").fetchone()[0] > 0
    # Reference rows for the registered store were created (step 5).
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM indicators WHERE indicator_id = 'FISCAL_TOT_NET_TAXABLE_INC'"
        ).fetchone()[0]
        == 1
    )
    conn.close()


def test_assemble_loads_an_in_db_store_with_its_vintage_chain_intact(tmp_path):
    """The reason the stores are reloaded BEFORE the syncs: upsert_observation
    compares against the is_latest row, so a superseded vintage and the flag
    on the current one must arrive exactly as committed."""
    source = _fresh_source_db(tmp_path / "source.db")
    working = tmp_path / "local" / "working.db"
    registry = _in_db_store(
        tmp_path,
        [
            _row("2023", "2026-01-01T00:00:00+00:00", "100.5", "0"),
            _row("2023", "2026-02-01T00:00:00+00:00", "101.5", "1"),
            # A masked cell: NULL value, suppressed -- never collapsed into a zero.
            _row("2024", "2026-02-01T00:00:00+00:00", "", "1", status="suppressed"),
        ],
    )

    build(source_db=source, working_db=working, stores_path=registry)

    conn = sqlite3.connect(str(working))
    rows = conn.execute(
        "SELECT period, vintage, value, status, is_latest FROM observations ORDER BY period, vintage"
    ).fetchall()
    runs = conn.execute("SELECT source_id, adapter FROM fetch_runs").fetchall()
    conn.close()
    assert rows == [
        ("2023", "2026-01-01T00:00:00+00:00", 100.5, "final", 0),
        ("2023", "2026-02-01T00:00:00+00:00", 101.5, "final", 1),
        ("2024", "2026-02-01T00:00:00+00:00", None, "suppressed", 1),
    ]
    # Recorded as a rebuild under the store's own source, never as a fetch.
    assert runs == [("walstat", "rebuild")]


def test_assemble_refuses_a_source_db_that_still_holds_in_db_rows(tmp_path):
    """A database from before the cutover still carries ONEM and WalStat.
    Loading the CSV on top would collide on the primary key halfway through;
    the refusal must name the real problem instead."""
    row = _row("2023", "2026-01-01T00:00:00+00:00", "100.5", "1")
    registry = _in_db_store(tmp_path, [row])
    source = _fresh_source_db(tmp_path / "source.db")
    staged = tmp_path / "staged.db"
    build(source_db=source, working_db=staged, stores_path=registry)

    with pytest.raises(StagingBuildError, match="already holds rows"):
        build(source_db=staged, working_db=tmp_path / "local" / "working.db", stores_path=registry)


def test_assemble_is_idempotent(tmp_path):
    """Run twice; the second run must succeed (not IntegrityError on the
    rm'd-and-rebuilt file) and produce byte-identical output."""
    source = _fresh_source_db(tmp_path / "source.db")
    working = tmp_path / "local" / "working.db"
    registry = _extra_csv_only(tmp_path)

    build(source_db=source, working_db=working, stores_path=registry)
    first = tmp_path / "run1.db"
    shutil.copy2(working, first)

    build(source_db=source, working_db=working, stores_path=registry)

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

    build(source_db=source, working_db=working, stores_path=_extra_csv_only(tmp_path))

    assert not (working.parent / (working.name + "-shm")).is_file()
    assert not (working.parent / (working.name + "-wal")).is_file()
