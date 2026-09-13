"""scripts/validate_data.py validates every store, not only the database --
pipeline repair part 4.

The six hand-loaded stores (extra_csv in config/stores.yaml) are never in any
database, and the rules only query SQLite, so until part 4 they got a field
count and nothing else. These tests plant a violation in an extra_csv store
and require the real CLI to catch it, and check the one thing the throwaway
copy writes back: the volume snapshot.

Marked slow: each builds a real working database with
scripts/build_staging_db.py.
"""

import csv
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from build_staging_db import build  # noqa: E402
from export_observations_csv import COLUMNS  # noqa: E402

from src.db import migrate  # noqa: E402

pytestmark = pytest.mark.slow

INDICATOR = "FISCAL_TOT_TAXES"
GEO = "be:mun:11002"  # Antwerp


def _row(period, vintage, value, is_latest):
    return {
        "indicator_id": INDICATOR,
        "geo_id": GEO,
        "period": period,
        "vintage": vintage,
        "value": value,
        "status": "final",
        "period_start": f"{period}-01-01",
        "period_end": f"{period}-12-31",
        "is_latest": is_latest,
        "created_at": vintage,
    }


def _setup(tmp_path: Path, rows: list[dict], indicator_ids=(INDICATOR,)) -> tuple[Path, Path]:
    """A working database built from an empty committed one, and a registry
    naming one extra_csv store holding `rows`."""
    store = tmp_path / "fiscal_income_observations.csv"
    with store.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    registry = tmp_path / "stores.yaml"
    registry.write_text(
        yaml.safe_dump(
            {
                "stores": {
                    "fiscal_income": {
                        "path": str(store),
                        "source_id": "statbel",
                        "mode": "extra_csv",
                        "indicators": list(indicator_ids),
                        "reference_rows": {
                            "script": "scripts/sync_fiscal_income.py",
                            "args": ["--reference-rows-only"],
                        },
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    committed = tmp_path / "committed.db"
    migrate.run(committed, migrations_dir=REPO / "migrations")
    working = tmp_path / "local" / "working.db"
    build(source_db=committed, working_db=working, stores_path=registry)
    return working, registry


def _validate(working: Path, registry: Path | str, *extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            str(REPO / "scripts" / "validate_data.py"),
            "--db",
            str(working),
            "--stores",
            str(registry),
            *extra,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO,
        timeout=300,
    )


def _observation_count(db: Path) -> int:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    finally:
        conn.close()


def test_a_violation_in_a_hand_loaded_store_is_caught(tmp_path):
    """Two is_latest rows for one cell -- the class of hand-edit mistake a
    field count can never see. unique_latest must fire on the extra_csv store."""
    working, registry = _setup(
        tmp_path,
        [
            _row("2023", "2026-01-01T00:00:00+00:00", "100.0", "1"),
            _row("2023", "2026-02-01T00:00:00+00:00", "101.0", "1"),
        ],
    )

    result = _validate(working, registry)

    assert result.returncode == 1, result.stdout + result.stderr
    assert "unique_latest" in result.stderr

    # The same database validated alone cannot see it: the row is not there.
    alone = _validate(working, "")
    assert "unique_latest" not in alone.stderr


def test_a_store_that_cannot_load_is_a_failure_not_a_crash(tmp_path):
    """An indicator with no config: the loader refuses, and that refusal is
    reported as a violation with the store named, exit code 1."""
    good = [_row("2023", "2026-01-01T00:00:00+00:00", "100.0", "1")]
    working, registry = _setup(tmp_path, good)
    bad = {**good[0], "indicator_id": "NO_SUCH_INDICATOR"}
    store = Path(
        yaml.safe_load(registry.read_text(encoding="utf-8"))["stores"]["fiscal_income"]["path"]
    )
    with store.open("a", newline="", encoding="utf-8") as fh:
        csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n").writerow(bad)

    result = _validate(working, registry)

    assert result.returncode == 1
    assert "store_loads" in result.stderr
    assert "fiscal_income" in result.stderr
    assert "Traceback" not in result.stderr


def test_the_copy_is_thrown_away_but_its_counts_become_the_baseline(tmp_path):
    """--record-volume writes the consolidated count -- the one tomorrow's
    row_collapse compares against -- into --db, and adds no observation to it."""
    working, registry = _setup(
        tmp_path,
        [
            _row("2022", "2026-01-01T00:00:00+00:00", "99.0", "1"),
            _row("2023", "2026-01-01T00:00:00+00:00", "100.0", "1"),
        ],
    )
    before = _observation_count(working)

    result = _validate(working, registry, "--record-volume")

    assert result.returncode == 0, result.stdout + result.stderr
    assert _observation_count(working) == before
    conn = sqlite3.connect(working)
    try:
        recorded = conn.execute(
            "SELECT new_count FROM indicator_volume WHERE indicator_id = ?", (INDICATOR,)
        ).fetchall()
    finally:
        conn.close()
    assert recorded == [(2,)]
