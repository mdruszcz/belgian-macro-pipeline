"""Tests for scripts/offload_stores.py's one_csv_per_indicator path
(international pilot PR 1) -- the directory-store analogue of
tests/test_offload_stores.py's single_csv checks: a fixed point across
assemble+offload, an undeclared indicator refused, and a zero-row indicator
writing no file.

Marked slow for the same reason as test_offload_stores.py: each test runs a
real build_staging_db.py + offload_stores.py pair.
"""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from build_staging_db import build  # noqa: E402
from export_observations_csv import COLUMNS  # noqa: E402
from offload_stores import OffloadError, offload  # noqa: E402

from src.db import migrate  # noqa: E402
from src.db.vintages import upsert_observation  # noqa: E402

pytestmark = pytest.mark.slow

GDP = "GDP_VOLUME_EUROPE"
HICP = "HICP_ANNUAL_RATE_EUROPE"
GEO = "be:country"


def _row(indicator, period, vintage, value, is_latest):
    return {
        "indicator_id": indicator,
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


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def pipeline(tmp_path):
    """A committed database, an international-style directory store with one
    committed indicator file, the registry naming it, and the assembled
    working database -- the state right after `Assemble the working
    database` for a directory store."""
    committed_db = tmp_path / "belgian_macro.db"
    migrate.run(committed_db, migrations_dir=REPO / "migrations")

    store_dir = tmp_path / "international"
    _write_csv(
        store_dir / f"{GDP}.csv",
        [
            _row(GDP, "2023", "2026-01-01T00:00:00+00:00", "100.0", "0"),
            _row(GDP, "2023", "2026-02-01T00:00:00+00:00", "101.0", "1"),
        ],
    )
    # HICP is declared but has no file yet -- the in_db asymmetry.

    registry = tmp_path / "stores.yaml"
    registry.write_text(
        yaml.safe_dump(
            {
                "stores": {
                    "international": {
                        "path": str(store_dir),
                        "source_id": "eurostat",
                        "mode": "in_db",
                        "layout": "one_csv_per_indicator",
                        "indicators": [GDP, HICP],
                        "reference_rows": {"script": "scripts/sync_international.py"},
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    working = tmp_path / "local" / "working.db"
    build(source_db=committed_db, working_db=working, stores_path=registry)

    return {
        "committed_db": committed_db,
        "store_dir": store_dir,
        "registry": registry,
        "working": working,
    }


def test_offload_writes_one_file_per_indicator_and_none_for_zero_rows(pipeline):
    counts = offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])

    assert counts == {"international": 2}
    assert (pipeline["store_dir"] / f"{GDP}.csv").is_file()
    assert not (
        pipeline["store_dir"] / f"{HICP}.csv"
    ).exists(), "an indicator with zero rows in the working database must get no file at all"

    conn = sqlite3.connect(str(pipeline["committed_db"]))
    try:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM observations WHERE indicator_id IN (?, ?)", (GDP, HICP)
            ).fetchone()[0]
            == 0
        )
    finally:
        conn.close()


def test_assemble_then_offload_is_a_fixed_point_for_a_directory_store(pipeline):
    offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])
    first = (pipeline["store_dir"] / f"{GDP}.csv").read_bytes()

    build(
        source_db=pipeline["committed_db"],
        working_db=pipeline["working"],
        stores_path=pipeline["registry"],
    )
    offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])

    assert (pipeline["store_dir"] / f"{GDP}.csv").read_bytes() == first
    assert not (pipeline["store_dir"] / f"{HICP}.csv").exists()


def test_a_new_indicator_gets_its_own_file_on_the_next_offload(pipeline):
    conn = sqlite3.connect(pipeline["working"])
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('eurostat', 'eurostat', '2026-09-14T05:00:00+00:00', 'ok')"
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    upsert_observation(
        conn,
        indicator_id=HICP,
        geo_id=GEO,
        period="2023",
        vintage="2026-09-14T05:00:00+00:00",
        value=2.5,
        status="final",
        period_start="2023-01-01",
        period_end="2023-12-31",
        fetch_run_id=run_id,
    )
    conn.commit()
    conn.close()

    counts = offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])

    assert counts == {"international": 3}
    assert (pipeline["store_dir"] / f"{HICP}.csv").is_file()


def test_refuses_an_undeclared_indicator_in_a_directory_store(pipeline):
    conn = sqlite3.connect(pipeline["working"])
    conn.execute("""INSERT INTO indicators (indicator_id, source_id, name_nl, name_fr, name_en,
               frequency, unit, preferred_direction, aggregation_method, is_additive,
               decimals, config_path, is_active)
           VALUES ('GDP_UNDECLARED', 'eurostat', 'x', 'x', 'x', 'A', 'eur', 'neutral',
               'sum', 1, 0, 'x', 1)""")
    run_id = conn.execute("SELECT MAX(fetch_run_id) FROM fetch_runs").fetchone()[0]
    conn.execute(
        """INSERT INTO observations (indicator_id, geo_id, period, vintage, value, status,
               period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('GDP_UNDECLARED', ?, '2024', 'v', 1.0, 'final', '2024-01-01',
               '2024-12-31', 1, ?, 'v')""",
        (GEO, run_id),
    )
    conn.commit()
    conn.close()

    with pytest.raises(OffloadError, match="GDP_UNDECLARED"):
        offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])
