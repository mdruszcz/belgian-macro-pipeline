"""Tests for scripts/offload_stores.py -- the only writer of the committed
database in the daily run (docs/decisions/0006-stores-split-by-volume.md).

What must hold, each tested against a real assembled database:

  * the dump and the delete are the same row set: everything offloaded is in
    the CSV, nothing offloaded is left in the database, nothing else is touched;
  * assemble -> offload is a fixed point: with no sync in between, the CSV
    comes back byte-identical and the committed observations unchanged;
  * a sync's new vintage between the two lands in the CSV beside the old one;
  * every refusal leaves every committed file byte-identical and no scratch
    file behind.

Marked slow for the same reason as tests/test_build_staging_db.py.
"""

import csv
import hashlib
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

INDICATOR = "MUN_DEBT_TOTAL_PER_CAPITA"
OTHER_WALSTAT = "MUN_DEBT_LONG_TERM_PER_CAPITA"
GEO = "be:mun:92094"  # Namur


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


COMMITTED_ROWS = [
    _row("2023", "2026-01-01T00:00:00+00:00", "3077.5", "0"),
    _row("2023", "2026-02-01T00:00:00+00:00", "2965.5", "1"),
    _row("2024", "2026-02-01T00:00:00+00:00", "3154.5", "1"),
]


def _write_csv(path: Path, rows) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def pipeline(tmp_path):
    """A committed database with one national-style row that must stay, a
    committed in_db CSV, a registry naming it, and the working database
    assembled from those two -- the state the daily run is in right after
    `Assemble the working database`."""
    committed_db = tmp_path / "belgian_macro.db"
    migrate.run(committed_db, migrations_dir=REPO / "migrations")

    store_csv = tmp_path / "walstat_observations.csv"
    _write_csv(store_csv, COMMITTED_ROWS)
    registry = tmp_path / "stores.yaml"
    registry.write_text(
        yaml.safe_dump(
            {
                "stores": {
                    "walstat": {
                        "path": str(store_csv),
                        "source_id": "walstat",
                        "mode": "in_db",
                        "indicators": [INDICATOR, OTHER_WALSTAT],
                        "reference_rows": {
                            "script": "scripts/sync_walstat.py",
                            "args": ["--reference-rows-only"],
                        },
                    },
                    "population": {
                        "path": "data/population_observations.csv",
                        "source_id": "statbel",
                        "mode": "extra_csv",
                        "indicators": [
                            "POPULATION_AGE_0_14",
                            "POPULATION_AGE_15_64",
                            "POPULATION_AGE_65_PLUS",
                            "POPULATION_BY_COMMUNE",
                        ],
                        "reference_rows": {
                            "script": "scripts/sync_population.py",
                            "args": ["--reference-rows-only"],
                        },
                    },
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    working = tmp_path / "local" / "working.db"
    build(source_db=committed_db, working_db=working, stores_path=registry)

    # A row that belongs in the committed database: a real fetch run, a
    # non-offloaded indicator.
    conn = sqlite3.connect(working)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('statbel', 'statbel', '2026-09-13T05:00:00+00:00', 'ok')"
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    upsert_observation(
        conn,
        indicator_id="POPULATION_BY_COMMUNE",
        geo_id=GEO,
        period="2026",
        vintage="2026-09-13T05:00:00+00:00",
        value=115000.0,
        status="final",
        period_start="2026-01-01",
        period_end="2026-12-31",
        fetch_run_id=run_id,
    )
    conn.commit()
    conn.close()

    return {
        "committed_db": committed_db,
        "csv": store_csv,
        "registry": registry,
        "working": working,
    }


def _observations(db: Path, indicator: str | None = None) -> list[tuple]:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        sql = f"SELECT {', '.join(COLUMNS)} FROM observations"
        args: tuple = ()
        if indicator:
            sql += " WHERE indicator_id = ?"
            args = (indicator,)
        return sorted(conn.execute(sql + " ORDER BY 1, 2, 3, 4", args).fetchall())
    finally:
        conn.close()


def test_offload_dumps_exactly_the_store_and_strips_exactly_the_same_rows(pipeline):
    working_rows = _observations(pipeline["working"], INDICATOR)

    counts = offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])

    assert counts == {"walstat": 3}
    with pipeline["csv"].open(encoding="utf-8", newline="") as fh:
        dumped = [tuple(r.values()) for r in csv.DictReader(fh)]
    assert len(dumped) == len(working_rows) == 3

    committed = pipeline["committed_db"]
    assert _observations(committed, INDICATOR) == []
    remaining = _observations(committed)
    assert [r[0] for r in remaining] == ["POPULATION_BY_COMMUNE"]

    conn = sqlite3.connect(committed)
    try:
        assert (
            conn.execute("SELECT COUNT(*) FROM fetch_runs WHERE adapter = 'rebuild'").fetchone()[0]
            == 0
        )
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        # Reference rows stay: the exporters read name and unit from here.
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM indicators WHERE indicator_id = ?", (INDICATOR,)
            ).fetchone()[0]
            == 1
        )
    finally:
        conn.close()


def test_assemble_then_offload_is_a_fixed_point(pipeline, tmp_path):
    """No sync in between: the CSV is byte-identical and the committed
    observations unchanged, on the first round and on the second."""
    offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])
    first_csv = _sha(pipeline["csv"])
    first_rows = _observations(pipeline["committed_db"])

    build(
        source_db=pipeline["committed_db"],
        working_db=pipeline["working"],
        stores_path=pipeline["registry"],
    )
    offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])

    assert _sha(pipeline["csv"]) == first_csv
    assert _observations(pipeline["committed_db"]) == first_rows
    with pipeline["csv"].open(encoding="utf-8", newline="") as fh:
        assert [dict(r) for r in csv.DictReader(fh)] == COMMITTED_ROWS


def test_a_new_vintage_from_a_sync_lands_in_the_csv_beside_the_old_one(pipeline):
    conn = sqlite3.connect(pipeline["working"])
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('walstat', 'walstat', '2026-09-14T05:00:00+00:00', 'ok')"
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    changed = upsert_observation(
        conn,
        indicator_id=INDICATOR,
        geo_id=GEO,
        period="2024",
        vintage="2026-09-14T05:00:00+00:00",
        value=3160.0,
        status="revised",
        period_start="2024-01-01",
        period_end="2024-12-31",
        fetch_run_id=run_id,
    )
    conn.commit()
    conn.close()
    assert changed

    offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])

    with pipeline["csv"].open(encoding="utf-8", newline="") as fh:
        rows_2024 = [
            (r["vintage"], r["value"], r["is_latest"])
            for r in csv.DictReader(fh)
            if r["period"] == "2024"
        ]
    assert rows_2024 == [
        ("2026-02-01T00:00:00+00:00", "3154.5", "0"),
        ("2026-09-14T05:00:00+00:00", "3160.0", "1"),
    ]
    # The real sync's run stays in the committed audit trail even though its
    # observation moved to the CSV; only rebuild runs are dropped.
    conn = sqlite3.connect(pipeline["committed_db"])
    try:
        assert (
            conn.execute("SELECT COUNT(*) FROM fetch_runs WHERE adapter = 'walstat'").fetchone()[0]
            == 1
        )
    finally:
        conn.close()


def _assert_nothing_changed(pipeline, before: dict, scratch: Path) -> None:
    assert _sha(pipeline["csv"]) == before["csv"]
    assert _sha(pipeline["committed_db"]) == before["db"]
    leftovers = [p.name for p in scratch.iterdir() if p.suffix == ".tmp"]
    assert leftovers == []


def _snapshot(pipeline) -> dict:
    return {"csv": _sha(pipeline["csv"]), "db": _sha(pipeline["committed_db"])}


def test_refuses_an_undeclared_indicator_of_an_offloaded_source(pipeline):
    """A configured WalStat indicator nobody registered would otherwise stay
    in the committed database and quietly re-grow it."""
    conn = sqlite3.connect(pipeline["working"])
    run_id = conn.execute("SELECT MAX(fetch_run_id) FROM fetch_runs").fetchone()[0]
    conn.execute("""INSERT INTO indicators (indicator_id, source_id, name_nl, name_fr, name_en,
               frequency, unit, preferred_direction, aggregation_method, is_additive,
               decimals, config_path, is_active)
           VALUES ('MUN_SOMETHING_NEW', 'walstat', 'x', 'x', 'x', 'A', 'eur', 'neutral',
               'sum', 1, 0, 'x', 1)""")
    conn.execute(
        """INSERT INTO observations (indicator_id, geo_id, period, vintage, value, status,
               period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('MUN_SOMETHING_NEW', ?, '2024', 'v', 1.0, 'final', '2024-01-01',
               '2024-12-31', 1, ?, 'v')""",
        (GEO, run_id),
    )
    conn.commit()
    conn.close()
    before = _snapshot(pipeline)

    with pytest.raises(OffloadError, match="MUN_SOMETHING_NEW"):
        offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])

    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)


def test_refuses_a_working_db_that_lost_committed_rows(pipeline):
    """The catastrophic case: a working database not assembled from the
    committed CSVs. Offloading it would overwrite the history with one day."""
    conn = sqlite3.connect(pipeline["working"])
    conn.execute(
        "DELETE FROM observations WHERE indicator_id = ? AND period = '2023'", (INDICATOR,)
    )
    conn.commit()
    conn.close()
    before = _snapshot(pipeline)

    with pytest.raises(OffloadError, match="absent from the working database"):
        offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])

    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)


def test_refuses_a_missing_working_db(pipeline, tmp_path):
    before = _snapshot(pipeline)
    with pytest.raises(OffloadError, match="No working database"):
        offload(tmp_path / "nope.db", pipeline["committed_db"], pipeline["registry"])
    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)
