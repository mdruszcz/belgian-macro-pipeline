"""Tests for scripts/offload_stores.py -- the only writer of the committed
database in the daily run (docs/decisions/0006-stores-split-by-volume.md).

What must hold, each tested against a real assembled database:

  * the dump and the delete are the same row set: everything offloaded is in
    the CSV, nothing offloaded is left in the database, nothing else is touched;
  * assemble -> offload is a fixed point: with no sync in between, the CSV
    comes back byte-identical and the committed observations unchanged;
  * a sync's new vintage between the two lands in the CSV beside the old one;
  * every refusal leaves every committed file byte-identical and no scratch
    file behind -- including the refusal while a -wal or -shm file sits beside
    the committed database, which also leaves that file where it is;
  * an exception while publishing puts back every file already replaced, from
    a backup it never consumes, and removes a file that did not exist before;
  * a file that cannot be put back raises PartialPublication naming it, and
    the run's backups are kept -- also through a later retry.

Every rollback test first adds a new vintage, so the offload really changes
the CSV: build_pipeline alone is a fixed point, and a rollback on it would
prove nothing.

Marked slow for the same reason as tests/test_build_staging_db.py.
"""

import csv
import hashlib
import logging
import os
import shutil
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
from offload_stores import (  # noqa: E402
    BACKUP_DIR_NAME,
    BACKUP_MANIFEST,
    RESTORE_SUFFIX,
    OffloadError,
    PartialPublication,
    offload,
)

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


def build_pipeline(root: Path) -> dict:
    """A committed database with one national-style row that must stay, a
    committed in_db CSV, a registry naming it, and the working database
    assembled from those two -- the state the daily run is in right after
    `Assemble the working database`. tests/test_orchestration_parity.py builds
    it too, to offload the same state by both routes."""
    root.mkdir(parents=True, exist_ok=True)
    committed_db = root / "belgian_macro.db"
    migrate.run(committed_db, migrations_dir=REPO / "migrations")

    store_csv = root / "walstat_observations.csv"
    _write_csv(store_csv, COMMITTED_ROWS)
    registry = root / "stores.yaml"
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

    working = root / "local" / "working.db"
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


@pytest.fixture
def pipeline(tmp_path):
    return build_pipeline(tmp_path)


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


def _add_a_new_vintage(pipeline) -> bool:
    """What a sync does between assemble and offload: one revised 2024 value."""
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
    return changed


def _offload(pipeline):
    return offload(pipeline["working"], pipeline["committed_db"], pipeline["registry"])


def test_offload_dumps_exactly_the_store_and_strips_exactly_the_same_rows(pipeline):
    working_rows = _observations(pipeline["working"], INDICATOR)

    counts = _offload(pipeline)

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
    _offload(pipeline)
    first_csv = _sha(pipeline["csv"])
    first_rows = _observations(pipeline["committed_db"])

    build(
        source_db=pipeline["committed_db"],
        working_db=pipeline["working"],
        stores_path=pipeline["registry"],
    )
    _offload(pipeline)

    assert _sha(pipeline["csv"]) == first_csv
    assert _observations(pipeline["committed_db"]) == first_rows
    with pipeline["csv"].open(encoding="utf-8", newline="") as fh:
        assert [dict(r) for r in csv.DictReader(fh)] == COMMITTED_ROWS


def test_a_new_vintage_from_a_sync_lands_in_the_csv_beside_the_old_one(pipeline):
    assert _add_a_new_vintage(pipeline)

    _offload(pipeline)

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


def _backup_dirs(pipeline) -> list[Path]:
    root = pipeline["working"].parent / BACKUP_DIR_NAME
    return sorted(root.iterdir()) if root.is_dir() else []


def _assert_nothing_changed(pipeline, before: dict, scratch: Path) -> None:
    assert _sha(pipeline["csv"]) == before["csv"]
    assert _sha(pipeline["committed_db"]) == before["db"]
    leftovers = [p.name for p in scratch.iterdir() if p.suffix == ".tmp"]
    assert leftovers == []
    assert _backup_dirs(pipeline) == [], "this run left a backup directory behind"
    assert not list(pipeline["csv"].parent.glob("*" + RESTORE_SUFFIX))


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
        _offload(pipeline)

    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)


def test_tolerates_a_store_less_indicator_already_in_the_committed_database(pipeline):
    """The LOCAL_UNITS_BY_COMMUNE case (feat/ns1-bankruptcies): an indicator
    under an offloaded source (walstat here, statbel for the real
    LOCAL_UNITS_BY_COMMUNE) that NO store declares, but that already sits in
    the committed database untouched -- present there before this run, same
    rows, nothing new. This must NOT refuse: the indicator was never part of
    what this mechanism offloads, so its presence is not a regression the
    safety net should catch, unlike test_refuses_an_undeclared_indicator_of_an_offloaded_source's
    MUN_SOMETHING_NEW, which has no committed-database history at all."""
    import load_geography

    load_geography.load(
        pipeline["committed_db"], REPO / "config" / "geography", allow_unverified=True
    )

    conn = sqlite3.connect(pipeline["committed_db"])
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("""INSERT OR IGNORE INTO sources
               (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
           VALUES ('walstat', 'WalStat', 'IWEPS', 'walstat', 'x', 'x', 'x', 'x', 1)""")
    conn.execute("""INSERT INTO indicators (indicator_id, source_id, name_nl, name_fr, name_en,
               frequency, unit, preferred_direction, aggregation_method, is_additive,
               decimals, config_path, is_active)
           VALUES ('WALSTAT_UNSTORED', 'walstat', 'x', 'x', 'x', 'A', 'eur', 'neutral',
               'sum', 1, 0, 'x', 1)""")
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('walstat', 'walstat', '2026-09-01T05:00:00+00:00', 'ok')"
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """INSERT INTO observations (indicator_id, geo_id, period, vintage, value, status,
               period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('WALSTAT_UNSTORED', ?, '2024', 'v', 1.0, 'final', '2024-01-01',
               '2024-12-31', 1, ?, 'v')""",
        (GEO, run_id),
    )
    conn.commit()
    conn.close()

    # Rebuild the working database from this committed state (with the extra
    # indicator) plus the existing in_db CSV -- the same "assembled from what
    # is committed" state every other test in this file starts from,
    # WALSTAT_UNSTORED carried over unchanged, exactly like
    # LOCAL_UNITS_BY_COMMUNE is carried over by the real
    # scripts/build_staging_db.py.
    build(
        source_db=pipeline["committed_db"],
        working_db=pipeline["working"],
        stores_path=pipeline["registry"],
    )

    counts = _offload(pipeline)
    assert counts  # offload proceeded rather than refusing

    conn = sqlite3.connect(pipeline["committed_db"])
    try:
        still_there = conn.execute(
            "SELECT COUNT(*) FROM observations WHERE indicator_id = 'WALSTAT_UNSTORED'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert still_there == 1, "the store-less indicator must stay in the committed database"


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
        _offload(pipeline)

    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)


def test_refuses_a_missing_working_db(pipeline, tmp_path):
    before = _snapshot(pipeline)
    with pytest.raises(OffloadError, match="No working database"):
        offload(tmp_path / "nope.db", pipeline["committed_db"], pipeline["registry"])
    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)


@pytest.mark.parametrize("suffix", ["-wal", "-shm"])
def test_refuses_while_a_wal_or_shm_file_sits_beside_the_committed_database(pipeline, suffix):
    """A program holding the committed database open in WAL mode. The same
    function serves `make offload` and the Dagster asset, so both refuse; the
    command line is checked too. The file is never deleted."""
    assert _add_a_new_vintage(pipeline)
    sidecar = pipeline["committed_db"].with_name(pipeline["committed_db"].name + suffix)
    sidecar.write_bytes(b"")
    before = _snapshot(pipeline)

    with pytest.raises(OffloadError, match="close the DB viewer"):
        _offload(pipeline)
    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)
    assert sidecar.is_file() and sidecar.read_bytes() == b""

    argv = [sys.executable, "scripts/offload_stores.py", "--working-db", str(pipeline["working"])]
    argv += ["--committed-db", str(pipeline["committed_db"]), "--stores", str(pipeline["registry"])]
    cli = subprocess.run(argv, cwd=REPO, capture_output=True, text=True, encoding="utf-8")
    assert cli.returncode == 1
    assert "OFFLOAD REFUSED" in cli.stderr and "close the DB viewer" in cli.stderr
    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)
    assert sidecar.is_file()


# ── An exception while publishing ────────────────────────────────────────────


def _fail_publish(monkeypatch, *, on: int) -> list[Path]:
    """os.replace raising on the `on`-th publish -- never on a restore's own
    replace. Returns the destinations of every publish attempted."""
    real = os.replace
    attempts: list[Path] = []

    def replace(src, dst):
        if not str(src).endswith(RESTORE_SUFFIX):
            attempts.append(Path(dst))
            if len(attempts) == on:
                raise OSError(f"disk full while publishing {Path(dst).name}")
        return real(src, dst)

    monkeypatch.setattr(os, "replace", replace)
    return attempts


def _lock_restores(monkeypatch) -> None:
    """A restore's copy fails: the committed file cannot be put back."""
    real = shutil.copy2

    def copy2(src, dst, **kwargs):
        if str(dst).endswith(RESTORE_SUFFIX):
            raise PermissionError(f"{Path(dst).name} is locked by another process")
        return real(src, dst, **kwargs)

    monkeypatch.setattr(shutil, "copy2", copy2)


@pytest.mark.parametrize(
    "fail_on, replaced_first",
    [(1, None), (2, "walstat_observations.csv")],
    ids=["the first csv", "the database, after every csv was published"],
)
def test_a_failed_replace_puts_every_published_file_back(
    pipeline, monkeypatch, caplog, fail_on, replaced_first
):
    assert _add_a_new_vintage(pipeline)
    before = _snapshot(pipeline)

    with monkeypatch.context() as patch:
        attempts = _fail_publish(patch, on=fail_on)
        with caplog.at_level(logging.WARNING, logger="offload_stores"):
            with pytest.raises(OSError, match="disk full") as raised:
                _offload(pipeline)

    assert type(raised.value) is OSError, "the original exception, unchanged"
    assert [p.name for p in attempts][-1] == (
        "belgian_macro.db" if fail_on == 2 else "walstat_observations.csv"
    )
    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)
    warning = " ".join(r.getMessage() for r in caplog.records)
    assert "was put back" in warning
    assert (replaced_first or "none had been replaced yet") in warning

    # Nothing was left behind that stops the next run, and that run does
    # change the files: the rollback above undid a real change.
    assert _offload(pipeline) == {"walstat": 4}
    assert _sha(pipeline["csv"]) != before["csv"]
    assert _backup_dirs(pipeline) == []


def test_a_restore_copies_beside_the_file_and_never_consumes_the_backup(pipeline, monkeypatch):
    assert _add_a_new_vintage(pipeline)
    before = _snapshot(pipeline)
    real = os.replace
    publishes: list[Path] = []
    during_restore: list[tuple[Path, Path, list[str]]] = []

    def replace(src, dst):
        src, dst = Path(src), Path(dst)
        if src.name.endswith(RESTORE_SUFFIX):
            (backup_dir,) = _backup_dirs(pipeline)
            during_restore.append((src, dst, sorted(p.name for p in backup_dir.iterdir())))
        else:
            publishes.append(dst)
            if len(publishes) == 2:
                raise OSError("disk full while publishing the database")
        return real(src, dst)

    monkeypatch.setattr(os, "replace", replace)
    with pytest.raises(OSError, match="disk full"):
        _offload(pipeline)
    monkeypatch.undo()

    ((src, dst, backups),) = during_restore
    assert dst.resolve() == pipeline["csv"].resolve()
    assert src.parent == dst.parent, "the restore copy sits beside the committed file"
    assert BACKUP_MANIFEST in backups and any(
        n.endswith("walstat_observations.csv") for n in backups
    )
    _assert_nothing_changed(pipeline, before, pipeline["working"].parent)


def test_an_interrupted_restore_never_truncates_the_file_and_keeps_the_backup(
    pipeline, monkeypatch
):
    assert _add_a_new_vintage(pipeline)
    committed_csv = pipeline["csv"].read_bytes()
    real = shutil.copy2

    def copy2(src, dst, **kwargs):
        if str(dst).endswith(RESTORE_SUFFIX):
            Path(dst).write_bytes(Path(src).read_bytes()[:20])
            raise OSError("the disk filled up halfway through the copy")
        return real(src, dst, **kwargs)

    with monkeypatch.context() as patch:
        _fail_publish(patch, on=2)
        patch.setattr(shutil, "copy2", copy2)
        with pytest.raises(PartialPublication) as raised:
            _offload(pipeline)

    new_csv = pipeline["csv"].read_bytes()
    assert new_csv != committed_csv and len(new_csv) > len(committed_csv)
    with pipeline["csv"].open(encoding="utf-8", newline="") as fh:
        assert len(list(csv.DictReader(fh))) == 4, "this run's whole file, not a truncated one"
    assert not list(pipeline["csv"].parent.glob("*" + RESTORE_SUFFIX))
    (backup_dir,) = _backup_dirs(pipeline)
    assert raised.value.backup_dir == backup_dir
    (csv_backup,) = backup_dir.glob("*_walstat_observations.csv")
    assert csv_backup.read_bytes() == committed_csv


def test_a_file_that_cannot_be_put_back_raises_partial_publication_and_keeps_the_backups(
    pipeline, monkeypatch
):
    assert _add_a_new_vintage(pipeline)
    before = _snapshot(pipeline)
    committed_csv = pipeline["csv"].read_bytes()

    with monkeypatch.context() as patch:
        _fail_publish(patch, on=2)
        _lock_restores(patch)
        with pytest.raises(PartialPublication) as raised:
            _offload(pipeline)

    error = raised.value
    assert type(error.__cause__) is OSError and "disk full" in str(error.__cause__)
    assert [p.resolve() for p in error.left_new] == [pipeline["csv"].resolve()]
    assert error.restored == ()
    assert _sha(pipeline["csv"]) != before["csv"], "still this run's version"
    assert _sha(pipeline["committed_db"]) == before["db"], "the database was never replaced"

    (backup_dir,) = _backup_dirs(pipeline)
    assert error.backup_dir == backup_dir
    assert str(backup_dir) in str(error) and str(error.left_new[0]) in str(error)
    manifest = (backup_dir / BACKUP_MANIFEST).read_text(encoding="utf-8").splitlines()
    entries = dict(reversed(line.split("\t")) for line in manifest if not line.startswith("#"))
    csv_backup = backup_dir / entries[str(error.left_new[0])]
    assert csv_backup.read_bytes() == committed_csv
    assert set(entries) == {str(error.left_new[0]), str(pipeline["committed_db"])}


def test_a_retry_keeps_the_backups_of_a_run_that_could_not_be_put_back(pipeline, monkeypatch):
    assert _add_a_new_vintage(pipeline)
    with monkeypatch.context() as patch:
        _fail_publish(patch, on=2)
        _lock_restores(patch)
        with pytest.raises(PartialPublication):
            _offload(pipeline)
    (earlier,) = _backup_dirs(pipeline)
    kept = {p.name: p.read_bytes() for p in earlier.iterdir()}

    assert _offload(pipeline) == {"walstat": 4}

    assert _backup_dirs(pipeline) == [earlier], "the retry removed only its own backups"
    assert {p.name: p.read_bytes() for p in earlier.iterdir()} == kept


def test_a_successful_offload_leaves_no_backup_of_its_own(pipeline):
    assert _add_a_new_vintage(pipeline)
    _offload(pipeline)
    assert _backup_dirs(pipeline) == []
    assert not list(pipeline["csv"].parent.glob("*" + RESTORE_SUFFIX))


def test_a_rollback_removes_an_indicator_file_that_did_not_exist_before(pipeline, monkeypatch):
    """A directory store has one file per indicator, and an indicator's first
    rows create its file. That file has no previous version: putting it back
    means removing it, while a file that did exist gets its bytes back."""
    store_dir = pipeline["registry"].parent / "walstat_dir"
    store_dir.mkdir()
    existing = store_dir / f"{INDICATOR}.csv"
    _write_csv(existing, COMMITTED_ROWS)
    created = store_dir / f"{OTHER_WALSTAT}.csv"
    registry = yaml.safe_load(pipeline["registry"].read_text(encoding="utf-8"))
    registry["stores"]["walstat"].update(path=str(store_dir), layout="one_csv_per_indicator")
    pipeline["registry"].write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    build(
        source_db=pipeline["committed_db"],
        working_db=pipeline["working"],
        stores_path=pipeline["registry"],
    )
    conn = sqlite3.connect(pipeline["working"])
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('walstat', 'walstat', '2026-09-14T05:00:00+00:00', 'ok')"
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    assert upsert_observation(
        conn,
        indicator_id=OTHER_WALSTAT,
        geo_id=GEO,
        period="2024",
        vintage="2026-09-14T05:00:00+00:00",
        value=1200.0,
        status="final",
        period_start="2024-01-01",
        period_end="2024-12-31",
        fetch_run_id=run_id,
    )
    conn.commit()
    conn.close()
    existing_before = _sha(existing)
    db_before = _sha(pipeline["committed_db"])
    assert not created.exists()

    with monkeypatch.context() as patch:
        attempts = _fail_publish(patch, on=3)
        with pytest.raises(OSError, match="disk full"):
            _offload(pipeline)

    assert [p.name for p in attempts] == [existing.name, created.name, "belgian_macro.db"]
    assert not created.exists(), "a file this run created is removed again"
    assert _sha(existing) == existing_before
    assert _sha(pipeline["committed_db"]) == db_before
    assert _backup_dirs(pipeline) == []
    assert not list(store_dir.glob("*" + RESTORE_SUFFIX))
