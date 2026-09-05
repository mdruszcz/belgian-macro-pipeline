"""Tests for the committed-CSV store used by manual-only sources.

The population data can never be fetched in CI (statbel.fgov.be is
unreachable from there), so it lives as a committed CSV rather than as rows
in the daily-committed database -- see
docs/decisions/0002-split-committed-stores.md. What must hold:

  - the CSV round-trips losslessly, so nothing is quietly dropped by moving
    the store out of SQLite;
  - the commune export produces the same rows whether an indicator came from
    the database or from the CSV.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from export_communes_csv import export_communes_csv  # noqa: E402
from export_observations_csv import export_observations  # noqa: E402
from load_observations_csv import ObservationsCsvError, load  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
POP = "POPULATION_BY_COMMUNE"


def _db_with_population(db_path: Path, rows: list[tuple[str, str, float]]) -> None:
    """rows: (geo_id, period, value)."""
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) VALUES (?,?,?,?,?)",
        ("statbel", "Statbel Bestat API", "Statbel", "statbel", "docs/data_catalog.md#statbel"),
    )
    conn.execute(
        """INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES (?, 'statbel', 'Bevolking', 'Population', 'Population', 'A', 'count',
                   'contextual', 1, 'x')""",
        (POP,),
    )
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('statbel','statbel','2026-01-01','ok')"
    )
    conn.execute("""INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
           VALUES ('be:country','country','België','Belgique','Belgium','1830-01-01')""")
    for geo_id, period, value in rows:
        conn.execute(
            """INSERT OR IGNORE INTO geographies
               (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from)
               VALUES (?, ?, 'municipality', 'X', 'X', 'X', '1830-01-01')""",
            (geo_id, geo_id.rsplit(":", 1)[-1]),
        )
        conn.execute(
            """INSERT INTO observations
               (indicator_id, geo_id, period, vintage, value, status,
                period_start, period_end, is_latest, fetch_run_id, created_at)
               VALUES (?, ?, ?, 'v1', ?, 'final', ? || '-01-01', ? || '-12-31', 1, 1,
                       '2026-09-06T00:00:00+00:00')""",
            (POP, geo_id, period, value, period, period),
        )
    conn.commit()
    conn.close()


def test_export_is_sorted_and_stable(tmp_path):
    """Byte-stability is the whole point: two exports of unchanged data must
    produce an identical file, or every run would show a spurious git diff."""
    db = tmp_path / "a.db"
    _db_with_population(db, [("be:mun:21004", "2026", 5.0), ("be:mun:11002", "2016", 1.0)])

    first, second = tmp_path / "1.csv", tmp_path / "2.csv"
    export_observations(db, first, [POP])
    export_observations(db, second, [POP])
    assert first.read_bytes() == second.read_bytes()

    lines = first.read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("indicator_id,geo_id,period,vintage,value,status")
    # sorted by (indicator_id, geo_id, period, vintage) -- the observations PK
    assert "be:mun:11002" in lines[1] and "be:mun:21004" in lines[2]


def test_roundtrip_is_lossless(tmp_path):
    db = tmp_path / "a.db"
    _db_with_population(
        db, [("be:mun:11002", "2016", 517042.0), ("be:mun:11002", "2026", 565615.0)]
    )
    original = tmp_path / "orig.csv"
    export_observations(db, original, [POP])

    rebuilt_db = tmp_path / "rebuilt.db"
    n = load(rebuilt_db, original)
    assert n == 2

    again = tmp_path / "again.csv"
    export_observations(rebuilt_db, again, [POP])
    assert again.read_bytes() == original.read_bytes()

    conn = sqlite3.connect(str(rebuilt_db))
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_loader_refuses_an_unknown_indicator(tmp_path):
    """An indicator with no config would need invented metadata -- refuse
    rather than guess (CLAUDE.md rule 13)."""
    csv_path = tmp_path / "bad.csv"
    csv_path.write_text(
        "indicator_id,geo_id,period,vintage,value,status,period_start,period_end,"
        "is_latest,created_at\n"
        "NOT_A_REAL_INDICATOR,be:mun:11002,2026,v1,1.0,final,2026-01-01,2026-12-31,1,x\n",
        encoding="utf-8",
    )
    with pytest.raises(ObservationsCsvError, match="no config/indicators"):
        load(tmp_path / "out.db", csv_path)


def test_loader_refuses_a_missing_file(tmp_path):
    with pytest.raises(ObservationsCsvError, match="No observations CSV"):
        load(tmp_path / "out.db", tmp_path / "nope.csv")


def test_commune_export_matches_whether_source_is_db_or_csv(tmp_path):
    """The decisive property: moving an indicator out of the database and
    into the committed CSV must not change a single byte of the published
    commune export."""
    db = tmp_path / "a.db"
    rows = [("be:mun:11002", "2016", 517042.0), ("be:mun:11002", "2026", 565615.0)]
    _db_with_population(db, rows)

    from_db = tmp_path / "from_db.csv"
    export_communes_csv(db, from_db)

    # Now move population out of the DB and into a CSV, as the split does.
    store = tmp_path / "population_observations.csv"
    export_observations(db, store, [POP])
    conn = sqlite3.connect(str(db))
    conn.execute("DELETE FROM observations WHERE indicator_id = ?", (POP,))
    conn.commit()
    conn.close()

    from_csv = tmp_path / "from_csv.csv"
    export_communes_csv(db, from_csv, extra_observations=(store,))

    assert from_csv.read_bytes() == from_db.read_bytes()


def test_extra_observations_file_must_exist(tmp_path):
    """A typo'd path must not silently yield a commune export missing that
    source entirely."""
    db = tmp_path / "a.db"
    _db_with_population(db, [("be:mun:11002", "2026", 1.0)])
    with pytest.raises(FileNotFoundError, match="does not exist"):
        export_communes_csv(db, tmp_path / "out.csv", extra_observations=(tmp_path / "no.csv",))
