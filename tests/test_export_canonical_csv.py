import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from export_canonical_csv import export_canonical_csv, status_to_obs_status  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


@pytest.mark.parametrize(
    "status,expected",
    [
        ("final", "A"),
        ("provisional", "P"),
        ("estimate", ""),
        ("revised", ""),
        ("suppressed", ""),
        ("na", ""),
    ],
)
def test_status_to_obs_status(status, expected):
    assert status_to_obs_status(status) == expected


def test_export_produces_expected_header_and_row(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) VALUES (?,?,?,?,?)",
        ("nbb", "NBB SDMX", "NBB", "nbb", "docs/data_catalog.md#nbb"),
    )
    conn.execute("""INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
           VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01')""")
    conn.execute(
        """INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('GDP', 'nbb', 'BBP', 'PIB', 'GDP', 'Q', 'percent_yy', 'higher_is_better', 0, 'x')"""
    )
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES ('nbb','nbb','2026-01-01','ok')"
    )
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('GDP', 'be:country', '2024-Q1', 'v1', 1.5, 'final',
                   '2024-01-01', '2024-03-31', 1, 1, '2026-01-01T00:00:00+00:00')""")
    conn.commit()
    conn.close()

    out_path = tmp_path / "export.csv"
    n = export_canonical_csv(db_path, out_path)
    assert n == 1

    lines = out_path.read_text().splitlines()
    assert lines[0] == "indicator_code,name,period,value,obs_status,unit,source_agency,fetched_at"
    assert lines[1] == "GDP,GDP,2024-Q1,1.5,A,percent_yy,NBB,2026-01-01T00:00:00+00:00"


def test_export_excludes_non_latest_rows(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) VALUES (?,?,?,?,?)",
        ("nbb", "NBB", "NBB", "nbb", "x"),
    )
    conn.execute("""INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
           VALUES ('be:country', 'country', 'a', 'b', 'c', '1830-01-01')""")
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('GDP', 'nbb', 'a', 'b', 'c', 'Q', 'percent_yy', 'higher_is_better', 0, 'x')""")
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES ('nbb','nbb','x','ok')"
    )
    conn.executemany(
        """INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('GDP', 'be:country', '2024-Q1', ?, ?, 'final',
                   '2024-01-01', '2024-03-31', ?, 1, 'x')""",
        [("v1", 1.0, 0), ("v2", 2.0, 1)],
    )
    conn.commit()
    conn.close()

    out_path = tmp_path / "export.csv"
    export_canonical_csv(db_path, out_path)
    lines = out_path.read_text().splitlines()
    assert len(lines) == 2  # header + exactly one row (the is_latest=1 one)
    assert "2.0" in lines[1]
