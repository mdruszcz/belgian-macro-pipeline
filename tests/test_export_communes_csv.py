import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from export_communes_csv import export_communes_csv  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"


def _base_db(db_path: Path) -> sqlite3.Connection:
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) VALUES (?,?,?,?,?)",
        ("statbel", "Statbel Bestat API", "Statbel", "statbel", "docs/data_catalog.md#statbel"),
    )
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('LOCAL_UNITS_BY_COMMUNE', 'statbel', 'Vestigingseenheden',
                   'Unités d''établissements', 'Local business units', 'Q', 'count',
                   'higher_is_better', 0, 'x')""")
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('statbel','statbel','2026-01-01','ok')"
    )
    return conn


def test_export_walks_the_full_hierarchy(tmp_path):
    """A regular commune resolves region, province and arrondissement."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, parent_geo_id, valid_from)
          VALUES ('be:reg:02000', 'region', 'Vlaanderen', 'Flandre', 'Flanders', 'be:country', '1830-01-01');
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, parent_geo_id, valid_from)
          VALUES ('be:prov:10000', 'province', 'Antwerpen', 'Anvers', 'Antwerp', 'be:reg:02000', '1830-01-01');
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, parent_geo_id, valid_from)
          VALUES ('be:arr:11000', 'arrondissement', 'Antwerpen', 'Anvers', 'Arrondissement Antwerpen',
                  'be:prov:10000', '1830-01-01');
        INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id, valid_from)
          VALUES ('be:mun:11002', '11002', 'municipality', 'Antwerpen', 'Anvers', 'Antwerp',
                  'be:arr:11000', '1830-01-01');
        """)
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('LOCAL_UNITS_BY_COMMUNE', 'be:mun:11002', '2023-Q4', 'v1', 66381.0, 'final',
                   '2023-10-01', '2023-12-31', 1, 1, '2026-09-05T19:00:00+00:00')""")
    conn.commit()
    conn.close()

    out_path = tmp_path / "communes_export.csv"
    n = export_communes_csv(db_path, out_path)
    assert n == 1

    lines = out_path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == (
        "geo_id,nis_code,name_en,name_fr,name_nl,region,province,arrondissement,"
        "indicator_code,indicator_name,unit,period,value,status,fetched_at"
    )
    fields = lines[1].split(",")
    assert fields[0] == "be:mun:11002"
    assert fields[5] == "Flanders"
    assert fields[6] == "Antwerp"
    assert fields[7] == "Arrondissement Antwerpen"
    assert fields[8] == "LOCAL_UNITS_BY_COMMUNE"
    assert fields[13] == "A"


def test_brussels_commune_has_no_province(tmp_path):
    """Brussels communes parent straight from arrondissement to region --
    the province field must come back empty, not raise or default to
    something invented (docs/features/geography.md Q3)."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, parent_geo_id, valid_from)
          VALUES ('be:reg:04000', 'region', 'Brussels Hoofdstedelijk Gewest',
                  'Région de Bruxelles-Capitale', 'Brussels-Capital Region', 'be:country', '1830-01-01');
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, parent_geo_id, valid_from)
          VALUES ('be:arr:21000', 'arrondissement', 'Brussel-Hoofdstad', 'Bruxelles-Capitale',
                  'Arrondissement Brussel-Hoofdstad', 'be:reg:04000', '1830-01-01');
        INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id, valid_from)
          VALUES ('be:mun:21004', '21004', 'municipality', 'Brussel', 'Bruxelles', 'Brussels',
                  'be:arr:21000', '1830-01-01');
        """)
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('LOCAL_UNITS_BY_COMMUNE', 'be:mun:21004', '2023-Q4', 'v1', 37755.0, 'final',
                   '2023-10-01', '2023-12-31', 1, 1, '2026-09-05T19:00:00+00:00')""")
    conn.commit()
    conn.close()

    out_path = tmp_path / "communes_export.csv"
    export_communes_csv(db_path, out_path)
    fields = out_path.read_text(encoding="utf-8").splitlines()[1].split(",")
    assert fields[5] == "Brussels-Capital Region"
    assert fields[6] == ""
    assert fields[7] == "Arrondissement Brussel-Hoofdstad"


def test_only_municipal_geo_ids_are_exported(tmp_path):
    """be:country observations must never leak into the commune export --
    that table already has its own file (belgian_macro_export.csv)."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.execute("""INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
           VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01')""")
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('LOCAL_UNITS_BY_COMMUNE', 'be:country', '2023-Q4', 'v1', 999.0, 'final',
                   '2023-10-01', '2023-12-31', 1, 1, '2026-09-05T19:00:00+00:00')""")
    conn.commit()
    conn.close()

    out_path = tmp_path / "communes_export.csv"
    n = export_communes_csv(db_path, out_path)
    assert n == 0


def test_only_is_latest_rows_are_exported(tmp_path):
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:mun:11002', '11002', 'municipality', 'Antwerpen', 'Anvers', 'Antwerp', '1830-01-01');
        """)
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('LOCAL_UNITS_BY_COMMUNE', 'be:mun:11002', '2023-Q3', 'v1', 111.0, 'final',
                   '2023-07-01', '2023-09-30', 0, 1, '2026-09-05T19:00:00+00:00')""")
    conn.commit()
    conn.close()

    out_path = tmp_path / "communes_export.csv"
    n = export_communes_csv(db_path, out_path)
    assert n == 0
