import sqlite3
import sys
from pathlib import Path

import pytest

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


def test_historical_predecessor_geo_id_is_excluded(tmp_path):
    """A pre-merger year's population resolves to the historical
    predecessor's own geo_id (resolve_geo is period-aware) -- that entity no
    longer exists today and must not surface as a phantom extra commune."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to)
          VALUES ('be:mun:12030', '12030', 'municipality', 'Puurs', 'Puurs', 'Puurs',
                  '1977-01-01', '2019-01-01');
        """)
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('POPULATION_BY_COMMUNE', 'be:mun:12030', '2018', 'v1', 17000.0, 'final',
                   '2018-01-01', '2018-12-31', 1, 1, '2026-09-06T00:00:00+00:00')""")
    conn.commit()
    conn.close()

    out_path = tmp_path / "communes_export.csv"
    n = export_communes_csv(db_path, out_path)
    assert n == 0


def test_only_the_most_recent_period_is_exported_per_indicator(tmp_path):
    """is_latest marks 'not superseded by a revision', not 'the newest
    period' -- an indicator with real multi-year history has many
    is_latest=1 rows simultaneously. Only the latest period shows here."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:mun:11002', '11002', 'municipality', 'Antwerpen', 'Anvers', 'Antwerp',
                  '1830-01-01');
        """)
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('POPULATION_BY_COMMUNE', 'statbel', 'Bevolking', 'Population', 'Population',
                   'A', 'count', 'contextual', 1, 'x')""")
    for period, value in [("2016", 517042.0), ("2025", 562002.0), ("2026", 565615.0)]:
        conn.execute(
            """INSERT INTO observations
               (indicator_id, geo_id, period, vintage, value, status,
                period_start, period_end, is_latest, fetch_run_id, created_at)
               VALUES ('POPULATION_BY_COMMUNE', 'be:mun:11002', ?, 'v1', ?, 'final',
                       ? || '-01-01', ? || '-12-31', 1, 1, '2026-09-06T00:00:00+00:00')""",
            (period, value, period, period),
        )
    conn.commit()
    conn.close()

    out_path = tmp_path / "communes_export.csv"
    n = export_communes_csv(db_path, out_path)
    assert n == 1

    fields = out_path.read_text(encoding="utf-8").splitlines()[1].split(",")
    assert fields[11] == "2026"
    assert fields[12] == "565615.0"


# ── Merger back-aggregation via an --extra-observations CSV ─────────────────


def _write_extra_csv(path, rows):
    header = [
        "indicator_id",
        "geo_id",
        "period",
        "vintage",
        "value",
        "status",
        "period_start",
        "period_end",
        "is_latest",
        "created_at",
    ]
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(str(row.get(h, "")) for h in header))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_extra_csv_reconstruction_gap_fills_the_successors_latest_snapshot(tmp_path):
    """FISCAL_TOT_NET_TAXABLE_INC lives only in the extra CSV, exactly like
    the real fiscal_income_observations.csv store. The successor has no row
    of its own for it, so it must be reconstructed from its two predecessors'
    own latest rows -- summed, never averaged."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('FISCAL_TOT_NET_TAXABLE_INC', 'statbel', 'x', 'x',
                   'Total net taxable income', 'A', 'eur', 'higher_is_better', 1, 'x')""")
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:mun:900', '900', 'municipality', 'Succ', 'Succ', 'Successor', '1830-01-01');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to, successor_geo_id)
          VALUES ('be:mun:901', '901', 'municipality', 'A', 'A', 'Predecessor A',
                  '1830-01-01', '2025-01-01', 'be:mun:900');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to, successor_geo_id)
          VALUES ('be:mun:902', '902', 'municipality', 'B', 'B', 'Predecessor B',
                  '1830-01-01', '2025-01-01', 'be:mun:900');
        """)
    conn.commit()
    conn.close()

    extra_csv = tmp_path / "fiscal.csv"
    _write_extra_csv(
        extra_csv,
        [
            {
                "indicator_id": "FISCAL_TOT_NET_TAXABLE_INC",
                "geo_id": "be:mun:901",
                "period": "2023",
                "vintage": "v1",
                "value": "280373890.32",
                "status": "final",
                "period_start": "2023-01-01",
                "period_end": "2023-12-31",
                "is_latest": "1",
                "created_at": "2026-09-06T11:24:33+00:00",
            },
            {
                "indicator_id": "FISCAL_TOT_NET_TAXABLE_INC",
                "geo_id": "be:mun:902",
                "period": "2023",
                "vintage": "v1",
                "value": "759840493.45",
                "status": "final",
                "period_start": "2023-01-01",
                "period_end": "2023-12-31",
                "is_latest": "1",
                "created_at": "2026-09-06T11:24:33+00:00",
            },
        ],
    )

    out_path = tmp_path / "communes_export.csv"
    export_communes_csv(db_path, out_path, extra_observations=(extra_csv,))
    rows = out_path.read_text(encoding="utf-8").splitlines()[1:]
    succ_rows = [r.split(",") for r in rows if r.split(",")[0] == "be:mun:900"]
    assert len(succ_rows) == 1
    assert float(succ_rows[0][12]) == pytest.approx(1040214383.77, abs=0.01)
    assert succ_rows[0][13] == "reconstructed"
    # A predecessor never gets its own row in this current-communes-only export.
    assert not any(r.split(",")[0] in ("be:mun:901", "be:mun:902") for r in rows)


def test_extra_csv_police_rates_are_never_reconstructed(tmp_path):
    """The four police per-10k rates are is_additive=0 and must never appear
    for a successor via reconstruction, even when both predecessors report
    one."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('CAR_THEFT_PER_10K', 'police', 'x', 'x', 'Car theft per 10k',
                   'A', 'per_10k', 'lower_is_better', 0, 'x')""")
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:mun:900', '900', 'municipality', 'Succ', 'Succ', 'Successor', '1830-01-01');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to, successor_geo_id)
          VALUES ('be:mun:901', '901', 'municipality', 'A', 'A', 'Predecessor A',
                  '1830-01-01', '2025-01-01', 'be:mun:900');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to, successor_geo_id)
          VALUES ('be:mun:902', '902', 'municipality', 'B', 'B', 'Predecessor B',
                  '1830-01-01', '2025-01-01', 'be:mun:900');
        """)
    conn.commit()
    conn.close()

    extra_csv = tmp_path / "police.csv"
    _write_extra_csv(
        extra_csv,
        [
            {
                "indicator_id": "CAR_THEFT_PER_10K",
                "geo_id": "be:mun:901",
                "period": "2023",
                "vintage": "v1",
                "value": "10.0",
                "status": "final",
                "period_start": "2023-01-01",
                "period_end": "2023-12-31",
                "is_latest": "1",
                "created_at": "2026-09-06T21:55:49+00:00",
            },
            {
                "indicator_id": "CAR_THEFT_PER_10K",
                "geo_id": "be:mun:902",
                "period": "2023",
                "vintage": "v1",
                "value": "20.0",
                "status": "final",
                "period_start": "2023-01-01",
                "period_end": "2023-12-31",
                "is_latest": "1",
                "created_at": "2026-09-06T21:55:49+00:00",
            },
        ],
    )

    out_path = tmp_path / "communes_export.csv"
    export_communes_csv(db_path, out_path, extra_observations=(extra_csv,))
    rows = out_path.read_text(encoding="utf-8").splitlines()[1:]
    assert not any(r.split(",")[0] == "be:mun:900" for r in rows)


# ── Latest-vs-reconstructed selection (fix/map-latest-row-per-commune) ──────
#
# The bug: export_communes_csv.py fed reconstruct() only each geo_id's
# single latest-period row (_latest_rows_from_any_csv), so an ABSORBING
# successor that keeps its OWN pre-merger geo_id -- Antwerp's case, geo_id
# be:mun:11002 unchanged since 1977 -- had its own older real row hidden
# behind reconstruct()'s existing-cells check, which only ever saw that one
# latest row. The gap-fill logic then treated a period the successor
# genuinely already had as a gap and reconstructed it from the predecessor
# alone. The fix feeds reconstruct() every is_latest=1 row (all periods),
# then re-collapses to one row per (geo_id, indicator) with a real row
# always beating a reconstructed one at the same or an older period.
#
# All geo_ids/values below are synthetic, hand-computed for this test only.


def test_absorbing_successor_keeps_its_own_real_latest_row(tmp_path):
    """Antwerp shape: a successor that kept ITS OWN geo_id, with real rows at
    three periods, plus a predecessor's row at the middle period. The
    successor's own newest real row must win -- not a reconstruction built
    from the predecessor because only the successor's single latest row was
    visible to the gap-fill check."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('POP_X', 'statbel', 'x', 'x', 'Population X', 'A', 'count',
                   'contextual', 1, 'x')""")
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:mun:800', '800', 'municipality', 'Absorber', 'Absorber', 'Absorber',
                  '1830-01-01');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to, successor_geo_id)
          VALUES ('be:mun:801', '801', 'municipality', 'Absorbed', 'Absorbed', 'Absorbed',
                  '1830-01-01', '2025-01-01', 'be:mun:800');
        """)
    conn.commit()
    conn.close()

    extra_csv = tmp_path / "pop.csv"
    _write_extra_csv(
        extra_csv,
        [
            # Absorber (successor, own geo_id): real rows at three periods.
            {
                "indicator_id": "POP_X",
                "geo_id": "be:mun:800",
                "period": "2024",
                "vintage": "v1",
                "value": "544759.0",
                "status": "final",
                "period_start": "2024-01-01",
                "period_end": "2024-12-31",
                "is_latest": "1",
                "created_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "indicator_id": "POP_X",
                "geo_id": "be:mun:800",
                "period": "2025",
                "vintage": "v1",
                "value": "562002.0",
                "status": "final",
                "period_start": "2025-01-01",
                "period_end": "2025-12-31",
                "is_latest": "1",
                "created_at": "2026-06-01T00:00:00+00:00",
            },
            {
                "indicator_id": "POP_X",
                "geo_id": "be:mun:800",
                "period": "2026",
                "vintage": "v1",
                "value": "565615.0",
                "status": "final",
                "period_start": "2026-01-01",
                "period_end": "2026-12-31",
                "is_latest": "1",
                "created_at": "2026-09-01T00:00:00+00:00",
            },
            # Absorbed (predecessor): its own last snapshot, 2024, small.
            {
                "indicator_id": "POP_X",
                "geo_id": "be:mun:801",
                "period": "2024",
                "vintage": "v1",
                "value": "11379.0",
                "status": "final",
                "period_start": "2024-01-01",
                "period_end": "2024-12-31",
                "is_latest": "1",
                "created_at": "2026-01-01T00:00:00+00:00",
            },
        ],
    )

    out_path = tmp_path / "communes_export.csv"
    export_communes_csv(db_path, out_path, extra_observations=(extra_csv,))
    rows = [r.split(",") for r in out_path.read_text(encoding="utf-8").splitlines()[1:]]
    succ_rows = [r for r in rows if r[0] == "be:mun:800"]
    assert len(succ_rows) == 1
    assert succ_rows[0][11] == "2026"
    assert float(succ_rows[0][12]) == pytest.approx(565615.0)
    assert succ_rows[0][13] == "A"  # final, not reconstructed
    # The predecessor never gets its own row in this current-communes-only export.
    assert not any(r[0] == "be:mun:801" for r in rows)


def test_new_code_successor_with_own_recent_history_is_not_reconstructed(tmp_path):
    """Deinze shape: a successor created with a NEW geo_id, predecessors
    ending in an earlier period, own real rows spanning several later
    periods. The newest own row must be published, never a reconstruction
    from the predecessors' final snapshot."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('POP_X', 'statbel', 'x', 'x', 'Population X', 'A', 'count',
                   'contextual', 1, 'x')""")
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:mun:810', '810', 'municipality', 'NewCode', 'NewCode', 'NewCode',
                  '2019-01-01');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to, successor_geo_id)
          VALUES ('be:mun:811', '811', 'municipality', 'OldA', 'OldA', 'OldA',
                  '1830-01-01', '2019-01-01', 'be:mun:810');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to, successor_geo_id)
          VALUES ('be:mun:812', '812', 'municipality', 'OldB', 'OldB', 'OldB',
                  '1830-01-01', '2019-01-01', 'be:mun:810');
        """)
    conn.commit()
    conn.close()

    extra_csv = tmp_path / "pop.csv"
    _write_extra_csv(
        extra_csv,
        [
            # Predecessors' last snapshot, 2018 -- would sum to a stale total.
            {
                "indicator_id": "POP_X",
                "geo_id": "be:mun:811",
                "period": "2018",
                "vintage": "v1",
                "value": "20000.0",
                "status": "final",
                "period_start": "2018-01-01",
                "period_end": "2018-12-31",
                "is_latest": "1",
                "created_at": "2018-06-01T00:00:00+00:00",
            },
            {
                "indicator_id": "POP_X",
                "geo_id": "be:mun:812",
                "period": "2018",
                "vintage": "v1",
                "value": "23264.0",
                "status": "final",
                "period_start": "2018-01-01",
                "period_end": "2018-12-31",
                "is_latest": "1",
                "created_at": "2018-06-01T00:00:00+00:00",
            },
            # Successor's own real rows, 2024-2026.
            {
                "indicator_id": "POP_X",
                "geo_id": "be:mun:810",
                "period": "2024",
                "vintage": "v1",
                "value": "45000.0",
                "status": "final",
                "period_start": "2024-01-01",
                "period_end": "2024-12-31",
                "is_latest": "1",
                "created_at": "2024-06-01T00:00:00+00:00",
            },
            {
                "indicator_id": "POP_X",
                "geo_id": "be:mun:810",
                "period": "2026",
                "vintage": "v1",
                "value": "46380.0",
                "status": "final",
                "period_start": "2026-01-01",
                "period_end": "2026-12-31",
                "is_latest": "1",
                "created_at": "2026-06-01T00:00:00+00:00",
            },
        ],
    )

    out_path = tmp_path / "communes_export.csv"
    export_communes_csv(db_path, out_path, extra_observations=(extra_csv,))
    rows = [r.split(",") for r in out_path.read_text(encoding="utf-8").splitlines()[1:]]
    succ_rows = [r for r in rows if r[0] == "be:mun:810"]
    assert len(succ_rows) == 1
    assert succ_rows[0][11] == "2026"
    assert float(succ_rows[0][12]) == pytest.approx(46380.0)
    assert succ_rows[0][13] == "A"


def test_genuine_gap_is_still_reconstructed_and_labelled(tmp_path):
    """A successor with NO row of its own for an indicator must still get a
    published, hand-summed, `reconstructed`-labelled row -- the fix must not
    remove legitimate gap-fills for the 13 new-code successors."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('FISCAL_X', 'statbel', 'x', 'x', 'Fiscal X', 'A', 'eur',
                   'higher_is_better', 1, 'x')""")
    conn.executescript("""
        INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:country', 'country', 'België', 'Belgique', 'Belgium', '1830-01-01');
        INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from)
          VALUES ('be:mun:820', '820', 'municipality', 'GapSucc', 'GapSucc', 'GapSucc',
                  '2019-01-01');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to, successor_geo_id)
          VALUES ('be:mun:821', '821', 'municipality', 'GapA', 'GapA', 'GapA',
                  '1830-01-01', '2019-01-01', 'be:mun:820');
        INSERT INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, valid_from, valid_to, successor_geo_id)
          VALUES ('be:mun:822', '822', 'municipality', 'GapB', 'GapB', 'GapB',
                  '1830-01-01', '2019-01-01', 'be:mun:820');
        """)
    conn.commit()
    conn.close()

    extra_csv = tmp_path / "fiscal.csv"
    _write_extra_csv(
        extra_csv,
        [
            {
                "indicator_id": "FISCAL_X",
                "geo_id": "be:mun:821",
                "period": "2023",
                "vintage": "v1",
                "value": "100000.0",
                "status": "final",
                "period_start": "2023-01-01",
                "period_end": "2023-12-31",
                "is_latest": "1",
                "created_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "indicator_id": "FISCAL_X",
                "geo_id": "be:mun:822",
                "period": "2023",
                "vintage": "v1",
                "value": "250000.0",
                "status": "final",
                "period_start": "2023-01-01",
                "period_end": "2023-12-31",
                "is_latest": "1",
                "created_at": "2026-01-01T00:00:00+00:00",
            },
        ],
    )

    out_path = tmp_path / "communes_export.csv"
    export_communes_csv(db_path, out_path, extra_observations=(extra_csv,))
    rows = [r.split(",") for r in out_path.read_text(encoding="utf-8").splitlines()[1:]]
    succ_rows = [r for r in rows if r[0] == "be:mun:820"]
    assert len(succ_rows) == 1
    assert succ_rows[0][11] == "2023"
    assert float(succ_rows[0][12]) == pytest.approx(350000.0)
    assert succ_rows[0][13] == "reconstructed"


def test_real_row_beats_reconstructed_row_at_the_same_period_regardless_of_input_order(
    tmp_path,
):
    """If a real row and a would-be reconstructed row ever land on the SAME
    period for the same cell, the real one must win -- never sort or
    insertion order. Exercised directly on `_latest_per_cell` since
    provoking this through the DB+CSV pipeline would require a successor
    that is simultaneously fully covered by predecessors AND has its own row
    for the same period, which `reconstruct()`'s own gap-fill rule already
    prevents from reaching it as two rows for one key -- this guards the
    export's OWN merge step, independent of that upstream guarantee ever
    weakening.
    """
    from export_communes_csv import _latest_per_cell

    real = ("be:mun:999", "POP_X", "Pop X", "count", "2024", 46380.0, "final", "t1")
    reconstructed = (
        "be:mun:999",
        "POP_X",
        "Pop X",
        "count",
        "2024",
        43264.0,
        "reconstructed",
        "t0",
    )

    assert _latest_per_cell([reconstructed, real]) == [real]
    assert _latest_per_cell([real, reconstructed]) == [real]
