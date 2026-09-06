"""Tests for the Statbel fiscal-income loader.

The cases that matter here are the ones where a plausible-looking shortcut
publishes a wrong number rather than crashing:

  * a suppressed cell becoming 0 instead of NULL,
  * a real 0 being read as suppressed,
  * a status change at an unchanged value writing no new vintage,
  * rows silently dropped because the file's geography is a fixed vintage
    rather than the boundaries of each year.

Every one of those was observed in the real file before being written down.
"""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from sync_fiscal_income import (  # noqa: E402
    COLUMN_TO_INDICATOR,
    GEOGRAPHY_REFERENCE_PERIOD,
    MissingFiscalData,
    _assert_geography_vintage,
    _coerce,
    _upsert_observation,
)

from src.analytics.derived import mean_from_total  # noqa: E402
from src.db import migrate  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
STORE = REPO / "data" / "fiscal_income_observations.csv"


# ── Reading Statbel's cells ─────────────────────────────────────────────────


def test_a_number_is_final():
    assert _coerce(1849503.78, "X", "73028", 2023) == (1849503.78, "final")


def test_the_asterisk_is_suppressed_and_never_zero():
    """Statbel marks a withheld cell with '*', so a naive numeric read does
    not fail -- it puts a string where a number belongs."""
    value, status = _coerce("*", "X", "73028", 2023)
    assert value is None
    assert status == "suppressed"


def test_a_real_zero_stays_zero():
    """Knokke-Heist, Koksijde and De Panne levy no municipal surcharge at
    all. Reading that as 'missing' would erase a true fact about them."""
    assert _coerce(0, "FISCAL_TOT_MUNICIP_TAXES", "31043", 2023) == (0.0, "final")


def test_an_unknown_marker_raises_rather_than_guesses():
    with pytest.raises(ValueError, match="Refusing to guess"):
        _coerce("n/a", "X", "11001", 2023)


# ── The fixed geography vintage ─────────────────────────────────────────────


def _db_with_geographies(tmp_path: Path, codes: dict[str, tuple[str, str | None]]) -> Path:
    db = tmp_path / "t.db"
    migrate.run(db, migrations_dir=REPO / "migrations")
    conn = sqlite3.connect(str(db))
    for nis, (valid_from, valid_to) in codes.items():
        conn.execute(
            "INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, "
            "valid_from, valid_to) VALUES (?,?,'municipality',?,?,?,?,?)",
            (f"be:mun:{nis}", nis, nis, nis, nis, valid_from, valid_to),
        )
    conn.commit()
    conn.close()
    return db


def test_vintage_check_passes_when_every_code_resolves(tmp_path):
    db = _db_with_geographies(tmp_path, {"11001": ("1830-01-01", None)})
    conn = sqlite3.connect(str(db))
    _assert_geography_vintage(conn, [{"nis": "11001"}], GEOGRAPHY_REFERENCE_PERIOD)
    conn.close()


def test_vintage_check_refuses_a_partial_panel(tmp_path):
    """The real failure this guards: resolving each row at its own period
    dropped 252 rows (2.3%) because Statbel back-casts the 2019 commune
    structure across 2005-2023. A partial fiscal panel looks complete."""
    db = _db_with_geographies(tmp_path, {"11001": ("1830-01-01", None)})
    conn = sqlite3.connect(str(db))
    with pytest.raises(MissingFiscalData, match="do not resolve at the reference period"):
        _assert_geography_vintage(conn, [{"nis": "11001"}, {"nis": "99999"}], "2024")
    conn.close()


# ── Vintage discipline ──────────────────────────────────────────────────────


def _write(conn, value, status, vintage):
    return _upsert_observation(
        conn,
        indicator_id="FISCAL_TOT_MUNICIP_TAXES",
        geo_id="be:mun:11001",
        period="2023",
        period_start="2023-01-01",
        period_end="2023-12-31",
        value=value,
        status=status,
        vintage=vintage,
        fetch_run_id=1,
    )


def _seeded(tmp_path: Path) -> sqlite3.Connection:
    db = _db_with_geographies(tmp_path, {"11001": ("1830-01-01", None)})
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) "
        "VALUES ('statbel','Statbel','Statbel','statbel','x')"
    )
    conn.execute(
        "INSERT INTO indicators (indicator_id, source_id, name_nl, name_fr, name_en, "
        "frequency, unit, preferred_direction, is_additive, config_path) "
        "VALUES ('FISCAL_TOT_MUNICIP_TAXES','statbel','a','b','c','A','eur',"
        "'contextual',1,'x')"
    )
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('statbel','statbel','2026-01-01','ok')"
    )
    conn.commit()
    return conn


def test_unchanged_value_writes_no_new_vintage(tmp_path):
    conn = _seeded(tmp_path)
    assert _write(conn, 100.0, "final", "v1") == 1
    assert _write(conn, 100.0, "final", "v2") == 0
    assert conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1
    conn.close()


def test_status_change_at_an_unchanged_value_writes_a_new_vintage(tmp_path):
    """The divergence found in sync_population.py, which compares value only:
    a cell moving final -> suppressed at an unchanged number would write no
    vintage and the suppression would be lost. Here status is part of the
    predicate, so it cannot be."""
    conn = _seeded(tmp_path)
    _write(conn, 100.0, "final", "v1")
    assert _write(conn, None, "suppressed", "v2") == 1
    rows = conn.execute(
        "SELECT value, status, is_latest FROM observations ORDER BY vintage"
    ).fetchall()
    assert rows == [(100.0, "final", 0), (None, "suppressed", 1)]
    conn.close()


# ── The published average ───────────────────────────────────────────────────


def test_mean_from_total_matches_the_hand_computed_figure():
    """Herstappe 2023, the commune that is 100% suppressed in the sector
    file and complete in this one."""
    assert round(mean_from_total(1849503.78, 41), 2) == 45109.85


@pytest.mark.parametrize("total,count", [(None, 41), (100.0, None), (100.0, 0)])
def test_mean_from_total_refuses_to_invent_a_number(total, count):
    assert mean_from_total(total, count) is None


# ── The committed store ─────────────────────────────────────────────────────


@pytest.mark.skipif(not STORE.is_file(), reason="manual store not present")
def test_the_committed_store_is_complete_and_consistent():
    with STORE.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    by_indicator = {}
    for row in rows:
        by_indicator.setdefault(row["indicator_id"], []).append(row)

    assert set(by_indicator) == set(COLUMN_TO_INDICATOR.values())
    # 581 communes x 19 years (2005-2023), every indicator, no gaps.
    for indicator_id, obs in by_indicator.items():
        assert len(obs) == 11039, indicator_id
        periods = {o["period"] for o in obs}
        assert periods == {str(y) for y in range(2005, 2024)}, indicator_id

    # A suppressed row must carry no value, and a value must carry no
    # suppressed status -- the two directions of the same mistake.
    for row in rows:
        if row["status"] == "suppressed":
            assert row["value"] == ""
        else:
            assert row["value"] != ""
