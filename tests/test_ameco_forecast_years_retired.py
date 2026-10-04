"""ADR 0017 (docs/decisions/0017-ameco-forecast-periods.md, ACCEPTED
2026-10-04): a European Commission forecast year must never sit in
`observations` as if it were a measurement. Ten rows did -- LABOUR_COST_BE,
_DE, _EA, _FR, _NL, each for 2026 and 2027 -- because
src/fetchers/dbnomics.py stamped every row "final" unconditionally.

Two separate guarantees, each with its own test below:
  - the ADAPTER no longer WRITES a forecast year (tests/test_dbnomics_source.py
    covers the logic across a Spring and an Autumn release in detail; this
    file does not repeat that);
  - migrations/006_retire_ameco_forecast_years.sql REMOVES the ten rows
    that were already written before the adapter was fixed -- the gap ADR
    0017 names directly: fixing the adapter alone does not retroactively
    unwrite what a previous, wrong fetch already stored, because
    legacy_observations is never deleted by a normal upsert and
    scripts/sync_to_canonical.py re-reads every row for a code on every run,
    not only today's.

This is the same shape of problem as migrations/005_population_weighted_label.sql
(tests/test_aggregation_method_label.py), and this file follows its pattern:
a minimal standalone fixture table rather than the full schema, so the
migration's own two statements are exercised directly and in isolation.
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
COMMITTED_DB = REPO_ROOT / "data" / "belgian_macro.db"
MIGRATION = REPO_ROOT / "migrations" / "006_retire_ameco_forecast_years.sql"

AMECO_IDS = (
    "LABOUR_COST_BE",
    "LABOUR_COST_DE",
    "LABOUR_COST_EA",
    "LABOUR_COST_FR",
    "LABOUR_COST_NL",
)
FORECAST_PERIODS = ("2026", "2027")

# The live boundary as of this PR (AMECO's May 2026 release, indexed_at
# ~2026-05-22 -> last outturn year 2025 per ADR 0017's own worked example).
# This number moves with every AMECO release (mid-May, mid-November) --
# exactly the behaviour tests/test_dbnomics_source.py pins across both --
# so this integration check must be re-verified (and this constant bumped)
# against the committed database after the next real AMECO fetch picks up a
# new release. It is not what guards the adapter's logic across a release;
# it only guards today's committed snapshot.
LAST_KNOWN_OUTTURN_YEAR = 2025


@pytest.mark.skipif(not COMMITTED_DB.exists(), reason="committed database not present")
def test_no_stored_ameco_row_has_a_period_after_its_last_outturn_year():
    conn = sqlite3.connect(f"file:{COMMITTED_DB}?mode=ro", uri=True)
    try:
        offenders = conn.execute(
            f"""SELECT indicator_id, period FROM observations
                WHERE indicator_id IN ({",".join("?" for _ in AMECO_IDS)})
                  AND is_latest = 1
                  AND CAST(period AS INTEGER) > ?
                ORDER BY indicator_id, period""",
            (*AMECO_IDS, LAST_KNOWN_OUTTURN_YEAR),
        ).fetchall()
    finally:
        conn.close()
    assert not offenders, (
        f"{len(offenders)} AMECO row(s) are stored as current beyond the last outturn "
        f"year {LAST_KNOWN_OUTTURN_YEAR}: {offenders}. A Commission forecast must not "
        "sit in observations as a measurement (ADR 0017, CLAUDE.md rule 6)."
    )


@pytest.mark.skipif(not COMMITTED_DB.exists(), reason="committed database not present")
def test_no_stale_legacy_row_survives_for_the_retired_forecast_periods():
    """The other half of the gap this migration closes: a legacy row left
    behind would make sync_to_canonical.py resurrect the retired canonical
    row on the next run (src/db/vintages.py's upsert_observation finds no
    is_latest=1 row, treats the stale value as new, and inserts a fresh
    one)."""
    conn = sqlite3.connect(f"file:{COMMITTED_DB}?mode=ro", uri=True)
    try:
        offenders = conn.execute(
            f"""SELECT indicator_code, period FROM legacy_observations
                WHERE indicator_code IN ({",".join("?" for _ in AMECO_IDS)})
                  AND period IN ({",".join("?" for _ in FORECAST_PERIODS)})
                ORDER BY indicator_code, period""",
            (*AMECO_IDS, *FORECAST_PERIODS),
        ).fetchall()
    finally:
        conn.close()
    assert not offenders, (
        f"stale legacy_observations row(s) for a retired AMECO forecast period: "
        f"{offenders}. Left in place, the next scripts/sync_to_canonical.py run "
        "would resurrect them as a fresh, 'final' vintage."
    )


def _fixture_db(tmp_path) -> sqlite3.Connection:
    db = tmp_path / "probe.db"
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE observations (
            indicator_id TEXT NOT NULL,
            geo_id       TEXT NOT NULL,
            period       TEXT NOT NULL,
            vintage      TEXT NOT NULL,
            value        REAL,
            status       TEXT NOT NULL,
            is_latest    INTEGER NOT NULL,
            PRIMARY KEY (indicator_id, geo_id, period, vintage)
        )""")
    conn.execute("""CREATE TABLE legacy_observations (
            indicator_code TEXT NOT NULL,
            period         TEXT NOT NULL,
            value           REAL NOT NULL,
            obs_status      TEXT,
            PRIMARY KEY (indicator_code, period)
        )""")
    return conn


def _seed(conn: sqlite3.Connection) -> None:
    rows = []
    for code in AMECO_IDS:
        geo = {"LABOUR_COST_BE": "be:country", "LABOUR_COST_DE": "de:country"}.get(
            code, "ea:aggregate"
        )
        # The row to retire: is_latest=1, a forecast period.
        for period in FORECAST_PERIODS:
            rows.append((code, geo, period, "v-forecast", 142.0, "final", 1))
        # A real outturn year for the SAME indicator, is_latest=1 -- must
        # survive untouched: it is not a forecast period.
        rows.append((code, geo, "2025", "v-outturn", 139.0, "final", 1))
        # An OLDER, already-superseded vintage of one of the forecast
        # periods, is_latest=0 already -- must stay 0, and must not be
        # double-counted as something this migration changed.
        rows.append((code, geo, "2026", "v-old", 140.0, "final", 0))
    conn.executemany(
        "INSERT INTO observations "
        "(indicator_id, geo_id, period, vintage, value, status, is_latest) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    # An unrelated indicator at period 2026, is_latest=1 -- must never be
    # touched: this migration names five AMECO ids explicitly, nothing else.
    conn.execute(
        "INSERT INTO observations "
        "(indicator_id, geo_id, period, vintage, value, status, is_latest) "
        "VALUES ('GDP_BE', 'be:country', '2026', 'v1', 100.0, 'final', 1)"
    )
    for code in AMECO_IDS:
        for period in FORECAST_PERIODS:
            conn.execute(
                "INSERT INTO legacy_observations (indicator_code, period, value, obs_status) "
                "VALUES (?, ?, 142.0, 'A')",
                (code, period),
            )
        conn.execute(
            "INSERT INTO legacy_observations (indicator_code, period, value, obs_status) "
            "VALUES (?, '2025', 139.0, 'A')",
            (code,),
        )
    conn.execute(
        "INSERT INTO legacy_observations (indicator_code, period, value, obs_status) "
        "VALUES ('GDP_BE', '2026', 100.0, 'A')"
    )
    conn.commit()


def test_the_migration_retires_exactly_ten_canonical_rows_and_deletes_ten_legacy_rows(tmp_path):
    conn = _fixture_db(tmp_path)
    _seed(conn)
    sql = MIGRATION.read_text(encoding="utf-8")

    before_latest = dict(
        conn.execute(
            "SELECT indicator_id || '/' || period || '/' || vintage, is_latest " "FROM observations"
        )
    )
    assert sum(1 for v in before_latest.values() if v == 0) == 5  # the five v-old rows

    conn.executescript(sql)
    conn.commit()

    after = dict(
        conn.execute(
            "SELECT indicator_id || '/' || period || '/' || vintage, is_latest " "FROM observations"
        )
    )
    changed = {k for k in before_latest if before_latest[k] != after[k]}
    assert len(changed) == 10, changed
    assert changed == {
        f"{code}/{period}/v-forecast" for code in AMECO_IDS for period in FORECAST_PERIODS
    }
    assert all(after[k] == 0 for k in changed)
    # Untouched: the real outturn year, the already-old vintage, and the
    # unrelated indicator.
    for code in AMECO_IDS:
        assert after[f"{code}/2025/v-outturn"] == 1
        assert after[f"{code}/2026/v-old"] == 0
    assert after["GDP_BE/2026/v1"] == 1

    legacy_left = {
        row[0]
        for row in conn.execute("SELECT indicator_code || '/' || period FROM legacy_observations")
    }
    conn.close()
    assert legacy_left == {f"{code}/2025" for code in AMECO_IDS} | {"GDP_BE/2026"}


def test_the_migration_is_idempotent(tmp_path):
    conn = _fixture_db(tmp_path)
    _seed(conn)
    sql = MIGRATION.read_text(encoding="utf-8")

    for _ in range(2):
        conn.executescript(sql)
        conn.commit()

    remaining_latest_forecast_rows = conn.execute(
        f"""SELECT COUNT(*) FROM observations
            WHERE indicator_id IN ({",".join("?" for _ in AMECO_IDS)})
              AND period IN ({",".join("?" for _ in FORECAST_PERIODS)})
              AND is_latest = 1""",
        (*AMECO_IDS, *FORECAST_PERIODS),
    ).fetchone()[0]
    remaining_legacy_rows = conn.execute(
        f"""SELECT COUNT(*) FROM legacy_observations
            WHERE indicator_code IN ({",".join("?" for _ in AMECO_IDS)})
              AND period IN ({",".join("?" for _ in FORECAST_PERIODS)})""",
        (*AMECO_IDS, *FORECAST_PERIODS),
    ).fetchone()[0]
    conn.close()
    assert remaining_latest_forecast_rows == 0
    assert remaining_legacy_rows == 0


def test_the_migration_touches_only_the_two_tables_and_columns_it_should():
    sql = MIGRATION.read_text(encoding="utf-8")
    statements = [
        line.strip()
        for line in sql.splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]
    body = " ".join(statements).upper()
    assert body.count("UPDATE") == 1
    assert body.count("DELETE") == 1
    assert "UPDATE OBSERVATIONS" in body
    assert "SET IS_LATEST = 0" in body
    assert "DELETE FROM LEGACY_OBSERVATIONS" in body
    # CREATE TABLE IF NOT EXISTS is allowed -- defensive only (see the
    # migration file's own comment): it never touches an existing table,
    # it only lets this migration also run against a bare canonical-only
    # database that was never bootstrapped through MacroDatabase.
    assert body.count("CREATE TABLE") == 1
    assert body.count("CREATE TABLE IF NOT EXISTS") == 1
    for forbidden in ("DROP", "ALTER", "INSERT"):
        assert forbidden not in body, f"unexpected {forbidden} in migration 006"
    for ind in AMECO_IDS:
        assert ind in body
    for period in FORECAST_PERIODS:
        assert f"'{period}'" in body


def test_the_migration_needs_no_recreate_mode_marker():
    first_line = MIGRATION.read_text(encoding="utf-8").splitlines()[0]
    assert "migration-mode:" not in first_line, first_line


def test_migration_filename_matches_the_runners_pattern():
    from src.db.migrate import FILENAME_RE  # noqa: PLC0415

    assert FILENAME_RE.match(MIGRATION.name)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(subprocess.call([sys.executable, "-m", "pytest", __file__]))
