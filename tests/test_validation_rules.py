"""Tests for the validation rule catalogue (Block H).

Two properties matter, and the second is the one people forget:

  1. Every rule FIRES on a violating fixture. A rule that cannot fire is
     worse than no rule, because it produces false confidence.
  2. No rule fires on the clean baseline. A rule that cries wolf gets the
     whole layer switched off within a week -- the roadmap says so
     explicitly, and it is the reason severity exists.

Two tests are deliberately regression tests for defects that actually reached
production this session: the CSV comma bug (287 rows) and the indicator
publishing an index under a name asserting millions of euro (91 rows).
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from src.db import migrate  # noqa: E402
from src.validation.rules import (  # noqa: E402
    FAIL,
    RULES,
    WARN,
    Context,
    has_failures,
    run_all,
)

REPO = Path(__file__).resolve().parents[1]
REAL_MIGRATIONS_DIR = REPO / "migrations"


def _clean_db(tmp_path: Path) -> Path:
    """A minimal, valid store: one source, one indicator, one geography, one
    observation. Every rule must be silent on this."""
    db = tmp_path / "test.db"
    migrate.run(db, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO sources (source_id, name, agency, adapter, catalog_ref) VALUES (?,?,?,?,?)",
        ("statbel", "Statbel", "Statbel", "statbel", "x"),
    )
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('POP', 'statbel', 'Bevolking', 'Population', 'Population', 'A', 'count',
                   'contextual', 1, 'x')""")
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('statbel','statbel','2026-01-01','ok')"
    )
    conn.execute("""INSERT INTO geographies (geo_id, level, name_nl, name_fr, name_en, valid_from)
           VALUES ('be:mun:11002','municipality','A','A','Antwerp','1830-01-01')""")
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('POP','be:mun:11002','2026','v1', 565615.0,'final',
                   '2026-01-01','2026-12-31',1,1,'2026-09-06T00:00:00+00:00')""")
    conn.commit()
    conn.close()
    return db


def _ctx(db: Path, **kw) -> Context:
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA foreign_keys=ON")
    return Context(conn=conn, **kw)


def _fired(violations, rule_name):
    return [v for v in violations if v.rule == rule_name]


# ── The baseline must be silent ─────────────────────────────────────────────


def test_no_rule_fires_on_a_clean_store(tmp_path):
    """A rule that cries wolf gets the whole layer disabled."""
    assert run_all(_ctx(_clean_db(tmp_path))) == []


def test_every_registered_rule_has_a_severity():
    assert RULES, "no rules registered"
    assert all(sev in (FAIL, WARN) for sev, _ in RULES.values())


# ── Referential ─────────────────────────────────────────────────────────────


def test_observation_with_no_indicator_fires(tmp_path):
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('GHOST','be:mun:11002','2026','v1',1.0,'final',
                   '2026-01-01','2026-12-31',1,1,'x')""")
    conn.commit()
    conn.close()
    v = run_all(_ctx(db), only=["observation_has_indicator"])
    assert _fired(v, "observation_has_indicator")
    assert has_failures(v)


def test_derived_value_stored_as_an_observation_fires(tmp_path):
    """CONTROL G, enforced continuously rather than checked once."""
    db = _clean_db(tmp_path)
    v = run_all(_ctx(db, derived_ids=frozenset({"POP"})), only=["derived_not_stored"])
    assert _fired(v, "derived_not_stored")
    assert has_failures(v)


def test_derived_rule_is_silent_when_nothing_leaked(tmp_path):
    db = _clean_db(tmp_path)
    v = run_all(_ctx(db, derived_ids=frozenset({"SOME_DERIVED"})), only=["derived_not_stored"])
    assert v == []


# ── Structural ──────────────────────────────────────────────────────────────


def test_duplicate_is_latest_fires(tmp_path):
    """Two current values for one cell would silently double an average."""
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('POP','be:mun:11002','2026','v2', 999.0,'final',
                   '2026-01-01','2026-12-31',1,1,'x')""")
    conn.commit()
    conn.close()
    v = run_all(_ctx(db), only=["unique_latest"])
    assert _fired(v, "unique_latest")


def test_period_not_matching_frequency_fires(tmp_path):
    """An annual indicator holding a quarterly period."""
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('POP','be:mun:11002','2026-Q1','v1',1.0,'final',
                   '2026-01-01','2026-03-31',1,1,'x')""")
    conn.commit()
    conn.close()
    v = run_all(_ctx(db), only=["period_matches_frequency"])
    assert _fired(v, "period_matches_frequency")
    assert "2026-Q1" in v[0].message


def test_export_with_an_unquoted_comma_fires(tmp_path):
    """REGRESSION: the real defect. Adding indicator names containing commas
    to a hand-rolled CSV writer shifted every column after the name on 287
    published rows. The file was still valid UTF-8 with the right line count
    -- only parsing it catches this."""
    bad = tmp_path / "broken_export.csv"
    bad.write_text(
        "indicator_code,name,period,value\n"
        "EUROSTAT_GDP_Q_MEUR,GDP volume index, Belgium (2010=100),2008-Q1,100.03\n",
        encoding="utf-8",
    )
    v = run_all(_ctx(_clean_db(tmp_path), exports=(bad,)), only=["export_parses"])
    assert _fired(v, "export_parses")
    assert has_failures(v)


def test_export_with_a_properly_quoted_comma_does_not_fire(tmp_path):
    good = tmp_path / "good_export.csv"
    good.write_text(
        "indicator_code,name,period,value\n"
        'EUROSTAT_GDP_Q_MEUR,"GDP volume index, Belgium (2010=100)",2008-Q1,100.03\n',
        encoding="utf-8",
    )
    assert run_all(_ctx(_clean_db(tmp_path), exports=(good,)), only=["export_parses"]) == []


def test_a_missing_export_fires(tmp_path):
    v = run_all(_ctx(_clean_db(tmp_path), exports=(tmp_path / "nope.csv",)), only=["export_parses"])
    assert _fired(v, "export_parses")


# ── Range ───────────────────────────────────────────────────────────────────


def test_negative_count_fires(tmp_path):
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE observations SET value = -5 WHERE indicator_id = 'POP'")
    conn.commit()
    conn.close()
    v = run_all(_ctx(db), only=["counts_non_negative"])
    assert _fired(v, "counts_non_negative")


def test_percentage_outside_bounds_fires(tmp_path):
    """Catches the classic unit error -- a rate stored on the wrong scale."""
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE indicators SET unit = 'percent' WHERE indicator_id = 'POP'")
    conn.execute("UPDATE observations SET value = 5200 WHERE indicator_id = 'POP'")
    conn.commit()
    conn.close()
    v = run_all(_ctx(db), only=["percent_bounded"])
    assert _fired(v, "percent_bounded")


def test_a_suppressed_cell_carrying_zero_fires(tmp_path):
    """The case the data model spec singles out: "If suppression looks like
    zero, you will publish 'median income EUR 0' for a small commune."

    Note the schema enforces the OPPOSITE direction already
    (001_core_schema.sql:86), so this is the genuinely unguarded one -- a
    rule hunting stray nulls could never fire."""
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute(
        "UPDATE observations SET status = 'suppressed', value = 0 WHERE indicator_id = 'POP'"
    )
    conn.commit()
    conn.close()
    v = run_all(_ctx(db), only=["suppressed_has_no_value"])
    assert _fired(v, "suppressed_has_no_value")
    assert has_failures(v)


def test_a_properly_suppressed_cell_does_not_fire(tmp_path):
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute(
        "UPDATE observations SET status = 'suppressed', value = NULL WHERE indicator_id = 'POP'"
    )
    conn.commit()
    conn.close()
    assert run_all(_ctx(db), only=["suppressed_has_no_value"]) == []


# ── Labelling ───────────────────────────────────────────────────────────────


def test_a_currency_name_over_an_index_unit_fires(tmp_path):
    """REGRESSION: the real defect. EUROSTAT_GDP_Q_MEUR published a 2010-based
    index under a name asserting millions of euro -- 91 rows reached the
    public CSV, invisible to every other rule here (the values were
    plausible, the structure sound, the references intact)."""
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute(
        "UPDATE indicators SET unit = 'index_2010', name_en = 'EUROSTAT_GDP_Q_MEUR' "
        "WHERE indicator_id = 'POP'"
    )
    conn.commit()
    conn.close()
    v = run_all(_ctx(db), only=["unit_name_agreement"])
    assert _fired(v, "unit_name_agreement")
    assert v[0].severity == WARN, "heuristic rules must warn, not block"


def test_an_honest_index_name_does_not_fire(tmp_path):
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute(
        "UPDATE indicators SET unit = 'index_2010', "
        "name_en = 'GDP volume index, Belgium (2010=100)' WHERE indicator_id = 'POP'"
    )
    conn.commit()
    conn.close()
    assert run_all(_ctx(db), only=["unit_name_agreement"]) == []


def test_a_placeholder_name_fires(tmp_path):
    """Would have caught the twelve indicators whose name.en/fr/nl were all
    the raw indicator id."""
    db = _clean_db(tmp_path)
    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE indicators SET name_fr = 'POP' WHERE indicator_id = 'POP'")
    conn.commit()
    conn.close()
    v = run_all(_ctx(db), only=["has_trilingual_name"])
    assert _fired(v, "has_trilingual_name")
    assert "fr" in v[0].message


# ── Severity discipline ─────────────────────────────────────────────────────


@pytest.mark.parametrize("name", sorted(RULES))
def test_each_rule_is_individually_silent_on_a_clean_store(tmp_path, name):
    """Parametrized so a newly added noisy rule is named in the failure
    rather than hiding inside an aggregate assertion."""
    assert run_all(_ctx(_clean_db(tmp_path)), only=[name]) == []


def test_the_real_committed_stores_pass(tmp_path):
    """The baseline the spec claims. If this fails, something regressed in
    the committed data rather than in the rules."""
    conn = sqlite3.connect(str(REPO / "data" / "belgian_macro.db"))
    conn.execute("PRAGMA foreign_keys=ON")
    exports = (
        REPO / "data" / "belgian_macro_export.csv",
        REPO / "data" / "communes_export.csv",
        REPO / "data" / "population_observations.csv",
    )
    violations = run_all(Context(conn=conn, exports=exports))
    conn.close()
    assert not has_failures(violations), [str(v) for v in violations]
