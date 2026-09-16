"""Tests for the full-history commune export (export_communes_history_csv.py).

Two properties are unique to THIS export and not already covered by
test_export_communes_csv.py or test_derived_engine.py, so they are what this
file actually tests:

  1. Every period survives, not just the latest -- the whole point of this
     export existing as something separate from the snapshot one.
  2. A commune merged away since still enters a PAST year's peer set for
     percentile/z_score, even though it never appears as an output row --
     the two-stage "compute wide, display narrow" shape the module docstring
     describes.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from export_communes_history_csv import export_communes_history_csv  # noqa: E402

from src.db import migrate  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
REAL_MIGRATIONS_DIR = REPO / "migrations"


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
           VALUES ('POPULATION_BY_COMMUNE', 'statbel', 'Bevolking', 'Population', 'Population',
                   'A', 'count', 'contextual', 1, 'x')""")
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) "
        "VALUES ('statbel','statbel','2026-01-01','ok')"
    )
    return conn


def _geo(conn, geo_id, nis, name, valid_to=None, successor_geo_id=None):
    conn.execute(
        "INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, "
        "valid_from, valid_to, successor_geo_id) VALUES (?,?, 'municipality', ?,?,?, "
        "'1830-01-01', ?, ?)",
        (geo_id, nis, name, name, name, valid_to, successor_geo_id),
    )


def _pop(conn, geo_id, period, value):
    conn.execute(
        "INSERT INTO observations (indicator_id, geo_id, period, vintage, value, status, "
        "period_start, period_end, is_latest, fetch_run_id, created_at) "
        "VALUES ('POPULATION_BY_COMMUNE', ?, ?, 'v1', ?, 'final', ?, ?, 1, 1, "
        "'2026-01-01T00:00:00+00:00')",
        (geo_id, period, value, f"{period}-01-01", f"{period}-12-31"),
    )


@pytest.fixture
def merger_db(tmp_path):
    """Two current communes (A, B) and one merged away after 2020 (OLD).

    2020: A=100, B=50, OLD=10   -- 3 communes existed.
    2021: A=110, B=55           -- OLD is gone, 2 communes exist.
    """
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    _geo(conn, "be:mun:A", "1", "Commune A")
    _geo(conn, "be:mun:B", "2", "Commune B")
    _geo(conn, "be:mun:OLD", "9", "Merged Commune", valid_to="2020-12-31")
    _pop(conn, "be:mun:A", "2020", 100.0)
    _pop(conn, "be:mun:B", "2020", 50.0)
    _pop(conn, "be:mun:OLD", "2020", 10.0)
    _pop(conn, "be:mun:A", "2021", 110.0)
    _pop(conn, "be:mun:B", "2021", 55.0)
    conn.commit()
    conn.close()
    return db_path


_PERCENTILE_YAML = """
id: POPULATION_PERCENTILE
name: {en: Population percentile, fr: Percentile, nl: Percentiel}
unit: percent
frequency: A
geo_levels: [municipal]
preferred_direction: contextual
derived:
  function: percentile
  inputs: [POPULATION_BY_COMMUNE]
"""

_CAGR_YAML = """
id: POPULATION_CAGR_10Y
name: {en: 10y CAGR, fr: TCAC 10 ans, nl: CAGR 10j}
unit: percent_per_year
frequency: A
geo_levels: [municipal]
preferred_direction: contextual
derived:
  function: cagr
  inputs: [POPULATION_BY_COMMUNE]
  args: {years: 10}
"""


def _derived_dir(tmp_path) -> Path:
    """A derived-config directory scoped to what these tests actually
    exercise -- the real config/indicators/derived/ also has configs whose
    inputs (fiscal, age bands) this minimal fixture DB does not carry, and
    load_and_validate_derived would reject them as referencing indicators
    nothing here provides."""
    d = tmp_path / "derived"
    d.mkdir()
    (d / "POPULATION_PERCENTILE.yaml").write_text(_PERCENTILE_YAML)
    (d / "POPULATION_CAGR_10Y.yaml").write_text(_CAGR_YAML)
    return d


def _rows(out_path):
    lines = out_path.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    return [dict(zip(header, line.split(","), strict=False)) for line in lines[1:]]


# ── Every period survives ───────────────────────────────────────────────────


def test_both_periods_appear_not_just_the_latest(merger_db, tmp_path):
    derived_dir = _derived_dir(tmp_path)
    out = tmp_path / "history.csv"
    export_communes_history_csv(merger_db, out, derived_dir=derived_dir)
    rows = _rows(out)
    periods = {r["period"] for r in rows if r["indicator_code"] == "POPULATION_BY_COMMUNE"}
    assert periods == {"2020", "2021"}


# ── The peer-set-by-period property, unique to this export ─────────────────


def test_a_merged_away_commune_still_widens_a_past_years_peer_set(merger_db, tmp_path):
    derived_dir = _derived_dir(tmp_path)
    """The 2020 percentile must be computed against N=3 (A, B, OLD all
    existed), not N=2 (today's surviving pair) -- proven by comparing to the
    hand-worked value for each denominator, which differ."""
    out = tmp_path / "history.csv"
    export_communes_history_csv(merger_db, out, derived_dir=derived_dir)
    rows = _rows(out)

    def pct(geo_id, period):
        for r in rows:
            if (
                r["geo_id"] == geo_id
                and r["indicator_code"] == "POPULATION_PERCENTILE"
                and r["period"] == period
            ):
                return float(r["value"])
        raise AssertionError(f"no POPULATION_PERCENTILE row for {geo_id}/{period}")

    # 2020, N=3 {100,50,10}: A is highest -> below=2, equal=1 -> 100*2.5/3.
    assert pct("be:mun:A", "2020") == pytest.approx(100 * 2.5 / 3)
    # If OLD had been silently excluded (the bug this test guards against),
    # N would be 2 and A would score 100*1.5/2 = 75.0 instead -- a different
    # number, so this assertion actually distinguishes the two behaviours.
    assert pct("be:mun:A", "2020") != pytest.approx(75.0)

    # 2021, N=2 {110,55}: A is highest -> below=1, equal=1 -> 100*1.5/2.
    assert pct("be:mun:A", "2021") == pytest.approx(75.0)
    assert pct("be:mun:B", "2021") == pytest.approx(25.0)


def test_the_merged_away_commune_itself_never_appears_in_the_output(merger_db, tmp_path):
    derived_dir = _derived_dir(tmp_path)
    """OLD contributes to the 2020 peer set but is not a current commune, so
    it must not get its own row -- this page shows today's communes only."""
    out = tmp_path / "history.csv"
    export_communes_history_csv(merger_db, out, derived_dir=derived_dir)
    rows = _rows(out)
    assert not any(r["geo_id"] == "be:mun:OLD" for r in rows)


def test_a_derived_cell_with_no_value_is_dropped_not_written_blank(merger_db, tmp_path):
    derived_dir = _derived_dir(tmp_path)
    """POPULATION_CAGR_10Y needs a value ten years earlier, which this
    two-year fixture never has. The row must be absent, not present with an
    empty value -- an absent cell already renders as 'n/a' in communes.html;
    a present-but-blank one would look like a different kind of gap."""
    out = tmp_path / "history.csv"
    export_communes_history_csv(merger_db, out, derived_dir=derived_dir)
    rows = _rows(out)
    assert not any(r["indicator_code"] == "POPULATION_CAGR_10Y" for r in rows)


# ── The size-guard trim: last N years by default, --all-periods for the rest ─


@pytest.fixture
def long_history_db(tmp_path):
    """One commune, one value per year from 2005 to 2026 -- 22 years, wide
    enough that a 10-year default trim and a full 22-year read are visibly
    different outputs, not an off-by-one away from each other."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    _geo(conn, "be:mun:A", "1", "Commune A")
    for year in range(2005, 2027):
        _pop(conn, "be:mun:A", str(year), float(year))
    conn.commit()
    conn.close()
    return db_path


def test_by_default_only_the_last_ten_years_are_written(long_history_db, tmp_path):
    out = tmp_path / "history.csv"
    n = export_communes_history_csv(long_history_db, out, derived_dir=tmp_path / "empty")
    rows = _rows(out)
    periods = {r["period"] for r in rows}
    # Newest row is 2026, so the last 10 calendar years are 2017-2026 inclusive.
    assert periods == {str(y) for y in range(2017, 2027)}
    assert n == len(rows) == 10


def test_all_periods_writes_every_year_back_to_the_start(long_history_db, tmp_path):
    out = tmp_path / "history.csv"
    export_communes_history_csv(
        long_history_db, out, derived_dir=tmp_path / "empty", all_periods=True
    )
    rows = _rows(out)
    periods = {r["period"] for r in rows}
    assert periods == {str(y) for y in range(2005, 2027)}


def test_recent_years_is_configurable(long_history_db, tmp_path):
    out = tmp_path / "history.csv"
    export_communes_history_csv(
        long_history_db, out, derived_dir=tmp_path / "empty", recent_years=3
    )
    rows = _rows(out)
    periods = {r["period"] for r in rows}
    assert periods == {"2024", "2025", "2026"}


def test_the_trim_never_starves_a_derived_indicators_own_lookback(long_history_db, tmp_path):
    """THE CORRECTNESS PROPERTY THE WHOLE DESIGN DEPENDS ON. POPULATION_CAGR_10Y
    for 2026 needs 2016's value as an INPUT, and 2016 falls seven years before
    the default 10-year output WINDOW's start (2017). If the trim were applied
    to the observation set fed to the engine rather than only to the rows
    written afterwards, this value would silently vanish -- exactly the
    indicators built to look back the furthest would be the first casualty of
    a fix aimed at file size, not correctness."""
    derived_dir = _derived_dir(tmp_path)
    out = tmp_path / "history.csv"
    export_communes_history_csv(long_history_db, out, derived_dir=derived_dir)
    rows = _rows(out)
    cagr_2026 = [
        r for r in rows if r["indicator_code"] == "POPULATION_CAGR_10Y" and r["period"] == "2026"
    ]
    assert len(cagr_2026) == 1, "the trim must not have starved this value of its own input"
    # 2016 -> 2026, 10 years, values equal to the year themselves: CAGR of
    # 2026/2016 over 10 years.
    expected = (2026 / 2016) ** (1 / 10) - 1
    assert float(cagr_2026[0]["value"]) == pytest.approx(expected * 100, abs=1e-6)


# ── Merger back-aggregation, integrated through the exporter ────────────────


@pytest.fixture
def merger_lineage_db(tmp_path):
    """Two predecessors (OLDA, OLDB) merged into SUCC in 2025; a third,
    unrelated current commune (OTHER) exists throughout and is never involved
    in any merger -- present so the peer-set-unchanged test has something to
    compare a pre-merger percentile against."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    _geo(conn, "be:mun:SUCC", "900", "Successor")
    _geo(
        conn,
        "be:mun:OLDA",
        "901",
        "Predecessor A",
        valid_to="2025-01-01",
        successor_geo_id="be:mun:SUCC",
    )
    _geo(
        conn,
        "be:mun:OLDB",
        "902",
        "Predecessor B",
        valid_to="2025-01-01",
        successor_geo_id="be:mun:SUCC",
    )
    _geo(conn, "be:mun:OTHER", "903", "Other")
    # 2020-2024: only the predecessors have population (pre-merger years).
    for period, a, b, other in [
        ("2020", 100.0, 50.0, 30.0),
        ("2021", 110.0, 55.0, 31.0),
    ]:
        _pop(conn, "be:mun:OLDA", period, a)
        _pop(conn, "be:mun:OLDB", period, b)
        _pop(conn, "be:mun:OTHER", period, other)
    # From 2025 the successor has its own value.
    _pop(conn, "be:mun:SUCC", "2025", 200.0)
    _pop(conn, "be:mun:OTHER", "2025", 32.0)
    conn.commit()
    conn.close()
    return db_path


def test_the_successor_gains_reconstructed_pre_merger_population(merger_lineage_db, tmp_path):
    out = tmp_path / "history.csv"
    export_communes_history_csv(
        merger_lineage_db, out, derived_dir=tmp_path / "empty", all_periods=True
    )
    rows = _rows(out)
    succ_2020 = [r for r in rows if r["geo_id"] == "be:mun:SUCC" and r["period"] == "2020"]
    assert len(succ_2020) == 1
    assert float(succ_2020[0]["value"]) == pytest.approx(150.0)  # 100 + 50
    assert succ_2020[0]["status"] == "reconstructed"


def test_the_successor_keeps_its_own_value_where_it_has_one(merger_lineage_db, tmp_path):
    """Gap-fill, not restatement: 2025 is the successor's own real value and
    must not be replaced or supplemented by a reconstructed sum."""
    out = tmp_path / "history.csv"
    export_communes_history_csv(
        merger_lineage_db, out, derived_dir=tmp_path / "empty", all_periods=True
    )
    rows = _rows(out)
    succ_2025 = [r for r in rows if r["geo_id"] == "be:mun:SUCC" and r["period"] == "2025"]
    assert len(succ_2025) == 1
    assert float(succ_2025[0]["value"]) == pytest.approx(200.0)
    assert succ_2025[0]["status"] == "A"  # the real status letter, not reconstructed


def test_a_predecessor_never_gets_its_own_row(merger_lineage_db, tmp_path):
    out = tmp_path / "history.csv"
    export_communes_history_csv(
        merger_lineage_db, out, derived_dir=tmp_path / "empty", all_periods=True
    )
    rows = _rows(out)
    assert not any(r["geo_id"] in ("be:mun:OLDA", "be:mun:OLDB") for r in rows)


def test_percentile_for_a_pre_merger_period_is_unchanged_by_reconstruction(
    merger_lineage_db, tmp_path
):
    """Reconstructed rows must never enter the engine's peer set: OTHER's 2020
    percentile is computed over {OLDA=100, OLDB=50, OTHER=30}, N=3, exactly as
    it would be with no merger back-aggregation at all. If the reconstructed
    SUCC=150 leaked into that set, N would become 4 and the percentile would
    change to a different, wrong number."""
    derived_dir = _derived_dir(tmp_path)
    out = tmp_path / "history.csv"
    export_communes_history_csv(merger_lineage_db, out, derived_dir=derived_dir, all_periods=True)
    rows = _rows(out)

    def pct(geo_id, period):
        for r in rows:
            if (
                r["geo_id"] == geo_id
                and r["indicator_code"] == "POPULATION_PERCENTILE"
                and r["period"] == period
            ):
                return float(r["value"])
        raise AssertionError(f"no POPULATION_PERCENTILE row for {geo_id}/{period}")

    # N=3 {100, 50, 30}: OTHER (30) is lowest -> below=0, equal=1 -> 100*0.5/3.
    assert pct("be:mun:OTHER", "2020") == pytest.approx(100 * 0.5 / 3)
    # If the reconstructed SUCC value had leaked into the peer set (N=4), the
    # result would differ from the N=3 answer above.
    assert pct("be:mun:OTHER", "2020") != pytest.approx(100 * 0.5 / 4)


def test_a_partial_predecessor_set_produces_no_reconstructed_row(tmp_path):
    """Rule 2 end to end: if OLDB never reports 2020 at all, SUCC must get no
    reconstructed 2020 row -- not a sum of OLDA alone."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    _geo(conn, "be:mun:SUCC", "900", "Successor")
    _geo(
        conn,
        "be:mun:OLDA",
        "901",
        "Predecessor A",
        valid_to="2025-01-01",
        successor_geo_id="be:mun:SUCC",
    )
    _geo(
        conn,
        "be:mun:OLDB",
        "902",
        "Predecessor B",
        valid_to="2025-01-01",
        successor_geo_id="be:mun:SUCC",
    )
    _pop(conn, "be:mun:OLDA", "2020", 100.0)
    # OLDB has no 2020 row at all.
    conn.commit()
    conn.close()

    out = tmp_path / "history.csv"
    export_communes_history_csv(db_path, out, derived_dir=tmp_path / "empty", all_periods=True)
    rows = _rows(out)
    assert not any(r["geo_id"] == "be:mun:SUCC" and r["period"] == "2020" for r in rows)


_AVG_NET_TAXABLE_INCOME_YAML = """
id: AVG_NET_TAXABLE_INCOME
name: {en: Average net taxable income per tax return, fr: x, nl: x}
unit: eur
frequency: A
geo_levels: [municipal]
preferred_direction: higher_is_better
derived:
  function: mean_from_total
  inputs: [FISCAL_TOT_NET_TAXABLE_INC, FISCAL_NBR_NON_ZERO_INC]
"""


def test_antwerp_keeps_its_own_value_gap_fill_not_restatement(tmp_path):
    """The maintainer's named example: Antwerp (11002) already publishes its
    own fiscal figures, and Borsbeek (11007, merged into it in 2025) must
    never change them -- gap-fill only, never a recombination of the two.

    Exercises the REAL derived config (AVG_NET_TAXABLE_INCOME, recomputed
    from FISCAL_TOT_NET_TAXABLE_INC / FISCAL_NBR_NON_ZERO_INC), because the
    ratio is exactly what FINDING 1 showed slipping past gap-fill: it is
    derived-only and never appears in `raw`, so a version of this test that
    points `derived_dir` at an empty directory never computes the ratio at
    all and cannot see the collision. `derived_dir=tmp_path / "empty"` was
    the bug in this test, not just in the code.
    """
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('FISCAL_TOT_NET_TAXABLE_INC', 'statbel', 'x', 'x',
                   'Total net taxable income', 'A', 'eur', 'higher_is_better', 1, 'x')""")
    conn.execute("""INSERT INTO indicators
           (indicator_id, source_id, name_nl, name_fr, name_en, frequency, unit,
            preferred_direction, is_additive, config_path)
           VALUES ('FISCAL_NBR_NON_ZERO_INC', 'statbel', 'x', 'x',
                   'Non-zero income returns', 'A', 'count', 'contextual', 1, 'x')""")
    _geo(conn, "be:mun:11002", "11002", "Antwerp")
    _geo(
        conn,
        "be:mun:11007",
        "11007",
        "Borsbeek",
        valid_to="2025-01-01",
        successor_geo_id="be:mun:11002",
    )
    # Antwerp's own real fiscal figures -- these two together give the
    # correct AVG_NET_TAXABLE_INCOME derived value for Antwerp alone.
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('FISCAL_TOT_NET_TAXABLE_INC', 'be:mun:11002', '2023', 'v1',
                   11097002409.83, 'final', '2023-01-01', '2023-12-31', 1, 1,
                   '2026-01-01T00:00:00+00:00')""")
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('FISCAL_NBR_NON_ZERO_INC', 'be:mun:11002', '2023', 'v1',
                   313772.0, 'final', '2023-01-01', '2023-12-31', 1, 1,
                   '2026-01-01T00:00:00+00:00')""")
    # Borsbeek's own real fiscal figures -- reconstruction would sum these
    # onto Antwerp if gap-fill did not block it.
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('FISCAL_TOT_NET_TAXABLE_INC', 'be:mun:11007', '2023', 'v1',
                   300000000.0, 'final', '2023-01-01', '2023-12-31', 1, 1,
                   '2026-01-01T00:00:00+00:00')""")
    conn.execute("""INSERT INTO observations
           (indicator_id, geo_id, period, vintage, value, status,
            period_start, period_end, is_latest, fetch_run_id, created_at)
           VALUES ('FISCAL_NBR_NON_ZERO_INC', 'be:mun:11007', '2023', 'v1',
                   9000.0, 'final', '2023-01-01', '2023-12-31', 1, 1,
                   '2026-01-01T00:00:00+00:00')""")
    conn.commit()
    conn.close()

    derived_dir = tmp_path / "derived"
    derived_dir.mkdir()
    (derived_dir / "AVG_NET_TAXABLE_INCOME.yaml").write_text(_AVG_NET_TAXABLE_INCOME_YAML)

    out = tmp_path / "history.csv"
    export_communes_history_csv(db_path, out, derived_dir=derived_dir, all_periods=True)
    rows = _rows(out)

    antwerp_2023_raw = [
        r
        for r in rows
        if r["geo_id"] == "be:mun:11002"
        and r["indicator_code"] == "FISCAL_TOT_NET_TAXABLE_INC"
        and r["period"] == "2023"
    ]
    assert len(antwerp_2023_raw) == 1
    assert float(antwerp_2023_raw[0]["value"]) == pytest.approx(11097002409.83)
    assert antwerp_2023_raw[0]["status"] == "A"  # its own real status, never reconstructed

    # FINDING 1: the derived ratio must ALSO stay singular and ALSO keep
    # Antwerp's own value -- 11097002409.83 / 313772 -- never the combined
    # (Antwerp + Borsbeek) sum-ratio a reconstruction would otherwise produce.
    antwerp_2023_ratio = [
        r
        for r in rows
        if r["geo_id"] == "be:mun:11002"
        and r["indicator_code"] == "AVG_NET_TAXABLE_INCOME"
        and r["period"] == "2023"
    ]
    assert len(antwerp_2023_ratio) == 1, (
        "Antwerp must have exactly one AVG_NET_TAXABLE_INCOME/2023 row, not a "
        "derived one plus a reconstructed one"
    )
    expected_own_ratio = 11097002409.83 / 313772.0
    assert float(antwerp_2023_ratio[0]["value"]) == pytest.approx(expected_own_ratio, rel=1e-9)
    assert antwerp_2023_ratio[0]["status"] == "derived"

    # And the general property FINDING 1 asked for: no duplicate
    # (geo_id, indicator_code, period) key anywhere in the export.
    keys = [(r["geo_id"], r["indicator_code"], r["period"]) for r in rows]
    duplicates = {k for k in keys if keys.count(k) > 1}
    assert not duplicates, f"duplicate (geo_id, indicator_code, period) rows: {duplicates}"


def test_the_database_file_is_byte_identical_after_a_reconstructing_export(
    merger_lineage_db, tmp_path
):
    """CLAUDE.md rule 6: a derived value is never written into observations
    as if it were source data. Reconstruction runs entirely in the query
    layer -- the database file itself must be bit-for-bit unchanged by an
    export that reconstructs several communes' history."""
    import hashlib

    def sha256(path):
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    before = sha256(merger_lineage_db)
    out = tmp_path / "history.csv"
    export_communes_history_csv(
        merger_lineage_db, out, derived_dir=tmp_path / "empty", all_periods=True
    )
    after = sha256(merger_lineage_db)
    assert before == after


def test_two_consecutive_builds_are_byte_identical(merger_lineage_db, tmp_path):
    """CLAUDE.md rule 35: identical inputs must keep producing byte-identical
    output. Reconstruction adds no nondeterminism (no timestamps, no random
    ordering) that a second run against the same database could disagree
    with."""
    out1 = tmp_path / "history1.csv"
    out2 = tmp_path / "history2.csv"
    export_communes_history_csv(
        merger_lineage_db, out1, derived_dir=tmp_path / "empty", all_periods=True
    )
    export_communes_history_csv(
        merger_lineage_db, out2, derived_dir=tmp_path / "empty", all_periods=True
    )
    assert out1.read_bytes() == out2.read_bytes()


def test_bastogne_style_lineage_is_selected_despite_a_2024_12_02_date(tmp_path):
    """The exact trap named in the handoff: a successor_geo_id row can carry
    valid_to = 2024-12-02, not 2025-01-01. The exporter must still reconstruct
    it -- there is no date-based wave filter anywhere in the query it runs."""
    db_path = tmp_path / "test.db"
    conn = _base_db(db_path)
    _geo(conn, "be:mun:82039", "82039", "Bastogne")
    _geo(
        conn,
        "be:mun:82003",
        "82003",
        "Bastenaken",
        valid_to="2024-12-02",
        successor_geo_id="be:mun:82039",
    )
    _geo(
        conn,
        "be:mun:82005",
        "82005",
        "Bertogne",
        valid_to="2024-12-02",
        successor_geo_id="be:mun:82039",
    )
    _pop(conn, "be:mun:82003", "2020", 100.0)
    _pop(conn, "be:mun:82005", "2020", 50.0)
    conn.commit()
    conn.close()

    out = tmp_path / "history.csv"
    export_communes_history_csv(db_path, out, derived_dir=tmp_path / "empty", all_periods=True)
    rows = _rows(out)
    bastogne_2020 = [r for r in rows if r["geo_id"] == "be:mun:82039" and r["period"] == "2020"]
    assert len(bastogne_2020) == 1
    assert float(bastogne_2020[0]["value"]) == pytest.approx(150.0)
