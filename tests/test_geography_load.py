import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography as load_mod  # noqa: E402

from src.db import migrate  # noqa: E402
from src.geography.resolve import (  # noqa: E402
    UnknownGeographyError,
    period_to_date,
    resolve_geo,
    resolve_to_current,
)

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"


@pytest.fixture
def loaded_db(tmp_path):
    """A migrated database with the real committed geography CSVs loaded.

    Uses the real config rather than a fixture so that the counts asserted below
    are counts of Belgium, not of a toy hierarchy.
    """
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    # The committed crosswalk still has rows awaiting the maintainer's [H]
    # sign-off, which the loader refuses by design; tests opt past that gate
    # deliberately rather than by weakening it.
    load_mod.load(db_path, REAL_CONFIG_DIR, allow_unverified=True)
    return db_path


def test_load_writes_expected_row_counts(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    counts = dict(conn.execute("SELECT level, COUNT(*) FROM geographies GROUP BY level"))
    assert counts["country"] == 1
    assert counts["region"] == 3
    # More than one row per code where an entity's territory changed while it
    # kept its code -- arrondissement 57000 was Tournai, then Tournai-Mouscron.
    assert counts["province"] == 11
    assert counts["arrondissement"] == 53
    # 565 current communes plus the 55 ended by the 2019 and 2025 waves.
    assert counts["municipality"] == 620
    current = dict(
        conn.execute(
            "SELECT level, COUNT(*) FROM geographies WHERE valid_to IS NULL GROUP BY level"
        )
    )
    assert current == {
        "country": 1,
        "region": 3,
        "province": 10,
        "arrondissement": 43,
        "municipality": 565,
    }


def test_every_current_commune_resolves_to_belgium(loaded_db):
    """No orphans and no cycles. An orphaned commune vanishes from regional
    aggregates silently, understating a region with no error anywhere."""
    conn = sqlite3.connect(str(loaded_db))
    reached = conn.execute("""
        WITH RECURSIVE up(geo_id, root, depth) AS (
            SELECT geo_id, geo_id, 0 FROM geographies
            WHERE level = 'municipality' AND valid_to IS NULL
            UNION ALL
            SELECT g.parent_geo_id, up.root, up.depth + 1
            FROM up JOIN geographies g ON g.geo_id = up.geo_id
            WHERE g.parent_geo_id IS NOT NULL AND up.depth < 10
        )
        SELECT COUNT(DISTINCT root) FROM up WHERE geo_id = 'be:country'
        """).fetchone()[0]
    total = conn.execute(
        "SELECT COUNT(*) FROM geographies WHERE level='municipality' AND valid_to IS NULL"
    ).fetchone()[0]
    assert reached == total == 565


def test_load_is_idempotent(loaded_db):
    """The loader may be re-run; a second pass must not duplicate or drop rows."""
    before = sqlite3.connect(str(loaded_db)).execute("SELECT COUNT(*) FROM geographies").fetchone()
    load_mod.load(loaded_db, REAL_CONFIG_DIR, allow_unverified=True)
    after = sqlite3.connect(str(loaded_db)).execute("SELECT COUNT(*) FROM geographies").fetchone()
    assert before == after


def test_load_preserves_the_country_row_observations_depend_on(loaded_db):
    """Every existing observation references be:country by foreign key, so the
    loader must upsert it rather than replace it."""
    conn = sqlite3.connect(str(loaded_db))
    row = conn.execute(
        "SELECT name_en, valid_from, nis_code FROM geographies WHERE geo_id = 'be:country'"
    ).fetchone()
    assert row == ("Belgium", "1830-01-01", "01000")
    assert conn.execute("SELECT COUNT(*) FROM pragma_foreign_key_check").fetchone()[0] == 0


def test_brussels_communes_parent_through_arrondissement_to_region(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    parent = conn.execute(
        "SELECT parent_geo_id FROM geographies WHERE geo_id = 'be:mun:21004'"
    ).fetchone()[0]
    assert parent == "be:arr:21000"
    grandparent = conn.execute(
        "SELECT parent_geo_id FROM geographies WHERE geo_id = 'be:arr:21000'"
    ).fetchone()[0]
    assert grandparent == "be:reg:04000"


def test_resolve_geo_is_period_aware(loaded_db):
    """The point of the whole block: a 2015 file about Kruibeke must resolve to
    Kruibeke, not to the commune that replaced it in 2025."""
    conn = sqlite3.connect(str(loaded_db))
    assert resolve_geo(conn, "46013", "2015") == "be:mun:46013"
    assert resolve_geo(conn, "46013", "2024-Q4") == "be:mun:46013"
    assert resolve_geo(conn, "46030", "2025-Q1") == "be:mun:46030"


def test_resolve_geo_raises_on_unknown_code(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    with pytest.raises(UnknownGeographyError, match="not present"):
        resolve_geo(conn, "99999", "2024")


def test_resolve_geo_raises_outside_the_validity_window(loaded_db):
    """Kruibeke ceased to exist in 2025; asking about it afterwards must fail
    rather than silently returning its successor."""
    conn = sqlite3.connect(str(loaded_db))
    with pytest.raises(UnknownGeographyError, match="no row covers"):
        resolve_geo(conn, "46013", "2026")


def test_successor_chain_walks_to_the_current_entity(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    assert resolve_to_current(conn, "be:mun:46013") == "be:mun:46030"
    assert resolve_to_current(conn, "be:mun:11001") == "be:mun:11001"


def test_successor_chain_detects_cycles(loaded_db):
    conn = sqlite3.connect(str(loaded_db))
    conn.execute(
        "UPDATE geographies SET successor_geo_id = 'be:mun:46013' WHERE geo_id = 'be:mun:46030'"
    )
    with pytest.raises(UnknownGeographyError, match="Cycle"):
        resolve_to_current(conn, "be:mun:46013")


@pytest.mark.parametrize(
    "period,expected",
    [
        ("2024", "2024-01-01"),
        ("2024-Q1", "2024-01-01"),
        ("2024-Q3", "2024-07-01"),
        ("2024-07", "2024-07-01"),
        ("2024-07-15", "2024-07-15"),
    ],
)
def test_period_to_date(period, expected):
    assert period_to_date(period) == expected


def test_period_to_date_rejects_unknown_shapes():
    with pytest.raises(ValueError, match="Unrecognized period format"):
        period_to_date("2024-7")


# --- Regression tests for the Block C adversarial audit findings ---------------


def test_historical_communes_are_not_orphaned_from_the_hierarchy(loaded_db):
    """Audit S1. Historical predecessors were first loaded with a NULL parent,
    so any aggregate rolling up through parent_geo_id silently dropped 55
    communes for pre-2025 periods -- a 2015 Limburg total was missing Hasselt,
    with no error anywhere."""
    conn = sqlite3.connect(str(loaded_db))
    for as_of, expected in [("2015-01-01", 589), ("2020-01-01", 581), ("2026-01-01", 565)]:
        valid, orphaned = conn.execute(
            """
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE parent_geo_id IS NULL)
            FROM geographies
            WHERE level = 'municipality'
              AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
            """,
            (as_of, as_of),
        ).fetchone()
        assert (valid, orphaned) == (expected, 0), f"at {as_of}"


def test_historical_commune_keeps_the_arrondissement_it_actually_had(loaded_db):
    """Kortessem sat in arrondissement 73000 and was merged into Hasselt, which
    is in 71000 -- so its historical parent must be 73000, not its successor's."""
    conn = sqlite3.connect(str(loaded_db))
    parent = conn.execute(
        "SELECT parent_geo_id FROM geographies WHERE nis_code = '73040'"
    ).fetchone()[0]
    assert parent.startswith("be:arr:73000")


def test_aggregates_do_not_resolve_before_they_existed(loaded_db):
    """Audit S2. Every arrondissement, province and region was dated to the
    structural epoch because the vintage diff only looked at commune rows."""
    conn = sqlite3.connect(str(loaded_db))
    # Arrondissement La Louviere was created by the 2019 Hainaut reform.
    with pytest.raises(UnknownGeographyError):
        resolve_geo(conn, "58000", "2015")
    assert resolve_geo(conn, "58000", "2020") == "be:arr:58000"
    # Arrondissement Mouscron ceased to exist in the same reform.
    with pytest.raises(UnknownGeographyError):
        resolve_geo(conn, "54000", "2020")


def test_arrondissement_that_changed_territory_has_separate_windows(loaded_db):
    """Audit S2, the subtle half: 57000 kept its code but went from Tournai
    (10 communes) to Tournai-Mouscron (12) in 2019. One row for both would
    attach 2015 Tournai figures to the larger territory."""
    conn = sqlite3.connect(str(loaded_db))
    early = resolve_geo(conn, "57000", "2010")
    late = resolve_geo(conn, "57000", "2020")
    assert early != late
    names = dict(
        conn.execute("SELECT geo_id, name_fr FROM geographies WHERE nis_code = '57000'").fetchall()
    )
    assert "Mouscron" not in names[early]
    assert "Mouscron" in names[late]


def test_loader_refuses_unverified_crosswalk_rows(tmp_path):
    """Audit S3. The derivation flags rows whose successor was guessed; the
    loader used to write them anyway, making the flags decorative.

    The committed crosswalk is now fully signed off, so this drives the gate
    with a row whose sign-off has been withdrawn.
    """
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    for name in ("geographies.csv", "municipality_crosswalk.csv", "merger_effective_dates.csv"):
        (config_dir / name).write_text((REAL_CONFIG_DIR / name).read_text(encoding="utf-8"))
    path = config_dir / "municipality_crosswalk.csv"
    path.write_text(path.read_text(encoding="utf-8").replace(",true,", ",false,"), encoding="utf-8")

    db_path = tmp_path / "gate.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    with pytest.raises(load_mod.UnverifiedCrosswalkError) as excinfo:
        load_mod.load(db_path, config_dir)
    message = str(excinfo.value)
    # Exactly the two whose successor was guessed -- the gate must name them,
    # and must not have drifted back to flagging the 1977 partial-transfer rows.
    assert "73009" in message and "73083" in message
    assert "52063" not in message


def test_committed_crosswalk_now_loads_without_an_override(tmp_path):
    """The gate is satisfied by real sign-offs, not by --allow-unverified."""
    db_path = tmp_path / "signed.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    rows, links = load_mod.load(db_path, REAL_CONFIG_DIR)
    assert (rows, links) == (688, 55)


def test_bastogne_merger_resolves_on_its_official_date(tmp_path):
    """Bastogne and Bertogne merged on 2024-12-02, a month before the REFNIS
    snapshot that first shows it. Without the official date the whole of
    December 2024 would resolve to the merged commune.

    Note the mid-month boundary: a period resolves as of its *first day*, so
    December 2024 -- which begins one day before the merger -- resolves to the
    predecessors. That is the documented convention, not an accident; see
    docs/features/geography.md.
    """
    db_path = tmp_path / "bastogne.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_mod.load(db_path, REAL_CONFIG_DIR)
    conn = sqlite3.connect(str(db_path))

    assert resolve_geo(conn, "82003", "2024-11") == "be:mun:82003"
    assert resolve_geo(conn, "82003", "2024-12") == "be:mun:82003"
    # From 2 December the old code refers to nothing -- it is not silently
    # forwarded to the successor, which has its own code.
    assert resolve_geo(conn, "82039", "2024-12-15") == "be:mun:82039"
    with pytest.raises(UnknownGeographyError):
        resolve_geo(conn, "82003", "2024-12-15")
    assert resolve_geo(conn, "82039", "2025-01") == "be:mun:82039"
    with pytest.raises(UnknownGeographyError):
        resolve_geo(conn, "82003", "2025-01")


def test_loader_rejects_a_blank_valid_from(tmp_path):
    """Audit S8. An empty string satisfies NOT NULL and sorts before every
    date, making the entity valid for all of recorded history."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    for name in ("geographies.csv", "municipality_crosswalk.csv", "name_en_exonyms.csv"):
        (config_dir / name).write_text((REAL_CONFIG_DIR / name).read_text(encoding="utf-8"))
    rows = (config_dir / "geographies.csv").read_text(encoding="utf-8").splitlines()
    header, first = rows[0], rows[1].split(",")
    first[header.split(",").index("valid_from")] = ""
    (config_dir / "geographies.csv").write_text(
        "\n".join([header, ",".join(first)] + rows[2:]), encoding="utf-8"
    )

    db_path = tmp_path / "blank.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    with pytest.raises(ValueError, match="not a YYYY-MM-DD date"):
        load_mod.load(db_path, config_dir, allow_unverified=True)


def test_resolve_to_current_refuses_a_dead_end(loaded_db):
    """Audit S7. A commune that ended with no recorded successor reported
    itself as current, because only successor_geo_id was checked."""
    conn = sqlite3.connect(str(loaded_db))
    conn.execute("UPDATE geographies SET successor_geo_id = NULL WHERE nis_code = '46013'")
    with pytest.raises(UnknownGeographyError, match="ceased to exist"):
        resolve_to_current(conn, "be:mun:46013")


def test_german_speaking_communes_keep_their_own_name(loaded_db):
    """Audit S10. name_en fell back to Dutch for the nine German-regime
    communes because name_de was never threaded through."""
    conn = sqlite3.connect(str(loaded_db))
    name_en, name_fr = conn.execute(
        "SELECT name_en, name_fr FROM geographies WHERE nis_code = '63040'"
    ).fetchone()
    assert name_en == "Kelmis"
    assert name_fr == "La Calamine"
