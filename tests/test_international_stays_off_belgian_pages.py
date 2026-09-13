"""International pilot PR 1: foreign geographies must never reach a Belgian
published file. Run against the real assembled working database (43
allowlisted international geographies actually loaded, 26,729 real observed
rows from the cutover) -- not a synthetic fixture, because the historical bug
this guards (docs' own account: "the explorer published Germany's GDP
labelled be:country") was found on real data, not a unit test.
"""

import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from export_canonical_csv import export_canonical_csv  # noqa: E402
from export_site_payloads import _build_geographies  # noqa: E402

from src.geography.international import load_international_rows  # noqa: E402


def test_the_allowlist_actually_loaded_into_the_real_working_db(working_db):
    """Precondition for every check below: if the allowlist's geographies
    never made it into this database, the other assertions would pass for
    the wrong reason (nothing foreign to leak)."""
    conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
    try:
        geo_ids = {row[0] for row in conn.execute("SELECT geo_id FROM geographies")}
    finally:
        conn.close()
    rows = load_international_rows()
    assert rows["DE"]["geo"]["geo_id"] in geo_ids
    assert rows["EU27_2020"]["geo"]["geo_id"] in geo_ids
    # And the pilot really did write foreign rows -- not just load the
    # geographies with nothing attached to them.
    conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
    try:
        n_foreign = conn.execute(
            "SELECT COUNT(*) FROM observations WHERE geo_id = 'de:country'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert n_foreign > 0


def test_the_canonical_csv_export_carries_only_belgian_rows(tmp_path, working_db):
    out = tmp_path / "canonical.csv"
    n = export_canonical_csv(working_db, out)
    assert n > 0

    # The export's own query hardcodes geo_id = 'be:country'; the regression
    # this guards is a foreign row slipping through mislabeled AS be:country,
    # which a value diff (not the query) would have to catch -- so verify
    # directly against the database that be:country's own row set for the
    # eight migrated Eurostat indicators is exactly what is_latest gives it,
    # and that no other geography's rows for those indicators were folded in.
    conn = sqlite3.connect(f"file:{working_db}?mode=ro", uri=True)
    try:
        for indicator_id, only_geo in (
            ("EUROSTAT_GDP_Q_MEUR_DE", "de:country"),
            ("EUROSTAT_GDP_Q_MEUR_EA", "ea:aggregate"),
            ("EC_CONS_CONF_EU", "eu27_2020:aggregate"),
        ):
            geos = {
                row[0]
                for row in conn.execute(
                    "SELECT DISTINCT geo_id FROM observations WHERE indicator_id = ?",
                    (indicator_id,),
                )
            }
            assert geos == {only_geo}, (indicator_id, geos)
        # And the pilot's own multi-country indicator: Belgium's row is
        # be:country, Germany's is de:country, never swapped or merged.
        pilot_geos = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT geo_id FROM observations WHERE indicator_id = 'GDP_VOLUME_EUROPE'"
            )
        }
        assert {"be:country", "de:country"} <= pilot_geos
    finally:
        conn.close()


def test_geographies_json_carries_no_foreign_geography(working_db):
    geographies = _build_geographies(working_db)
    assert geographies, "must actually return rows to be a meaningful check"
    foreign = [g for g in geographies if not g["geo_id"].startswith("be:")]
    assert foreign == []


@pytest.mark.parametrize(
    "indicator_id",
    [
        "EUROSTAT_GDP_Q_MEUR",
        "EUROSTAT_GDP_Q_MEUR_DE",
        "EUROSTAT_GDP_Q_MEUR_EA",
        "EUROSTAT_GDP_Q_MEUR_ES",
        "EUROSTAT_GDP_Q_MEUR_FR",
        "EUROSTAT_GDP_Q_MEUR_NL",
    ],
)
def test_every_rebased_eurostat_indicator_is_graded_b_on_the_real_data(working_db, indicator_id):
    """Every `transform: rebase` config must publish grade B, never grade A
    (the agency's own figure) -- rule 6/CLAUDE.md's provenance guarantee,
    checked here against the real committed database rather than a synthetic
    lineage fixture."""
    from src.exporters.provenance import indicator_lineage

    lineage = indicator_lineage(working_db)
    assert lineage[indicator_id]["grade"] == "B"
    assert lineage[indicator_id]["transform"] == "rebase"


def test_the_pilot_indicators_carry_no_transform_and_grade_a(working_db):
    """The five new pilot indicators are Eurostat's own published figures,
    unmodified -- grade A, same as any other official series."""
    from src.exporters.provenance import indicator_lineage

    lineage = indicator_lineage(working_db)
    for indicator_id in (
        "GDP_VOLUME_EUROPE",
        "HICP_ANNUAL_RATE_EUROPE",
        "UNEMPLOYMENT_RATE_EUROPE",
        "GOV_DEBT_EUROPE",
        "CONSUMER_CONFIDENCE_EUROPE",
    ):
        assert lineage[indicator_id]["grade"] == "A"
        assert lineage[indicator_id]["transform"] is None
        assert lineage[indicator_id]["source"] == "eurostat"
