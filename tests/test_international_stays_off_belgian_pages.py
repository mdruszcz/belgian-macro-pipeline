"""International pilot PR 1: foreign geographies must never reach a Belgian
published file. Run against the real assembled working database (43
allowlisted international geographies actually loaded, 26,729 real observed
rows from the cutover) -- not a synthetic fixture, because the historical bug
this guards (docs' own account: "the explorer published Germany's GDP
labelled be:country") was found on real data, not a unit test.
"""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from export_canonical_csv import export_canonical_csv  # noqa: E402
from export_site_payloads import _build_geographies, _read_national  # noqa: E402

from src.geography.international import load_international_rows  # noqa: E402
from src.validation.config_schema import is_multi_geo, load_and_validate_all  # noqa: E402

PILOT_INDICATORS = (
    "GDP_VOLUME_EUROPE",
    "HICP_ANNUAL_RATE_EUROPE",
    "UNEMPLOYMENT_RATE_EUROPE",
    "GOV_DEBT_EUROPE",
    "CONSUMER_CONFIDENCE_EUROPE",
    # Europe countries batch (docs/features/europe_countries.md, added
    # 2026-09-15): two more multi-geo indicators in the same `international`
    # store, subject to the exact same Belgian-page exclusion -- extended
    # here rather than duplicated in a sibling file, since every test below
    # already iterates this tuple generically.
    "GDP_PC_PPS_COUNTRY",
    "POPULATION_COUNTRY",
)


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


def test_every_configured_pilot_indicator_is_multi_geo():
    """Precondition for the two tests below: if PILOT_INDICATORS drifted from
    what is_multi_geo() actually flags, they would pass for the wrong reason."""
    indicators, _sources = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    for indicator_id in PILOT_INDICATORS:
        assert is_multi_geo(indicators[indicator_id]), indicator_id


def test_the_canonical_csv_export_carries_no_multi_geo_indicator(tmp_path, working_db):
    """BLOCKER 2 (audit): a multi-geo pilot indicator's own be:country row
    (Belgium is one of the countries it fetches) used to leak into this
    Belgium-only export beside the pre-existing national indicator for the
    same concept -- 755 extra rows, e.g. two different consumer-confidence
    figures for the same month. Reads the REAL exported file (not just the
    database) so a regression in export_canonical_csv.py's own query, not
    only in the database, would be caught here."""
    out = tmp_path / "canonical.csv"
    n = export_canonical_csv(working_db, out)
    assert n > 0

    with out.open(encoding="utf-8", newline="") as fh:
        exported_ids = {row["indicator_code"] for row in csv.DictReader(fh)}

    leaked = exported_ids & set(PILOT_INDICATORS)
    assert leaked == set(), f"multi-geo indicator(s) leaked into the Belgian export: {leaked}"

    # And the eight migrated single-country indicators are still there --
    # this must exclude the pilot, not every Eurostat-sourced indicator.
    assert "EUROSTAT_GDP_Q_MEUR" in exported_ids
    assert "EC_CONS_CONF_BE" in exported_ids


def test_national_json_carries_no_multi_geo_indicator(tmp_path, working_db):
    """The same guard one layer up: export_site_payloads.py's national.json
    is built by re-reading the exported CSV (_read_national), so this proves
    the fix reaches the actually-published payload, not just the CSV."""
    out = tmp_path / "canonical.csv"
    export_canonical_csv(working_db, out)

    national = _read_national(out)

    leaked = set(national) & set(PILOT_INDICATORS)
    assert leaked == set(), f"multi-geo indicator(s) leaked into national.json: {leaked}"
    assert "EUROSTAT_GDP_Q_MEUR" in national


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
