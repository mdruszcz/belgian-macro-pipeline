"""Grade and source: how every published figure says what it is.

Three axes are kept apart on purpose (docs/features/provenance.md) -- grade is
"how was this made", status is "how sure is the source", freshness is "when did
we fetch it". These tests pin the grade rules and the refusals, because a badge
that quietly grades a rescaled number as the agency's own figure is worse than
no badge at all.
"""

import json
import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.exporters.provenance import (  # noqa: E402
    DB_TO_CONFIG_SOURCE_ID,
    GRADES,
    REBASED_UNIT,
    indicator_lineage,
    source_registry,
)

LANGS = ("en", "fr", "nl")


# --- the vocabulary --------------------------------------------------------


def test_the_four_grades_are_defined_in_all_three_languages():
    """Rule 7. These words appear beside figures, and a page falling back to
    English for one of them is the half-translation the rule exists to stop."""
    assert sorted(GRADES) == ["A", "B", "C", "D"]
    for letter, grade in GRADES.items():
        for lang in LANGS:
            assert grade.get(lang), f"grade {letter} has no {lang} label"
            assert grade["definition"].get(lang), f"grade {letter} has no {lang} definition"


# --- the registry ----------------------------------------------------------


def test_the_registry_is_keyed_by_the_id_the_indicators_table_uses():
    """The config files and the database disagree about two source ids --
    `dbnomics_eurostat` vs `eurostat`, `dbnomics_ameco` vs `ameco_ec`. The
    lineage joins on the database's id, so the registry has to answer to that
    one or two national indicators would publish with no attributable source.
    """
    registry = source_registry()
    for db_id in DB_TO_CONFIG_SOURCE_ID:
        assert db_id in registry, f"{db_id} is not resolvable"
    assert "dbnomics_eurostat" not in registry, "the config id leaked into the registry"


def test_every_municipal_source_carries_a_trilingual_label_and_licence_note():
    """These are the sources whose figures appear on a commune page, and
    each grant or reuse notice is published per source and never
    composed into one string -- Statbel and ONEM state commercial reuse, the
    federal police state only attribution."""
    registry = source_registry()
    for source_id in ("statbel", "onem", "police", "steunpunt_werk"):
        entry = registry[source_id]
        for lang in LANGS:
            assert entry["label"].get(lang), f"{source_id} has no {lang} label"
            assert entry["licence_note"].get(lang), f"{source_id} has no {lang} licence note"
        assert entry["homepage"], f"{source_id} has no homepage to attribute to"

    # The distinction that must not blur.
    assert "not" in registry["police"]["licence_note"]["en"].lower()
    assert "commercial" in registry["statbel"]["licence_note"]["en"].lower()


def test_a_source_without_a_translated_label_still_has_a_usable_one():
    """The three national sources have no short label yet. They fall back to
    the agency name rather than rendering blank, so adding a source never
    requires translation work before it can be attributed."""
    registry = source_registry()
    for source_id in ("nbb", "eurostat", "ameco_ec"):
        entry = registry[source_id]
        for lang in LANGS:
            assert entry["label"][lang] == entry["agency"]


# --- the grade rules -------------------------------------------------------


@pytest.fixture(scope="module")
def lineage():
    db = REPO / "data" / "belgian_macro.db"
    if not db.is_file():
        pytest.skip("database not built in this working tree")
    return indicator_lineage(db)


def test_a_stored_indicator_is_official_and_names_its_agency(lineage):
    entry = lineage["POPULATION_BY_COMMUNE"]
    assert entry["grade"] == "A"
    assert entry["source"] == "statbel"
    assert entry.get("derived_from") is None


def test_a_derived_indicator_is_graded_c_and_names_no_source(lineage):
    """Attributing a computed figure to one of its inputs' agencies would say
    that agency published this number. It did not. What travels instead is the
    input list, so a page can say "computed from Statbel figures" without
    claiming Statbel published it."""
    entry = lineage["AVG_NET_TAXABLE_INCOME"]
    assert entry["grade"] == "C"
    assert entry["source"] is None
    assert entry["derived_from"] == ["FISCAL_TOT_NET_TAXABLE_INC", "FISCAL_NBR_NON_ZERO_INC"]
    assert entry["input_sources"] == ["statbel"]


def test_a_rescaled_indicator_is_restated_not_official(lineage):
    """The two loaded index_2010 series are rebased by
    EurostatFetcher._rebase_to_2010 -- this pipeline changed the number, so it
    is not the agency's own published figure."""
    restated = sorted(k for k, v in lineage.items() if v["grade"] == "B")
    assert restated == ["EUROSTAT_GDP_Q_MEUR", "LABOUR_COST_BE"]
    assert lineage["LABOUR_COST_BE"]["transform"] == "rebase"


def test_index_units_the_source_publishes_itself_stay_official(lineage):
    """INDUSTRIAL_PROD is index_2021 as NBB publishes it. Grading off the unit
    string would have demoted it; grade B comes from a declared transform."""
    assert lineage["INDUSTRIAL_PROD"]["grade"] == "A"
    assert lineage["INDUSTRIAL_PROD"]["transform"] is None


def test_every_index_2010_config_declares_its_transform():
    """Eleven configs carry the rebased unit and only two are loaded. The
    fetcher rebases on the unit alone, so the other nine are one load away
    from silently grading themselves as the agency's own figure."""
    undeclared = []
    for path in sorted((REPO / "config" / "indicators").glob("*.yaml")):
        cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if cfg.get("unit") == REBASED_UNIT and not cfg.get("transform"):
            undeclared.append(path.name)
    assert not undeclared, f"rebased but undeclared: {undeclared}"


def test_the_forecast_grade_is_empty_and_this_test_says_when_that_changes(lineage):
    """Grade D is defined and ships with nothing in it. FPB forecasts stay on
    the legacy table, outside the canonical model, so `fpb` owns zero rows in
    `indicators`. When that stops being true this test fails, which is the
    signal that the D badge now has a live case to render and check."""
    forecasts = sorted(k for k, v in lineage.items() if v["grade"] == "D")
    assert forecasts == [], (
        "grade D now has members: " + ", ".join(forecasts) + ". Forecasts have entered the "
        "canonical model, so the D badge needs rendering and a real test rather than this one."
    )


def test_an_undeclared_rebase_stops_the_build(tmp_path):
    """Rule 13. A config carrying the rebased unit without declaring the
    transform would publish a rescaled number as the agency's own figure."""
    db = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE indicators (indicator_id TEXT, source_id TEXT)")
    conn.execute("INSERT INTO indicators VALUES ('SNEAKY', 'statbel')")
    conn.commit()
    conn.close()

    indicators = tmp_path / "indicators"
    indicators.mkdir()
    (indicators / "SNEAKY.yaml").write_text(f"id: SNEAKY\nunit: {REBASED_UNIT}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="declares no `transform:` block"):
        indicator_lineage(db, indicators_dir=indicators, derived_dir=tmp_path / "none")


def test_an_unattributable_source_stops_the_build(tmp_path):
    """A figure whose source no config describes cannot be credited, and a
    licence notice that cannot name the producer is not a licence notice."""
    db = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE indicators (indicator_id TEXT, source_id TEXT)")
    conn.execute("INSERT INTO indicators VALUES ('ORPHAN', 'nowhere')")
    conn.commit()
    conn.close()

    with pytest.raises(ValueError, match="which no config"):
        indicator_lineage(db, indicators_dir=tmp_path / "none", derived_dir=tmp_path / "none")


# --- the published artifact ------------------------------------------------


@pytest.fixture(scope="module")
def published():
    index = REPO / "public" / "data" / "metadata" / "indicators.json"
    sources = REPO / "public" / "data" / "metadata" / "sources.json"
    if not index.is_file() or not sources.is_file():
        pytest.skip("payloads not built in this working tree")
    return (
        {
            row["indicator_code"]: row
            for row in json.loads(index.read_text(encoding="utf-8"))["indicators"]
        },
        json.loads(sources.read_text(encoding="utf-8")),
    )


def test_every_published_indicator_carries_a_resolvable_badge(published):
    """Run against the REAL payloads, so this grows by itself as indicators are
    added rather than pinning today's list."""
    index, registry = published
    sources = registry["sources"]
    assert index, "the index is empty, so this test would prove nothing"
    for code, row in index.items():
        assert row["grade"] in GRADES, f"{code} has grade {row['grade']!r}"
        if row["grade"] in ("A", "B"):
            assert row["source"] in sources, f"{code} names unknown source {row['source']!r}"
        else:
            assert row["source"] is None, f"{code} is grade {row['grade']} but names a source"


def test_no_indicator_claims_both_its_own_date_and_its_inputs_date(published):
    """The two are mutually exclusive by the licence reasoning: a figure was
    either fetched or computed. Carrying both would state that a computed
    figure was also retrieved on a date, which is the misstatement point 5 of
    Statbel's 2015 licence forbids."""
    index, _ = published
    both = [c for c, r in index.items() if r.get("updated") and r.get("inputs_updated")]
    assert both == [], f"carrying two contradictory dates: {both}"


def test_every_derived_indicator_publishes_its_inputs_and_their_date(published):
    """The answer to the question docs/data_catalog.md left open: a derived
    figure must expose the retrieval date of the information it reuses."""
    index, _ = published
    derived = {c: r for c, r in index.items() if r["grade"] == "C"}
    assert derived, "no derived indicators published, so this proves nothing"
    for code, row in derived.items():
        assert row["derived_from"], f"{code} is derived but names no inputs"
        for parent in row["derived_from"]:
            assert parent in index, f"{code} names input {parent}, which is not published"
        assert row["input_sources"], f"{code} names no source for its inputs"
        assert row["inputs_updated"], f"{code} exposes no date for its inputs"


def test_the_derived_date_is_the_newest_of_its_inputs(published):
    """Hand-computed (rule 5): average income is total income over the count of
    returns, both Statbel, both retrieved on the same day."""
    index, _ = published
    row = index["AVG_NET_TAXABLE_INCOME"]
    inputs = ["FISCAL_TOT_NET_TAXABLE_INC", "FISCAL_NBR_NON_ZERO_INC"]
    assert row["derived_from"] == inputs
    assert row["inputs_updated"] == max(index[i]["updated"] for i in inputs)
    assert row["updated"] is None, "a computed figure must carry no retrieval date of its own"


def test_every_source_config_resolves_to_a_row_in_the_sources_table():
    """The guard for the mismatch that has now bitten twice.

    `dbnomics_eurostat` vs `eurostat` and `dbnomics_ameco` vs `ameco_ec` broke
    the provenance join first, and then broke the fetch-silence rule, which
    reported both as "never fetched" while they were being fetched daily. Any
    third mismatch fails here instead of surfacing as a wrong badge or a false
    alarm months later.
    """
    db = REPO / "data" / "belgian_macro.db"
    if not db.is_file():
        pytest.skip("database not built in this working tree")
    conn = sqlite3.connect(db)
    try:
        db_ids = {row[0] for row in conn.execute("SELECT source_id FROM sources")}
    finally:
        conn.close()

    unresolved = []
    for path in sorted((REPO / "config" / "sources").glob("*.yaml")):
        config_id = (yaml.safe_load(path.read_text(encoding="utf-8")) or {})["source_id"]
        resolved = {v: k for k, v in DB_TO_CONFIG_SOURCE_ID.items()}.get(config_id, config_id)
        if resolved not in db_ids:
            unresolved.append(f"{path.name}: {config_id} -> {resolved}")
    assert not unresolved, (
        "source configs that match no row in the sources table: "
        + "; ".join(unresolved)
        + ". Either the id is wrong or DB_TO_CONFIG_SOURCE_ID needs an entry."
    )
