import csv
from pathlib import Path

from src.geography.crosswalk import (
    classify,
    derive_crosswalk,
    successor_candidates_from_nis6,
    unresolved,
)

CROSSWALK_CSV = (
    Path(__file__).resolve().parents[1] / "config" / "geography" / "municipality_crosswalk.csv"
)


def _nis6(nis6, commune, name="SOMEWHERE"):
    return {"nis6": nis6, "commune_code": commune, "nis6_name": name}


def _vintage(filename, communes):
    return (filename, {code: {"name_nl": name, "name_fr": name} for code, name in communes.items()})


def test_prefix_rule_finds_former_commune_codes():
    rows = [
        _nis6("46013A", "46030"),  # Kruibeke's old code preserved as prefix
        _nis6("46030A", "46030"),  # same-prefix row is not lineage
    ]
    assert successor_candidates_from_nis6(rows) == {"46013": {"46030"}}


def test_classify_distinguishes_the_three_relationships():
    names = {"12041": "Puurs-Sint-Amands", "58001": "La Louvière", "11002": "Antwerpen"}
    # Two predecessors formed one new entity.
    assert classify("Puurs", {"12041"}, names, predecessor_count=2) == "merged"
    # Sole predecessor, name unchanged -- the 2019 Hainaut arrondissement reform.
    assert classify("La Louvière", {"58001"}, names, predecessor_count=1) == "recoded"
    # Sole predecessor, different name -- taken into an existing commune.
    assert classify("Borsbeek", {"11002"}, names, predecessor_count=1) == "absorbed"


def test_vintage_diff_is_authoritative_where_the_prefix_rule_is_blind():
    """Regression test for the Tongeren/Borgloon case.

    Their NIS6 codes were renumbered under the new commune, so the prefix rule
    sees no lineage at all. If the diff did not drive membership, both communes'
    entire pre-2025 history would be silently orphaned.
    """
    vintages = [
        _vintage(
            "REFNIS_2019.csv", {"73009": "Borgloon", "73083": "Tongeren", "11001": "Aartselaar"}
        ),
        _vintage("REFNIS_2025.csv", {"73111": "Tongeren-Borgloon", "11001": "Aartselaar"}),
    ]
    # Deliberately no lineage-bearing NIS6 rows: the prefix rule finds nothing.
    nis9 = [_nis6("73111A", "73111")]
    rows = derive_crosswalk(vintages, nis9, {"73111": "Tongeren-Borgloon"})

    by_old = {r["old_nis"]: r for r in rows}
    assert set(by_old) == {"73009", "73083"}
    # Name matching recovered the successor, and both rows are flagged.
    assert by_old["73009"]["new_nis"] == "73111"
    assert by_old["73083"]["new_nis"] == "73111"
    assert all(r["note"] for r in rows)
    assert unresolved(rows) == rows


def test_predecessor_validity_window_is_not_degenerate():
    """`geographies` enforces valid_to > valid_from; equal dates would be
    rejected at load time."""
    vintages = [
        _vintage("REFNIS_DEFINITIEF.csv", {"46013": "Kruibeke"}),
        _vintage("REFNIS_2019.csv", {"46013": "Kruibeke"}),
        _vintage("REFNIS_2025.csv", {}),
    ]
    rows = derive_crosswalk(
        vintages,
        [_nis6("46013A", "46030")],
        {"46030": "Beveren-Kruibeke-Zwijndrecht"},
        valid_from_by_code={"46013": "1977-01-01"},
    )
    assert len(rows) == 1
    assert rows[0]["valid_from"] == "1977-01-01"
    assert rows[0]["valid_to"] == "2025-01-01"
    assert rows[0]["valid_to"] > rows[0]["valid_from"]


def test_partial_transfer_is_a_flag_not_a_relationship():
    """A boundary transfer is orthogonal to what happened to the commune, so it
    must not overwrite `recoded`/`merged`."""
    vintages = [
        _vintage("REFNIS_DEFINITIEF.csv", {"55022": "La Louvière"}),
        _vintage("REFNIS_2019.csv", {"58001": "La Louvière"}),
    ]
    nis9 = [
        _nis6("55022A", "58001", name="LA LOUVIERE"),
        _nis6("55022B", "58001", name="PARTIE DE FAMILLEUREUX"),
    ]
    rows = derive_crosswalk(vintages, nis9, {"58001": "La Louvière"})
    assert len(rows) == 1
    assert rows[0]["relationship"] == "recoded"
    assert rows[0]["has_partial_transfer"] == "true"


def test_committed_crosswalk_covers_both_waves():
    with CROSSWALK_CSV.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 55
    waves: dict[str, int] = {}
    for row in rows:
        waves[row["valid_to"]] = waves.get(row["valid_to"], 0) + 1
    assert waves == {"2019-01-01": 26, "2025-01-01": 29}
    # Every row must have a validity window the schema will accept.
    assert all(r["valid_to"] > r["valid_from"] for r in rows)
    # Nothing is silently marked verified.
    assert {r["verified"] for r in rows} == {"false"}


def test_committed_crosswalk_flags_only_the_genuinely_ambiguous_rows():
    with CROSSWALK_CSV.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    flagged = {r["old_nis"] for r in unresolved(rows)}
    # Only the two whose successor came from name matching rather than code
    # lineage. The five 1977-era partial-transfer markers no longer gate.
    assert flagged == {"73009", "73083"}


def test_name_comparison_ignores_apostrophe_style():
    """Statbel writes "Arrondissement d'Anvers" with a straight quote in one
    vintage and a typographic one in another. Comparing raw strings would split
    the entity's validity window on punctuation alone."""
    from src.geography.crosswalk import _normalize

    assert _normalize("Arrondissement d'Anvers") == _normalize("Arrondissement d’Anvers")


def test_entity_windows_cover_aggregates_not_just_communes():
    """Audit S2: only commune rows were diffed, so every aggregate silently
    inherited the structural epoch and resolved for periods before it existed."""
    from src.geography.crosswalk import derive_entity_windows

    def entity(level, parent, name, children=()):
        return {
            "level": level,
            "parent_nis": parent,
            "name_fr": name,
            "name_nl": name,
            "children": tuple(children),
        }

    old = {
        "01000": entity("country", None, "ROYAUME", ["54000"]),
        "54000": entity("arrondissement", "01000", "Mouscron", ["54007"]),
        "54007": entity("municipality", "54000", "Mouscron"),
    }
    new = {
        "01000": entity("country", None, "ROYAUME", ["58000"]),
        "58000": entity("arrondissement", "01000", "La Louvière", ["58001"]),
        "58001": entity("municipality", "58000", "La Louvière"),
    }
    windows = derive_entity_windows([("REFNIS_DEFINITIEF.csv", old), ("REFNIS_2019.csv", new)])
    assert windows["54000"][0]["valid_to"] == "2019-01-01"
    assert windows["58000"][0]["valid_from"] == "2019-01-01"
    assert windows["58000"][0]["valid_to"] is None


def test_internal_merger_does_not_split_the_parent_window():
    """Communes merging *inside* an arrondissement leave its territory
    unchanged, so it must not be treated as a new entity."""
    from src.geography.crosswalk import canonical_code_map, derive_entity_windows

    def entity(level, parent, name, children=()):
        return {
            "level": level,
            "parent_nis": parent,
            "name_fr": name,
            "name_nl": name,
            "children": tuple(children),
        }

    old = {
        "46000": entity("arrondissement", None, "Saint-Nicolas", ["46003", "46013"]),
        "46003": entity("municipality", "46000", "Beveren"),
        "46013": entity("municipality", "46000", "Kruibeke"),
    }
    new = {
        "46000": entity("arrondissement", None, "Saint-Nicolas", ["46030"]),
        "46030": entity("municipality", "46000", "Beveren-Kruibeke"),
    }
    canonical = canonical_code_map(
        [
            {"old_nis": "46003", "new_nis": "46030"},
            {"old_nis": "46013", "new_nis": "46030"},
        ]
    )
    windows = derive_entity_windows([("REFNIS_2019.csv", old), ("REFNIS_2025.csv", new)], canonical)
    assert len(windows["46000"]) == 1
    assert windows["46000"][0]["valid_to"] is None


def test_partial_marker_does_not_gate_the_review():
    """The `*` / `PARTIE DE` markers annotate the 1977 merger, not the waves this
    crosswalk records, and roughly 200 of them exist nationwide. Gating on them
    flagged five lineages that were never in doubt; a review gate that cries
    wolf gets ignored."""
    vintages = [
        _vintage("REFNIS_DEFINITIEF.csv", {"82003": "Bastenaken", "82005": "Bertogne"}),
        _vintage("REFNIS_2019.csv", {"82039": "Bastenaken"}),
    ]
    nis9 = [
        _nis6("82003A", "82039", name="BASTOGNE + PARTIE DE LONGCHAMPS ET SIBRET"),
        _nis6("82005B", "82039", name="LONGCHAMPS*"),
    ]
    rows = derive_crosswalk(vintages, nis9, {"82039": "Bastenaken"})
    # Recorded as context...
    assert {r["has_partial_transfer"] for r in rows} == {"true"}
    # ...but the lineage is unambiguous, so nothing is asked of the maintainer.
    assert unresolved(rows) == []


def test_territory_leaving_for_another_commune_is_still_caught():
    """The case the partial flag was meant to cover is already covered: land
    that went elsewhere shows up as a second successor."""
    vintages = [
        _vintage("REFNIS_DEFINITIEF.csv", {"52063": "Seneffe"}),
        _vintage("REFNIS_2019.csv", {"55085": "Seneffe", "58001": "La Louvière"}),
    ]
    nis9 = [
        _nis6("52063A", "55085", name="SENEFFE"),
        _nis6("52063E", "58001", name="FAMILLEUREUX*"),
    ]
    rows = derive_crosswalk(vintages, nis9, {"55085": "Seneffe", "58001": "La Louvière"})
    assert rows[0]["new_nis"] == "55085;58001"
    assert unresolved(rows) == rows


def test_signoff_survives_regeneration_but_only_for_the_same_claim():
    """Regeneration must not silently discard the maintainer's [H] verification
    -- but a sign-off attaches to the claim that was checked, so a changed
    successor or wave correctly asks for another look."""
    vintages = [
        _vintage("REFNIS_DEFINITIEF.csv", {"46013": "Kruibeke"}),
        _vintage("REFNIS_2019.csv", {"46030": "Beveren-Kruibeke"}),
    ]
    nis9 = [_nis6("46013A", "46030")]
    names = {"46030": "Beveren-Kruibeke"}

    kept = derive_crosswalk(
        vintages, nis9, names, previously_verified={("46013", "46030", "2019-01-01")}
    )
    assert kept[0]["verified"] == "true"

    # Same predecessor, but the sign-off was recorded against a different
    # successor -- it must not carry over.
    reset = derive_crosswalk(
        vintages, nis9, names, previously_verified={("46013", "99999", "2019-01-01")}
    )
    assert reset[0]["verified"] == "false"


def test_committed_crosswalk_is_not_silently_pre_verified():
    """Nothing in the repo may claim a verification that was not performed."""
    with CROSSWALK_CSV.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert {r["verified"] for r in rows} == {"false"}
    assert {r["old_nis"] for r in unresolved(rows)} == {"73009", "73083"}
