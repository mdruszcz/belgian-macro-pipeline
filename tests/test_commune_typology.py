"""The Belfius typology, resolved onto today's communes (scripts/export_commune_typology.py).

A classification is the easiest thing in this repository to get quietly
wrong: a name matched to the wrong town looks exactly as tidy as the right
one. So the assertions here are hand-checked assignments read off the printed
Belfius pages, the merger cases worked out by hand from the crosswalk, and the
refusals -- an unresolvable name, a commune printed twice, a name whose region
disagrees with its publication -- which must stop the export rather than
publish a guess.
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.export_commune_typology import (  # noqa: E402
    DEFAULT_CLUSTERS,
    DEFAULT_CROSSWALK,
    DEFAULT_GEOGRAPHIES,
    DEFAULT_LABELS,
    DEFAULT_OUT,
    TypologyError,
    build,
    load_labels,
    normalise,
)

LANGS = ("en", "fr", "nl")


def _rows(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture(scope="module")
def payload() -> dict:
    return build(
        _rows(DEFAULT_CLUSTERS),
        load_labels(DEFAULT_LABELS),
        _rows(DEFAULT_GEOGRAPHIES),
        _rows(DEFAULT_CROSSWALK),
    )


# --- the print, normalised ------------------------------------------------------


@pytest.mark.parametrize(
    "printed, canonical",
    [
        ("FONTAINE L'ÉVEQUE", "Fontaine-l'Évêque"),
        ("SCHERPENHEUVEL - ZICHEM", "Scherpenheuvel-Zichem"),
        ("BELŒIL", "Belœil"),
        ("LA ROCHE EN ARDENNE", "La Roche-en-Ardenne"),
        ("BULLINGEN", "Büllingen"),
        ("MONT-DE-L'ENCLUS", "Mont-de-l’Enclus"),
    ],
)
def test_the_print_and_the_geography_normalise_to_the_same_key(printed, canonical):
    assert normalise(printed) == normalise(canonical)


def test_ham_is_not_ham_sur_heure():
    """Whole-name matching: a short name must never match a longer one."""
    assert normalise("HAM") != normalise("Ham-sur-Heure-Nalinnes")


# --- the composition is complete and consistent ---------------------------------


def test_every_current_commune_has_exactly_one_entry(payload):
    current = {
        r["nis_code"]
        for r in _rows(DEFAULT_GEOGRAPHIES)
        if r["level"] == "municipality" and not r["valid_to"]
    }
    assert set(payload["communes"]) == current
    assert len(current) == 565
    counts = payload["counts"]
    assert counts["classified"] + counts["mixed"] + counts["unclassified"] == 565


def test_the_print_holds_the_pre_2019_commune_map(payload):
    """588 printed names over the 589 communes of 2018; the crosswalk carries
    them to 565. One 2018 commune is absent from the print -- see
    test_the_one_commune_belfius_did_not_print."""
    printed = _rows(DEFAULT_CLUSTERS)
    assert len(printed) == 261 + 308 + 19
    per_region = {}
    for row in printed:
        per_region[row["region"]] = per_region.get(row["region"], 0) + 1
    assert per_region == {"WAL": 261, "VLA": 308, "BXL": 19}


def test_every_cluster_in_the_labels_is_published_and_trilingual(payload):
    labels = load_labels(DEFAULT_LABELS)
    assert set(payload["clusters"]) == set(labels["clusters"])
    assert len(payload["clusters"]) == 37
    for code, cluster in payload["clusters"].items():
        assert all(cluster["label"][lang] for lang in LANGS), code
        assert cluster["family"] in payload["families"], code
    for fid, family in payload["families"].items():
        assert all(family["label"][lang] for lang in LANGS), fid
        assert family["clusters"], f"family {fid} has no cluster"


def test_the_two_printed_code_typos_are_corrected_on_the_record(payload):
    assert payload["clusters"]["W16"]["printed_as"] == "W14"
    assert payload["clusters"]["V16"]["printed_as"] == "V15"
    assert "printed_as" not in payload["clusters"]["W14"]
    assert "printed_as" not in payload["clusters"]["V15"]


def test_no_commune_is_a_member_of_two_clusters(payload):
    seen = {}
    for code, cluster in payload["clusters"].items():
        for nis in cluster["members"]:
            assert nis not in seen, f"{nis} is in {seen[nis]} and {code}"
            seen[nis] = code
    assert len(seen) == payload["counts"]["classified"]


# --- hand-checked assignments, read off the printed pages -----------------------


@pytest.mark.parametrize(
    "nis, cluster, why",
    [
        ("92094", "W16", "Namur: 'Grandes villes', last Walloon page"),
        ("11001", "V10", "Aartselaar: first name under V10"),
        ("21004", "BX5", "Brussel stad: the cluster that IS the city"),
        ("63067", "W3", "Sankt Vith, printed in German in the French publication"),
        ("63040", "W11", "Kelmis, printed in German; La Calamine in French"),
        ("62093", "W12", "Saint-Nicolas (Liège), not Sint-Niklaas"),
        ("46021", "V15", "Sint-Niklaas, whose French name is also Saint-Nicolas"),
        ("81001", "W14", "Arlon: 'Villes moyennes'"),
        ("57097", "W13", "Comines-Warneton, printed as the short 'COMINES'"),
        ("81004", "W10", "Aubange, printed as 'AUBAGNE'"),
        ("63080", "W2", "Waimes, printed as 'WAISMES'"),
        ("92048", "W7", "Fosses-la-Ville, printed without its final s"),
        (
            "92138",
            "W5",
            "Fernelmont -- the NIS one digit from Fosses-la-Ville's, and a different cluster",
        ),
        ("52043", "W12", "Manage: a recoded commune (52043 -> 55086) resolves cleanly"),
        ("35029", "V16", "De Haan: the coastal cluster"),
        ("31043", "V16", "Knokke-Heist: the coastal cluster"),
    ],
)
def test_hand_checked_assignments(payload, nis, cluster, why):
    entry = payload["communes"].get(nis) or payload["communes"].get(_recoded(nis))
    assert entry is not None, why
    assert entry["status"] == "classified", (nis, why, entry)
    assert entry["cluster"] == cluster, (nis, why)


def _recoded(nis: str) -> str:
    """A NIS the crosswalk recoded (the 2019 Hainaut renumbering) is looked up
    under its new code."""
    for row in _rows(DEFAULT_CROSSWALK):
        if row["old_nis"] == nis and row["relationship"] == "recoded":
            return row["new_nis"]
    return nis


def test_subgroups_survive_where_the_print_has_them(payload):
    assert payload["communes"]["92094"]["subgroup"] == "grandes_villes"
    assert payload["communes"]["57096"]["subgroup"] == "poles_regionaux"  # Mouscron
    assert payload["communes"]["81001"]["subgroup"] == "villes_moyennes"  # Arlon
    assert payload["communes"]["91030"]["subgroup"] == "petites_villes"  # Ciney
    assert "subgroup" not in payload["communes"]["11001"]


# --- the mergers: refuse, do not invent -----------------------------------------

#: Today's communes whose constituent parts of 2018 sat in DIFFERENT clusters,
#: each worked out by hand from municipality_crosswalk.csv against the print.
MIXED = {
    "12041": {"V10", "V4"},  # Puurs (V10) + Sint-Amands (V4)
    "44084": {"V11", "V8"},  # Aalter (V11) + Knesselare (V8)
    "44083": {"V12", "V6"},  # Deinze (V12) + Nevele (V6)
    "44085": {"V5", "V8"},  # Lovendegem, Waarschoot (V5) + Zomergem (V8) -> Lievegem
    "45068": {"V7", "V4"},  # Kruishoutem (V7) + Zingem (V4) -> Kruisem
    "72042": {"V5", "V10"},  # Meeuwen-Gruitrode (V5) + Opglabbeek (V10) -> Oudsbergen
    "72043": {"V5", "V12"},  # Neerpelt (V5) + Overpelt (V12) -> Pelt
    "11002": {"V15", "V14"},  # Antwerpen (V15) absorbed Borsbeek (V14), 2025
    "46030": {"V12", "V4", "V9"},  # Beveren + Kruibeke + Zwijndrecht
    "37022": {"V12", "V7"},  # Tielt (V12) + Meulebeke (V7)
    "37021": {"V7", "V8"},  # Wingene (V7) + Ruiselede (V8)
    "44086": {"V7", "V1"},  # Nazareth (V7) + De Pinte (V1)
    "44087": {"V6", "V4"},  # Lochristi (V6) + Wachtebeke (V4)
    "46029": {"V12", "V4"},  # Lokeren (V12) + Moerbeke (V4)
    "71072": {"V15", "V5"},  # Hasselt (V15) + Kortessem (V5)
    "71071": {"V10", "V4"},  # Tessenderlo (V10) + Ham (V4)
    "73110": {"V13", "V5"},  # Bilzen (V13) + Hoeselt (V5)
    "73111": {"V13", "V8"},  # Tongeren (V13) + Borgloon (V8)
    "82039": {"W14", "W8"},  # Bastogne (W14) + Bertogne (W8)
}


def test_a_commune_merged_from_different_clusters_is_mixed_and_assigned_to_none(payload):
    mixed = {nis: entry for nis, entry in payload["communes"].items() if entry["status"] == "mixed"}
    assert set(mixed) == set(MIXED)
    for nis, entry in mixed.items():
        assert {p["cluster"] for p in entry["parts"]} == MIXED[nis], nis
        assert "cluster" not in entry
        for part in entry["parts"]:
            assert all(part["name"][lang] for lang in LANGS)
    # ... and every cluster it touches lists it as a mixed part, never a member
    for nis, clusters in MIXED.items():
        for code in clusters:
            assert nis in payload["clusters"][code]["mixed_parts"], (nis, code)
            assert nis not in payload["clusters"][code]["members"], (nis, code)
    assert payload["counts"]["mixed"] == len(MIXED)


def test_the_one_commune_belfius_did_not_print(payload):
    """Braine-le-Comte (55004) appears on none of the nine Walloon pages -- the
    print holds 261 Walloon names for 262 communes of 2018. It is published as
    what it is, unclassified, never slotted into a plausible cluster."""
    unclassified = [nis for nis, e in payload["communes"].items() if e["status"] == "unclassified"]
    assert unclassified == ["55004"]
    assert payload["communes"]["55004"] == {"status": "unclassified"}
    assert payload["counts"] == {"classified": 545, "mixed": 19, "unclassified": 1}


def test_a_commune_merged_from_one_cluster_keeps_it_and_names_its_parts(payload):
    pajottegem = payload["communes"]["23106"]  # Galmaarden + Gooik + Herne, all V6
    assert pajottegem["status"] == "classified"
    assert pajottegem["cluster"] == "V6"
    assert sorted(p["name"]["nl"] for p in pajottegem["parts"]) == ["Galmaarden", "Gooik", "Herne"]
    merelbeke_melle = payload["communes"]["44088"]  # Melle + Merelbeke, both V2
    assert merelbeke_melle["cluster"] == "V2"
    assert len(merelbeke_melle["parts"]) == 2


# --- refusals ----------------------------------------------------------------------


def _with_row(rows: list[dict], row: dict) -> list[dict]:
    return rows + [row]


def test_an_unresolvable_name_stops_the_export():
    rows = _with_row(
        _rows(DEFAULT_CLUSTERS),
        {"region": "WAL", "cluster": "W1", "subgroup": "", "name_printed": "NOWHERE-SUR-MEUSE"},
    )
    with pytest.raises(TypologyError, match="NOWHERE-SUR-MEUSE"):
        build(
            rows, load_labels(DEFAULT_LABELS), _rows(DEFAULT_GEOGRAPHIES), _rows(DEFAULT_CROSSWALK)
        )


def test_a_commune_printed_in_two_clusters_stops_the_export():
    rows = _with_row(
        _rows(DEFAULT_CLUSTERS),
        {"region": "WAL", "cluster": "W1", "subgroup": "", "name_printed": "NAMUR"},
    )
    with pytest.raises(TypologyError, match="already printed in cluster W16"):
        build(
            rows, load_labels(DEFAULT_LABELS), _rows(DEFAULT_GEOGRAPHIES), _rows(DEFAULT_CROSSWALK)
        )


def test_a_name_whose_region_disagrees_with_its_publication_stops_the_export():
    rows = [r for r in _rows(DEFAULT_CLUSTERS) if r["name_printed"] != "NAMUR"]
    rows = _with_row(
        rows, {"region": "VLA", "cluster": "V1", "subgroup": "", "name_printed": "NAMUR"}
    )
    with pytest.raises(TypologyError, match="printed in the VLA publication"):
        build(
            rows, load_labels(DEFAULT_LABELS), _rows(DEFAULT_GEOGRAPHIES), _rows(DEFAULT_CROSSWALK)
        )


def test_a_label_missing_a_language_stops_the_export(tmp_path):
    text = DEFAULT_LABELS.read_text(encoding="utf-8")
    broken = text.replace(
        "label: {fr: Rurales peu denses, nl: Dunbevolkte landelijke gemeenten, en: Sparsely populated rural}",
        "label: {fr: Rurales peu denses, nl: Dunbevolkte landelijke gemeenten}",
    )
    assert broken != text, "the fixture no longer matches the labels file"
    path = tmp_path / "labels.yaml"
    path.write_text(broken, encoding="utf-8")
    with pytest.raises(TypologyError, match="rule 7"):
        load_labels(path)


# --- the committed payload and the CLI ----------------------------------------------


def test_the_committed_payload_is_what_a_fresh_build_produces(payload):
    if not DEFAULT_OUT.is_file():
        pytest.skip("public/data/metadata/typology.json not built")
    committed = json.loads(DEFAULT_OUT.read_text(encoding="utf-8"))
    assert (
        committed == payload
    ), "public/data/metadata/typology.json is stale; run scripts/export_commune_typology.py"


def test_the_build_is_deterministic(payload):
    again = build(
        _rows(DEFAULT_CLUSTERS),
        load_labels(DEFAULT_LABELS),
        _rows(DEFAULT_GEOGRAPHIES),
        _rows(DEFAULT_CROSSWALK),
    )
    assert json.dumps(again, sort_keys=True) == json.dumps(payload, sort_keys=True)


def test_the_cli_writes_the_payload(tmp_path):
    out = tmp_path / "typology.json"
    result = subprocess.run(
        [sys.executable, str(REPO / "scripts" / "export_commune_typology.py"), "--out", str(out)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert out.is_file()
    assert "37 clusters" in result.stdout
