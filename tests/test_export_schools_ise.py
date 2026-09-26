"""Contracts for the FWB site-class snapshot."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from export_schools_ise import normalize  # noqa: E402


def row(**changes):
    record = {
        "type_d_enseignement": "Fondamental ordinaire",
        "acp_resultat_calcul_acp": "FO",
        "numero_fase_de_l_etablissement": 1.0,
        "nom_de_l_etablissement": "École du Centre",
        "numero_fase_de_l_implantation": 42.0,
        "adresse_voie": "Rue Haute",
        "numero_de_porte": "2",
        "code_postal": 5000.0,
        "numero_de_classe": "3a",
        "numero_de_classe_hed": 3.0,
        "annee_de_reference_de_la_donnee": 2025.0,
    }
    record.update(changes)
    return record


def test_preserves_qualified_class_and_does_not_invent_commune():
    result = normalize([row()])["records"][0]
    assert result["ed"] == "3a"
    assert result["postcode"] == 5000
    assert "nis" not in result


def test_rejects_duplicate_site_and_formula():
    with pytest.raises(ValueError, match="duplicate"):
        normalize([row(), row(nom_de_l_etablissement="Other")])


def test_rejects_schema_or_year_change():
    with pytest.raises(ValueError, match="schema changed"):
        normalize([row(extra="new column")])
    with pytest.raises(ValueError, match="reference year"):
        normalize([row(annee_de_reference_de_la_donnee=2026.0)])
