"""Export the FWB school-site ISE classes as a small, deterministic public snapshot.

The source identifies a school site by FASE number and postal address. It does
not provide a municipality NIS code, so this export deliberately does not
assign records to communes or aggregate classes across schools.
"""

from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path

DATASET = "fwb-age_classes-dise-pour-les-implantations-scolaires"
SOURCE_URL = f"https://www.odwb.be/explore/dataset/{DATASET}/"
EXPORT_URL = f"https://www.odwb.be/api/explore/v2.1/catalog/datasets/{DATASET}/exports/json"
OUTPUT = Path(__file__).resolve().parents[1] / "public/data/schools_ise_2025.json"
REQUIRED = {
    "type_d_enseignement",
    "acp_resultat_calcul_acp",
    "numero_fase_de_l_etablissement",
    "nom_de_l_etablissement",
    "numero_fase_de_l_implantation",
    "adresse_voie",
    "numero_de_porte",
    "code_postal",
    "numero_de_classe",
    "numero_de_classe_hed",
    "annee_de_reference_de_la_donnee",
}


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
        raise ValueError(f"invalid {label}: {value!r}")
    return int(value)


def normalize(rows: list[dict]) -> dict:
    if not rows:
        raise ValueError("empty school-site export")
    result = []
    seen = set()
    for row in rows:
        if set(row) != REQUIRED:
            raise ValueError(f"school-site schema changed: {set(row) ^ REQUIRED}")
        year = _integer(row["annee_de_reference_de_la_donnee"], "reference year")
        if year != 2025:
            raise ValueError(f"unexpected reference year: {year}")
        site = _integer(row["numero_fase_de_l_implantation"], "FASE site number")
        key = (site, row["acp_resultat_calcul_acp"])
        if key in seen:
            raise ValueError(f"duplicate site/formula: {key}")
        seen.add(key)
        formula = row["acp_resultat_calcul_acp"]
        if formula not in ("FO", "SO"):
            raise ValueError(f"unknown calculation formula: {formula!r}")
        ed = row["numero_de_classe"]
        if ed is not None and (not isinstance(ed, str) or not (ed.isdigit() or ed in ("3a", "3b"))):
            raise ValueError(f"invalid ED class: {ed!r}")
        hed = row["numero_de_classe_hed"]
        result.append({
            "site": site,
            "school": str(row["nom_de_l_etablissement"]).strip(),
            "type": str(row["type_d_enseignement"]).strip(),
            "formula": formula,
            "street": str(row["adresse_voie"] or "").strip(),
            "number": str(row["numero_de_porte"] or "").strip(),
            "postcode": _integer(row["code_postal"], "postcode"),
            "ed": ed,
            "hed": _integer(hed, "HED class") if hed is not None else None,
        })
    result.sort(key=lambda x: (x["postcode"], x["school"], x["site"], x["formula"]))
    return {"source": SOURCE_URL, "publisher": "Fédération Wallonie-Bruxelles",
            "license": "CC BY", "year": 2025, "records": result}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    request = urllib.request.Request(EXPORT_URL, headers={"User-Agent": "BelPulse/1.0 (data export)"})
    with urllib.request.urlopen(request, timeout=60) as response:
        rows = json.load(response)
    payload = normalize(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(f"Exported {len(payload['records'])} school-site rows to {args.output}")


if __name__ == "__main__":
    main()
