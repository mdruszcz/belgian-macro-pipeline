"""Export the FWB school-site ISE classes as a small, deterministic public snapshot,
joined to each site's commune NIS code.

Two ODWB datasets (docs/data_catalog.md, "FWB school-site ISE classes" and "FWB AGE
Fichier signaletique des etablissements..."), both Fédération Wallonie-Bruxelles,
licence CC BY:

  ISE classes (year 2025): the differentiated-support (ED) and specialised (HED)
  regulatory class per school site/formula. Identifies a site only by FASE number.

  Site register ("FASE" 2026): one row per site x level, carrying the site's
  commune name (French), education network, and coordinates. No NIS code either --
  commune assignment still has to go through name resolution below.

JOIN. Sites are matched on the integer FASE site id (numero_fase_de_l_implantation /
ndeg_fase_de_l_implantation). A site with no register row keeps nis: null and is
counted in `unassigned_sites`; CLAUDE.md rule 13 -- if that unmatched share exceeds
2% of ISE sites, the run refuses rather than silently publishing a mostly-unassigned
directory. Measured 2026-09-26: 48 / 4,005 = 1.2%.

COMMUNE NAME -> NIS. The register's commune name has no NIS code, so this resolves
it against the 1 January 2026 municipality map in config/geography/geographies.csv,
copying scripts/sync_ipp_rate.py's own pattern (_normalize_name, _build_name_map,
NAME_OVERRIDES) -- NFKD accent stripping, case folding, non-alphanumeric collapse,
and one explicit override for the genuinely ambiguous "Saint-Nicolas" (Liège NIS
62093, not Sint-Niklaas, which the register lists under its Dutch name). This
script ALSO folds a typographic apostrophe (' U+2019) to a straight one (') before
normalising -- the register's own "Braine-l'Alleud", "Fontaine-l'Evêque" and
"Mont-de-l'Enclus" use the typographic form and would otherwise fail to match
geographies.csv's straight-apostrophe spelling.

MERGED COMMUNES. "Bertogne" (11 sites) is not on the 1 January 2026 map: it merged
into Bastogne on 2024-12-02. config/geography/geographies.csv's own successor_geo_id
column is empty for every row in the committed file (a pre-existing gap in that
export, outside this script's scope to fix -- the live database does carry it, but
the handoff for this batch is explicit that this script must not require SQLite).
config/geography/municipality_crosswalk.csv, the OTHER committed CSV that exists
specifically to record merger lineage, does carry it (old_nis 82005 -> new_nis
82039, relationship "merged"). A name that fails to resolve on the target map is
looked up there by old_name_fr/old_name_nl (after the same normalisation) and
followed to its new_nis, which is what's written. Any name that is unresolved on
BOTH the live map and the crosswalk refuses the run and lists every such name
(CLAUDE.md rule 13) -- no fuzzy matching, ever.

No Belgium/region/province aggregate is computed here (see export_schools_by_commune.py
for the one defensible aggregate, per-commune, built downstream of this file).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "config"

DATASET = "fwb-age_classes-dise-pour-les-implantations-scolaires"
SOURCE_URL = f"https://www.odwb.be/explore/dataset/{DATASET}/"
EXPORT_URL = f"https://www.odwb.be/api/explore/v2.1/catalog/datasets/{DATASET}/exports/json"

REGISTER_DATASET = (
    "fwb-age-fichier-signaletique-des-etablissements-d-enseignement-de-la-federation-"
)
REGISTER_SOURCE_URL = f"https://www.odwb.be/explore/dataset/{REGISTER_DATASET}/"
REGISTER_EXPORT_URL = (
    f"https://www.odwb.be/api/explore/v2.1/catalog/datasets/{REGISTER_DATASET}/exports/json"
)

OUTPUT = REPO_ROOT / "public/data/schools_ise_2025.json"
GEOGRAPHIES_CSV = CONFIG_DIR / "geography/geographies.csv"
CROSSWALK_CSV = CONFIG_DIR / "geography/municipality_crosswalk.csv"

#: The register is the "FASE 2026" edition (module docstring) -- as-of date for
#: which communes are on the map. Independent of the ISE data's own 2025 reference
#: year (they are two different vintages of two different files).
REGISTER_AS_OF = "2026-01-01"

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

REGISTER_REQUIRED = {
    "ndeg_fase_de_l_implantation",
    "commune_de_l_implantation",
    "reseau",
    "niveau",
    "latitude",
    "longitude",
}

#: Refuse rather than publish a mostly-unassigned directory (CLAUDE.md rule 13).
#: Measured 2026-09-26: 48 / 4,005 = 1.2%.
MAX_UNASSIGNED_SHARE = 0.02

#: One explicit, verified name->NIS override for the one genuinely ambiguous name
#: in the register, same shape and same commune as scripts/sync_ipp_rate.py's own
#: NAME_OVERRIDES -- no fuzzy fallback, ever.
NAME_OVERRIDES: dict[str, str] = {
    "saint nicolas": "62093",  # Liège province, NOT Sint-Niklaas (46021) -- the
    # register lists Sint-Niklaas separately under its Dutch name.
}


class SchoolExportError(ValueError):
    """The source data, or the name resolution against the committed geography
    files, did not meet this script's contract -- refused rather than guessed."""


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
        raise ValueError(f"invalid {label}: {value!r}")
    return int(value)


def _normalize_name(name: str) -> str:
    """Accent/case/punctuation-insensitive key for matching a commune name against
    geographies.csv's name_nl/name_fr or municipality_crosswalk.csv's
    old_name_nl/old_name_fr.

    Folds a typographic apostrophe (U+2019) to a straight one FIRST -- the
    register's "Braine-l'Alleud" etc. use the typographic form, and folding it
    before NFKD decomposition means both spellings collapse to the same key.
    Then: NFKD-decompose, drop combining marks (accents), lowercase, collapse
    every run of non-alphanumeric characters to a single space, strip.
    """
    folded = name.replace("’", "'")
    decomposed = unicodedata.normalize("NFKD", folded)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    lowered = stripped.lower()
    return re.sub(r"[^a-z0-9]+", " ", lowered).strip()


def _load_name_map(geographies_csv: Path, as_of: str) -> dict[str, set[str]]:
    """{normalized_name: {nis_code, ...}} for every municipality valid at `as_of`,
    read from the committed geographies.csv (no SQLite required) -- mirrors
    scripts/sync_ipp_rate.py:_build_name_map, CSV instead of a DB query."""
    name_map: dict[str, set[str]] = {}
    with geographies_csv.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["level"] != "municipality":
                continue
            if row["valid_from"] > as_of:
                continue
            if row["valid_to"] and row["valid_to"] <= as_of:
                continue
            for raw_name in (row["name_nl"], row["name_fr"]):
                name_map.setdefault(_normalize_name(raw_name), set()).add(row["nis_code"])
    return name_map


def _load_merger_successors(crosswalk_csv: Path) -> dict[str, str]:
    """{normalized_old_name: new_nis} from municipality_crosswalk.csv -- the
    committed record of merger lineage (config/geography/municipality_crosswalk.csv's
    own docstring/header), used as this script's CSV-based equivalent of
    src/geography/resolve.py:resolve_to_current when a name is not on the current
    map at all (e.g. "Bertogne", merged into Bastogne on 2024-12-02).

    Only follows a SINGLE hop: every row seen so far (2026-09-26) needs at most
    one, and a name requiring two would mean this map's naive one-hop lookup
    should have been extended deliberately, not silently chained."""
    successors: dict[str, str] = {}
    with crosswalk_csv.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            new_nis = row["new_nis"]
            if not new_nis:
                continue
            for raw_name in (row["old_name_nl"], row["old_name_fr"]):
                if raw_name:
                    successors[_normalize_name(raw_name)] = new_nis
    return successors


def resolve_commune_names(
    names: set[str],
    geographies_csv: Path = GEOGRAPHIES_CSV,
    crosswalk_csv: Path = CROSSWALK_CSV,
    as_of: str = REGISTER_AS_OF,
) -> dict[str, str]:
    """{register_name: nis_code} for every name in `names`. Refuses with every
    unresolved or ambiguous name listed (never one at a time, never partial) if
    any name matches zero or more-than-one NIS code on the current map, has no
    override, and has no single-hop merger successor either."""
    name_map = _load_name_map(geographies_csv, as_of)
    successors = _load_merger_successors(crosswalk_csv)

    resolved: dict[str, str] = {}
    unresolved: list[str] = []
    for name in sorted(names):
        key = _normalize_name(name)
        if key in NAME_OVERRIDES:
            resolved[name] = NAME_OVERRIDES[key]
            continue
        nis_set = name_map.get(key)
        if nis_set and len(nis_set) == 1:
            resolved[name] = next(iter(nis_set))
            continue
        if nis_set and len(nis_set) > 1:
            unresolved.append(f"{name!r} matches {len(nis_set)} communes: {sorted(nis_set)}")
            continue
        successor = successors.get(key)
        if successor:
            resolved[name] = successor
            continue
        unresolved.append(
            f"{name!r} matches no commune on the {as_of} map and has no merger successor"
        )

    if unresolved:
        raise SchoolExportError(
            "school-site commune names failed to resolve to exactly one NIS code "
            f"(CLAUDE.md rule 25, no fuzzy matching): {'; '.join(unresolved)}"
        )
    return resolved


def _load_display_names(geographies_csv: Path, nis_codes: set[str], as_of: str) -> dict[str, dict]:
    """{nis_code: {"fr": ..., "nl": ..., "en": ...}} for every NIS this export
    actually uses, read straight from geographies.csv -- never hand-typed
    (CLAUDE.md rule 36)."""
    display: dict[str, dict] = {}
    with geographies_csv.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["level"] != "municipality" or row["nis_code"] not in nis_codes:
                continue
            if row["valid_from"] > as_of:
                continue
            if row["valid_to"] and row["valid_to"] <= as_of:
                continue
            display[row["nis_code"]] = {
                "fr": row["name_fr"],
                "nl": row["name_nl"],
                "en": row["name_en"] or row["name_fr"],
            }
    return display


def _load_register(rows: list[dict]) -> dict[int, dict]:
    """{site_id: {"commune_name":..., "network":..., "lat":..., "lon":...}}, one
    row per site (a site can appear more than once, once per level -- niveau is
    not part of this export's key, so the first row seen for a site wins; the
    handoff measured no site with two different commune names, so this can never
    silently pick between conflicting communes)."""
    by_site: dict[int, dict] = {}
    for row in rows:
        if not REGISTER_REQUIRED.issubset(row):
            raise SchoolExportError(
                f"site-register schema changed: missing {REGISTER_REQUIRED - set(row)}"
            )
        site = _integer(row["ndeg_fase_de_l_implantation"], "register FASE site number")
        commune_name = row["commune_de_l_implantation"]
        if not commune_name:
            raise SchoolExportError(f"site {site} has no commune_de_l_implantation")
        existing = by_site.get(site)
        if existing is not None and existing["commune_name"] != commune_name:
            raise SchoolExportError(
                f"site {site} has two different commune names in the register: "
                f"{existing['commune_name']!r} and {commune_name!r}"
            )
        if existing is None:
            by_site[site] = {
                "commune_name": commune_name,
                "network": str(row["reseau"] or "").strip(),
                "lat": float(row["latitude"]) if row["latitude"] is not None else None,
                "lon": float(row["longitude"]) if row["longitude"] is not None else None,
            }
    return by_site


def normalize(
    rows: list[dict],
    register_rows: list[dict] | None = None,
    geographies_csv: Path = GEOGRAPHIES_CSV,
    crosswalk_csv: Path = CROSSWALK_CSV,
) -> dict:
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
        result.append(
            {
                "site": site,
                "school": str(row["nom_de_l_etablissement"]).strip(),
                "type": str(row["type_d_enseignement"]).strip(),
                "formula": formula,
                "street": str(row["adresse_voie"] or "").strip(),
                "number": str(row["numero_de_porte"] or "").strip(),
                "postcode": _integer(row["code_postal"], "postcode"),
                "ed": ed,
                "hed": _integer(hed, "HED class") if hed is not None else None,
            }
        )

    unassigned_sites = 0
    if register_rows is not None:
        register_by_site = _load_register(register_rows)
        commune_names_used = {info["commune_name"] for info in register_by_site.values()}
        name_to_nis = resolve_commune_names(commune_names_used, geographies_csv, crosswalk_csv)
        display_names = _load_display_names(
            geographies_csv, set(name_to_nis.values()), REGISTER_AS_OF
        )

        ise_sites = {r["site"] for r in result}
        for record in result:
            info = register_by_site.get(record["site"])
            if info is None:
                record["nis"] = None
                record["commune"] = None
                record["network"] = None
                record["lat"] = None
                record["lon"] = None
                continue
            nis = name_to_nis[info["commune_name"]]
            record["nis"] = nis
            record["commune"] = display_names[nis]
            record["network"] = info["network"] or None
            record["lat"] = info["lat"]
            record["lon"] = info["lon"]

        unassigned_sites = sum(1 for r in result if r["nis"] is None) if ise_sites else 0
        # unassigned_sites counts RECORDS (site/formula rows), consistent with
        # the site-level unmatched share the handoff measured against `len(rows)`
        # ISE records -- both counts move together since every unmatched site's
        # every formula row is unmatched.
        share = unassigned_sites / len(result)
        if share > MAX_UNASSIGNED_SHARE:
            raise SchoolExportError(
                f"{unassigned_sites}/{len(result)} ISE records ({share:.1%}) have no matching "
                f"site-register row -- exceeds the {MAX_UNASSIGNED_SHARE:.0%} refusal threshold "
                "(CLAUDE.md rule 13); refusing to publish a mostly-unassigned directory."
            )

    result.sort(key=lambda x: (x["postcode"], x["school"], x["site"], x["formula"]))
    payload = {
        "source": SOURCE_URL,
        "publisher": "Fédération Wallonie-Bruxelles",
        "license": "CC BY",
        "year": 2025,
        "records": result,
    }
    if register_rows is not None:
        payload["register_source"] = REGISTER_SOURCE_URL
        payload["register_year"] = 2026
        payload["unassigned_sites"] = unassigned_sites
    return payload


def _fetch_json(url: str) -> list[dict]:
    request = urllib.request.Request(url, headers={"User-Agent": "BelPulse/1.0 (data export)"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--geographies", type=Path, default=GEOGRAPHIES_CSV)
    parser.add_argument("--crosswalk", type=Path, default=CROSSWALK_CSV)
    args = parser.parse_args()
    rows = _fetch_json(EXPORT_URL)
    register_rows = _fetch_json(REGISTER_EXPORT_URL)
    payload = normalize(rows, register_rows, args.geographies, args.crosswalk)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(
        f"Exported {len(payload['records'])} school-site rows to {args.output} "
        f"({payload['unassigned_sites']} unassigned)"
    )


if __name__ == "__main__":
    main()
