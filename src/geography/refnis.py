"""
Parsers for Statbel's official geography reference files.

Pure functions: they read a path and return dicts. No database, no network,
no writes. The DB-facing work lives in scripts/load_geography.py, and the
raw-to-CSV derivation in scripts/derive_geography_csv.py, so that the parsing
rules -- which is where a silent NIS mismapping would hide -- stay directly
unit-testable against small fixtures.

Two file shapes are handled:

1. REFNIS_*.csv -- a flat list of administrative entities, one vintage per
   file. Its hierarchy is implied by *document order* only (ROYAUME, then a
   region, then its provinces, then their arrondissements, then communes), so
   it is NOT used to build parent links. It is used for two things the NIS9
   file cannot supply: the per-commune language regime (which drives name_en,
   see docs/features/geography.md) and, by diffing vintages, the dates on
   which communes appeared and disappeared.

   Delimiter varies by vintage -- REFNIS_2025.csv is pipe-delimited while
   REFNIS_2019.csv and REFNIS_DEFINITIEF.csv are semicolon-delimited -- so it
   is detected per file rather than assumed. All vintages are UTF-8 with a BOM
   and CRLF line endings.

2. Nis9_Nis6_refnis_names_*.xlsx -- the primary source. Every one of its
   ~20,800 statistical-sector rows carries the *explicit* full hierarchy
   (sector, NIS6 sub-municipality, commune, arrondissement, province, region,
   country) with NL/FR/DE names and NUTS codes. Parent links come from here,
   never from slicing digits off a NIS code.
"""

import csv
from pathlib import Path

# Statbel writes these as the "Langue"/"Taal" column of REFNIS. They are the
# commune's official language regime, not a translation availability flag.
LANGUAGE_REGIME_TO_FIELD = {
    "N": "name_nl",
    "F": "name_fr",
    "D": "name_de",
    # Brussels' 19 communes are officially bilingual FR/NL. French is used as
    # the name_en base for them; the exonym file overrides the handful that
    # genuinely differ in English (Brussels itself, notably).
    "FN": "name_fr",
}

# The 1977 merger of Belgian municipalities created the modern commune
# structure. It is the one date in this module not evidenced by the files
# themselves -- see docs/features/geography.md.
STRUCTURE_EPOCH = "1977-01-01"


def _detect_delimiter(text: str) -> str:
    """REFNIS vintages disagree on the delimiter; pick by first-line count."""
    header = text.splitlines()[0] if text else ""
    return "|" if header.count("|") > header.count(";") else ";"


def parse_refnis_csv(path: Path) -> list[dict]:
    """One dict per administrative entity in a single REFNIS vintage.

    `level_hint` is 'commune' when the language-regime column is populated and
    'aggregate' otherwise -- that column is empty for the kingdom, regions,
    provinces and arrondissements, and populated for every commune. It is a
    hint, not the authority: the real level comes from the NIS9 file.
    """
    text = path.read_text(encoding="utf-8-sig")
    delimiter = _detect_delimiter(text)
    rows = list(csv.reader(text.splitlines(), delimiter=delimiter))
    if not rows:
        raise ValueError(f"{path.name} is empty")

    out = []
    for row in rows[1:]:
        if not row or not row[0].strip():
            continue
        if len(row) < 5:
            raise ValueError(
                f"{path.name}: row {row!r} has {len(row)} fields, expected at least 5. "
                "Refusing to guess at the layout (CLAUDE.md rule 13)."
            )
        regime = row[2].strip()
        out.append(
            {
                "nis_code": row[0].strip(),
                "name_fr": row[1].strip(),
                "name_nl": row[4].strip(),
                "language_regime": regime,
                "level_hint": "commune" if regime else "aggregate",
            }
        )
    return out


def parse_nis9_xlsx(path: Path) -> list[dict]:
    """One dict per statistical-sector row of the NIS9 reference workbook.

    Read-only streaming: the real file is ~2.7 MB / 20,800 rows and is only
    ever consumed to be aggregated upward, so nothing is held beyond the
    columns below.
    """
    import openpyxl

    workbook = openpyxl.load_workbook(str(path), read_only=True)
    try:
        sheet = workbook.active
        rows = sheet.iter_rows(values_only=True)
        header = list(next(rows))
        missing = [c for c in _REQUIRED_NIS9_COLUMNS if c not in header]
        if missing:
            raise ValueError(
                f"{path.name}: missing expected column(s) {missing}. "
                "Statbel changed the layout -- failing rather than parsing the wrong "
                "columns (CLAUDE.md rule 13)."
            )
        index = {name: position for position, name in enumerate(header)}
        return [_nis9_row_to_dict(row, index) for row in rows]
    finally:
        workbook.close()


# Column names are Statbel's own, including the year suffixes. They are
# checked explicitly so that a renamed column fails loudly instead of
# silently producing a hierarchy with missing parents.
_REQUIRED_NIS9_COLUMNS = (
    "C_NIS6",
    "T_NIS6_NL",
    "CNIS5_2026",
    "T_MUN_NL",
    "T_MUN_FR",
    "T_MUN_DE",
    "CNIS_ARRD_2026",
    "T_ARRD_NL",
    "T_ARRD_FR",
    "T_ARRD_DE",
    "CNIS_PROVI_2026",
    "T_PROVI_NL",
    "T_PROVI_FR",
    "T_PROVI_DE",
    "CNIS_REGIO_2026",
    "T_REGIO_NL",
    "T_REGIO_FR",
    "T_REGIO_DE",
    "NUTS1_2024",
    "NUTS2_2024",
    "NUTS3_2024",
)


def _clean_code(value) -> str | None:
    """Statbel mixes int and str codes across columns; province is empty for
    Brussels, which openpyxl surfaces as None."""
    if value is None or value == "":
        return None
    text = str(value).strip()
    if not text or text == "None":
        return None
    # Arrondissement/province codes arrive as ints (11000), regions as
    # zero-padded strings ('02000'). Normalize to 5-character zero-padded.
    return text.zfill(5)


def _nis9_row_to_dict(row: tuple, index: dict) -> dict:
    def cell(name):
        return row[index[name]]

    return {
        "nis6": str(cell("C_NIS6")).strip() if cell("C_NIS6") else None,
        # Statbel flags partial boundary transfers inside this name field
        # ("PARTIE DE ...", a trailing "*"), which crosswalk.py keys off.
        "nis6_name": cell("T_NIS6_NL"),
        "commune_code": _clean_code(cell("CNIS5_2026")),
        "commune_nl": cell("T_MUN_NL"),
        "commune_fr": cell("T_MUN_FR"),
        "commune_de": cell("T_MUN_DE"),
        "arrondissement_code": _clean_code(cell("CNIS_ARRD_2026")),
        "arrondissement_nl": cell("T_ARRD_NL"),
        "arrondissement_fr": cell("T_ARRD_FR"),
        "arrondissement_de": cell("T_ARRD_DE"),
        "province_code": _clean_code(cell("CNIS_PROVI_2026")),
        "province_nl": cell("T_PROVI_NL"),
        "province_fr": cell("T_PROVI_FR"),
        "province_de": cell("T_PROVI_DE"),
        "region_code": _clean_code(cell("CNIS_REGIO_2026")),
        "region_nl": cell("T_REGIO_NL"),
        "region_fr": cell("T_REGIO_FR"),
        "region_de": cell("T_REGIO_DE"),
        "nuts1": cell("NUTS1_2024"),
        "nuts2": cell("NUTS2_2024"),
        "nuts3": cell("NUTS3_2024"),
    }


def geo_id_for(level: str, nis_code: str | None) -> str:
    """The canonical id convention fixed in docs/features/geography.md."""
    if level == "country":
        return "be:country"
    prefixes = {
        "region": "be:reg:",
        "province": "be:prov:",
        "arrondissement": "be:arr:",
        "municipality": "be:mun:",
    }
    if level not in prefixes:
        raise ValueError(f"Unknown geography level {level!r}")
    if not nis_code:
        raise ValueError(f"Level {level!r} requires a NIS code")
    return f"{prefixes[level]}{nis_code}"


def _pick_name_en(
    names: dict, regime: str | None, exonyms: dict, nis_code: str | None, default_field: str
) -> str:
    """name_en per docs/features/geography.md: the entity's official-language
    name, overridden by the hand-verified exonym list.

    No source file supplies English, and inventing translations for 565
    communes would be fabricating user-facing text. Using the official-language
    name is the honest default; the exonym file covers the ~20 entities English
    genuinely renames.
    """
    if nis_code and nis_code in exonyms:
        return exonyms[nis_code]
    field = LANGUAGE_REGIME_TO_FIELD.get(regime or "", default_field)
    return names.get(field) or names[default_field]


def build_hierarchy(
    nis9_rows: list[dict],
    refnis_rows: list[dict],
    exonyms: dict[str, str],
    valid_from_by_code: dict[str, str] | None = None,
) -> list[dict]:
    """Collapse the sector-level NIS9 rows into the administrative hierarchy.

    Returns one dict per geography entity (country, regions, provinces,
    arrondissements, communes) ready to be written to `geographies`.

    Brussels has no province level: its communes' province code is empty in the
    source, so their arrondissement parents straight to the region. That is a
    real feature of Belgian administrative geography, not a data defect, and is
    handled explicitly rather than by a null-coalescing accident.
    """
    regime_by_code = {r["nis_code"]: r["language_regime"] for r in refnis_rows}
    valid_from_by_code = valid_from_by_code or {}

    entities: dict[str, dict] = {}

    def add(level, code, names, parent_geo_id, nuts=None):
        geo_id = geo_id_for(level, code)
        if geo_id in entities:
            return
        regime = regime_by_code.get(code)
        entities[geo_id] = {
            "geo_id": geo_id,
            "nis_code": code,
            "level": level,
            "name_nl": names["name_nl"],
            "name_fr": names["name_fr"],
            "name_en": _pick_name_en(names, regime, exonyms, code, "name_nl"),
            "parent_geo_id": parent_geo_id,
            "valid_from": valid_from_by_code.get(code, STRUCTURE_EPOCH),
            "valid_to": None,
            "successor_geo_id": None,
            "nuts": nuts,
        }

    # The country row already exists in the database (created in Block A with
    # valid_from 1830-01-01 and referenced by every existing observation). It
    # is emitted here so the CSV is self-contained, and upserted -- never
    # replaced -- by the loader.
    add(
        "country",
        "01000",
        {"name_nl": "HET RIJK", "name_fr": "ROYAUME"},
        None,
        nuts="BE",
    )
    entities["be:country"]["name_en"] = "Belgium"
    entities["be:country"]["valid_from"] = "1830-01-01"

    for row in nis9_rows:
        if not row["commune_code"]:
            continue

        region_id = None
        if row["region_code"]:
            add(
                "region",
                row["region_code"],
                {"name_nl": row["region_nl"], "name_fr": row["region_fr"]},
                "be:country",
                nuts=row["nuts1"],
            )
            region_id = geo_id_for("region", row["region_code"])

        province_id = None
        if row["province_code"]:
            add(
                "province",
                row["province_code"],
                {"name_nl": row["province_nl"], "name_fr": row["province_fr"]},
                region_id,
                nuts=row["nuts2"],
            )
            province_id = geo_id_for("province", row["province_code"])

        # Brussels: province_id is None, so the arrondissement parents to the
        # region directly.
        arrondissement_parent = province_id or region_id
        arrondissement_id = None
        if row["arrondissement_code"]:
            add(
                "arrondissement",
                row["arrondissement_code"],
                {"name_nl": row["arrondissement_nl"], "name_fr": row["arrondissement_fr"]},
                arrondissement_parent,
                nuts=row["nuts3"],
            )
            arrondissement_id = geo_id_for("arrondissement", row["arrondissement_code"])

        add(
            "municipality",
            row["commune_code"],
            {"name_nl": row["commune_nl"], "name_fr": row["commune_fr"]},
            arrondissement_id,
            nuts=row["nuts3"],
        )

    return sorted(entities.values(), key=lambda e: (e["level"], e["nis_code"] or ""))
