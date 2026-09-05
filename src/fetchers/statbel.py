"""
StatbelSource -- fetches Statbel Bestat API data at commune level (Block F).

Scoped to exactly one dataset this pass: business/enterprise units by commune
(datasource IM_EAF_LCL_UNIT_POP). Fiscal income and population are NOT built
here -- every standard Bestat view for them stops at province level, and
statbel.fgov.be (the open-data host that claims commune-level files) refused
every direct fetch attempt this session. See docs/features/statbel_adapter.md
for the full reasoning; guessing at an unseen column layout is exactly what
that document's own [SPEC] step exists to prevent.

The hard part is geography, not parsing: this dataset names communes only in
French text (Commune/Arrondissement/Province/Région), with no NIS code
anywhere in the response -- resolve_geo(nis, period) cannot be used directly.
_resolve_geo_id below implements the (name, arrondissement) compound match
this module's tests and docs/features/statbel_adapter.md derive and justify,
including the one confirmed override for Bestat's live data disagreeing with
this repo's own NIS9-derived geographies.csv on a single commune's French
name (Sint-Niklaas / Saint-Nicolas).
"""

import csv
import re
from pathlib import Path

from src.fetchers.base import MunicipalTimeSeriesSource

GEOGRAPHIES_CSV = Path(__file__).resolve().parents[2] / "config" / "geography" / "geographies.csv"

# The one confirmed case where Bestat's live API uses a different French name
# than this repo's own geographies.csv (itself faithfully derived from
# Statbel's NIS9 reference file). Sint-Niklaas is a Flemish-only commune;
# Bestat correctly leaves it untranslated in its French-locale export, while
# NIS9's own T_MUN_FR field says "Saint-Nicolas" -- which collides with a
# genuinely different, real commune of that name in Liège province
# (be:mun:62093). Adding Arrondissement as a second key resolves every other
# commune uniquely; this is the single documented exception, not a general
# fuzzy-matching mechanism. See docs/features/statbel_adapter.md.
NAME_OVERRIDES = {
    ("Sint-Niklaas", "Arrondissement de Saint-Nicolas"): "be:mun:46021",
}

# "4ème trimestre 2023" -> ("4", "2023"). Handles the ordinal-suffix variants
# Statbel actually uses (1er, 2ème/2e, 3ème/3e, 4ème/4e) rather than assuming
# one spelling.
_QUARTER_RE = re.compile(r"^(\d)(?:er|ème|e)?\s+trimestre\s+(\d{4})$", re.IGNORECASE)


class UnresolvedCommuneError(Exception):
    """A commune name in the Statbel response matches nothing in our
    geography table and no documented override. Raised rather than
    guessed or silently dropped -- see docs/features/statbel_adapter.md."""


def _build_name_lookup(geographies_csv: Path = GEOGRAPHIES_CSV) -> dict[tuple[str, str], str]:
    """(commune name_fr, arrondissement name_fr) -> geo_id, current communes only."""
    with geographies_csv.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    by_geo_id = {r["geo_id"]: r for r in rows}
    lookup: dict[tuple[str, str], str] = {}
    for row in rows:
        if row["level"] != "municipality" or row["valid_to"]:
            continue
        arrondissement = by_geo_id.get(row["parent_geo_id"])
        arr_name = arrondissement["name_fr"] if arrondissement else ""
        lookup[(row["name_fr"], arr_name)] = row["geo_id"]
    return lookup


def _parse_quarter(text: str) -> str:
    match = _QUARTER_RE.match(text.strip())
    if not match:
        raise ValueError(
            f"Unrecognized quarter format {text!r}. Refusing to guess " "(CLAUDE.md rule 13)."
        )
    quarter, year = match.groups()
    return f"{year}-Q{quarter}"


class StatbelSource(MunicipalTimeSeriesSource):
    source_id = "statbel"
    adapter = "statbel"
    raw_extension = "json"

    def __init__(self, geographies_csv: Path = GEOGRAPHIES_CSV):
        self._name_lookup = _build_name_lookup(geographies_csv)
        self._skipped_unattributed = 0

    def _resolve_geo_id(self, commune: str, arrondissement: str | None) -> str:
        key = (commune, arrondissement or "")
        if key in NAME_OVERRIDES:
            return NAME_OVERRIDES[key]
        if key in self._name_lookup:
            return self._name_lookup[key]
        raise UnresolvedCommuneError(
            f"Commune {commune!r} (arrondissement {arrondissement!r}) matches no row in "
            "geographies.csv and no documented override. Refusing to guess which commune "
            "this is (CLAUDE.md rule 13) -- add a verified override to "
            "src/fetchers/statbel.py:NAME_OVERRIDES only after confirming the identity "
            "against an independent source, per docs/features/statbel_adapter.md."
        )

    def _rows_read_hint(self, rows: list[dict]) -> int | None:
        # rows_read includes the excluded "Localisation indéterminée" row;
        # rows_written (len(rows)) does not -- the gap is the signal.
        return len(rows) + self._skipped_unattributed

    def _parse(self, raw: bytes, **kwargs) -> list[dict]:
        import json

        data = json.loads(raw)
        try:
            facts = data["facts"]
        except (KeyError, TypeError) as exc:
            raise ValueError(f"Unexpected Bestat JSON structure: {exc}") from exc

        self._skipped_unattributed = 0
        results = []
        for fact in facts:
            commune = fact.get("Commune")
            if commune is None:
                # "Localisation indéterminée" -- Statbel itself could not
                # attribute this to a commune. Excluded, not guessed at.
                self._skipped_unattributed += 1
                continue
            geo_id = self._resolve_geo_id(commune, fact.get("Arrondissement"))
            period = _parse_quarter(fact["Trimestre"])
            value = fact["Nombre d’établissements"]
            results.append(
                {
                    "geo_id": geo_id,
                    "period": period,
                    "value": float(value),
                    "status": "final",
                }
            )
        return results
