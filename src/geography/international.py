"""
The international geography allowlist -- config/geography/international.csv
and international_excluded.csv (international pilot PR 1).

Two files, two questions:
  * international.csv answers "what geo_id does this Eurostat code resolve
    to, and does the pilot sync WRITE its rows or only recognize them?"
  * international_excluded.csv answers "why is this code, which some pilot
    dataset really does return, deliberately not published?"

A code Eurostat returns that is in NEITHER file is not a decision this
pipeline has made yet -- scripts/sync_international.py fails the fetch on it
(CLAUDE.md rule 13) rather than silently dropping or silently adding a
country. See docs/features/international.md, "Geography".

`scope` on an international.csv row is the difference between the two
consumers of this same file:
  * "pilot" rows are the ones scripts/sync_international.py actually writes
    to `observations` for the five multi-country pilot indicators.
  * "legacy" rows (today: only EA, Eurostat's dynamic "current euro area"
    code) exist so the ONE Eurostat national indicator whose config still
    names this code as its `country` field (EUROSTAT_GDP_Q_MEUR_EA) keeps
    resolving through the same COUNTRY_GEOS this module loads -- but the
    pilot sync recognizes and then discards a bare "EA" row rather than
    writing a second, ambiguous-vintage euro-area total beside EA21.
"""

from __future__ import annotations

import csv
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
GEOGRAPHY_CONFIG_DIR = REPO_ROOT / "config" / "geography"
INTERNATIONAL_CSV = GEOGRAPHY_CONFIG_DIR / "international.csv"
INTERNATIONAL_EXCLUDED_CSV = GEOGRAPHY_CONFIG_DIR / "international_excluded.csv"

#: Exact key order the `geographies` table's INSERT statements use elsewhere
#: (scripts/load_geography.py's UPSERT_SQL, port_existing_indicators.py's
#: BE_COUNTRY_GEO) -- `tuple(geo.values())` must produce these 12 values in
#: this order for a positional INSERT to land in the right columns.
GEO_COLUMNS = (
    "geo_id",
    "nis_code",
    "level",
    "name_nl",
    "name_fr",
    "name_en",
    "parent_geo_id",
    "valid_from",
    "valid_to",
    "successor_geo_id",
    "population",
    "area_km2",
)


class InternationalGeographyError(Exception):
    """international.csv/international_excluded.csv are missing, malformed,
    or disagree with each other."""


def _read_csv(path: Path) -> list[dict]:
    if not path.is_file():
        raise InternationalGeographyError(f"Missing {path}")
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def load_international_rows(csv_path: Path = INTERNATIONAL_CSV) -> dict[str, dict]:
    """eurostat_code -> {"geo": <12-column geography dict>, "scope": ...,
    "basis": ..., "listed_on": ...}. Raises on a duplicate eurostat_code (an
    upsert into `geographies` would silently overwrite, the same failure mode
    scripts/load_geography.py already guards against for the Belgian
    hierarchy) or a `scope` outside {"pilot", "legacy"}.
    """
    rows = _read_csv(csv_path)
    out: dict[str, dict] = {}
    for row in rows:
        code = row["eurostat_code"]
        if code in out:
            raise InternationalGeographyError(
                f"{csv_path}: duplicate eurostat_code {code!r} -- an upsert would silently "
                "overwrite one geography with another"
            )
        if row["scope"] not in ("pilot", "legacy"):
            raise InternationalGeographyError(
                f"{csv_path}: {code} has scope {row['scope']!r}, expected 'pilot' or 'legacy'"
            )
        geo = {
            "geo_id": row["geo_id"],
            "nis_code": None,
            "level": row["level"],
            "name_nl": row["name_nl"],
            "name_fr": row["name_fr"],
            "name_en": row["name_en"],
            "parent_geo_id": None,
            "valid_from": row["valid_from"],
            "valid_to": None,
            "successor_geo_id": None,
            "population": None,
            "area_km2": None,
        }
        out[code] = {
            "geo": geo,
            "scope": row["scope"],
            "basis": row["basis"],
            "listed_on": row["listed_on"],
        }
    return out


def load_excluded(csv_path: Path = INTERNATIONAL_EXCLUDED_CSV) -> dict[str, str]:
    """code -> reason, from international_excluded.csv."""
    return {row["code"]: row["reason"] for row in _read_csv(csv_path)}


def load_country_geos(csv_path: Path = INTERNATIONAL_CSV) -> dict[str, dict]:
    """eurostat_code -> the plain 12-column geography dict, for
    scripts/port_existing_indicators.py's COUNTRY_GEOS (the national/legacy
    single-geo fetch path, which has no use for `scope`/`basis`/`listed_on`
    and inserts this dict's values positionally)."""
    return {code: row["geo"] for code, row in load_international_rows(csv_path).items()}


def check_allowlist_integrity(
    international_path: Path = INTERNATIONAL_CSV,
    excluded_path: Path = INTERNATIONAL_EXCLUDED_CSV,
) -> list[str]:
    """PR-time guard (tests/test_international_geography.py): every property
    the allowlist must hold, checked in one place instead of scattered
    assertions that could each individually pass while the whole is broken.
    Returns problem strings; empty means clean."""
    problems: list[str] = []
    rows = load_international_rows(international_path)
    excluded = load_excluded(excluded_path)

    overlap = sorted(set(rows) & set(excluded))
    if overlap:
        problems.append(f"code(s) both allowlisted and excluded: {overlap}")

    for required in ("UK", "XK", "US", "JP"):
        if required not in excluded:
            problems.append(f"{required} must be excluded and is not")
        if required in rows:
            problems.append(f"{required} must not be allowlisted")

    geo_ids: dict[str, str] = {}
    for code, row in rows.items():
        geo_id = row["geo"]["geo_id"]
        if geo_id in geo_ids and geo_ids[geo_id] != code:
            # BE is deliberately shared with the Belgian hierarchy's own
            # be:country -- that is one code, one geo_id, not a collision.
            problems.append(f"geo_id {geo_id!r} used by both {geo_ids[geo_id]!r} and {code!r}")
        geo_ids[geo_id] = code

    pilot_aggregates = sorted(
        code
        for code, row in rows.items()
        if row["scope"] == "pilot" and row["basis"] == "aggregate"
    )
    if pilot_aggregates != ["EA21", "EU27_2020"]:
        problems.append(
            f"expected exactly the two pilot aggregates EA21 and EU27_2020, got {pilot_aggregates}"
        )

    return problems
