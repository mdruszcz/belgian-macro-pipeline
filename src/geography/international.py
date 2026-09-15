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
    to `observations` for the multi-country pilot indicators (five original,
    plus GDP_PC_PPS_COUNTRY/POPULATION_COUNTRY added by the Europe countries
    batch, docs/features/europe_countries.md).
  * "legacy" rows (today: only EA, Eurostat's dynamic "current euro area"
    code) exist so the ONE Eurostat national indicator whose config still
    names this code as its `country` field (EUROSTAT_GDP_Q_MEUR_EA) keeps
    resolving through the same COUNTRY_GEOS this module loads -- but the
    pilot sync recognizes and then discards a bare "EA" row rather than
    writing a second, ambiguous-vintage euro-area total beside EA21.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
GEOGRAPHY_CONFIG_DIR = REPO_ROOT / "config" / "geography"
INTERNATIONAL_CSV = GEOGRAPHY_CONFIG_DIR / "international.csv"
INTERNATIONAL_EXCLUDED_CSV = GEOGRAPHY_CONFIG_DIR / "international_excluded.csv"

#: A NUTS 1/2/3 regional subdivision code, Eurostat's shape: the 2-letter
#: country prefix immediately followed by 1-3 more letters/digits with NO
#: separator (BE1, BE10, BE100). Matches src/geography/nuts2.py's own
#: is_nuts2_code() one level up (any subdivision depth, not only NUTS 2).
_NUTS_SUBDIVISION_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{1,3}$")


def is_country_level_code(code: str) -> bool:
    """True for a real NUTS 0 (whole-country) code or an underscore-joined
    EU/EFTA aggregate spelling (EU27_2020, EU27_2007) -- False for a NUTS
    1/2/3 regional subdivision code.

    Used as EurostatSource._parse's `geo_filter` for the two Europe
    countries batch indicators (GDP_PC_PPS_COUNTRY, POPULATION_COUNTRY;
    docs/features/europe_countries.md) whose datasets (nama_10r_2gdp,
    demo_r_pjanaggr3) are the SAME ones the NUTS 2 batch reads and mix every
    NUTS level into one `geo` dimension, exactly the problem
    src/geography/nuts2.py's is_nuts2_code() solves for the NUTS 2 loader.

    A country code is always exactly 2 letters (BE, DE, TR, UK -- no
    trailing digit or letter). Every aggregate code these two datasets
    carry uses an underscore (EU27_2020, EU27_2007) -- confirmed against a
    live fetch of both datasets (2026-09-15): no genuine NUTS subdivision
    code contains one. A shape that happens to also match a NUTS-subdivision
    pattern (EU28 -> "EU"+"28", EFTA -> "EF"+"TA") is filtered out here the
    same as a real region would be -- harmless, since neither is a country
    this pipeline would ever publish anyway (international_excluded.csv
    already excludes EU28 for other datasets, and EFTA is not a country).
    A code that reaches the allow/exclude decision one layer up
    (scripts/sync_international.py's _resolve_geo) unresolved -- as
    EU27_2007 and DE_TOT do, both real live findings -- still fails loudly
    exactly as before (CLAUDE.md rule 13); this filter narrows what gets
    checked, it does not weaken the check.

    Deliberately NOT applied to the five original pilot indicators: their
    aggregate code is EA21 ("EA"+"21"), which DOES match the NUTS-
    subdivision shape -- applying this filter there would silently drop the
    euro-area reference row from all five. Those datasets never mix NUTS
    levels in the first place, so no filter is needed for them.
    """
    return not _NUTS_SUBDIVISION_RE.match(code)


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
