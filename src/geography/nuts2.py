"""
The NUTS 2 geography allowlist -- config/geography/nuts2.csv and
nuts2_excluded.csv (Europe NUTS 2 batch B2, docs/features/europe_nuts2.md,
docs/decisions/0009-nuts2-regional-geography.md and its 2026-09-14
amendment).

Mirrors src/geography/international.py's two-file shape one geographic
level down (country -> NUTS 2 region), with one deliberate difference the
amendment to ADR 0009 records: EVERY NUTS 2 region, Belgium's own 11
included, gets its own `:nuts2` geo_id here. international.py's BE rows
share `be:country` with the Belgian hierarchy (ADR 0008's overlap rule);
nuts2.csv does NOT do the equivalent for Belgian provinces, on purpose --
storing a NUTS 2 regional figure under an existing `be:prov:...` geo_id
would risk it leaking into a Belgian province/aggregate payload and mixing
a NUTS-built figure with a NIS-built one (CLAUDE.md rules 3, 25, 27). A
Belgian row's `belgian_geo_id` column is a pure cross-reference for
tooling/humans, resolved from config/geography/geographies.csv's own
`nuts` column (BE21..BE35) or, for Brussels (BE10, which has no province
row to carry a NUTS 2 tag), the maintainer-approved alias to the Brussels
region row `be:reg:04000` -- see nuts2.csv's own generation and ADR 0009's
amendment for why BE10 needs an explicit alias rather than a `nuts` lookup.
It is NEVER used to resolve an observation's geo_id: a NUTS 2 loader always
writes under the `:nuts2` geo_id, never under `belgian_geo_id`.

Two files, two questions, exactly as international.py's docstring puts it:
  * nuts2.csv answers "what geo_id does this NUTS 2 code resolve to, and
    is it real region this pipeline has decided to publish?"
  * nuts2_excluded.csv answers "why is this 4-character, NUTS-2-shaped code
    NOT a real region?" -- Eurostat aggregate codes (EA21, EFTA, ...) that
    pass a naive 2-letter-prefix + 2-more-characters shape check but name a
    country grouping, not a place. A pseudo-region code (Eurostat's own
    'ZZ' extra-regio / 'XX' not-regionalised suffixes) is EXCLUDED here too,
    under is_pseudo_region() -- it never becomes a nuts2.csv row and never
    needs a reason row of its own, because the suffix itself is the reason
    (see is_pseudo_region's docstring).

A NUTS-2-shaped code that is in NEITHER nuts2.csv NOR nuts2_excluded.csv,
and whose 2-letter prefix is not in international_excluded.csv either (the
third, licence-based reason a code is not published -- see
resolve_nuts2_code below) is not a decision this pipeline has made yet:
scripts/sync_nuts2.py fails the fetch on it (CLAUDE.md rule 13), exactly
like scripts/sync_international.py already does for a whole-country code.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
GEOGRAPHY_CONFIG_DIR = REPO_ROOT / "config" / "geography"
NUTS2_CSV = GEOGRAPHY_CONFIG_DIR / "nuts2.csv"
NUTS2_EXCLUDED_CSV = GEOGRAPHY_CONFIG_DIR / "nuts2_excluded.csv"

#: Exact key order the `geographies` table's INSERT statements use elsewhere
#: (scripts/load_geography.py's UPSERT_SQL, src/geography/international.py's
#: GEO_COLUMNS) -- tuple(geo.values()) must produce these 12 values in this
#: order for a positional INSERT to land in the right columns.
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

#: A NUTS 2 code is exactly 4 characters: a 2-letter country prefix
#: (uppercase ASCII letters) followed by 2 more characters Eurostat mixes
#: digits and letters in (e.g. 'BE10', 'LU00', 'TRA1'). Same rule
#: scripts/report_nuts2_coverage.py's is_nuts2_code() already verified
#: against a live nama_10r_2gdp fetch (2026-09-14): 3-char entries are
#: NUTS1/country, 5-char entries are NUTS3.
_NUTS2_CODE_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{2}$")

#: Eurostat's own pseudo-region suffixes -- never a real, mappable area.
#: 'ZZ': national-accounts "extra-regio" (activity not attributable to any
#: region). 'XX': demography's "Not regionalised/Unknown NUTS 2". Verified
#: against the three candidate datasets' own geo-dimension labels
#: (docs/features/europe_nuts2.md, "Coverage"). '00' is deliberately NOT
#: here -- it is a real single-region country's own NUTS 2 code (LU00,
#: IS00, LI00), not a pseudo-region marker.
_PSEUDO_REGION_SUFFIXES = frozenset({"ZZ", "XX"})


class Nuts2GeographyError(Exception):
    """nuts2.csv/nuts2_excluded.csv are missing, malformed, or disagree
    with each other."""


def is_nuts2_code(code: str) -> bool:
    return bool(_NUTS2_CODE_RE.match(code))


def is_pseudo_region(code: str) -> bool:
    """True for a structurally NUTS2-shaped code that is one of Eurostat's
    pseudo-regions rather than a real, mappable NUTS 2 area."""
    return is_nuts2_code(code) and code[2:] in _PSEUDO_REGION_SUFFIXES


def nuts2_country_prefix(code: str) -> str:
    """The 2-letter country prefix of a NUTS 2 code. Not validated here --
    callers apply is_nuts2_code() first."""
    return code[:2]


def _read_csv(path: Path) -> list[dict]:
    if not path.is_file():
        raise Nuts2GeographyError(f"Missing {path}")
    with path.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def load_nuts2_rows(csv_path: Path = NUTS2_CSV) -> dict[str, dict]:
    """nuts_code -> {"geo": <12-column geography dict, level='nuts2',
    nis_code=None>, "country_prefix", "belgian_geo_id" (empty string for
    every non-Belgian row)}.

    Raises on a duplicate nuts_code (an upsert into `geographies` would
    silently overwrite one region with another) or a code that fails
    is_nuts2_code() (nuts2.csv is meant to hold real NUTS 2 regions only,
    never a country or NUTS1/3 code).
    """
    rows = _read_csv(csv_path)
    out: dict[str, dict] = {}
    for row in rows:
        code = row["nuts_code"]
        if not is_nuts2_code(code):
            raise Nuts2GeographyError(
                f"{csv_path}: {code!r} is not a 4-character NUTS 2 code (2-letter "
                "prefix + 2 more characters)"
            )
        if code in out:
            raise Nuts2GeographyError(
                f"{csv_path}: duplicate nuts_code {code!r} -- an upsert would silently "
                "overwrite one region with another"
            )
        geo = {
            "geo_id": row["geo_id"],
            "nis_code": None,
            "level": "nuts2",
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
            "country_prefix": row["country_prefix"],
            "nuts_version": row["nuts_version"],
            "belgian_geo_id": row["belgian_geo_id"] or None,
        }
    return out


def load_excluded(csv_path: Path = NUTS2_EXCLUDED_CSV) -> dict[str, str]:
    """code -> reason, from nuts2_excluded.csv (aggregate codes only --
    pseudo-region codes are recognized structurally by is_pseudo_region()
    and never need a row here)."""
    return {row["code"]: row["reason"] for row in _read_csv(csv_path)}


def check_allowlist_integrity(
    nuts2_path: Path = NUTS2_CSV,
    excluded_path: Path = NUTS2_EXCLUDED_CSV,
) -> list[str]:
    """PR-time guard (tests/test_nuts2_geography.py): every property the
    NUTS 2 allowlist must hold, checked in one place. Returns problem
    strings; empty means clean."""
    problems: list[str] = []
    rows = load_nuts2_rows(nuts2_path)
    excluded = load_excluded(excluded_path)

    overlap = sorted(set(rows) & set(excluded))
    if overlap:
        problems.append(f"code(s) both allowlisted and excluded: {overlap}")

    for code in rows:
        if is_pseudo_region(code):
            problems.append(f"{code}: a pseudo-region code must never be allowlisted")
    for code in excluded:
        if not is_nuts2_code(code):
            problems.append(f"{code}: nuts2_excluded.csv entries must be NUTS-2-shaped")

    geo_ids: dict[str, str] = {}
    for code, row in rows.items():
        geo_id = row["geo"]["geo_id"]
        if geo_id in geo_ids:
            problems.append(f"geo_id {geo_id!r} used by both {geo_ids[geo_id]!r} and {code!r}")
        geo_ids[geo_id] = code
        if not geo_id.endswith(":nuts2"):
            problems.append(f"{code}: geo_id {geo_id!r} must end with ':nuts2'")

    return problems


#: The classification outcomes for one fetched NUTS 2 geo code.
KEPT = "kept"
PSEUDO_REGION = "pseudo_region"
NON_REGION_AGGREGATE = "non_region_aggregate"
EXCLUDED_BY_LICENCE = "excluded_by_licence"


def classify_nuts2_code(
    code: str,
    nuts2_rows: dict[str, dict],
    nuts2_excluded: dict[str, str],
    international_allowed_prefixes: set[str],
    international_excluded_prefixes: set[str],
) -> tuple[str, str | None]:
    """(classification, geo_id or None). classification is one of KEPT,
    PSEUDO_REGION, NON_REGION_AGGREGATE, EXCLUDED_BY_LICENCE.

    Order matters: a pseudo-region suffix is checked FIRST, before the
    country-prefix licence check, because a pseudo-region's prefix can
    itself be licence-allowed (e.g. 'ALXX' -- Albania, 'AL', is allowlisted)
    -- checking licence first would wrongly keep it as if it were a real
    Albanian region.

    Raises Nuts2GeographyError if `code` is NUTS-2-shaped but in none of
    nuts2.csv, nuts2_excluded.csv, or either international.csv/
    international_excluded.csv (by 2-letter prefix) -- CLAUDE.md rule 13.
    """
    if is_pseudo_region(code):
        return PSEUDO_REGION, None
    if code in nuts2_excluded:
        return NON_REGION_AGGREGATE, None
    prefix = nuts2_country_prefix(code)
    if code in nuts2_rows:
        return KEPT, nuts2_rows[code]["geo"]["geo_id"]
    if prefix in international_excluded_prefixes:
        return EXCLUDED_BY_LICENCE, None
    if prefix in international_allowed_prefixes:
        raise Nuts2GeographyError(
            f"{code!r}: prefix {prefix!r} is licence-allowed but the code is not a row in "
            f"{NUTS2_CSV} -- a real region Eurostat returned that this pipeline has not "
            "catalogued yet. Add it to nuts2.csv rather than silently dropping or guessing "
            "at it (CLAUDE.md rule 13)."
        )
    raise Nuts2GeographyError(
        f"{code!r} is neither allowlisted (config/geography/nuts2.csv), a known non-region "
        f"aggregate (config/geography/nuts2_excluded.csv), nor licence-excluded "
        "(config/geography/international_excluded.csv, by its 2-letter prefix). Refusing to "
        "guess whether it may be published (CLAUDE.md rule 13) -- add it to one file or the "
        "other."
    )
