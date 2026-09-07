"""Real-payload lookups for the Batch 9 page-document tests.

claude.md rule 36 and the batch-9 spec (Batch 9 of docs/features/page_builder.md, §5) are
explicit: no indicator id, NIS code, or figure is ever hand-typed into a test or fixture.
Every id and code used by the Batch 9 test suite is read here, at test time, from the real
published payloads -- never copy-pasted as a string literal into a test file.

This module is read-only with respect to the repository: it only opens files under
public/data/ and config/geography/ (both explicitly listed as read-only injected inputs in
the batch spec, §5) and never writes anything.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[3]
PUBLIC_DATA = REPO / "public" / "data"
INDICATORS_JSON = PUBLIC_DATA / "metadata" / "indicators.json"
GEOGRAPHIES_JSON = PUBLIC_DATA / "metadata" / "geographies.json"
NATIONAL_JSON = PUBLIC_DATA / "national.json"
CROSSWALK_CSV = REPO / "config" / "geography" / "municipality_crosswalk.csv"
REGISTRY_JSON = REPO / "assets" / "belpulse" / "blocks" / "registry.json"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


# --- indicators ---------------------------------------------------------------


def municipal_indicators() -> list[dict]:
    """The 52 municipal indicator rows from metadata/indicators.json."""
    return _load_json(INDICATORS_JSON)["indicators"]


def municipal_indicator_ids() -> list[str]:
    return [row["indicator_code"] for row in municipal_indicators()]


def national_indicators() -> dict[str, dict]:
    """The 17 national indicator rows from national.json's `indicators` map."""
    return _load_json(NATIONAL_JSON)["indicators"]


def national_indicator_ids() -> list[str]:
    return list(national_indicators().keys())


def indicator_universe() -> set[str]:
    """The union of municipal + national indicator ids -- 69 total, 0 overlap
    (batch-9 spec §5, P1 correction)."""
    return set(municipal_indicator_ids()) | set(national_indicator_ids())


def an_additive_municipal_indicator_id() -> str:
    for row in municipal_indicators():
        if row["additive"] is True:
            return row["indicator_code"]
    raise AssertionError("no additive:true municipal indicator found in the real payload")


def a_non_additive_municipal_indicator_id() -> str:
    for row in municipal_indicators():
        if row["additive"] is False:
            return row["indicator_code"]
    raise AssertionError("no additive:false municipal indicator found in the real payload")


def an_unknown_indicator_id() -> str:
    """A synthetic id guaranteed (checked, not assumed) to be absent from the
    real indicator universe -- not a real indicator, so not a rule-36 figure."""
    candidate = "DOES_NOT_EXIST_INDICATOR"
    assert candidate not in indicator_universe(), "test candidate collided with a real indicator"
    return candidate


# --- geographies ---------------------------------------------------------------


def geographies() -> list[dict]:
    """All 622 rows from metadata/geographies.json."""
    return _load_json(GEOGRAPHIES_JSON)["geographies"]


def nis_codes_at_level(level: str) -> list[str]:
    return [g["nis_code"] for g in geographies() if g["level"] == level]


def a_municipal_nis_code() -> str:
    return nis_codes_at_level("municipality")[0]


def a_country_nis_code() -> str:
    codes = nis_codes_at_level("country")
    assert codes, "no country-level geography row found"
    return codes[0]


def a_region_nis_code() -> str:
    codes = nis_codes_at_level("region")
    assert codes, "no region-level geography row found"
    return codes[0]


def a_province_nis_code() -> str:
    codes = nis_codes_at_level("province")
    assert codes, "no province-level geography row found"
    return codes[0]


def retired_nis_codes() -> list[str]:
    """The old_nis column of the merger crosswalk -- read for the codes only,
    never for the successor relationship (batch-9 spec §8, trap B/C: the
    crosswalk may be read for error-message text only, never to resolve a
    successor -- that boundary belongs to resolve_geo(), forbidden here)."""
    with CROSSWALK_CSV.open(encoding="utf-8") as fh:
        return [row["old_nis"] for row in csv.DictReader(fh)]


def a_retired_nis_code() -> str:
    codes = retired_nis_codes()
    assert codes, "no old_nis rows found in the crosswalk"
    return codes[0]


def an_invalid_nis_code() -> str:
    """A synthetic NIS code confirmed (not assumed) absent from both the
    published geography list and the merger crosswalk -- a plain typo, not a
    historical code."""
    published = {g["nis_code"] for g in geographies()}
    retired = set(retired_nis_codes())
    for candidate_int in range(0, 100000, 7919):  # a prime stride, arbitrary
        candidate = f"{candidate_int:05d}"
        if candidate not in published and candidate not in retired:
            return candidate
    raise AssertionError("could not find a spare NIS code outside both real lists")


# --- registry -------------------------------------------------------------------


def raw_registry() -> dict:
    return _load_json(REGISTRY_JSON)
