"""The read-only metadata bundle a page-document validator checks against.

Four already-published inputs, none of them written here:

  public/data/metadata/indicators.json   52 MUNICIPAL indicators. Carries
                                         `additive`, and a trilingual
                                         `names{en,fr,nl}`.
  public/data/national.json              17 NATIONAL indicators, keyed by
                                         code. NO `additive` field, and a
                                         MONOLINGUAL `name` string.
  public/data/metadata/geographies.json  622 entries: geo_id, nis_code, level,
                                         parent_geo_id.
  config/geography/municipality_crosswalk.csv
                                         55 retired NIS codes. Read for ERROR
                                         MESSAGE TEXT ONLY -- see the boundary
                                         in semantics.py's docstring.

The indicator universe is the UNION of the first two files. `indicators.json`
is municipal-only (its exporter's own docstring says "One row per municipal
indicator"); measured, the two sets are 52 and 17 with zero overlap, 69 total.
A validator that treated `indicators.json` as the whole universe would reject
every national binding -- that is, all of the homepage and the macro page.

This module never opens a commune payload, never resolves a binding and never
computes a value. That is Batch 14's work (docs/features/data_binding.md).

Every load failure raises. A metadata file that is missing or malformed must
never degrade a check to a silent pass: an unchecked binding that looks
checked is the failure mode this whole batch exists to prevent.
"""

import csv
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from src.pages.schema import PageDocumentError

REPO_ROOT = Path(__file__).resolve().parents[2]

MUNICIPAL_INDICATORS_PATH = Path("public") / "data" / "metadata" / "indicators.json"
GEOGRAPHIES_PATH = Path("public") / "data" / "metadata" / "geographies.json"
NATIONAL_PATH = Path("public") / "data" / "national.json"
CROSSWALK_PATH = Path("config") / "geography" / "municipality_crosswalk.csv"


class MetadataError(PageDocumentError):
    """A metadata input is missing or malformed. Always fatal, never skipped."""


@dataclass(frozen=True)
class PageMetadata:
    """Published metadata, loaded once per validator instance (§16), not once
    per document."""

    municipal_indicators: Mapping[str, dict]
    national_indicators: Mapping[str, dict]
    geographies: Mapping[str, dict]
    retired_nis: Mapping[str, dict]

    def indicator_scope(self, code: str) -> str | None:
        """`"municipal"`, `"national"`, or None if the code exists in neither
        published file."""
        if code in self.municipal_indicators:
            return "municipal"
        if code in self.national_indicators:
            return "national"
        return None

    def has_indicator(self, code: str) -> bool:
        return self.indicator_scope(code) is not None

    def is_additive(self, code: str) -> bool | None:
        """Published additivity, or None when the question does not apply.

        None for a national indicator (national.json publishes no `additive`
        field) and None for an unknown code. None means "no basis to judge",
        and the caller must not read it as False.
        """
        entry = self.municipal_indicators.get(code)
        if entry is None:
            return None
        value = entry.get("additive")
        return value if isinstance(value, bool) else None

    def geo_level(self, nis: str) -> str | None:
        """The level of a currently-published NIS code, or None.

        Every one of the 622 entries has a `nis_code`, so a bare membership
        test also passes `01000` (country) and `02000` (region). Callers that
        mean "a municipality" must compare this level, not just check
        membership.
        """
        entry = self.geographies.get(nis)
        return entry.get("level") if isinstance(entry, dict) else None

    def retirement(self, nis: str) -> dict | None:
        """Crosswalk row for a NIS code that used to exist, or None.

        For MESSAGE TEXT only. Returning a successor here is not a licence to
        substitute one.
        """
        return self.retired_nis.get(nis)


def _read_json(path: Path) -> dict:
    if not path.is_file():
        raise MetadataError(f"required metadata file is missing: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise MetadataError(f"metadata file {path} could not be read: {exc}") from exc
    if not isinstance(data, dict):
        raise MetadataError(f"metadata file {path} is not a JSON object")
    return data


def _load_municipal(path: Path) -> dict[str, dict]:
    data = _read_json(path)
    rows = data.get("indicators")
    if not isinstance(rows, list) or not rows:
        raise MetadataError(f"{path} has no non-empty 'indicators' array")
    out: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("indicator_code"), str):
            raise MetadataError(f"{path} has an indicator row without a string indicator_code")
        out[row["indicator_code"]] = row
    return out


def _load_national(path: Path) -> dict[str, dict]:
    data = _read_json(path)
    rows = data.get("indicators")
    if not isinstance(rows, dict) or not rows:
        raise MetadataError(f"{path} has no non-empty 'indicators' object")
    for code, row in rows.items():
        if not isinstance(code, str) or not isinstance(row, dict):
            raise MetadataError(f"{path} has a malformed national indicator entry: {code!r}")
    return dict(rows)


def _load_geographies(path: Path) -> dict[str, dict]:
    data = _read_json(path)
    rows = data.get("geographies")
    if not isinstance(rows, list) or not rows:
        raise MetadataError(f"{path} has no non-empty 'geographies' array")
    out: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise MetadataError(f"{path} has a geography row that is not an object")
        nis = row.get("nis_code")
        level = row.get("level")
        if not isinstance(nis, str) or not isinstance(level, str):
            raise MetadataError(f"{path} has a geography row without a string nis_code and level")
        out[nis] = row
    return out


def _load_crosswalk(path: Path) -> dict[str, dict]:
    """Retired NIS codes, for message text only.

    Required, not optional: without it, a legitimate historical code would be
    reported as `invalid_nis_code` -- i.e. read as a typo -- and the check
    would look like it had run.
    """
    if not path.is_file():
        raise MetadataError(f"required crosswalk is missing: {path}")
    try:
        rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    except OSError as exc:
        raise MetadataError(f"crosswalk {path} could not be read: {exc}") from exc
    if not rows:
        raise MetadataError(f"crosswalk {path} is empty")
    out: dict[str, dict] = {}
    for row in rows:
        old = (row.get("old_nis") or "").strip()
        if not old:
            raise MetadataError(f"crosswalk {path} has a row with no old_nis")
        out[old] = {
            "old_nis": old,
            "new_nis": (row.get("new_nis") or "").strip(),
            "relationship": (row.get("relationship") or "").strip(),
            "valid_to": (row.get("valid_to") or "").strip(),
            "old_name_nl": (row.get("old_name_nl") or "").strip(),
            "old_name_fr": (row.get("old_name_fr") or "").strip(),
        }
    return out


def load_metadata(root: Path | str | None = None) -> PageMetadata:
    """Load the published metadata bundle.

    `root` defaults to the repository root and exists so tests can point at
    fixtures. Injecting metadata rather than reaching for the real payloads
    inside the validator is what keeps `src/pages/` free of a data dependency
    it would otherwise have to mock.
    """
    base = Path(root) if root is not None else REPO_ROOT
    return PageMetadata(
        municipal_indicators=MappingProxyType(_load_municipal(base / MUNICIPAL_INDICATORS_PATH)),
        national_indicators=MappingProxyType(_load_national(base / NATIONAL_PATH)),
        geographies=MappingProxyType(_load_geographies(base / GEOGRAPHIES_PATH)),
        retired_nis=MappingProxyType(_load_crosswalk(base / CROSSWALK_PATH)),
    )
