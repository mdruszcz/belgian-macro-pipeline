"""Tests for the Europe NUTS 2 batch's geography allowlist -- PR-time guard
for config/geography/nuts2.csv + nuts2_excluded.csv (src/geography/nuts2.py).
Europe NUTS 2, batch B2 (docs/features/europe_nuts2.md, docs/decisions/
0009-nuts2-regional-geography.md and its 2026-09-14 amendment)."""

import csv
from pathlib import Path

import pytest

from src.geography.international import load_excluded as load_international_excluded
from src.geography.international import load_international_rows
from src.geography.nuts2 import (
    EXCLUDED_BY_LICENCE,
    KEPT,
    NON_REGION_AGGREGATE,
    PSEUDO_REGION,
    Nuts2GeographyError,
    check_allowlist_integrity,
    classify_nuts2_code,
    is_nuts2_code,
    is_pseudo_region,
    load_excluded,
    load_nuts2_rows,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
GEOGRAPHIES_CSV = REPO_ROOT / "config" / "geography" / "geographies.csv"


def _international_allow_exclude():
    allowed = {
        code
        for code, row in load_international_rows().items()
        if row["scope"] == "pilot" and len(code) == 2
    }
    excluded = {c for c in load_international_excluded() if len(c) == 2}
    return allowed, excluded


def test_the_real_allowlist_has_no_integrity_problems():
    assert check_allowlist_integrity() == []


def test_every_nuts2_code_is_four_characters_with_an_allowlisted_prefix():
    rows = load_nuts2_rows()
    allowed_prefixes, _excluded_prefixes = _international_allow_exclude()
    assert len(rows) > 0
    for code, row in rows.items():
        assert is_nuts2_code(code), f"{code} is not 4-character NUTS-2-shaped"
        assert row["country_prefix"] == code[:2]
        assert code[:2] in allowed_prefixes, f"{code}'s prefix {code[:2]!r} is not allowlisted"


def test_nuts2_csv_never_duplicates_a_row_from_geographies_csv():
    """geographies.csv is Belgium's NIS-keyed table (CLAUDE.md rules 3/25)
    and must never gain a foreign row; nuts2.csv is the mirror image -- it
    must never re-mint a geo_id geographies.csv already owns. Every
    nuts2.csv geo_id is namespaced with a ':nuts2' suffix precisely so the
    two tables' geo_ids can never collide."""
    with GEOGRAPHIES_CSV.open(encoding="utf-8", newline="") as fh:
        belgian_geo_ids = {row["geo_id"] for row in csv.DictReader(fh)}
    rows = load_nuts2_rows()
    nuts2_geo_ids = {row["geo"]["geo_id"] for row in rows.values()}
    assert nuts2_geo_ids & belgian_geo_ids == set()
    for geo_id in nuts2_geo_ids:
        assert geo_id.endswith(":nuts2")


def test_every_nuts2_row_has_level_nuts2_and_no_nis_code():
    rows = load_nuts2_rows()
    for code, row in rows.items():
        assert row["geo"]["level"] == "nuts2", code
        assert row["geo"]["nis_code"] is None, code


def test_belgian_provinces_cross_reference_the_real_geo_id_from_geographies_csv():
    """BE21..BE35 (the 10 provinces) must resolve, by geographies.csv's own
    `nuts` column, to their real be:prov:... geo_id -- read, never hand-typed
    (the lead's decision, amending ADR 0009's original BE10-alias-only
    proposal: ALL NUTS 2 regions, Belgium's included, get their own
    :nuts2 geo_id; belgian_geo_id is a pure cross-reference)."""
    with GEOGRAPHIES_CSV.open(encoding="utf-8", newline="") as fh:
        by_nuts = {row["nuts"]: row for row in csv.DictReader(fh) if row["nuts"]}

    rows = load_nuts2_rows()
    provinces = [c for c in rows if c.startswith("BE") and c != "BE10"]
    assert len(provinces) == 10, provinces
    for code in provinces:
        expected = by_nuts[code]["geo_id"]
        assert by_nuts[code]["level"] == "province"
        assert rows[code]["belgian_geo_id"] == expected, code


def test_be10_cross_references_the_brussels_region_row():
    """Brussels has no province-level row (its arrondissement carries the
    NUTS3 tag, its region row carries NUTS1 'BE1', per ADR 0009) -- BE10 is
    the maintainer-approved alias to the Brussels region row, read from
    geographies.csv by geo_id, not hand-typed, with a sanity check against
    that row's own NUTS1 tag."""
    with GEOGRAPHIES_CSV.open(encoding="utf-8", newline="") as fh:
        by_geo_id = {row["geo_id"]: row for row in csv.DictReader(fh)}
    brussels = by_geo_id["be:reg:04000"]
    assert brussels["nuts"] == "BE1"

    rows = load_nuts2_rows()
    assert rows["BE10"]["belgian_geo_id"] == "be:reg:04000"


def test_non_belgian_rows_have_no_belgian_geo_id():
    rows = load_nuts2_rows()
    for code, row in rows.items():
        if not code.startswith("BE"):
            assert row["belgian_geo_id"] is None, code


def test_geographies_csv_was_not_modified_by_this_batch():
    """The lead's decision: nuts2.csv is a wholly separate file; geographies.csv
    keeps exactly its pre-existing 11-column shape plus `nuts` -- no new
    columns, no foreign rows."""
    with GEOGRAPHIES_CSV.open(encoding="utf-8", newline="") as fh:
        header = next(csv.reader(fh))
    assert header == [
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
        "nuts",
    ]


@pytest.mark.parametrize("code", ["EA20", "EA21", "EFTA", "EU28"])
def test_known_aggregate_codes_are_excluded_not_allowlisted(code):
    excluded = load_excluded()
    rows = load_nuts2_rows()
    assert code in excluded
    assert code not in rows


@pytest.mark.parametrize("code", ["DEZZ", "ATZZ", "FRXX", "ALXX", "HUXX", "MKXX"])
def test_pseudo_region_suffixes_are_recognized(code):
    assert is_pseudo_region(code)


def test_pseudo_region_00_suffix_is_not_a_pseudo_region():
    """LU00/IS00/LI00 are real single-region countries' own NUTS 2 code, not
    a pseudo-region marker -- '00' must never be treated like 'ZZ'/'XX'."""
    assert not is_pseudo_region("LU00")
    assert not is_pseudo_region("IS00")


class TestClassifyNuts2Code:
    """classify_nuts2_code's five branches (scripts/sync_nuts2.py's own
    consumer) -- order matters, so each branch is tested against a code that
    would be misclassified if checked in the wrong order (CLAUDE.md rule 13:
    a pseudo-region's prefix can itself be licence-allowed)."""

    def setup_method(self):
        self.rows = load_nuts2_rows()
        self.excluded = load_excluded()
        self.allowed_prefixes, self.excluded_prefixes = _international_allow_exclude()

    def _classify(self, code):
        return classify_nuts2_code(
            code, self.rows, self.excluded, self.allowed_prefixes, self.excluded_prefixes
        )

    def test_a_real_allowlisted_region_is_kept(self):
        classification, geo_id = self._classify("BE21")
        assert classification == KEPT
        assert geo_id == "be21:nuts2"

    def test_pseudo_region_with_an_allowlisted_prefix_is_pseudo_not_kept(self):
        """ALXX's prefix (AL, Albania) is allowlisted -- checking licence
        before the pseudo-region suffix would wrongly keep it."""
        classification, geo_id = self._classify("ALXX")
        assert classification == PSEUDO_REGION
        assert geo_id is None

    def test_a_known_aggregate_code_is_non_region_aggregate(self):
        classification, geo_id = self._classify("EA21")
        assert classification == NON_REGION_AGGREGATE
        assert geo_id is None

    def test_a_licence_excluded_prefix_is_excluded_by_licence(self):
        classification, geo_id = self._classify("UKC1")
        assert classification == EXCLUDED_BY_LICENCE
        assert geo_id is None

    def test_an_allowlisted_prefix_with_no_catalogued_row_raises(self):
        """A real, allowlisted-country code Eurostat could plausibly return
        (Bosnia, first NUTS coding) but that nuts2.csv has never catalogued
        -- must fail loudly, not be silently dropped or silently kept."""
        with pytest.raises(Nuts2GeographyError):
            self._classify("BA99")

    def test_a_code_in_no_list_at_all_raises(self):
        with pytest.raises(Nuts2GeographyError):
            self._classify("ZZ99")
