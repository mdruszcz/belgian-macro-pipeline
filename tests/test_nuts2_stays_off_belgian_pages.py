"""Sibling to tests/test_international_stays_off_belgian_pages.py, for the
Europe NUTS 2 batch (B2, docs/features/europe_nuts2.md): a NUTS 2 geo_id or
indicator must never reach a Belgian payload.

Deliberately does NOT use the `working_db` fixture (the assembled working
database with the full committed data) -- NUTS 2 is not wired into the daily
run this batch (no sync_nuts2.py call in `make fetch`/`make assemble`), so
there is nothing NUTS-2-shaped for that fixture to ever contain; asserting
against it would prove only that nobody wired it in yet, not that the
config-level exclusions this batch actually added are correct. These tests
instead exercise the exact functions that decide Belgian-page eligibility.
"""

import csv
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.geography.nuts2 import load_nuts2_rows  # noqa: E402
from src.validation.config_schema import (  # noqa: E402
    is_canonical_eligible,
    is_multi_geo,
    load_and_validate_all,
)

NUTS2_INDICATOR_IDS = ("GDP_PC_PPS_NUTS2", "POPULATION_NUTS2", "UNEMPLOYMENT_RATE_NUTS2")


def _configs():
    return load_and_validate_all(REPO / "config" / "indicators", REPO / "config" / "sources")


def test_every_nuts2_indicator_is_multi_geo():
    """Precondition: if this were false, is_canonical_eligible's own
    has_fetchable_national_adapter() check below would pass for the wrong
    reason."""
    indicators, _sources = _configs()
    for indicator_id in NUTS2_INDICATOR_IDS:
        assert is_multi_geo(indicators[indicator_id]), indicator_id


def test_no_nuts2_indicator_is_canonical_eligible():
    """is_canonical_eligible() is what decides whether an indicator's rows
    reach be:country / the Belgian canonical CSV and national.json (see
    scripts/port_existing_indicators.py, export_canonical_csv.py). A
    multi-geo fetch is never eligible -- the same rule that already keeps
    the five country-level pilot indicators off Belgian pages."""
    indicators, sources = _configs()
    for indicator_id in NUTS2_INDICATOR_IDS:
        assert not is_canonical_eligible(indicators[indicator_id], sources), indicator_id


def test_no_nuts2_geo_id_appears_in_geographies_csv():
    with (REPO / "config" / "geography" / "geographies.csv").open(
        encoding="utf-8", newline=""
    ) as fh:
        belgian_geo_ids = {row["geo_id"] for row in csv.DictReader(fh)}
    nuts2_geo_ids = {row["geo"]["geo_id"] for row in load_nuts2_rows().values()}
    assert nuts2_geo_ids.isdisjoint(belgian_geo_ids)
    assert all(g.endswith(":nuts2") for g in nuts2_geo_ids)


def test_nuts2_payloads_live_under_their_own_directory_not_a_belgian_one():
    """Structural check: the Europe NUTS 2 payloads are published under
    public/data/europe/nuts2/, never under any of the directories a Belgian
    page reads (public/data/communes/, indicators/, national.json,
    aggregates.json, metadata/geographies.json)."""
    europe_dir = REPO / "public" / "data" / "europe" / "nuts2"
    belgian_dirs = [
        REPO / "public" / "data" / "communes",
        REPO / "public" / "data" / "indicators",
        REPO / "public" / "data" / "metadata",
    ]
    assert europe_dir.parts[-3:] == ("data", "europe", "nuts2")
    for d in belgian_dirs:
        assert d not in europe_dir.parents and d != europe_dir


def test_stores_yaml_keeps_nuts2_as_its_own_store_not_merged_into_international():
    import yaml

    stores = yaml.safe_load((REPO / "config" / "stores.yaml").read_text(encoding="utf-8"))["stores"]
    assert "nuts2" in stores
    assert set(stores["nuts2"]["indicators"]) == set(NUTS2_INDICATOR_IDS)
    assert set(stores["nuts2"]["indicators"]).isdisjoint(stores["international"]["indicators"])
