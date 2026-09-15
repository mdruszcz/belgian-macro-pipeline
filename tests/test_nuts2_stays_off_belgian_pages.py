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

NUTS2_INDICATOR_IDS = (
    "GDP_PC_PPS_NUTS2",
    "POPULATION_NUTS2",
    "UNEMPLOYMENT_RATE_NUTS2",
    # Eurostat additional domains batch (docs/data_catalog.md, 2026-09-15).
    "VALUE_ADDED_GROWTH_NUTS2",
    "EMPLOYMENT_RATE_NUTS2",
    "HOUSEHOLD_INCOME_TOTAL_NUTS2",
)


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


def test_exporter_writes_only_under_its_own_directory_and_touches_no_belgian_file(tmp_path):
    """Behavioural, not just a path-string check (PR #174 audit, NIT: the
    previous version of this test only compared Path objects and would have
    passed even if the exporter wrote nothing at all). Runs the REAL
    exporter at a throwaway --out-dir, and separately hashes every real
    Belgian payload file before and after to prove none of them moved."""
    import hashlib

    sys.path.insert(0, str(REPO / "scripts"))
    import export_europe_nuts2 as ex

    belgian_paths = (
        [
            p
            for p in (
                REPO / "public" / "data" / "national.json",
                REPO / "public" / "data" / "aggregates.json",
            )
            if p.is_file()
        ]
        + list((REPO / "public" / "data" / "communes").glob("*.json"))[:5]
        + list((REPO / "public" / "data" / "indicators").glob("*.json"))[:5]
    )
    assert belgian_paths, "no real Belgian payload files found to check -- test would be vacuous"
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in belgian_paths}

    out_dir = tmp_path / "nuts2_out"
    written = ex.export(out_dir)

    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in belgian_paths}
    assert before == after, "the exporter touched a real Belgian payload file"

    # And it really did write real content, only under the throwaway dir.
    on_disk = sorted(p.name for p in out_dir.glob("*.json"))
    assert on_disk == sorted([f"{i}.json" for i in written] + ["index.json"])
    for name in on_disk:
        assert (out_dir / name).stat().st_size > 0
    for belgian_dir in (
        REPO / "public" / "data" / "communes",
        REPO / "public" / "data" / "indicators",
    ):
        assert not any((belgian_dir / name).exists() for name in on_disk)


def test_stores_yaml_keeps_nuts2_as_its_own_store_not_merged_into_international():
    import yaml

    stores = yaml.safe_load((REPO / "config" / "stores.yaml").read_text(encoding="utf-8"))["stores"]
    assert "nuts2" in stores
    assert set(stores["nuts2"]["indicators"]) == set(NUTS2_INDICATOR_IDS)
    assert set(stores["nuts2"]["indicators"]).isdisjoint(stores["international"]["indicators"])
