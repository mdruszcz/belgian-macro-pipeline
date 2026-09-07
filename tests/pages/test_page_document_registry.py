"""Tests for assets/belpulse/blocks/registry.json (batch spec §13) and
src/pages/registry.py's loader.

Checks the file's own content requirements directly (registry_version, the
forward-compat clause, the six minimum block types with their declared
interactive/accepts-binding flags, kpi_card's two versions, the deliberately
excluded types) plus a smoke test that `load_registry()` -- the public API Batch
10's renderer will actually call -- succeeds against the same file.
"""

import json

import pytest

from src.pages import load_registry
from tests.fixtures.pages import builders, real_data

REQUIRED_BLOCK_TYPES = {
    "hero": {"binding": False, "interactive": False},
    "kpi_card": {"binding": True, "interactive": False},
    "chart": {"binding": True, "interactive": True},
    "comparison_table": {"binding": True, "interactive": False},
    "map": {"binding": True, "interactive": True},
    "rich_text": {"binding": False, "interactive": False},
}

DELIBERATELY_EXCLUDED_TYPES = {"ranking_list", "news_card", "simulated_live_counter"}


def test_registry_file_exists():
    assert real_data.REGISTRY_JSON.exists(), f"missing block registry: {real_data.REGISTRY_JSON}"


def test_registry_version_is_declared_as_one():
    raw = real_data.raw_registry()
    assert raw.get("registry_version") == 1


def test_registry_documents_the_forward_compat_clause():
    """Spec §13: the file must say, in writing, that Batch 10 may ADD keys but
    not change or remove Batch 9's -- checked as free text anywhere in the
    raw JSON (e.g. a top-level `_comment`/`notes` field), not a specific key
    name, since the exact field name is builder-core's to choose."""
    text = real_data.REGISTRY_JSON.read_text(encoding="utf-8").lower()
    assert "add" in text
    assert "batch 10" in text or "renderer" in text


@pytest.mark.parametrize("block_type", sorted(REQUIRED_BLOCK_TYPES))
def test_required_block_type_is_declared(block_type):
    entry = builders.registry_entry(block_type)
    assert entry.get("type", block_type) == block_type


@pytest.mark.parametrize("block_type,flags", sorted(REQUIRED_BLOCK_TYPES.items()))
def test_accepts_binding_flag_matches_spec_table(block_type, flags):
    assert builders.accepts_binding(block_type) is flags["binding"], block_type


@pytest.mark.parametrize("block_type,flags", sorted(REQUIRED_BLOCK_TYPES.items()))
def test_interactive_flag_matches_spec_table(block_type, flags):
    assert builders.is_interactive(block_type) is flags["interactive"], block_type


@pytest.mark.parametrize("block_type", sorted(REQUIRED_BLOCK_TYPES))
def test_props_schema_status_is_provisional(block_type):
    """props_schema_status is declared per supported version (registry.json's
    own props_schema_note: no block has been rendered yet, so no version's
    prop shape is frozen), not once per block type."""
    entry = builders.registry_entry(block_type)
    for version in builders._versions_of(entry):
        status = entry["versions"][str(version)].get("props_schema_status")
        assert status == "provisional", (block_type, version)


def test_kpi_card_declares_at_least_two_supported_versions():
    entry = builders.registry_entry("kpi_card")
    versions = builders._versions_of(entry)
    assert len(versions) >= 2, versions


@pytest.mark.parametrize("excluded_type", sorted(DELIBERATELY_EXCLUDED_TYPES))
def test_deliberately_excluded_type_is_not_declared(excluded_type):
    raw = real_data.raw_registry()
    types = raw.get("block_types") or raw.get("types") or raw.get("blocks") or {}
    names = set(types.keys()) if isinstance(types, dict) else {t.get("type") for t in types}
    assert excluded_type not in names, (
        f"{excluded_type} should not be declared yet (spec §13): no content source / "
        "model exists for it"
    )


# --- load_registry(): the actual public API Batch 10 will call ------------------


def test_load_registry_default_path_succeeds():
    registry = load_registry()
    assert registry is not None


def test_load_registry_with_explicit_path_succeeds():
    registry = load_registry(path=real_data.REGISTRY_JSON)
    assert registry is not None


def test_registry_json_is_well_formed_json():
    # Belt-and-braces: if this fails, every other test in this file fails for
    # an uninformative reason, so it gets its own clear assertion.
    json.loads(real_data.REGISTRY_JSON.read_text(encoding="utf-8"))
