"""Tests for src/pages/migrations.py: forward-only, never silently drops a field it
doesn't understand (block_contract.md, "Versioning and migration"; batch spec §2.5).

At the time this batch ships, schema_version 1 is the *only* version that has ever
existed -- there is no schema_version 0 fixture to migrate from, so "migration from
each superseded schema_version" (spec §15) is exercised here only to the extent that
is currently possible: migrate() is a safe no-op on the current version, it preserves
fields it doesn't recognise rather than dropping them, and it refuses (rather than
guesses at) a schema_version that has never existed or is newer than this code
understands. See the batch report for what could not be tested and why.
"""

import pytest

from src.pages import CURRENT_SCHEMA_VERSION, PageDocumentError, migrate
from tests.fixtures.pages import builders


def test_migrate_is_a_no_op_on_a_document_already_at_the_current_version():
    doc = builders.realistic_multi_section_document()
    migrated = migrate(doc)
    assert migrated["schema_version"] == CURRENT_SCHEMA_VERSION
    assert migrated == doc


def test_migrate_output_document_is_always_at_the_current_schema_version():
    doc = builders.minimal_valid_document()
    migrated = migrate(doc)
    assert migrated["schema_version"] == CURRENT_SCHEMA_VERSION


def test_migrate_preserves_an_unknown_top_level_field_rather_than_dropping_it():
    """A field this batch's schema doesn't know about (e.g. authored by a
    later batch, or hand-edited) must survive migrate() untouched -- silently
    dropping it is exactly the failure mode block_contract.md rules out."""
    doc = builders.minimal_valid_document()
    doc["x_future_batch_field"] = {"anything": ["at", "all"]}
    migrated = migrate(doc)
    assert migrated.get("x_future_batch_field") == {"anything": ["at", "all"]}


def test_migrate_preserves_an_unknown_field_inside_a_block():
    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"][0]["x_future_prop"] = "kept"
    migrated = migrate(doc)
    assert migrated["sections"][0]["blocks"][0].get("x_future_prop") == "kept"


def test_migrate_does_not_mutate_its_input():
    doc = builders.minimal_valid_document()
    original = builders.deep_clone(doc)
    migrate(doc)
    assert doc == original


def test_migrate_rejects_a_schema_version_that_never_existed():
    doc = builders.minimal_valid_document()
    doc["schema_version"] = 0
    with pytest.raises(PageDocumentError):
        migrate(doc)


def test_migrate_rejects_a_schema_version_newer_than_this_code_understands():
    doc = builders.minimal_valid_document()
    doc["schema_version"] = CURRENT_SCHEMA_VERSION + 1
    with pytest.raises(PageDocumentError):
        migrate(doc)
