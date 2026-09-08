"""Tests for src/pages/migrations.py: forward-only, never silently drops a field it
doesn't understand (block_contract.md, "Versioning and migration"; batch spec §2.5).

Batch 13 added schema_version 2 (every block carries a required `locked` flag),
so this file now exercises a REAL document migration rather than only the
framework around one: a v1 document is upgraded, the flag defaults to unlocked,
an author's existing choice is never overwritten, and the result validates.
"""

import copy

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


# ---------------------------------------------------------------------------
# schema_version 1 -> 2: every block gains a required `locked` flag (Batch 13)
# ---------------------------------------------------------------------------


def _as_v1(doc: dict) -> dict:
    """The same document as it would have been written before Batch 13:
    schema_version 1 and no `locked` on any block."""
    out = copy.deepcopy(doc)
    out["schema_version"] = 1
    for section in out["sections"]:
        for block in section["blocks"]:
            block.pop("locked", None)
    return out


def test_a_v1_document_gains_locked_on_every_block():
    doc = _as_v1(builders.realistic_multi_section_document())
    assert all("locked" not in b for s in doc["sections"] for b in s["blocks"])

    migrated = migrate(doc)

    assert migrated["schema_version"] == CURRENT_SCHEMA_VERSION
    blocks = [b for s in migrated["sections"] for b in s["blocks"]]
    assert blocks, "fixture must have blocks or this asserts nothing"
    assert all(b["locked"] is False for b in blocks)


def test_the_upgrade_defaults_to_unlocked_not_locked():
    """Defaulting to True would silently freeze every block on every existing
    page, and would read as a broken builder rather than as a migration."""
    migrated = migrate(_as_v1(builders.minimal_valid_document()))
    assert migrated["sections"][0]["blocks"][0]["locked"] is False


def test_the_upgrade_never_overwrites_a_choice_already_recorded():
    doc = _as_v1(builders.minimal_valid_document())
    doc["sections"][0]["blocks"][0]["locked"] = True
    assert migrate(doc)["sections"][0]["blocks"][0]["locked"] is True


def test_a_migrated_v1_document_actually_validates():
    """The migration is only worth anything if its output passes the schema it
    was written for -- otherwise every pre-Batch-13 draft becomes unopenable."""
    from src.pages.document import validate_document
    from src.pages.metadata import load_metadata
    from src.pages.registry import load_registry

    migrated = migrate(_as_v1(builders.realistic_multi_section_document()))
    errors = validate_document(migrated, metadata=load_metadata(), registry=load_registry())
    assert errors == [], [(e.path, e.code, e.message) for e in errors]


def test_a_v1_document_is_not_mutated_by_its_own_upgrade():
    doc = _as_v1(builders.minimal_valid_document())
    before = copy.deepcopy(doc)
    migrate(doc)
    assert doc == before
