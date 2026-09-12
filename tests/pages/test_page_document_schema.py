"""Structural / resource-exhaustion tests for the Batch 9 page-document schema
(docs/features/page_document.schema.json, src/pages/schema.py), against the public
contract in the batch spec (§11) and block_contract.md.

Scope: "is this document shaped like a page document at all" -- required fields, grid
coordinate typing, and the four resource-exhaustion guards in the required order (§10):
raw byte length, the json.loads/RecursionError wrapper, the parsed-tree caps (depth,
sections, blocks, string length), then jsonschema itself. The semantic rejection list
(indicator/NIS/aggregation/route/etc.) lives in test_page_document_semantics.py.

No indicator id, NIS code or figure is hand-typed here (claude.md rule 36); real ids/codes
come from tests/fixtures/pages/real_data.py, which reads them off the real payloads.
"""

import json

import pytest

from src.pages import (
    CURRENT_SCHEMA_VERSION,
    PageDocumentError,
    is_guard_error,
    load_document,
    load_metadata,
    load_registry,
    validate_document,
)
from tests.fixtures.pages import builders


@pytest.fixture(scope="module")
def metadata():
    return load_metadata()


@pytest.fixture(scope="module")
def registry():
    return load_registry()


def _codes(result) -> set[str]:
    """Normalise either a list[PageValidationError] (validate_document) or a raised
    exception (load_document) into the set of error codes it carries. load_document's
    exact raise shape is a genuine ambiguity in the contract (§11 says only "raises");
    this helper tolerates either a `.errors` list of PageValidationError or the
    exception itself exposing `.code` directly, so the tests assert on the closed
    vocabulary rather than on an unstated exception shape."""
    if isinstance(result, list):
        return {e.code for e in result}
    exc = result
    if hasattr(exc, "errors"):
        return {e.code for e in exc.errors}
    if hasattr(exc, "code"):
        return {exc.code}
    raise AssertionError(f"could not extract error codes from {exc!r}")


# --- minimal / realistic valid documents ---------------------------------------


def test_minimal_valid_document_passes(metadata, registry):
    doc = builders.minimal_valid_document()
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert errors == []


def test_realistic_multi_section_document_passes(metadata, registry):
    doc = builders.realistic_multi_section_document()
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert errors == []


def test_current_schema_version_is_declared():
    assert isinstance(CURRENT_SCHEMA_VERSION, int)
    assert CURRENT_SCHEMA_VERSION >= 1


# --- grid coordinates: integer type, in-bounds ------------------------------


def test_grid_w_must_be_integer_not_float(metadata, registry):
    """w: 12.0 must be rejected -- x/y/w/h are schema type integer, not number
    (batch spec §14), or a float slips through untouched to the renderer."""
    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"][0]["layout"]["desktop"]["w"] = 12.0
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert errors, "w: 12.0 (a float) was accepted"
    assert "invalid_grid_position" in _codes(errors) or "schema_violation" in _codes(errors)


@pytest.mark.parametrize("breakpoint,columns", [("desktop", 12), ("tablet", 8), ("mobile", 4)])
def test_grid_width_at_column_boundary_passes(metadata, registry, breakpoint, columns):
    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"][0]["layout"][breakpoint] = {
        "x": 0,
        "y": 0,
        "w": columns,
        "h": 1,
    }
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert errors == [], f"w == {columns} (the {breakpoint} column count) should pass: {errors}"


@pytest.mark.parametrize("breakpoint,columns", [("desktop", 12), ("tablet", 8), ("mobile", 4)])
def test_grid_width_one_past_column_boundary_rejected(metadata, registry, breakpoint, columns):
    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"][0]["layout"][breakpoint] = {
        "x": 0,
        "y": 0,
        "w": columns + 1,
        "h": 1,
    }
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "invalid_grid_position" in _codes(errors)


@pytest.mark.parametrize(
    "field,value", [("w", 0), ("w", -1), ("h", 0), ("h", -1), ("x", -1), ("y", -1)]
)
def test_grid_position_rejects_zero_negative(metadata, registry, field, value):
    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"][0]["layout"]["desktop"][field] = value
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "invalid_grid_position" in _codes(errors)


# --- resource-exhaustion guards (§10) -------------------------------------------


def test_document_over_one_megabyte_raw_is_rejected(metadata, registry):
    doc = builders.minimal_valid_document()
    text = json.dumps(doc)
    # Guard 1 checks raw byte length BEFORE parsing (§10), so padding with
    # non-JSON trailing bytes is enough to exercise it without needing a
    # >1MB value that would also trip the (unrelated) string-length cap.
    text += " " * (1024 * 1024 + 1)
    with pytest.raises(PageDocumentError) as exc_info:
        load_document(text, metadata=metadata, registry=registry)
    assert "document_too_large" in _codes(exc_info.value)


def test_document_just_under_one_megabyte_is_not_rejected_for_size(metadata, registry):
    doc = builders.minimal_valid_document()
    text = json.dumps(doc)
    assert len(text.encode("utf-8")) < 1024 * 1024
    # Should get past the size guard; may still fail other checks, but never
    # document_too_large.
    try:
        load_document(text, metadata=metadata, registry=registry)
    except PageDocumentError as exc:
        assert "document_too_large" not in _codes(exc)


def test_pathologically_deep_json_does_not_crash_with_recursion_error(metadata, registry):
    """Guard 2: json.loads is wrapped so CPython's own ~1000-level recursion
    limit becomes a domain error, not an uncaught RecursionError crash."""
    text = "[" * 5000 + "]" * 5000
    with pytest.raises(PageDocumentError) as exc_info:
        load_document(text, metadata=metadata, registry=registry)
    assert not isinstance(exc_info.value, RecursionError)


@pytest.mark.slow
@pytest.mark.parametrize("count,should_pass", [(512, True), (513, False)])
def test_block_count_boundary(metadata, registry, count, should_pass):
    hero_type = "hero"
    blocks = [
        builders.make_block(hero_type, block_id=f"blk-{i}", binding=None) for i in range(count)
    ]
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", blocks)]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    if should_pass:
        assert "too_many_blocks" not in _codes(errors)
    else:
        assert "too_many_blocks" in _codes(errors)


@pytest.mark.parametrize("count,should_pass", [(64, True), (65, False)])
def test_section_count_boundary(metadata, registry, count, should_pass):
    hero = builders.make_block("hero", block_id="blk-hero-1", binding=None)
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section(f"sec-{i}", [hero]) for i in range(count)]
    # Give every block a unique id -- duplicate ids are a separate rejection.
    for i, section in enumerate(doc["sections"]):
        section["blocks"][0]["id"] = f"blk-hero-{i}"
    errors = validate_document(doc, metadata=metadata, registry=registry)
    if should_pass:
        assert "too_many_sections" not in _codes(errors)
    else:
        assert "too_many_sections" in _codes(errors)


@pytest.mark.parametrize("length,should_pass", [(8 * 1024, True), (8 * 1024 + 1, False)])
def test_string_length_boundary(metadata, registry, length, should_pass):
    doc = builders.minimal_valid_document()
    doc["seo"]["description"]["en"] = "x" * length
    errors = validate_document(doc, metadata=metadata, registry=registry)
    if should_pass:
        assert "string_too_long" not in _codes(errors)
    else:
        assert "string_too_long" in _codes(errors)


def test_deeply_nested_context_beyond_cap_is_rejected(metadata, registry):
    """Guard 3's nesting-depth cap (<=16). The tree-limit walk (schema.py's
    check_tree_limits) runs generically over the parsed tree before the JSON
    Schema does, so it fires regardless of what `context`'s own shape allows
    -- exercised through `context` because it is a field this test can nest
    arbitrarily under without needing a specific block type's props shape."""
    doc = builders.minimal_valid_document()
    nested: dict = {"leaf": "x"}
    for _ in range(20):
        nested = {"nested": nested}
    doc["context"] = nested
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "too_deep" in _codes(errors), errors


# --- unsupported / unknown schema_version ---------------------------------------


def test_schema_version_too_new_is_rejected(metadata, registry):
    doc = builders.minimal_valid_document()
    doc["schema_version"] = CURRENT_SCHEMA_VERSION + 1
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsupported_schema_version" in _codes(errors)


def test_schema_version_zero_never_existed_and_is_rejected(metadata, registry):
    doc = builders.minimal_valid_document()
    doc["schema_version"] = 0
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsupported_schema_version" in _codes(errors)


# --- schema_violation: a document that is not even structurally a page document -


def test_missing_required_top_level_field_is_schema_violation(metadata, registry):
    doc = builders.minimal_valid_document()
    del doc["route"]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "schema_violation" in _codes(errors)


# --- deterministic error ordering (§11) -----------------------------------------


def test_errors_are_sorted_by_path_then_code(metadata, registry):
    doc = builders.minimal_valid_document()
    doc["schema_version"] = CURRENT_SCHEMA_VERSION + 1
    doc["page_type"] = "not-a-real-page-type"
    errors_a = validate_document(doc, metadata=metadata, registry=registry)
    errors_b = validate_document(doc, metadata=metadata, registry=registry)
    assert [(e.path, e.code) for e in errors_a] == sorted((e.path, e.code) for e in errors_a)
    assert [(e.path, e.code) for e in errors_a] == [(e.path, e.code) for e in errors_b]


# --- trilingual object shape enforced everywhere (§7) ---------------------------


def test_seo_title_missing_a_language_is_rejected(metadata, registry):
    doc = builders.minimal_valid_document()
    del doc["seo"]["title"]["nl"]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert errors, "seo.title missing 'nl' should not validate"


def test_seo_title_rejects_unknown_language_key(metadata, registry):
    doc = builders.minimal_valid_document()
    doc["seo"]["title"]["de"] = "German is not one of the three supported languages"
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert errors, "seo.title should be additionalProperties: false over {en, fr, nl}"


# --- guard vs content, told apart (Batch 11's 413-shaped answer) ----------------


def test_a_guard_rejection_is_distinguishable_from_a_content_rejection(metadata, registry):
    """Batch 11's API answers the two differently: a guard rejection is
    413-shaped and echoes no detail back, a content rejection tells the author
    what to fix. That only works if the distinction is reachable through the
    public API, so this pins both halves against real findings rather than
    against a hand-written code string."""
    hero = builders.make_block("hero", block_id="blk-hero-1", binding=None)
    oversized = builders.minimal_valid_document()
    # 65 sections, one past the documented cap of 64.
    oversized["sections"] = [builders.make_section(f"sec-{i}", [hero]) for i in range(65)]
    for i, section in enumerate(oversized["sections"]):
        section["blocks"][0]["id"] = f"blk-hero-{i}"
    guard_errors = validate_document(oversized, metadata=metadata, registry=registry)
    assert "too_many_sections" in _codes(guard_errors)
    assert all(is_guard_error(e) for e in guard_errors), [e.code for e in guard_errors]

    bad_content = builders.minimal_valid_document()
    bad_content["page_type"] = "not-a-real-page-type"
    content_errors = validate_document(bad_content, metadata=metadata, registry=registry)
    assert content_errors, "an unknown page_type should be rejected"
    assert not any(is_guard_error(e) for e in content_errors), [e.code for e in content_errors]
