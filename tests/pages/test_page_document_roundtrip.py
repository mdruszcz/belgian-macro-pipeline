"""Canonical serialization tests (batch spec §14): byte-stable dumps/loads, matching
claude.md rule 35 and invariant 9 -- two saves of an unchanged document must be
byte-identical.

Canonical form: indent=2, sort_keys=True, ensure_ascii=False, trailing newline,
allow_nan=False. Deliberately different from the exporters' minified payload form
(src/exporters/metadata.py, scripts/export_site_payloads.py) -- page documents are
human-diffed source, not machine output.
"""

import json

import pytest

from src.pages import PageDocumentError, dumps, loads
from tests.fixtures.pages import builders


def test_dumps_uses_two_space_indent():
    doc = builders.minimal_valid_document()
    text = dumps(doc)
    assert "\n  " in text  # a nested key is indented by exactly 2 spaces


def test_dumps_sorts_keys():
    doc = builders.minimal_valid_document()
    text = dumps(doc)
    # "schema_version" sorts before "sections" sorts before "seo" alphabetically;
    # check the whole top-level key order matches sorted().
    parsed = json.loads(text)
    # Reconstruct the *rendered* key order from the text itself (not from the
    # parsed dict, which loses order-in-source-text information once loaded a
    # second way) by scanning for each top-level key's first appearance.
    top_level_keys = list(parsed.keys())
    assert top_level_keys == sorted(top_level_keys)


def test_dumps_does_not_escape_unicode():
    doc = builders.minimal_valid_document()
    doc["seo"]["title"] = {"en": "Test", "fr": "Étude à l’œil", "nl": "Onderzoek"}
    text = dumps(doc)
    assert "\\u00e9" not in text  # é not escaped
    assert "Étude à l’œil" in text


def test_dumps_ends_with_a_trailing_newline():
    doc = builders.minimal_valid_document()
    text = dumps(doc)
    assert text.endswith("\n")
    assert not text.endswith("\n\n")


def test_dumps_two_calls_on_the_same_document_are_byte_identical():
    doc = builders.minimal_valid_document()
    assert dumps(doc).encode("utf-8") == dumps(doc).encode("utf-8")


def test_dumps_output_is_independent_of_input_key_order():
    doc = builders.minimal_valid_document()
    reordered = dict(reversed(list(doc.items())))
    assert dumps(doc) == dumps(reordered)


def test_serialize_deserialize_serialize_roundtrip_is_byte_identical():
    doc = builders.realistic_multi_section_document()
    first = dumps(doc)
    reloaded = loads(first)
    second = dumps(reloaded)
    assert first == second


def test_loads_recovers_the_same_structure_dumps_wrote():
    doc = builders.realistic_multi_section_document()
    reloaded = loads(dumps(doc))
    assert reloaded == doc


def test_unicode_accents_survive_the_roundtrip():
    doc = builders.minimal_valid_document()
    doc["seo"]["description"] = {
        "en": "plain",
        "fr": "Voici un été à Liège avec des enfants – façon d’écrire",
        "nl": "Één jaar geleden in Brugge, mét accenttekens",
    }
    reloaded = loads(dumps(doc))
    assert reloaded["seo"]["description"]["fr"] == doc["seo"]["description"]["fr"]
    assert reloaded["seo"]["description"]["nl"] == doc["seo"]["description"]["nl"]


# --- the NaN trap ----------------------------------------------------------------


def test_dumps_rejects_a_document_containing_nan():
    """json.dumps defaults to allow_nan=True, which serializes float('nan') to
    the bare token NaN -- invalid JSON that Python's own json.loads happily
    accepts back (so a naive round-trip test would pass while writing a file
    no other JSON parser can read). The canonical `dumps` must refuse."""
    doc = builders.minimal_valid_document()
    doc["revision"] = float("nan")
    with pytest.raises(ValueError):
        dumps(doc)


def test_dumps_rejects_infinity_too():
    doc = builders.minimal_valid_document()
    doc["revision"] = float("inf")
    with pytest.raises(ValueError):
        dumps(doc)


def test_loads_rejects_a_bare_nan_token():
    """The other half of the trap: even if a NaN-containing file reaches
    `loads` some other way (hand-edited, or written by a different tool),
    the canonical loader must not silently accept it back as a float."""
    naive_json_with_nan = (
        '{"schema_version": 1, "page_id": "x", "revision": NaN, '
        '"route": "/", "page_type": "blank", "sections": []}'
    )
    with pytest.raises(PageDocumentError):
        loads(naive_json_with_nan)


def test_python_stdlib_json_loads_would_have_accepted_the_nan_trap():
    """Sanity check on the trap itself: confirms this is a real gap in the
    stdlib, not a hypothetical -- so the two tests above are proven to be
    testing something the canonical loader actually has to guard against."""
    naive_json_with_nan = '{"a": NaN}'
    accepted = json.loads(naive_json_with_nan)  # stdlib default: allow_nan-ish accept
    assert accepted["a"] != accepted["a"]  # NaN != NaN, confirms it parsed as float nan
