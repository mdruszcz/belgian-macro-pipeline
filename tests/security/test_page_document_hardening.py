"""Adversarial coverage for the page-document validator's unsafe-content rule
(batch spec §12, correcting block_contract.md's "raw SQL, a <script> tag, a remote
URL, or unsafe rich text" line into a structural method).

The method under test is explicitly NOT keyword scanning: `<`/`>` rejection, a URL
scheme/host allowlist, and rejecting any `on*`-prefixed key are structural rules
that never look at whether a string merely *contains* an English or French word.
Every "must reject" case here has a "must pass" sibling proving legitimate Belgian
content survives -- a validator that fires on real content is the exact failure
mode this file exists to catch (repository-architect's review, and
src/validation/rules.py's own documented severity-design reasoning).

security-red-team's full adversarial pass against a real attack surface is
deliberately deferred to Batch 11 (builder HTTP API) and Batch 14 (binding
resolution) -- see docs/implementation/known-risks.md. This file covers Batch 9's
own surface: the validator run directly against an untrusted document.
"""

import pytest

from src.pages import load_metadata, load_registry, validate_document
from tests.fixtures.pages import builders


@pytest.fixture(scope="module")
def metadata():
    return load_metadata()


@pytest.fixture(scope="module")
def registry():
    return load_registry()


def _codes(errors) -> set[str]:
    return {e.code for e in errors}


# --- must reject -----------------------------------------------------------------


UNSAFE_STRINGS = [
    "<script>alert(document.cookie)</script>",
    "plain text with a stray < angle bracket",
    "plain text with a stray > angle bracket",
    "<img src=x onerror=alert(1)>",
]


@pytest.mark.parametrize("unsafe", UNSAFE_STRINGS)
def test_angle_bracket_content_in_a_label_is_rejected(metadata, registry, unsafe):
    doc = builders.minimal_valid_document()
    doc["seo"]["title"]["en"] = unsafe
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" in _codes(errors), (unsafe, errors)


# Schemes and absolute-URL hosts that must be rejected wherever they appear
# in the document -- not just inside a URL-shaped prop.
UNSAFE_ANYWHERE_URLS = [
    "javascript:alert(1)",
    "JavaScript:alert(1)",
    "vbscript:msgbox(1)",
    "https://evil.example.com/",
    "http://attacker.test/steal",
]


@pytest.mark.parametrize("unsafe_url", UNSAFE_ANYWHERE_URLS)
def test_unsafe_scheme_or_absolute_host_anywhere_in_the_document_is_rejected(
    metadata, registry, unsafe_url
):
    doc = builders.minimal_valid_document()
    doc["seo"]["description"]["en"] = unsafe_url
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" in _codes(errors), (unsafe_url, errors)


# hero's own registry-declared `cta.href` (assets/belpulse/blocks/registry.json's
# $defs.cta) is a real URL-shaped prop -- used here instead of an invented key
# name, so this exercises the validator's actual URL-prop path rather than a
# guess at one.
UNSAFE_HREF_VALUES = [
    "data:text/html,<script>alert(1)</script>",
    "//evil.example.com/payload.js",
    "/local/11001/../../secret",
]


@pytest.mark.parametrize("unsafe_href", UNSAFE_HREF_VALUES)
def test_unsafe_value_in_a_url_shaped_prop_is_rejected(metadata, registry, unsafe_href):
    props = builders.props_for("hero")
    props["cta"] = {"label": builders.trilingual_instance("cta"), "href": unsafe_href}
    hero = builders.make_block("hero", block_id="blk-hero-1", binding=None, props=props)
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [hero])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" in _codes(errors), (unsafe_href, errors)


def test_relative_href_in_a_url_shaped_prop_passes(metadata, registry):
    props = builders.props_for("hero")
    props["cta"] = {"label": builders.trilingual_instance("cta"), "href": "/about.html"}
    hero = builders.make_block("hero", block_id="blk-hero-1", binding=None, props=props)
    doc = builders.minimal_valid_document()
    doc["sections"] = [builders.make_section("sec-1", [hero])]
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" not in _codes(errors), errors


def test_event_handler_key_in_block_props_is_rejected(metadata, registry):
    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"][0]["props"]["onclick"] = "doSomethingBad()"
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" in _codes(errors), "an on*-prefixed prop key must be rejected"


def test_onerror_key_in_block_props_is_rejected(metadata, registry):
    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"][0]["props"]["onerror"] = "stillBad()"
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" in _codes(errors)


# --- must NOT reject: this is not keyword scanning --------------------------------


LEGITIMATE_STRINGS_WITH_SQL_LOOKING_WORDS = [
    "Union des communes wallonnes",
    "Classé par ORDER alphabétique",
    "Sélection des indicateurs clés",
    "SELECT your preferred language below",  # imperative English sentence, not SQL
    "DELETE key removes a block in the builder",  # documentation-style copy
    "A WHERE-do-we-go-from-here retrospective",
]


@pytest.mark.parametrize("legitimate", LEGITIMATE_STRINGS_WITH_SQL_LOOKING_WORDS)
def test_sql_keyword_looking_but_legitimate_content_passes(metadata, registry, legitimate):
    """The reasoning §12 exists to forbid: a validator that scans for SQL
    keywords would reject a legitimate French label containing "Union" or
    "ORDER". None of these strings contain a real SQL statement, an angle
    bracket, or an unsafe URL -- only English/French words that happen to
    collide with SQL keywords."""
    doc = builders.minimal_valid_document()
    doc["seo"]["title"]["en"] = legitimate
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" not in _codes(errors), (legitimate, errors)


def test_belgian_trilingual_torture_string_is_not_flagged_as_unsafe(metadata, registry):
    torture = {
        "en": "Union of Namur’s neighbourhoods — a case study",
        "fr": (
            "Coïncidence heureuse : l’Union régionale des cœurs de "
            "villages, classée par ORDER (tri) alphabétique — sélection "
            "étudiée avec l’œil"
        ),
        "nl": (
            "Bevoegdheidsverdeling tussen de Vlaamse Gemeenschap, "
            "de Franse Gemeenschap en het Brussels Hoofdstedelijk Gewest"
        ),
    }
    doc = builders.minimal_valid_document()
    doc["seo"]["title"] = torture
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" not in _codes(errors), errors


def test_ordinary_https_relative_asset_reference_is_not_penalised_for_containing_letters(
    metadata, registry
):
    """A plain, safe piece of body text mentioning a domain name in prose
    (not as a clickable/loadable URL value) must not be treated the same as
    an actual unsafe URL value -- this fixture uses free text, not a URL
    field, to keep the two concerns distinct."""
    doc = builders.minimal_valid_document()
    doc["seo"]["description"]["en"] = "Data sourced from statbel.fgov.be, published quarterly."
    errors = validate_document(doc, metadata=metadata, registry=registry)
    assert "unsafe_content" not in _codes(errors), errors
