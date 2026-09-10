"""Tests for the shared block renderer (Batch 10, src/pages/render.py).

The renderer's contract is unusual and worth stating: it is the ONE
implementation both the public build and the builder preview call, so these
tests care most about the properties that make that safe -- determinism, escaping,
per-block failure isolation, and rendering every block type the registry
declares. Canvas drawing and map hydration need a browser and are covered by
the manual headless check recorded in the batch report, as everywhere else in
this repo.

No indicator id, NIS code or figure is hand-typed (claude.md rule 36); block
props are derived from the real registry by tests/fixtures/pages/builders.py.
"""

import re

import pytest

from src.pages import BlockRenderError, load_registry, render_document
from src.pages.render import BLOCK_RENDERERS, LANGUAGES, safe_href, text_in
from tests.fixtures.pages import builders


@pytest.fixture(scope="module")
def registry():
    return load_registry()


@pytest.fixture(scope="module")
def doc():
    return builders.realistic_multi_section_document()


# --- every declared block type actually renders ------------------------------


def test_every_registry_block_type_has_a_renderer(registry):
    """A type declared in registry.json with no renderer here would render as
    an empty block on the public site. The registry is the contract; this
    keeps the renderer honest against it in both directions."""
    declared = set(registry.types)
    implemented = set(BLOCK_RENDERERS)
    assert declared == implemented, (
        f"declared but not renderable: {sorted(declared - implemented)}; "
        f"renderable but not declared: {sorted(implemented - declared)}"
    )


@pytest.mark.parametrize("block_type", sorted(BLOCK_RENDERERS))
def test_each_block_type_renders_without_data(registry, block_type):
    """A block with no resolved data must still render -- in an honest state,
    never as a crash and never as a blank page."""
    version = builders.current_version(block_type)
    block = builders.make_block(block_type, block_id="blk-1", version=version)
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [block])]

    errors: list[BlockRenderError] = []
    html = render_document(document, registry=registry, lang="en", errors=errors)
    assert not errors, [e.reason for e in errors]
    assert f'data-block-type="{block_type}"' in html
    assert 'data-state="' in html


@pytest.mark.parametrize("block_type", sorted(BLOCK_RENDERERS))
def test_every_supported_version_still_renders(registry, block_type):
    """kpi_card ships v1 AND v2 on purpose. A renderer that only handled the
    current version would break documents migrations.py deliberately still
    accepts, so every supported version must render."""
    for version in registry.supported_versions(block_type):
        block = builders.make_block(block_type, block_id="blk-1", version=version)
        document = builders.minimal_valid_document()
        document["sections"] = [builders.make_section("sec-1", [block])]
        errors: list[BlockRenderError] = []
        render_document(document, registry=registry, lang="en", errors=errors)
        assert not errors, f"{block_type} v{version}: {[e.reason for e in errors]}"


# --- determinism (claude.md rule 35) -----------------------------------------


def test_rendering_is_byte_identical_across_runs(registry, doc):
    """Identical inputs, identical bytes -- the property the whole static
    build depends on. Catches attribute order drifting with dict iteration or
    anything sneaking a clock or a random id into the output."""
    first = render_document(doc, registry=registry, lang="en")
    second = render_document(doc, registry=registry, lang="en")
    assert first == second


def test_language_changes_the_text_but_not_the_structure(registry, doc):
    """Swapping language must not move a block or change the grid: the same
    document is the same layout in all three languages (rule 7)."""

    def skeleton(html):
        # Strip TEXT NODES and ATTRIBUTE VALUES, leaving tags and attribute
        # names. Stripping only text nodes is not enough: an accessible name
        # is a translated string that lives in an attribute (aria-label), so
        # a naive version of this test failed on correct output.
        without_values = re.sub(r'="[^"]*"', "=", html)
        return re.sub(r">[^<]*<", "><", without_values)

    renders = {lang: render_document(doc, registry=registry, lang=lang) for lang in LANGUAGES}
    skeletons = {lang: skeleton(html) for lang, html in renders.items()}
    assert len(set(skeletons.values())) == 1, "language changed the page structure"


def test_an_unknown_language_falls_back_rather_than_failing(registry, doc):
    assert render_document(doc, registry=registry, lang="de") == render_document(
        doc, registry=registry, lang="en"
    )


# --- per-block failure isolation ---------------------------------------------


def test_one_failing_block_does_not_cost_the_others(registry):
    """The point of isolation: a broken block must not blank the page. The
    document is built valid and then one block's type is swapped for one the
    registry does not know, which is the shape a stale document would have."""
    good = builders.make_block("hero", block_id="blk-good")
    bad = builders.make_block("hero", block_id="blk-bad")
    bad["type"] = "no_such_block_type"
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [good, bad])]

    errors: list[BlockRenderError] = []
    html = render_document(document, registry=registry, lang="en", errors=errors)

    assert 'data-block-id="blk-good"' in html, "the good block was lost"
    assert 'data-block-id="blk-bad"' in html, "the failed block vanished silently"
    assert "bp-block--failed" in html
    assert len(errors) == 1 and errors[0].block_id == "blk-bad"


def test_a_failed_block_reveals_no_internal_detail(registry):
    """A failure message can quote payload values, so what reaches the reader
    says which block failed and nothing about why."""
    bad = builders.make_block("hero", block_id="blk-bad")
    bad["type"] = "no_such_block_type"
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [bad])]
    html = render_document(document, registry=registry, lang="en")
    for leak in ("Traceback", "KeyError", "Exception", "src/pages"):
        assert leak not in html


def test_errors_are_collected_not_raised(registry):
    """render_document returns a page even when every block in it failed."""
    bad = builders.make_block("hero", block_id="blk-bad")
    bad["type"] = "no_such_block_type"
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [bad])]
    html = render_document(document, registry=registry, lang="en")  # no errors list
    assert "bp-page" in html


# --- escaping and URL safety (claude.md rules 22, 23) -------------------------


def test_text_is_escaped_even_though_the_schema_forbids_markup():
    """The schema rejects '<' and '>' anywhere in a document, so this can only
    fire if that guard is ever relaxed -- which is exactly why the renderer
    escapes independently. Defence in depth, deliberately redundant."""
    from src.pages.render import esc

    hostile = {"en": "<script>alert(1)</script>", "fr": "x", "nl": "x"}
    raw = text_in(hostile, "en")
    # text_in is a lookup, not a sanitiser -- it returns the string as stored.
    # Escaping happens on the way OUT, which is what the next two assertions
    # pin. Stating both halves keeps the division of responsibility explicit.
    assert raw == "<script>alert(1)</script>"
    assert "<" not in esc(raw)
    assert "&lt;script&gt;" in esc(raw)


@pytest.mark.parametrize(
    "href",
    [
        "javascript:alert(1)",
        "//evil.example",
        "https://evil.example",
        "data:text/html,x",
        "../../etc/passwd",
        "vbscript:x",
    ],
)
def test_unsafe_hrefs_are_refused(href):
    """No scheme, no host, no protocol-relative, no traversal. Anything that
    is not a site-relative path or an in-page fragment becomes '#'."""
    assert safe_href(href) == "#"


@pytest.mark.parametrize(
    "href, prefix, expected",
    [
        # A fragment means "here" and has no depth to correct, so it is
        # untouched at any prefix.
        ("#section-2", "", "#section-2"),
        ("#section-2", "../", "#section-2"),
        # A site-absolute href is REWRITTEN RELATIVE TO THE PAGE. It used to
        # pass through unchanged, which was wrong the moment a page document
        # contained a link: this site is served from
        # mdruszcz.github.io/belgian-macro-pipeline/, so a leading slash points
        # at the domain root and 404s. The first converted page with a menu had
        # six such links and every one was broken.
        ("/communes.html", "", "communes.html"),
        ("/communes.html", "../", "../communes.html"),
        ("/communes.html", "../../", "../../communes.html"),
        ("/local/12345/", "../", "../local/12345/"),
        # "/" strips to nothing, and an empty href means "this page" rather
        # than "the home page".
        ("/", "", "./"),
        ("/", "../", "../"),
    ],
)
def test_a_safe_href_is_rewritten_relative_to_the_page(href, prefix, expected):
    assert safe_href(href, prefix) == expected


def test_a_hostile_href_never_reaches_the_output(registry):
    block = builders.make_block("hero", block_id="blk-1")
    props = block.setdefault("props", {})
    if "cta" in props:
        props["cta"]["href"] = "javascript:alert(1)"
        document = builders.minimal_valid_document()
        document["sections"] = [builders.make_section("sec-1", [block])]
        html = render_document(document, registry=registry, lang="en")
        assert "javascript:" not in html


# --- the grid (docs/features/block_contract.md) -------------------------------


def test_layout_is_emitted_in_grid_units_for_all_three_breakpoints(registry):
    """Grid units, never pixels -- so a layout survives a redesign of the
    stylesheet. All three breakpoints travel in one inline style."""
    block = builders.make_block("hero", block_id="blk-1")
    block["layout"] = builders.layout(
        desktop=(1, 2, 6, 3), tablet=(0, 1, 4, 2), mobile=(0, 0, 4, 1)
    )
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [block])]
    html = render_document(document, registry=registry, lang="en")

    for token in ("--dx:1", "--dy:2", "--dw:6", "--dh:3", "--tw:4", "--mw:4"):
        assert token in html, f"{token} missing from the emitted grid style"
    assert "px" not in re.search(r'style="([^"]*)"', html).group(1)


def test_hidden_breakpoints_are_marked_not_dropped(registry):
    """A block hidden on mobile is still IN the document -- the builder's
    structure tree has to keep showing it. Hiding is presentation."""
    block = builders.make_block("hero", block_id="blk-1")
    block["visibility"] = builders.visibility(desktop=True, tablet=False, mobile=False)
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [block])]
    html = render_document(document, registry=registry, lang="en")
    assert 'data-block-id="blk-1"' in html
    assert "tablet" in re.search(r'data-hidden="([^"]*)"', html).group(1)
    assert "mobile" in re.search(r'data-hidden="([^"]*)"', html).group(1)


# --- states: a missing figure is never a blank or a zero ----------------------


def test_a_bound_block_without_data_is_not_rendered_as_ready(registry):
    """The failure this guards against: a block whose data never resolved
    rendering as an ordinary card with an empty value, so a missing figure
    looks like no figure rather than a gap."""
    block = builders.make_block("kpi_card", block_id="blk-1")
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [block])]
    html = render_document(document, registry=registry, lang="en")
    state = re.search(r'data-block-id="blk-1"[^>]*data-state="([a-z]+)"', html)
    assert state, "no state on a bound block"
    assert state.group(1) != "ready"
    assert "0" not in re.search(r'<div class="bp-value">([^<]*)</div>', html).group(1)


def test_a_resolved_state_is_carried_through(registry):
    """Batch 14 will pass the state it resolved -- suppressed, missing and the
    rest are distinct and must not be flattened here (claude.md rule 26)."""
    block = builders.make_block("kpi_card", block_id="blk-1")
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [block])]
    for state in ("ready", "missing", "suppressed", "unavailable", "error"):
        html = render_document(
            document, registry=registry, lang="en", data={"blk-1": {"state": state}}
        )
        found = re.search(r'data-block-id="blk-1"[^>]*data-state="([a-z]+)"', html)
        assert found.group(1) == state


# --- rich text is an allowlist, not a sanitiser -------------------------------


def test_rich_text_skips_node_types_it_does_not_know(registry):
    """There is no node that carries markup, and an unrecognised node is
    dropped rather than passed through."""
    block = builders.make_block("rich_text", block_id="blk-1")
    block["props"]["content"] = [
        {"node": "paragraph", "text": {"en": "kept", "fr": "kept", "nl": "kept"}},
        {"node": "raw_html", "html": "<script>alert(1)</script>"},
    ]
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [block])]
    html = render_document(document, registry=registry, lang="en")
    assert "kept" in html
    assert "script" not in html
    assert "alert" not in html


# --- rule 36: the renderer names no indicator ---------------------------------


def test_the_renderer_names_no_indicator_and_no_commune():
    """The renderer is generic by construction: an indicator id in here would
    mean a block type that only works for one dataset (claude.md rule 24)."""
    from pathlib import Path

    source = (Path(__file__).resolve().parents[2] / "src" / "pages" / "render.py").read_text(
        encoding="utf-8"
    )
    hits = re.findall(r"\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+){2,}\b", source)
    assert not hits, f"render.py names something indicator-shaped: {hits}"


def test_a_stateful_block_is_wired_into_the_state_component(registry):
    """data-state alone does nothing: components.css keys its seven states off
    the `bp-state` class plus a body/message pair. Without them a KPI whose
    data had not resolved rendered as an ordinary card with an empty value --
    a missing figure looking like no figure, which is the exact failure the
    states exist to prevent."""
    block = builders.make_block("kpi_card", block_id="blk-1")
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [block])]
    html = render_document(document, registry=registry, lang="en")
    assert "bp-state" in html
    assert "bp-state-body" in html
    assert "bp-state-message" in html


def test_a_block_that_cannot_carry_data_gets_no_state_wrapper(registry):
    """A hero has no data to be missing, so dressing it in a state component
    would be noise -- and a skeleton shimmer on static text reads as broken."""
    block = builders.make_block("hero", block_id="blk-1")
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [block])]
    html = render_document(document, registry=registry, lang="en")
    assert "bp-state-body" not in html


def test_a_ready_block_carries_no_state_message(registry):
    """A real measured zero is `ready`. If ready printed a message beside the
    figure, a zero would read as an absence -- claude.md rule 26."""
    block = builders.make_block("kpi_card", block_id="blk-1")
    document = builders.minimal_valid_document()
    document["sections"] = [builders.make_section("sec-1", [block])]
    html = render_document(
        document,
        registry=registry,
        lang="en",
        data={"blk-1": {"state": "ready", "formatted_value": "0"}},
    )
    assert "bp-state-message" not in html
    assert ">0<" in html


@pytest.mark.parametrize("lang", LANGUAGES)
def test_state_messages_exist_in_every_language(lang):
    """A state word missing in one language would silently fall back to
    English on a page that is otherwise translated (rule 7)."""
    from src.pages.render import STATE_TEXT

    for state, table in STATE_TEXT.items():
        assert table.get(lang), f"{state} has no {lang} text"
