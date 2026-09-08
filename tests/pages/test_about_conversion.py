"""Batch 15a: is the block-built page equivalent to the hand-built one?

The batch's own rule is that the hand-built page stays live until the block
version is "proven equivalent or better". Nothing in this repository could
prove that -- there is no diff tooling and no committed baseline -- so this
file is what "proven" means for a conversion.

IT COMPARES CONTENT AND URLs, NOT PIXELS. What protects a reader is that the
same words, the same links and the same figures appear, not that the layout
matches to a threshold. A pixel gate would also be measuring the wrong thing
here: the Batch 12 audit recorded that preview typography falls back to a
different font because the CSP blocks Google Fonts, so a diff would flag the
tooling's difference as the page's.

WHAT THIS FILE CANNOT PROVE. about.html carries no municipal figure, so this
conversion exercises the publish path and NOT the resolver. The data-
equivalence assertions below are written anyway, so the next page inherits
them; they are trivially satisfied here.
"""

from __future__ import annotations

import html
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HAND_BUILT = REPO_ROOT / "about.html"
DOCUMENT = REPO_ROOT / "config" / "pages" / "about" / "published.json"
BUILT = REPO_ROOT / "preview" / "about.html"

EXPORTER = REPO_ROOT / "scripts" / "export_page_documents.py"


def _build() -> str:
    """Run the real exporter and return what it wrote."""
    result = subprocess.run(
        [sys.executable, str(EXPORTER)], capture_output=True, text=True, cwd=REPO_ROOT
    )
    assert result.returncode == 0, result.stderr
    return BUILT.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def built() -> str:
    return _build()


def _visible_text(markup: str) -> str:
    """Rendered text with tags, scripts and styles removed."""
    without = re.sub(r"<(script|style)\b.*?</\1>", " ", markup, flags=re.S | re.I)
    return html.unescape(re.sub(r"<[^>]+>", " ", without))


def _hand_built_prose() -> list[str]:
    """The sentences about.html actually shows, from its own translations
    object -- read, never retyped, so this test cannot drift from the page."""
    source = HAND_BUILT.read_text(encoding="utf-8")
    blob = re.search(r"const translations = (\{.*?\n        \});", source, re.S).group(1)
    as_json = re.sub(r"^(\s*)([a-z0-9_]+):", r'\1"\2":', blob, flags=re.M)
    english = json.loads(as_json)["en"]
    return [
        english["title"],
        english["subtitle"],
        english["text1"],
        english["text2"],
        *english["features"],
    ]


# --- content equivalence ---------------------------------------------------


def test_every_sentence_of_the_hand_built_page_survives_the_conversion(built):
    """The point of the whole batch. A conversion that drops a paragraph has
    not converted the page, however well it renders."""
    text = " ".join(_visible_text(built).split())
    missing = [s for s in _hand_built_prose() if " ".join(s.split()) not in text]
    assert not missing, f"the block version does not say: {missing}"


def test_the_document_carries_all_three_languages(built):
    """Rule 7. about.html is trilingual; a conversion that keeps only English
    would quietly drop two thirds of its audience, and the loss would be
    invisible in an English review."""
    doc = json.loads(DOCUMENT.read_text(encoding="utf-8"))
    hero = doc["sections"][0]["blocks"][0]["props"]["heading"]
    assert set(hero) == {"en", "fr", "nl"}
    assert len({hero["en"], hero["fr"], hero["nl"]}) == 3, "three real translations, not one copied"


def test_every_link_in_the_hand_built_page_still_exists(built):
    """The beginning of the URL inventory rule 31 requires and nothing
    currently enforces (known-risks.md:37). about.html has no outbound links
    today, so this asserts the comparison rather than a list -- and it starts
    failing the moment a converted page drops one."""
    hand = set(re.findall(r'href="([^"#]+)"', HAND_BUILT.read_text(encoding="utf-8")))
    hand = {h for h in hand if not h.startswith(("http", "//"))}
    built_links = set(re.findall(r'href="([^"#]+)"', built))
    built_links = {link.lstrip("./") for link in built_links}
    missing = {h for h in hand if h.lstrip("./") not in built_links}
    assert not missing, f"links present on the hand-built page and lost: {missing}"


# --- the page is a real page ------------------------------------------------


def test_the_built_page_has_exactly_one_h1(built):
    """A published page with no h1 fails a screen reader's document outline and
    every SEO check. The renderer emitted h2 for a hero -- correct-looking
    while nothing published a document, wrong the moment something did."""
    assert len(re.findall(r"<h1\b", built)) == 1


def test_the_built_page_has_a_title_and_a_canonical(built):
    assert re.search(r"<title>[^<]+</title>", built)
    assert re.search(r'<link rel="canonical" href="https?://[^"]+"', built)


def test_asset_links_resolve_from_the_page_s_own_directory(built):
    """A page one directory down linking "assets/..." asks for
    /preview/assets/..., which does not exist: every stylesheet 404s while the
    page still renders. export_local_pages.py records the same trap."""
    local = [
        href
        for href in re.findall(r'<link rel="stylesheet" href="([^"]+)"', built)
        if not href.startswith(("http://", "https://", "//"))
    ]
    assert local, "the page must link the design system"
    for href in local:
        resolved = (BUILT.parent / href).resolve()
        assert resolved.is_file(), f"{href} does not resolve to a real file from {BUILT.parent}"


def test_the_page_links_the_font_the_design_system_declares(built):
    """tokens.css says a page using it must link the font itself -- the
    stylesheet declares the variable and cannot fetch a face. Without it the
    page silently falls back to IBM Plex Sans: legitimate, but not the look,
    and invisible in any test that only reads the markup."""
    assert "fonts.googleapis.com" in built


def test_the_hand_built_page_is_untouched():
    """The cutover gate: the block version ships BESIDE the live page, never
    over it. If this fails, the batch has done the one thing it promised not
    to."""
    result = subprocess.run(
        ["git", "diff", "--stat", "HEAD", "--", "about.html"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    assert result.stdout.strip() == "", f"about.html was modified: {result.stdout}"


# --- properties every future conversion inherits ----------------------------


def test_building_twice_produces_identical_bytes():
    """Rule 35. Anything putting a timestamp or an unsorted dict into a page
    breaks this, and nothing visibly fails when it does."""
    first = _build()
    second = _build()
    assert first == second


def test_a_page_carrying_municipal_data_without_attribution_is_refused():
    """Statbel's licence terminates automatically on non-compliance, and this
    project has published municipal figures without attribution once already.
    The refusal lives in the shell so every future exporter inherits it."""
    from src.pages.shell import ShellError, wrap

    doc = json.loads(DOCUMENT.read_text(encoding="utf-8"))
    doc["sections"][0]["blocks"][0] = {
        "id": "kpi-1",
        "type": "kpi_card",
        "binding": {"provider": "municipal", "indicator": "X", "operation": "latest"},
    }
    with pytest.raises(ShellError, match="attribution"):
        wrap("<div></div>", doc, lang="en", canonical="https://example.test/x", attribution=None)


def test_a_route_that_escapes_the_repository_is_refused():
    """The route was allowlisted by the validator, but the check that matters
    is the one on the path actually used -- the lesson the Batch 14 audit
    taught about the payload reader."""
    from src.pages.shell import ShellError

    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from export_page_documents import output_path_for

    for route in ("/../escape.html", "not-a-route", "/a/../../b.html"):
        with pytest.raises(ShellError):
            output_path_for(route)
