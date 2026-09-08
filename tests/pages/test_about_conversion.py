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

from src.pages.strings import LANGS
from src.site.routes import path_for, route_for

REPO_ROOT = Path(__file__).resolve().parents[2]
#: THE FROZEN HAND-BUILT PAGE, not the live file.
#:
#: Batch 15d cut this page over: `about.html` is now GENERATED, so comparing
#: the built page against `REPO_ROOT/"about.html"` would compare a file to
#: itself. Every assertion below would pass while proving nothing, and the
#: only equivalence guarantee in this programme would disappear silently at
#: the moment it started to matter. `docs/steps:902` calls this step "freeze
#: the hand-built version"; this file IS that freeze, committed before the
#: route moved.
#:
#: It is deliberately never regenerated. It is the record of what readers had
#: before, and a fixture that tracked the thing it audits is not a fixture.
HAND_BUILT = REPO_ROOT / "tests" / "fixtures" / "pages" / "about-hand-built.html"
DOCUMENT = REPO_ROOT / "config" / "pages" / "about" / "published.json"
#: The canonical route this page publishes at. Its translations are derived
#: through the inventory rather than spelled out, so a future move updates one
#: place.
BUILT_ROUTE = "/about.html"
BUILT = REPO_ROOT / "about.html"

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


def _hand_built_prose(lang: str = "en") -> list[str]:
    """The sentences about.html actually showed IN `lang`, from its own
    translations object -- read, never retyped, so this test cannot drift from
    the page it audits.

    The frozen page carried all three languages in that object. Checking only
    English would have left the exact failure docs/steps names as the gate on
    this cutover -- "drop two thirds of the audience invisibly" -- undetectable:
    paste the English paragraph into the `fr` key and every test stays green
    while the French page ships English prose.
    """
    source = HAND_BUILT.read_text(encoding="utf-8")
    blob = re.search(r"const translations = (\{.*?\n        \});", source, re.S).group(1)
    as_json = re.sub(r"^(\s*)([a-z0-9_]+):", r'\1"\2":', blob, flags=re.M)
    table = json.loads(as_json)[lang]
    return [
        table["title"],
        table["subtitle"],
        table["text1"],
        table["text2"],
        *table["features"],
    ]


def _built_in(lang: str) -> str:
    """The published page in `lang`, at the URL the inventory says it lives at."""
    return path_for(route_for(BUILT_ROUTE, lang)).read_text(encoding="utf-8")


# --- content equivalence ---------------------------------------------------


@pytest.mark.parametrize("lang", LANGS)
def test_every_sentence_of_the_hand_built_page_survives_the_conversion(built, lang):
    """The point of the whole batch, in every language it was published in.

    A conversion that drops a paragraph has not converted the page, however
    well it renders -- and a conversion that drops it only in Dutch is worse,
    because nobody reviewing in English will ever see it.
    """
    text = " ".join(_visible_text(_built_in(lang)).split())
    missing = [s for s in _hand_built_prose(lang) if " ".join(s.split()) not in text]
    assert not missing, f"the {lang} page does not say: {missing}"


@pytest.mark.parametrize("lang", ["fr", "nl"])
def test_a_translated_page_is_not_quietly_serving_english(built, lang):
    """The specific way rule 7 breaks in a build pipeline: the structure is
    perfect, all three files exist, all three differ (in `<html lang>`, title
    and canonical) -- and the body is English three times over."""
    text = " ".join(_visible_text(_built_in(lang)).split())
    english_only = [
        s
        for s, translated in zip(_hand_built_prose("en"), _hand_built_prose(lang), strict=True)
        if s != translated and " ".join(s.split()) in text
    ]
    assert not english_only, f"the {lang} page still shows English: {english_only}"


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


def test_the_frozen_hand_built_page_is_never_regenerated():
    """The fixture is the record of what readers had before the cutover.

    This used to assert `git diff --stat about.html` was empty -- the hand-built
    page had to stay untouched while the block version shipped beside it. Batch
    15d cut it over, so about.html is GENERATED now and that assertion would
    forbid the very change it was guarding.

    The property that survives is the one that matters: the copy this file
    audits against must never become a copy of the thing it audits. Asserted on
    CONTENT rather than on git state, deliberately -- a `git diff` check passes
    the moment someone stages the regenerated file, and staging is exactly what
    happens on the way to a commit.
    """
    frozen = HAND_BUILT.read_text(encoding="utf-8")

    # Markers only the hand-built page has. The renderer emits none of them.
    assert "const translations = {" in frozen, "the frozen page lost its own translations table"
    assert 'class="feature-icon"' in frozen
    assert "aboutTagline" in frozen

    # Markers only a generated page has. Any of them means it was overwritten.
    for generated_only in ('class="bp-page"', 'class="bp-block', 'class="bp-lang-switch"'):
        assert generated_only not in frozen, (
            f"the frozen page contains {generated_only} -- it has been overwritten "
            "with the generated page it is supposed to be compared against"
        )


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
