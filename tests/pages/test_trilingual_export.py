"""Batch 15c: are the block-built pages really three pages, or one page thrice?

Batches 15a and 15b built the publish path and proved it on a real choropleth,
in ENGLISH. The documents were never the problem -- every user-facing string in
`config/pages/*/published.json` has carried `{en, fr, nl}` since Batch 9. The
exporter threw two thirds of it away: it looped over documents and not over
languages, and `output_path_for(route)` returned the same path whichever
language was asked for, so `--lang fr` did not publish a French page. It
overwrote the English one.

THE DEFECT THIS FILE EXISTS FOR IS NOT "MISSING TRANSLATION". It is a French
page that looks finished and carries an ENGLISH LICENCE NOTICE. The notice used
to be lifted out of `communes.html`, which has exactly one language. Statbel's
2015 licence terminates automatically on non-compliance, this project has had
one attribution breach already, and nothing about an English notice on a French
page looks broken. So the notice assertions below compare against
`assets/i18n.js` itself, never against a string typed into this file (rule 36).

The rest follows `docs/features/i18n.md:59-69`, which had already decided how
three languages are published and was simply never applied here: `hreflang`
naming all three plus `x-default`, each page's `canonical` being ITSELF, and a
link prefix computed from the language.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPORTER = REPO_ROOT / "scripts" / "export_page_documents.py"
SITE_BASE = "https://mdruszcz.github.io/belgian-macro-pipeline"

sys.path.insert(0, str(REPO_ROOT))

from scripts.export_page_documents import asset_prefix_for, route_for  # noqa: E402
from src.pages.strings import DEFAULT_LANG, LANGS, interface_strings  # noqa: E402

#: The pages built today, by page id. Read from the documents rather than
#: listed, so a new conversion is covered the day it lands.
PAGE_IDS = sorted(
    d.name for d in (REPO_ROOT / "config" / "pages").iterdir() if (d / "published.json").is_file()
)


def _build() -> None:
    result = subprocess.run(
        [sys.executable, str(EXPORTER)], capture_output=True, text=True, cwd=REPO_ROOT
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture(scope="module", autouse=True)
def built() -> None:
    _build()


@pytest.fixture(scope="module")
def strings() -> dict:
    return interface_strings()


def _page(page_id: str, lang: str) -> str:
    import json

    doc = json.loads(
        (REPO_ROOT / "config" / "pages" / page_id / "published.json").read_text("utf-8")
    )
    route = route_for(doc["route"], lang)
    path = REPO_ROOT / route.lstrip("/")
    assert path.is_file(), f"{page_id} has no {lang} page at {route}"
    return path.read_text(encoding="utf-8")


ALL_PAGES = [(page_id, lang) for page_id in PAGE_IDS for lang in LANGS]


# --- route_for: the URL shape, chosen once ----------------------------------


@pytest.mark.parametrize(
    "route, lang, expected",
    [
        ("/preview/about.html", "en", "/preview/about.html"),
        ("/preview/about.html", "fr", "/preview/fr/about.html"),
        ("/preview/about.html", "nl", "/preview/nl/about.html"),
        # The shape a canonical cutover will produce.
        ("/about.html", "fr", "/fr/about.html"),
        # Directory routes keep the shape the 1,695 commune pages already use.
        ("/local/11001/", "en", "/local/11001/"),
        ("/local/11001/", "fr", "/local/11001/fr/"),
    ],
)
def test_the_language_is_a_directory_immediately_above_the_file(route, lang, expected):
    """Rule 31 makes this a one-time choice, so it is pinned rather than left
    to whatever the implementation happened to do. English never moves: those
    URLs are indexed and linked."""
    assert route_for(route, lang) == expected


def test_english_keeps_the_url_it_already_has():
    for page_id in PAGE_IDS:
        import json

        declared = json.loads(
            (REPO_ROOT / "config" / "pages" / page_id / "published.json").read_text("utf-8")
        )["route"]
        assert route_for(declared, DEFAULT_LANG) == declared


def test_a_translated_page_is_one_directory_deeper_and_its_prefix_says_so():
    """A wrong prefix breaks every asset while the page still renders. That
    exact failure has now happened three times in this programme -- stylesheets,
    block payloads, and the map's boundary file -- so it gets an assertion
    rather than an assumption."""
    assert asset_prefix_for("/preview/about.html") == "../"
    assert asset_prefix_for("/preview/fr/about.html") == "../../"


# --- every language actually gets a page ------------------------------------


@pytest.mark.parametrize("page_id, lang", ALL_PAGES)
def test_each_page_exists_in_each_language(page_id, lang):
    assert _page(page_id, lang)


@pytest.mark.parametrize("page_id, lang", ALL_PAGES)
def test_the_html_lang_matches_the_route(page_id, lang):
    assert f'<html lang="{lang}">' in _page(page_id, lang)


def test_the_three_languages_actually_differ():
    """Three identical files would satisfy every structural assertion above
    and translate nothing."""
    for page_id in PAGE_IDS:
        rendered = {lang: _page(page_id, lang) for lang in LANGS}
        assert len(set(rendered.values())) == 3, f"{page_id} renders the same in all languages"


@pytest.mark.parametrize("page_id", PAGE_IDS)
def test_a_language_changes_the_text_but_not_the_structure(page_id):
    """Rule 7 is about words, not layout. The same document must be the same
    page in all three languages -- the property
    tests/pages/test_page_document_render.py:91 asserts on the fragment,
    asserted here on the finished file."""
    skeletons = {}
    for lang in LANGS:
        tags = re.findall(r"<(/?[a-zA-Z][a-zA-Z0-9-]*)", _page(page_id, lang))
        skeletons[lang] = tags
    assert skeletons["fr"] == skeletons["en"]
    assert skeletons["nl"] == skeletons["en"]


# --- hreflang and canonical -------------------------------------------------


@pytest.mark.parametrize("page_id, lang", ALL_PAGES)
def test_every_page_declares_every_language_plus_x_default(page_id, lang):
    """docs/features/i18n.md:61 -- without this a search engine treats the
    three as duplicates COMPETING with each other, and generating them makes
    the ranking worse rather than better."""
    import json

    page = _page(page_id, lang)
    declared = json.loads(
        (REPO_ROOT / "config" / "pages" / page_id / "published.json").read_text("utf-8")
    )["route"]
    for other in LANGS:
        expected = f'<link rel="alternate" hreflang="{other}" href="{SITE_BASE}{route_for(declared, other)}">'
        assert expected in page, f"{page_id}/{lang} does not name {other}"
    default = f'<link rel="alternate" hreflang="x-default" href="{SITE_BASE}{declared}">'
    assert default in page


@pytest.mark.parametrize("page_id, lang", ALL_PAGES)
def test_each_page_is_its_own_canonical(page_id, lang):
    """Three real pages, not three views of one (i18n.md:63). A French page
    canonicalising to English asks a search engine to drop it."""
    import json

    declared = json.loads(
        (REPO_ROOT / "config" / "pages" / page_id / "published.json").read_text("utf-8")
    )["route"]
    expected = f'<link rel="canonical" href="{SITE_BASE}{route_for(declared, lang)}">'
    assert expected in _page(page_id, lang)


# --- the licence notice, per language ---------------------------------------


@pytest.mark.parametrize("lang", LANGS)
def test_a_page_publishing_municipal_data_carries_that_language_s_notice(lang, strings):
    """THE POINT OF THIS BATCH. The notice used to be lifted from
    communes.html, which has one language, so a French page would have carried
    an English licence condition. Compared against assets/i18n.js, never
    against a string typed here (rule 36)."""
    page = _page("map", lang)
    notice = strings[lang]["attribution"]
    assert notice in page, f"the {lang} map page does not carry the {lang} licence notice"
    for other in LANGS:
        if other != lang:
            assert strings[other]["attribution"] not in page


def test_the_shell_refuses_a_municipal_page_with_no_notice_in_any_language():
    """The refusal is what makes reading the notice from one place safe."""
    from src.pages.shell import ShellError, wrap
    from tests.fixtures.pages import builders

    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"].append(
        builders.make_block(
            "kpi_card",
            block_id="kpi-1",
            props=builders.props_for("kpi_card"),
            binding=builders.municipal_binding("POPULATION_BY_COMMUNE", nis="11001"),
        )
    )
    for lang in LANGS:
        with pytest.raises(ShellError, match="licence"):
            wrap("<div></div>", doc, lang=lang, canonical="https://example.invalid/x.html")


def test_the_strings_reader_refuses_a_file_with_no_notice(tmp_path):
    """The guard itself, against a strings file that parses and publishes
    nothing. `scripts/export_local_pages.py` has the same test against the same
    function -- it is shared now, and this asserts the page-document side
    reaches it too."""
    stub = tmp_path / "i18n.js"
    stub.write_text(
        "const I={LANGS:['en','fr','nl'],STRINGS:{en:{},fr:{},nl:{}}};module.exports=I;\n",
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="no licence notice"):
        interface_strings(stub)


# --- the switcher -----------------------------------------------------------


@pytest.mark.parametrize("page_id, lang", ALL_PAGES)
def test_the_switcher_reaches_every_language_without_javascript(page_id, lang):
    """Plain links, so switching works with scripting off. The generated
    commune pages have NO switcher -- a reader on /local/85039/fr/ cannot reach
    the Dutch version at all -- and the hand-built pages switch only in the
    browser."""
    page = _page(page_id, lang)
    nav = re.search(r'<nav class="bp-lang-switch".*?</nav>', page, re.S)
    assert nav, f"{page_id}/{lang} has no language switcher"
    for other in LANGS:
        assert f'data-lang="{other}"' in nav.group(0)
    assert nav.group(0).count('aria-current="page"') == 1
    assert f'lang="{lang}"' in nav.group(0)


@pytest.mark.parametrize("page_id, lang", ALL_PAGES)
def test_the_switcher_links_are_relative_and_resolve_on_disk(page_id, lang):
    """An absolute href would walk a reader off whatever server they are on --
    a local checkout, a fork's Pages, a review build."""
    import json

    page = _page(page_id, lang)
    nav = re.search(r'<nav class="bp-lang-switch".*?</nav>', page, re.S).group(0)
    declared = json.loads(
        (REPO_ROOT / "config" / "pages" / page_id / "published.json").read_text("utf-8")
    )["route"]
    here = (REPO_ROOT / route_for(declared, lang).lstrip("/")).parent
    for href in re.findall(r'href="([^"]+)"', nav):
        assert not href.startswith(("http://", "https://", "/")), f"{href} is not relative"
        assert (here / href).resolve().is_file(), f"{href} does not resolve from {here}"


# --- the exporter's own behaviour -------------------------------------------


def test_lang_restricts_what_is_built_and_never_overwrites_another_language():
    """Before this batch `--lang fr` wrote the FRENCH page over the ENGLISH
    file, because the output path did not vary with language. That is a defect
    a reader sees and no test caught."""
    english = _page("about", "en")
    result = subprocess.run(
        [sys.executable, str(EXPORTER), "--lang", "fr"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, result.stderr
    assert _page("about", "en") == english
    # Derived, not typed: this assertion named "preview/fr/about.html" until
    # Batch 15d moved the route, and a literal path in a test is a second
    # inventory that drifts from the first.
    assert route_for("/about.html", "fr").lstrip("/") in result.stdout
    assert route_for("/about.html", "nl").lstrip("/") not in result.stdout
    _build()


def test_every_language_rebuilds_byte_identically():
    """claude.md rule 35."""
    before = {(p, lang): _page(p, lang) for p, lang in ALL_PAGES}
    _build()
    assert {(p, lang): _page(p, lang) for p, lang in ALL_PAGES} == before


def test_the_committed_pages_match_a_fresh_build():
    """`--check` exits 1 if anything would change, so a stale committed page
    fails here rather than shipping."""
    result = subprocess.run(
        [sys.executable, str(EXPORTER), "--check"], capture_output=True, text=True, cwd=REPO_ROOT
    )
    assert result.returncode == 0, result.stdout


def test_the_hand_built_page_that_is_still_hand_built_is_untouched():
    result = subprocess.run(
        # map.html ONLY. about.html was cut over in Batch 15d and is generated
        # now; tests/pages/test_about_conversion.py guards its frozen copy
        # instead. One cutover at a time, and the map is the page with figures
        # on it.
        ["git", "diff", "--stat", "--", "map.html"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    assert result.stdout.strip() == "", result.stdout
