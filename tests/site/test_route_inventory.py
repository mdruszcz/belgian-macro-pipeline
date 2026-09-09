"""Rule 31, enforced mechanically for the first time.

claude.md rule 31: "Every existing canonical and legacy URL remains valid after
the redesign." docs/implementation/known-risks.md has carried this as an open
P1 since 2026-09-07 -- nothing in tests/ asserted that any public URL resolves,
across 1,695 commune pages in three languages plus the root pages. The Batch 17
spec calls the inventory "the most valuable thing in this batch […] worth
keeping even if everything else here is reverted."

THIS FILE WENT RED THE MOMENT IT WAS WRITTEN, which is the only reason to
believe it. index.html's navigation loads clock.html, news.html and
analysis.html into its iframe; none of the three exists, and all three returned
404 on the live site. Three of the five menu items on the front page, broken,
with a full green suite. That is what "enforced by nobody" looked like in
practice.

WHY LINKS AND NOT JUST FILES. An inventory of what exists cannot catch a link
to something that does not. Both directions are checked here: every URL the
inventory names resolves, AND every link a published page makes resolves.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from xml.etree import ElementTree

import pytest

from src.pages.semantics import ROUTE_EXACT
from src.pages.strings import LANGS
from src.site.routes import (
    NOINDEX_PREFIXES,
    ROOT_PAGES,
    SITE_BASE,
    all_routes,
    block_page_routes,
    commune_routes,
    is_indexable,
    path_for,
    root_routes,
    route_for,
    sitemap_routes,
    translations_of,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Attributes that name a URL the browser will actually fetch. Applied to
#: MARKUP ONLY -- script bodies are stripped first, because `href = 'local/' +
#: nis + '/'` is a prefix in a concatenation, not a link, and reporting it
#: would train a reader to ignore this test.
_LINK = re.compile(r"""\b(?:href|src)\s*=\s*["']([^"']+)["']""")

_SCRIPT_OR_STYLE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.S | re.I)

#: A bare page filename quoted inside a script -- "clock.html". Deliberately
#: narrow: no slash, no `${`, nothing that could be one half of a concatenated
#: URL. index.html's navigation is an ARRAY of these, which is why the markup
#: check above cannot see it and why this exists.
_SCRIPT_PAGE = re.compile(r"""["']([A-Za-z0-9_-]+\.html)["']""")

#: JavaScript comments. Stripped before scanning for code, because a comment
#: explaining a bug naturally quotes the buggy code -- the note above
#: index.html's dashboards array names the very call it warns against, and
#: without this that note would fail the test it exists to explain.
_JS_COMMENT = re.compile(r"//[^\n]*|/\*.*?\*/", re.S)

#: A commune sample for the link check. Every commune is inventoried and
#: existence-checked below; the LINK check reads and parses whole files, so it
#: runs over a spread rather than all 1,695 -- the pages are generated from one
#: template, so a systematic gap shows up in any of them.
_COMMUNE_LINK_SAMPLE = 12


def _pages_to_link_check() -> list[Path]:
    pages = [REPO_ROOT / name for name in sorted(p.name for p in REPO_ROOT.glob("*.html"))]
    pages += [path_for(route) for route in block_page_routes()]
    communes = list(commune_routes())
    step = max(1, len(communes) // _COMMUNE_LINK_SAMPLE)
    pages += [path_for(route) for route in communes[::step]]
    return [p for p in pages if p.is_file()]


def _internal_links(source: str) -> list[str]:
    """Every same-site URL the page fetches or navigates to.

    Skips protocol-relative and absolute-scheme URLs (another host's problem),
    in-page fragments, template placeholders a script fills in at runtime, and
    the query string on a link whose path is what matters -- local.html?nis=
    11001 is a link to local.html.
    """
    markup = _SCRIPT_OR_STYLE.sub(" ", source)
    out = []
    for raw in _LINK.findall(markup):
        target = raw.strip()
        if not target or target.startswith(("#", "data:", "mailto:", "javascript:")):
            continue
        if "://" in target or target.startswith("//") or "${" in target:
            continue
        out.append(target.split("#", 1)[0].split("?", 1)[0])
    return [t for t in out if t]


def _script_page_targets(source: str) -> list[str]:
    """Bare page filenames a script navigates to.

    index.html's whole navigation is a JavaScript array of these, so the markup
    check above sees NOTHING -- and that is where three dead links lived.
    """
    out = []
    for block in _SCRIPT_OR_STYLE.finditer(source):
        if block.group(1).lower() != "script":
            continue
        out.extend(_SCRIPT_PAGE.findall(_JS_COMMENT.sub(" ", block.group(0))))
    return out


# --- the inventory resolves -------------------------------------------------


@pytest.mark.parametrize("route", root_routes())
def test_every_root_route_resolves(route):
    """The frozen list, asserted literally. A rename fails here rather than
    404ing for a reader who bookmarked it."""
    assert path_for(route).is_file(), f"{route} is inventoried but does not exist"


@pytest.mark.parametrize("route", commune_routes())
def test_every_commune_route_resolves(route):
    """Parameterised over all 1,695 rather than sampled: the Batch 17 spec is
    explicit that this is the scale at which a spot check misses a systematic
    gap."""
    assert path_for(route).is_file()


@pytest.mark.parametrize("route", block_page_routes())
def test_every_block_page_route_resolves(route):
    assert path_for(route).is_file()


def test_the_inventory_is_not_accidentally_empty():
    """Every assertion above is parameterised, and a parameterised test over an
    empty list passes silently. This is what stops the whole file becoming
    decorative if the derivation ever breaks."""
    assert len(commune_routes()) > 1000
    assert len(block_page_routes()) == len(LANGS) * 2

    # all_routes() DEDUPES, and the overlap is real rather than a bug: since
    # Batch 15d, /about.html is both a root page (frozen, so a rename fails
    # loudly) and a block-built one (generated). Asserting the union and the
    # duplicate count separately keeps this honest -- a plain sum would have
    # to be "fixed" every time a page is cut over, which is how a guard turns
    # into a formality.
    parts = root_routes() + commune_routes() + block_page_routes()
    assert set(all_routes()) == set(parts)
    assert len(all_routes()) == len(set(parts))
    overlap = set(root_routes()) & set(block_page_routes())
    assert overlap == {"/about.html"}, f"unexpected overlap: {sorted(overlap)}"


# --- every link resolves ----------------------------------------------------


@pytest.mark.parametrize(
    "page", _pages_to_link_check(), ids=lambda p: str(p.relative_to(REPO_ROOT))
)
def test_every_internal_link_on_a_published_page_resolves(page):
    """THE TEST THAT FOUND THE LIVE 404s.

    A link is a promise the page makes to a reader. index.html promised three
    pages that were never built, and nothing anywhere noticed.
    """
    source = page.read_text(encoding="utf-8")
    broken = []
    for target in _internal_links(source):
        resolved = (page.parent / target).resolve()
        if not (resolved.is_file() or (resolved / "index.html").is_file()):
            broken.append(target)
    assert not broken, f"{page.relative_to(REPO_ROOT)} links to {broken}, which do not exist"


@pytest.mark.parametrize(
    "page", _pages_to_link_check(), ids=lambda p: str(p.relative_to(REPO_ROOT))
)
def test_every_page_a_script_navigates_to_exists(page):
    """THE TEST THAT FOUND THE LIVE 404s ON THE FRONT PAGE.

    index.html's nav is a JavaScript array -- dashboard, clock, news, analysis,
    about -- loaded into an iframe. Three of those five files were never built,
    and all three returned 404 on the live site while every test in this
    repository was green.
    """
    source = page.read_text(encoding="utf-8")
    broken = sorted(
        {name for name in _script_page_targets(source) if not (page.parent / name).is_file()}
    )
    assert not broken, f"{page.relative_to(REPO_ROOT)} navigates to {broken}, which do not exist"


def test_index_never_asks_for_a_dashboard_it_does_not_have():
    """The other half of the same defect.

    index.html's nav is positional -- `loadDashboard(2)` indexes an array of
    filenames -- and `window.onload` called `loadDashboard(2)` because index 2
    once meant "News". news.html has never existed, so THE FRONT PAGE LOADED A
    404 INTO ITS OWN IFRAME ON EVERY VISIT, while the nav highlighted a
    different tab entirely. Removing the dead entries would have turned that
    into an out-of-range index, which is a quieter version of the same bug.

    A positional index is a footgun; this test is the safety catch.
    """
    source = _JS_COMMENT.sub(" ", (REPO_ROOT / "index.html").read_text(encoding="utf-8"))
    array = re.search(r"const dashboards = \[(.*?)\]", source, re.S)
    assert array, "index.html no longer declares a `dashboards` array"
    size = len(_SCRIPT_PAGE.findall(array.group(1)))
    assert size, "the dashboards array is empty"
    requested = {int(n) for n in re.findall(r"loadDashboard\((\d+)\)", source)}
    assert requested, "nothing calls loadDashboard, so the nav is dead"
    out_of_range = sorted(n for n in requested if n >= size)
    assert (
        not out_of_range
    ), f"index.html calls loadDashboard{out_of_range} but only has {size} dashboards"


# --- the inventory is complete ----------------------------------------------


def test_every_root_html_file_is_in_the_inventory():
    """A new root page must be a decision, not an accident. Adding one without
    a row here fails, which forces someone to say whether it belongs in the
    sitemap and why."""
    on_disk = {f"/{p.name}" for p in REPO_ROOT.glob("*.html")}
    inventoried = set(root_routes())
    assert on_disk - inventoried == set(), "root pages missing from ROOT_PAGES"


def test_every_inventoried_page_records_why_it_is_or_is_not_submitted():
    for page in ROOT_PAGES:
        assert page.reason.strip(), f"{page.route} has no recorded reason"


def test_the_inventory_agrees_with_the_page_document_route_allowlist():
    """`src/pages/semantics.py:ROUTE_EXACT` is a second route list, governing
    what a page document may declare. Two inventories that disagree are worse
    than one: before this test it omitted /home.html and /commune.html, both
    live served pages, which known-risks.md:41 recorded and nothing enforced.
    """
    missing = set(root_routes()) - set(ROUTE_EXACT)
    assert not missing, f"ROUTE_EXACT does not know about {sorted(missing)}"


# --- indexing ---------------------------------------------------------------


@pytest.mark.parametrize("route", [r for r in block_page_routes() if not is_indexable(r)])
def test_a_preview_page_tells_crawlers_not_to_index_it(route):
    """A page still under /preview/ duplicates a live page word for word and
    carries a self-referencing canonical, while the live page carries none --
    so without this the preview is the only version claiming to be canonical
    for its own content.

    Parameterised over the NOINDEX routes rather than every block route:
    Batch 15d cut about.html over, so /about.html is block-built AND indexable,
    and a list of "block pages" is no longer a list of previews.
    """
    assert '<meta name="robots" content="noindex">' in path_for(route).read_text("utf-8")


@pytest.mark.parametrize("route", [r for r in block_page_routes() if is_indexable(r)])
def test_a_block_built_page_that_went_live_is_not_left_noindexed(route):
    """The other direction, and the one a cutover gets wrong.

    /preview/about.html carried noindex for three batches. Moving the route
    has to take that with it, or the page replaces a live, indexed page with
    one that asks to be dropped from the index -- a silent traffic loss that
    looks like a successful deployment.
    """
    assert "noindex" not in path_for(route).read_text("utf-8")


@pytest.mark.parametrize("route", sitemap_routes())
def test_a_submitted_page_is_never_told_not_to_be_indexed(route):
    """Submitting a URL and telling a crawler to ignore it is a contradiction
    a sitemap cannot express and a reviewer would not spot."""
    assert is_indexable(route)
    assert "noindex" not in path_for(route).read_text("utf-8")


def test_noindex_is_driven_by_the_inventory_not_by_the_route_string():
    assert is_indexable("/about.html")
    assert not is_indexable("/preview/about.html")
    assert not is_indexable("/preview/fr/map.html")
    assert NOINDEX_PREFIXES, "an empty prefix tuple would make every page indexable"


# --- sitemaps ---------------------------------------------------------------


def _locs(path: Path) -> list[str]:
    tree = ElementTree.fromstring(path.read_text(encoding="utf-8"))
    ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    return [e.text for e in tree.iter(f"{ns}loc")]


@pytest.mark.parametrize("sitemap", ["sitemap.xml", "sitemap-pages.xml", "local/sitemap.xml"])
def test_every_url_a_sitemap_declares_resolves(sitemap):
    """Batch 17 spec, acceptance criterion 4. "A sitemap that advertises a URL
    the deploy no longer publishes is a rule 31 breach that also poisons search
    indexing."
    """
    path = REPO_ROOT / sitemap
    assert path.is_file(), f"{sitemap} does not exist"
    broken = []
    for loc in _locs(path):
        if loc.endswith(".xml"):  # a sitemap index names sitemaps, not pages
            target = REPO_ROOT / loc.split("/belgian-macro-pipeline/", 1)[1]
        else:
            target = path_for("/" + loc.split("/belgian-macro-pipeline/", 1)[1])
        if not target.is_file():
            broken.append(loc)
    assert not broken, f"{sitemap} advertises {len(broken)} unreachable URL(s): {broken[:5]}"


def test_the_sitemap_index_names_every_child_sitemap():
    locs = _locs(REPO_ROOT / "sitemap.xml")
    assert any(loc.endswith("/sitemap-pages.xml") for loc in locs)
    assert any(loc.endswith("/local/sitemap.xml") for loc in locs)


def _alternates(path: Path) -> dict:
    """Every <loc> in a sitemap, mapped to the hreflang set it declares."""
    tree = ElementTree.fromstring(path.read_text(encoding="utf-8"))
    ns = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    xh = "{http://www.w3.org/1999/xhtml}link"
    out = {}
    for url in tree.iter(f"{ns}url"):
        loc = url.find(f"{ns}loc").text
        out[loc] = {link.get("hreflang"): link.get("href") for link in url.iter(xh)}
    return out


def test_a_sitemapped_page_with_translations_declares_them_all():
    """A page published in three languages and submitted with no alternates is
    three pages competing with each other in the index -- the failure
    docs/features/i18n.md:61 says makes ranking WORSE than not translating.

    Checked against the inventory rather than a hardcoded list, so the next
    cutover is covered on the day it lands.
    """
    declared = _alternates(REPO_ROOT / "sitemap-pages.xml")
    for route in sitemap_routes():
        siblings = translations_of(route)
        for published in siblings or (route,):
            loc = f"{SITE_BASE}{published}"
            assert loc in declared, f"{published} is not in sitemap-pages.xml"
            if not siblings:
                assert not declared[loc], f"{published} has one URL but declares alternates"
                continue
            expected = {
                lang: f"{SITE_BASE}{url}" for lang, url in zip(LANGS, siblings, strict=True)
            }
            expected["x-default"] = f"{SITE_BASE}{route}"
            assert declared[loc] == expected, f"{published} declares the wrong alternates"


def test_the_sitemap_alternates_agree_with_the_pages_own_hreflang():
    """Two statements of the same fact, in two files, written by two scripts.
    A reader never sees either; a crawler sees both and believes the pair."""
    declared = _alternates(REPO_ROOT / "sitemap-pages.xml")
    for route in sitemap_routes():
        for published in translations_of(route):
            page = path_for(published).read_text(encoding="utf-8")
            for lang, href in declared[f"{SITE_BASE}{published}"].items():
                assert (
                    f'<link rel="alternate" hreflang="{lang}" href="{href}">' in page
                ), f"{published} does not carry the {lang} alternate its sitemap entry claims"


def test_the_site_index_check_mode_agrees_with_what_is_committed():
    """`make all` regenerates these; CI runs pytest only. Without this a stale
    robots.txt or sitemap ships green."""
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "export_site_index.py"), "--check"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, result.stdout


def test_no_preview_url_is_submitted_anywhere():
    for sitemap in ("sitemap.xml", "sitemap-pages.xml", "local/sitemap.xml"):
        for loc in _locs(REPO_ROOT / sitemap):
            assert "/preview/" not in loc, f"{sitemap} submits a noindex page: {loc}"


def test_robots_names_the_sitemap_and_disallows_preview():
    robots = (REPO_ROOT / "robots.txt").read_text(encoding="utf-8")
    assert "Sitemap: " in robots
    assert "/sitemap.xml" in robots
    for prefix in NOINDEX_PREFIXES:
        assert f"Disallow: {prefix}" in robots


# --- the scripts run as scripts ---------------------------------------------


@pytest.mark.parametrize(
    "script",
    ["export_local_pages.py", "export_page_documents.py", "export_site_index.py"],
)
def test_every_exporter_runs_as_a_script(script):
    """`make all` runs these with `python scripts/NAME.py`, and pytest does
    not. Batch 15c added a `src.pages` import to export_local_pages.py without
    a sys.path line: it resolved under pytest, whose rootdir is already on the
    path, and `make pages` died on ModuleNotFoundError with a fully green
    suite. A test that only ever imports a module cannot see that.
    """
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / script), "--help"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, f"{script} --help failed:\n{result.stderr}"


# --- route_for, moved but unchanged -----------------------------------------


@pytest.mark.parametrize(
    "route, lang, expected",
    [
        ("/preview/about.html", "en", "/preview/about.html"),
        ("/preview/about.html", "fr", "/preview/fr/about.html"),
        ("/about.html", "nl", "/nl/about.html"),
        ("/local/11001/", "en", "/local/11001/"),
        ("/local/11001/", "fr", "/local/11001/fr/"),
    ],
)
def test_route_for_still_produces_the_shape_batch_15c_chose(route, lang, expected):
    """It moved modules; it must not have moved behaviour. Rule 31 makes this
    URL shape a one-time choice."""
    assert route_for(route, lang) == expected
