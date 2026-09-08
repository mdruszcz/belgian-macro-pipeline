"""What URLs this site publishes. One module, so there is one answer.

claude.md rule 31 says "every existing canonical and legacy URL remains valid
after the redesign", and until this module existed NOTHING in `tests/` asserted
that any public URL resolves at all -- recorded as an open P1 in
docs/implementation/known-risks.md, across 1,695 commune pages in three
languages plus 9 root pages. A rule enforced by nobody is a wish.

THE THREE PARTS ARE DELIBERATELY DIFFERENT IN KIND, because the failures they
guard against are different:

  * ROOT_PAGES is an EXPLICIT, FROZEN TABLE. Nine hand-built pages, each one a
    URL somebody may have bookmarked or linked. A derived rule would happily
    follow a rename and report success; a literal list fails loudly, which is
    the whole point of a rule-31 baseline.

  * Commune routes are DERIVED from the published payloads. 1,695 of them,
    entirely mechanical, and a hand-maintained list would be wrong within a day
    of the next commune merger.

  * Block-page routes are derived through `route_for`, which is the only
    function that knows how a page document's one declared route becomes three
    published URLs.

WHAT `sitemap` MEANS HERE, and why it is not simply "everything". Two of the
root pages are the unreleased redesign of two others, and one is rendered
inside another's iframe. Submitting both halves of such a pair asks a search
engine to index the same content twice and lets the two compete -- the same
failure `hreflang` exists to prevent between languages, one level up. So every
row carries a REASON, and a page is excluded on the record rather than by
being forgotten.

`indexable` is separate from `sitemap`. A page can be reachable and not
submitted (the redesign drafts); a page can be reachable and actively told not
to be indexed (`/preview/`). robots.txt alone cannot do the second: a
disallowed URL can still be indexed if something links to it, so the page
itself has to say `noindex`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from src.pages.strings import DEFAULT_LANG, LANGS

REPO_ROOT = Path(__file__).resolve().parents[2]

#: The live site. Not derived from an environment variable: a canonical URL
#: that changes with the machine that built it is worse than none at all.
SITE_BASE = "https://mdruszcz.github.io/belgian-macro-pipeline"


@dataclass(frozen=True)
class Page:
    """One hand-built root page and what the site claims about it."""

    route: str
    #: Submitted for indexing in sitemap-pages.xml.
    sitemap: bool
    #: Why it is or is not submitted. Written down so an exclusion is a
    #: decision on the record rather than an omission nobody noticed.
    reason: str


#: THE FROZEN LIST. Every .html file at the repository root appears here, and a
#: test asserts that -- so a new root page cannot be silently forgotten, and
#: cannot silently enter the sitemap either. "/" and "/index.html" are the same
#: file; only "/" is submitted, because a search engine should not be offered
#: two URLs for one page.
ROOT_PAGES: tuple[Page, ...] = (
    Page("/", True, "the front page"),
    Page(
        "/index.html",
        False,
        "the same file as /; submitting both offers one page under two URLs",
    ),
    Page("/communes.html", True, "the commune table -- a landing page in its own right"),
    Page("/map.html", True, "the choropleth explorer"),
    Page("/all_data.html", True, "every observation, browsable"),
    Page("/about.html", True, "what this project is"),
    Page(
        "/dashboard.html",
        False,
        "rendered inside index.html's iframe; / is the stronger URL for the "
        "same content, and both would compete",
    ),
    Page(
        "/local.html",
        False,
        "a JavaScript app shell -- local.html?nis=NNNNN. The indexable form of "
        "a commune is /local/{nis}/, which local/sitemap.xml already submits, "
        "and a crawler sees nothing useful here",
    ),
    Page(
        "/home.html",
        False,
        "the unreleased redesign of /. Reachable, but submitting it would put "
        "two homepages in the index; it enters the sitemap when it replaces /",
    ),
    Page(
        "/commune.html",
        False,
        "the unreleased redesign of local.html, and linked from nowhere. Same "
        "reason as /home.html",
    ),
)

#: Routes whose pages must tell crawlers not to index them. The block-built
#: conversions duplicate a live page word for word, and each carries a
#: self-referencing canonical (three real editions of one page, which is
#: correct AMONG THEMSELVES and wrong against about.html and map.html). The
#: live pages carry no canonical at all, so without this the preview is the
#: only version claiming to be canonical.
NOINDEX_PREFIXES = ("/preview/",)


def is_indexable(route: str) -> bool:
    return not route.startswith(NOINDEX_PREFIXES)


def route_for(route: str, lang: str) -> str:
    """The site-relative URL of `route` in `lang`.

    THE LANGUAGE IS A DIRECTORY IMMEDIATELY ABOVE THE FILE, and English keeps
    the URL it already has:

        /preview/about.html  ->  /preview/fr/about.html
        /local/11001/        ->  /local/11001/fr/

    Both shapes are ones this site already publishes -- the second is exactly
    what `scripts/export_local_pages.py:_route` produces -- so nothing here
    invents a URL convention. English staying put is rule 31: those routes are
    indexed and linked, and moving them under /en/ would break them for no gain.

    Moved here from `scripts/export_page_documents.py` in Batch 17a. It was
    already documented as "the only place that knows what URLs a page
    occupies", and the inventory has to expand routes through the same function
    the exporter writes them with, or the inventory and the build can disagree.
    `src/` may not import from `scripts/`, which is why it moved rather than
    being imported.
    """
    if lang == DEFAULT_LANG:
        return route
    if route.endswith("/"):
        return f"{route}{lang}/"
    head, _, name = route.rpartition("/")
    return f"{head}/{lang}/{name}"


def path_for(route: str, repo_root: Path = REPO_ROOT) -> Path:
    """The file a route is served from. A directory route serves index.html."""
    relative = route.lstrip("/")
    if route.endswith("/") or not relative:
        relative += "index.html"
    return repo_root / relative


def root_routes() -> tuple[str, ...]:
    return tuple(page.route for page in ROOT_PAGES)


def sitemap_routes() -> tuple[str, ...]:
    """The root pages submitted for indexing, in table order."""
    return tuple(page.route for page in ROOT_PAGES if page.sitemap)


def commune_routes(repo_root: Path = REPO_ROOT) -> tuple[str, ...]:
    """Every published commune page, in every language.

    Derived from what was actually written, not from the payloads: a commune
    with no values at all is skipped by the exporter (the thin-page ban), and
    an inventory that listed it would demand a URL the site deliberately does
    not publish.
    """
    local = repo_root / "local"
    if not local.is_dir():
        return ()
    routes = []
    for directory in sorted(p.parent.name for p in local.glob("*/index.html")):
        for lang in LANGS:
            route = route_for(f"/local/{directory}/", lang)
            if path_for(route, repo_root).is_file():
                routes.append(route)
    return tuple(routes)


def block_page_routes(repo_root: Path = REPO_ROOT) -> tuple[str, ...]:
    """Every published page document's routes, one per language."""
    pages_root = repo_root / "config" / "pages"
    if not pages_root.is_dir():
        return ()
    routes = []
    for page_dir in sorted(pages_root.iterdir()):
        document = page_dir / "published.json"
        if not document.is_file():
            continue
        declared = json.loads(document.read_text(encoding="utf-8"))["route"]
        routes.extend(route_for(declared, lang) for lang in LANGS)
    return tuple(routes)


def all_routes(repo_root: Path = REPO_ROOT) -> tuple[str, ...]:
    """Every URL this site publishes."""
    return root_routes() + commune_routes(repo_root) + block_page_routes(repo_root)
