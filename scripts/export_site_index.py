"""robots.txt and the site-wide sitemap index.

Until Batch 17a there was NO robots.txt and NO root sitemap.xml on this site.
`local/sitemap.xml` submitted 1,695 commune pages; the root pages had never
been submitted for indexing at all, and nothing told a crawler what to leave
alone.

A SITEMAP INDEX, NOT ONE BIG SITEMAP. `/sitemap.xml` names two children:
`/sitemap-pages.xml` (written here) and `/local/sitemap.xml` (written by
scripts/export_local_pages.py, and untouched by this script). The commune
sitemap is generated from the database on a different schedule by a different
exporter, and merging the two would mean one script writing a file the other
owns. An index is the format's own answer to exactly this.

WHICH ROOT PAGES ARE SUBMITTED is not this script's decision. It reads
`src/site/routes.py`, where every root page carries `sitemap: True|False` and a
written reason -- because two of the nine are the unreleased redesign of two
others and one is rendered inside another's iframe, and submitting both halves
of such a pair asks a search engine to index the same content twice.

Deterministic (claude.md rule 35): no clock is read here. `lastmod` is
deliberately omitted rather than stamped with today's date -- see the note in
`scripts/export_local_pages.py:_write_sitemap`, where doing that rewrote all
1,695 lines of the commune sitemap on every build.

Usage:  python scripts/export_site_index.py [--check]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.pages.strings import DEFAULT_LANG  # noqa: E402
from src.site.routes import (  # noqa: E402
    NOINDEX_PREFIXES,
    SITE_BASE,
    path_for,
    sitemap_routes,
    translations_of,
)


def _lang_of(canonical: str, translated: str) -> str:
    """Which language `translated` is, given the page's canonical route.

    Read back from the URL the inventory produced rather than zipped against
    LANGS by position: `translations_of` is ordered, but a sitemap that
    mislabels French as Dutch is a defect no test of counts would catch.
    """
    if translated == canonical:
        return DEFAULT_LANG
    head, _, _name = canonical.rpartition("/")
    return translated[len(head) + 1 :].split("/", 1)[0]


#: The commune sitemap, written by the other exporter and only referenced here.
COMMUNE_SITEMAP = "local/sitemap.xml"


def render_pages_sitemap() -> str:
    """The root pages that stand alone.

    THE ROOT PAGES ARE NO LONGER ALL THE SAME SHAPE. A hand-built one exists
    at one URL and switches language in the browser; a block-built one is
    rendered per language and exists at three. Since Batch 15d cut about.html
    over, this file contains both kinds, so each route is asked
    (`translations_of`) rather than assumed. Declaring alternates a page does
    not have would be worse than declaring none -- and NOT declaring the ones
    it does have is the competing-duplicates failure docs/features/i18n.md:61
    describes, where three editions of a page fight each other in the index.
    """
    entries = []
    for route in sitemap_routes():
        for published in translations_of(route) or (route,):
            if not path_for(published).is_file():
                raise SystemExit(
                    f"refusing to submit {published}: no file serves it. A sitemap "
                    "that advertises a URL the site does not publish is a rule 31 "
                    "breach that also poisons search indexing."
                )
            links = "".join(
                f'<xhtml:link rel="alternate" hreflang="{_lang_of(route, other)}" '
                f'href="{SITE_BASE}{other}"/>'
                for other in translations_of(route)
            )
            if links:
                links += (
                    f'<xhtml:link rel="alternate" hreflang="x-default" '
                    f'href="{SITE_BASE}{route}"/>'
                )
            entries.append(f"  <url><loc>{SITE_BASE}{published}</loc>{links}</url>")
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"\n'
        '        xmlns:xhtml="http://www.w3.org/1999/xhtml">\n'
        + "\n".join(entries)
        + "\n</urlset>\n"
    )


def render_sitemap_index() -> str:
    children = ["sitemap-pages.xml", COMMUNE_SITEMAP]
    entries = "\n".join(
        f"  <sitemap><loc>{SITE_BASE}/{child}</loc></sitemap>" for child in children
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + entries
        + "\n</sitemapindex>\n"
    )


def render_robots() -> str:
    """Allow everything except the routes the inventory marks noindex.

    NOT a confidentiality mechanism, and nothing here should be described as
    one: this repository is public and every committed file stays fetchable
    from github.com whatever robots.txt says. This governs crawler behaviour
    on the site's URL surface, which is a different question.
    """
    disallow = "".join(f"Disallow: {prefix}\n" for prefix in NOINDEX_PREFIXES)
    return (
        "# What a crawler should and should not index on this site.\n"
        "# Generated by scripts/export_site_index.py -- edit that, not this.\n"
        "#\n"
        "# The /preview/ pages are block-built rebuilds of pages that are still\n"
        "# live elsewhere. They are duplicates, so they are kept out of the\n"
        '# index; each one also carries its own <meta name="robots"\n'
        '# content="noindex">, because a path disallowed here can still be\n'
        "# indexed if something links to it.\n"
        "User-agent: *\n"
        f"{disallow}"
        "\n"
        f"Sitemap: {SITE_BASE}/sitemap.xml\n"
    )


TARGETS = {
    "sitemap-pages.xml": render_pages_sitemap,
    "sitemap.xml": render_sitemap_index,
    "robots.txt": render_robots,
}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="report changes, write nothing")
    args = parser.parse_args(argv)

    if not (REPO_ROOT / COMMUNE_SITEMAP).is_file():
        raise SystemExit(
            f"{COMMUNE_SITEMAP} does not exist. The index would name a sitemap "
            "that is not there -- run `make pages` first."
        )

    changed = []
    for name, render in TARGETS.items():
        target = REPO_ROOT / name
        content = render()
        existing = target.read_text(encoding="utf-8") if target.exists() else None
        if existing != content:
            changed.append(name)
        if not args.check:
            target.write_text(content, encoding="utf-8")
        print(f"  {name}")

    if args.check:
        if changed:
            print(f"\n{len(changed)} file(s) would change: {', '.join(changed)}")
            return 1
        print("\nrobots.txt and the sitemaps are up to date.")
        return 0

    print(f"\n{len(TARGETS)} file(s) written, submitting {len(sitemap_routes())} root page(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
