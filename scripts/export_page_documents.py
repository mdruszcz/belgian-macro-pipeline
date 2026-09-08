"""Build every published page document into a real page on the site.

Batch 15a. This is the path that did not exist: `src/pages/render.py` produced
a fragment, `src/builder/` could edit and save a document, and nothing joined
the two. `config/pages/{page_id}/published.json` in, one HTML file out, at the
route the document itself declares.

DRAFTS ARE NEVER BUILT. Only `published.json` is read. A draft becoming public
without an explicit publish is the one thing invariant 10 forbids, and the way
that happens is an exporter that globs `*.json`.

THE ROUTE COMES FROM THE DOCUMENT, and the document's route was checked by the
validator against a closed allowlist (`src/pages/semantics.py`). This script
re-derives the output path from that route rather than from the directory name,
and refuses anything that escapes the repository -- the same belt-and-braces the
Batch 14 audit required of the payload reader, for the same reason: the check
that matters is the one on the path actually used.

Deterministic: two runs produce byte-identical files (rule 35). Nothing here
reads a clock.

Usage:  python scripts/export_page_documents.py [--check]
        --check builds into memory and reports what WOULD change, writing
        nothing. Useful in CI and before a release.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.builder.paths import PAGES_ROOT  # noqa: E402
from src.pages import load_registry, render_document  # noqa: E402
from src.pages.document import validate_document  # noqa: E402
from src.pages.metadata import load_metadata  # noqa: E402
from src.pages.resolve import resolve_document  # noqa: E402
from src.pages.shell import (  # noqa: E402
    ShellError,
    declares_municipal_data,
    read_attribution,
    wrap,
)
from src.pages.strings import LANGS  # noqa: E402

# route_for and the indexing decision both live in the site's route
# inventory now (Batch 17a): the exporter must write a page at exactly
# the URL the inventory claims it publishes, so both read one module.
from src.site.routes import is_indexable, route_for  # noqa: E402

#: The design system, linked rather than inlined: a published page is served
#: from the repository root, so it can fetch a stylesheet, unlike the builder's
#: sandboxed preview which has to inline it.
STYLESHEETS = (
    "assets/belpulse/tokens.css",
    "assets/belpulse/components.css",
    "assets/belpulse/layout.css",
    "assets/belpulse/blocks.css",
)

#: The site's own base, for the canonical link. Not read from the environment:
#: a canonical URL that changes with the machine that built it is worse than
#: none at all.
SITE_BASE = "https://mdruszcz.github.io/belgian-macro-pipeline"


def output_path_for(route: str) -> Path:
    """Where a route's file goes, derived from the route itself."""
    if not route.startswith("/") or ".." in route:
        raise ShellError(f"refusing a route that is not a plain site path: {route!r}")
    relative = route.lstrip("/")
    if route.endswith("/"):
        relative = relative + "index.html"
    target = (REPO_ROOT / relative).resolve()
    if not target.is_relative_to(REPO_ROOT):
        raise ShellError(f"route {route!r} resolves outside the repository")
    return target


def asset_prefix_for(route: str) -> str:
    """How many levels up the site root is from this route.

    A page at /preview/about.html linking `assets/...` asks for
    /preview/assets/..., which does not exist -- every stylesheet 404s while
    the page still renders, which is precisely the breakage
    scripts/export_local_pages.py:76 records for its own translated pages
    ("a wrong prefix breaks every link and every asset").
    """
    depth = route.strip("/").count("/")
    return "../" * depth


def build_one(page_dir: Path, *, lang: str, metadata, registry, attribution: str):
    """(path, html) for one published document. Writes nothing."""
    document_path = page_dir / "published.json"
    doc = json.loads(document_path.read_text(encoding="utf-8"))

    errors = validate_document(doc, metadata=metadata, registry=registry)
    if errors:
        # A page that does not validate must not reach the site. Reported with
        # its findings rather than as "export failed".
        listed = "\n".join(f"  {e.path}: {e.message}" for e in errors[:10])
        raise ShellError(f"{document_path} is not valid:\n{listed}")

    resolved = resolve_document(doc, metadata=metadata, lang=lang)
    fragment = render_document(doc, registry=registry, lang=lang, data=resolved)
    declared = doc["route"]
    route = route_for(declared, lang)
    # Every language's absolute URL, so each page can name the others. Built
    # from the declared route, so the alternates cannot drift from the files.
    alternates = {code: f"{SITE_BASE}{route_for(declared, code)}" for code in LANGS}
    prefix = asset_prefix_for(route)
    # The same targets as site-root-relative paths, so a reader clicking a
    # language stays on whatever server they are already on.
    switch_links = {code: prefix + route_for(declared, code).lstrip("/") for code in LANGS}
    html = wrap(
        fragment,
        doc,
        lang=lang,
        # Its OWN url, not English's: three real pages, not three views of one
        # (docs/features/i18n.md:63).
        canonical=f"{SITE_BASE}{route}",
        alternates=alternates,
        switch_links=switch_links,
        indexable=is_indexable(route),
        # Supplied only when the page owes it. Passing it always would hide the
        # refusal that exists to stop an unattributed page shipping.
        attribution=attribution if declares_municipal_data(doc) else None,
        stylesheets=tuple(prefix + href for href in STYLESHEETS),
        # The same resolved map the renderer used. A hydrated block is finished
        # in the browser, and it can only show what reaches it.
        data=resolved,
        asset_prefix=prefix,
    )
    return output_path_for(route), html


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="report changes, write nothing")
    # Restricts which languages are built; it does NOT change where a language
    # is written. Before Batch 15c this flag defaulted to "en" and every
    # language wrote to the SAME path, so `--lang fr` did not publish a French
    # page -- it overwrote the English one.
    parser.add_argument(
        "--lang", action="append", choices=LANGS, help="build only this language (repeatable)"
    )
    args = parser.parse_args(argv)
    languages = tuple(dict.fromkeys(args.lang)) if args.lang else LANGS

    page_dirs = sorted(d for d in PAGES_ROOT.iterdir() if (d / "published.json").is_file())
    if not page_dirs:
        print("No published page documents. Nothing to build.")
        return 0

    metadata, registry = load_metadata(), load_registry()
    # One notice per language, read once. `interface_strings()` inside refuses
    # if any language lacks one, so a build either has all three or stops.
    attributions = {code: read_attribution(code) for code in languages}

    changed, written = [], 0
    for page_dir in page_dirs:
        for lang in languages:
            target, html = build_one(
                page_dir,
                lang=lang,
                metadata=metadata,
                registry=registry,
                attribution=attributions[lang],
            )
            existing = target.read_text(encoding="utf-8") if target.exists() else None
            if existing != html:
                changed.append(target.relative_to(REPO_ROOT))
            if not args.check:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(html, encoding="utf-8")
                written += 1
            print(f"  {page_dir.name:20} {lang}  -> {target.relative_to(REPO_ROOT)}")

    if args.check:
        if changed:
            print(f"\n{len(changed)} page(s) would change:")
            for path in changed:
                print(f"  {path}")
            return 1
        print("\nEvery published page is up to date.")
        return 0

    print(
        f"\n{written} page(s) built from {len(page_dirs)} document(s) "
        f"in {len(languages)} language(s)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
