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
import re
import sys
from collections.abc import Mapping
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.builder.paths import PAGES_ROOT  # noqa: E402
from src.pages import load_registry, render_document  # noqa: E402
from src.pages.document import validate_document  # noqa: E402
from src.pages.metadata import load_metadata  # noqa: E402
from src.pages.resolve import PayloadReader, resolve_document  # noqa: E402
from src.pages.shell import (  # noqa: E402
    ShellError,
    declares_municipal_data,
    read_attribution,
    wrap,
)
from src.pages.strings import DEFAULT_LANG, LANGS  # noqa: E402

# route_for and the indexing decision both live in the site's route
# inventory now (Batch 17a): the exporter must write a page at exactly
# the URL the inventory claims it publishes, so both read one module.
from src.site.routes import NIS_PLACEHOLDER, is_indexable, route_for  # noqa: E402

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

    A DIRECTORY ROUTE IS ONE LEVEL DEEPER THAN IT LOOKS. `/local/11001/` counts
    one slash, but the file written for it is `local/11001/index.html` -- two
    directories down, so it needs `../../`. Counting slashes alone gave `../`,
    and every asset on all 1,695 commune pages would have 404'd while the pages
    still rendered. `scripts/export_local_pages.py:_root_prefix` hardcodes the
    correct depths, which is what this was checked against.

    It never fired before because no published document used a directory
    route; the templated commune page is the first.
    """
    relative = route.strip("/")
    depth = relative.count("/")
    if relative and route.endswith("/"):
        # The implicit index.html. Not for "/" itself: that writes index.html
        # at the site root, which is already depth zero.
        depth += 1
    return "../" * depth


PAYLOAD_DIR = REPO_ROOT / "public" / "data"


def communes_with_data(payload_dir: Path = PAYLOAD_DIR) -> tuple[str, ...]:
    """Every commune that gets a page, in NIS order.

    comparison.md's "no data, no page" rule: a commune whose every indicator is
    empty is skipped, because an empty page is worse than a missing one.
    `scripts/export_local_pages.py` applies the same rule to the pages live
    today, and `tests/pages/test_commune_loop.py` asserts the two exporters
    still agree on the set -- one rule stated twice is one rule until it is
    not.
    """
    codes = []
    for path in sorted((payload_dir / "communes").glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for entry in (payload.get("indicators") or {}).values():
            if any(cell.get("value") is not None for cell in (entry.get("periods") or {}).values()):
                codes.append(path.stem)
                break
    return tuple(codes)


#: What a TEMPLATED document's SEO strings may carry. Every one of these
#: resolves from real published data -- the commune payload's own name, and
#: figures already resolved for blocks on this very page -- so rule 36 holds:
#: no commune and no figure is typed into a page definition.
#:
#: `{figures:block-id,block-id}` names blocks, not indicators, deliberately.
#: The block already carries its label in three languages and its binding was
#: already resolved for the page, so the description cannot disagree with what
#: the reader sees, and a document cannot ask the description for a figure the
#: page does not show.
SEO_PLACEHOLDER = re.compile(r"\{(commune|nis|figures:[A-Za-z0-9_,-]+)\}")


def local_text(value, lang: str) -> str:
    """One language out of a trilingual label, English as the fallback."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for code in (lang, DEFAULT_LANG):
            text = value.get(code)
            if isinstance(text, str) and text:
                return text
    return ""


def commune_name(reader: PayloadReader, nis: str, lang: str) -> str:
    """The commune's name in `lang`, from its payload (rule 36).

    Falls back to the NIS code, which is ugly and correct: a title reading
    "11001" is a visible defect, while a title reading "Commune" for 565
    different pages is an invisible one.
    """
    payload = reader.commune(nis) or {}
    return local_text(payload.get("name"), lang) or nis


def figures_for(block_ids: str, doc: Mapping, resolved: Mapping, lang: str) -> str:
    """`Population 110 939 (2024); Average income 21 336 EUR (2023)`.

    Only blocks that actually RESOLVED are named. A commune missing one of them
    gets a shorter description rather than a sentence with a hole in it, which
    is the same shape `export_local_pages._describe` produces today.
    """
    wanted = [part for part in block_ids.split(",") if part]
    blocks = {
        block.get("id"): block
        for section in doc.get("sections") or []
        for block in section.get("blocks") or []
    }
    bits = []
    for block_id in wanted:
        entry = resolved.get(block_id) or {}
        if entry.get("state") != "ready" or not entry.get("formatted_value"):
            continue
        label = local_text(((blocks.get(block_id) or {}).get("props") or {}).get("label"), lang)
        period = entry.get("period")
        figure = f"{label} {entry['formatted_value']}".strip()
        bits.append(f"{figure} ({period})" if period else figure)
    return "; ".join(bits)


def localise(
    doc: Mapping, *, nis: str, lang: str, resolved: Mapping, reader: PayloadReader
) -> dict:
    """A copy of a templated document, made concrete for one commune.

    Three things change: the route, `context.nis`, and every trilingual string
    the document wrote a placeholder into -- the SEO title and description, and
    the block props that name the subject (the hero's overlay title, the map's
    accessible name, the comparison headings).

    WITHOUT THIS all 1,695 commune pages would carry one title, one description
    and one commune's name in their headings -- the duplicate-content state
    search engines demote, and a worse position than the hand-built pages hold
    today.
    """
    name = commune_name(reader, nis, lang)

    def fill(text: str) -> str:
        if not SEO_PLACEHOLDER.search(text):
            # UNTOUCHED, not merely unchanged: the whitespace collapse below
            # would otherwise rewrite every ordinary string on the page.
            return text

        def one(match: re.Match) -> str:
            token = match.group(1)
            if token == "commune":
                return name
            if token == "nis":
                return nis
            # Figures read from the ORIGINAL document, so a label that itself
            # carries a placeholder cannot feed on its own substitution.
            return figures_for(token.split(":", 1)[1], doc, resolved, lang)

        # Collapsed, because a `{figures:...}` that resolved to nothing leaves
        # the space around it behind.
        return re.sub(r"\s+", " ", SEO_PLACEHOLDER.sub(one, text)).strip()

    def walk(value):
        """Every string under here, filled in.

        Only the prose is walked -- block props, section labels and the SEO
        strings. An id, a type or an indicator code is not prose, and nothing
        good comes of rewriting one.
        """
        if isinstance(value, str):
            return fill(value)
        if isinstance(value, dict):
            return {key: walk(item) for key, item in value.items()}
        if isinstance(value, list):
            return [walk(item) for item in value]
        return value

    sections = []
    for section in doc.get("sections") or []:
        blocks = []
        for block in section.get("blocks") or []:
            blocks.append({**block, "props": walk(block["props"])} if block.get("props") else block)
        sections.append(
            {
                **section,
                "blocks": blocks,
                **({"label": walk(section["label"])} if section.get("label") else {}),
            }
        )

    out = dict(doc)
    out["route"] = doc["route"].replace(NIS_PLACEHOLDER, nis)
    out["context"] = {**(doc.get("context") or {}), "nis": nis}
    out["seo"] = walk(doc.get("seo") or {})
    out["sections"] = sections
    return out


def load_document(page_dir: Path, *, metadata, registry) -> dict:
    """The document on disk, parsed and CHECKED.

    Separate from `build_one` because a templated document is rendered once per
    commune per language -- 1,695 times -- and validating one file 1,695 times
    says nothing it did not say the first time.
    """
    document_path = page_dir / "published.json"
    doc = json.loads(document_path.read_text(encoding="utf-8"))
    errors = validate_document(doc, metadata=metadata, registry=registry)
    if errors:
        # A page that does not validate must not reach the site. Reported with
        # its findings rather than as "export failed".
        listed = "\n".join(f"  {e.path}: {e.message}" for e in errors[:10])
        raise ShellError(f"{document_path} is not valid:\n{listed}")
    return doc


def build_one(
    page_dir: Path,
    *,
    lang: str,
    metadata,
    registry,
    attribution: str,
    doc: dict | None = None,
    nis: str | None = None,
    reader: PayloadReader | None = None,
):
    """(path, html) for one published document. Writes nothing.

    `nis` builds a TEMPLATED document for ONE commune: `{nis}` in the route is
    substituted, the blocks resolve against that commune, and the SEO strings
    are filled in from that commune's own payload. `doc` accepts an
    already-validated document so a caller building 565 pages parses and checks
    the file once.
    """
    if doc is None:
        doc = load_document(page_dir, metadata=metadata, registry=registry)
    # A FRESH READER PER COMMUNE, which the caller supplies. PayloadReader
    # caches every file it opens for the life of the instance, so one reader
    # shared across 565 communes would end up holding all 565 payloads -- about
    # 37 MB -- to no purpose, since each page reads its own exactly once.
    reader = reader or PayloadReader()
    resolved = resolve_document(doc, metadata=metadata, lang=lang, nis=nis, reader=reader)
    if nis:
        doc = localise(doc, nis=nis, lang=lang, resolved=resolved, reader=reader)
    # The prefix the stylesheets already use, now also given to the renderer:
    # a block writes `/about.html`, and this site is served from a SUBPATH, so
    # a leading slash points at the wrong host and 404s.
    prefix = asset_prefix_for(route_for(doc["route"], lang))
    fragment = render_document(
        doc, registry=registry, lang=lang, data=resolved, asset_prefix=prefix
    )
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
    # For looking at ONE commune. A templated document otherwise builds all of
    # them, and reviewing a layout change does not need 1,695 files.
    parser.add_argument(
        "--nis", action="append", help="build only these communes, for a templated page"
    )
    args = parser.parse_args(argv)
    languages = tuple(dict.fromkeys(args.lang)) if args.lang else LANGS
    selected = tuple(dict.fromkeys(args.nis)) if args.nis else ()

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
        # Parsed and validated ONCE, then rendered per commune per language.
        doc = load_document(page_dir, metadata=metadata, registry=registry)
        templated = NIS_PLACEHOLDER in doc["route"]
        if templated:
            available = communes_with_data()
            if selected:
                # Loudly, per rule 13: an unknown code otherwise builds a page
                # of "no data" states under a name that is just its own number,
                # and looks like a commune with nothing published.
                unknown = [code for code in selected if code not in set(available)]
                if unknown:
                    raise ShellError(
                        f"--nis {', '.join(unknown)}: no commune payload with any figure. "
                        "Refusing to build a page for a commune this site does not publish."
                    )
            codes = selected or available
            if not codes:
                raise ShellError(
                    f"{page_dir.name} is a templated page and no commune payload has a "
                    "single figure. Run `make payloads` first; refusing to publish nothing."
                )
        else:
            if selected:
                print(f"  {page_dir.name:20} --nis ignored: this page is not templated")
            codes = (None,)

        for nis in codes:
            # One reader per commune, shared by that commune's three languages:
            # it reads the payload once, and is dropped before the next commune
            # rather than accumulating all 565.
            reader = PayloadReader() if nis else None
            for lang in languages:
                target, html = build_one(
                    page_dir,
                    lang=lang,
                    metadata=metadata,
                    registry=registry,
                    attribution=attributions[lang],
                    doc=doc,
                    nis=nis,
                    reader=reader,
                )
                existing = target.read_text(encoding="utf-8") if target.exists() else None
                if existing != html:
                    changed.append(target.relative_to(REPO_ROOT))
                if not args.check:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(html, encoding="utf-8")
                    written += 1
                if not templated:
                    print(f"  {page_dir.name:20} {lang}  -> {target.relative_to(REPO_ROOT)}")
        if templated:
            # One line, not 1,695. The per-commune paths are derived from the
            # route and say nothing a reader cannot already predict.
            print(
                f"  {page_dir.name:20} {len(codes)} commune(s) x "
                f"{len(languages)} language(s) -> {doc['route']}"
            )

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
