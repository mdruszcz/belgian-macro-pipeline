"""Wrap a rendered page-document fragment in a real HTML page.

Batch 15a. `render_document` returns a FRAGMENT -- `<div class="bp-page">` and
its sections, nothing more. That is correct: the same fragment is what the
builder previews inside a sandboxed iframe, and giving it a `<head>` there
would be wrong. But a published page needs a document around it, and until
this module existed there was no path from a page document to a file on the
site at all.

WHAT THIS MODULE IS CAREFUL ABOUT

*The licence attribution.* Statbel's 2015 licence terminates automatically on
non-compliance, and this project has already published municipal figures
without attribution once (docs/steps, Block K). So a page whose document
declares municipal data is REFUSED unless an attribution block is supplied --
not rendered with a gap where the notice should be. The notice itself is
LIFTED from the live site rather than retyped, the same decision
`scripts/export_local_pages.py:156` made and for the same reason: a second
hand-written copy drifts, and a drifted licence notice is a breach that looks
like a typo.

*Determinism.* Nothing here reads a clock or iterates an unordered set, so two
builds of the same document are byte-identical (rule 35). The build stamp that
used to sit in the commune pages' footer was removed for exactly this reason --
it churned 565 files per run and manifest.json already records it centrally.

*A reader without JavaScript.* Figures are already inline in the fragment
(Batch 10 chose a Python renderer precisely so a crawler sees them). This shell
adds no script that the content depends on.
"""

from __future__ import annotations

import json
import re
from html import escape
from pathlib import Path

from src.pages.schema import PageDocumentError

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Where the live licence notice is lifted from. communes.html, not local.html:
#: the same source `scripts/export_local_pages.py` uses, so one page cannot
#: drift from the other.
ATTRIBUTION_SOURCE = REPO_ROOT / "communes.html"

#: The webfont the design system declares. tokens.css says in as many words
#: that a page using it "must link the actual font, the same way every existing
#: page already links Google Fonts -- this file only declares the CSS variable,
#: it cannot fetch a font by itself." Without it the page falls back to IBM
#: Plex Sans, which is legitimate but not the intended look. The weights match
#: the ones tokens.css actually uses.
FONT_HREF = (
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700"
    "&family=Caveat:wght@500;600&display=swap"
)

#: Scripts an interactive block needs, in load order. A page with no chart and
#: no map links none of them: the boundary file alone is 1.2 MB, and Batch 0
#: measured map.html at 59 on performance because of it.
INTERACTIVE_SCRIPTS = (
    "assets/i18n.js",
    "assets/commune_map.js",
    "assets/belpulse/charts.js",
    "assets/belpulse/blocks.js",
)

#: i18n.js FIRST, and this is load-bearing order rather than tidiness:
#: commune_map.js captures the strings table in a `const` as it loads
#: (`const I18N_SRC = (typeof I18N !== 'undefined') ? I18N : null`), so a copy
#: that arrives afterwards is never seen. It degrades quietly -- the legend
#: note, coverage warning and tooltips print raw string keys.

#: What a MAP block needs on top of the design system. commune_map.css owns the
#: seven --ramp-* choropleth tokens and --nodata, the path strokes, the tooltip
#: and the legend's geometry. This is not optional polish: every fill the
#: component sets is `var(--ramp-N)`, so without this sheet the map draws 565
#: paths against undefined custom properties -- silently, with the block still
#: marked ready. Linked only when a map is present, because the rest of the
#: site's pages have no use for it.
MAP_STYLESHEET = "assets/commune_map.css"

#: Block types whose rendered output is a SLOT that JavaScript fills. The
#: server cannot finish these: a chart is canvas pixels and a map needs a
#: 1.2 MB boundary file. Everything else is complete HTML from the renderer,
#: which is why a reader without JavaScript still gets every figure.
_HYDRATED = frozenset({"chart", "map"})

#: Block types that put a municipal figure on the page. A document containing
#: one of these with a binding is publishing Statbel-derived data and owes the
#: attribution. Read from the document, never assumed from the page id.
_DATA_BEARING = frozenset({"kpi_card", "chart", "comparison_table", "map"})


class ShellError(PageDocumentError):
    """The page cannot be wrapped. Always fatal: the alternative is publishing
    a page that breaches a licence or misstates its own language."""


def read_attribution(path: Path = ATTRIBUTION_SOURCE) -> str:
    """The `.attribution` block's inner HTML, lifted from the live site.

    Refuses rather than returning empty: a missing notice must stop the build,
    not produce pages without one.
    """
    source = path.read_text(encoding="utf-8")
    match = re.search(r'<div class="attribution" id="attribution">(.*?)</div>', source, re.S)
    if not match:
        raise ShellError(
            f"{path.name} has no .attribution block to lift. It is a licence "
            "condition, not decoration -- refusing to generate pages without it."
        )
    return match.group(1)


def hydrated_block_types(doc) -> set:
    """Which hydrated block types a document contains -- the map's stylesheet
    is linked from this rather than from the page id."""
    out = set()
    for section in doc.get("sections") or []:
        for block in (section or {}).get("blocks") or []:
            if isinstance(block, dict) and block.get("type") in _HYDRATED:
                out.add(block["type"])
    return out


def hydrated_block_ids(doc) -> list:
    """Blocks on this page that JavaScript has to finish."""
    out = []
    for section in doc.get("sections") or []:
        for block in section.get("blocks") or []:
            if isinstance(block, dict) and block.get("type") in _HYDRATED:
                block_id = block.get("id")
                if isinstance(block_id, str):
                    out.append(block_id)
    return out


def declares_municipal_data(doc) -> bool:
    """Does this document put a municipal figure on the page?"""
    for section in doc.get("sections") or []:
        for block in section.get("blocks") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") in _DATA_BEARING and block.get("binding"):
                return True
    return False


def _text(value, lang: str) -> str:
    """A trilingual field in one language. Falls back to English rather than
    rendering an empty heading, but never invents a translation."""
    if isinstance(value, dict):
        return str(value.get(lang) or value.get("en") or "")
    return "" if value is None else str(value)


def wrap(
    fragment: str,
    doc,
    *,
    lang: str,
    canonical: str,
    attribution: str | None = None,
    stylesheets: tuple[str, ...] = (),
    data: dict | None = None,
    asset_prefix: str = "",
) -> str:
    """One published page.

    `attribution` is REQUIRED when the document carries municipal data, and
    passing it for a page that does not is harmless. The check is here rather
    than in the caller so that every future exporter inherits it -- a rule
    enforced in one place is a rule; enforced in each caller it is a habit.
    """
    if declares_municipal_data(doc) and not attribution:
        raise ShellError(
            "this document publishes municipal figures and no licence "
            "attribution was supplied. Statbel's licence terminates on "
            "non-compliance; refusing to write the page."
        )

    seo = doc.get("seo") or {}
    title = _text(seo.get("title"), lang)
    description = _text(seo.get("description"), lang)
    if not title:
        raise ShellError("a published page needs a title in the language it is written in")

    extra_sheets = (asset_prefix + MAP_STYLESHEET,) if "map" in hydrated_block_types(doc) else ()
    links = "".join(
        f'\n    <link rel="stylesheet" href="{escape(href, quote=True)}">'
        for href in (FONT_HREF, *stylesheets, *extra_sheets)
    )
    meta_description = (
        f'\n    <meta name="description" content="{escape(description, quote=True)}">'
        if description
        else ""
    )
    footer = (
        f'\n<footer class="bp-page-footer"><div class="attribution" '
        f'id="attribution">{attribution}</div></footer>'
        if attribution
        else ""
    )

    # A hydrated block renders as an empty slot unless its resolved data
    # reaches the browser. `about.html` had none, so this never surfaced until
    # a page carried a map: the block was there, the figures were there, and
    # the page showed a blank box.
    scripts = ""
    hydrated = hydrated_block_ids(doc)
    if hydrated:
        payload = {bid: (data or {}).get(bid) for bid in hydrated}
        # </script> inside the JSON would close this element early. The same
        # escape bootstrap_html applies to the shell's own inlined source.
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False).replace("</", "<\\/")
        tags = "".join(
            f'\n<script src="{escape(asset_prefix + src, quote=True)}"></script>'
            for src in INTERACTIVE_SCRIPTS
        )
        scripts = (
            f"{tags}"
            f'\n<script id="bp-block-data" type="application/json">{encoded}</script>'
            "\n<script>BPBlocks.hydrate(document, {"
            f"lang: {json.dumps(lang)}, "
            # How far the site root is from this page. A block fetching
            # "public/data/..." from a page one directory down would ask for
            # /preview/public/data/... -- the same prefix trap the stylesheets
            # already hit here.
            f"assetPrefix: {json.dumps(asset_prefix)}, "
            "data: JSON.parse(document.getElementById('bp-block-data').textContent)"
            "});</script>"
        )

    return (
        "<!DOCTYPE html>\n"
        f'<html lang="{escape(lang, quote=True)}">\n'
        "<head>\n"
        '    <meta charset="UTF-8">\n'
        '    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f"    <title>{escape(title)}</title>"
        f"{meta_description}\n"
        f'    <link rel="canonical" href="{escape(canonical, quote=True)}">'
        f"{links}\n"
        "</head>\n"
        "<body>\n"
        f"{fragment}"
        f"{footer}"
        f"{scripts}\n"
        "</body>\n"
        "</html>\n"
    )
