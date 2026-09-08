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

    links = "".join(
        f'\n    <link rel="stylesheet" href="{escape(href, quote=True)}">'
        for href in (FONT_HREF, *stylesheets)
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
        f"{footer}\n"
        "</body>\n"
        "</html>\n"
    )
