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
READ from `assets/i18n.js` through `src/pages/strings.py` rather than
retyped, the same source `scripts/export_local_pages.py` uses and for the same
reason: a second hand-written copy drifts, and a drifted licence notice is a
breach that looks like a typo.

It used to be lifted out of `communes.html`, which has exactly ONE language --
so a French page would have carried an English notice. That is not a
translation gap, it is a licence condition stated in the wrong language.

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
from collections.abc import Mapping
from functools import lru_cache
from html import escape
from pathlib import Path

from src.pages.schema import PageDocumentError
from src.pages.strings import DEFAULT_LANG, LANGS, interface_strings

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Language names as they name THEMSELVES. A French reader looking for their
#: language looks for "Français", not for "French" -- an endonym is what every
#: language switcher on the web uses, and translating them would be the
#: "foreign product" signal docs/steps warns about for this market.
LANGUAGE_NAMES = {"en": "English", "fr": "Fran\u00e7ais", "nl": "Nederlands"}

#: The theme MENU (Batch A1.1, docs/features/site_unification.md). THE SITE
#: HAS HAD A LIGHT AND A DARK PALETTE SINCE BATCH 1 and the block-built pages
#: offered no way to choose: they followed prefers-color-scheme alone, so a
#: maintainer whose laptop is in dark mode could not see the light design his
#: own reference is drawn in, on the very page built to match it.
#:
#: `auto` is DELIBERATELY NOT in this tuple -- the maquette's theme menu shows
#: only Clair/Sombre, and A1.2 appends a third entry (`papier`) to this same
#: tuple rather than inventing a second menu. `auto` is not gone, though: a
#: reader's OLD stored choice of `auto` is still read and honoured by
#: `SHELL_BOOTSTRAP` below, which resolves it to an explicit light/dark and
#: overwrites the stored value -- an upgrade, not a loss.
#:
#: Each entry is (stored value, the assets/i18n.js key for its menu label) --
#: labels live in the one interface-strings table so the browser
#: (assets/belpulse/shell.js) and the generator read the same word, never a
#: second hand-typed copy (claude.md rule 2 extended to the builder, rule 24).
THEME_CHOICES = (
    ("light", "themeLight"),
    ("dark", "themeDark"),
)

#: Read BEFORE THE FIRST PAINT, so a reader who chose light does not watch a
#: dark page flash first. Inline and in the head for that reason: a linked file
#: cannot promise to run before the stylesheets apply.
#:
#: BYTE-IDENTICAL ON EVERY PAGE THIS SHELL COVERS (rule 35) -- `wrap()` inlines
#: it verbatim, and `scripts/sync_site_shell.py` writes the same bytes into the
#: `bp-shell:bootstrap` zone of every hand-edited page, so there is exactly one
#: script to audit for this rather than one per page.
#:
#: Reads `belpulse-theme` first. Missing entirely, it falls back to
#: dashboard.html's legacy `theme` key (day/soft/night) through LEGACY_THEMES --
#: the same fallback the iframe contract below already applies, so a reader who
#: only ever used the old dashboard still gets the theme they chose. `auto`
#: (the menu no longer offers it, but a reader's storage may still hold it from
#: before this batch) is resolved against prefers-color-scheme and the
#: EXPLICIT result is written back to storage, so this branch fires at most
#: once per reader. Anything else unrecognised becomes `light`, never applied
#: raw -- the same reasoning as the iframe contract's `data-theme="soft"`
#: failure mode below.
#:
#: NOT written into communes.html, all_data.html or local.html -- those keep
#: their own older switcher and reading the legacy `theme` key there is exactly
#: what tests/test_theme_toggle.py:83 forbids. This constant is simply never
#: placed on those pages.
SHELL_BOOTSTRAP = (
    "<script>(function(){try{"
    "var KEY='belpulse-theme',LEGACY_KEY='theme',"
    "LEGACY={day:'light',soft:'light',night:'dark'};"
    "var v=localStorage.getItem(KEY);"
    "if(v===null){"
    "var legacy=localStorage.getItem(LEGACY_KEY);"
    "if(legacy&&Object.prototype.hasOwnProperty.call(LEGACY,legacy))v=LEGACY[legacy];"
    "}"
    "if(v==='auto'){"
    "v=(window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)').matches)"
    "?'dark':'light';"
    "try{localStorage.setItem(KEY,v);}catch(e){}"
    "}"
    "if(v!=='light'&&v!=='dark')v='light';"
    "document.documentElement.setAttribute('data-theme',v);"
    "}catch(e){document.documentElement.setAttribute('data-theme','light');}"
    "})();</script>"
)

#: THE SITE'S OWN CHROME -- top bar, breadcrumb, footer.
#:
#: This is interface text, not content, so it lives here beside the switcher
#: labels rather than in a page document: every page says the same thing, and a
#: document that had to restate the navigation would be 30 lines of menu before
#: it got to its own subject. Trilingual for the same reason everything else is
#: (rule 7).
#:
#: A block-built page had NONE of this until the commune profile made the
#: absence obvious -- it rendered as a bare stack of cards with a language
#: switcher floating above it and nothing identifying the site at all.
#: The tagline the ORIGINAL top bar carried beside the wordmark is gone from
#: Batch A1.1's header and footer -- the maquette
#: (docs/design-references/maquettes-unification-v1/01-accueil.png) shows the
#: logo mark and "BelPulse" alone, nothing else, in both places.
BRAND = "BelPulse"
#: THE SIX DESTINATIONS (Batch A1.1). Every public page proposes exactly
#: these, in this order -- docs/features/site_unification.md section 2. Each
#: entry is (the file the link points at, its assets/i18n.js label key), never
#: a hand-typed trilingual dict: the label lives in the one interface-strings
#: table `render_header`/`render_footer` and assets/belpulse/shell.js all read,
#: so a wording change is one edit, not three call sites agreeing by hand.
#:
#: "profiles.html" stands in for the commune profile: `commune.html` (not yet
#: converted, batch A3.1) and the generated `preview/commune.html` both mark
#: this entry current, per `_current_nav_page` below.
NAV = (
    ("home2.html", "navHome"),
    ("profiles.html", "navProfiles"),
    ("macro.html", "navMacro"),
    ("micro.html", "navMicro"),
    ("map.html", "navMaps"),
    ("sources.html", "navSources"),
)
NAV_LABEL = {"en": "Main navigation", "fr": "Navigation principale", "nl": "Hoofdnavigatie"}
FOOTER_NAV_LABEL = {
    "en": "Footer navigation",
    "fr": "Navigation de pied de page",
    "nl": "Voettekstnavigatie",
}
CRUMB_LABEL = {"en": "Breadcrumb", "fr": "Fil d’Ariane", "nl": "Kruimelpad"}

#: THE SITE HAS TWO THEME VOCABULARIES AND THEY DO NOT OVERLAP.
#:
#: index.html offers `day`, `soft` and `night` (its `t-day`/`t-soft`/`t-night`
#: buttons) and pushes the chosen one into its iframe. The design system these
#: pages use defines only `[data-theme="light"]` and `[data-theme="dark"]`
#: (assets/belpulse/tokens.css). Passing the parent's value straight through
#: sets `data-theme="soft"`, which no stylesheet matches -- so the page keeps
#: its default and silently stops following the shell, looking fine the whole
#: time.
#:
#: Written out here, legacy names included, because that is the only place the
#: two vocabularies meet. An unknown value maps to light rather than being
#: applied raw: the shell is allowed to grow a fourth theme without this page
#: rendering against a token set that does not exist.
LEGACY_THEMES = {"day": "light", "soft": "light", "night": "dark"}

#: The webfonts the design system declares. tokens.css says in as many words
#: that a page using either "must link the actual font, the same way every
#: existing page already links Google Fonts -- this file only declares the CSS
#: variable, it cannot fetch a font by itself." Without it the page silently
#: falls back to IBM Plex Sans, legitimate but not the intended look. Spectral
#: is the H1/heading serif every editorial-shell page (home2, the commune
#: profile) already links directly; folded in here in Batch A1.1 so a
#: block-built page's own heading -- and now the shared header's wordmark --
#: renders in the same face rather than falling back silently. The weights
#: match what tokens.css actually uses.
FONT_HREF = (
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700"
    "&family=Spectral:wght@500;600;700&family=Caveat:wght@500;600&display=swap"
)

#: The shared shell's own script -- the theme menu, the language menu, the
#: mobile nav toggle, menu keyboard navigation. Loaded on every page this
#: module wraps, static-mode and client-mode alike (docs/features/
#: site_unification.md section 2: "One shared JS module handles the menus,
#: language and theme"). Deliberately NOT in INTERACTIVE_SCRIPTS below: those
#: are loaded only when a page actually carries the block type that needs
#: them, but the shell chrome is on every page, so this is unconditional.
SHELL_JS = "assets/belpulse/shell.js"

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
#:
#: comparison_picker joins them for a different reason: it does not draw a
#: figure, but its options are 565 commune names read from published geography
#: metadata, and inlining that list into every page is what the fetch avoids.
#:
#: kpi_card joins them for its sparkline. Coarse on purpose: the set is by
#: block TYPE, so a page with a kpi_card that asks for no sparkline still links
#: the chart script. Splitting it per-prop would mean the shell inspecting
#: block props to decide what a page loads, which is a worse trade than one
#: small script.
_HYDRATED = frozenset({"chart", "map", "comparison_picker", "kpi_card"})

#: Block types that put a municipal figure on the page. A document containing
#: one of these with a binding is publishing Statbel-derived data and owes the
#: attribution. Read from the document, never assumed from the page id.
#:
#: MISSING A TYPE HERE IS A LICENCE BREACH, not a styling bug: the page ships
#: without the Statbel notice, `wrap()` does not catch it because it is asking
#: this set, and Statbel's 2015 licence terminates automatically on
#: non-compliance. Every new block that can show a municipal number belongs
#: here the moment it exists, which is why the four added for the commune
#: profile were added in the same change as their renderers.
#:
#: comparison_picker is deliberately NOT here: it chooses which geographies a
#: page compares, and shows no figure of its own.
_DATA_BEARING = frozenset(
    {
        "kpi_card",
        "chart",
        "comparison_table",
        "map",
        "stat_tile",
        "ranking_list",
        "neighbour_list",
        "sources_panel",
    }
)


class ShellError(PageDocumentError):
    """The page cannot be wrapped. Always fatal: the alternative is publishing
    a page that breaches a licence or misstates its own language."""


def read_attribution(lang: str = DEFAULT_LANG) -> str:
    """The licence notice, IN `lang`, from the site's one strings table.

    `interface_strings()` already refuses if any language lacks a notice, so
    reaching this function at all means all three exist. The guard below covers
    the remaining case -- a language nobody has written a notice for -- and
    refuses rather than returning empty: a missing notice must stop the build,
    not produce pages without one.
    """
    table = interface_strings().get(lang)
    notice = (table or {}).get("attribution")
    if not notice:
        raise ShellError(
            f"assets/i18n.js has no licence notice for {lang!r}. It is a licence "
            "condition, not decoration -- refusing to generate pages without it."
        )
    return notice


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


@lru_cache(maxsize=1)
def _strings() -> dict:
    """assets/i18n.js's STRINGS table, read through node exactly once per
    process. `render_header`/`render_footer` are called once PER PAGE -- up to
    several thousand times in one export run once commune profiles are real
    pages -- and `interface_strings()` shells out to node; caching here is
    what keeps a build one subprocess rather than one per page."""
    return interface_strings()


def _t(lang: str, key: str) -> str:
    """One interface string. Falls back to English and then to the key
    itself -- the same two-step fallback `_text()` above applies to a
    document field, kept separate because this one reads the STATIC
    assets/i18n.js table rather than a value the caller passed in."""
    table = _strings()
    return (table.get(lang) or {}).get(key) or (table.get(DEFAULT_LANG) or {}).get(key) or key


def _href(path: str, asset_prefix: str) -> str:
    """A NAV path made relative to wherever the current page lives.

    Every NAV entry is a bare filename ("home2.html", "sources.html", ...) --
    unlike the old NAV_LINKS this module used to carry, nothing here is
    site-absolute, because every page these functions render onto lives
    somewhere under the repository root and `asset_prefix` already says how
    many directories down (the same prefix the stylesheets and scripts use).
    """
    return escape(asset_prefix + path, quote=True)


def _current_nav_page(doc) -> str | None:
    """Which NAV entry this document is, if any -- derived from the
    document's own declared route rather than asked of every caller by hand,
    so a new page type does not silently mark nothing current by omission.

    `/about.html` is not one of the six destinations (Batch A1.1 dropped
    Methodology from the header) and correctly returns None: the header marks
    nothing current rather than guessing at the nearest match.
    """
    route = (doc.get("route") or "").rstrip("/")
    if route.endswith("commune.html"):
        return "profiles.html"
    if route.endswith("map.html"):
        return "map.html"
    return None


#: Inline SVGs for the shell's own controls. `currentColor` only -- a raw hex
#: value here would be the rule tests/pages/test_shared_components.py:100
#: exists to catch, just written in Python instead of a stylesheet.
#: `aria-hidden` because the button or link that carries each one always
#: supplies its own accessible name (an `aria-label`, or in the nav's case its
#: own text).
_ICON_SUN = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" aria-hidden="true">'
    '<circle cx="12" cy="12" r="4"/>'
    '<path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41'
    'M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>'
)
_ICON_GLOBE = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" aria-hidden="true">'
    '<circle cx="12" cy="12" r="9"/>'
    '<path d="M3 12h18M12 3a14 14 0 0 1 0 18M12 3a14 14 0 0 0 0 18"/></svg>'
)
_ICON_CARET = (
    '<svg class="bp-menu-caret" width="10" height="10" viewBox="0 0 24 24" '
    'fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">'
    '<path d="M6 9l6 6 6-6"/></svg>'
)
_ICON_NAV_TOGGLE = (
    '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" aria-hidden="true">'
    '<path d="M3 6h18M3 12h18M3 18h18"/></svg>'
)


def render_header(
    *,
    lang: str,
    current: str | None,
    asset_prefix: str = "",
    mode: str,
    switch_links: Mapping[str, str] | None = None,
) -> str:
    """The one header every public page carries -- docs/features/
    site_unification.md section 2. Returns a `<header class="bp-topbar">`
    fragment (preceded by the shared skip link); the caller places it, the
    same contract `wrap()` already had for its own inline topbar.

    `mode="client"` is for a page that is ONE file in all three languages
    (home2.html, macro.html, synced by scripts/sync_site_shell.py): visible
    text is English, marked `data-t="..."` (and accessible names
    `data-t-aria="..."`) so assets/i18n.js -- driven by
    assets/belpulse/shell.js -- swaps it in the browser, exactly the pattern
    these two pages already used for their nav before this batch.

    `mode="static"` is for a page rendered once per language on the server
    (every block-built page): the text is already in `lang`, and the language
    menu holds real per-language links rather than a script-driven swap, so
    switching still works with scripting off -- `switch_links` is REQUIRED in
    this mode for exactly that reason (mirroring the REQUIRED `attribution`
    check `wrap()` already applies for a different licence condition).

    `current` is a NAV path ("profiles.html", ...) or None; the matching link
    gets `aria-current="page"`. `_current_nav_page` derives it for a
    page-document; the two hand pages pass their own filename directly.
    """
    if mode not in ("client", "static"):
        raise ShellError(f"render_header: mode must be 'client' or 'static', got {mode!r}")
    if mode == "static" and not switch_links:
        raise ShellError("render_header: mode='static' needs switch_links for the language menu")

    def label(key: str) -> str:
        return _t(lang if mode == "static" else DEFAULT_LANG, key)

    def data_t(key: str) -> str:
        return f' data-t="{key}"' if mode == "client" else ""

    def aria_t(key: str) -> str:
        return f' data-t-aria="{key}"' if mode == "client" else ""

    def href(path: str) -> str:
        return _href(path, asset_prefix)

    skip_link = (
        f'<a class="bp-skip-link" href="#bp-main"{data_t("skipToContent")}>'
        f"{escape(label('skipToContent'))}</a>"
    )

    logo = (
        f'<a class="bp-logo" href="{href(NAV[0][0])}">'
        '<span class="bp-logo-mark"><span></span><span></span><span></span></span>'
        f'<span class="bp-wordmark">{escape(BRAND)}</span></a>'
    )

    nav_items = "".join(
        f'<a href="{href(path)}"'
        + (' aria-current="page"' if path == current else "")
        + data_t(key)
        + f">{escape(label(key))}</a>"
        for path, key in NAV
    )
    nav_label = escape(_text(NAV_LABEL, lang if mode == "static" else DEFAULT_LANG), quote=True)
    nav = f'<nav class="bp-nav" id="bp-nav" aria-label="{nav_label}">{nav_items}</nav>'

    theme_label = escape(label("theme"), quote=True)
    theme_items = "".join(
        f'<button type="button" class="bp-menu-item" role="menuitemradio" '
        f'data-theme-choice="{choice}" aria-checked="false"{data_t(key)}>'
        f"{escape(label(key))}</button>"
        for choice, key in THEME_CHOICES
    )
    theme_menu = (
        '<div class="bp-menu bp-theme-menu" data-menu="theme">'
        '<button type="button" class="bp-menu-btn" id="bp-theme-menu-btn" '
        'aria-haspopup="menu" aria-expanded="false" aria-controls="bp-theme-menu-panel" '
        f'aria-label="{theme_label}"{aria_t("theme")}>'
        f"{_ICON_SUN}{_ICON_CARET}</button>"
        '<div class="bp-menu-panel bp-theme-switch" id="bp-theme-menu-panel" role="menu" '
        f'aria-label="{theme_label}"{aria_t("theme")} hidden>{theme_items}</div>'
        "</div>"
    )

    lang_label = escape(label("language"), quote=True)
    if mode == "static":
        lang_items = "".join(
            f'<a href="{escape(switch_links[code], quote=True)}" hreflang="{code}" '
            f'lang="{code}" data-lang="{code}" role="menuitem"'
            + (' aria-current="page"' if code == lang else "")
            + f">{escape(LANGUAGE_NAMES.get(code, code))}</a>"
            for code in LANGS
            if code in switch_links
        )
        lang_panel_tag = "nav"
    else:
        lang_items = "".join(
            f'<button type="button" role="menuitemradio" data-lang="{code}" '
            f'aria-checked="false">{escape(LANGUAGE_NAMES.get(code, code))}</button>'
            for code in LANGS
        )
        lang_panel_tag = "div"
    lang_menu = (
        '<div class="bp-menu bp-lang-menu" data-menu="lang">'
        '<button type="button" class="bp-menu-btn" id="bp-lang-menu-btn" '
        'aria-haspopup="menu" aria-expanded="false" aria-controls="bp-lang-menu-panel" '
        f'aria-label="{lang_label}"{aria_t("language")}>'
        f'{_ICON_GLOBE}<span class="bp-menu-current">{escape(lang.upper())}</span>'
        f"{_ICON_CARET}</button>"
        f'<{lang_panel_tag} class="bp-lang-switch" id="bp-lang-menu-panel" role="menu" '
        f'aria-label="{lang_label}"{aria_t("language")} hidden>{lang_items}</{lang_panel_tag}>'
        "</div>"
    )

    nav_toggle = (
        '<button type="button" class="bp-nav-toggle" aria-expanded="false" '
        f'aria-controls="bp-nav" aria-label="{escape(label("navMenu"), quote=True)}"'
        f'{aria_t("navMenu")}>{_ICON_NAV_TOGGLE}</button>'
    )

    return (
        f"{skip_link}"
        '<header class="bp-topbar">'
        '<div class="wrap">'
        f"{logo}{nav}"
        f'<div class="bp-topbar-actions">{theme_menu}{lang_menu}{nav_toggle}</div>'
        "</div>"
        "</header>"
    )


def render_footer(*, lang: str, asset_prefix: str = "", mode: str, current: str | None) -> str:
    """The one footer every public page carries: the logo, the same six
    destinations, and a short credit line -- nothing else (docs/features/
    site_unification.md section 2: no About, no Contact, no legacy link).

    `mode` only changes whether the visible text carries `data-t` for
    assets/i18n.js to swap (`client`) or is already written in `lang`
    (`static`) -- unlike `render_header`'s language menu, the footer's six
    links are the same NAV destinations in every language, never a
    per-language URL, so this needs no `switch_links`.
    """
    if mode not in ("client", "static"):
        raise ShellError(f"render_footer: mode must be 'client' or 'static', got {mode!r}")

    def label(key: str) -> str:
        return _t(lang if mode == "static" else DEFAULT_LANG, key)

    def data_t(key: str) -> str:
        return f' data-t="{key}"' if mode == "client" else ""

    def href(path: str) -> str:
        return _href(path, asset_prefix)

    logo = (
        f'<a class="bp-logo" href="{href(NAV[0][0])}">'
        '<span class="bp-logo-mark"><span></span><span></span><span></span></span>'
        f'<span class="bp-wordmark">{escape(BRAND)}</span></a>'
    )
    nav_items = "".join(
        f'<a href="{href(path)}"'
        + (' aria-current="page"' if path == current else "")
        + data_t(key)
        + f">{escape(label(key))}</a>"
        for path, key in NAV
    )
    nav_label = escape(
        _text(FOOTER_NAV_LABEL, lang if mode == "static" else DEFAULT_LANG), quote=True
    )

    return (
        '\n<footer class="foot">'
        '<div class="wrap">'
        f"{logo}"
        f'<nav class="bp-nav" aria-label="{nav_label}">{nav_items}</nav>'
        f'<p class="bp-footer-credit"{data_t("footerCredit")}>{escape(label("footerCredit"))}</p>'
        "</div>"
        "</footer>"
    )


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
    alternates: Mapping[str, str] | None = None,
    switch_links: Mapping[str, str] | None = None,
    indexable: bool = True,
) -> str:
    """One published page.

    `attribution` is REQUIRED when the document carries municipal data, and
    passing it for a page that does not is harmless. The check is here rather
    than in the caller so that every future exporter inherits it -- a rule
    enforced in one place is a rule; enforced in each caller it is a habit.

    `alternates` maps a language to this page's ABSOLUTE URL in that language.
    Supplying it turns three files into three declared editions of one page;
    omitting it publishes them as three pages that happen to say the same
    thing, which docs/features/i18n.md records as making search ranking WORSE
    rather than better.

    `switch_links` is the same map as hrefs RELATIVE to this page, for the
    visible switcher. Two maps rather than one because the two have different
    jobs -- see the switcher block below.

    `indexable=False` emits `<meta name="robots" content="noindex">`. The
    CALLER decides, from `src/site/routes.py`, never this function from the
    shape of the route -- a rule that reads a URL string is a rule that a
    renamed directory silently switches off.
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
    # NOINDEX, and why robots.txt is not enough on its own. A path disallowed
    # in robots.txt can still be indexed if something links to it -- the
    # crawler simply indexes the URL without fetching it. Only the page can
    # say "do not index me", and only a page that gets fetched can say it.
    robots = '\n    <meta name="robots" content="noindex">' if not indexable else ""
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

    # THE ALTERNATES. Every language names every language, itself included, plus
    # x-default pointing at English. Following export_local_pages.py:583-593
    # rather than reinventing it, and for the reason docs/features/i18n.md:61
    # gives: without this a search engine treats the three as duplicates
    # COMPETING with each other, and generating them makes ranking worse.
    hreflang = ""
    if alternates:
        hreflang = "".join(
            f'\n    <link rel="alternate" hreflang="{escape(code, quote=True)}" '
            f'href="{escape(href, quote=True)}">'
            for code, href in sorted(alternates.items())
        )
        default = alternates.get(DEFAULT_LANG)
        if default:
            hreflang += (
                '\n    <link rel="alternate" hreflang="x-default" '
                f'href="{escape(default, quote=True)}">'
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

    # THE IFRAME CONTRACT. index.html loads some of these pages into an iframe
    # and pushes the reader's theme and language in by postMessage; the page it
    # replaced (the hand-built about.html) listened for both and announced
    # itself back. A block-built page that ignores this sits in the front page
    # frozen in one language and one theme, and looks perfectly fine doing it.
    #
    # `setLang` NAVIGATES rather than re-translating: these pages are rendered
    # per language on the server, so the French edition is a different URL, not
    # a different DOM. The guard against re-navigating when the language
    # already matches is load-bearing -- the parent re-sends on every frame
    # load, so without it the frame reloads forever.
    #
    # AND THE SWITCHER IS HIDDEN WHEN FRAMED (`data-framed`, styled in
    # layout.css). Not tidiness -- leaving it visible made it actively
    # destructive, confirmed in a browser: clicking "Francais" navigated the
    # frame to /fr/about.html, the new page posted `dashboard-ready`, the
    # parent answered with its own unchanged `setLang('en')`, and the reader
    # watched their choice snap back to English while localStorage said `fr`.
    # The parent owns the language when it owns the frame; two language
    # controls in one viewport is the bug, not the symptom.
    #
    # Only when framed. A top-level page must never navigate because something
    # sent it a message.
    if switch_links:
        by_lang = json.dumps({code: switch_links[code] for code in switch_links})
        scripts += (
            "\n<script>(function(){if(window.self===window.top)return;"
            "document.documentElement.setAttribute('data-framed','1');"
            f"var here={json.dumps(lang)},urls={by_lang},"
            f"themes={json.dumps(LEGACY_THEMES)};"
            "window.addEventListener('message',function(e){"
            "var m=e.data;if(!m||!m.type)return;"
            "if(m.type==='setTheme'){"
            "document.documentElement.setAttribute("
            "'data-theme',Object.prototype.hasOwnProperty.call(themes,m.value)"
            "?themes[m.value]:'light');}"
            "else if(m.type==='setLang'&&m.value!==here&&urls[m.value]){"
            "window.location.href=urls[m.value];}});"
            "window.parent.postMessage('dashboard-ready','*');})();</script>"
        )

    # THE SHARED SHELL SCRIPT -- the theme menu, the language menu and the
    # mobile nav toggle that render_header() below just emitted markup for.
    # Loaded unconditionally: every page this module wraps carries the shell
    # chrome, unlike INTERACTIVE_SCRIPTS above which only a page with the
    # matching block type needs.
    scripts += f'\n<script src="{escape(asset_prefix + SHELL_JS, quote=True)}"></script>'

    # THE HEADER AND FOOTER -- one shared implementation
    # (docs/features/site_unification.md section 2), so a generated page and
    # the two hand-edited pilots (home2.html, macro.html, via
    # scripts/sync_site_shell.py) render the identical fragment. `current` is
    # derived from the document's own route rather than guessed from `doc`'s
    # shape a second time here.
    current = _current_nav_page(doc)
    topbar = render_header(
        lang=lang,
        current=current,
        asset_prefix=asset_prefix,
        mode="static",
        switch_links=switch_links,
    )
    site_footer = render_footer(
        lang=lang,
        asset_prefix=asset_prefix,
        mode="static",
        current=current,
    )

    # The breadcrumb, and the page's own title as an h1. The document's seo
    # title is the page's name; repeating it in the body is what gives a
    # reader (and a crawler) the heading the hero used to have to carry.
    # THE PAGE'S h1 IS EMITTED HERE ONLY IF NO BLOCK CLAIMS IT.
    #
    # A hero block renders an h1 -- Batch 15a made that change deliberately,
    # because a published page with no h1 fails both a screen reader's document
    # outline and every SEO check. So a page with a hero already has one, and
    # adding a second here gave about.html two, which is the same failure in
    # the other direction.
    #
    # A commune profile has no hero (it opens on a photograph), so there the
    # shell's title IS the page heading.
    has_hero = any(
        isinstance(block, Mapping) and block.get("type") == "hero"
        for section in (doc.get("sections") or [])
        if isinstance(section, Mapping)
        for block in (section.get("blocks") or [])
    )
    page_title = "" if has_hero else f'<h1 class="bp-page-title">{escape(title)}</h1>'
    crumb = (
        '\n<div class="bp-page-head">'
        f'<nav class="bp-breadcrumb" aria-label="{escape(_text(CRUMB_LABEL, lang), quote=True)}">'
        f'<a href="{_href(NAV[0][0], asset_prefix)}">{escape(_t(lang, "navHome"))}</a>'
        f"<span>{escape(title)}</span></nav>"
        f"{page_title}"
        "</div>"
    )

    return (
        "<!DOCTYPE html>\n"
        f'<html lang="{escape(lang, quote=True)}">\n'
        "<head>\n"
        '    <meta charset="UTF-8">\n'
        f"    {SHELL_BOOTSTRAP}\n"
        '    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f"    <title>{escape(title)}</title>"
        f"{robots}"
        f"{meta_description}\n"
        f'    <link rel="canonical" href="{escape(canonical, quote=True)}">'
        f"{hreflang}"
        f"{links}\n"
        "</head>\n"
        "<body>\n"
        # .bp-shell wraps everything: it carries the page background, the base
        # font and the min-height, and the navy variant recolours the top bar
        # through it. The top bar has to be inside it, not above it.
        '<div class="bp-shell">'
        f"{topbar}"
        # `id="bp-main"` is the shared skip link's target (render_header
        # above emits `<a class="bp-skip-link" href="#bp-main">`) -- a
        # keyboard or screen-reader reader can jump straight past the header
        # and its menus to the page's own content.
        '<main id="bp-main">'
        f"{crumb}"
        f"{fragment}"
        "</main>"
        f"{site_footer}"
        f"{footer}"
        "\n</div>"
        f"{scripts}\n"
        "</body>\n"
        "</html>\n"
    )
