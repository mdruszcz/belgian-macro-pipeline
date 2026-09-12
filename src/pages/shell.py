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

#: What the switcher calls itself, for a screen reader.
SWITCHER_LABEL = {"en": "Language", "fr": "Langue", "nl": "Taal"}

#: The theme switch. THE SITE HAS HAD A LIGHT AND A DARK PALETTE SINCE BATCH 1
#: and the block-built pages offered no way to choose: they followed
#: prefers-color-scheme alone, so a maintainer whose laptop is in dark mode
#: could not see the light design his own reference is drawn in, on the very
#: page built to match it.
#:
#: Three states, and `auto` is not decoration: it REMOVES the override and
#: hands the page back to the operating system, which is the only way back
#: once a reader has chosen. `auto` is NOT the default, though: every design
#: this site is drawn from is light, so a page with no saved choice opens
#: light whatever the machine prefers. Following the OS by default handed a
#: reader on a dark laptop a page the design was never drawn in. The key is `belpulse-theme` -- the same one
#: all_data.html, commune.html and the component gallery already write, so a
#: choice made anywhere on this site holds everywhere on it.
THEME_LABEL = {"en": "Theme", "fr": "Thème", "nl": "Thema"}
THEME_CHOICES = (
    ("light", {"en": "Light", "fr": "Clair", "nl": "Licht"}),
    ("auto", {"en": "Auto", "fr": "Auto", "nl": "Auto"}),
    ("dark", {"en": "Dark", "fr": "Sombre", "nl": "Donker"}),
)

#: Read BEFORE THE FIRST PAINT, so a reader who chose light does not watch a
#: dark page flash first. Inline and in the head for that reason: a linked file
#: cannot promise to run before the stylesheets apply.
THEME_BOOTSTRAP = (
    "<script>(function(){try{var t=localStorage.getItem('belpulse-theme');"
    "if(t!=='auto')document.documentElement.setAttribute('data-theme',"
    "t==='dark'?'dark':'light');"
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
BRAND = "BelPulse"
TAGLINE = {
    "en": "The data that moves Belgium forward",
    "fr": "Les données qui font avancer la Belgique",
    "nl": "De data die België vooruithelpt",
}
NAV_LINKS = (
    ("/", {"en": "Home", "fr": "Accueil", "nl": "Start"}),
    ("/communes.html", {"en": "Communes", "fr": "Communes", "nl": "Gemeenten"}),
    ("/map.html", {"en": "Maps", "fr": "Cartes", "nl": "Kaarten"}),
    ("/all_data.html", {"en": "All data", "fr": "Toutes les données", "nl": "Alle data"}),
    ("/about.html", {"en": "Methodology", "fr": "Méthodologie", "nl": "Methodologie"}),
)
HOME_LABEL = {"en": "Home", "fr": "Accueil", "nl": "Start"}
NAV_LABEL = {"en": "Main navigation", "fr": "Navigation principale", "nl": "Hoofdnavigatie"}
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

    # THE SWITCHER, as three plain links. The generated commune pages have no
    # switcher at all -- a reader on /local/85039/fr/ cannot reach the Dutch
    # version except through a crawler's hreflang -- and every hand-built page
    # switches with JavaScript against one URL. Because these are three real
    # routes, links work, and they work with scripting disabled.
    #
    # RELATIVE, unlike the alternates above. hreflang is a statement to a
    # crawler about the live site, so it is absolute; a link a reader clicks
    # has to work wherever the site is being served -- a local checkout, a
    # fork's Pages, a review build. Absolute hrefs here would walk a reader
    # off the server they are on.
    switcher = ""
    if switch_links and len(switch_links) > 1:
        items = "".join(
            (
                f'<a href="{escape(switch_links[code], quote=True)}" hreflang="{code}" '
                f'lang="{code}" data-lang="{code}"'
                + (' aria-current="page"' if code == lang else "")
                + f">{escape(LANGUAGE_NAMES.get(code, code))}</a>"
            )
            for code in LANGS
            if code in switch_links
        )
        switcher = (
            f'\n<nav class="bp-lang-switch" aria-label='
            f'"{escape(SWITCHER_LABEL.get(lang, SWITCHER_LABEL[DEFAULT_LANG]), quote=True)}">'
            f"{items}</nav>"
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

    # Carry the choice to the JavaScript pages. A reader who picks French here
    # and clicks through to map.html should not land in English -- one choice
    # across the whole site is docs/features/i18n.md's own goal, and the key is
    # I18N.STORAGE_KEY. The link has already navigated by the time this runs,
    # so a reader without JavaScript loses the memory, not the page.
    # THE THEME SWITCH. Self-contained rather than calling
    # BPComponents.initThemeToggle: components.js is linked only by a page that
    # has an interactive block, and every page carries this control.
    scripts += (
        "\n<script>(function(){var k='belpulse-theme',"
        "b=document.querySelectorAll('.bp-theme-toggle button[data-theme-choice]'),"
        "saved=null;try{saved=localStorage.getItem(k);}catch(e){}"
        "b.forEach(function(x){x.setAttribute('aria-pressed',"
        "String((saved||'light')===x.getAttribute('data-theme-choice')));"
        "x.addEventListener('click',function(){"
        "var c=x.getAttribute('data-theme-choice');"
        "if(c==='auto'){document.documentElement.removeAttribute('data-theme');}"
        "else{document.documentElement.setAttribute('data-theme',c);}"
        "try{localStorage.setItem(k,c);}catch(e){}"
        "b.forEach(function(y){y.setAttribute('aria-pressed',String(y===x));});"
        "});});})();</script>"
    )

    if switcher:
        scripts += (
            "\n<script>document.querySelectorAll('.bp-lang-switch a')"
            ".forEach(function(a){a.addEventListener('click',function(){"
            "try{localStorage.setItem('belpulse-lang',a.dataset.lang);}catch(e){}"
            "});});</script>"
        )

    # THE TOP BAR. Every link is site-relative and prefixed the same way the
    # stylesheets are, so it resolves from a page one or two directories down.
    def _local(href):
        return (
            escape(asset_prefix + href.lstrip("/"), quote=True)
            if href != "/"
            else escape(asset_prefix or "./", quote=True)
        )

    nav_items = "".join(
        f'<a href="{_local(href)}">{escape(_text(labels, lang))}</a>' for href, labels in NAV_LINKS
    )
    # aria-pressed says `auto` here because a static file cannot know what the
    # reader chose; the script below corrects it from localStorage on load.
    # Stated rather than left off, so the control is never in no state at all.
    theme_switch = (
        '<div class="bp-theme-toggle" role="group" '
        f'aria-label="{escape(_text(THEME_LABEL, lang), quote=True)}">'
        + "".join(
            f'<button type="button" data-theme-choice="{choice}" '
            f'aria-pressed="{"true" if choice == "auto" else "false"}">'
            f"{escape(_text(labels, lang))}</button>"
            for choice, labels in THEME_CHOICES
        )
        + "</div>"
    )
    topbar = (
        '\n<header class="bp-topbar">'
        f'<a class="bp-logo" href="{_local("/")}">'
        f'<span class="bp-logo-mark"><span></span><span></span><span></span></span>'
        f"<span><strong>{escape(BRAND)}</strong>"
        f'<span class="bp-tagline">{escape(_text(TAGLINE, lang))}</span></span></a>'
        f'<nav class="bp-nav" aria-label="{escape(_text(NAV_LABEL, lang), quote=True)}">'
        f"{nav_items}</nav>"
        f'<div class="bp-topbar-actions">{theme_switch}{switcher}</div>'
        "</header>"
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
        f'<a href="{_local("/")}">{escape(_text(HOME_LABEL, lang))}</a>'
        f"<span>{escape(title)}</span></nav>"
        f"{page_title}"
        "</div>"
    )

    site_footer = (
        '\n<div class="bp-site-footer">'
        f"<span><strong>{escape(BRAND)}</strong> — {escape(_text(TAGLINE, lang))}</span>"
        f'<nav aria-label="{escape(_text(NAV_LABEL, lang), quote=True)}">{nav_items}</nav>'
        "</div>"
    )

    return (
        "<!DOCTYPE html>\n"
        f'<html lang="{escape(lang, quote=True)}">\n'
        "<head>\n"
        '    <meta charset="UTF-8">\n'
        f"    {THEME_BOOTSTRAP}\n"
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
        f"{crumb}"
        f"{fragment}"
        f"{site_footer}"
        f"{footer}"
        "\n</div>"
        f"{scripts}\n"
        "</body>\n"
        "</html>\n"
    )
