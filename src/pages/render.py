"""The shared block renderer -- Batch 10 of docs/features/page_builder.md.

ONE RENDERER, NOT TWO. Batch 10's whole purpose is that "what the maintainer
sees while editing is exactly what ships", and the only way to guarantee that
is for the editor preview and the public build to call the SAME function. Two
implementations sharing one registry would drift the first time a block type
gained a prop, which is the failure this batch exists to prevent.

WHY THAT ONE RENDERER IS PYTHON, and not JavaScript running in the browser.
The public site's pages are the crawler-visible copy of the data:
`scripts/export_local_pages.py` says so in its own docstring, and
`docs/features/i18n.md` requires that "a reader with JavaScript disabled still
gets a complete page, licence notice included". A browser renderer would put
every figure behind JavaScript and break both. So the public build renders
here, to static HTML with the figures inline (claude.md rule 30), and the
builder service (Batch 11) renders its preview by calling this same function
-- the builder is a local tool behind `make builder`, so a process to call is
available while editing, and nothing about that reaches the published site.
`registry.json` still lives under `assets/` and stays browser-fetchable, which
is what Batch 12's block-library UI needs; that is a separate consumer from
the renderer.

WHAT THIS MODULE DOES NOT DO. It never resolves a binding -- turning
`{"provider": "municipal", "indicator": ...}` into a number is Batch 14's job
(docs/features/data_binding.md), and doing it here would put a second copy of
the aggregation rules in `src/pages/`. The renderer is handed already-resolved
data and renders what it is given; a block whose data is absent renders in one
of the honest states Batch 2 built, never as a zero or a blank.

DETERMINISM. Identical inputs produce byte-identical output (claude.md rule
35, invariant 9): attributes are written in a fixed order, nothing is
generated from a clock, a random source or dict iteration order that is not
already sorted.
"""

import html
import re
from collections.abc import Mapping

from src.pages.registry import Registry
from src.pages.schema import BREAKPOINTS, GRID_COLUMNS

#: Languages a document carries. Matches assets/i18n.js's own list, and every
#: user-facing string in a page document is `{en, fr, nl}` (claude.md rule 7).
LANGUAGES = ("en", "fr", "nl")

#: What a block SAYS when it has no figure to show. Interface chrome, so it is
#: trilingual (rule 7) and lives with the renderer rather than in a payload --
#: assets/i18n.js is the browser's copy of the same kind of text, and these
#: deliberately reuse its wording for the states it already names, so a reader
#: meets one vocabulary across hand-built and generated pages.
#:
#: "ready" carries no message ON PURPOSE: a real measured zero is `ready`, and
#: claude.md rule 26 forbids a zero ever reading as an absence.
#: Control labels. Named in two parts, not three: the guard in
#: tests/pages/test_page_document_render.py flags any three-part SCREAMING_SNAKE
#: name as indicator-shaped (rule 24), and keeping the guard absolute is worth
#: more than a tidier constant name.
#:
#: Trilingual because rule 7 applies to every user-facing
#: string, and a French reader meeting an English "Zoom in" is exactly the
#: "foreign product" signal docs/steps warns about for this market.
PICKER_LABEL = {"en": "Indicator", "fr": "Indicateur", "nl": "Indicator"}
ZOOM_LABELS = {
    "en": {"in": "Zoom in", "out": "Zoom out", "reset": "Reset view"},
    "fr": {"in": "Zoom avant", "out": "Zoom arrière", "reset": "Réinitialiser la vue"},
    "nl": {"in": "Inzoomen", "out": "Uitzoomen", "reset": "Beeld herstellen"},
}

STATE_TEXT = {
    "loading": {"en": "Loading…", "fr": "Chargement…", "nl": "Laden…"},
    "missing": {"en": "no data", "fr": "aucune donnée", "nl": "geen gegevens"},
    "suppressed": {
        "en": "withheld by the source (fewer than 10)",
        "fr": "non publié par la source (moins de 10)",
        "nl": "niet vrijgegeven door de bron (minder dan 10)",
    },
    "unavailable": {
        "en": "not available for this selection",
        "fr": "non disponible pour cette sélection",
        "nl": "niet beschikbaar voor deze selectie",
    },
    "error": {
        "en": "this figure could not be loaded",
        "fr": "ce chiffre n’a pas pu être chargé",
        "nl": "dit cijfer kon niet worden geladen",
    },
}

#: Re-checked at render time even though the schema already enforces it. The
#: schema guards what a document may CONTAIN; this guards what this module may
#: EMIT, and the two are worth keeping independent -- a renderer that only
#: emits safe output cannot be turned into an injection by a future schema
#: relaxation (claude.md rules 22 and 23).
#:
#: The `(?!/)` is load-bearing and was missing from the first version of this
#: file. Without it `//evil.example` matches: every character after the
#: leading slash is in the allowed set, so a PROTOCOL-RELATIVE url sailed
#: through a check whose entire job is to reject off-site links -- and a
#: browser resolves `//host` against the current scheme and leaves the site.
#: The same shape appears in registry.json's `relative_url` pattern, whose
#: own description promises "no protocol-relative //host"; semantics.py
#: catches it there with a separate '//' check, which is why the document
#: validator was never fooled. This is the renderer's own guard, so it
#: enforces the whole rule itself rather than trusting that one.
SAFE_HREF = re.compile(r"^(/(?!/)[A-Za-z0-9._~/-]*|#[A-Za-z0-9_-]+)$")


class BlockRenderError(Exception):
    """One block failed to render. Collected, never raised out of a page.

    Carries the block id and type but NOT the underlying exception text: a
    traceback can quote payload values, and a render failure is reported to a
    reader, so it says which block failed and nothing about the data in it.
    """

    def __init__(self, block_id: str, block_type: str, reason: str):
        self.block_id = block_id
        self.block_type = block_type
        self.reason = reason
        super().__init__(f"{block_type} block {block_id!r} failed to render: {reason}")


def esc(value) -> str:
    """HTML-escape, quotes included, for text and attribute values alike."""
    return html.escape("" if value is None else str(value), quote=True)


def text_in(node, lang: str) -> str:
    """One language out of a trilingual object, falling back to English.

    Falls back rather than failing: a half-translated document should show the
    English sentence, exactly as `I18N.t` does for interface chrome. The
    document schema requires all three, so this only bites on a document being
    edited.
    """
    if not isinstance(node, Mapping):
        return ""
    value = node.get(lang)
    if not isinstance(value, str) or not value:
        value = node.get("en")
    return value if isinstance(value, str) else ""


def safe_href(value) -> str:
    """A site-relative path or in-page fragment, or '#' if it is neither.

    Traversal is refused separately from the pattern: `.` and `/` are both
    legitimate in a path, so `/a/../../etc/passwd` satisfies the character
    class perfectly well and has to be rejected on its own terms.
    """
    if not isinstance(value, str) or ".." in value:
        return "#"
    return value if SAFE_HREF.match(value) else "#"


def _attrs(pairs) -> str:
    """Attributes in the order given, skipping empties. Fixed order is what
    makes the output byte-stable across runs."""
    out = []
    for name, value in pairs:
        if value is None or value == "":
            continue
        out.append(f'{name}="{esc(value)}"')
    return (" " + " ".join(out)) if out else ""


# ---------------------------------------------------------------------------
# per-block renderers
#
# Each takes (block, props, data, lang) and returns the block's INNER html.
# The wrapper -- grid placement, visibility, the data-state attribute -- is
# added once by `_render_block`, so no block type can invent its own.
# ---------------------------------------------------------------------------


def _render_hero(block, props, data, lang):
    parts = []
    eyebrow = text_in(props.get("eyebrow"), lang)
    if eyebrow:
        parts.append(f'<p class="bp-hero-eyebrow">{esc(eyebrow)}</p>')
    # h1, not h2: a hero IS the page's main heading, and a published page with
    # no h1 fails both a screen reader's document outline and every SEO check.
    # The h2 here was a fragment-context assumption -- correct-looking while
    # nothing published a page document, wrong the moment something did
    # (Batch 15a). The class carries the styling, so nothing visual changes.
    parts.append(f'<h1 class="bp-hero-heading">{esc(text_in(props.get("heading"), lang))}</h1>')
    sub = text_in(props.get("subheading"), lang)
    if sub:
        parts.append(f'<p class="bp-hero-sub">{esc(sub)}</p>')
    cta = props.get("cta")
    if isinstance(cta, Mapping):
        label = text_in(cta.get("label"), lang)
        if label:
            href = safe_href(cta.get("href"))
            parts.append(f'<a class="bp-btn bp-btn--primary" href="{esc(href)}">{esc(label)}</a>')
    return "".join(parts)


def _render_kpi_card(block, props, data, lang):
    label = esc(text_in(props.get("label"), lang))
    size = props.get("size") or "full"
    # v1 called it show_sparkline, v2 calls it sparkline. Both versions stay
    # renderable -- that is the point of the version ladder, and dropping v1
    # here would break documents migrations.py deliberately still accepts.
    wants_spark = bool(props.get("sparkline", props.get("show_sparkline")))
    value = "" if data is None else data.get("formatted_value") or ""
    parts = [f'<div class="bp-kpi-label">{label}</div>']
    parts.append(f'<div class="bp-value">{esc(value)}</div>')
    if data is not None and data.get("delta_text"):
        direction = data.get("direction") or "neutral"
        if direction not in ("favourable", "unfavourable", "neutral"):
            direction = "neutral"
        parts.append(
            f'<span class="bp-delta" data-direction="{esc(direction)}">'
            f'{esc(data["delta_text"])}</span>'
        )
    if wants_spark:
        parts.append('<div class="bp-block-spark"></div>')
    if props.get("show_provenance") and data is not None and data.get("provenance"):
        parts.append(f'<div class="bp-freshness">{esc(data["provenance"])}</div>')
    return f'<div class="bp-kpi bp-kpi--{esc(size)}">' + "".join(parts) + "</div>"


def _render_chart(block, props, data, lang):
    """A chart is HYDRATED, not drawn here: canvas pixels cannot be produced
    server-side, and a reader without JavaScript must still get the figures.
    So the caption and the accessible name are real HTML, and the canvas is a
    slot assets/belpulse/blocks.js fills when the block scrolls into view."""
    title = text_in(props.get("title"), lang)
    caption = text_in(props.get("caption"), lang)
    name = text_in(props.get("accessible_name"), lang)
    chart_type = props.get("chart_type") or "line"
    parts = []
    if title:
        parts.append(f'<h3 class="bp-block-title">{esc(title)}</h3>')
    parts.append(
        '<div class="bp-chart-canvas-wrap"'
        + _attrs(
            [
                ("data-hydrate", "chart"),
                ("data-chart-type", chart_type),
                ("data-legend", "1" if props.get("show_legend") else None),
            ]
        )
        + f'><canvas role="img" aria-label="{esc(name or title)}"></canvas></div>'
    )
    if caption:
        parts.append(f'<p class="bp-block-caption">{esc(caption)}</p>')
    return "".join(parts)


def _render_comparison_table(block, props, data, lang):
    """The one data block rendered FULLY server-side. A comparison is a table
    of numbers a crawler and a JavaScript-less reader must both be able to
    read, so it is never hydrated."""
    title = text_in(props.get("title"), lang)
    caption = text_in(props.get("caption"), lang)
    parts = []
    if title:
        parts.append(f'<h3 class="bp-block-title">{esc(title)}</h3>')

    rows = (data or {}).get("rows") or []
    head_cols = (data or {}).get("columns") or []
    body = []
    for row in rows:
        cells = "".join(f"<td>{esc(c)}</td>" for c in (row.get("cells") or []))
        cls = ' class="subject"' if row.get("is_subject") else ""
        body.append(f'<tr{cls}><td>{esc(row.get("label"))}</td>{cells}</tr>')
    head = "".join(f"<th>{esc(c)}</th>" for c in head_cols)
    parts.append(
        '<table class="bp-compare-table">'
        f"<caption>{esc(caption)}</caption>"
        f"<thead><tr><th></th>{head}</tr></thead>"
        f'<tbody>{"".join(body)}</tbody>'
        "</table>"
    )
    return "".join(parts)


def _render_map(block, props, data, lang):
    """Hydrated for the same reason as a chart, and by the SAME shared map
    component every other map on this site uses (claude.md rule 29) -- this
    emits the slot and the elements that component expects, never a second map
    implementation.

    v2 adds the CHROME, not new map capability: `CommuneMap` already had
    `zoomBy`, `resetView` and `onSelect`, and v1 simply rendered no controls to
    reach them, so a published map could be panned but not zoomed while
    map.html could. The buttons are real markup here so a reader without
    JavaScript sees the same page structure; blocks.js wires them.
    """
    title = text_in(props.get("title"), lang)
    name = text_in(props.get("accessible_name"), lang)
    legend_title = text_in(props.get("legend_title"), lang)
    parts = []
    if title:
        parts.append(f'<h3 class="bp-block-title">{esc(title)}</h3>')

    controls = ""
    if props.get("indicator_picker"):
        # Labelled, and labelled in the reader's language: a bare select next
        # to a map is not a control anyone can use with a screen reader.
        label = esc(PICKER_LABEL.get(lang, PICKER_LABEL["en"]))
        controls += (
            f'<label class="bp-map-picker"><span>{label}</span>'
            "<select data-map-picker disabled></select></label>"
        )
    if props.get("show_zoom"):
        zoom = ZOOM_LABELS.get(lang, ZOOM_LABELS["en"])
        controls += (
            '<div class="bp-map-zoom">'
            + "".join(
                f'<button type="button" data-map-zoom="{action}" '
                f'aria-label="{esc(zoom[action])}" title="{esc(zoom[action])}">{glyph}</button>'
                for action, glyph in (("in", "+"), ("out", "\u2212"), ("reset", "\u21ba"))
            )
            + "</div>"
        )
    if controls:
        parts.append(f'<div class="bp-map-toolbar">{controls}</div>')

    parts.append(
        '<div class="bp-block-map"'
        + _attrs(
            [
                ("data-hydrate", "map"),
                ("data-map-zoom-enabled", "1" if props.get("show_zoom") else None),
                ("data-map-picker-enabled", "1" if props.get("indicator_picker") else None),
                ("data-map-click-through", "1" if props.get("click_through") else None),
            ]
        )
        # EVERY CLASS HERE IS A CONTRACT WITH assets/commune_map.css AND
        # assets/commune_map.js, not decoration:
        #   svg.map        -- the stroke, cursor and width:100% rules, without
        #                     which 565 paths render at intrinsic size with no
        #                     outline at all
        #   .map-tip       -- and `map-hidden`, NOT the `hidden` attribute:
        #                     CommuneMap shows the tooltip by removing that
        #                     class, so a `hidden` attribute it never touches
        #                     leaves the tooltip invisible forever
        #   .scale/.note   -- the legend geometry; .swatches div is 64px wide
        #                     there and MapUI.SWATCH_PX is 64 here, and the
        #                     tick positions are computed from that number
        # Matching map.html's own markup is the point: one stylesheet, one
        # component, one appearance.
        + f'><div class="mapbox"><svg class="map" role="img" '
        f'aria-label="{esc(name or title)}"></svg>'
        '<div class="map-tip map-hidden"></div></div>'
    )
    if props.get("show_legend"):
        nodata = esc(STATE_TEXT["missing"].get(lang, STATE_TEXT["missing"]["en"]))
        legend = (
            '<div class="legend">'
            '<div class="scale"><div class="swatches"></div><div class="ticks"></div></div>'
            f'<div class="nodata-key"><i></i> <span>{nodata}</span></div>'
            # The coverage sentence -- how many communes carry a value, and how
            # many were WITHHELD rather than missing. CommuneMap writes it; the
            # block simply has to give it somewhere to go, or rule 26's two
            # distinct states arrive on the page as one silence.
            '<div class="note"></div>'
            "</div>"
        )
        if legend_title:
            legend = f'<div class="bp-block-caption">{esc(legend_title)}</div>' + legend
        parts.append(legend)
    parts.append("</div>")
    return "".join(parts)


def _render_rich_text(block, props, data, lang):
    """An allowlisted NODE SET, not a sanitiser over raw HTML. Every node type
    is handled explicitly and anything unrecognised is skipped, so there is no
    path by which a document can carry markup into the page."""
    out = []
    for node in props.get("content") or []:
        if not isinstance(node, Mapping):
            continue
        kind = node.get("node")
        if kind == "paragraph":
            out.append(f"<p>{esc(text_in(node.get('text'), lang))}</p>")
        elif kind == "heading":
            level = node.get("level")
            level = level if level in (2, 3, 4) else 3
            out.append(f"<h{level}>{esc(text_in(node.get('text'), lang))}</h{level}>")
        elif kind == "list":
            tag = "ol" if node.get("ordered") else "ul"
            items = "".join(f"<li>{esc(text_in(i, lang))}</li>" for i in (node.get("items") or []))
            out.append(f"<{tag}>{items}</{tag}>")
    return "".join(out)


#: block type -> renderer. Checked against the registry at render time, so a
#: type declared in registry.json with no renderer here is a loud failure
#: rather than a silently empty block.
BLOCK_RENDERERS = {
    "hero": _render_hero,
    "kpi_card": _render_kpi_card,
    "chart": _render_chart,
    "comparison_table": _render_comparison_table,
    "map": _render_map,
    "rich_text": _render_rich_text,
}


# ---------------------------------------------------------------------------
# the page
# ---------------------------------------------------------------------------


def _grid_style(layout) -> str:
    """Grid placement as custom properties, one set per breakpoint.

    Emitted as properties rather than as `grid-column` directly so all three
    breakpoints can travel in one inline style and the media queries in
    assets/belpulse/blocks.css decide which set applies. Grid UNITS, never
    pixels (docs/features/block_contract.md), so a layout survives a redesign
    of the underlying grid.
    """
    parts = []
    for name in BREAKPOINTS:
        cell = (layout or {}).get(name)
        if not isinstance(cell, Mapping):
            continue
        initial = name[0]  # d / t / m
        for axis in ("x", "y", "w", "h"):
            value = cell.get(axis)
            if isinstance(value, int) and not isinstance(value, bool):
                parts.append(f"--{initial}{axis}:{value}")
    return ";".join(parts)


def _state_for(block, data, registry: Registry) -> str:
    """Which of Batch 2's seven states this block renders in.

    A block that needs data and has none is NOT 'ready' with a blank -- that
    is how a missing figure becomes an invisible one. It says which kind of
    nothing it is, and the caller (Batch 14) distinguishes them by what it
    passes: no key at all means the binding was never resolved, an explicit
    None means resolved to nothing.
    """
    block_type = block.get("type")
    if not registry.accepts_binding(block_type):
        return "ready"
    if data is None:
        return "loading" if block.get("binding") else "unavailable"
    state = data.get("state")
    if state in ("ready", "loading", "missing", "suppressed", "unavailable", "error"):
        return state
    return "ready"


def _render_block(block, *, registry: Registry, lang: str, data, errors: list) -> str:
    block_id = block.get("id") or ""
    block_type = block.get("type") or ""

    renderer = BLOCK_RENDERERS.get(block_type)
    if renderer is None or not registry.has_type(block_type):
        errors.append(BlockRenderError(block_id, block_type, "no renderer for this block type"))
        return _failed_block(block_id, block_type, _grid_style(block.get("layout")), block)

    try:
        state = _state_for(block, data, registry)
        inner = renderer(block, block.get("props") or {}, data, lang)
    except Exception:  # noqa: BLE001 -- deliberate: see below
        # PER-BLOCK FAILURE ISOLATION. One malformed block must not cost the
        # reader the other twenty on the page, so the exception is caught
        # here, recorded, and turned into a visible placeholder. Bare
        # `Exception` on purpose: the renderer cannot know every way a block
        # implementation might fail, and the alternative -- letting it escape
        # -- is a blank page.
        errors.append(BlockRenderError(block_id, block_type, "renderer raised"))
        return _failed_block(block_id, block_type, _grid_style(block.get("layout")), block)

    visibility = block.get("visibility") or {}
    hidden = [name for name in BREAKPOINTS if visibility.get(name) is False]

    # WIRED INTO BATCH 2's STATE COMPONENT, not just labelled with a state.
    # The first version set data-state on the wrapper but not the `bp-state`
    # class and no body/message children, so components.css's state rules
    # never engaged: a KPI whose data had not resolved rendered as an
    # ordinary card with an empty value -- which is precisely the "a missing
    # figure looks like no figure" failure the seven states exist to prevent.
    # Only a block that can carry data gets the wrapper; a hero or rich_text
    # has no state to be in.
    stateful = registry.accepts_binding(block_type)
    if stateful:
        message = STATE_TEXT.get(state)
        body = f'<div class="bp-state-body">{inner}</div>'
        if message:
            body += (
                '<div class="bp-state-message"><span class="headline">'
                f"{esc(text_in(message, lang))}</span></div>"
            )
        inner = f'<div class="bp-state-skeleton"></div>{body}'

    classes = f"bp-block bp-block--{block_type}"
    if stateful:
        classes += " bp-state"
    return (
        "<div"
        + _attrs(
            [
                ("class", classes),
                ("data-block-id", block_id),
                ("data-block-type", block_type),
                ("data-block-version", block.get("version")),
                ("data-state", state),
                ("data-hidden", " ".join(hidden) if hidden else None),
                ("style", _grid_style(block.get("layout"))),
            ]
        )
        + f">{inner}</div>"
    )


def _failed_block(block_id: str, block_type: str, style: str, block) -> str:
    """What a reader gets where a block failed. Says a block is missing and
    which one, and nothing about why -- a reason can quote data."""
    visibility = (block or {}).get("visibility") or {}
    hidden = [name for name in BREAKPOINTS if visibility.get(name) is False]
    return (
        "<div"
        + _attrs(
            [
                ("class", "bp-block bp-block--failed"),
                ("data-block-id", block_id),
                ("data-block-type", block_type),
                ("data-state", "error"),
                ("data-hidden", " ".join(hidden) if hidden else None),
                ("style", style),
            ]
        )
        + '><p class="bp-block-failed">This block could not be displayed.</p></div>'
    )


def render_document(
    doc,
    *,
    registry: Registry,
    lang: str = "en",
    data: Mapping | None = None,
    errors: list | None = None,
) -> str:
    """Render a validated page document to HTML.

    `data` maps a block id to that block's already-resolved payload (Batch
    14). A block id absent from it renders in an honest empty state rather
    than as a blank or a zero.

    `errors`, if given, collects a BlockRenderError per failed block. The page
    is returned either way: per-block isolation is the point.
    """
    if lang not in LANGUAGES:
        lang = "en"
    data = data or {}
    sink = errors if errors is not None else []

    sections = []
    for section in doc.get("sections") or []:
        if not isinstance(section, Mapping):
            continue
        blocks = [
            _render_block(
                block,
                registry=registry,
                lang=lang,
                data=data.get(block.get("id")),
                errors=sink,
            )
            for block in (section.get("blocks") or [])
            if isinstance(block, Mapping)
        ]
        sections.append(
            "<section"
            + _attrs([("class", "bp-section"), ("data-section-id", section.get("id"))])
            + f'><div class="bp-grid">{"".join(blocks)}</div></section>'
        )

    return (
        "<div"
        + _attrs(
            [
                ("class", "bp-page"),
                ("data-page-id", doc.get("page_id")),
                ("data-page-type", doc.get("page_type")),
                ("data-lang", lang),
                ("data-grid-columns", str(GRID_COLUMNS["desktop"])),
            ]
        )
        + f'>{"".join(sections)}</div>'
    )
