"""The semantic rejection list for a page document.

Everything JSON Schema cannot express, because it needs another file to decide:
block types and versions against the registry, indicator codes and NIS codes
against published metadata, grid bounds and overlap, duplicate block ids,
accessible names on interactive blocks, and content safety. Each finding
carries its own error code from `schema.ERROR_CODES`, and each is a REJECTION
-- block_contract.md's list rejects, it does not warn, and Batch 9 is the only
place these checks exist.

=====================================================================
THE GEOGRAPHY BOUNDARY -- read this before deciding to be helpful.
=====================================================================
This module existence-checks NIS codes against an already-published list. That
is NOT geography resolution, and it must never become it. `src/pages/` must
never:

  * derive a `geo_id` from a NIS code,
  * walk `parent_geo_id` to find a parent or child geography,
  * apply `config/geography/municipality_crosswalk.csv` to SUBSTITUTE a
    successor commune for a retired one,
  * reason about period-dependent validity -- ten NIS codes have two rows in
    `config/geography/geographies.csv` with different validity windows, and
    only `resolve_geo()` knows that.

`resolve_geo()` and everything under `src/geography/` are forbidden here
(claude.md rule 19, invariant 4). The crosswalk is read for ERROR MESSAGE TEXT
only: a code retired in a merger gets `retired_nis_code` and a message saying
so, because a maintainer or Batch 15 reading `invalid_nis_code` against a
legitimate historical code would treat it as a typo. Auto-substituting the
successor would be geography resolution, would be silently wrong across a
merger boundary, and would need an ADR.

The same boundary applies to data: this module never opens a commune payload,
never resolves a binding and never computes a value. "Does a payload exist for
this NIS" is Batch 14's question (docs/features/data_binding.md).
"""

import re
from collections.abc import Iterable

from src.pages.metadata import PageMetadata
from src.pages.registry import Registry
from src.pages.schema import (
    BREAKPOINTS,
    GRID_COLUMNS,
    PageValidationError,
    format_path,
    sort_errors,
    validate_against,
)

#: Provisional until Batch 15 converts the real pages -- it is
#: block_contract.md's own open-question list. An unknown value is rejected,
#: never passed through: a typo'd page_type that silently renders as "blank"
#: is how a page quietly loses its layout.
PAGE_TYPES = frozenset(
    {
        "homepage",
        "macro",
        "micro",
        "municipality-profile",
        "explorer",
        "data-explorer",
        "map-explorer",
        "blank",
    }
)

#: Batch 0's real route inventory. Batch 15 cannot convert a page whose route
#: is not here (invariant 2), so this list is the allowlist, not a sample.
ROUTE_EXACT = frozenset(
    {
        "/",
        "/index.html",
        "/dashboard.html",
        "/communes.html",
        "/map.html",
        "/local.html",
        "/all_data.html",
        "/about.html",
        # Batch 3 and 4's rebuilt pages, live and served, and MISSING from this
        # list until Batch 17a -- known-risks.md:41 recorded the gap and nothing
        # enforced it. tests/site/test_route_inventory.py now requires this set
        # and src/site/routes.py's inventory to agree, so a served page cannot
        # be absent from one of them again.
        "/home.html",
        "/home2.html",
        "/macro.html",
        "/micro.html",
        "/commune.html",
        "/profiles.html",
        # PREVIEW routes, added one at a time as Batch 15 converts a page.
        #
        # Batches 3 and 4 established the pattern -- ship the rebuilt page
        # beside the live one and switch the canonical route only after review
        # -- but did it with /home.html and /commune.html, which are NOT in
        # this allowlist, so a page DOCUMENT could not declare the very shape
        # that precedent set (known-risks.md:41).
        #
        # Under /preview/ rather than beside the real pages, so the whole set
        # is one decision for Batch 17's publication allowlist instead of a
        # judgement per file. Added individually, so this stays an inventory
        # rather than an open door.
        # Batch 15d cut about.html over: it is BUILT from
        # config/pages/about/published.json now, and its two translations are
        # real URLs the site publishes. Added by hand because the allowlist
        # cannot mint a route for itself (known-risks.md:44) -- which is the
        # guard working, not a gap.
        "/fr/about.html",
        "/nl/about.html",
        # /preview/about.html is GONE: the page it previewed is live. The
        # preview routes were always temporary, carried noindex, were in no
        # sitemap and were linked from nowhere.
        "/preview/map.html",
        # The commune profile, authored once against Namur. The real route it
        # is heading for is the templated /local/{nis}/ below -- this preview
        # exists so the layout can be looked at before it is multiplied by 565.
        "/preview/commune.html",
    }
)

#: Templated commune routes, carrying the literal `{nis}` placeholder. The
#: page's `context.nis` supplies the value at build time.
ROUTE_TEMPLATES = frozenset({"/local/{nis}/", "/local/{nis}/fr/", "/local/{nis}/nl/"})

#: The same three routes with a concrete NIS substituted, which is what the
#: 565 published commune pages actually live at. The NIS is then checked like
#: any other (existence, level, retirement).
ROUTE_CONCRETE_COMMUNE = re.compile(r"^/local/([0-9]{5})/(?:fr/|nl/)?$")

#: Providers whose binding is meaningless without an indicator code.
PROVIDERS_REQUIRING_INDICATOR = frozenset(
    {"national", "municipal", "indicator_snapshot", "comparison", "ranking"}
)

#: The indicator scope a provider CANNOT resolve. Only these two providers are
#: unambiguous about which published file they draw from; every other provider
#: may legitimately name an indicator from either half of the universe.
PROVIDER_WRONG_SCOPE = {"national": "municipal", "municipal": "national"}

#: The geography levels a provider can legitimately address. Trap A: every one
#: of the 622 published geographies has a `nis_code`, so a bare membership test
#: passes `01000` (country) for a municipal binding.
PROVIDER_LEVELS = {
    "national": frozenset({"country"}),
    "municipal": frozenset({"municipality"}),
}

#: Aggregate functions that imply a SUM across geographies. `sum` is the only
#: function the schema lets a binding declare at all, which is deliberate:
#: deciding whether a non-additive indicator has a defensible RECOMPUTED
#: aggregate is Batch 14's job, by importing `src/analytics/aggregate.py`'s own
#: answer. Copying that module's RECOMPUTABLE_FUNCTIONS set into src/pages/
#: would violate claude.md rule 19 and invariant 4 -- and would be wrong in
#: practice, since this pipeline genuinely publishes a province-level
#: AVG_NET_TAXABLE_INCOME despite it being published `additive: false`.
SUM_IMPLYING_FUNCTIONS = frozenset({"sum"})

#: Props keys whose value is a URL. Only these are checked as URLs; scanning
#: every string for a scheme would reject a French label like
#: "Attention: ceci" on its colon.
URL_KEYS = frozenset(
    {
        "action",
        "background_url",
        "href",
        "icon_url",
        "image",
        "link",
        "poster",
        "src",
        "srcset",
        "url",
    }
)

#: Site-relative path or in-page fragment. No scheme, no host, no
#: protocol-relative `//host` (claude.md rule 23).
SAFE_URL = re.compile(r"^(?:/[A-Za-z0-9._~/-]*|#[A-Za-z0-9_-]+)$")

#: Two structural rules that hold for EVERY string in the document, not just a
#: URL-shaped prop: a script scheme, and a host-bearing absolute URL. Both are
#: safe to apply globally because neither can occur in legitimate Belgian prose
#: -- unlike a bare scheme regex, which would fire on a French label such as
#: "Attention : ceci", or a `data:` check, which would fire on "Data: Statbel".
#: Those two stay confined to URL_KEYS above, which is why this is a structural
#: rule and not the keyword scan spec §12 forbids.
SCRIPT_SCHEME = re.compile(r"(?:javascript|vbscript)\s*:", re.IGNORECASE)

_TRILINGUAL_KEYS = ("en", "fr", "nl")


def _is_trilingual(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    if set(value) != set(_TRILINGUAL_KEYS):
        return False
    return all(isinstance(value[k], str) and value[k].strip() for k in _TRILINGUAL_KEYS)


def _is_int(value: object) -> bool:
    """A strict integer.

    `isinstance(True, int)` is True, and JSON Schema's `integer` type accepts
    `12.0` (verified against jsonschema 4.26 -- draft 2020-12 says a number
    with a zero fractional part is an integer). Both would let a bogus grid
    coordinate through, so the grid check does not rely on the schema for this.
    """
    return isinstance(value, int) and not isinstance(value, bool)


def _iter_blocks(doc: dict) -> Iterable[tuple[tuple, dict, dict]]:
    """(path, section, block) for every block, defensively.

    Runs even on a document that failed schema validation, because
    `validate_document` collects all errors rather than stopping at the first
    layer.
    """
    sections = doc.get("sections")
    if not isinstance(sections, list):
        return
    for s_index, section in enumerate(sections):
        if not isinstance(section, dict):
            continue
        blocks = section.get("blocks")
        if not isinstance(blocks, list):
            continue
        for b_index, block in enumerate(blocks):
            if not isinstance(block, dict):
                continue
            yield ("sections", s_index, "blocks", b_index), section, block


# --------------------------------------------------------------------------
# page-level checks
# --------------------------------------------------------------------------


def check_page_type(doc: dict) -> list[PageValidationError]:
    page_type = doc.get("page_type")
    if page_type in PAGE_TYPES:
        return []
    return [
        PageValidationError(
            "unknown_page_type",
            "page_type",
            f"{page_type!r} is not a known page type ({', '.join(sorted(PAGE_TYPES))})",
        )
    ]


def check_route(doc: dict, metadata: PageMetadata) -> list[PageValidationError]:
    """Allowlisted routes only.

    Traversal, schemes, hosts, backslashes and encoded separators are all
    rejected by the allowlist itself -- nothing containing them can match an
    exact route, a template or the concrete-commune pattern. They are named in
    the message anyway so a rejection is diagnosable.
    """
    route = doc.get("route")
    if not isinstance(route, str):
        return [
            PageValidationError("invalid_route", "route", f"route must be a string, got {route!r}")
        ]

    if route in ROUTE_EXACT or route in ROUTE_TEMPLATES:
        return []

    match = ROUTE_CONCRETE_COMMUNE.match(route)
    if match:
        return check_nis(match.group(1), "municipality", ("route",), metadata)

    return [
        PageValidationError(
            "invalid_route",
            "route",
            f"{route!r} is not an allowlisted route. Allowed: "
            f"{', '.join(sorted(ROUTE_EXACT | ROUTE_TEMPLATES))}, or /local/<5-digit NIS>/ "
            "with an optional fr/ or nl/ suffix. No traversal, scheme, host, backslash or "
            "encoded separator can match any of those.",
        )
    ]


def check_context(doc: dict, metadata: PageMetadata) -> list[PageValidationError]:
    context = doc.get("context")
    if not isinstance(context, dict):
        return []
    nis = context.get("nis")
    if not isinstance(nis, str) or nis == "{nis}":
        # `{nis}` is the template placeholder; there is no value to check yet.
        return []
    return check_nis(nis, "municipality", ("context", "nis"), metadata)


def check_block_ids(doc: dict) -> list[PageValidationError]:
    """Unique across the WHOLE document, not per section.

    Undo/redo history can refer to a deleted block's id, so an id colliding
    with a live block in another section would make history ambiguous.
    """
    errors: list[PageValidationError] = []
    first_seen: dict[str, str] = {}
    for path, _section, block in _iter_blocks(doc):
        block_id = block.get("id")
        if not isinstance(block_id, str):
            continue
        where = format_path((*path, "id"))
        if block_id in first_seen:
            errors.append(
                PageValidationError(
                    "duplicate_block_id",
                    where,
                    f"block id {block_id!r} is already used at {first_seen[block_id]}",
                )
            )
        else:
            first_seen[block_id] = where
    return errors


# --------------------------------------------------------------------------
# block-level checks
# --------------------------------------------------------------------------


def check_block_type_and_version(
    path: tuple, block: dict, registry: Registry
) -> list[PageValidationError]:
    block_type = block.get("type")
    version = block.get("version")

    if not isinstance(block_type, str) or not registry.has_type(block_type):
        return [
            PageValidationError(
                "unknown_block_type",
                format_path((*path, "type")),
                f"{block_type!r} is not a declared block type. The registry is the only "
                f"source of block types; declared: {', '.join(registry.types)}",
            )
        ]

    if not registry.supports_version(block_type, version):
        return [
            PageValidationError(
                "unsupported_block_version",
                format_path((*path, "version")),
                f"block type {block_type!r} does not support version {version!r}; "
                f"supported: {registry.supported_versions(block_type)}",
            )
        ]
    return []


def check_props(path: tuple, block: dict, registry: Registry) -> list[PageValidationError]:
    """A block's props against its own version's registry schema.

    `additionalProperties: false` in every props schema is the bulk of the
    content-safety story: an unrecognised key is rejected before anything
    examines its value.
    """
    block_type = block.get("type")
    version = block.get("version")
    if not registry.supports_version(block_type, version):
        return []
    props = block.get("props")
    if not isinstance(props, dict):
        return []
    schema = registry.props_schema(block_type, version)
    return validate_against(schema, props, (*path, "props"))


def check_binding_presence(
    path: tuple, block: dict, registry: Registry
) -> list[PageValidationError]:
    """Does this block type take a binding at all?

    Reported as `schema_violation` rather than a code of its own: the closed
    vocabulary has none for it, and it is a shape error, not a data one.
    """
    block_type = block.get("type")
    if not isinstance(block_type, str) or not registry.has_type(block_type):
        return []
    binding = block.get("binding")
    where = format_path((*path, "binding"))
    if binding is not None and not registry.accepts_binding(block_type):
        return [
            PageValidationError(
                "schema_violation",
                where,
                f"block type {block_type!r} is static and accepts no binding",
            )
        ]
    # THE ONE MAP WITH NOTHING TO BIND. A locator answers "where is this
    # commune"; it draws no figures at all, so requiring an indicator would
    # make a page fetch a payload it never reads in order to satisfy a rule
    # about having something to show -- when what it shows is the outline.
    locator = block_type == "map" and bool((block.get("props") or {}).get("locate_context"))
    if binding is None and registry.requires_binding(block_type) and not locator:
        return [
            PageValidationError(
                "schema_violation",
                where,
                f"block type {block_type!r} needs a binding to have anything to show",
            )
        ]
    return []


def check_accessible_name(
    path: tuple, block: dict, registry: Registry
) -> list[PageValidationError]:
    """An interactive block must carry a trilingual accessible name.

    Trilingual, all three languages non-empty (claude.md rule 7): an
    English-only accessible name is not an accessible name on a French or
    Dutch page.
    """
    block_type = block.get("type")
    if not isinstance(block_type, str) or not registry.has_type(block_type):
        return []
    if not registry.is_interactive(block_type):
        return []
    props = block.get("props")
    name = props.get("accessible_name") if isinstance(props, dict) else None
    if _is_trilingual(name):
        return []
    return [
        PageValidationError(
            "missing_accessible_name",
            format_path((*path, "props", "accessible_name")),
            f"interactive block type {block_type!r} needs an accessible_name with "
            "non-empty en, fr and nl strings",
        )
    ]


# --------------------------------------------------------------------------
# grid
# --------------------------------------------------------------------------


def check_grid_position(path: tuple, block: dict) -> list[PageValidationError]:
    errors: list[PageValidationError] = []
    layout = block.get("layout")
    if not isinstance(layout, dict):
        return errors

    for breakpoint_name in BREAKPOINTS:
        cell = layout.get(breakpoint_name)
        if not isinstance(cell, dict):
            continue
        where = format_path((*path, "layout", breakpoint_name))
        columns = GRID_COLUMNS[breakpoint_name]

        bad_types = [k for k in ("x", "y", "w", "h") if not _is_int(cell.get(k))]
        if bad_types:
            errors.append(
                PageValidationError(
                    "invalid_grid_position",
                    where,
                    f"grid coordinates must be whole numbers of grid units, not pixels or "
                    f"fractions; {', '.join(bad_types)} "
                    f"is {', '.join(repr(cell.get(k)) for k in bad_types)}",
                )
            )
            continue

        x, y, w, h = cell["x"], cell["y"], cell["w"], cell["h"]
        if w < 1 or h < 1:
            errors.append(
                PageValidationError(
                    "invalid_grid_position",
                    where,
                    f"a block must occupy at least 1x1 grid cell, got w={w}, h={h}",
                )
            )
        if x < 0 or y < 0:
            errors.append(
                PageValidationError(
                    "invalid_grid_position",
                    where,
                    f"grid origin cannot be negative, got x={x}, y={y}",
                )
            )
        if x >= 0 and w >= 1 and x + w > columns:
            errors.append(
                PageValidationError(
                    "invalid_grid_position",
                    where,
                    f"x={x} plus w={w} runs past the {columns}-column {breakpoint_name} grid",
                )
            )
    return errors


def _rect(cell: dict) -> tuple[int, int, int, int] | None:
    if not isinstance(cell, dict):
        return None
    if not all(_is_int(cell.get(k)) for k in ("x", "y", "w", "h")):
        return None
    if cell["w"] < 1 or cell["h"] < 1:
        return None
    return cell["x"], cell["y"], cell["w"], cell["h"]


def _rectangles_by_breakpoint(block: dict) -> dict[str, tuple[int, int, int, int]]:
    """This block's rectangle at each breakpoint where it is actually visible.

    A block hidden at a breakpoint occupies nothing there, so `visibility` is
    honoured rather than ignored.
    """
    layout = block.get("layout")
    if not isinstance(layout, dict):
        return {}
    visibility = block.get("visibility")
    out: dict[str, tuple[int, int, int, int]] = {}
    for breakpoint_name in BREAKPOINTS:
        if isinstance(visibility, dict) and visibility.get(breakpoint_name) is False:
            continue
        rect = _rect(layout.get(breakpoint_name))
        if rect is not None:
            out[breakpoint_name] = rect
    return out


def check_grid_overlap(doc: dict) -> list[PageValidationError]:
    """Two blocks occupying the same grid cell, within one section.

    Checked at EVERY breakpoint independently, and reported per (pair,
    breakpoint), because in this document format every breakpoint's x/y/w/h is
    explicitly authored. There is no reflow engine: nothing derives the tablet
    or mobile rectangle from the desktop one, and nothing "converges" as the
    grid narrows from 12 columns to 8 to 4. So two blocks both declared at
    mobile (0, 0, 4, 2) is not a reflow artefact -- it is an authored
    collision, exactly as much a mistake as the same collision on desktop, and
    a viewport nobody validates is a viewport where a block silently covers
    another one.

    Honoured throughout: a section that opts in with `allow_overlap: true` is
    skipped entirely, and a block hidden at a breakpoint (`visibility`)
    occupies nothing there, so hiding one of two stacked blocks on mobile is a
    legitimate way to resolve a collision.

    The breakpoint appears in both the error path and the message, so a
    rejection says which viewport to go and fix.
    """
    errors: list[PageValidationError] = []
    sections = doc.get("sections")
    if not isinstance(sections, list):
        return errors

    for s_index, section in enumerate(sections):
        if not isinstance(section, dict) or section.get("allow_overlap") is True:
            continue
        blocks = section.get("blocks")
        if not isinstance(blocks, list):
            continue

        placed = [
            (index, block, _rectangles_by_breakpoint(block))
            for index, block in enumerate(blocks)
            if isinstance(block, dict)
        ]
        for position, (b_index, block, rects) in enumerate(placed):
            block_id = block.get("id") if isinstance(block.get("id"), str) else f"#{b_index}"
            for other_index, other_block, other_rects in placed[:position]:
                other_id = (
                    other_block.get("id")
                    if isinstance(other_block.get("id"), str)
                    else f"#{other_index}"
                )
                for name in BREAKPOINTS:
                    if name not in rects or name not in other_rects:
                        continue
                    if not _overlaps(rects[name], other_rects[name]):
                        continue
                    errors.append(
                        PageValidationError(
                            "grid_overlap",
                            format_path(("sections", s_index, "blocks", b_index, "layout", name)),
                            f"block {block_id!r} overlaps block {other_id!r} on the "
                            f"{name} grid, and this section does not set allow_overlap",
                        )
                    )
    return errors


def _overlaps(a: tuple[int, int, int, int], b: tuple[int, int, int, int]) -> bool:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    return ax < bx + bw and bx < ax + aw and ay < by + bh and by < ay + ah


# --------------------------------------------------------------------------
# bindings
# --------------------------------------------------------------------------


def check_nis(
    nis: object, expected_level: str | None, path: tuple, metadata: PageMetadata
) -> list[PageValidationError]:
    """Existence, retirement and level for one NIS code.

    Existence-checking a published list. See this module's docstring for what
    this must never become.
    """
    where = format_path(path)
    if not isinstance(nis, str) or not re.fullmatch(r"[0-9]{5}", nis):
        return [
            PageValidationError("invalid_nis_code", where, f"{nis!r} is not a 5-digit NIS code")
        ]

    level = metadata.geo_level(nis)
    if level is None:
        retired = metadata.retirement(nis)
        if retired:
            year = (retired.get("valid_to") or "")[:4] or "an earlier merger cycle"
            name = retired.get("old_name_nl") or retired.get("old_name_fr") or ""
            label = f" ({name})" if name else ""
            successor = retired.get("new_nis")
            relationship = retired.get("relationship") or "merged"
            tail = f" into {successor}" if successor else ""
            return [
                PageValidationError(
                    "retired_nis_code",
                    where,
                    f"NIS {nis}{label} no longer exists — {relationship} in {year}{tail}. "
                    "This is a legitimate historical code, not a typo. Nothing here "
                    "substitutes the successor: choosing one is geography resolution.",
                )
            ]
        return [
            PageValidationError(
                "invalid_nis_code",
                where,
                f"NIS {nis} is not in the published geography metadata and is not a known "
                "retired code",
            )
        ]

    if expected_level is not None and level != expected_level:
        return [
            PageValidationError(
                "geo_level_mismatch",
                where,
                f"NIS {nis} is a {level}, but this binding addresses a {expected_level}",
            )
        ]
    return []


def check_binding(
    path: tuple, block: dict, doc: dict, metadata: PageMetadata
) -> list[PageValidationError]:
    binding = block.get("binding")
    if not isinstance(binding, dict):
        return []

    errors: list[PageValidationError] = []
    base = (*path, "binding")
    provider = binding.get("provider")
    indicator = binding.get("indicator")
    operation = binding.get("operation")

    # -- indicator ---------------------------------------------------------
    if provider in PROVIDERS_REQUIRING_INDICATOR and indicator is None:
        errors.append(
            PageValidationError(
                "schema_violation",
                format_path((*base, "indicator")),
                f"provider {provider!r} needs an indicator code",
            )
        )
    elif isinstance(indicator, str):
        scope = metadata.indicator_scope(indicator)
        where = format_path((*base, "indicator"))
        if scope is None:
            errors.append(
                PageValidationError(
                    "unknown_indicator",
                    where,
                    f"{indicator!r} is published in neither metadata/indicators.json "
                    "(municipal) nor national.json (national)",
                )
            )
        elif PROVIDER_WRONG_SCOPE.get(provider) == scope:
            # e.g. provider "national" naming a municipal-only indicator.
            errors.append(
                PageValidationError(
                    "unknown_indicator",
                    where,
                    f"{indicator!r} is a {scope} indicator, so provider {provider!r} "
                    "cannot resolve it",
                )
            )

    # -- geography ---------------------------------------------------------
    geography = binding.get("geography")
    expected_levels = PROVIDER_LEVELS.get(provider)
    if isinstance(geography, dict):
        mode = geography.get("mode")
        if mode == "fixed":
            # Both entries in PROVIDER_LEVELS hold exactly one level; a
            # provider with a choice of levels gets no NIS-level expectation.
            expected = (
                sorted(expected_levels)[0]
                if expected_levels is not None and len(expected_levels) == 1
                else None
            )
            errors.extend(
                check_nis(geography.get("nis"), expected, (*base, "geography", "nis"), metadata)
            )
        elif mode == "level":
            level = geography.get("level")
            if expected_levels is not None and level not in expected_levels:
                errors.append(
                    PageValidationError(
                        "geo_level_mismatch",
                        format_path((*base, "geography", "level")),
                        f"provider {provider!r} addresses "
                        f"{', '.join(sorted(expected_levels))}, not {level!r}",
                    )
                )
        elif mode == "context":
            context = doc.get("context")
            context_nis = context.get("nis") if isinstance(context, dict) else None
            if not isinstance(context_nis, str):
                errors.append(
                    PageValidationError(
                        "invalid_nis_code",
                        format_path((*base, "geography", "mode")),
                        "geography mode 'context' needs the page to declare context.nis; "
                        "this page declares none",
                    )
                )

    # -- aggregation -------------------------------------------------------
    aggregate = binding.get("aggregate")
    if operation == "aggregate" and not isinstance(aggregate, dict):
        errors.append(
            PageValidationError(
                "schema_violation",
                format_path((*base, "aggregate")),
                "operation 'aggregate' needs an aggregate object saying which function "
                "over which level",
            )
        )
    if isinstance(aggregate, dict) and isinstance(indicator, str):
        function = aggregate.get("function")
        if function in SUM_IMPLYING_FUNCTIONS and metadata.is_additive(indicator) is False:
            errors.append(
                PageValidationError(
                    "forbidden_aggregation",
                    format_path((*base, "aggregate", "function")),
                    f"{indicator!r} is published additive: false, so summing it across "
                    f"{aggregate.get('over')!r} would produce a figure the pipeline itself "
                    "refuses (claude.md rule 27). Note this rejects SUMMING only -- whether "
                    "a defensible RECOMPUTED aggregate exists is Batch 14's question, "
                    "answered by src/analytics/aggregate.py, never restated here",
                )
            )

    return errors


# --------------------------------------------------------------------------
# content safety
# --------------------------------------------------------------------------


def _walk(node, path: tuple):
    """Iterative depth-first walk. Iterative for the same reason as
    `schema._iter_nodes`: a recursive one would crash on the deeply-nested
    input the guards exist to reject."""
    stack = [(path, node)]
    while stack:
        current_path, current = stack.pop()
        yield current_path, current
        if isinstance(current, dict):
            for key, value in current.items():
                stack.append(((*current_path, key), value))
        elif isinstance(current, list):
            for index, value in enumerate(current):
                stack.append(((*current_path, index), value))


def check_content_safety(doc: dict) -> list[PageValidationError]:
    """Structural safety checks -- deliberately NOT a keyword scan.

    No SQL-keyword scanning: a legitimate French or Dutch label containing
    "Union", "ORDER" or "Sélection" would be rejected, and a validator that
    fires on real content gets switched off. `src/validation/rules.py` already
    documents that reasoning for its own severity design.

    What actually removes the attack surface, in order of how much it removes:

    1. Allowlisted keys and value types per props schema, every one
       `additionalProperties: false` (checked in `check_props`). An
       unrecognised key never gets as far as having its value examined.
    2. No `<` or `>` in any string anywhere, so no tag of any kind -- script
       or otherwise -- can be expressed (invariant 5, claude.md rule 22).
    3. No key beginning `on` inside a props object, so no inline event
       handler.
    4. URL-bearing props must be site-relative or an in-page fragment: no
       scheme (`javascript:`, `data:`, `vbscript:`), no host, no
       protocol-relative `//host`, no traversal (claude.md rule 23).
    5. Rich text is an allowlisted NODE set in the registry schema, not a
       sanitiser over raw HTML -- there is no node that carries markup.
    """
    errors: list[PageValidationError] = []

    for path, node in _walk(doc, ()):
        if isinstance(node, str) and ("<" in node or ">" in node):
            errors.append(
                PageValidationError(
                    "unsafe_content",
                    format_path(path),
                    "'<' and '>' are not allowed in a page document: no markup, no tag, "
                    "no script (invariant 5)",
                )
            )
        if isinstance(node, str) and SCRIPT_SCHEME.search(node):
            errors.append(
                PageValidationError(
                    "unsafe_content",
                    format_path(path),
                    "a javascript: or vbscript: URL is not allowed anywhere in a page "
                    "document, not only in a URL-shaped prop (claude.md rule 22)",
                )
            )
        if isinstance(node, str) and "://" in node:
            errors.append(
                PageValidationError(
                    "unsafe_content",
                    format_path(path),
                    "an absolute URL with a host is not allowed anywhere in a page "
                    "document (claude.md rule 23)",
                )
            )
        if isinstance(node, str) and path and path[-1] in URL_KEYS:
            if not SAFE_URL.match(node) or ".." in node or node.startswith("//"):
                errors.append(
                    PageValidationError(
                        "unsafe_content",
                        format_path(path),
                        f"{node!r} is not a site-relative path or in-page fragment. A page "
                        "document carries no scheme, no host and no traversal "
                        "(claude.md rule 23)",
                    )
                )

    for path, _section, block in _iter_blocks(doc):
        props = block.get("props")
        if not isinstance(props, dict):
            continue
        for prop_path, prop_node in _walk(props, (*path, "props")):
            if not isinstance(prop_node, dict):
                continue
            for key in prop_node:
                if isinstance(key, str) and key.lower().startswith("on"):
                    errors.append(
                        PageValidationError(
                            "unsafe_content",
                            format_path((*prop_path, key)),
                            f"prop key {key!r} looks like an inline event handler; no "
                            "arbitrary JavaScript lives in a page document "
                            "(claude.md rule 22)",
                        )
                    )

    return errors


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------


def validate_semantics(
    doc: dict, *, metadata: PageMetadata, registry: Registry
) -> list[PageValidationError]:
    """Every semantic rejection, collected (not raised) and sorted.

    Collected so one pass tells a page author everything wrong with their
    document, and sorted by (path, code, message) so two runs report
    identically.
    """
    if not isinstance(doc, dict):
        return []

    errors: list[PageValidationError] = []
    errors.extend(check_page_type(doc))
    errors.extend(check_route(doc, metadata))
    errors.extend(check_context(doc, metadata))
    errors.extend(check_block_ids(doc))
    errors.extend(check_grid_overlap(doc))
    errors.extend(check_content_safety(doc))

    for path, _section, block in _iter_blocks(doc):
        errors.extend(check_block_type_and_version(path, block, registry))
        errors.extend(check_props(path, block, registry))
        errors.extend(check_binding_presence(path, block, registry))
        errors.extend(check_accessible_name(path, block, registry))
        errors.extend(check_grid_position(path, block))
        errors.extend(check_binding(path, block, doc, metadata))

    return sort_errors(errors)
