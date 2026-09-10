"""The commune profile beside its design reference.

Batch A proved the block TYPES. This file guards the things that were right in
every test and wrong on the screen -- the reasons the first build of this page
read as a stack of half-empty boxes next to the design it was built from:

* six block types had no card of their own, because the Batch 2 components
  style the INSIDE of a panel and the gallery supplied the box;
* the locator map beside the photograph drew a full-country choropleth of
  population, since a map with no figures was not a thing a block could be;
* the honest sentence a block carries about its own missing data was thrown
  away and replaced with "not available for this selection";
* half the grid was empty, and a third of the blocks that were on it had no
  data behind them at all.

None of that is visible to a test that checks markup and states. What IS
checkable is the structure underneath each one, which is what this file does.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from src.pages import load_registry
from src.pages.metadata import load_metadata
from src.pages.render import render_document
from src.pages.resolve import resolve_document

REPO = Path(__file__).resolve().parents[2]
DOCUMENT = REPO / "config" / "pages" / "commune-profile" / "published.json"
BLOCKS_CSS = REPO / "assets" / "belpulse" / "blocks.css"

#: What the page is allowed NOT to show, and why. Both are structural absences
#: in this pipeline, not gaps in the page: there is no commune-adjacency table
#: anywhere in it, and a composite rank needs the unbuilt Peer Model. Anything
#: else rendering empty is a defect, which is the point of pinning the list.
ALLOWED_EMPTY = {"cp-neighbours", "cp-rank-composite"}


@pytest.fixture(scope="module")
def document():
    return json.loads(DOCUMENT.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def rendered(document):
    registry = load_registry()
    data = resolve_document(document, metadata=load_metadata(), lang="en")
    return render_document(document, registry=registry, lang="en", data=data)


def blocks_of(document):
    for section in document["sections"]:
        yield from section["blocks"]


def test_almost_every_block_on_the_page_has_data_behind_it(rendered, document):
    """The page is FULL, and the emptiness that remains is named.

    The first version put four municipal-finance tiles and an age donut on the
    page with nothing to fill them, while eighteen real figures this pipeline
    publishes for every commune -- age bands, activity status, benefit
    recipients, dwellings, household size, recorded crime, the fiscal
    aggregates -- appeared on no page at all.
    """
    empty = set(
        re.findall(r'data-block-id="([^"]+)"[^>]*data-state="(?:unavailable|missing)"', rendered)
    )
    assert empty == ALLOWED_EMPTY, f"unexpectedly empty: {sorted(empty - ALLOWED_EMPTY)}"


def test_a_block_says_its_own_reason_rather_than_the_generic_one(rendered):
    """ "Not available for this selection" reads as a glitch a reload might
    fix. "This pipeline holds no commune-adjacency table" is the truth, and it
    is what rule 26 means by five distinguishable kinds of nothing."""
    assert "no commune-adjacency table" in rendered
    assert "Peer Model" in rendered


def test_the_locator_map_shows_the_page_subject_not_a_choropleth(rendered, document):
    """A map answering "where is this commune" carries no figures at all.

    Without the locator the header opened on the whole country coloured by
    population -- a picture of Belgium where the design asks for a picture of
    one commune.
    """
    locator = next(b for b in blocks_of(document) if b["props"].get("locate_context"))
    assert locator["binding"] is None, "a locator resolves nothing; it outlines the subject"
    assert 'data-map-locate="1"' in rendered
    # And it is READY, not unavailable: it is showing exactly what it is for.
    ready = re.search(
        rf'data-block-id="{locator["id"]}"[^>]*data-state="ready"', rendered
    )
    assert ready, "the locator renders as having nothing to show"


def test_the_page_carries_its_subject_for_the_blocks_finished_in_the_browser(rendered, document):
    """A hydrated block is handed its own payload and nothing else, so the
    commune the locator outlines can only come from the page itself -- and it
    has to be the same commune every figure was resolved for."""
    nis = document["context"]["nis"]
    assert f'data-context-nis="{nis}"' in rendered


@pytest.mark.parametrize(
    "block_type",
    sorted({b["type"] for b in blocks_of(json.loads(DOCUMENT.read_text(encoding="utf-8")))}),
)
def test_every_block_type_on_this_page_has_a_surface(block_type):
    """A block is a card on this design, and six types had no card.

    .bp-stat-tile, .bp-ranking-list and .bp-list-panel style the CONTENTS of a
    panel -- Batch 2's gallery drew the box around them -- so the whole
    right-hand column rendered as unboxed text floating on the page
    background, which reads as a stylesheet that failed to load.

    Asserted against the stylesheet rather than a browser because the failure
    is silent in both: the page renders, every word is there, and it looks
    broken.
    """
    # Comments stripped first: a rule's selector list is whatever follows the
    # previous closing brace, and this stylesheet explains itself at length.
    css = re.sub(r"/\*.*?\*/", "", BLOCKS_CSS.read_text(encoding="utf-8"), flags=re.S)
    # Either the block's own rule paints a surface, or it delegates to a
    # component that does (a KPI card, a chart card, a CTA band, the tab strip
    # and the photo frame each bring their own).
    delegating = {"kpi_card", "cta_band", "section_nav", "photo", "feature_tiles", "hero"}
    if block_type in delegating:
        pytest.skip(f"{block_type} carries its own component surface")
    assert f".bp-block--{block_type}" in css, f"no rule at all for .bp-block--{block_type}"
    surfaces = re.findall(r"([^{}]*)\{([^{}]*background:var\(--bp-surface\)[^{}]*)\}", css)
    named = {selector.strip() for selectors, _ in surfaces for selector in selectors.split(",")}
    assert any(
        f".bp-block--{block_type}" == selector for selector in named
    ), f"{block_type} renders with no card around it"


def test_the_grid_is_dense_rather_than_a_column_of_cards(document):
    """Two thirds of the desktop grid was empty.

    Counted rather than eyeballed: the sum of every block's area against the
    area of the grid it sits in. The first version filled 63% of it and read
    as a page of gaps; the design reference has almost no white between cards
    at all.
    """
    for section in document["sections"]:
        cells = [b["layout"]["desktop"] for b in section["blocks"]]
        rows = max(c["y"] + c["h"] for c in cells)
        used = sum(c["w"] * c["h"] for c in cells)
        assert (
            used / (rows * 12) > 0.85
        ), f"section {section['id']!r} fills {used / (rows * 12):.0%} of its grid"


@pytest.mark.parametrize("breakpoint_name", ["desktop", "tablet", "mobile"])
def test_no_two_blocks_overlap(document, breakpoint_name):
    """The layout is generated, and a generated layout is exactly where an
    overlap hides: two cards in one cell render on top of each other and the
    lower one is simply not there."""
    for section in document["sections"]:
        taken = {}
        for block in section["blocks"]:
            cell = block["layout"][breakpoint_name]
            for x in range(cell["x"], cell["x"] + cell["w"]):
                for y in range(cell["y"], cell["y"] + cell["h"]):
                    other = taken.get((x, y))
                    assert other is None, (
                        f"{section['id']}: {block['id']} overlaps {other} at "
                        f"({x},{y}) on {breakpoint_name}"
                    )
                    taken[(x, y)] = block["id"]


def test_the_tabs_point_at_sections_that_exist(document):
    """An in-page nav is the one kind of link a route inventory cannot check."""
    nav = next(b for b in blocks_of(document) if b["type"] == "section_nav")
    targets = {item["href"].lstrip("#") for item in nav["props"]["items"]}
    assert targets <= {s["id"] for s in document["sections"]}
