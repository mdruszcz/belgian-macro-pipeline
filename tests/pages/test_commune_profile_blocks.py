"""The ten block types the commune profile needs (Batch A).

The generic guards in `test_page_document_render.py` already assert that every
registry type has a renderer, renders with no data, and renders at every
supported version. They cannot assert what each type is FOR, and three of these
have a property that a generic test would happily miss:

  * a block whose data does not exist must say so, not render an empty shape;
  * a block that shows a municipal figure must be in `_DATA_BEARING`, or the
    page publishes Statbel figures with no licence notice and nothing catches
    it;
  * a block that needs JavaScript must be in `_HYDRATED`, or its slot stays
    empty while the block still reports `ready` -- the Batch 15b map failure.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from src.pages import load_registry
from src.pages.render import BLOCK_RENDERERS, render_document
from src.pages.shell import _DATA_BEARING, _HYDRATED

REPO = Path(__file__).resolve().parents[2]

#: Added for the commune profile. Kept as a literal list rather than derived
#: from the registry: this file is about what these ten are supposed to do, and
#: a list derived from the thing under test would shrink silently if one were
#: dropped.
NEW_TYPES = [
    "cta_band",
    "feature_tiles",
    "link_list",
    "section_nav",
    "stat_tile",
    "photo",
    "sources_panel",
    "ranking_list",
    "neighbour_list",
    "comparison_picker",
]

#: Of those, the ones that can put a municipal number in front of a reader.
SHOWS_A_FIGURE = ["stat_tile", "ranking_list", "neighbour_list", "sources_panel"]


@pytest.fixture(scope="module")
def registry():
    return load_registry()


def _render(block_type, props, data=None, lang="en", registry=None):
    doc = {
        "schema_version": 2,
        "page_id": "t",
        "revision": 1,
        "route": "/about.html",
        "page_type": "blank",
        "seo": {"title": {"en": "t", "fr": "t", "nl": "t"}},
        "sections": [
            {
                "id": "s",
                "blocks": [
                    {
                        "id": "b",
                        "locked": False,
                        "type": block_type,
                        "version": registry.current_version(block_type),
                        "props": props,
                        "layout": {
                            bp: {"x": 0, "y": 0, "w": 12, "h": 2}
                            for bp in ("mobile", "tablet", "desktop")
                        },
                    }
                ],
            }
        ],
    }
    return render_document(
        doc, registry=registry, lang=lang, data={"b": data} if data is not None else None
    )


TRI = {"en": "En", "fr": "Fr", "nl": "Nl"}


# --- every new type is actually wired ---------------------------------------


@pytest.mark.parametrize("block_type", NEW_TYPES)
def test_the_type_is_declared_and_has_a_renderer(block_type, registry):
    assert block_type in registry.types, f"{block_type} is not in the registry"
    assert block_type in BLOCK_RENDERERS, f"{block_type} has no renderer"


# --- the two registrations that fail SILENTLY --------------------------------


@pytest.mark.parametrize("block_type", SHOWS_A_FIGURE)
def test_a_block_showing_a_figure_owes_the_licence_notice(block_type):
    """Missing from `_DATA_BEARING` means `declares_municipal_data()` returns
    false and the page ships with NO Statbel attribution. `wrap()` cannot catch
    it, because `wrap()` is the thing asking this set. Statbel's 2015 licence
    terminates automatically on non-compliance and this project has had one
    breach already."""
    assert block_type in _DATA_BEARING


def test_the_picker_shows_no_figure_and_so_owes_nothing():
    """The other direction, so the set stays a real distinction rather than
    'everything we added'. The picker chooses which geographies a page compares;
    it renders no number of its own."""
    assert "comparison_picker" not in _DATA_BEARING


def test_a_block_needing_javascript_is_registered_for_it():
    """Missing from `_HYDRATED` means the scripts are never linked, so the slot
    stays empty while the block still reports `ready` -- exactly how the
    Batch 15b map failed, silently and looking fine."""
    assert "comparison_picker" in _HYDRATED
    # kpi_card too, since its sparkline is drawn in the browser.
    assert "kpi_card" in _HYDRATED

    # A `data-hydrate` value names a SLOT, not a block type -- `spark` is a
    # slot inside kpi_card, not a type of its own -- so the check is that
    # blocks.js has a hydrator for every slot the renderer emits. Asserting
    # slot names against _HYDRATED was the wrong pairing and passed only while
    # every slot happened to share its block type's name.
    slots = set(
        re.findall(
            r'data-hydrate="([a-z_]+)"',
            (REPO / "src" / "pages" / "render.py").read_text(encoding="utf-8"),
        )
    )
    hydrators = (REPO / "assets" / "belpulse" / "blocks.js").read_text(encoding="utf-8")
    for name in slots:
        assert re.search(rf"\b{name}\s*:", hydrators), (
            f"render.py emits data-hydrate={name} but blocks.js has no hydrator for it, "
            "so the slot stays empty while the block still reports ready"
        )


# --- absent data is stated, never drawn as an empty shape --------------------


def test_a_photo_with_no_source_renders_a_frame_and_its_words(registry):
    """No photo library exists in this repository, so this is the NORMAL case,
    not an edge case. A broken-image icon would read as a bug."""
    html = _render("photo", {"alt": TRI, "overlay_title": TRI, "caption": TRI}, registry=registry)
    assert "bp-photo-empty" in html
    assert "<img" not in html
    assert "En" in html


def test_a_neighbour_list_says_why_it_is_empty(registry):
    """This pipeline holds no commune-adjacency table anywhere -- not 'not
    loaded yet'. The reason is editorial, so it travels in the document."""
    reason = {"en": "No adjacency data", "fr": "Pas de donnees", "nl": "Geen gegevens"}
    html = _render(
        "neighbour_list", {"title": TRI, "unavailable_reason": reason}, registry=registry
    )
    assert "No adjacency data" in html
    assert "bp-commune-card" not in html


def test_a_rank_with_no_figure_is_marked_as_waiting_on_the_model(registry):
    """A named composite rank needs the unbuilt peer model. It renders with the
    --composite modifier components.css already carries for this case, rather
    than showing a number borrowed from somewhere else."""
    html = _render("ranking_list", {"label": TRI}, registry=registry)
    assert "bp-ranking-item--composite" in html
    # The resolver's `percentile` payload nests each scope under `scopes` --
    # the block reads the one its `scope` prop names, so a regional block
    # cannot quietly show a national rank.
    html_with_rank = _render(
        "ranking_list",
        {"label": TRI, "scope": "national"},
        data={"state": "ready", "scopes": {"national": {"rank": 78, "peers": 581}}},
        registry=registry,
    )
    assert "78 / 581" in html_with_rank
    assert "bp-ranking-item--composite" not in html_with_rank


# --- rule 28: provenance comes from the data, never from the document -------


def test_a_sources_panel_takes_its_date_from_the_resolved_binding(registry):
    """Rule 28 puts source, unit, period and freshness in published metadata so
    a page cannot state a date that has drifted from the figures."""
    html = _render(
        "sources_panel",
        {"title": TRI, "body": TRI},
        data={"state": "ready", "provenance": "statbel - retrieved 2026-09-05"},
        registry=registry,
    )
    assert "statbel - retrieved 2026-09-05" in html
    assert "bp-freshness" in html


# --- links go through the same guard everywhere -----------------------------


@pytest.mark.parametrize(
    "block_type, props",
    [
        ("link_list", {"links": [{"label": TRI, "href": "https://evil.example"}]}),
        (
            "section_nav",
            {"accessible_name": TRI, "items": [{"label": TRI, "href": "//evil.example"}]},
        ),
        ("feature_tiles", {"tiles": [{"label": TRI, "href": "/a/../../etc/passwd"}]}),
        (
            "cta_band",
            {"heading": TRI, "primary_cta": {"label": TRI, "href": "javascript:alert(1)"}},
        ),
    ],
)
def test_no_new_block_can_emit_an_off_site_link(block_type, props, registry):
    """Every href goes through `safe_href`, which refuses an absolute URL, a
    protocol-relative one and traversal alike."""
    html = _render(block_type, props, registry=registry)
    assert "evil.example" not in html
    assert "javascript:" not in html
    assert "etc/passwd" not in html


# --- the tab strip is navigation, not a tablist -----------------------------


def test_the_section_nav_works_without_javascript(registry):
    """The design draws tabs, but the page below is one scrolling document.
    In-page anchors work with scripting off; a tablist would not, and would
    also lie to a screen reader about there being panels to step through."""
    items = [{"label": TRI, "href": "#demography"}]
    html = _render("section_nav", {"accessible_name": TRI, "items": items}, registry=registry)
    assert 'href="#demography"' in html
    assert 'role="tablist"' not in html
    assert "<nav" in html


# --- the registry records the reversal, rather than hiding it ---------------


def test_the_registry_says_why_ranking_list_stopped_being_excluded():
    raw = json.loads(
        (REPO / "assets" / "belpulse" / "blocks" / "registry.json").read_text(encoding="utf-8")
    )
    assert "ranking_list" not in raw["deliberately_absent"]
    assert "ranking_list" in raw.get("reversals", {}), (
        "a type removed from deliberately_absent must record why, or the next "
        "reader sees a decision reversed with no reasoning"
    )
