"""Batch 15b: is the block-built map equivalent to `map.html`?

This is the conversion `test_about_conversion.py` said it could not do. about
carries no municipal figure, so it proved the publish path and nothing about
the resolver. A choropleth of 565 communes proves the rest -- and the bugs it
found were all of one kind, which is what this file is shaped to catch.

EVERY DEFECT THIS CONVERSION EXPOSED WAS SILENT. Not one raised an error, and
the block reported `ready` through all of them:

  * `const MapUI = {}` in commune_map.js is a LEXICAL global, so `window.MapUI`
    is undefined and hydration returned at its first guard -- an empty map.
  * `CommuneMap.setData` wants rows (`{value, period, status}`); it was handed
    bare numbers, so every commune failed the number test and painted as
    no-data -- a complete grey map with a legend saying nothing carried a value.
  * The suppressed cells were DROPPED rather than passed as null rows, so a
    withheld figure would have been reported as missing -- two of the five
    states claude.md rule 26 keeps apart, collapsed into one.
  * `commune_map.css` was never linked, so every `var(--ramp-N)` fill resolved
    to nothing.
  * `i18n.js` was never linked, so the legend note would have printed keys.
  * The tooltip was hidden with the `hidden` ATTRIBUTE while the component
    shows it by removing a `map-hidden` CLASS -- permanently invisible.

A test asserting "the page renders" passes through all six. So the assertions
here are contracts: the exact class names, the exact row shape, the exact
assets. They compare DATA against the published payload, never pixels.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HAND_BUILT = REPO_ROOT / "map.html"
DOCUMENT = REPO_ROOT / "config" / "pages" / "map" / "published.json"
BUILT = REPO_ROOT / "preview" / "map.html"
EXPORTER = REPO_ROOT / "scripts" / "export_page_documents.py"

MAP_CSS = REPO_ROOT / "assets" / "commune_map.css"
MAP_JS = REPO_ROOT / "assets" / "commune_map.js"
BLOCKS_JS = REPO_ROOT / "assets" / "belpulse" / "blocks.js"


def _build() -> str:
    result = subprocess.run(
        [sys.executable, str(EXPORTER)], capture_output=True, text=True, cwd=REPO_ROOT
    )
    assert result.returncode == 0, result.stderr
    return BUILT.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def built() -> str:
    return _build()


@pytest.fixture(scope="module")
def block_data(built: str) -> dict:
    """The resolved payload the page hands the browser."""
    match = re.search(
        r'<script id="bp-block-data" type="application/json">(.*?)</script>', built, re.S
    )
    assert match, "the page ships no block data, so the map can only render empty"
    return json.loads(match.group(1).replace("<\\/", "</"))


def _map_payload(block_data: dict) -> dict:
    maps = [p for p in block_data.values() if p and p.get("values") is not None]
    assert len(maps) == 1, f"expected exactly one map payload, got {len(maps)}"
    return maps[0]


# --- the figures match the published payload, commune by commune -----------


def test_every_value_matches_the_published_indicator_payload(block_data):
    """Not a sample: all 565, against the file map.html itself reads.

    claude.md rule 36 -- no figure in this repository is hand-typed, including
    in its tests. The expectation is the payload.
    """
    payload = _map_payload(block_data)
    published = json.loads(
        (REPO_ROOT / "public" / "data" / "indicators" / f"{payload['indicator']}.json").read_text(
            encoding="utf-8"
        )
    )
    communes = published["communes"]

    expected = {
        nis: cell["value"]
        for nis, cell in communes.items()
        if cell.get("status") != "suppressed" and cell.get("value") is not None
    }
    assert payload["values"] == expected
    assert len(expected) > 0, "an empty expectation would make this test vacuous"


def test_withheld_figures_are_named_and_never_valued(block_data):
    """Rule 26: withheld is not missing, and it is certainly not zero.

    A suppressed commune must appear in `suppressed` and NOT in `values` --
    the component counts them separately and says so in its coverage note.
    """
    payload = _map_payload(block_data)
    published = json.loads(
        (REPO_ROOT / "public" / "data" / "indicators" / f"{payload['indicator']}.json").read_text(
            encoding="utf-8"
        )
    )
    withheld = {
        nis for nis, cell in published["communes"].items() if cell.get("status") == "suppressed"
    }
    assert set(payload["suppressed"]) == withheld
    assert withheld.isdisjoint(payload["values"])


def test_the_payload_carries_what_the_component_needs_to_format(block_data):
    """unit, decimals and direction come from metadata, never guessed.

    Without them the legend ticks lose their € or %, and the tooltip prints a
    raw float -- the map would be right and unreadable.
    """
    payload = _map_payload(block_data)
    for key in ("unit", "decimals", "direction", "provenance"):
        assert key in payload, f"the resolved map payload has no {key}"
    assert payload["state"] == "ready"


def test_the_block_map_reads_the_same_indicator_family_as_map_html():
    """Both pages read `public/data/indicators/{id}.json`. If the block version
    ever grew its own source, the two pages could disagree about the same
    commune and nothing else here would notice."""
    hand = HAND_BUILT.read_text(encoding="utf-8")
    assert "public/data/indicators/" in hand
    assert "public/data/indicators/" in BLOCKS_JS.read_text(encoding="utf-8")


# --- the markup contract with commune_map.css and commune_map.js -----------


@pytest.mark.parametrize(
    "selector, why",
    [
        (
            '<svg class="map"',
            "commune_map.css styles `svg.map path`; without the class, no strokes",
        ),
        ('class="map-tip', "the component shows the tooltip by removing `map-hidden` from this"),
        ("map-hidden", "the hide mechanism is a CLASS; a `hidden` attribute is never touched"),
        ('class="mapbox"', "the component measures the tooltip against this box"),
        ('class="swatches"', "the legend colour bar"),
        ('class="ticks"', "the legend tick labels, positioned from MapUI.SWATCH_PX"),
        ('class="note"', "the coverage sentence, which is where WITHHELD is stated"),
    ],
)
def test_the_page_carries_the_markup_the_component_expects(built, selector, why):
    assert selector in built, why


def test_the_swatch_width_in_css_matches_the_constant_the_ticks_are_placed_from():
    """`.swatches div{width:64px}` and `MapUI.SWATCH_PX = 64` are one number
    written twice. Change either alone and every tick label slides off its
    seam -- a legend that is subtly, plausibly wrong."""
    css_px = re.search(r"\.swatches div\{width:(\d+)px", MAP_CSS.read_text(encoding="utf-8"))
    js_px = re.search(r"MapUI\.SWATCH_PX\s*=\s*(\d+)", MAP_JS.read_text(encoding="utf-8"))
    assert css_px and js_px
    assert css_px.group(1) == js_px.group(1)


# --- the assets a map page cannot work without ------------------------------


def test_a_map_page_links_the_choropleth_stylesheet(built):
    """Every fill the component sets is `var(--ramp-N)`, and those tokens live
    only in commune_map.css. Without this link the map draws 565 paths against
    undefined custom properties and shows nothing at all."""
    assert "assets/commune_map.css" in built


def test_a_map_page_loads_the_strings_before_the_component(built):
    """commune_map.js captures I18N in a `const` AS IT LOADS, so a copy that
    arrives afterwards is never seen and the legend prints raw keys."""
    i18n = built.index("assets/i18n.js")
    component = built.index("assets/commune_map.js")
    assert i18n < component, "i18n.js must be linked before commune_map.js"


def test_a_page_with_no_map_links_neither(tmp_path):
    """The boundary file alone is 1.2 MB. A page without a map must not pay
    for one -- Batch 0 measured map.html at 59 on performance for exactly this.
    """
    # about.html, which went canonical in Batch 15d and lives at the site root
    # now. Still the block-built page with no map -- which is the point.
    about = (REPO_ROOT / "about.html").read_text(encoding="utf-8")
    assert "commune_map.css" not in about
    assert "commune_map.js" not in about


# --- the hydration boundary -------------------------------------------------


def test_blocks_js_resolves_the_component_through_the_scope_chain():
    """`const MapUI = {}` at the top level of a classic script is a LEXICAL
    global: reachable by name, never a property of `window`. Reading it as
    `global.MapUI` returns undefined and hydration stops at its first line --
    which is exactly what happened, with no error and the block still `ready`.
    """
    source = BLOCKS_JS.read_text(encoding="utf-8")
    assert "typeof MapUI !== 'undefined'" in source
    assert "if (!global.MapUI) return;" not in source


def test_blocks_js_hands_the_component_rows_and_not_bare_numbers():
    """`setData` tests `typeof row.value === 'number'`. Bare numbers fail it
    for all 565 communes, and the map goes uniformly grey without complaining.
    """
    source = BLOCKS_JS.read_text(encoding="utf-8")
    assert "rowsFromResolved" in source and "rowsFromIndicator" in source
    assert re.search(r"status:\s*'suppressed'", source), (
        "a withheld commune must reach the component as a null-valued row, or "
        "its coverage note reports it as missing"
    )
    assert "map.setData(data.values" not in source


def test_the_boundary_file_is_fetched_through_the_asset_prefix():
    """A page one directory down asking for `data/geo/...` gets
    /preview/data/geo/..., which does not exist -- and the map draws nothing,
    silently. The third time this prefix trap appeared in this programme."""
    source = BLOCKS_JS.read_text(encoding="utf-8")
    assert "boundaries(prefix)" in source
    assert re.search(r"fetch\(\(prefix \|\| ''\) \+ GEO_URL\)", source)


# --- unchanged, and reproducible -------------------------------------------


def test_the_hand_built_map_is_untouched():
    """map.html stays live and byte-identical: the block version ships beside
    it, and nothing about this conversion is allowed to change the page readers
    are using today."""
    result = subprocess.run(
        ["git", "diff", "--stat", "--", "map.html"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    assert result.stdout.strip() == "", result.stdout


def test_two_builds_are_byte_identical(built):
    """claude.md rule 35."""
    assert _build() == built


def test_the_document_declares_the_controls_map_html_has():
    """The conversion is only honest if the block offers what the page it
    replaces offers. A subset would be a quieter page, not an equivalent one.
    """
    doc = json.loads(DOCUMENT.read_text(encoding="utf-8"))
    props = [
        b["props"] for section in doc["sections"] for b in section["blocks"] if b["type"] == "map"
    ]
    assert len(props) == 1
    assert props[0]["show_zoom"] is True
    assert props[0]["indicator_picker"] is True
    assert props[0]["show_legend"] is True
