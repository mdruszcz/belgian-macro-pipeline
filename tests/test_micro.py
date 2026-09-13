"""Static contract for micro.html -- the Batch 7 microeconomics page.

A clone of tests/test_macro.py's contract, adapted for the one real
difference between the two pages: macro.html draws from a single payload
(national.json), while micro.html draws from TWO -- national.json for a
handful of micro-adjacent national series, and aggregates.json's `be:country`
row for municipal indicators aggregated from the ground up
(docs/features/comparison.md). "The universe" below always means the union of
both, because that is the one config/micro_sections.yaml and the exporter's
own `_check_micro_sections` are held to.

What these tests actually protect is the same property Batch 6 established --
that this page can be built with NO indicator id and NO figure in its markup,
so adding a series is a config change (docs/steps' 50% gate: "zero
indicator-specific frontend logic").
"""

import json
import re
from pathlib import Path

import pytest
import yaml

from src.pages.semantics import ROUTE_EXACT
from src.site.routes import is_indexable, root_routes, sitemap_routes

REPO = Path(__file__).resolve().parents[1]
PAGE = REPO / "micro.html"
LAYOUT = REPO / "config" / "micro_sections.yaml"
I18N = REPO / "assets" / "i18n.js"
LANGS = ("en", "fr", "nl")

#: Figures the supplied design (docs/design-references/micro.md) prints. None
#: of them is a real value this pipeline holds for the series the design
#: names, so none of them may appear on the page -- a mockup's number
#: rendered as if it were data is the one failure this page could not
#: recover from. "2015 – 2024" is the design's dropdown range for a chart
#: (grouped-bar business fabric) this pipeline cannot build at all.
DESIGN_FIGURES = (
    "1,29 million",
    "26 350",
    "4 120",
    "285 000",
    "2015 – 2024",
)


def _html() -> str:
    return PAGE.read_text(encoding="utf-8")


def _layout() -> dict:
    return yaml.safe_load(LAYOUT.read_text(encoding="utf-8"))


def _strings() -> dict:
    """The three string tables, read out of assets/i18n.js by Node-free
    parsing -- the same approach test_macro.py and test_i18n.py use."""
    text = I18N.read_text(encoding="utf-8")
    tables = {}
    for lang in LANGS:
        start = text.index(f"\n  {lang}: {{")
        end = text.index("\n  },", start)
        tables[lang] = set(re.findall(r"\n    ([A-Za-z0-9_]+):", text[start:end]))
    return tables


def _universe() -> set[str] | None:
    """Every indicator code micro.html can legally read: national.json's
    series, plus whatever carries a `be:country` row in aggregates.json.
    None if the payloads have not been built, so callers can skip cleanly.
    """
    national = REPO / "public" / "data" / "national.json"
    aggregates = REPO / "public" / "data" / "aggregates.json"
    if not national.exists():
        return None
    codes = set(json.loads(national.read_text(encoding="utf-8"))["indicators"])
    if aggregates.exists():
        agg = json.loads(aggregates.read_text(encoding="utf-8"))
        codes |= {code for code, geos in agg.get("indicators", {}).items() if "be:country" in geos}
    index = REPO / "public" / "data" / "metadata" / "indicators.json"
    if index.exists():
        codes |= {
            row["indicator_code"]
            for row in json.loads(index.read_text(encoding="utf-8"))["indicators"]
        }
    return codes


# --- the page holds no data of its own ---------------------------------------


def test_micro_names_no_indicator_anywhere():
    """The whole point of config/micro_sections.yaml. Checked against every
    id the payloads actually publish -- national.json, aggregates.json and
    metadata/indicators.json -- so it cannot be satisfied by renaming a
    constant and it grows as indicators are added."""
    codes = _universe()
    if codes is None:
        pytest.skip("site payloads not built")
    assert codes, "no indicator ids to check against, so this test would prove nothing"
    named = sorted(code for code in codes if code in _html())
    assert not named, f"micro.html names indicators directly: {named}"


def test_micro_prints_none_of_the_designs_invented_figures():
    """The mockup's KPIs, list values and chart range are illustrations. Not
    one of them is a real figure this pipeline holds for the series named;
    typing them in would publish an invention (CLAUDE.md rule 36)."""
    html = _html()
    found = [figure for figure in DESIGN_FIGURES if figure in html]
    assert not found, f"micro.html contains design mockup figures: {found}"


def test_micro_simulates_nothing():
    html = _html().lower()
    for forbidden in ("data-simulated", "setinterval(", "settimeout(function tick"):
        assert forbidden not in html, f"micro.html looks like it animates a counter: {forbidden}"
    # requestAnimationFrame is allowed exactly once, and only as a ONE-SHOT
    # redraw after layout (the comparison and housing charts are sized by the
    # row they land in). A second call is how a one-shot becomes a ticking
    # loop, so the count is the test.
    assert html.count("requestanimationframe(") <= 1, "more than one rAF: is something ticking?"


def test_micro_loads_no_third_party_asset():
    """Narrower than test_macro's identically-named check, and deliberately
    so: macro.html carries no external hyperlink at all, but this page
    renders municipal Statbel/ONEM/VAR data and therefore MUST carry the
    licence's required attribution links to those sources' own sites
    (tests/test_statbel_attribution.py) -- real <a href> citations, not a
    loaded asset. What must still never happen is a RESOURCE (a script, a
    stylesheet, an image) fetched from anywhere but fonts.googleapis.com, or
    a CSS url() reaching the network.
    """
    html = _html()
    assert not re.search(r"<img[^>]+src=[\"']https?://", html, re.IGNORECASE)
    assert not re.search(r"url\([\"']?https?://", html, re.IGNORECASE)
    resources = re.findall(
        r"<(?:script|link)\b[^>]*\s(?:src|href)=[\"'](https?://[^\"']+)", html, re.IGNORECASE
    )
    assert all(url.startswith("https://fonts.googleapis.com/") for url in resources), resources


# --- the layout and the page agree -------------------------------------------


def test_every_series_the_layout_names_exists_in_the_union_universe():
    """The exporter refuses to publish a layout naming a series neither
    national.json nor aggregates.json's country row carries; this asserts the
    committed layout is one it would accept, so the refusal is never the
    thing a reader discovers."""
    codes = _universe()
    if codes is None:
        pytest.skip("site payloads not built")
    layout = _layout()
    comparison = layout.get("comparison") or {}
    map_spec = layout.get("map") or {}
    history = layout.get("history") or {}
    named = set(layout["kpis"]) | set(layout["key_list"])
    named |= set((layout.get("tiles") or {}).get("indicators") or [])
    named |= set(comparison.get("indicators") or [])
    named |= set(map_spec.get("indicators") or [])
    if history.get("series"):
        named.add(history["series"])
    assert named <= codes, f"layout names series the payloads lack: {sorted(named - codes)}"


def test_the_page_renders_a_slot_for_every_unavailable_section():
    html = _html()
    for entry in _layout()["unavailable"]:
        assert f'data-slot="{entry["id"]}"' in html, entry["id"]


def test_every_label_in_the_layout_is_trilingual():
    """CLAUDE.md rule 7, on the strings this file owns."""
    layout = _layout()
    blocks = [layout["kpis_note"]]
    blocks += [layout["comparison"]["label"], layout["comparison"]["note"]]
    blocks += [layout["tiles"]["label"]]
    blocks += [layout["map"]["label"]]
    blocks += [layout["history"]["label"], layout["history"]["note"]]
    for entry in layout["unavailable"]:
        blocks += [entry["label"], entry["reason"]]
    for block in blocks:
        assert set(block) == set(LANGS), block
        assert all(str(block[lang]).strip() for lang in LANGS), block


def test_every_sidebar_anchor_points_at_a_section_on_this_page():
    html = _html()
    ids = set(re.findall(r'\sid="([^"]+)"', html))
    anchors = set(re.findall(r'href="#([^"]+)"', html))
    assert anchors, "the sidebar has no in-page anchors"
    assert anchors <= ids, f"anchors with no section: {sorted(anchors - ids)}"


def test_a_slot_exists_for_every_configured_unavailable_id_and_no_orphans():
    """Every data-slot in the markup must be one the layout actually
    describes, and vice versa -- a slot with no matching config entry would
    render with no title and no reason (renderUnavailable() falls back to a
    bare "Not available yet"), which is a silent regression waiting to
    happen, not a designed state."""
    html = _html()
    slots = set(re.findall(r'data-slot="([^"]+)"', html))
    configured = {entry["id"] for entry in _layout()["unavailable"]}
    assert slots == configured, (slots, configured)


# --- translation --------------------------------------------------------------


def test_every_key_the_page_asks_for_exists_in_all_three_languages():
    html = _html()
    used = set(re.findall(r'data-t(?:-[a-z]+)?="([A-Za-z0-9_]+)"', html))
    used |= set(re.findall(r"T\('([A-Za-z0-9_]+)'", html))
    used.discard("status_")
    used |= {f"status_{s}" for s in ("provisional", "estimate", "revised")}
    assert used, "micro.html marks nothing for translation"
    strings = _strings()
    for lang in LANGS:
        missing = sorted(key for key in used if key not in strings[lang])
        assert not missing, f"{lang} is missing {missing}"


def test_the_english_stays_in_the_markup():
    """A reader whose JavaScript never runs still gets a page, not a grid of
    empty boxes -- the same rule communes.html and map.html are held to."""
    html = _html()
    assert re.search(r'data-t="microTitle">[^<]{6,}<', html)
    assert re.search(r'data-t="microLead">[^<]{20,}<', html)


# --- routing ------------------------------------------------------------------


def test_micro_is_registered_noindex_and_absent_from_every_sitemap():
    assert '<meta name="robots" content="noindex">' in _html()
    assert "/micro.html" in root_routes()
    assert "/micro.html" in ROUTE_EXACT
    assert not is_indexable("/micro.html")
    assert "/micro.html" not in sitemap_routes()


def test_micro_links_and_scripts_all_resolve():
    html = _html()
    targets = re.findall(r"(?:href|src)=[\"']([^\"'#]+)[\"']", html)
    broken = [
        t
        for t in targets
        if not t.startswith(("http://", "https://", "mailto:")) and not (REPO / t).is_file()
    ]
    assert not broken, f"micro.html references files that do not exist: {broken}"
