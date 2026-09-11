"""Static contract for macro.html — the Batch 6 macroeconomics page.

The page is a preview: noindexed, registered, and absent from every sitemap.
What these tests actually protect is the property the batch exists to prove --
that a national page can be built with NO indicator id and NO figure in its
markup, so adding a series is a config change (docs/steps' 50% gate: "zero
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
PAGE = REPO / "macro.html"
LAYOUT = REPO / "config" / "national_sections.yaml"
I18N = REPO / "assets" / "i18n.js"
LANGS = ("en", "fr", "nl")

#: Figures the supplied design prints. None of them is in this pipeline, so
#: none of them may appear on the page -- a mockup's number rendered as if it
#: were data is the one failure this page could not recover from.
DESIGN_FIGURES = (
    "+1,2",
    "11,7",
    "71,4",
    "2,8 %",
    "-4,4",
    "106,2",
    "590,3",
    "50 400",
    "25 482 317 901",
    "28 908 445 772",
    "3 426 127 871",
)


def _html() -> str:
    return PAGE.read_text(encoding="utf-8")


def _layout() -> dict:
    return yaml.safe_load(LAYOUT.read_text(encoding="utf-8"))


def _strings() -> dict:
    """The three string tables, read out of assets/i18n.js by Node-free
    parsing -- the same approach tests/test_i18n.py uses."""
    text = I18N.read_text(encoding="utf-8")
    tables = {}
    for lang in LANGS:
        start = text.index(f"\n  {lang}: {{")
        end = text.index("\n  },", start)
        tables[lang] = set(re.findall(r"\n    ([A-Za-z0-9_]+):", text[start:end]))
    return tables


# --- the page holds no data of its own ---------------------------------------


def test_macro_names_no_indicator_anywhere():
    """The whole point of config/national_sections.yaml. Checked against the
    ids the payloads actually publish, so it cannot be satisfied by renaming a
    constant and it grows as indicators are added."""
    national = REPO / "public" / "data" / "national.json"
    if not national.exists():
        pytest.skip("site payloads not built")
    codes = set(json.loads(national.read_text(encoding="utf-8"))["indicators"])
    index = REPO / "public" / "data" / "metadata" / "indicators.json"
    if index.exists():
        codes |= {
            row["indicator_code"]
            for row in json.loads(index.read_text(encoding="utf-8"))["indicators"]
        }
    assert codes, "no indicator ids to check against, so this test would prove nothing"
    named = sorted(code for code in codes if code in _html())
    assert not named, f"macro.html names indicators directly: {named}"


def test_macro_prints_none_of_the_designs_invented_figures():
    """The mockup's six KPIs and its three live counters are illustrations.
    Four of the six and all three counters have no series in this pipeline;
    typing them in would publish an invention (CLAUDE.md rule 36)."""
    html = _html()
    found = [figure for figure in DESIGN_FIGURES if figure in html]
    assert not found, f"macro.html contains design mockup figures: {found}"


def test_macro_simulates_nothing():
    """macro.md's own note: the public-finance counters are the one component
    in all four designs that is not a real data binding. This page does not
    build them at all -- it says why."""
    html = _html().lower()
    for forbidden in ("data-simulated", "setinterval(", "settimeout(function tick"):
        assert forbidden not in html, f"macro.html looks like it animates a counter: {forbidden}"
    # requestAnimationFrame is allowed exactly once, and only as a ONE-SHOT
    # redraw after layout (the history chart is sized by the row it lands in).
    # A second call is how a one-shot becomes a ticking loop, so the count is
    # the test.
    assert html.count("requestanimationframe(") <= 1, "more than one rAF: is something ticking?"


def test_macro_loads_no_third_party_asset():
    html = _html()
    assert not re.search(r"<img[^>]+src=[\"']https?://", html, re.IGNORECASE)
    assert not re.search(r"url\([\"']?https?://", html, re.IGNORECASE)
    # The shared font stylesheet is the one exception, as on every other page.
    external = re.findall(r"(?:href|src)=[\"'](https?://[^\"']+)", html)
    assert all(url.startswith("https://fonts.googleapis.com/") for url in external), external


# --- the layout and the page agree -------------------------------------------


def test_every_series_the_layout_names_exists_in_the_national_payload():
    """The exporter refuses to publish a layout naming a series nothing
    provides; this asserts the committed layout is one it would accept, so
    the refusal is never the thing a reader discovers."""
    national = REPO / "public" / "data" / "national.json"
    if not national.exists():
        pytest.skip("site payloads not built")
    published = set(json.loads(national.read_text(encoding="utf-8"))["indicators"])
    layout = _layout()
    named = set(layout["kpis"]) | set(layout["key_list"]) | set(layout["contributions"]["parts"])
    named.add(layout["history"]["series"])
    named.add(layout["contributions"]["whole"])
    assert named <= published, f"layout names series the payload lacks: {sorted(named - published)}"


def test_the_page_renders_a_slot_for_every_unavailable_section():
    """A card the layout describes and the page has no slot for would be a
    reason nobody ever reads."""
    html = _html()
    for entry in _layout()["unavailable"]:
        assert f'data-slot="{entry["id"]}"' in html, entry["id"]


def test_every_label_in_the_layout_is_trilingual():
    """CLAUDE.md rule 7, on the strings this file owns."""
    layout = _layout()
    blocks = [layout["kpis_note"], layout["history"]["label"], layout["history"]["note"]]
    blocks += [layout["contributions"]["label"], layout["contributions"]["note"]]
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


# --- translation --------------------------------------------------------------


def test_every_key_the_page_asks_for_exists_in_all_three_languages():
    html = _html()
    used = set(re.findall(r'data-t(?:-[a-z]+)?="([A-Za-z0-9_]+)"', html))
    used |= set(re.findall(r"T\('([A-Za-z0-9_]+)'", html))
    # The page builds a status key at runtime (`'status_' + latest.status`),
    # which no regex over the source can enumerate -- so the statuses the
    # schema allows are listed here instead. A source that starts marking a
    # national figure provisional must not print `status_provisional` raw.
    used.discard("status_")
    # `suppressed` is not in this set: a suppressed cell carries no number, and
    # the page only ever labels the latest cell that HAS one -- so it renders
    # as absent, which is the honest state, not as an unlabelled status key.
    used |= {f"status_{s}" for s in ("provisional", "estimate", "revised")}
    assert used, "macro.html marks nothing for translation"
    strings = _strings()
    for lang in LANGS:
        missing = sorted(key for key in used if key not in strings[lang])
        assert not missing, f"{lang} is missing {missing}"


def test_the_english_stays_in_the_markup():
    """A reader whose JavaScript never runs still gets a page, not a grid of
    empty boxes -- the same rule communes.html and map.html are held to."""
    html = _html()
    assert re.search(r'data-t="macroTitle">[^<]{6,}<', html)
    assert re.search(r'data-t="macroLead">[^<]{20,}<', html)


# --- routing ------------------------------------------------------------------


def test_macro_is_registered_noindex_and_absent_from_every_sitemap():
    assert '<meta name="robots" content="noindex">' in _html()
    assert "/macro.html" in root_routes()
    assert "/macro.html" in ROUTE_EXACT
    assert not is_indexable("/macro.html")
    assert "/macro.html" not in sitemap_routes()


def test_macro_links_and_scripts_all_resolve():
    html = _html()
    targets = re.findall(r"(?:href|src)=[\"']([^\"'#]+)[\"']", html)
    broken = [
        t
        for t in targets
        if not t.startswith(("http://", "https://", "mailto:")) and not (REPO / t).is_file()
    ]
    assert not broken, f"macro.html references files that do not exist: {broken}"
