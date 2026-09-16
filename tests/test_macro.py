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
    for item in layout.get("extra_lists") or []:
        named |= set(item["series"])
    for item in layout.get("panel_charts") or []:
        named |= set(item["series"])
    assert named <= published, f"layout names series the payload lacks: {sorted(named - published)}"


# --- BATCH A1.4b: history charts inside Prix/Emploi/Conjoncture ---------------


def test_every_panel_chart_series_exists_in_the_national_payload():
    """Same guarantee as extra_lists, checked on its own: a panel chart
    naming a series nothing provides would render as an empty canvas, not
    caught by the combined check above if that one were ever loosened."""
    national = REPO / "public" / "data" / "national.json"
    if not national.exists():
        pytest.skip("site payloads not built")
    published = set(json.loads(national.read_text(encoding="utf-8"))["indicators"])
    layout = _layout()
    charts = layout.get("panel_charts") or []
    assert charts, "no panel_charts declared"
    for item in charts:
        assert item["series"], f"panel chart {item['id']!r} names no series"
        missing = set(item["series"]) - published
        assert (
            not missing
        ), f"panel chart {item['id']!r} names series the payload lacks: {sorted(missing)}"


def test_panel_chart_labels_are_trilingual():
    for item in _layout().get("panel_charts") or []:
        assert set(item["label"]) == set(LANGS), item
        assert all(str(item["label"][lang]).strip() for lang in LANGS), item


def test_every_panel_chart_id_is_used_by_exactly_one_panel_card():
    layout = _layout()
    chart_ids = {item["id"] for item in layout.get("panel_charts") or []}
    card_ids: set[str] = set()
    for panel in layout["panels"]:
        card_ids |= set(panel["cards"])
    missing = chart_ids - card_ids
    assert not missing, f"panel_charts with no panel card: {missing}"


def test_every_panel_chart_has_a_canvas_in_its_own_article_in_macro_html():
    """Each `panel_charts[].id` must both exist as its own `<article>` and
    carry a `<canvas>` -- the chart could not otherwise draw at all."""
    html = _html()
    for item in _layout().get("panel_charts") or []:
        assert f'id="{item["id"]}"' in html, f"no element for panel chart {item['id']!r}"
        m = re.search(
            r'<article\b[^>]*id="' + re.escape(item["id"]) + r'"[^>]*>([\s\S]*?)</article>', html
        )
        assert m, f"no <article id={item['id']!r}> in macro.html"
        assert "<canvas" in m.group(1), f"panel chart {item['id']!r} has no <canvas>"


def test_panel_charts_sit_above_their_matching_list_card():
    """The brief: the history chart goes ABOVE the existing list, the list
    stays -- checked by markup order inside each of the three panels this
    batch touches, not just presence of both."""
    html = _html()
    pairs = {
        "prices-chart": "prices",
        "employment-chart": "employment",
        "business-cycle-chart": "business-cycle",
    }
    for chart_id, list_id in pairs.items():
        chart_pos = html.index(f'id="{chart_id}"')
        list_pos = html.index(f'id="{list_id}"')
        assert chart_pos < list_pos, f"{chart_id} does not sit above {list_id}"


# --- BATCH A1.4: the seven selectable panels ----------------------------------

#: The anchors macro.html exposed before this batch -- every one of them must
#: still resolve, via some panel's `legacy_anchors` (CLAUDE.md rule 31).
OLD_ANCHORS = {
    "overview",
    "growth",
    "key",
    "drivers",
    "international",
    "public-finance",
    "map",
    "news",
}


_SECTION_TAG = re.compile(r"<section\b|</section>")


def _panel_sections(html: str) -> dict[str, str]:
    """Maps each `panels[].id` to the HTML of its `[data-panel]` section, so
    a test can check containment without a real DOM parser. Finds the
    MATCHING `</section>` with a real depth count over every `<section`/
    `</section>` in between -- a panel nests a `<section class="row ...">`
    (sometimes more than one, as siblings), so the first `</section>` found
    after the panel opens is not reliably its own."""
    sections: dict[str, str] = {}
    for m in re.finditer(r'<section class="bp-panel" id="([^"]+)"[^>]*>', html):
        start = m.end()
        depth = 1
        end = None
        for tm in _SECTION_TAG.finditer(html, start):
            if tm.group() == "</section>":
                depth -= 1
                if depth == 0:
                    end = tm.start()
                    break
            else:
                depth += 1
        assert end is not None, f"unclosed <section> for panel {m.group(1)!r}"
        sections[m.group(1)] = html[start:end]
    return sections


def test_every_panel_has_a_data_panel_section_in_macro_html():
    html = _html()
    sections = _panel_sections(html)
    ids = [p["id"] for p in _layout()["panels"]]
    assert ids, "no panels declared"
    missing = [pid for pid in ids if pid not in sections]
    assert not missing, f"panels with no <section data-panel> in macro.html: {missing}"


def test_every_card_the_layout_lists_exists_inside_its_own_panel():
    html = _html()
    sections = _panel_sections(html)
    for panel in _layout()["panels"]:
        for card_id in panel["cards"]:
            assert (
                f'id="{card_id}"' in sections[panel["id"]]
            ), f"card {card_id!r} is not inside panel {panel['id']!r}"


def test_no_article_sits_outside_a_panel():
    """Every card the page draws belongs to exactly one panel -- a card left
    outside every `[data-panel]` section would always be visible, defeating
    "exactly one panel is visible"."""
    html = _html()
    sections = _panel_sections(html)
    inside = "".join(sections.values())
    total_articles = len(re.findall(r"<article\b", html))
    inside_articles = len(re.findall(r"<article\b", inside))
    assert (
        total_articles == inside_articles
    ), f"{total_articles - inside_articles} <article> element(s) sit outside every panel"


def test_every_old_anchor_is_covered_by_some_panels_legacy_anchors():
    layout = _layout()
    covered: set[str] = set()
    for panel in layout["panels"]:
        covered |= set(panel.get("legacy_anchors") or [])
    missing = OLD_ANCHORS - covered
    assert not missing, f"anchors from before this batch resolve nowhere: {sorted(missing)}"


def test_the_sidebar_anchors_are_exactly_the_panel_ids():
    html = _html()
    nav_html = re.search(r'<ul class="bp-sidebar-nav"[^>]*>([\s\S]*?)</ul>', html).group(1)
    anchors = set(re.findall(r'href="#([^"]+)"', nav_html))
    ids = {p["id"] for p in _layout()["panels"]}
    assert anchors == ids, (anchors, ids)


def test_panel_labels_are_trilingual():
    for panel in _layout()["panels"]:
        assert set(panel["label"]) == set(LANGS), panel
        assert all(str(panel["label"][lang]).strip() for lang in LANGS), panel


def test_panel_empty_reasons_are_trilingual_where_present():
    for panel in _layout()["panels"]:
        reason = panel.get("empty_reason")
        if reason is None:
            continue
        assert set(reason) == set(LANGS), panel
        assert all(str(reason[lang]).strip() for lang in LANGS), panel


def test_the_europe_panel_declares_no_empty_reason():
    """Superseded by the Europe countries batch (docs/features/
    europe_countries.md, 2026-09-15): the europe panel used to declare an
    empty_reason about the foreign-GDP comparison card being unbuilt (Batch
    B3). That card is now built for real (the country map, its picker, and
    the "Comparaison internationale" small multiples,
    assets/belpulse/europe_map.js), so the panel has nothing left to
    apologise for -- config/national_sections.yaml no longer carries the
    key, and macro.html's own renderPanelChrome() already treats a missing
    empty_reason as "no note" (hides #europeNote)."""
    panels = {p["id"]: p for p in _layout()["panels"]}
    assert "empty_reason" not in panels["europe"], panels["europe"]


def test_extra_list_labels_are_trilingual():
    for item in _layout().get("extra_lists") or []:
        assert set(item["label"]) == set(LANGS), item
        assert all(str(item["label"][lang]).strip() for lang in LANGS), item


def test_every_extra_list_id_is_used_by_exactly_one_panel_card():
    layout = _layout()
    extra_ids = {item["id"] for item in layout.get("extra_lists") or []}
    card_ids: set[str] = set()
    for panel in layout["panels"]:
        card_ids |= set(panel["cards"])
    missing = extra_ids - card_ids
    assert not missing, f"extra_lists with no panel card: {missing}"


def test_panels_js_is_loaded_and_carries_no_indicator_id():
    html = _html()
    assert 'src="assets/belpulse/panels.js"' in html
    panels_js = (REPO / "assets" / "belpulse" / "panels.js").read_text(encoding="utf-8")
    national = REPO / "public" / "data" / "national.json"
    if not national.exists():
        pytest.skip("site payloads not built")
    codes = set(json.loads(national.read_text(encoding="utf-8"))["indicators"])
    named = sorted(code for code in codes if code in panels_js)
    assert not named, f"panels.js names indicators directly: {named}"


def test_the_page_renders_a_slot_for_every_unavailable_section():
    """A card the layout describes and the page has no slot for would be a
    reason nobody ever reads."""
    html = _html()
    for entry in _layout()["unavailable"]:
        assert f'data-slot="{entry["id"]}"' in html, entry["id"]


def test_the_two_named_unavailable_cards_are_compact_not_a_designed_size_box():
    """Batch A2b, item 7: 'Carte économique' and 'Actualités économiques'
    used to hold open a large hatched placeholder at the design's drawn size
    (see the `unavailable:` block's own comment in
    config/national_sections.yaml: "built at their designed size") -- right
    beside #key, a real populated card, so a third of the row read as empty
    hatching. That placeholder is macro.html's own markup/CSS (.placeholder,
    defined in this file), not something the config controls: the config
    only ever supplied the label/reason/link text, unchanged here (see
    test_the_page_renders_a_slot_for_every_unavailable_section and
    test_every_label_in_the_layout_is_trilingual, both still passing).
    #public-finance is out of item 7's scope and keeps its box -- it is its
    own single-card panel, not a row beside a taller populated card."""
    html = _html()
    map_card = re.search(r'<article[^>]+id="map"[^>]*>.*?</article>', html, re.DOTALL)
    news_card = re.search(r'<article[^>]+id="news"[^>]*>.*?</article>', html, re.DOTALL)
    finance_card = re.search(
        r'<article[^>]+id="public-finance"[^>]*>.*?</article>', html, re.DOTALL
    )
    assert map_card and news_card and finance_card
    assert 'class="placeholder"' not in map_card.group(0)
    assert 'class="placeholder"' not in news_card.group(0)
    assert 'class="placeholder"' in finance_card.group(
        0
    ), "public-finance is out of scope for item 7 and must keep its designed-size box"
    assert "card-compact-empty" in map_card.group(0)
    assert "card-compact-empty" in news_card.group(0)
    assert "card-compact-empty" not in finance_card.group(0)
    # The label/reason/link text itself is untouched -- still config-driven.
    assert 'data-slot="economic_map"' in map_card.group(0)
    assert 'data-slot="news"' in news_card.group(0)
    assert "slot-link" in map_card.group(0)


def test_the_compact_empty_cards_do_not_stretch_to_match_a_taller_sibling():
    """Removing the placeholder alone would still leave a tall, mostly blank
    card: CSS grid stretches row items to the row's tallest member (#key,
    the real 'Indicateurs clés' list) by default."""
    rule = re.search(r"\.card-compact-empty\{([^}]+)\}", _html())
    assert rule, "no .card-compact-empty rule"
    assert "align-self:start" in rule.group(1).replace(" ", "")


def test_the_desktop_rail_background_fills_the_whole_column():
    """Batch A2b, item 8: the fixed sidebar (position:fixed, pinned since
    #201) always covers the live viewport correctly as a reader scrolls, but
    a `position:fixed` box does not stretch to match a full-page capture
    rendered at the whole document's height in one shot -- the navy stopped
    wherever the sidebar's own content happened to end, with plain page
    background for the rest of the column. A plain absolutely-positioned
    fill, behind the fixed nav and sized to the column's real content height,
    closes that gap. Desktop-only, inside the same >=1025px block #201 added
    -- tablet/phone layouts (the media queries below it) are untouched."""
    html = _html()
    block = re.search(r"@media \(min-width:1025px\)\{(.*?)\n  \}\n", html, re.DOTALL)
    assert block, "no @media (min-width:1025px) block"
    body = block.group(1)
    assert ".bp-body--analytical{" in body and "position:relative" in body
    assert ".bp-body--analytical::before{" in body
    fill = re.search(r"\.bp-body--analytical::before\{([^}]+)\}", body)
    assert fill, "no .bp-body--analytical::before rule"
    fill_body = fill.group(1).replace(" ", "")
    assert "position:absolute" in fill_body
    assert "background:var(--bp-navy-surface)" in fill_body
    # Still inside the >=1025px block, alongside the fixed sidebar it backs.
    assert ".bp-sidebar{" in body and "position:fixed" in body


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


def test_macro_is_registered_indexable_and_in_the_sitemap():
    assert '<meta name="robots" content="noindex">' not in _html()
    assert "/macro.html" in root_routes()
    assert "/macro.html" in ROUTE_EXACT
    assert is_indexable("/macro.html")
    assert "/macro.html" in sitemap_routes()


def test_macro_links_and_scripts_all_resolve():
    html = _html()
    targets = re.findall(r"(?:href|src)=[\"']([^\"'#]+)[\"']", html)
    broken = [
        t
        for t in targets
        if not t.startswith(("http://", "https://", "mailto:")) and not (REPO / t).is_file()
    ]
    assert not broken, f"macro.html references files that do not exist: {broken}"
