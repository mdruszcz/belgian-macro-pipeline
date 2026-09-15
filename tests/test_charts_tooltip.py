"""Browser contract for BPCharts' interactive layer on home2.html's hero
(docs/features/site_unification.md, Batch A1.3).

Served over real HTTP (not file://) the same way tests/site/test_iframe_contract.py
serves the repo, since the page's own `fetch()` calls for national.json etc.
need a real origin. Runs against Chromium via tests/conftest.py's shared
`chromium` fixture; a machine without Playwright/Chromium skips the whole
module cleanly through that fixture's own skip-guard.
"""

from __future__ import annotations

import functools
import http.server
import json
import re
import socketserver
import threading
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def site():
    """The repository served over HTTP -- file:// would still work for this
    page (no cross-frame postMessage here), but a real origin is what
    production actually is, and costs nothing extra."""
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO))

    class Quiet(socketserver.TCPServer):
        allow_reuse_address = True

        def log_message(self, *args):  # pragma: no cover - silence the server
            pass

        def handle_error(self, *args):  # pragma: no cover - a closed socket is not a failure
            pass

    server = Quiet(("127.0.0.1", 0), handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _hero_codes() -> list[str]:
    layout = yaml.safe_load(
        (REPO / "config" / "national_sections.yaml").read_text(encoding="utf-8")
    )
    return layout.get("hero") or []


def _national() -> dict:
    return json.loads((REPO / "public" / "data" / "national.json").read_text(encoding="utf-8"))[
        "indicators"
    ]


def _open_home2(chromium, site, width, height, lang="fr"):
    """A fresh context+page at the given viewport, light theme (home2's own
    pre-paint script always forces light), and the given language via the
    same localStorage key the language picker itself writes."""
    context = chromium.new_context(viewport={"width": width, "height": height})
    context.add_init_script(f"try{{localStorage.setItem('belpulse-lang', '{lang}');}}catch(e){{}}")
    page = context.new_page()
    page.goto(f"{site}/home2.html")
    # Wait on a real condition, never a duration: the hero mini-charts exist
    # once init()'s fetches resolve and renderHeroMini() has run.
    page.wait_for_function(
        "document.querySelectorAll('#heroMini .glass canvas').length >= 1", timeout=15000
    )
    page.wait_for_function(
        "(() => { const c = document.querySelector('#heroMini .glass canvas');"
        " return c && c.getBoundingClientRect().width > 10; })()",
        timeout=15000,
    )
    return context, page


@pytest.fixture
def desktop(chromium, site):
    context, page = _open_home2(chromium, site, 1122, 900)
    try:
        yield page
    finally:
        context.close()


@pytest.fixture
def mobile(chromium, site):
    context, page = _open_home2(chromium, site, 390, 844)
    try:
        yield page
    finally:
        context.close()


def test_hovering_the_last_point_shows_its_real_value_and_period(desktop):
    hero = _hero_codes()
    assert len(hero) >= 1
    national = _national()

    cards = desktop.locator("#heroMini .glass")
    count = cards.count()
    assert count == len(hero[:2]) or count >= 1

    for i in range(count):
        code = hero[i]
        entry = national[code]
        periods = sorted(entry["periods"].keys())
        # The chart draws at most the last 24 periods (seriesPoints' limit in
        # home2.html) -- the LAST one drawn is still the series' real last
        # period regardless of that window.
        last_period = periods[-1]
        last_value = entry["periods"][last_period]["value"]
        assert last_value is not None, f"fixture assumption broken for {code}"

        canvas = cards.nth(i).locator("canvas")
        box = canvas.bounding_box()
        assert box, f"canvas {i} has no layout box"
        desktop.mouse.move(box["x"] + box["width"] - 3, box["y"] + box["height"] / 2)

        desktop.wait_for_function(
            "document.querySelector('.bp-chart-tip') && "
            "!document.querySelector('.bp-chart-tip').hidden",
            timeout=5000,
        )
        tip_text = desktop.locator(".bp-chart-tip").inner_text()
        assert last_period in tip_text, f"tooltip for {code} missing period: {tip_text!r}"

        # Independently formatted in the SAME runtime/locale the tooltip uses
        # (fr), from the raw payload -- not by calling back into charts.js.
        expected_value_text = desktop.evaluate(
            "(v) => v.toLocaleString('fr', {maximumFractionDigits: 1})", last_value
        )
        assert (
            expected_value_text in tip_text
        ), f"tooltip for {code} missing value {expected_value_text!r}: {tip_text!r}"


def test_hero_cards_are_compact_no_data_table_no_see_more(desktop):
    """The hero cards follow the compact design: title, latest value, plot,
    source -- no "see data" table and no "see more" link."""
    cards = desktop.locator("#heroMini .glass")
    assert cards.count() >= 1
    for i in range(cards.count()):
        card = cards.nth(i)
        assert card.locator("details.bp-chart-data").count() == 0
        assert card.locator("a").count() == 0
        assert card.locator(".mini-head .mini-reading b").inner_text().strip()


def test_hero_tooltip_does_not_repeat_the_indicator_name(desktop):
    national = _national()
    cards = desktop.locator("#heroMini .glass")
    for i in range(cards.count()):
        code = _hero_codes()[i]
        title = cards.nth(i).locator("h3").inner_text().strip()
        assert title
        box = cards.nth(i).locator("canvas").bounding_box()
        assert box
        desktop.mouse.move(box["x"] + box["width"] - 3, box["y"] + box["height"] / 2)
        desktop.wait_for_function(
            "document.querySelector('.bp-chart-tip') && "
            "!document.querySelector('.bp-chart-tip').hidden",
            timeout=5000,
        )
        tip_text = desktop.locator(".bp-chart-tip").inner_text()
        assert title not in tip_text, f"tooltip for {code} still names the series: {tip_text!r}"
        assert sorted(national[code]["periods"].keys())[-1] in tip_text


def test_canvas_is_focusable_and_arrow_left_changes_the_announced_point(desktop):
    canvas = desktop.locator("#heroMini .glass").first.locator("canvas")
    assert canvas.get_attribute("tabindex") == "0"

    live = desktop.locator("#heroMini .glass").first.locator(".bp-sr-only")
    assert live.count() == 1
    before = live.inner_text()

    canvas.focus()
    desktop.keyboard.press("ArrowLeft")
    desktop.wait_for_function(
        "document.querySelectorAll('#heroMini .glass')[0].querySelector('.bp-sr-only').textContent.length > 0",
        timeout=5000,
    )
    after = live.inner_text()
    assert after != before
    assert re.search(r"\d{4}", after), f"announced text has no period in it: {after!r}"


def test_zoom_controls_do_not_intersect_the_indicator_select(desktop, mobile):
    for page in (desktop, mobile):
        select_box = page.locator("#mapIndicator").bounding_box()
        zoom_box = page.locator(".zoombtns").bounding_box()
        assert select_box and zoom_box
        overlap_x = select_box["x"] < zoom_box["x"] + zoom_box["width"] and zoom_box["x"] < (
            select_box["x"] + select_box["width"]
        )
        overlap_y = select_box["y"] < zoom_box["y"] + zoom_box["height"] and zoom_box["y"] < (
            select_box["y"] + select_box["height"]
        )
        assert not (
            overlap_x and overlap_y
        ), f"zoom controls overlap the indicator select at {page.viewport_size}"


def test_at_390px_the_map_card_and_the_chart_cards_do_not_overlap(mobile):
    map_box = mobile.locator("#mapCard").bounding_box()
    mini_box = mobile.locator("#heroMini").bounding_box()
    assert map_box and mini_box
    # Stacked: the mini-chart cluster starts at or below where the map card
    # ends (a hairline of sub-pixel rounding is fine; real overlap is not).
    assert (
        mini_box["y"] >= map_box["y"] + map_box["height"] - 1
    ), f"map card {map_box} overlaps the chart cards {mini_box} at 390px"
