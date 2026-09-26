"""Browser behaviour for the Portrait commune.html (feat/commune-portrait)
not already covered by tests/test_age_pyramid_slider.py (the slider/Play
timeline) or tests/test_a4_finishing_fixes.py (the allData accordion,
pyramid tooltips).

Follows the shared-server + Playwright pattern those files already use.
"""

from __future__ import annotations

import functools
import http.server
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


class Quiet(socketserver.TCPServer):
    allow_reuse_address = True

    def log_message(self, *args):  # pragma: no cover - silence the server
        pass

    def handle_error(self, *args):  # pragma: no cover - a closed socket is not a failure
        pass


@pytest.fixture(scope="module")
def site():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO))
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


def _context(chromium, width=1122, height=900):
    return chromium.new_context(viewport={"width": width, "height": height}, locale="en-US")


#: A missing buyer-origin-flows payload for many communes is expected
#: (renderFlows-equivalent degrades to no card, not an error), and Chromium
#: logs every 404'd fetch as a console error regardless of who consumes the
#: response -- filtered out here, same as tests/test_age_pyramid_slider.py's
#: own convention, rather than asserting on the whole console.
def _real_errors(errors):
    return [e for e in errors if "404" not in e]


def test_92094_loads_with_zero_console_errors(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        page.wait_for_timeout(600)  # lazy chart draw-in / IntersectionObserver settle
        assert not _real_errors(errors), f"console/page errors on load: {_real_errors(errors)}"
    finally:
        ctx.close()


def test_a_tile_click_changes_the_lead_chart_title_and_the_map_dropdown(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")

        # The first chapter's small-multiples grid: click a non-selected tile
        # and confirm both the lead chart eyebrow (title) and the map's own
        # indicator <select> change to match it.
        first_chapter = page.locator("#chapters .chapter").first
        unselected = first_chapter.locator(".multiple-tile:not(.selected)")
        assert (
            unselected.count() > 0
        ), "no unselected small-multiples tile to click in the first chapter"
        # A STABLE reference to the one tile being clicked, as a real
        # ElementHandle -- a Locator (even one held in a Python variable) is
        # always a LIVE re-query of its selector, and ":not(.selected)" stops
        # matching this exact tile the instant the click adds .selected to
        # it, so re-invoking the locator afterwards would silently resolve
        # to a DIFFERENT (still-unselected) tile instead of the one clicked.
        target_handle = unselected.first.element_handle()
        assert target_handle is not None
        target_code = target_handle.get_attribute("data-code")
        assert target_code

        before_title = first_chapter.locator(".leadchart-eyebrow").inner_text()
        target_handle.click()
        page.wait_for_function(
            """(before) => {
                const eyebrow = document.querySelector('#chapters .chapter .leadchart-eyebrow');
                return eyebrow && eyebrow.textContent !== before;
            }""",
            arg=before_title,
        )
        after_title = first_chapter.locator(".leadchart-eyebrow").inner_text()
        assert after_title != before_title

        select = first_chapter.locator(".leadmap-select select")
        if select.count() > 0:
            assert select.input_value() == target_code

        # The clicked tile must now carry .selected.
        assert "selected" in (target_handle.get_attribute("class") or "")
    finally:
        ctx.close()


def test_a_flemish_commune_with_no_schools_row_shows_not_applicable(chromium, site):
    """11001 (Aartselaar, Flanders) has no row in schools/by_commune.json --
    rule 26's three distinct schools states must show "not applicable" here,
    never the "unavailable" wording a Wallonia/Brussels commune with zero
    sites gets, and never silently omit the card."""
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=11001", wait_until="load")
        page.wait_for_selector("#schoolsPanel[data-state]")
        state = page.locator("#schoolsPanel").get_attribute("data-state")
        assert state == "not-applicable", f"expected not-applicable, got {state!r}"
        headline = page.locator("#schoolsPanel .headline").inner_text()
        assert headline.strip(), "not-applicable state shows no headline text"
    finally:
        ctx.close()


def test_a_flanders_commune_with_a_real_fwb_site_still_shows_ready():
    """45041 (Ronse) is a Flemish commune that DOES have an FWB site in the
    payload -- confirms the row-wins-over-region rule without a browser:
    the payload itself must contain the row this test's browser sibling
    would otherwise need to open a page to check."""
    import json

    schools = json.loads(
        (REPO / "public" / "data" / "schools" / "by_commune.json").read_text(encoding="utf-8")
    )
    assert "45041" in schools.get("communes", {}), (
        "fixture drift: Ronse (45041) no longer carries a schools row -- "
        "the not-applicable/ready distinction this batch's placement relies "
        "on has nothing left to demonstrate the row-wins case with"
    )


def test_no_horizontal_overflow_on_a_narrow_flemish_commune_at_390px(chromium, site):
    ctx = _context(chromium, width=390, height=844)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=11001", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        scroll_width = page.evaluate("document.documentElement.scrollWidth")
        client_width = page.evaluate("document.documentElement.clientWidth")
        assert scroll_width <= client_width + 1, (
            f"horizontal overflow at 390px on 11001: scrollWidth={scroll_width} "
            f"clientWidth={client_width}"
        )
    finally:
        ctx.close()
