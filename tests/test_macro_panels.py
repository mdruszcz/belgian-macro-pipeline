"""Browser contract for macro.html's panel switcher -- Batch A1.4
(docs/features/site_unification.md, "Macro/Micro : panneaux thématiques").

Served over real HTTP (not file://) for the same reason
tests/site/test_iframe_contract.py is: `history.pushState`/`replaceState`
and `location.hash` behave the same either way here, but a relative fetch of
public/data/*.json -- which this page needs for its layout and its
localStorage-backed "last panel" behaviour -- is friendlier to reason about
against a real origin, and it matches every other browser test in this repo.
"""

from __future__ import annotations

import functools
import http.server
import socketserver
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

DESKTOP_VIEWPORT = {"width": 1280, "height": 900}
PHONE_VIEWPORT = {"width": 390, "height": 844}

#: Every panel this batch adds, in sidebar order.
PANEL_IDS = [
    "apercu",
    "croissance",
    "prix",
    "emploi",
    "conjoncture",
    "finances-publiques",
    "europe",
]


@pytest.fixture(scope="module")
def site():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO_ROOT))

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


@pytest.fixture(scope="module")
def browser(chromium):
    """The session's Chromium (tests/conftest.py); each test opens its own
    context so nothing but the process is shared between tests."""
    return chromium


def test_a_legacy_anchor_resolves_to_its_panel_and_rewrites_the_url(browser, site):
    """#growth was the GDP-history section's own anchor before this batch;
    it is now inside the "croissance" panel, and CLAUDE.md rule 31 says the
    old link must still work."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#growth", wait_until="load")
        page.wait_for_selector("#croissance:not([hidden])")
        assert page.evaluate("location.hash") == "#croissance"
        assert not page.is_visible("#apercu")
    finally:
        context.close()


def test_clicking_a_panel_then_going_back_shows_the_previous_one(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click('.bp-sidebar-nav a[href="#prix"]')
        page.wait_for_selector("#prix:not([hidden])")
        page.go_back()
        page.wait_for_selector("#apercu:not([hidden])")
    finally:
        context.close()


def test_a_shown_panels_first_canvas_has_real_width(browser, site):
    """The regression this whole switching mechanism exists to avoid: a
    canvas measured while its panel is `hidden` is 0px wide -- or, worse,
    PERMANENTLY pinned to assets/belpulse/charts.js's fallback (300px):
    that fallback gets written back as the canvas's own inline style, so a
    later, correctly-timed redraw still measures the stale value rather
    than the container. `width > 0` alone would not have caught 300px
    sitting inside a ~500px card, which is exactly what the lead's review
    of PR #170's screenshots found -- so this also checks the canvas fills
    (at least 90% of) the wrapper that gives it its size."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click('.bp-sidebar-nav a[href="#croissance"]')
        page.wait_for_selector("#croissance:not([hidden])")
        page.wait_for_function("document.getElementById('historyCanvas').width > 0")
        # The onShow redraw is deferred one animation frame (panels.js) on
        # top of that -- wait for the canvas's OWN rendered width to reach
        # something close to its wrapper's, not just for the first non-zero
        # value, which the stale-300px bug would also produce immediately.
        page.wait_for_function(
            "(function(){"
            "var c = document.getElementById('historyCanvas');"
            "var wrap = c.parentElement;"
            "return c.getBoundingClientRect().width >= wrap.clientWidth * 0.9;"
            "})()"
        )
        canvas_width = page.evaluate(
            "document.getElementById('historyCanvas').getBoundingClientRect().width"
        )
        wrap_width = page.evaluate("document.getElementById('historyCanvas').parentElement.clientWidth")
        assert canvas_width >= wrap_width * 0.9, (
            f"the GDP history canvas is {canvas_width}px wide inside a {wrap_width}px card "
            "-- looks like the stale-300px-fallback bug, not a real fit"
        )
    finally:
        context.close()


def test_no_growth_driver_label_is_truncated(browser, site):
    """The lead's review of PR #170's screenshots: "Dépenses de
    consommation des APU" was rendered as "Dépen...". A statistical label
    losing its ending is not the same label -- CLAUDE.md's own rule that a
    reader must never be shown a number (or a name) that isn't real.
    Checks BOTH the CSS (no `text-overflow: ellipsis` computed on the
    element) and the actual layout (nothing is wider than its own box),
    since either one alone could pass while the other still clips text."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click('.bp-sidebar-nav a[href="#croissance"]')
        page.wait_for_selector("#croissance:not([hidden])")
        page.wait_for_selector("#contribList li .name")
        names = page.locator("#contribList li .name")
        count = names.count()
        assert count > 0, "the growth-drivers list rendered no rows"
        for i in range(count):
            el = names.nth(i)
            overflow = el.evaluate("e => getComputedStyle(e).textOverflow")
            assert overflow != "ellipsis", f"row {i} ({el.text_content()!r}) is set to ellipsis"
            clipped = el.evaluate("e => e.scrollWidth > e.clientWidth + 1")
            assert not clipped, f"row {i} ({el.text_content()!r}) overflows its own box"
    finally:
        context.close()


def test_reload_keeps_the_panel_the_hash_names(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#prix", wait_until="load")
        page.wait_for_selector("#prix:not([hidden])")
        page.reload(wait_until="load")
        page.wait_for_selector("#prix:not([hidden])")
        assert page.evaluate("location.hash") == "#prix"
    finally:
        context.close()


def test_at_phone_width_the_select_replaces_the_sidebar(browser, site):
    context = browser.new_context(viewport=PHONE_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        assert page.is_visible("#panelPick")
        assert not page.is_visible(".bp-sidebar")
        page.select_option("#panelPick", "emploi")
        page.wait_for_selector("#emploi:not([hidden])")
        assert page.evaluate("location.hash") == "#emploi"
    finally:
        context.close()


@pytest.mark.parametrize("panel_id", PANEL_IDS)
def test_exactly_one_panel_is_visible_after_choosing_each_one(browser, site, panel_id):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click(f'.bp-sidebar-nav a[href="#{panel_id}"]')
        page.wait_for_selector(f"#{panel_id}:not([hidden])")
        visible = page.eval_on_selector_all(
            "[data-panel]", "els => els.filter(e => !e.hidden).map(e => e.id)"
        )
        assert visible == [panel_id], visible
        # The sidebar agrees with what is actually shown.
        current = page.eval_on_selector_all(
            '.bp-sidebar-nav a[aria-current="page"]', "els => els.map(e => e.getAttribute('href'))"
        )
        assert current == [f"#{panel_id}"], current
    finally:
        context.close()


def test_choosing_a_panel_moves_focus_to_its_heading(browser, site):
    """Keyboard/screen-reader users need to land somewhere after a panel
    switch, not stay wherever the sidebar link was."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        page.click('.bp-sidebar-nav a[href="#conjoncture"]')
        page.wait_for_selector("#conjoncture:not([hidden])")
        page.wait_for_function(
            "document.activeElement && document.activeElement.closest('#conjoncture') !== null"
        )
        focused_tag = page.evaluate("document.activeElement.tagName")
        assert focused_tag == "H2", focused_tag
    finally:
        context.close()


def test_loading_the_page_fresh_does_not_move_focus(browser, site):
    """`onShow` fires for the default panel too (macro.html's chart redraw
    needs it), but a first load must not steal focus from the address bar --
    only a user CHOICE does."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        focused_tag = page.evaluate("document.activeElement && document.activeElement.tagName")
        assert focused_tag != "H2", "the default panel's heading stole focus on load"
    finally:
        context.close()
