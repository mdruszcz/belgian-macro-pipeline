"""Three properties this file pins, the third added by the issue #312 batch
2 audit.

1. NO-JS REACHABILITY. The shared header/footer (src/pages/shell.py,
   assets/belpulse/layout.css, assets/belpulse/shell.js) is progressive
   enhancement: the raw HTML carries no `hidden` attribute anywhere, and the
   mobile nav / dropdown-panel behaviour only exists once shell.js has run
   and added `.bp-js` to `<html>`. Without it, the six nav links and the
   language switcher's links must be directly reachable -- exactly what
   develop's always-visible switcher and wrapping nav already offered before
   this batch, and what render_header()'s own docstring claims.

2. TOUCH TARGETS. `.bp-nav-toggle` and `.bp-menu-btn` are the controls a
   phone reader presses to reach everything else in the header; each must be
   at least 44x44 CSS pixels (WCAG 2.5.5 / the iOS and Android platform
   minimums), not just visually present.

3. OCCLUSION (added after the issue #312 batch 2 audit). The first CLS fix
   in that batch made the theme panel, the language panel and the mobile
   nav `position:absolute` UNCONDITIONALLY, reasoning that an element
   nothing ever closes (no JS means no `hidden` attribute, ever) should at
   least not push the page down. That broke reachability a different way:
   with no `hidden` and no JS to add one, all three floated open,
   permanently, over whatever `<main>` content sits beneath the header --
   confirmed covering home2/profiles's own H1, sources's H1 and breadcrumb,
   map's zoom controls, about's breadcrumb, with the nav's own links
   unreachable underneath the other two. `assets/belpulse/layout.css` now
   gates `position:absolute` on `:root.bp-js`, and `src/pages/shell.py`'s
   `SHELL_BOOTSTRAP` sets that class from a blocking inline script in
   <head> (not the linked, end-of-body `shell.js`), so it is present
   before paint for any reader who runs JavaScript at all, and never
   appears for one who does not -- the no-JS default falls back to the
   ORIGINAL in-flow, visible, reachable layout. This section checks the
   geometry directly: with JS off, at both 390 and 1440, none of the
   page's own landmarks (`h1`, `.bp-breadcrumb`, the map's zoom buttons)
   may be covered by the nav, the theme panel or the language panel.
"""

from __future__ import annotations

import functools
import http.server
import socketserver
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

MOBILE_WIDTH = 390
DESKTOP_WIDTH = 1440


class Quiet(socketserver.TCPServer):
    allow_reuse_address = True

    def log_message(self, *args):  # pragma: no cover - silence the server
        pass

    def handle_error(self, *args):  # pragma: no cover - a closed socket is not a failure
        pass


@pytest.fixture(scope="module")
def site():
    """The repository served over HTTP -- file:// would do here (no
    cross-frame concern, unlike test_iframe_contract.py), but a real origin
    keeps this file honest against the same serving model the site actually
    uses."""
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO_ROOT))
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


# --- 1. no-JS reachability ----------------------------------------------------


#: A link squeezed to a sliver by an unwrapped flex row is still
#: `is_visible()` in Playwright's sense (positive size, not display:none,
#: attached) -- caught post-audit, when `.bp-topbar .wrap` did not wrap at
#: 390px and every nav link collapsed to an ~11px-wide column with its text
#: clipped past the right edge. A real minimum width plus "does not run off
#: the viewport" is what actually catches that; `is_visible()` alone does not.
_MIN_READABLE_WIDTH = 15


def _assert_nav_links_are_actually_usable(page_obj, links, *, viewport_width: int, context: str):
    assert links.count() == 6, f"{context}: expected 6 nav links, found {links.count()}"
    for i in range(links.count()):
        link = links.nth(i)
        assert link.is_visible(), f"{context}: nav link {i} is not visible"
        box = link.bounding_box()
        assert box is not None, f"{context}: nav link {i} has no box"
        assert box["width"] >= _MIN_READABLE_WIDTH, (
            f"{context}: nav link {i} ({link.text_content()!r}) is only "
            f"{box['width']}px wide -- squeezed, not reachable"
        )
        assert box["x"] + box["width"] <= viewport_width + 1, (
            f"{context}: nav link {i} ({link.text_content()!r}) at x={box['x']}, "
            f"width={box['width']} runs past the {viewport_width}px viewport"
        )


@pytest.mark.parametrize(
    "page", ["home2.html", "macro.html", "profiles.html", "commune.html", "micro.html", "map.html"]
)
def test_no_js_390px_the_six_nav_links_are_reachable(chromium, site, page):
    """Client-mode pages: scripting off, a phone width, the primary nav has
    no toggle to press (`.bp-nav-toggle` stays `display:none` without
    `.bp-js`) -- so `#bp-nav` itself must already show all six links, each
    one actually wide enough to read and inside the viewport."""
    context = chromium.new_context(
        viewport={"width": MOBILE_WIDTH, "height": 800}, java_script_enabled=False
    )
    try:
        p = context.new_page()
        p.goto(f"{site}/{page}", wait_until="load")
        assert "bp-js" not in (p.evaluate("document.documentElement.className") or "")
        _assert_nav_links_are_actually_usable(
            p, p.locator("#bp-nav a"), viewport_width=MOBILE_WIDTH, context=page
        )
    finally:
        context.close()


@pytest.mark.parametrize("page", ["about.html", "preview/commune.html", "preview/map.html"])
def test_no_js_390px_generated_pages_nav_links_are_reachable(chromium, site, page):
    """Static-mode (generated) pages get the identical no-JS guarantee."""
    context = chromium.new_context(
        viewport={"width": MOBILE_WIDTH, "height": 800}, java_script_enabled=False
    )
    try:
        p = context.new_page()
        p.goto(f"{site}/{page}", wait_until="load")
        _assert_nav_links_are_actually_usable(
            p, p.locator("#bp-nav a"), viewport_width=MOBILE_WIDTH, context=page
        )
    finally:
        context.close()


def test_no_js_fr_about_the_english_and_dutch_editions_are_visible(chromium, site):
    """THE REGRESSION THE AUDIT NAMED: fr/about.html must let a reader without
    scripting reach the other two languages -- the whole reason the language
    switcher is real per-language `<a>` links in static mode, not a
    JS-driven swap."""
    context = chromium.new_context(
        viewport={"width": MOBILE_WIDTH, "height": 800}, java_script_enabled=False
    )
    try:
        p = context.new_page()
        p.goto(f"{site}/fr/about.html", wait_until="load")
        en = p.locator('.bp-lang-switch a[hreflang="en"]')
        nl = p.locator('.bp-lang-switch a[hreflang="nl"]')
        assert en.count() == 1 and en.is_visible(), "the English edition is not reachable"
        assert nl.count() == 1 and nl.is_visible(), "the Dutch edition is not reachable"
    finally:
        context.close()


def test_js_still_collapses_the_nav_once_it_has_run(chromium, site):
    """The other half: `.bp-js` DOES appear once shell.js runs, and the nav
    collapses behind the toggle again -- the fix must not have simply deleted
    the mobile-collapse feature."""
    context = chromium.new_context(viewport={"width": MOBILE_WIDTH, "height": 800})
    try:
        p = context.new_page()
        p.goto(f"{site}/home2.html", wait_until="load")
        assert "bp-js" in p.evaluate("document.documentElement.className")
        assert p.locator(".bp-nav-toggle").is_visible()
        assert not p.locator("#bp-nav").is_visible(), "the nav is not collapsed once JS has run"
        p.click(".bp-nav-toggle")
        assert p.locator("#bp-nav").is_visible()
        assert p.locator("#bp-nav a").count() == 6
    finally:
        context.close()


# --- 2. touch targets (>= 44x44 CSS px) ---------------------------------------


def _min_side(box) -> float:
    return min(box["width"], box["height"])


@pytest.mark.parametrize(
    "page", ["home2.html", "macro.html", "profiles.html", "commune.html", "micro.html", "map.html"]
)
def test_390px_touch_targets_are_at_least_44px(chromium, site, page):
    context = chromium.new_context(viewport={"width": MOBILE_WIDTH, "height": 800})
    try:
        p = context.new_page()
        p.goto(f"{site}/{page}", wait_until="load")

        toggle = p.locator(".bp-nav-toggle").bounding_box()
        assert toggle is not None, f"{page}: .bp-nav-toggle has no box"
        assert _min_side(toggle) >= 44, f"{page}: .bp-nav-toggle is {toggle}, smaller than 44px"

        for selector in (".bp-theme-menu .bp-menu-btn", ".bp-lang-menu .bp-menu-btn"):
            box = p.locator(selector).bounding_box()
            assert box is not None, f"{page}: {selector} has no box"
            assert box["height"] >= 44, f"{page}: {selector} is {box}, height under 44px"

        p.click(".bp-nav-toggle")
        links = p.locator("#bp-nav a")
        for i in range(links.count()):
            box = links.nth(i).bounding_box()
            assert box is not None
            assert box["height"] >= 44, f"{page}: nav link {i} is {box}, height under 44px"
    finally:
        context.close()


# --- 3. occlusion (issue #312 batch 2 audit) ----------------------------------

#: The page's own landmarks that a floating header panel must never cover.
#: Not every page carries every one of these -- a page without a given
#: selector is simply skipped for it, same as `_has_block_type`-style checks
#: elsewhere in this codebase never assume a shape that is not there.
_OCCLUSION_TARGETS = ("h1", ".bp-breadcrumb", "#zoomIn", "#zoomOut")

#: The three things the CLS fix made `position:absolute` (gated on
#: `:root.bp-js`) -- exactly what must never cover a landmark above when
#: JavaScript is off and that gate never fires.
_FLOATING_PANELS = (".bp-theme-switch", ".bp-lang-switch", "#bp-nav")


def _boxes_overlap(a, b) -> bool:
    # Playwright's bounding_box() returns {x, y, width, height}, not
    # left/top/right/bottom.
    a_right, b_right = a["x"] + a["width"], b["x"] + b["width"]
    a_bottom, b_bottom = a["y"] + a["height"], b["y"] + b["height"]
    return a["x"] < b_right and b["x"] < a_right and a["y"] < b_bottom and b["y"] < a_bottom


@pytest.mark.parametrize("width", [MOBILE_WIDTH, DESKTOP_WIDTH])
@pytest.mark.parametrize(
    "page",
    [
        "home2.html",
        "macro.html",
        "profiles.html",
        "commune.html",
        "micro.html",
        "map.html",
        "sources.html",
        "about.html",
    ],
)
def test_no_js_no_header_panel_covers_a_page_landmark(chromium, site, page, width):
    """With JavaScript off, `.bp-js` never appears (confirmed below), so the
    theme panel, the language panel and the nav all fall back to their
    no-JS default -- real, visible, in-flow. This is the geometric half of
    that guarantee: none of them may OVERLAP the page's own H1, breadcrumb
    or (on map.html) the zoom controls, at a phone width or a desktop one."""
    context = chromium.new_context(
        viewport={"width": width, "height": 900}, java_script_enabled=False
    )
    try:
        p = context.new_page()
        p.goto(f"{site}/{page}", wait_until="load")
        assert "bp-js" not in (p.evaluate("document.documentElement.className") or "")

        targets = {}
        for selector in _OCCLUSION_TARGETS:
            locator = p.locator(selector)
            if locator.count():
                targets[selector] = locator.first.bounding_box()

        panels = {}
        for selector in _FLOATING_PANELS:
            locator = p.locator(selector)
            if locator.count():
                panels[selector] = locator.first.bounding_box()

        bad = [
            (target_name, panel_name)
            for target_name, target_box in targets.items()
            if target_box
            for panel_name, panel_box in panels.items()
            if panel_box and _boxes_overlap(target_box, panel_box)
        ]
        assert not bad, f"{page} at {width}px: panel(s) cover landmark(s): {bad}"
    finally:
        context.close()
