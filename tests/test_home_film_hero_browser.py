"""Browser contract for the brand-film hero on home2.html (docs/features/home_film.md).

Runs against Chromium via tests/conftest.py's shared `chromium` fixture (the
`browser` marker is applied automatically to any test that reaches it -- see
that file's own comment); a machine without Playwright/Chromium skips this
whole module cleanly through that fixture's skip-guard, matching
tests/test_charts_tooltip.py's own pattern, which this file borrows its HTTP
server fixture from (file:// would also work here since the film never
cross-frame postMessages, but a real origin costs nothing extra and matches
production).
"""

from __future__ import annotations

import functools
import http.server
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def site():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO))

    class Quiet(socketserver.TCPServer):
        allow_reuse_address = True

        def log_message(self, *args):  # pragma: no cover
            pass

        def handle_error(self, *args):  # pragma: no cover
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


def _new_page(chromium, viewport=None, reduced_motion=None, save_data=False):
    kwargs = {}
    if viewport:
        kwargs["viewport"] = viewport
    if reduced_motion:
        kwargs["reduced_motion"] = reduced_motion
    context = chromium.new_context(**kwargs)
    if save_data:
        # navigator.connection is not controllable via Playwright context
        # options, so it is stubbed before any page script runs -- exactly
        # what the real API surface looks like to initHeroFilm().
        context.add_init_script(
            "Object.defineProperty(navigator, 'connection', {value: {saveData: true, effectiveType: '4g'}});"
        )
    page = context.new_page()
    return context, page


def test_first_visit_autoplays_muted(chromium, site):
    """A first visit in a fresh session (default reduced-motion, no Save-Data)
    must actually start playing the video, muted, shortly after load -- not
    merely be technically eligible to."""
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "(() => { const v = document.getElementById('heroVideo');"
            " return v && !v.hidden && !v.paused; })()",
            timeout=10000,
        )
        muted = page.eval_on_selector("#heroVideo", "el => el.muted")
        assert muted is True
    finally:
        context.close()


def test_second_navigation_same_session_shows_poster_with_replay_and_no_autoplay(chromium, site):
    """sessionStorage carries across navigations in the same context/tab, so a
    second `page.goto` of the same page in one browser context is exactly the
    'later page view in the same session' the plan describes: poster, no
    autoplay, and a visible Replay button."""
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "(() => { const v = document.getElementById('heroVideo'); return v && !v.hidden; })()",
            timeout=10000,
        )
        # initHeroFilm() marks the session played and swaps to the poster on
        # the video's own 'ended' event -- dispatch it directly rather than
        # waiting out the film's real length, which keeps this test fast and
        # independent of whatever duration the current cut happens to have.
        page.evaluate(
            "(() => { document.getElementById('heroVideo')"
            ".dispatchEvent(new Event('ended')); })()"
        )
        page.wait_for_function(
            "(() => sessionStorage.getItem('belpulse-home-film-played') === '1')()",
            timeout=10000,
        )

        page.goto(f"{site}/home2.html")
        page.wait_for_selector("#heroReplay:not([hidden])", timeout=10000)
        poster_hidden = page.eval_on_selector("#heroPoster", "el => el.hidden")
        video_hidden = page.eval_on_selector("#heroVideo", "el => el.hidden")
        assert poster_hidden is False
        assert video_hidden is True
        video_paused = page.eval_on_selector("#heroVideo", "el => el.paused")
        assert video_paused is True
        assert page.locator("#heroReplay").is_visible()
    finally:
        context.close()


def test_reduced_motion_never_requests_the_video(chromium, site):
    """With prefers-reduced-motion: reduce, initHeroFilm() returns before
    wireSources() ever runs, so no <source>'s data-src is ever copied into a
    live src and the browser issues no request for any film-*.mp4/webm file."""
    context, page = _new_page(
        chromium, viewport={"width": 1280, "height": 900}, reduced_motion="reduce"
    )
    video_requests = []
    context.on(
        "request",
        lambda req: video_requests.append(req.url) if "home-film/film-" in req.url else None,
    )
    try:
        page.goto(f"{site}/home2.html")
        # Give initHeroFilm() (which runs synchronously on script execution,
        # no fetch involved) time to have made its decision.
        page.wait_for_timeout(500)
        assert video_requests == []
        # The <video> element itself is still hidden -- the poster is what's showing.
        assert page.eval_on_selector("#heroVideo", "el => el.hidden") is True
        for src in page.eval_on_selector_all("#heroVideo source", "els => els.map(e => e.src)"):
            assert src == "" or src is None
    finally:
        context.close()


def test_save_data_never_requests_the_video(chromium, site):
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900}, save_data=True)
    video_requests = []
    context.on(
        "request",
        lambda req: video_requests.append(req.url) if "home-film/film-" in req.url else None,
    )
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_timeout(500)
        assert video_requests == []
    finally:
        context.close()


def test_mobile_viewport_uses_the_720p_source(chromium, site):
    """At <=768px, the 720p mobile source is the one actually wired into the
    <video>'s <source> once initHeroFilm() runs (after load/idle on mobile,
    per the plan) -- never the 1080p/1440p desktop sources."""
    context, page = _new_page(chromium, viewport={"width": 390, "height": 844})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "(() => { const v = document.getElementById('heroVideo');"
            " return v && v.querySelector('source[data-variant=\"mobile\"]').src; })()",
            timeout=10000,
        )
        mobile_src = page.eval_on_selector(
            '#heroVideo source[data-variant="mobile"]', "el => el.src"
        )
        desktop_srcs = page.eval_on_selector_all(
            '#heroVideo source[data-variant="desktop"]', "els => els.map(e => e.src)"
        )
        assert "film-720.mp4" in mobile_src
        assert all(s == "" for s in desktop_srcs)
    finally:
        context.close()


def test_cta_is_clickable_at_t0(chromium, site):
    """The 'Explore the data' CTA sits above the film layer (z-index) from
    the very first frame -- a click must register immediately, without
    waiting for the video to load or play."""
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        cta = page.locator('.hero a.bp-btn--primary[href="profiles.html"]')
        cta.wait_for(state="visible", timeout=5000)
        cta.click()
        page.wait_for_url("**/profiles.html", timeout=10000)
    finally:
        context.close()
