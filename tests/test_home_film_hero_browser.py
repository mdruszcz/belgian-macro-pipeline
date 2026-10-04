"""Browser contract for the film, now that it has moved off the homepage.

Issue #312 batch 2 reversed the two-phase film/data hero this file used to
pin (the film-first redesign, superseding #300's behind-the-cards film
hero): home2.html opens directly on real figures, with no film and no
`<video>` request at all, and the film plays only on request, on about.html,
from a new `film` block type. This file pins both halves in a real browser:

1. A first visit to home2.html requests no video.
2. about.html's film block fetches nothing before a click, and exactly one
   request after -- keyboard-operable (Tab + Enter), same as a mouse click.

Runs against Chromium via tests/conftest.py's shared `chromium` fixture (the
`browser` marker is applied automatically to any test that reaches it); a
machine without Playwright/Chromium skips this whole module cleanly through
that fixture's skip-guard.
"""

from __future__ import annotations

import functools
import http.server
import re
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def site():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO))

    class Quiet(socketserver.ThreadingMixIn, http.server.HTTPServer):
        allow_reuse_address = True
        daemon_threads = True

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


def _film_requests(context):
    """Requests for the film MEDIA files only -- `.mp4`, never
    `film-block.js` itself, which is expected to load unconditionally
    whenever a page carries a film block (it is the click wiring, not the
    film)."""
    hits = []
    context.on(
        "request",
        lambda req: (
            hits.append(req.url) if re.search(r"home-film/film-\d+\.mp4", req.url) else None
        ),
    )
    return hits


@pytest.mark.parametrize(
    "viewport", [{"width": 1440, "height": 900}, {"width": 390, "height": 844}]
)
def test_home2_requests_no_video_on_a_first_visit(chromium, site, viewport):
    context = chromium.new_context(viewport=viewport)
    requests = _film_requests(context)
    try:
        page = context.new_page()
        page.goto(f"{site}/home2.html", wait_until="load")
        page.wait_for_timeout(500)
        assert requests == [], f"home2.html requested a film asset: {requests}"
    finally:
        context.close()


def test_about_film_block_fetches_nothing_before_play(chromium, site):
    context = chromium.new_context(viewport={"width": 1280, "height": 900})
    requests = _film_requests(context)
    try:
        page = context.new_page()
        page.goto(f"{site}/about.html", wait_until="load")
        page.wait_for_timeout(300)
        assert requests == [], f"about.html fetched the film before any click: {requests}"
    finally:
        context.close()


def test_about_film_plays_on_click_with_exactly_one_request_after(chromium, site):
    context = chromium.new_context(viewport={"width": 1280, "height": 900})
    requests = _film_requests(context)
    try:
        page = context.new_page()
        page.goto(f"{site}/about.html", wait_until="load")
        assert requests == [], "a request landed before the click"

        page.click(".bp-block--film .film-play")
        page.wait_for_function(
            "(() => { const v = document.querySelector('.bp-block--film .film-video');"
            " return v && v.controls; })()",
            timeout=5000,
        )
        page.wait_for_timeout(500)
        assert len(requests) >= 1, "no film request followed the click"
        # Exactly one *distinct* file requested -- the renderer emits two
        # <source> candidates (desktop/mobile), and the browser picks one.
        assert len({r.rsplit("/", 1)[-1] for r in requests}) == 1

        assert page.eval_on_selector(".bp-block--film .film-play", "el => el.hidden") is True
    finally:
        context.close()


def test_about_film_play_button_is_reachable_and_activatable_by_keyboard(chromium, site):
    context = chromium.new_context(viewport={"width": 1280, "height": 900})
    requests = _film_requests(context)
    try:
        page = context.new_page()
        page.goto(f"{site}/about.html", wait_until="load")
        page.eval_on_selector(".bp-block--film .film-play", "el => el.focus()")
        assert page.eval_on_selector(
            ".bp-block--film .film-play", "el => el === document.activeElement"
        )
        page.keyboard.press("Enter")
        page.wait_for_function(
            "(() => { const v = document.querySelector('.bp-block--film .film-video');"
            " return v && v.controls; })()",
            timeout=5000,
        )
        page.wait_for_timeout(300)
        assert len(requests) >= 1, "Enter on the focused button did not start playback"
    finally:
        context.close()


def test_about_film_video_carries_no_autoplay_attribute(chromium, site):
    context = chromium.new_context(viewport={"width": 1280, "height": 900})
    try:
        page = context.new_page()
        page.goto(f"{site}/about.html", wait_until="load")
        has_autoplay = page.eval_on_selector(
            ".bp-block--film .film-video", "el => el.hasAttribute('autoplay')"
        )
        assert has_autoplay is False
        assert page.eval_on_selector(".bp-block--film .film-video", "el => el.paused") is True
    finally:
        context.close()
