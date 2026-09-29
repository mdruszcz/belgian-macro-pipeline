"""Browser contract for the two-phase film hero on home2.html (the film-first
redesign, screenshots-review/home-v5/, superseding #300's behind-the-cards
film hero).

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

    # ThreadingMixIn, unlike tests/test_charts_tooltip.py's plain TCPServer:
    # this suite's own film hero deliberately leaves an in-flight film-*.mp4
    # request open across a Skip/navigation click (matching real playback --
    # pausing the <video> element does not cancel its underlying network
    # request). A single-threaded server would then serve that multi-MB
    # response before it could even accept the next page's GET, which is a
    # server-fixture artifact, not a real behaviour: a real static host
    # (GitHub Pages) serves both requests concurrently.
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
        # what the real API surface looks like to initHero().
        context.add_init_script(
            "Object.defineProperty(navigator, 'connection', {value: {saveData: true, effectiveType: '4g'}});"
        )
    page = context.new_page()
    return context, page


def test_phase1_shows_only_the_film_and_autoplays_muted(chromium, site):
    """A first visit in a fresh session (default reduced-motion, no Save-Data)
    opens in phase 1 (data-phase="1"): the film alone, actually playing, muted
    -- and the phase-2 data hero's map/chart cards must not be visibly
    competing with it (the #300 defect the maintainer rejected)."""
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "(() => { const v = document.getElementById('filmVideo');"
            " return v && !v.hidden && !v.paused; })()",
            timeout=10000,
        )
        assert page.eval_on_selector("#filmHero", "el => el.dataset.phase") == "1"
        muted = page.eval_on_selector("#filmVideo", "el => el.muted")
        assert muted is True
        # The film band actually occupies real vertical space (~92vh), i.e.
        # it is not a thin strip fighting a data hero rendered on top of it.
        film_height = page.eval_on_selector("#filmHero", "el => el.getBoundingClientRect().height")
        viewport_height = 900
        assert film_height > viewport_height * 0.8
    finally:
        context.close()


def test_skip_button_moves_to_phase2(chromium, site):
    """Clicking 'Passer' (v5SkipFilm) ends phase 1 immediately: the film
    freezes (pauses, poster shown), the band collapses out of its 92vh
    height, and the data hero becomes reachable."""
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_selector("#filmSkip:not([hidden])", timeout=10000)
        page.click("#filmSkip")
        page.wait_for_function(
            "(() => document.getElementById('filmHero').dataset.phase === '2')()",
            timeout=10000,
        )
        assert page.eval_on_selector("#filmVideo", "el => el.paused") is True
        assert page.eval_on_selector("#filmPoster", "el => el.hidden") is False
    finally:
        context.close()


def test_film_ending_moves_to_phase2_with_replay_visible(chromium, site):
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "(() => { const v = document.getElementById('filmVideo'); return v && !v.hidden; })()",
            timeout=10000,
        )
        page.evaluate(
            "(() => { document.getElementById('filmVideo')"
            ".dispatchEvent(new Event('ended')); })()"
        )
        page.wait_for_function(
            "(() => document.getElementById('filmHero').dataset.phase === '2')()",
            timeout=10000,
        )
        page.wait_for_selector("#filmReplay:not([hidden])", timeout=10000)
        # The film freezes on its LAST FRAME as a visible slim banner (per
        # the plan) on every phase-2 transition, including the film's own
        # natural end -- not just a second-session visit. #filmHero's
        # poster/video are position:absolute, so without the .banner class
        # (which is what actually gives the band a height) the band would
        # collapse to 0 and the frozen frame would not be visible at all.
        assert (
            page.eval_on_selector("#filmHero", "el => el.classList.contains('banner')") is True
        ), "the frozen last frame must be visible (banner-sized), not collapsed to 0 height"
        film_height = page.eval_on_selector("#filmHero", "el => el.getBoundingClientRect().height")
        assert 0 < film_height < 900 * 0.6
    finally:
        context.close()


def test_second_navigation_same_session_opens_straight_into_phase2_banner(chromium, site):
    """sessionStorage carries across navigations in the same context/tab, so a
    second `page.goto` of the same page in one browser context is exactly the
    'later page view in the same session' the plan describes: the page opens
    DIRECTLY in phase 2, the film collapsed to a slim banner (its last frame),
    no autoplay, and a visible 'Revoir le film' control."""
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "(() => { const v = document.getElementById('filmVideo'); return v && !v.hidden; })()",
            timeout=10000,
        )
        page.evaluate(
            "(() => { document.getElementById('filmVideo')"
            ".dispatchEvent(new Event('ended')); })()"
        )
        page.wait_for_function(
            "(() => sessionStorage.getItem('belpulse-home-film-played') === '1')()",
            timeout=10000,
        )

        page.goto(f"{site}/home2.html")
        page.wait_for_selector("#filmReplay:not([hidden])", timeout=10000)
        assert page.eval_on_selector("#filmHero", "el => el.dataset.phase") == "2"
        assert page.eval_on_selector("#filmHero", "el => el.classList.contains('banner')") is True
        poster_hidden = page.eval_on_selector("#filmPoster", "el => el.hidden")
        video_hidden = page.eval_on_selector("#filmVideo", "el => el.hidden")
        assert poster_hidden is False
        assert video_hidden is True
        video_paused = page.eval_on_selector("#filmVideo", "el => el.paused")
        assert video_paused is True
        assert page.locator("#filmReplay").is_visible()

        # The banner is materially smaller than the full 92vh phase-1 band.
        banner_height = page.eval_on_selector(
            "#filmHero", "el => el.getBoundingClientRect().height"
        )
        assert banner_height < 900 * 0.5
    finally:
        context.close()


def test_replay_button_replays_the_film_in_place(chromium, site):
    """'Revoir le film' on the second-visit banner plays the film again,
    without leaving the page or losing the data hero below it."""
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "(() => { const v = document.getElementById('filmVideo'); return v && !v.hidden; })()",
            timeout=10000,
        )
        page.evaluate(
            "(() => { document.getElementById('filmVideo')"
            ".dispatchEvent(new Event('ended')); })()"
        )
        page.goto(f"{site}/home2.html")
        page.wait_for_selector("#filmReplay:not([hidden])", timeout=10000)
        page.click("#filmReplay")
        page.wait_for_function(
            "(() => { const v = document.getElementById('filmVideo');"
            " return v && !v.hidden && !v.paused; })()",
            timeout=10000,
        )
        assert page.eval_on_selector("#filmHero", "el => el.dataset.phase") == "1"
        assert page.eval_on_selector("#filmHero", "el => el.classList.contains('banner')") is False
    finally:
        context.close()


def test_reduced_motion_never_requests_the_video_and_opens_phase2(chromium, site):
    """With prefers-reduced-motion: reduce, initHero() goes straight to phase
    2 (poster-only banner) before wireSources() ever runs, so no <source>'s
    data-src is ever copied into a live src and the browser issues no request
    for any film-*.mp4 file."""
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
        page.wait_for_function(
            "(() => document.getElementById('filmHero').dataset.phase === '2')()",
            timeout=10000,
        )
        page.wait_for_timeout(300)
        assert video_requests == []
        assert page.eval_on_selector("#filmVideo", "el => el.hidden") is True
        for src in page.eval_on_selector_all("#filmVideo source", "els => els.map(e => e.src)"):
            assert src == "" or src is None
        # No replay control either -- there is nothing to replay.
        assert page.eval_on_selector("#filmReplay", "el => el.hidden") is True
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
    <video>'s <source> once initHero() runs -- never the 1080p desktop
    source."""
    context, page = _new_page(chromium, viewport={"width": 390, "height": 844})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "(() => { const v = document.getElementById('filmVideo');"
            " return v && v.querySelector('source[data-variant=\"mobile\"]').src; })()",
            timeout=10000,
        )
        mobile_src = page.eval_on_selector(
            '#filmVideo source[data-variant="mobile"]', "el => el.src"
        )
        desktop_srcs = page.eval_on_selector_all(
            '#filmVideo source[data-variant="desktop"]', "els => els.map(e => e.src)"
        )
        assert "film-720.mp4" in mobile_src
        assert all(s == "" for s in desktop_srcs)
    finally:
        context.close()


def test_cta_and_search_are_usable_in_phase2(chromium, site):
    """Once in phase 2 (here: via Skip), the 'Explore the data' CTA and the
    commune search box are real, interactable elements -- not obscured by the
    film layer, which has collapsed out of the way."""
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_selector("#filmSkip:not([hidden])", timeout=10000)
        page.click("#filmSkip")
        page.wait_for_function(
            "(() => document.getElementById('filmHero').dataset.phase === '2')()",
            timeout=10000,
        )
        # Skip triggers a smooth scrollIntoView() on #dataHero; let it settle
        # before clicking, otherwise Playwright's own actionability check
        # (element must be stable) races the in-flight smooth scroll.
        page.wait_for_timeout(600)
        cta = page.locator('.datahero a.bp-btn--primary[href="profiles.html"]')
        cta.wait_for(state="visible", timeout=5000)
        cta.click()
        page.wait_for_url("**/profiles.html", timeout=10000)
    finally:
        context.close()


def test_film_element_is_never_covered_by_the_hero_cards(chromium, site):
    """The #300 defect the maintainer rejected: the film played BEHIND the
    map/chart cards. Structurally verified here by z-order/geometry: in
    phase 1, #filmHero's bounding box and #dataHero's (which holds the
    map/chart cards) must not overlap at all -- the data hero only exists
    below the fold, not stacked underneath the film."""
    context, page = _new_page(chromium, viewport={"width": 1280, "height": 900})
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_selector("#filmHero", timeout=10000)
        overlap = page.evaluate("""() => {
                const film = document.getElementById('filmHero').getBoundingClientRect();
                const data = document.getElementById('dataHero').getBoundingClientRect();
                const top = Math.max(film.top, data.top);
                const bottom = Math.min(film.bottom, data.bottom);
                return Math.max(0, bottom - top);
            }""")
        assert overlap == 0, f"#filmHero and #dataHero overlap by {overlap}px"
    finally:
        context.close()
