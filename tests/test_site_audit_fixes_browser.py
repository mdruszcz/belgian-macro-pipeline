"""Browser contract for the 2026-09-29 site-audit fixes that can only be
observed by actually rendering a page: horizontal overflow at a real phone
width, keyboard focus actually being visible, and a document's <title>
changing after a client-side language switch (which only fires once JS runs
I18N.applyStrings()).

Served over real HTTP the same way tests/test_charts_tooltip.py and
tests/test_home_film_hero_browser.py do (these pages fetch() JSON payloads
that need a real origin). Runs against Chromium via tests/conftest.py's
shared `chromium` fixture; a machine without Playwright/Chromium skips this
whole module cleanly through that fixture's own skip-guard.
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


def test_profiles_html_has_no_horizontal_overflow_at_390px(chromium, site):
    """The reported defect: #pickType (a <select> inside .toolbar) grew to
    618px wide inside a 390px viewport (max-width:480px's own
    `.toolbar select{flex:1 1 100%; max-width:none}` let it, with no
    min-width:0 to stop it), pushing the whole document wider than the
    viewport and making the page scroll sideways. documentElement's
    scrollWidth must not exceed its clientWidth once the page has settled."""
    context = chromium.new_context(viewport={"width": 390, "height": 844})
    page = context.new_page()
    try:
        page.goto(f"{site}/profiles.html")
        page.wait_for_load_state("networkidle", timeout=15000)
        overflow = page.evaluate(
            "document.documentElement.scrollWidth - document.documentElement.clientWidth"
        )
        assert overflow <= 1, f"profiles.html overflows its 390px viewport by {overflow}px"
    finally:
        context.close()


def test_commune_search_input_shows_a_visible_focus_change(chromium, site):
    """#communeSearch's own <input> has outline:0 (its .searchbox wrapper
    draws the border instead), so before this fix, tabbing to it produced NO
    visible change at all -- neither the browser's default ring (suppressed)
    nor a replacement (never added). After the fix, the .searchbox WRAPPER
    gets a :focus-within outline. Compares the wrapper's own outline style
    focused vs not, rather than asserting a specific colour, so the test
    does not need to know the accent token's exact value."""
    context = chromium.new_context(viewport={"width": 1280, "height": 900})
    page = context.new_page()
    try:
        page.goto(f"{site}/home2.html")
        page.wait_for_selector("#communeSearch", timeout=15000)
        unfocused = page.eval_on_selector(
            "#communeSearch", "el => getComputedStyle(el.closest('.searchbox')).outlineStyle"
        )
        page.focus("#communeSearch")
        focused = page.eval_on_selector(
            "#communeSearch", "el => getComputedStyle(el.closest('.searchbox')).outlineStyle"
        )
        assert unfocused == "none", f"searchbox has a visible outline even unfocused: {unfocused}"
        assert focused == "solid", f"searchbox has no visible focus outline: {focused}"
    finally:
        context.close()


def test_document_title_changes_when_language_switches(chromium, site):
    """home2.html's <title> stayed the English
    "BelPulse — Belgium's economic data, updated automatically" forever, in
    every language, because nothing ever re-applied it on a language switch
    -- only the visible data-t elements re-translated. I18N.applyStrings()
    now also handles title[data-t-doctitle], and shell.js's language-menu
    click handler calls applyStrings() on every switch, so clicking French
    in the language menu must change document.title away from the English
    string."""
    context = chromium.new_context(viewport={"width": 1280, "height": 900})
    # I18N.initial() (assets/i18n.js) picks the reader's saved choice FIRST,
    # then the browser/OS language, then English -- this test machine's own
    # locale is Belgian, so a fresh context with no saved choice already
    # opens in French (a correct, separate behaviour, not what this test is
    # checking). Pinning 'en' via an init script pins the STARTING point so
    # the assertion below is actually about the language MENU CLICK causing
    # the title to change, not about what the browser's default locale
    # happens to be on whatever machine runs this test.
    context.add_init_script("try{localStorage.setItem('belpulse-lang','en');}catch(e){}")
    page = context.new_page()
    try:
        page.goto(f"{site}/home2.html")
        # NOT wait_for_load_state("networkidle"): home2.html's own film hero
        # (assets/belpulse/home-film/film-1080.mp4, ~11MB) deliberately keeps
        # an in-flight video request open past first paint (same reason
        # tests/test_home_film_hero_browser.py's own server fixture comment
        # gives), so "networkidle" never fires. Wait for the language menu
        # button to exist instead -- it is present as soon as the shared
        # header markup has rendered, well before the film finishes loading.
        page.wait_for_selector("#bp-lang-menu-btn", timeout=15000)
        english_title = page.title()
        assert "economic data" in english_title.lower() or "belgium" in english_title.lower()

        page.click("#bp-lang-menu-btn")
        page.wait_for_selector('.bp-lang-switch [data-lang="fr"]', timeout=10000)
        page.click('.bp-lang-switch [data-lang="fr"]')
        page.wait_for_function(
            "document.title !== " + repr(english_title),
            timeout=10000,
        )
        french_title = page.title()
        assert french_title != english_title
        assert "données économiques" in french_title.lower()
    finally:
        context.close()
