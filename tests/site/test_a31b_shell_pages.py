"""Batch A3.1b (docs/features/site_unification.md): profiles.html,
commune.html, map.html and micro.html carry the shared header and footer.

Two browser properties the maintainer asked for explicitly, neither covered
by the marker-sync tests (tests/site/test_site_shell_sync.py) or the
no-JS/touch-target sweep (tests/site/test_shell_no_js_and_touch_targets.py):

  1. A theme choice made on one converted page (map.html) survives
     navigating to another (micro.html), and is applied BEFORE PAINT there --
     the whole point of SHELL_BOOTSTRAP running inline in <head> rather than
     from a linked script.
  2. Switching language on a converted page re-renders the PAGE'S OWN CONTENT
     (not just the header/footer chrome) -- the 'bp:lang' event contract
     every converted page's boot script relies on.
"""

from __future__ import annotations

import functools
import http.server
import json
import socketserver
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


class Quiet(socketserver.TCPServer):
    allow_reuse_address = True

    def log_message(self, *args):  # pragma: no cover - silence the server
        pass

    def handle_error(self, *args):  # pragma: no cover - a closed socket is not a failure
        pass


@pytest.fixture(scope="module")
def site():
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


def test_papier_theme_persists_from_map_to_micro_before_paint(chromium, site):
    context = chromium.new_context(viewport={"width": 1280, "height": 900})
    try:
        page = context.new_page()
        page.goto(f"{site}/map.html", wait_until="load")

        page.click(".bp-theme-menu .bp-menu-btn")
        page.click('.bp-theme-menu [data-theme-choice="paper"]')
        assert page.evaluate("document.documentElement.getAttribute('data-theme')") == "paper"
        assert page.evaluate("localStorage.getItem('belpulse-theme')") == "paper"

        # A new page, same origin (same localStorage) -- navigate rather than
        # reload, to prove the choice carries SITE-WIDE, not just on the page
        # that set it.
        page.goto(f"{site}/micro.html", wait_until="domcontentloaded")
        # SHELL_BOOTSTRAP is inline in <head>, so by 'domcontentloaded' --
        # before the rest of the document, let alone a paint, has happened --
        # the attribute is already set. A page that only picked it up from a
        # linked script or its own body would still show the default here.
        assert page.evaluate("document.documentElement.getAttribute('data-theme')") == "paper"

        page.wait_for_load_state("load")
        assert page.evaluate("document.documentElement.getAttribute('data-theme')") == "paper"
        checked = page.eval_on_selector_all(
            ".bp-theme-menu [data-theme-choice]",
            "els => els.filter(e => e.getAttribute('aria-checked') === 'true')"
            ".map(e => e.getAttribute('data-theme-choice'))",
        )
        assert checked == ["paper"], "micro.html's theme menu does not show the carried choice"
    finally:
        context.close()


def test_switching_to_nl_on_profiles_relabels_page_content_not_just_the_header(chromium, site):
    context = chromium.new_context(viewport={"width": 1280, "height": 900})
    try:
        page = context.new_page()
        page.goto(f"{site}/profiles.html", wait_until="load")

        title = page.locator("#pfTitle")
        page.wait_for_function(
            "document.getElementById('pfTitle') && "
            "document.getElementById('pfTitle').textContent !== '—' && "
            "document.getElementById('pfTitle').textContent.length > 0"
        )
        english = title.text_content()
        assert "communes" in english.lower(), f"unexpected English heading: {english!r}"

        page.click(".bp-lang-menu .bp-menu-btn")
        page.click('.bp-lang-menu [data-lang="nl"]')

        # The header's own language menu switching state is covered by
        # test_iframe_contract.py elsewhere; the property THIS test exists
        # for is that the page's own content -- not just chrome -- re-rendered.
        page.wait_for_function(
            f"document.getElementById('pfTitle').textContent !== {json.dumps(english)}"
        )
        dutch = title.text_content()
        assert dutch != english
        assert "gemeenten" in dutch.lower(), f"unexpected Dutch heading: {dutch!r}"
        assert page.evaluate("document.documentElement.getAttribute('lang')") == "nl"
    finally:
        context.close()
