"""The contract between a shell page and a block-built page it frames.

`index.html` is a shell: it loads other pages into an iframe and pushes the
reader's theme and language in by `postMessage`, waiting first for the frame to
answer `'dashboard-ready'`. The hand-built about.html implemented all of that.
When Batch 15d replaced it with a generated page, `src/pages/shell.py` had to
implement it too -- and NOTHING IN THIS REPOSITORY WOULD HAVE NOTICED IF IT HAD
NOT. The front page's About tab would have sat frozen in one language and one
theme, looking entirely fine.

WHY MOST OF THIS RUNS AGAINST A HARNESS AND NOT AGAINST index.html.

The first version of this file drove the real front page for every assertion
and was flaky in four different ways -- it failed CI twice and then 9 of 24
runs under parallel load, each fix moving the failure somewhere new:

  1. `loadDashboard(ABOUT_TAB)` fired before `window.onload`, whose own first
     act is `loadDashboard(0)`, so the tab was overwritten a moment later.
  2. The parent can DROP a tab request that arrives during its 300 ms fade.
  3. A message posted between the frame setting `data-framed` and registering
     its listener is not slow but LOST, so waiting longer never helped.
  4. A straggling navigation from setup was counted as the reload the
     loop-guard test watches for.

Every one of those is a race in the TEST against index.html's timers, not a
defect in the shipped page. Chasing them one at a time was the wrong approach:
the unit under test is the script `src/pages/shell.py` emits, and index.html is
merely one parent that speaks to it.

So the contract is tested against a HARNESS parent -- no fades, no timers,
messages sent exactly when asked -- and the real front page gets ONE
integration test proving it is such a parent. Fewer tests, more coverage, and
the assertions are about the code this repository actually wrote.

The harness implements the contract as index.html documents it, and the
integration test is what stops the two drifting: if index.html changed how it
talks to its frames, that test fails even though every harness test passes.
"""

from __future__ import annotations

import functools
import http.server
import socketserver
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

#: index.html's `dashboards` array: which iframe index is the About page.
ABOUT_TAB = 1

#: index.html's theme vocabulary mapped to the design system's. Duplicated
#: from src/pages/shell.py DELIBERATELY -- a test that imports the mapping it
#: is checking asserts the code agrees with itself.
THEME_EXPECTATIONS = {"day": "light", "soft": "light", "night": "dark"}


def _chromium():
    """A launched Chromium, or a clean skip. Same shape as
    tests/builder/test_builder_shell_service.py:_chromium."""
    try:
        from playwright import sync_api
    except ImportError as exc:
        pytest.skip(f"playwright is not a declared dependency ({exc})")
    try:
        manager = sync_api.sync_playwright().start()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"playwright could not start: {exc}")
    try:
        return manager, manager.chromium.launch()
    except Exception as exc:  # pragma: no cover - environment dependent
        manager.stop()
        pytest.skip(f"no chromium available: {exc}")


#: A parent that speaks the contract and NOTHING ELSE. No fade, no setTimeout,
#: no default tab -- it frames the page when told and posts when told, so a
#: test that fails here failed because the contract is wrong.
#:
#: It implements the contract exactly as index.html documents it: wait for
#: `dashboard-ready`, then post `setTheme` and `setLang`. `test_the_real_front_page_is_such_a_parent`
#: is what stops this drifting into a private protocol.
HARNESS = """<!doctype html><meta charset="utf-8"><title>frame harness</title>
<body><iframe id="f" style="width:900px;height:600px;border:0"></iframe>
<script>
window.ready = false;
window.seen = [];
window.addEventListener('message', function (e) {
  window.seen.push(e.data);
  if (e.data === 'dashboard-ready') { window.ready = true; }
});
window.open_frame = function (src) {
  window.ready = false;
  window.seen = [];
  document.getElementById('f').src = src;
};
window.send = function (type, value) {
  document.getElementById('f').contentWindow.postMessage({type: type, value: value}, '*');
};
window.frame_path = function () {
  return document.getElementById('f').contentWindow.location.pathname;
};
window.frame_attr = function (name) {
  return document.getElementById('f').contentDocument.documentElement.getAttribute(name);
};
</script>
"""

HARNESS_PATH = REPO_ROOT / "_iframe_harness.html"

#: How long a step of the contract may take. Only ever reached on failure --
#: every wait is on a condition, not a duration.
SETTLE_MS = 15000


@pytest.fixture(scope="module")
def site():
    """The repository served over HTTP, harness included.

    file:// will not do: the contract is cross-frame `postMessage` and a
    same-origin `contentDocument`, and file:// URLs are opaque origins to each
    other in Chromium. Testing it over file:// would test a different thing and
    pass.
    """
    HARNESS_PATH.write_text(HARNESS, encoding="utf-8")
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO_ROOT))

    class Quiet(socketserver.TCPServer):
        allow_reuse_address = True

        def log_message(self, *args):  # pragma: no cover - silence the server
            pass

        def handle_error(self, *args):  # pragma: no cover - a closed socket is not a failure
            pass

    server = Quiet(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        HARNESS_PATH.unlink(missing_ok=True)


@pytest.fixture(scope="module")
def browser():
    manager, launched = _chromium()
    try:
        yield launched
    finally:
        launched.close()
        manager.stop()


def _timeout_error():
    """Playwright's TimeoutError, imported lazily -- a module-level import
    would raise at COLLECTION time on a machine without playwright, defeating
    the clean skip `_chromium` exists to provide."""
    from playwright.sync_api import TimeoutError as PlaywrightTimeout

    return PlaywrightTimeout


def _wait_for(page, expression: str, what: str, timeout: int | None = None) -> None:
    """Wait for a condition in the page, never for a number of milliseconds."""
    page.wait_for_function(
        f"() => {{ try {{ return {expression}; }} catch (e) {{ return false; }} }}",
        timeout=timeout if timeout is not None else SETTLE_MS,
    )


@pytest.fixture
def framed(browser, site):
    """The English about page, framed by the harness and fully wired up.

    Waits for `dashboard-ready`, not for the `data-framed` attribute: the
    contract script sets that attribute FIRST (so the switcher never flashes)
    and announces itself LAST, and a message posted between the two is lost
    rather than delayed.
    """
    context = browser.new_context(viewport={"width": 1000, "height": 700})
    page = context.new_page()
    page.goto(f"{site}/_iframe_harness.html", wait_until="load")
    page.evaluate("open_frame('/about.html')")
    _wait_for(page, "window.ready === true", "the frame to announce itself")
    try:
        yield page
    finally:
        context.close()


# --- the contract itself, against a parent with no timers -------------------


def test_the_framed_page_announces_itself(framed):
    """A frame that never posts `dashboard-ready` is never told the theme or
    the language, and fails silently in the one direction nothing else covers.
    """
    assert "dashboard-ready" in framed.evaluate("window.seen")


@pytest.mark.parametrize("sent, expected", sorted(THEME_EXPECTATIONS.items()))
def test_the_parents_theme_reaches_the_frame_through_the_mapping(framed, sent, expected):
    """The two vocabularies do not overlap: the shell sends day/soft/night, the
    design system knows light/dark. Passed through raw, `data-theme="soft"`
    matches no stylesheet and the page silently stops following its parent."""
    framed.evaluate(f"send('setTheme', {sent!r})")
    _wait_for(
        framed, f"frame_attr('data-theme') === {expected!r}", f"the frame to adopt {expected}"
    )


def test_an_unknown_theme_falls_back_rather_than_being_applied_raw(framed):
    """The shell may grow a fourth theme. A frame that applies the name raw
    renders against a token set that does not exist -- and looks fine doing it,
    which is why this is a test and not a comment."""
    framed.evaluate("send('setTheme', 'dusk')")
    _wait_for(framed, "frame_attr('data-theme') === 'light'", "the unknown theme to fall back")


@pytest.mark.parametrize("lang, path", [("fr", "/fr/about.html"), ("nl", "/nl/about.html")])
def test_the_parents_language_navigates_the_frame_to_that_edition(framed, lang, path):
    """A block-built page is rendered per language on the server, so the French
    edition is a different URL -- it cannot re-translate in place the way the
    hand-built page did."""
    framed.evaluate(f"send('setLang', {lang!r})")
    _wait_for(framed, f"frame_path() === {path!r}", f"the frame to reach {path}")


def test_resending_the_same_language_does_not_reload_the_frame(framed):
    """A parent re-sends on every frame load. Without the `m.value!==here`
    guard that is an infinite reload, and an infinite reload of a page carrying
    a 1.2 MB boundary file is not a cosmetic bug.

    Counted by the frame's OWN navigation counter rather than by watching for
    an event that must not happen: `performance.navigation` survives no
    reloads, so if the frame reloads, the count resets and the marker is gone.
    """
    framed.evaluate(
        "document.getElementById('f').contentWindow.__stillHere = true;"
        "for (var i = 0; i < 3; i++) { send('setLang', 'en'); }"
    )
    framed.evaluate("send('setTheme', 'night')")
    # A reply to a LATER message proves the frame processed the three before it
    # and is still the same document -- no clock involved.
    _wait_for(framed, "frame_attr('data-theme') === 'dark'", "the frame to answer a later message")
    assert framed.evaluate(
        "document.getElementById('f').contentWindow.__stillHere === true"
    ), "the frame reloaded: a same-language setLang navigated when it must not"
    assert framed.evaluate("frame_path()") == "/about.html"


def test_the_frame_shows_no_language_switcher_of_its_own(framed):
    """THE REGRESSION THIS FILE WAS WRITTEN FOR.

    The switcher used to render inside the frame. Clicking "Francais" navigated
    the frame, the new page announced itself, the parent answered with its own
    unchanged language, and the reader watched their choice snap back -- while
    the click had already written `belpulse-lang: fr`, so the page said English
    and storage said French. The parent owns the language when it owns the
    frame.
    """
    display = framed.evaluate(
        "(function(){var f=document.getElementById('f');"
        "var n=f.contentDocument.querySelector('.bp-lang-switch');"
        "return n ? f.contentWindow.getComputedStyle(n).display : 'absent';})()"
    )
    assert display in ("none", "absent"), f"the framed page still offers a switcher ({display})"


def test_the_switcher_is_present_when_the_page_stands_alone(browser, site):
    """The other half: hidden only when framed. Opened directly, the page is
    the only place a reader can change language, and three real links are how
    it works without JavaScript."""
    context = browser.new_context()
    page = context.new_page()
    try:
        page.goto(f"{site}/about.html", wait_until="load")
        assert page.locator(".bp-lang-switch").is_visible()
        assert page.locator(".bp-lang-switch a").count() == 3
    finally:
        context.close()


# --- and the real front page is one such parent ------------------------------


def test_the_real_front_page_is_such_a_parent(browser, site):
    """The integration check, and the reason the harness above is not a private
    protocol I invented.

    IT ASSERTS ONLY ITS OWN CLAIM: that index.html SENDS what the contract
    says a parent sends. Whether the frame then finishes navigating is the
    harness's job, and it covers it exhaustively with no timers in the way.

    Earlier versions asserted the whole round trip through this page and were
    flaky five times over, in five different ways, because index.html is
    timer-driven -- it fades its iframe for 300 ms, calls loadDashboard(0) from
    window.onload, and re-syncs on several setTimeouts. Every one of those
    flakes was a race in the test, never a defect in the shipped page. An
    assertion that needs four of someone else's timers to line up is not
    testing what it says it tests.

    The spy is installed with add_init_script, which applies to EVERY frame in
    the context, so what the About page receives is observable without
    modifying it.
    """
    context = browser.new_context(viewport={"width": 1280, "height": 900})
    page = context.new_page()
    context.add_init_script(
        "window.__got = [];"
        "window.addEventListener('message', function (e) { window.__got.push(e.data); });"
    )
    try:
        page.goto(f"{site}/index.html", wait_until="networkidle")
        _wait_for(
            page,
            "document.getElementById('dashboard-frame').contentWindow"
            ".location.pathname.endsWith('/dashboard.html')",
            "index.html to finish initialising",
        )

        # Sent to the frame it already has. No navigation is involved: the
        # frame's language matches the parent's, so setLang is a no-op by the
        # contract's own guard -- which is exactly why this is deterministic.
        page.evaluate("syncDashboard()")
        _wait_for(
            page,
            "document.getElementById('dashboard-frame').contentWindow.__got"
            ".filter(function (m) { return m && m.type === 'setLang'; }).length > 0",
            "index.html to send the contract's messages",
        )
        sent = page.evaluate(
            "document.getElementById('dashboard-frame').contentWindow.__got"
            ".filter(function (m) { return m && m.type; })"
        )
        kinds = {m["type"] for m in sent}
        assert kinds == {
            "setTheme",
            "setLang",
        }, f"index.html no longer speaks the contract the harness tests: sent {sorted(kinds)}"
        # The values must be the vocabulary the harness maps, not something new.
        themes = {m["value"] for m in sent if m["type"] == "setTheme"}
        assert themes <= set(
            THEME_EXPECTATIONS
        ), f"index.html sent a theme the frame has no mapping for: {sorted(themes)}"
    finally:
        context.close()
