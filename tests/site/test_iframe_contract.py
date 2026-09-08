"""The contract between index.html and a block-built page it frames.

`index.html` is a shell: it loads other pages into an iframe and pushes the
reader's theme and language in by `postMessage`, waiting first for the frame to
answer `'dashboard-ready'`. The hand-built about.html implemented all of that.
When Batch 15d replaced it with a generated page, `src/pages/shell.py` had to
implement it too -- and NOTHING IN THIS REPOSITORY WOULD HAVE NOTICED IF IT
HAD NOT. The front page's About tab would have sat frozen in one language and
one theme, looking entirely fine.

DRIVEN IN A REAL BROWSER, because every part of this contract is cross-frame
runtime behaviour. Parsing the emitted script for the right substrings would
assert that the code was written, not that it works -- and the first version of
it did the wrong thing while containing every expected substring: clicking a
language INSIDE the frame navigated, the parent re-asserted its own unchanged
language on the new page's `dashboard-ready`, and the reader's choice snapped
back to English while localStorage said `fr`. That is the regression these
tests exist for.
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


@pytest.fixture(scope="module")
def site():
    """The repository served over HTTP.

    file:// will not do: the contract is cross-frame `postMessage` and a
    same-origin `contentDocument`, and file:// URLs are opaque origins to each
    other in Chromium. Testing it over file:// would test a different thing and
    pass.
    """
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


@pytest.fixture(scope="module")
def browser():
    manager, launched = _chromium()
    try:
        yield launched
    finally:
        launched.close()
        manager.stop()


#: How long any single step of the contract may take. Generous, because it is
#: only ever reached on failure -- every wait below is on a CONDITION, not a
#: duration.
SETTLE_MS = 15000

#: How long to watch for a navigation that must NOT happen. See the loop-guard
#: test for why this one cannot be a condition.
NO_EVENT_MS = 3000


def _wait_for(page, expression: str, what: str) -> None:
    """Wait for a condition in the page, never for a number of milliseconds.

    THIS FILE WAS FLAKY WITH FIXED SLEEPS AND WENT RED IN CI ON ITS FIRST RUN.
    The chain being tested is long and every hop has its own delay: index.html
    fades the iframe over 300 ms, sets `src`, the page loads, posts
    `dashboard-ready`, the parent waits `setTimeout(syncDashboard, 500)`, sends
    `setLang`, and the frame navigates and loads again. A sleep long enough on
    a developer laptop is not long enough on a cold CI runner, and picking a
    bigger number is guessing -- so nothing here sleeps.
    """
    page.wait_for_function(
        f"() => {{ try {{ return {expression}; }} catch (e) {{ return false; }} }}",
        timeout=SETTLE_MS,
    )


def _wait_for_frame_path(page, expected: str) -> None:
    _wait_for(
        page,
        f"document.getElementById('dashboard-frame').contentWindow"
        f".location.pathname === {expected!r}",
        f"the frame to reach {expected}",
    )


def _open_about(browser, site, *, lang=None, theme=None):
    """index.html with the About tab showing, optionally with a language and
    theme already chosen the way a returning reader would have them."""
    context = browser.new_context(viewport={"width": 1280, "height": 900})
    page = context.new_page()
    if lang or theme:
        page.add_init_script(
            "try{"
            + (f"localStorage.setItem('belpulse-lang',{lang!r});" if lang else "")
            + (f"localStorage.setItem('theme',{theme!r});" if theme else "")
            + "}catch(e){}"
        )
    page.goto(f"{site}/index.html", wait_until="networkidle")
    # The shell has to have defined its own controls before we drive them.
    _wait_for(page, "typeof loadDashboard === 'function'", "index.html to initialise")
    page.evaluate(f"loadDashboard({ABOUT_TAB})")
    # The frame has arrived when the contract script has run in it -- which is
    # the thing under test, so it is also the right thing to wait for.
    _wait_for(
        page,
        "document.getElementById('dashboard-frame').contentDocument"
        ".documentElement.getAttribute('data-framed') === '1'",
        "the framed page to run its contract script",
    )
    return context, page


def _frame_path(page):
    return page.eval_on_selector("#dashboard-frame", "e => e.contentWindow.location.pathname")


def _frame_theme(page):
    return page.eval_on_selector(
        "#dashboard-frame", "e => e.contentDocument.documentElement.getAttribute('data-theme')"
    )


def test_the_framed_page_announces_itself(browser, site):
    """`index.html:408` waits for `'dashboard-ready'` before syncing anything.
    A frame that never posts it is never told the theme or the language, and
    fails silently in the one direction nothing else covers."""
    context, page = _open_about(browser, site, lang="fr")
    try:
        # The proof it arrived: the parent acted on it. Nothing else would have
        # moved this frame off the English page.
        _wait_for_frame_path(page, "/fr/about.html")
    finally:
        context.close()


@pytest.mark.parametrize("parent_theme, expected", sorted(THEME_EXPECTATIONS.items()))
def test_the_parents_theme_reaches_the_frame_through_the_mapping(
    browser, site, parent_theme, expected
):
    """The two vocabularies do not overlap: index.html sends day/soft/night,
    the design system knows light/dark. Passed through raw, `data-theme="soft"`
    matches no stylesheet and the page silently stops following the shell."""
    context, page = _open_about(browser, site, theme=parent_theme)
    try:
        _wait_for(
            page,
            "document.getElementById('dashboard-frame').contentDocument"
            f".documentElement.getAttribute('data-theme') === {expected!r}",
            f"the frame to adopt {expected}",
        )
    finally:
        context.close()


def test_an_unknown_theme_falls_back_rather_than_being_applied_raw(browser, site):
    """The shell may grow a fourth theme. A frame that applies the name raw
    renders against a token set that does not exist -- and looks fine doing it,
    which is why this is a test and not a comment."""
    context, page = _open_about(browser, site)
    try:
        page.evaluate(
            "document.getElementById('dashboard-frame').contentWindow"
            ".postMessage({type:'setTheme',value:'dusk'},'*')"
        )
        _wait_for(
            page,
            "document.getElementById('dashboard-frame').contentDocument"
            ".documentElement.getAttribute('data-theme') === 'light'",
            "the unknown theme to fall back",
        )
    finally:
        context.close()


@pytest.mark.parametrize("lang, path", [("fr", "/fr/about.html"), ("nl", "/nl/about.html")])
def test_the_parents_language_navigates_the_frame_to_that_edition(browser, site, lang, path):
    """A block-built page is rendered per language on the server, so the French
    edition is a different URL -- it cannot re-translate in place the way the
    hand-built page did."""
    context, page = _open_about(browser, site)
    try:
        page.evaluate(f"setLang({lang!r})")
        _wait_for_frame_path(page, path)
    finally:
        context.close()


def test_resending_the_same_language_does_not_reload_the_frame(browser, site):
    """The parent re-sends on every frame load. Without the `m.value!==here`
    guard that is an infinite reload, and an infinite reload of a page carrying
    a 1.2 MB boundary file is not a cosmetic bug."""
    context, page = _open_about(browser, site)
    try:
        navigations = []
        page.on(
            "framenavigated",
            lambda frame: navigations.append(frame.url) if frame != page.main_frame else None,
        )
        for _ in range(3):
            page.evaluate("setLang('en')")
        # THE ONE WAIT HERE THAT MUST BE A DURATION. Every other wait in this
        # file is on a condition; you cannot wait on a condition for something
        # NOT happening. Erring long on purpose -- too short and a navigation
        # that is about to happen has simply not happened yet, and the test
        # passes for the wrong reason, which is worse than being slow.
        page.wait_for_timeout(NO_EVENT_MS)
        assert navigations == [], f"the frame reloaded {len(navigations)} time(s)"
        assert _frame_path(page) == "/about.html"
    finally:
        context.close()


def test_the_frame_shows_no_language_switcher_of_its_own(browser, site):
    """THE REGRESSION THIS FILE WAS WRITTEN FOR.

    The switcher used to render inside the frame. Clicking "Francais" navigated
    to /fr/about.html, the new page announced itself, the parent answered with
    its own unchanged setLang('en'), and the reader watched their choice snap
    back -- while the click had already written `belpulse-lang: fr`, so the page
    said English and storage said French. The parent owns the language when it
    owns the frame.
    """
    context, page = _open_about(browser, site)
    try:
        visible = page.eval_on_selector(
            "#dashboard-frame",
            "e => { var n = e.contentDocument.querySelector('.bp-lang-switch');"
            " return n ? e.contentWindow.getComputedStyle(n).display : 'absent'; }",
        )
        assert visible in ("none", "absent"), f"the framed page still offers a switcher ({visible})"
    finally:
        context.close()


def test_the_switcher_is_present_when_the_page_stands_alone(browser, site):
    """The other half: hidden only when framed. Opened directly, the page is
    the only place a reader can change language, and three real links are how
    it works without JavaScript."""
    context = browser.new_context()
    page = context.new_page()
    try:
        page.goto(f"{site}/about.html", wait_until="networkidle")
        assert page.locator(".bp-lang-switch").is_visible()
        assert page.locator(".bp-lang-switch a").count() == 3
    finally:
        context.close()
