"""The contract between a shell page and a block-built page it frames.

The former `index.html` shell loaded pages into an iframe and pushed the
reader's theme and language by `postMessage`, waiting for `'dashboard-ready'`.
That shell was retired when home2 became the public homepage, but generated
pages still support the framing contract for embedders and previews.

WHY THIS RUNS AGAINST A HARNESS.

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

Every one of those was a race in the TEST against the old index.html timers,
not a defect in the framed page. The unit under test is the script
`src/pages/shell.py` emits.

The contract is therefore tested against a HARNESS parent -- no fades, no
timers, messages sent exactly when asked. A separate integration test now
proves that the legacy front-page URL redirects to home2.
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
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.02},  # shutdown() returns in ~20 ms, not up to 500
        daemon=True,
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        HARNESS_PATH.unlink(missing_ok=True)


@pytest.fixture(scope="module")
def browser(chromium):
    """The session's Chromium (tests/conftest.py). Every test here still makes
    its own context -- `framed` does, and the two tests that take `browser`
    directly do -- so nothing is shared between tests but the process."""
    return chromium


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

    UPDATED FOR BATCH A1.1: queries `.bp-lang-menu`, the button-plus-panel
    wrapper the maquette's language menu renders as -- hiding only the inner
    `.bp-lang-switch` panel would still leave the globe/"EN" button itself
    visible and clickable inside the frame, which is the same regression this
    test exists to catch.
    """
    display = framed.evaluate(
        "(function(){var f=document.getElementById('f');"
        "var n=f.contentDocument.querySelector('.bp-lang-menu');"
        "return n ? f.contentWindow.getComputedStyle(n).display : 'absent';})()"
    )
    assert display in ("none", "absent"), f"the framed page still offers a switcher ({display})"


def test_the_frame_shows_no_theme_switch_of_its_own(framed):
    """Same fight, same resolution. index.html pushes `setTheme` in on every
    frame load, so a reader choosing Light inside the frame would watch it snap
    back to the parent's choice a moment later -- and the click has already
    been written to storage by then.

    UPDATED FOR BATCH A1.1: queries `.bp-theme-menu`, the wrapper the maquette's
    theme menu renders as (src/pages/shell.py, `render_header`) -- the old
    `.bp-theme-toggle` segmented control this test used to query does not
    appear on a page this batch covers any more, so that selector would have
    passed here vacuously (element absent, not element hidden) rather than
    proving the framed-hide CSS rule actually fires."""
    display = framed.evaluate(
        "(function(){var f=document.getElementById('f');"
        "var n=f.contentDocument.querySelector('.bp-theme-menu');"
        "return n ? f.contentWindow.getComputedStyle(n).display : 'absent';})()"
    )
    assert display in ("none", "absent"), f"the framed page still offers a theme switch ({display})"


def test_a_dark_machine_can_still_be_shown_the_light_design(browser, site):
    """THE COMPLAINT THIS EXISTS FOR: a block-built page followed
    prefers-color-scheme and nothing else, so a maintainer whose laptop is in
    dark mode could not see the light palette his own design reference is drawn
    in -- on the very page built to match it.

    Run in a context that reports a dark operating system, because that is the
    case where the design has to win.

    UPDATED FOR BATCH A1.1 (docs/features/site_unification.md): the three-button
    segmented `.bp-theme-toggle` is gone, replaced by the maquette's theme MENU
    (a button that opens a small panel), and the menu deliberately drops the
    `Auto` choice from its UI -- `THEME_CHOICES` in src/pages/shell.py is now
    Light/Dark only, with a comment recording that A1.2 appends Papier to the
    same tuple. This is an intended narrowing of the control surface, not a
    weakening of the underlying guarantee: `SHELL_BOOTSTRAP` still reads and
    honours a reader's PRE-EXISTING `auto` choice (resolving it against
    prefers-color-scheme and overwriting storage with the explicit result), so
    the assertion below about that upgrade path stays, just driven by seeding
    localStorage directly rather than by clicking a control that no longer
    exists. What survives from the original test: light is still the default
    on a dark machine with no saved choice, the reader's explicit choice
    persists across a reload, and the control shows what was chosen.
    """
    context = browser.new_context(color_scheme="dark")
    page = context.new_page()
    try:
        page.goto(f"{site}/about.html", wait_until="load")
        assert page.locator(".bp-theme-menu [data-theme-choice]").count() == 2
        # Nothing chosen yet, and the machine says dark: the page is light
        # anyway, because that is the design it was drawn in.
        assert page.evaluate("document.documentElement.getAttribute('data-theme')") == "light"

        page.click(".bp-theme-menu .bp-menu-btn")
        page.click('.bp-theme-menu [data-theme-choice="dark"]')
        assert page.evaluate("document.documentElement.getAttribute('data-theme')") == "dark"
        # The key every other page on this site already reads, so one choice
        # holds across the hand-built pages too.
        assert page.evaluate("localStorage.getItem('belpulse-theme')") == "dark"

        page.click(".bp-theme-menu .bp-menu-btn")
        page.click('.bp-theme-menu [data-theme-choice="light"]')
        assert page.evaluate("document.documentElement.getAttribute('data-theme')") == "light"
        assert page.evaluate("localStorage.getItem('belpulse-theme')") == "light"

        # And it survives a reload -- which is the whole point of storing it,
        # and needs the pre-paint script (SHELL_BOOTSTRAP) in the head to
        # avoid a dark flash.
        page.reload(wait_until="load")
        assert page.evaluate("document.documentElement.getAttribute('data-theme')") == "light"
        checked = page.eval_on_selector_all(
            ".bp-theme-menu [data-theme-choice]",
            "els => els.filter(e => e.getAttribute('aria-checked') === 'true')"
            ".map(e => e.getAttribute('data-theme-choice'))",
        )
        assert checked == ["light"], "the control does not show what the reader chose"

        # The UI no longer offers Auto, but a reader's OLD choice of it is
        # still honoured -- resolved against the OS preference and upgraded
        # to an explicit value in storage, never applied as a fourth live
        # theme. Seeded directly since no control writes 'auto' any more.
        page.evaluate("localStorage.setItem('belpulse-theme', 'auto')")
        page.reload(wait_until="load")
        assert page.evaluate("document.documentElement.getAttribute('data-theme')") == "dark"
        assert page.evaluate("localStorage.getItem('belpulse-theme')") == "dark"
    finally:
        context.close()


def test_the_switcher_is_present_when_the_page_stands_alone(browser, site):
    """The other half: hidden only when framed. Opened directly, the page is
    the only place a reader can change language.

    UPDATED FOR BATCH A1.1: the switcher is no longer an always-visible row of
    three links -- it is the panel a language-menu button opens (the maquette's
    globe icon + current code). The three real links behind it are unchanged
    (`.bp-lang-switch` keeps its `data-lang`/`aria-current` contract, still
    pinned by tests/pages/test_trilingual_export.py), so it still works with
    scripting off; this test now opens the menu first, the one step a reader
    driving a mouse or a keyboard also has to take.
    """
    context = browser.new_context()
    page = context.new_page()
    try:
        page.goto(f"{site}/about.html", wait_until="load")
        assert page.locator(".bp-lang-menu .bp-menu-btn").is_visible()
        page.click(".bp-lang-menu .bp-menu-btn")
        assert page.locator(".bp-lang-switch").is_visible()
        assert page.locator(".bp-lang-switch a").count() == 3
    finally:
        context.close()


# --- the retired front-page shell redirects to the new homepage --------------


def test_the_real_front_page_redirects_to_home2(browser, site):
    """Bookmarks for /index.html land on the new canonical homepage."""
    context = browser.new_context(viewport={"width": 1280, "height": 900})
    page = context.new_page()
    try:
        page.goto(f"{site}/index.html", wait_until="networkidle")
        assert page.url.endswith("/home2.html")
        assert page.locator(".bp-topbar .bp-logo").is_visible()
    finally:
        context.close()
