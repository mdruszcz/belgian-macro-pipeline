"""Shared fixtures: ONE Chromium per test session, a fresh context per test.

Until 2026-09-12 every browser-backed test launched its own Playwright driver
and its own Chromium process and tore both down again -- 52 launches per run
across tests/builder/test_builder_shell_e2e.py,
tests/builder/test_builder_shell_service.py and tests/site/test_iframe_contract.py,
each behind a copy of the same `_chromium()` skip-guard. That, more than
anything the tests assert, is what made the full suite a 14-minute default
loop nobody wanted to run.

The process is started once here. ISOLATION IS NOT WEAKENED: each test still
gets its own `BrowserContext` (own cookies, own localStorage, own pages), and
a context is what a test can observe -- the Chromium process behind it is not.

The skip-guard keeps its three shapes, once. Not `importorskip`: that only
skips on ModuleNotFoundError, and a half-installed playwright raises a plain
ImportError from its own module body. A skip raised inside a session-scoped
fixture is cached by pytest for the whole session, so a machine without
Chromium skips every browser test cleanly rather than failing each in turn.

The `browser` marker is applied HERE, to any test whose fixture closure reaches
`chromium`, rather than by hand on each test: a marker somebody forgot is a
browser test in the fast tier, which is the one mistake the tiers exist to
prevent. `generated_site` is a module-level `pytestmark` in the files that
sweep the generated local/ and preview/ output, because that property is a
file's, not a fixture's.
"""

import pytest


def _launch_chromium():
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


@pytest.fixture(scope="session")
def chromium():
    """The one Chromium process of the session. Never yield a page from this
    directly -- go through `browser_context` (or a file's own wrapper that
    does the same), so every test starts from a clean context."""
    manager, browser = _launch_chromium()
    try:
        yield browser
    finally:
        browser.close()
        manager.stop()


#: The viewport every browser test has used since Batch 12: a desktop wide
#: enough for the builder's three-column shell to lay out as designed.
VIEWPORT = {"width": 1440, "height": 900}


@pytest.fixture
def browser_context(chromium):
    """A fresh context for one test, closed afterwards. Closing it also closes
    its keep-alive sockets -- which matters against the single-threaded
    builder service (src/builder/service.py:30): a socket left open by the
    previous test would block the next test's first request for the
    handler's 2-second timeout."""
    context = chromium.new_context(viewport=VIEWPORT)
    try:
        yield context
    finally:
        context.close()


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "chromium" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.browser)
