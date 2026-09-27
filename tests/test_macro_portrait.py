"""Portrait layout must preserve real figures and usable macro panel navigation."""

import functools
import http.server
import threading
from pathlib import Path

import pytest
from playwright.sync_api import expect


@pytest.fixture(scope="module")
def portrait_site():
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(Path(__file__).resolve().parents[1])
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()
    thread.join()


@pytest.mark.parametrize(
    "width,lang,theme", [(1440, "fr", "dark"), (390, "nl", "light"), (768, "en", "paper")]
)
def test_portrait_retains_figures_and_chart_navigation(chromium, portrait_site, width, lang, theme):
    context = chromium.new_context(viewport={"width": width, "height": 900})
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.add_init_script(
        f"localStorage.setItem('belpulse-lang', '{lang}');"
        f"localStorage.setItem('belpulse-theme', '{theme}');"
    )
    try:
        page.goto(f"{portrait_site}/macro.html#apercu", wait_until="domcontentloaded")
        expect(page.locator(".macro-kpi[data-state=ready]")).to_have_count(6)
        expect(page.locator("#apercu #overview")).to_have_count(1)
        before = page.locator(".macro-kpi .num").all_text_contents()
        assert all(value.strip() for value in before)
        page.wait_for_function("document.documentElement.scrollWidth <= innerWidth")
        expect(page.locator(".portrait-hero h1")).to_be_visible()
        expect(page.locator("#sideUpdate")).to_be_visible()
        assert page.locator(".bp-sidebar").evaluate("e => getComputedStyle(e).position") == "sticky"
        page.locator('#sideNav a[href="#croissance"]').click()
        expect(page.locator("#croissance")).to_be_visible()
        expect(page.locator("#apercu")).to_be_hidden()
        expect(page.locator(".portrait-hero h1")).to_be_visible()
        page.wait_for_function(
            "document.getElementById('historyCanvas').getBoundingClientRect().width > 200"
        )
        page.locator('#sideNav a[href="#apercu"]').click()
        expect(page.locator("#apercu")).to_be_visible()
        assert page.locator(".macro-kpi .num").all_text_contents() == before
        assert not errors
    finally:
        context.close()
