"""A3.2: every commune selection opens commune.html?nis=.

Three layers, cheapest first:

  1. A static scan for the forbidden nis-destinations item 6 of the batch
     spec names -- profiles.html?nis= (the retired directory-embedded
     profile) and local.html?nis= (the JS app shell) -- across the six public
     pages plus the scripts that generate content for them. The only allowed
     nis-destinations are commune.html?nis= and the static local/{nis}/ SEO
     pages.
  2. A Node unit test of commune.html's `parseFrom`, the pure function that
     decides whether a breadcrumb `from=` query is safe to trust -- covering
     good and bad inputs, with no DOM and no browser.
  3. A Playwright check of the real round trip: profiles.html?nis= lands on
     commune.html?nis=, a quick-pick card click lands on commune.html, and
     commune.html's breadcrumb returns to the filtered directory it came
     from.
"""

from __future__ import annotations

import functools
import http.server
import json
import os
import re
import socketserver
import subprocess
import tempfile
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

#: The six-destination public journey plus the scripts that build content
#: for it (batch spec item 6). sources.html's own nav lives inside <header>,
#: owned by a parallel batch, and is out of scope here -- see the PR notes.
SCANNED_PAGES = [
    REPO / "home2.html",
    REPO / "profiles.html",
    REPO / "commune.html",
    REPO / "macro.html",
    REPO / "micro.html",
    REPO / "map.html",
    REPO / "sources.html",
]
SCANNED_SCRIPTS = [
    *sorted((REPO / "assets").glob("*.js")),
    *sorted((REPO / "src" / "pages").glob("*.py")),
    REPO / "scripts" / "export_local_pages.py",
]

FORBIDDEN = ("profiles.html?nis=", "local.html?nis=")


@pytest.mark.parametrize("path", SCANNED_PAGES + SCANNED_SCRIPTS, ids=lambda p: p.name)
def test_no_forbidden_commune_destination(path: Path):
    """The only nis-destinations left after A3.2: commune.html?nis= (every
    live page) and local/{nis}/ (the static SEO alternates, checked
    separately by tests/test_export_local_pages.py and
    tests/site/test_route_inventory.py)."""
    text = path.read_text(encoding="utf-8")
    found = [pattern for pattern in FORBIDDEN if pattern in text]
    assert not found, f"{path.relative_to(REPO)} still builds {found}"


def test_local_pages_open_the_interactive_profile_at_commune_html():
    """The generated local/{nis}/ pages' one link to the interactive profile,
    sampled rather than swept -- tests/test_export_local_pages.py and
    tests/site/test_route_inventory.py already cover the full 1,695 x 3."""
    local = REPO / "local"
    sample = sorted(p for p in local.glob("*/index.html"))[:5]
    assert sample, "no generated local/{nis}/ pages to sample"
    for page in sample:
        text = page.read_text(encoding="utf-8")
        assert 'class="interactive" href="../../commune.html?nis=' in text
        assert "local.html?nis=" not in text


# --- parseFrom, under Node, no DOM -------------------------------------------

COMMUNE_HTML = REPO / "commune.html"


def _extract_parse_from_js() -> str:
    text = COMMUNE_HTML.read_text(encoding="utf-8")
    start = text.index("--parseFromStart--")
    end = text.index("--parseFromEnd--", start)
    # The function's own closing brace sits between the two markers, on its
    # own line ("  }") right before the trailing comment -- back up to the
    # declaration and forward to that brace, so the harness gets a real
    # function and not the marker comments alone.
    fn_start = text.index("function parseFrom(", start)
    fn_end = text.rindex("\n  }", fn_start, end) + len("\n  }")
    return text[fn_start:fn_end]


def _run_node(js_body: str):
    harness = _extract_parse_from_js() + "\n" + js_body
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as handle:
        handle.write(harness)
        script = handle.name
    try:
        result = subprocess.run(
            ["node", script], capture_output=True, text=True, encoding="utf-8", timeout=10
        )
    finally:
        os.unlink(script)
    if result.returncode != 0:
        raise AssertionError(f"node failed:\n{result.stderr}")
    return json.loads(result.stdout)


def test_parse_from_extracts_cleanly():
    """The harness itself: a real, callable parseFrom, nothing else pulled in."""
    src = _extract_parse_from_js()
    assert src.startswith("function parseFrom(")
    assert src.rstrip().endswith("}")


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("family=urban_w", {"key": "family", "value": "urban_w"}),
        ("type=W14", {"key": "type", "value": "W14"}),
        ("province=be:prov:90000", {"key": "province", "value": "be:prov:90000"}),
        ("region=be:reg:04000", {"key": "region", "value": "be:reg:04000"}),
    ],
)
def test_parse_from_accepts_one_known_filter(raw, expected):
    assert _run_node(f"console.log(JSON.stringify(parseFrom({json.dumps(raw)})));") == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "nis=11001",  # not a directory filter key
        "foo=bar",  # key not in the allowlist
        "family=",  # empty value
        "family=urban_w&province=be:prov:90000",  # more than one pair
        "family=urban_w&evil=1",  # smuggled second key
        "__proto__=1",
    ],
)
def test_parse_from_rejects_everything_else(raw):
    assert _run_node(f"console.log(JSON.stringify(parseFrom({json.dumps(raw)})));") is None


def test_parse_from_never_returns_a_prototype_polluting_key():
    """type=__proto__ or family=__proto__ must not let a later
    `hasOwn(typ.clusters, value)` lookup see Object.prototype as a match --
    parseFrom itself stays permissive here (it does not know the real
    typology), but the shape it returns must be a plain, inert string pair."""
    result = _run_node("console.log(JSON.stringify(parseFrom('type=__proto__')));")
    assert result == {"key": "type", "value": "__proto__"}


def test_parse_from_marker_is_the_pure_no_dom_function():
    """A drift guard: parseFrom must stay DOM-free so this harness keeps
    working without a shim. If it ever needs `document` or `state`, this
    fails loudly instead of the Node test silently testing a stub."""
    src = _extract_parse_from_js()
    assert "document." not in src
    assert "state." not in src
    assert "window." not in src


# --- the real round trip, in a browser ---------------------------------------


@pytest.fixture(scope="module")
def site_server():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO))

    class Quiet(socketserver.TCPServer):
        allow_reuse_address = True

        def log_message(self, *args):  # pragma: no cover - silence the server
            pass

        def handle_error(self, *args):  # pragma: no cover - a closed socket is not a failure
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


@pytest.fixture
def page(browser_context):
    p = browser_context.new_page()
    try:
        yield p
    finally:
        p.close()


#: A commune with a published payload, used throughout map.html/profiles.html
#: tests elsewhere in this suite.
SAMPLE_NIS = "92094"


def test_profiles_nis_redirects_to_commune_html(site_server, page):
    page.goto(f"{site_server}/profiles.html?nis={SAMPLE_NIS}")
    page.wait_for_url(re.compile(r"commune\.html\?nis=" + SAMPLE_NIS))
    assert f"nis={SAMPLE_NIS}" in page.url
    assert "commune.html" in page.url
    assert "profiles.html" not in page.url


def test_a_quick_pick_card_opens_commune_html(site_server, page):
    page.goto(f"{site_server}/profiles.html")
    page.wait_for_selector("#quickGrid .ccard .clink")
    href = page.eval_on_selector("#quickGrid .ccard .clink", "el => el.getAttribute('href')")
    assert href is not None and href.startswith("commune.html?nis=")
    first_card_link = page.locator("#quickGrid .ccard .clink").first
    first_card_link.click()
    page.wait_for_url(re.compile(r"commune\.html\?nis=\d{5}"))


def test_breadcrumb_returns_to_the_filtered_directory(site_server, page):
    """A family view links to a commune with `from=`; commune.html revalidates
    it against real metadata and repoints its breadcrumb at that same filtered
    view rather than the bare directory."""
    page.goto(f"{site_server}/profiles.html")
    page.wait_for_selector("#families .family .fmore")
    family_href = page.eval_on_selector("#families .family .fmore", "el => el.getAttribute('href')")
    assert family_href and family_href.startswith("profiles.html?family=")

    page.goto(f"{site_server}/{family_href}")
    page.wait_for_selector("#listGrid .ccard .clink")
    card_href = page.eval_on_selector("#listGrid .ccard .clink", "el => el.getAttribute('href')")
    assert card_href and card_href.startswith("commune.html?nis=") and "&from=" in card_href

    page.goto(f"{site_server}/{card_href}")
    page.wait_for_function(
        "document.getElementById('crumbDirectory').getAttribute('href') !== 'profiles.html'"
    )
    back_href = page.eval_on_selector("#crumbDirectory", "el => el.getAttribute('href')")
    assert back_href == family_href.replace(f"{site_server}/", "")
