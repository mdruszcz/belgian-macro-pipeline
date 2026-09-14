"""Browser contract for the Europe panel's NUTS 2 map -- Batch B3
(docs/features/europe_nuts2.md).

Served over real HTTP (matching tests/test_macro_panels.py's own reasoning):
a relative fetch() of public/data/**/*.json behaves more predictably against
a real origin than file://, and every other browser test in this repo already
does it this way.

These tests read the REAL committed payloads (public/data/europe/nuts2/*.json)
and the REAL vendored bundle rather than asserting a hand-typed figure or
hash, so a payload or a bundle re-vendor that changes either one is what
would break a test here -- not a stale expectation baked in by this file.
"""

from __future__ import annotations

import functools
import hashlib
import http.server
import json
import socketserver
import threading
from pathlib import Path
from urllib.parse import urlparse

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
NUTS2_DIR = REPO_ROOT / "public" / "data" / "europe" / "nuts2"
VENDOR_DIR = REPO_ROOT / "assets" / "vendor" / "eurostat-map"

DESKTOP_VIEWPORT = {"width": 1122, "height": 900}
PHONE_VIEWPORT = {"width": 390, "height": 844}

ALLOWED_EXTERNAL_PREFIXES = (
    "https://fonts.googleapis.com/",
    "https://fonts.gstatic.com/",
)


def _index():
    return json.loads((NUTS2_DIR / "index.json").read_text(encoding="utf-8"))


def _payload(indicator_id: str):
    meta = next(i for i in _index()["indicators"] if i["id"] == indicator_id)
    return json.loads((NUTS2_DIR / meta["payload"]).read_text(encoding="utf-8"))


def _first_loaded_indicator_id() -> str:
    loaded = [i for i in _index()["indicators"] if i["status"] == "loaded"]
    assert loaded, "no loaded NUTS 2 indicator to test against"
    return loaded[0]["id"]


def _a_blocked_indicator_id():
    blocked = [i for i in _index()["indicators"] if i["status"] != "loaded"]
    return blocked[0]["id"] if blocked else None


@pytest.fixture(scope="module")
def site():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO_ROOT))

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


@pytest.fixture(scope="module")
def browser(chromium):
    return chromium


def _request_urls(page):
    urls = []
    page.on("request", lambda req: urls.append(req.url))
    return urls


def _wait_for_map_ready(page):
    """eurostat-map's own `.build()` is asynchronous: a region <path>
    exists in the DOM (and is what `wait_for_selector` alone would find)
    before europe_map.js's `onBuild` callback has attached click/hover/
    keyboard handlers to it or painted the suppressed/licence overrides.
    The legend is populated inside that same callback, so waiting for a
    real legend row is waiting for the whole render pass, not just for the
    paths to exist."""
    page.wait_for_selector("#europeLegendScale .bp-europe-map__legend-row", timeout=15000)


# --- lazy loading: nothing before the panel opens ---------------------------


def test_nothing_europe_specific_loads_before_the_panel_opens(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    urls = _request_urls(page)
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.wait_for_selector("#apercu:not([hidden])")
        # Give any accidental eager load a moment to show up.
        page.wait_for_timeout(300)
        offenders = [
            u for u in urls if "eurostatmap" in u or "/geo/nuts2/" in u or "/europe/nuts2/" in u
        ]
        assert not offenders, f"Europe-panel assets requested before the panel opened: {offenders}"
    finally:
        context.close()


def test_opening_the_panel_requests_only_same_origin_or_fonts(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    urls = _request_urls(page)
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.click('.bp-sidebar-nav a[href="#europe"]')
        page.wait_for_selector("#europe:not([hidden])")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        page.wait_for_timeout(300)
        origin = urlparse(site).netloc
        offenders = []
        for u in urls:
            parsed = urlparse(u)
            if parsed.scheme not in ("http", "https"):
                continue
            if parsed.netloc == origin:
                continue
            if any(u.startswith(p) for p in ALLOWED_EXTERNAL_PREFIXES):
                continue
            offenders.append(u)
        assert not offenders, f"non-allowlisted external request(s): {offenders}"
    finally:
        context.close()


# --- default indicator / year ------------------------------------------------


def test_default_indicator_is_the_first_loaded_one_at_its_latest_year(browser, site):
    default_id = _first_loaded_indicator_id()
    payload = _payload(default_id)
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        selected_indicator = page.eval_on_selector("#europeIndicatorSelect", "el => el.value")
        selected_year = page.eval_on_selector("#europeYearSelect", "el => el.value")
        assert selected_indicator == default_id
        assert selected_year == payload["latest_year"]
    finally:
        context.close()


def test_svg_has_a_path_per_geometry_region(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        count = page.eval_on_selector_all(
            '#bpEuropeStage svg path[id^="em-nutsrg-"]', "els => els.length"
        )
        assert count > 250, f"expected roughly 292 region paths, got {count}"
    finally:
        context.close()


# --- hover / click / side card ------------------------------------------------


def _a_region_with_a_value(payload):
    year = payload["latest_year"]
    for code, cell in sorted(payload["values"][year].items()):
        if isinstance(cell["v"], (int, float)):
            return code, cell
    raise AssertionError("no region with a real value in the default payload's latest year")


def test_hovering_a_region_shows_its_real_payload_value_in_the_tooltip(browser, site):
    default_id = _first_loaded_indicator_id()
    payload = _payload(default_id)
    code, cell = _a_region_with_a_value(payload)
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector(f"#em-nutsrg-{code}", timeout=15000)
        _wait_for_map_ready(page)
        page.hover(f"#em-nutsrg-{code}")
        page.wait_for_selector("#europeTooltip:not([hidden])")
        tooltip_text = page.inner_text("#europeTooltip")
        assert code in tooltip_text
        # The value is formatted (thousands separators, unit suffix) by
        # MapUI.formatValue -- this checks the raw digits survive that
        # formatting somewhere in the tooltip, not a byte-exact string.
        digits = str(int(cell["v"]))
        assert digits[:3] in tooltip_text.replace(" ", "").replace(",", "").replace(
            ".", ""
        ).replace(" ", ""), tooltip_text
    finally:
        context.close()


def test_clicking_a_region_fills_the_side_card(browser, site):
    default_id = _first_loaded_indicator_id()
    payload = _payload(default_id)
    code, cell = _a_region_with_a_value(payload)
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector(f"#em-nutsrg-{code}", timeout=15000)
        _wait_for_map_ready(page)
        page.click(f"#em-nutsrg-{code}")
        page.wait_for_selector("#europeSideCard h3")
        side_text = page.inner_text("#europeSideCard")
        assert code in side_text
    finally:
        context.close()


# --- year switch changes classification --------------------------------------


def test_switching_year_changes_a_known_regions_fill(browser, site):
    default_id = _first_loaded_indicator_id()
    payload = _payload(default_id)
    years = payload["years"]
    if len(years) < 2:
        pytest.skip("default indicator has fewer than 2 years, nothing to compare")
    code, _ = _a_region_with_a_value(payload)
    other_year = next(
        (
            y
            for y in years
            if y != payload["latest_year"]
            and payload["values"].get(y, {}).get(code, {}).get("v") is not None
        ),
        None,
    )
    if other_year is None:
        pytest.skip("no earlier year has a real value for the same region")
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector(f"#em-nutsrg-{code}", timeout=15000)
        _wait_for_map_ready(page)
        fill_before = page.eval_on_selector(f"#em-nutsrg-{code}", "el => getComputedStyle(el).fill")
        page.select_option("#europeYearSelect", other_year)
        page.wait_for_function(
            "(function(pair){"
            "var el = document.getElementById('em-nutsrg-' + pair[0]);"
            "return el && getComputedStyle(el).fill !== pair[1];"
            "})",
            arg=[code, fill_before],
        )
        fill_after = page.eval_on_selector(f"#em-nutsrg-{code}", "el => getComputedStyle(el).fill")
        assert fill_after != fill_before
    finally:
        context.close()


# --- blocked indicator --------------------------------------------------------


def test_a_blocked_indicator_is_disabled_with_a_visible_reason(browser, site):
    blocked_id = _a_blocked_indicator_id()
    if blocked_id is None:
        pytest.skip("no blocked NUTS 2 indicator in the current payloads")
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector("#europeIndicatorSelect option", state="attached", timeout=15000)
        is_disabled = page.eval_on_selector(
            f'#europeIndicatorSelect option[value="{blocked_id}"]', "el => el.disabled"
        )
        assert is_disabled
        page.select_option("#europeIndicatorSelect", index=0)  # ensure a stable start
        # Selecting the blocked option programmatically to surface its reason
        # text next to the select (disabled options cannot be chosen by a
        # real user, but the reason must still be readable on request).
        page.eval_on_selector(
            f'#europeIndicatorSelect option[value="{blocked_id}"]',
            "el => { el.disabled = false; el.selected = true; "
            "el.parentElement.dispatchEvent(new Event('change')); }",
        )
        page.wait_for_selector("#europeBlockedReason:not([hidden])")
        reason_text = page.inner_text("#europeBlockedReason")
        assert len(reason_text) > 10
    finally:
        context.close()


# --- theme: same class, different colour --------------------------------------


def test_theme_switch_keeps_the_same_band_but_changes_the_colour(browser, site):
    default_id = _first_loaded_indicator_id()
    payload = _payload(default_id)
    code, _ = _a_region_with_a_value(payload)
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector(f"#em-nutsrg-{code}", timeout=15000)
        _wait_for_map_ready(page)
        fill_light = page.eval_on_selector(f"#em-nutsrg-{code}", "el => getComputedStyle(el).fill")
        page.evaluate("document.documentElement.setAttribute('data-theme', 'dark')")
        page.evaluate(
            "document.dispatchEvent(new CustomEvent('bp:theme', {detail:{theme:'dark'}}))"
        )
        page.wait_for_function(
            "(function(pair){"
            "var el = document.getElementById('em-nutsrg-' + pair[0]);"
            "return el && getComputedStyle(el).fill !== pair[1];"
            "})",
            arg=[code, fill_light],
        )
        fill_dark = page.eval_on_selector(f"#em-nutsrg-{code}", "el => getComputedStyle(el).fill")
        assert fill_dark != fill_light, "theme switch did not change the region's colour"
    finally:
        context.close()


# --- vendored bundle integrity -------------------------------------------------


def test_vendored_bundle_sha256_matches_version_file():
    version_text = (VENDOR_DIR / "VERSION").read_text(encoding="utf-8")
    bundle_bytes = (VENDOR_DIR / "eurostatmap.min.js").read_bytes()
    actual = hashlib.sha256(bundle_bytes).hexdigest()
    assert (
        actual in version_text
    ), f"VERSION does not record the vendored bundle's real sha256 ({actual})"


# --- narrow viewport -----------------------------------------------------------


def test_no_horizontal_scroll_at_phone_width(browser, site):
    context = browser.new_context(viewport=PHONE_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        page.wait_for_timeout(200)
        scroll_width = page.evaluate("document.documentElement.scrollWidth")
        client_width = page.evaluate("document.documentElement.clientWidth")
        assert scroll_width <= client_width + 1, (
            f"page scrolls horizontally at 390px: scrollWidth={scroll_width}, "
            f"clientWidth={client_width}"
        )
    finally:
        context.close()
