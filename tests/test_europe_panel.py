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
# Europe countries batch (docs/features/europe_countries.md): the "Régions /
# Pays" map toggle and the "Comparaison internationale" small multiples.
COUNTRIES_DIR = REPO_ROOT / "public" / "data" / "europe" / "countries"

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


def _country_index():
    return json.loads((COUNTRIES_DIR / "index.json").read_text(encoding="utf-8"))


def _country_payload(indicator_id: str):
    meta = next(i for i in _country_index()["indicators"] if i["id"] == indicator_id)
    return json.loads((COUNTRIES_DIR / meta["payload"]).read_text(encoding="utf-8"))


def _first_loaded_map_country_indicator_id() -> str:
    loaded = [i for i in _country_index()["indicators"] if i["status"] == "loaded" and i["map"]]
    assert loaded, "no loaded, map-eligible country indicator to test against"
    return loaded[0]["id"]


def _a_country_with_a_value(payload, exclude=("BE",)):
    period = payload["latest_period"]
    for code, cell in sorted(payload["values"][period].items()):
        if isinstance(cell["v"], (int, float)) and code not in exclude:
            return code, cell
    raise AssertionError(
        "no non-excluded country with a real value in the default payload's latest period"
    )


def _switch_to_country_mode(page):
    page.click("#europeModeCountry")
    page.wait_for_selector('#bpEuropeCountryStage svg path[id^="em-nutsrg-"]', timeout=15000)


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


# =============================================================================
# Europe countries batch (docs/features/europe_countries.md): the "Régions /
# Pays" map toggle, country click-selection, the picker, the 8-country cap,
# and the "Comparaison internationale" small multiples.
# =============================================================================


def test_mode_toggle_switches_between_region_and_country_maps(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        assert page.eval_on_selector("#europeRegionWrap", "el => el.hidden") is False
        _switch_to_country_mode(page)
        assert page.eval_on_selector("#europeRegionWrap", "el => el.hidden") is True
        assert page.eval_on_selector("#europeCountryWrap", "el => el.hidden") is False
        assert (
            page.eval_on_selector("#europeModeCountry", "el => el.getAttribute('aria-pressed')")
            == "true"
        )
        assert (
            page.eval_on_selector("#europeModeRegion", "el => el.getAttribute('aria-pressed')")
            == "false"
        )
        count = page.eval_on_selector_all(
            '#bpEuropeCountryStage svg path[id^="em-nutsrg-"]', "els => els.length"
        )
        assert count > 30, f"expected roughly 39 country outlines, got {count}"
        # Switching back shows region again, without re-fetching (no crash,
        # same wrapper survives hide/show).
        page.click("#europeModeRegion")
        page.wait_for_function("document.getElementById('europeRegionWrap').hidden === false")
        assert page.eval_on_selector("#europeCountryWrap", "el => el.hidden") is True
    finally:
        context.close()


def test_clicking_a_country_toggles_selection_and_outline(browser, site):
    default_id = _first_loaded_map_country_indicator_id()
    payload = _country_payload(default_id)
    code, _ = _a_country_with_a_value(payload)
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        _switch_to_country_mode(page)
        page.wait_for_selector(f"#em-nutsrg-{code}", timeout=15000)
        page.click(f"#em-nutsrg-{code}")
        page.wait_for_selector(f'#em-nutsrg-{code}[data-selected="true"]')
        checked = page.eval_on_selector(f'label[data-code="{code}"] input', "el => el.checked")
        assert checked is True
        # The side card (like region mode's) fills in for the clicked country.
        side_text = page.inner_text("#europeCountrySideCard")
        assert code in side_text
        # Clicking again deselects it.
        page.click(f"#em-nutsrg-{code}")
        page.wait_for_function(
            "(code) => { var el = document.getElementById('em-nutsrg-' + code); "
            "return el && !el.hasAttribute('data-selected'); }",
            arg=code,
        )
        checked_after = page.eval_on_selector(
            f'label[data-code="{code}"] input', "el => el.checked"
        )
        assert checked_after is False
    finally:
        context.close()


def test_country_picker_search_filters_by_code(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        _switch_to_country_mode(page)
        page.wait_for_selector("#europeCountryPickerList .bp-europe-picker__chip")
        page.fill("#europeCountryPickerSearch", "(DE)")
        page.wait_for_timeout(100)
        visible = page.eval_on_selector_all(
            "#europeCountryPickerList .bp-europe-picker__chip",
            "els => els.filter(e => !e.hidden).map(e => e.dataset.code)",
        )
        assert visible == ["DE"], visible
        page.fill("#europeCountryPickerSearch", "")
        page.wait_for_timeout(100)
        visible_after_clear = page.eval_on_selector_all(
            "#europeCountryPickerList .bp-europe-picker__chip",
            "els => els.filter(e => !e.hidden).length",
        )
        assert visible_after_clear > 30
    finally:
        context.close()


def test_selecting_more_than_eight_countries_hits_the_cap(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        _switch_to_country_mode(page)
        page.wait_for_selector("#europeCountryPickerList .bp-europe-picker__chip")
        codes = page.eval_on_selector_all(
            "#europeCountryPickerList .bp-europe-picker__chip",
            "els => els.map(e => e.dataset.code)",
        )
        # BE is selected by default (spec requirement) -- 7 more reaches the
        # cap of 8.
        extra = [c for c in codes if c != "BE"][:7]
        assert len(extra) == 7, "fewer than 7 other allowlisted countries to test the cap with"
        for code in extra:
            page.check(f'label[data-code="{code}"] input')
        page.wait_for_selector("#europeCountryCapMsg:not([hidden])")
        ninth = next(c for c in codes if c != "BE" and c not in extra)
        ninth_disabled = page.eval_on_selector(
            f'label[data-code="{ninth}"] input', "el => el.disabled"
        )
        assert ninth_disabled is True, "a 9th country's checkbox was not disabled at the cap"
        cap_text = page.inner_text("#europeCountryCapMsg")
        assert len(cap_text) > 5
        # And the comparison charts drew exactly 8 lines' worth of legend chips
        # per card, never more.
        page.wait_for_selector("#international .bp-europe-compare__card canvas")
        legend_chip_count = page.eval_on_selector(
            "#international .bp-europe-compare__legend",
            "el => el.querySelectorAll('.chip').length",
        )
        assert legend_chip_count == 8, legend_chip_count
    finally:
        context.close()


def test_default_belgium_selection_renders_seven_comparison_charts(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        # The comparison card loads with the panel, independent of which map
        # mode is active (default selection is Belgium alone).
        page.wait_for_selector("#international .bp-europe-compare__card canvas", timeout=15000)
        count = page.eval_on_selector_all(
            "#international .bp-europe-compare__card", "els => els.length"
        )
        assert count == 7, f"expected 7 comparison cards (one per country indicator), got {count}"
        # data-code, not the (language-dependent) chip text -- the site's
        # default locale here is French ("Belgique"), not English.
        legend_codes = page.eval_on_selector_all(
            "#international .bp-europe-compare__legend .chip", "els => els.map(e => e.dataset.code)"
        )
        assert "BE" in legend_codes, legend_codes
        empty_hidden = page.eval_on_selector("#europeCompareEmpty", "el => el.hidden")
        assert empty_hidden is True
    finally:
        context.close()


def test_country_mode_shows_the_unemployment_methodology_note(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        _switch_to_country_mode(page)
        note_text = page.inner_text("#europeUnemploymentNote")
        assert len(note_text) > 20
    finally:
        context.close()


def test_country_mode_requests_only_same_origin_or_fonts(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    urls = _request_urls(page)
    try:
        page.goto(f"{site}/macro.html", wait_until="load")
        page.click('.bp-sidebar-nav a[href="#europe"]')
        page.wait_for_selector("#europe:not([hidden])")
        _switch_to_country_mode(page)
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


def test_no_indicator_id_in_europe_map_js_or_macro_html():
    """Rule 2/24: an indicator id belongs in config/pages and payloads, never
    in the generic renderer. Checked against the real ids the countries
    payload actually publishes (docs/features/europe_countries.md), so it
    cannot be satisfied by renaming a constant and it grows as indicators
    are added -- the same shape tests/test_macro.py's own
    test_macro_names_no_indicator_anywhere already uses for national.json."""
    ids = {i["id"] for i in _country_index()["indicators"]}
    assert ids, "no country indicator ids to check against, so this test would prove nothing"
    js = (REPO_ROOT / "assets" / "belpulse" / "europe_map.js").read_text(encoding="utf-8")
    html = (REPO_ROOT / "macro.html").read_text(encoding="utf-8")
    named_js = sorted(i for i in ids if i in js)
    named_html = sorted(i for i in ids if i in html)
    assert not named_js, f"europe_map.js names indicators directly: {named_js}"
    assert not named_html, f"macro.html names indicators directly: {named_html}"
