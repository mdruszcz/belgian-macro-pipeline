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

import bisect
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


def _two_regions_with_values(payload):
    year = payload["latest_year"]
    codes = [
        code
        for code, cell in sorted(payload["values"][year].items())
        if isinstance(cell["v"], (int, float))
    ]
    assert len(codes) >= 2, "need at least two regions with real values to test a multi-selection"
    return codes[0], codes[1]


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


# eurostat-map paints every region with its own default fill (#e1e1e1, read
# back as rgb(224, 224, 225)) on each rebuild and applies the class colour
# afterwards; a year change can rebuild more than once. A fill read during
# that window is the library's placeholder, not the classification.
LIBRARY_DEFAULT_FILL = "rgb(224, 224, 225)"


def _settled_class_fill(page, code, not_equal_to=None, timeout=15000):
    """The computed fill of region `code` once it is a real class colour --
    not the library default, not `not_equal_to` -- and unchanged across two
    consecutive animation frames. Waits on that condition, never on a timer."""
    page.wait_for_function(
        "(function(args){"
        "var el = document.getElementById('em-nutsrg-' + args[0]);"
        "if (!el) return false;"
        "var f = getComputedStyle(el).fill;"
        "if (f === args[1] || (args[2] && f === args[2])) return false;"
        "if (el.__bpLastFill !== f) { el.__bpLastFill = f; return false; }"
        "return true;"
        "})",
        arg=[code, LIBRARY_DEFAULT_FILL, not_equal_to],
        polling="raf",
        timeout=timeout,
    )
    return page.eval_on_selector(f"#em-nutsrg-{code}", "el => getComputedStyle(el).fill")


def test_switching_year_changes_a_known_regions_fill(browser, site):
    default_id = _first_loaded_indicator_id()
    payload = _payload(default_id)
    years = payload["years"]
    if len(years) < 2:
        pytest.skip("default indicator has fewer than 2 years, nothing to compare")
    code, _ = _a_region_with_a_value(payload)
    latest = payload["latest_year"]

    # A different year only changes the region's fill if it lands in a
    # DIFFERENT class band -- the page paints the payload's own precomputed
    # class_breaks (threshold classification), so two years whose values sit
    # in the same band paint the same colour, correctly. Pick a year where
    # the band genuinely differs (same number of breaks, so the band->colour
    # spread is comparable); skip if none. Before this, the test took the
    # first year with any value and relied on the band happening to differ,
    # which held for GDP per capita and stopped holding once a different
    # indicator became the region map's default (2026-09-15) -- and it read
    # the fill during the re-render, when the freshly rebuilt <path> carries
    # the library's default fill for an instant, so the "changed" it saw was
    # the rebuild, not the classification.
    def band(year):
        breaks = payload["class_breaks"].get(year) or []
        v = payload["values"].get(year, {}).get(code, {}).get("v")
        if v is None:
            return None
        return (len(breaks), bisect.bisect_right(breaks, v))

    band_latest = band(latest)
    other_year = next(
        (
            y
            for y in years
            if y != latest
            and band(y) is not None
            and band(y)[0] == band_latest[0]
            and band(y) != band_latest
        ),
        None,
    )
    if other_year is None:
        pytest.skip("no other year puts the same region in a different class band")
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector(f"#em-nutsrg-{code}", timeout=15000)
        _wait_for_map_ready(page)
        # Failed in CI on 2026-09-16 (PR #221, which touched only police data)
        # with fill_before == fill_after == rgb(224, 224, 225): that is the
        # vendored library's own default region fill (#e1e1e1), which every
        # <path> carries for an instant on each rebuild. Both reads had landed
        # inside a rebuild. Legend rows exist once onBuild has run, but the
        # class fill is applied to the paths after that, and a year change can
        # trigger more than one rebuild -- so "legend present" and "fill
        # differs from before" both pass momentarily and then lie. Read a fill
        # only once it is a real class colour AND stable across two frames.
        fill_before = _settled_class_fill(page, code)
        page.select_option("#europeYearSelect", other_year)
        # The map is rebuilt from scratch on a year change: wait for the
        # render pass to FINISH (legend rows are populated inside onBuild,
        # after every region is painted), not merely for a fill to flicker.
        page.wait_for_function(
            "() => !document.querySelector('#europeLegendScale .bp-europe-map__legend-row')"
            " || document.querySelectorAll('#bpEuropeStage svg').length === 1"
        )
        _wait_for_map_ready(page)
        fill_after = _settled_class_fill(page, code, not_equal_to=fill_before)
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
        page.click("#europeCountryPickerMenu summary")
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
        page.click("#europeCountryPickerMenu summary")
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
        page.wait_for_selector("#europe .bp-europe-compare__card canvas")
        legend_chip_count = page.eval_on_selector(
            "#europe .bp-europe-compare__legend",
            "el => el.querySelectorAll('.chip').length",
        )
        assert legend_chip_count == 8, legend_chip_count
    finally:
        context.close()


def test_default_belgium_selection_renders_seven_comparison_charts(browser, site):
    """2026-09-15 follow-up: "Comparaison internationale" is now mode-scoped
    (docs/features/europe_countries.md amendment) -- the 7 country cards
    only render in country mode, not the panel's default region mode, so
    this test switches mode first rather than asserting it loads with the
    panel regardless of mode (that used to be true; it deliberately is not
    any more)."""
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        _switch_to_country_mode(page)
        page.wait_for_selector("#europe .bp-europe-compare__card canvas", timeout=15000)
        count = page.eval_on_selector_all("#europe .bp-europe-compare__card", "els => els.length")
        # The card's indicator filter (2026-09-15) starts on the index's own
        # `compare_default` set -- the original seven -- not on every country
        # indicator (24 once the additional domains were wired in). Read the
        # expected number off the real index rather than hardcoding it.
        index_rows = _country_index()["indicators"]
        expected_default = len([i for i in index_rows if i.get("compare_default")])
        assert expected_default == 7
        assert (
            count == expected_default
        ), f"expected {expected_default} default comparison cards, got {count}"
        # Every indicator has a chip; "All" shows every card; unticking one
        # chip removes exactly that card.
        chip_count = page.eval_on_selector_all(
            "#europeCompareFilterList .bp-europe-compare__filter-chip", "els => els.length"
        )
        assert chip_count == len(index_rows)
        page.click("#europeCompareFilterMenu summary")
        page.click("#europeCompareFilterAll")
        page.wait_for_function(
            "(n) => document.querySelectorAll('#europe .bp-europe-compare__card').length === n",
            arg=len(index_rows),
        )
        first_chip = page.eval_on_selector(
            "#europeCompareFilterList .bp-europe-compare__filter-chip", "el => el.dataset.indicator"
        )
        page.uncheck(f'#europeCompareFilterList label[data-indicator="{first_chip}"] input')
        page.wait_for_function(
            "(n) => document.querySelectorAll('#europe .bp-europe-compare__card').length === n",
            arg=len(index_rows) - 1,
        )
        # data-code, not the (language-dependent) chip text -- the site's
        # default locale here is French ("Belgique"), not English.
        legend_codes = page.eval_on_selector_all(
            "#europe .bp-europe-compare__legend .chip", "els => els.map(e => e.dataset.code)"
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
    AND nuts2 payloads actually publish (docs/features/europe_countries.md
    -- the 2026-09-15 follow-up's region comparison card originally
    hardcoded its 3 NUTS2 ids directly in europe_map.js and was fixed to
    read state.index.indicators instead, exactly to keep this test green),
    so it cannot be satisfied by renaming a constant and it grows as
    indicators are added -- the same shape tests/test_macro.py's own
    test_macro_names_no_indicator_anywhere already uses for national.json."""
    ids = {i["id"] for i in _country_index()["indicators"]} | {
        i["id"] for i in _index()["indicators"]
    }
    assert ids, "no indicator ids to check against, so this test would prove nothing"
    js = (REPO_ROOT / "assets" / "belpulse" / "europe_map.js").read_text(encoding="utf-8")
    html = (REPO_ROOT / "macro.html").read_text(encoding="utf-8")
    named_js = sorted(i for i in ids if i in js)
    named_html = sorted(i for i in ids if i in html)
    assert not named_js, f"europe_map.js names indicators directly: {named_js}"
    assert not named_html, f"macro.html names indicators directly: {named_html}"


# --- 2026-09-15 follow-up (docs/features/europe_countries.md amendment) -----
# Maintainer-requested polish batch: Africa/Middle East filtering, the
# region <select>/picker grouped by country, region-mode comparison
# selection + charts, mode-switch-preserves-both-selections, the growth-
# rate toggle, no per-point markers on comparison lines, and the palette
# picker. One real-browser pass, not a large new suite (kept minimal per
# the maintainer's explicit "go fast" instruction for this batch).


def test_africa_middle_east_background_countries_are_hidden_licensed_ones_survive(browser, site):
    hidden_codes = [
        "LY",
        "EG",
        "IL",
        "PS",
        "JO",
        "LB",
        "SY",
        "SA",
        "KW",
        "IQ",
        "IR",
        "DZ",
        "TN",
        "EH",
        "MA",
    ]
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        hidden_selector = ",".join(f"#bpEuropeStage #em-cntrg-{c}" for c in hidden_codes)
        assert page.eval_on_selector_all(hidden_selector, "els => els.length") == 0
        # A non-denylisted background country (Russia) is untouched -- the
        # filter is scoped to Africa/Middle East, not a blanket removal.
        assert page.eval_on_selector_all("#bpEuropeStage #em-cntrg-RU", "els => els.length") == 1

        _switch_to_country_mode(page)
        hidden_selector_country = ",".join(
            f"#bpEuropeCountryStage #em-cntrg-{c}" for c in hidden_codes
        )
        assert page.eval_on_selector_all(hidden_selector_country, "els => els.length") == 0
        assert (
            page.eval_on_selector_all("#bpEuropeCountryStage #em-cntrg-RU", "els => els.length")
            == 1
        )
        # Turkey is licensed at the COUNTRY (NUTS 0) level -- em-nutsrg-TR
        # -- and must keep rendering regardless of the filter; region mode
        # has no bare "TR" id (its NUTS2 codes are TR10, TR21, ...), so this
        # check only makes sense here.
        assert (
            page.eval_on_selector_all(
                "#bpEuropeCountryStage path[id='em-nutsrg-TR']", "els => els.length"
            )
            == 1
        )
    finally:
        context.close()


def test_region_select_is_grouped_by_country_with_real_names(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        optgroup_count = page.eval_on_selector_all(
            "#europeRegionSelect optgroup", "els => els.length"
        )
        assert optgroup_count > 10, f"expected 30+ country groups, got {optgroup_count}"
        # Belgium's own group carries a real country name, not the bare
        # "BE" prefix -- proves country names loaded in time for the FIRST
        # render even though the panel opened straight into Région mode
        # (point 2's own requirement: loaded at init, not lazily).
        be_label = page.evaluate("""() => {
                const groups = Array.from(document.querySelectorAll('#europeRegionSelect optgroup'));
                const be = groups.find(g => Array.from(g.children).some(o => o.value.startsWith('BE')));
                return be ? be.label : null;
            }""")
        assert be_label and be_label != "BE", be_label
    finally:
        context.close()


def test_region_selection_renders_comparison_charts_and_survives_a_mode_switch(browser, site):
    default_id = _first_loaded_indicator_id()
    payload = _payload(default_id)
    code1, code2 = _two_regions_with_values(payload)
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        # No default region selection (unlike country mode's default
        # Belgium) -- lead review, 2026-09-15: no single Belgian region is
        # an obvious default.
        assert page.eval_on_selector("#europeCompareEmpty", "el => el.hidden") is False

        page.click(f"#em-nutsrg-{code1}")
        page.wait_for_selector(f'#em-nutsrg-{code1}[data-selected="true"]')
        page.click(f"#em-nutsrg-{code2}")
        page.wait_for_selector(f'#em-nutsrg-{code2}[data-selected="true"]')

        page.wait_for_selector("#europe .bp-europe-compare__card canvas", timeout=15000)
        card_count = page.eval_on_selector_all(
            "#europe .bp-europe-compare__card", "els => els.length"
        )
        # One card per NUTS2 indicator the store actually publishes -- not a
        # fixed number: the Eurostat additional domains batch
        # (docs/data_catalog.md, 2026-09-15) grew the nuts2 store from 3 to
        # 6 indicators (GDP_PC_PPS_NUTS2, POPULATION_NUTS2,
        # UNEMPLOYMENT_RATE_NUTS2, plus VALUE_ADDED_GROWTH_NUTS2,
        # EMPLOYMENT_RATE_NUTS2, HOUSEHOLD_INCOME_TOTAL_NUTS2), and
        # renderRegionComparisonCharts() renders one card per
        # state.index.indicators entry by design (CLAUDE.md rules 2/24 --
        # no hardcoded indicator count in this generic renderer). Read the
        # real, live count from the index the page itself just loaded
        # rather than hardcoding a number here too.
        expected_count = len(_index()["indicators"])
        assert (
            card_count == expected_count
        ), f"expected {expected_count} region-level comparison cards (one per nuts2 store indicator), got {card_count}"
        legend_codes = page.eval_on_selector_all(
            "#europe .bp-europe-compare__legend .chip", "els => els.map(e => e.dataset.code)"
        )
        assert code1 in legend_codes and code2 in legend_codes, legend_codes
        # No EU27/euro-area reference checkboxes in region mode -- they do
        # not exist for a NUTS2 payload (rule 26: a real "not applicable",
        # never faked).
        # The COMPUTED style, not just the `hidden` DOM property: a real
        # bug (an author CSS rule of equal specificity beating the UA
        # [hidden] default, same class as this file's own documented
        # .bp-europe-map__mode-section[hidden] fix) let `el.hidden` read
        # true while the checkboxes stayed visually on screen -- caught by
        # a real screenshot, not by the weaker property-only assertion
        # this replaced.
        assert (
            page.eval_on_selector(".bp-europe-compare__refs", "el => getComputedStyle(el).display")
            == "none"
        )

        # Switching to country mode and back clears neither selection.
        _switch_to_country_mode(page)
        page.click("#europeModeRegion")
        page.wait_for_function("document.getElementById('europeRegionWrap').hidden === false")
        still_selected = page.eval_on_selector(
            f"#em-nutsrg-{code1}", "el => el.getAttribute('data-selected')"
        )
        assert still_selected == "true"
        checkbox_checked = page.eval_on_selector(
            f'label[data-code="{code1}"] input', "el => el.checked"
        )
        assert checkbox_checked is True
        # Country mode's own default Belgium selection is untouched too.
        _switch_to_country_mode(page)
        be_checked = page.eval_on_selector('label[data-code="BE"] input', "el => el.checked")
        assert be_checked is True
    finally:
        context.close()


def test_growth_toggle_swaps_an_eligible_chart_to_percent_and_leaves_others_alone(browser, site):
    gdp = _country_payload("GDP_PC_PPS_COUNTRY")
    assert gdp["has_yoy"] is True
    unemployment = _country_payload("UNEMPLOYMENT_RATE_EUROPE")
    assert unemployment["has_yoy"] is False
    # The page picks its language from the BROWSER's locale when nothing is
    # stored (assets/i18n.js I18N.initial -> navigator.language): French on
    # the maintainer's machine, English on GitHub's en-US runner. Matching
    # a card by ONE language's title therefore passed locally and failed in
    # CI every time -- so a card is matched by any of its three names.
    gdp_names = list(gdp["names"].values())
    unemployment_names = list(unemployment["names"].values())

    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        _switch_to_country_mode(page)
        page.wait_for_selector("#europe .bp-europe-compare__card canvas", timeout=15000)

        find_card_js = """(names) => Array.from(document.querySelectorAll('#europe .bp-europe-compare__card'))
            .find(c => names.indexOf(c.querySelector('h4').textContent.trim()) !== -1)"""

        has_toggle = page.evaluate(
            f"(names) => {{ const c = ({find_card_js})(names); return !!(c && c.querySelector('.bp-europe-compare__growth')); }}",
            gdp_names,
        )
        assert has_toggle is True
        has_toggle_excluded = page.evaluate(
            f"(names) => {{ const c = ({find_card_js})(names); return !!(c && c.querySelector('.bp-europe-compare__growth')); }}",
            unemployment_names,
        )
        assert has_toggle_excluded is False

        def tooltip_value_for(names):
            return page.evaluate(
                f"""(names) => {{
                    const card = ({find_card_js})(names);
                    if (!card) return null;
                    const canvas = card.querySelector('canvas');
                    canvas.dispatchEvent(new KeyboardEvent('keydown', {{key: 'ArrowLeft'}}));
                    const tip = document.querySelector('.bp-chart-tip');
                    const valueEl = tip && tip.querySelector('.bp-chart-tip-value');
                    return valueEl ? valueEl.textContent : null;
                }}""",
                names,
            )

        level_text = tooltip_value_for(gdp_names)
        assert level_text and "%" not in level_text, level_text

        page.evaluate(
            f"(names) => {{ const c = ({find_card_js})(names); c.querySelector('.bp-europe-compare__growth input').click(); }}",
            gdp_names,
        )
        page.wait_for_timeout(150)
        growth_text = tooltip_value_for(gdp_names)
        assert growth_text and "%" in growth_text, growth_text
    finally:
        context.close()


def test_comparison_charts_draw_no_per_point_markers(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    context.add_init_script("""
        window.__arcCalls = {compare: 0, side: 0};
        const origArc = CanvasRenderingContext2D.prototype.arc;
        CanvasRenderingContext2D.prototype.arc = function(...args) {
            try {
                if (this.canvas.closest('.bp-europe-compare__card')) window.__arcCalls.compare++;
                else if (this.canvas.closest('.bp-europe-map__side')) window.__arcCalls.side++;
            } catch (e) {}
            return origArc.apply(this, args);
        };
        """)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        _switch_to_country_mode(page)
        page.wait_for_selector("#europe .bp-europe-compare__card canvas", timeout=15000)
        # A single-country detail chart (markers:true, unaffected by this
        # batch) still draws its usual per-point dots -- proves the spy
        # itself is wired correctly, not silently inert.
        default_id = _first_loaded_map_country_indicator_id()
        payload = _country_payload(default_id)
        code, _ = _a_country_with_a_value(payload)
        page.click(f"#em-nutsrg-{code}")
        page.wait_for_selector("#europeCountrySideCard canvas")
        counts = page.evaluate("window.__arcCalls")
        assert (
            counts["compare"] == 0
        ), f"comparison chart(s) drew {counts['compare']} point marker(s)"
        assert counts["side"] > 0, "sanity check failed: the detail chart drew no markers at all"
    finally:
        context.close()


def test_palette_picker_repaints_both_map_modes_with_its_own_storage_key(browser, site):
    context = browser.new_context(viewport=DESKTOP_VIEWPORT)
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        _switch_to_country_mode(page)
        page.wait_for_selector('#bpEuropeCountryStage svg path[id^="em-nutsrg-"]', timeout=15000)
        page.click("#europeModeRegion")
        page.wait_for_function("document.getElementById('europeRegionWrap').hidden === false")

        before = page.eval_on_selector(
            "#europeMapRoot", "el => getComputedStyle(el).getPropertyValue('--ramp-0').trim()"
        )
        page.select_option("#europePaletteSelect", "bluered")
        page.wait_for_timeout(200)
        after = page.eval_on_selector(
            "#europeMapRoot", "el => getComputedStyle(el).getPropertyValue('--ramp-0').trim()"
        )
        assert after != before, "palette change did not update the --ramp-0 token"
        stored = page.evaluate("() => localStorage.getItem('belpulse-europe-palette')")
        assert stored == "bluered"
        # A separate key from the commune map's own remembered palette
        # (map.html's 'belpulse-map-palette') -- deliberate, not a bug.
        commune_stored = page.evaluate("() => localStorage.getItem('belpulse-map-palette')")
        assert commune_stored is None
    finally:
        context.close()


# --- 2026-09-15 layout (docs/features/europe_countries.md, "Amendment
# 2026-09-15 (layout)"): map left, every control right, pickers as closed
# menus, legend and source inside the map, frozen header, pinned menu. ------


def _box(page, selector):
    box = page.locator(selector).first.bounding_box()
    assert box is not None, f"{selector} has no box"
    return box


def _inside(inner, outer, slack=1):
    return (
        inner["x"] >= outer["x"] - slack
        and inner["y"] >= outer["y"] - slack
        and inner["x"] + inner["width"] <= outer["x"] + outer["width"] + slack
        and inner["y"] + inner["height"] <= outer["y"] + outer["height"] + slack
    )


def test_map_sits_left_and_every_control_sits_in_the_rail_beside_it(browser, site):
    context = browser.new_context(viewport={"width": 1440, "height": 900})
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector("#europeLegendScale .bp-europe-map__legend-row", timeout=15000)
        mapcol = _box(page, ".bp-europe-map__mapcol")
        rail = _box(page, ".bp-europe-map__rail")
        assert rail["x"] >= mapcol["x"] + mapcol["width"], (mapcol, rail)
        assert abs(rail["y"] - mapcol["y"]) <= 2, (mapcol, rail)
        assert 0.55 < mapcol["width"] / (mapcol["width"] + rail["width"]) < 0.72
        # Nothing of the panel sits above the map: no breadcrumb, no search
        # box, no visible panel title.
        assert page.locator(".bp-breadcrumb").count() == 0
        assert page.locator("#communeSearch").count() == 0
        heading = _box(page, "#europe h2")
        assert heading["width"] <= 1 and heading["height"] <= 1
        for selector in (
            "#europeModeRegion",
            "#europePaletteSelect",
            "#europeIndicatorSelect",
            "#europeYearSelect",
            "#europeRegionPickerMenu summary",
            "#europeCompareFilterMenu summary",
        ):
            assert _inside(_box(page, selector), rail), selector
        # Both pickers are closed menus until clicked.
        assert page.eval_on_selector("#europeRegionPickerMenu", "el => el.open") is False
        assert page.eval_on_selector("#europeCompareFilterMenu", "el => el.open") is False
        page.click("#europeRegionPickerMenu summary")
        assert page.is_visible("#europeRegionPickerSearch")
        page.keyboard.press("Escape")
        assert page.eval_on_selector("#europeRegionPickerMenu", "el => el.open") is False
        # The legend is a small box in the map's bottom-left corner, the
        # source a short link in its bottom-right corner.
        legend = _box(page, "#europeRegionMapWrap .bp-europe-map__legend")
        assert _inside(legend, mapcol)
        assert legend["x"] - mapcol["x"] < 40
        assert (mapcol["y"] + mapcol["height"]) - (legend["y"] + legend["height"]) < 40
        assert legend["width"] < mapcol["width"] / 3
        assert _inside(_box(page, "#europeMeta a"), mapcol)
        source = _box(page, "#europeMeta")
        assert _inside(source, mapcol)
        assert (mapcol["x"] + mapcol["width"]) - (source["x"] + source["width"]) < 300
        assert "Eurostat" in page.inner_text("#europeMeta a")
        assert page.get_attribute("#europeMeta a", "href").startswith(
            "https://ec.europa.eu/eurostat/databrowser/view/"
        )
    finally:
        context.close()


def test_first_three_charts_sit_in_the_rail_and_the_rest_below_the_map(browser, site):
    context = browser.new_context(viewport={"width": 1440, "height": 900})
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        _switch_to_country_mode(page)
        page.wait_for_selector("#europeCompareTop .bp-europe-compare__card canvas", timeout=15000)
        expected = len([i for i in _country_index()["indicators"] if i.get("compare_default")])
        top = page.eval_on_selector_all(
            "#europeCompareTop .bp-europe-compare__card", "els => els.length"
        )
        below = page.eval_on_selector_all(
            "#international .bp-europe-compare__card", "els => els.length"
        )
        assert top == 3
        assert below == expected - 3
        mapcol = _box(page, ".bp-europe-map__mapcol")
        assert _box(page, "#international")["y"] >= mapcol["y"] + mapcol["height"]
        # A taller rail scrolls inside itself; it never stretches the page past
        # the footer (the charts' screen-reader text once did).
        code, _ = _a_country_with_a_value(
            _country_payload(_first_loaded_map_country_indicator_id())
        )
        page.click(f"#em-nutsrg-{code}")
        page.wait_for_selector("#europeCountrySideCard canvas")
        # Measured from the top: the click scrolls the map into view, and a
        # scrolled page can report its old height for a moment (seen in CI).
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(300)
        page_end = page.evaluate(
            "document.querySelector('.foot').getBoundingClientRect().bottom + window.scrollY"
        )
        assert page.evaluate("document.documentElement.scrollHeight") <= page_end + 1
    finally:
        context.close()


def test_header_stays_frozen_and_the_section_menu_stays_pinned_left(browser, site):
    context = browser.new_context(viewport={"width": 1440, "height": 900})
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector('#bpEuropeStage svg path[id^="em-nutsrg-"]', timeout=15000)
        # Opening on #europe does not tuck the panel under the frozen header.
        assert page.evaluate("window.scrollY") == 0
        _switch_to_country_mode(page)  # the charts below the map make it scroll
        page.wait_for_selector("#international .bp-europe-compare__card canvas", timeout=15000)
        for where in ("600", "document.documentElement.scrollHeight"):
            page.evaluate(f"window.scrollTo(0, {where})")
            page.wait_for_timeout(100)
            assert page.evaluate("window.scrollY") > 0, "page too short to prove anything"
            header = _box(page, ".bp-topbar")
            sidebar = _box(page, ".bp-sidebar")
            assert abs(header["y"]) <= 1, (where, header)
            assert sidebar["x"] == 0, (where, sidebar)
            assert abs(sidebar["y"] - header["height"]) <= 2, (where, header, sidebar)
            assert abs(sidebar["y"] + sidebar["height"] - 900) <= 2, (where, sidebar)
            footer = _box(page, ".foot")
            assert footer["x"] >= sidebar["x"] + sidebar["width"] - 1, (where, footer)
    finally:
        context.close()


def test_clicking_a_region_opens_a_closable_detail_card_in_the_rail(browser, site):
    default_id = _first_loaded_indicator_id()
    code, _ = _two_regions_with_values(_payload(default_id))
    context = browser.new_context(viewport={"width": 1440, "height": 900})
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector(f"#em-nutsrg-{code}", timeout=15000)
        assert page.eval_on_selector("#europeSideCard", "el => el.hidden") is True
        page.click(f"#em-nutsrg-{code}")
        page.wait_for_selector("#europeSideCard h3")
        assert _inside(_box(page, "#europeSideCard"), _box(page, ".bp-europe-map__rail"))
        page.click("#europeSideCard .bp-europe-map__side-close")
        assert page.eval_on_selector("#europeSideCard", "el => el.hidden") is True
    finally:
        context.close()


# Measured geometry of the drawn map, not a CSS string: eurostat-map inserts
# its own `div.em-map-wrapper` between the stage and the <svg> and sizes it to
# the library's nominal 760x790. Percentages on the <svg> resolve against THAT
# wrapper, so before the fix the wrapper -- not the stage -- decided the map's
# size: at 1870px it stayed content-sized (a 760px map in a 1016px stage), and
# at 1440px its width was capped while its height stayed aspect-driven, giving
# a 736x765 svg inside a 736x673 stage whose `overflow:hidden` cut 92px of
# southern Europe off the bottom. Asserting the RENDERED boxes is what catches
# that; asserting the rule text would not, since the rule was already there.
@pytest.mark.parametrize("width", [1870, 1440, 1122])
def test_the_whole_map_fits_inside_the_stage_and_fills_it(browser, site, width):
    context = browser.new_context(viewport={"width": width, "height": 900})
    page = context.new_page()
    try:
        page.goto(f"{site}/macro.html#europe", wait_until="load")
        page.wait_for_selector("#europeLegendScale .bp-europe-map__legend-row", timeout=15000)
        page.wait_for_selector(".bp-europe-map__stage svg .em-nutsrg path", timeout=15000)
        geom = page.evaluate("""() => {
              const stage = document.querySelector('.bp-europe-map__stage');
              const svg = stage.querySelector('svg');
              const s = stage.getBoundingClientRect();
              const v = svg.getBoundingClientRect();
              const r = svg.querySelector('.em-nutsrg').getBoundingClientRect();
              return {
                stage: {w: s.width, h: s.height},
                svg: {w: v.width, h: v.height},
                clipTop: s.top - r.top,
                clipBottom: r.bottom - s.bottom,
                clipLeft: s.left - r.left,
                clipRight: r.right - s.right,
                regionsH: r.height,
              };
            }""")
        # No edge of the drawn regions is outside the stage that clips them.
        for edge in ("clipTop", "clipBottom", "clipLeft", "clipRight"):
            assert geom[edge] <= 1, (edge, geom)
        # And the map is not merely uncropped by being tiny: the svg fills the
        # stage box, and the regions use most of the height available to them.
        assert abs(geom["svg"]["w"] - geom["stage"]["w"]) <= 2, geom
        assert abs(geom["svg"]["h"] - geom["stage"]["h"]) <= 2, geom
        assert geom["regionsH"] > geom["stage"]["h"] * 0.6, geom
    finally:
        context.close()
