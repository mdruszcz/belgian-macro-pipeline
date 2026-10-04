"""Browser contract for issue #309 PR 2 -- the live simulated public-finance
counters on macro.html (#finances-publiques) and home2.html (#financeStrip),
ADR docs/decisions/0016-simulated-live-counters.md.

Runs against Chromium via tests/conftest.py's shared `chromium` fixture (a
machine without Playwright/Chromium skips this whole module cleanly through
that fixture's own skip-guard), served over real HTTP the same way
tests/test_site_audit_fixes_browser.py does (these pages fetch() JSON
payloads that need a real origin).

The clock is frozen with Playwright's page.clock (install + pause_at), never
left to real wall-clock time: every value this file asserts is recomputed
independently in Python from the COMMITTED public/data/live_counters.json
(the exact formula live_counters.js itself uses, v0 + rate_per_ms*(t -
start_ms) -- see _value_at below, a deliberate restatement of
assets/belpulse/live_counters.js's own valueAt(), not a copy-paste, so a bug
shared by both would have to be the same mistake made twice independently),
never a hand-typed expected number (CLAUDE.md rule 36).
"""

from __future__ import annotations

import functools
import http.server
import json
import re
import socketserver
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
LIVE_COUNTERS_JSON = REPO / "public" / "data" / "live_counters.json"

# The one frozen instant most value-correctness assertions use: inside every
# counter's first (2026) segment (confirmed against the committed payload --
# population/revenue/spending/deficit start 2025-12-31T23:00Z, debt starts
# 2026-03-31T23:00Z, both well before this), matching the mockup's own round-4
# verification instant (LISEZMOI.md).
FROZEN = datetime(2026, 10, 3, 10, 0, 0, tzinfo=timezone.utc)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# --- a deliberate, independent restatement of live_counters.js's own pure
# segment math (never imported, never executed in-browser) --------------------


def _value_at(segments, t_ms):
    if not segments:
        return {"state": "not-started", "value": None, "rate_per_ms": None}
    first, last = segments[0], segments[-1]
    if t_ms < first["start_ms"]:
        return {"state": "not-started", "value": first["v0"], "rate_per_ms": first["rate_per_ms"]}
    if t_ms >= last["end_ms"]:
        return {"state": "expired", "value": last["v1"], "rate_per_ms": last["rate_per_ms"]}
    for s in segments:
        if s["start_ms"] <= t_ms < s["end_ms"]:
            return {
                "state": "running",
                "value": s["v0"] + s["rate_per_ms"] * (t_ms - s["start_ms"]),
                "rate_per_ms": s["rate_per_ms"],
            }
    for s in reversed(segments):
        if t_ms >= s["end_ms"]:
            return {"state": "expired", "value": s["v1"], "rate_per_ms": s["rate_per_ms"]}
    return {"state": "not-started", "value": first["v0"], "rate_per_ms": first["rate_per_ms"]}


@pytest.fixture(scope="module")
def payload():
    if not LIVE_COUNTERS_JSON.exists():
        pytest.skip("public/data/live_counters.json not built")
    return json.loads(LIVE_COUNTERS_JSON.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def counters_by_id(payload):
    return {c["id"]: c for c in payload["counters"]}


@pytest.fixture(scope="module")
def breakdown_parts_by_id(counters_by_id):
    out = {}
    for counter in counters_by_id.values():
        breakdown = counter.get("breakdown") or {}
        for part in breakdown.get("parts") or []:
            out[part["id"]] = part
    return out


@pytest.fixture(scope="module")
def site():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO))

    class Quiet(socketserver.ThreadingMixIn, http.server.HTTPServer):
        allow_reuse_address = True
        daemon_threads = True

        def log_message(self, *args):  # pragma: no cover
            pass

        def handle_error(self, *args):  # pragma: no cover
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


def _set_lang(context, lang="en"):
    context.add_init_script("try{localStorage.setItem('belpulse-lang','" + lang + "');}catch(e){}")


def _freeze(page, at: datetime):
    page.clock.install(time=at)
    page.clock.pause_at(at)


def _counter_values(page, region_selector: str) -> dict:
    """{counter_id: {state, raw}} for every counter row inside
    region_selector -- a plain strip/home-strip counter is a
    `.bp-live-counter[data-counter-id]` (buildCounterNode), a breakdown
    part is a `.bp-share-row[data-counter-id]` (mountBreakdown's own
    opts.buildNode) -- both carry data-counter-id and a `.bp-value`
    descendant, so matched here by `[data-counter-id]` alone rather than
    assuming the generic engine's own default node shape. A null raw means
    the counter currently carries no data-raw (not-started/unavailable) --
    a real, distinct state, never coerced to 0."""
    rows = page.eval_on_selector_all(
        region_selector + " [data-counter-id]",
        """els => els.map(el => ({
            id: el.dataset.counterId,
            state: el.dataset.counterState,
            raw: el.querySelector('.bp-value') ? el.querySelector('.bp-value').getAttribute('data-raw') : null,
        }))""",
    )
    return {row["id"]: row for row in rows}


def _now_ms(page) -> int:
    return page.evaluate("Date.now()")


# ============================= macro.html chapter ===========================


@pytest.mark.parametrize("width", [1440, 390], ids=["1440px", "390px"])
def test_macro_strip_values_match_python_computed_segments_at_frozen_time(
    chromium, site, counters_by_id, width
):
    """Issue #309 fix round 2: also run at 390px, not only desktop -- the
    regression this guards (the badge-visibility check rejecting a region
    the browser's own anchor-scroll had already scrolled past) only showed
    up at phone width, where macro.html#finances-publiques's own anchor
    scroll behaves differently against a shorter viewport."""
    context = chromium.new_context(viewport={"width": width, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/macro.html#finances-publiques")
        page.wait_for_selector('#finance-live-strip[data-state="ready"]', timeout=15000)
        rows = _counter_values(page, "#finance-live-strip")
        expected_ids = set(counters_by_id)
        assert set(rows) == expected_ids, f"strip counters mismatch: {sorted(rows)}"
        for counter_id, row in rows.items():
            expected = _value_at(counters_by_id[counter_id]["segments"], _ms(FROZEN))
            assert expected["state"] == "running", f"{counter_id}: test instant is not 'running'"
            assert row["state"] == "running", f"{counter_id}: page says {row['state']!r}"
            assert row["raw"] is not None, f"{counter_id}: no data-raw while running"
            assert float(row["raw"]) == pytest.approx(expected["value"], rel=1e-9), counter_id
    finally:
        context.close()


def test_macro_strip_values_after_run_for_5000ms_match_the_recomputed_segments(
    chromium, site, counters_by_id
):
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/macro.html#finances-publiques")
        page.wait_for_selector('#finance-live-strip[data-state="ready"]', timeout=15000)
        before = _counter_values(page, "#finance-live-strip")
        page.clock.run_for(5000)
        now = _now_ms(page)
        assert now == _ms(FROZEN) + 5000
        after = _counter_values(page, "#finance-live-strip")
        changed = 0
        for counter_id, row in after.items():
            expected = _value_at(counters_by_id[counter_id]["segments"], now)
            assert float(row["raw"]) == pytest.approx(expected["value"], rel=1e-9), counter_id
            if float(row["raw"]) != float(before[counter_id]["raw"]):
                changed += 1
        assert changed, "not one strip counter advanced after 5 real seconds of fake time"
    finally:
        context.close()


def test_macro_breakdown_parts_match_python_computed_segments(
    chromium, site, breakdown_parts_by_id
):
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/macro.html#finances-publiques")
        page.wait_for_selector('#finance-breakdown-revenue[data-state="ready"]', timeout=15000)
        page.wait_for_selector('#finance-breakdown-spending[data-state="ready"]', timeout=15000)
        checked = 0
        for region in ("#finance-breakdown-revenue", "#finance-breakdown-spending"):
            rows = _counter_values(page, region)
            assert rows, f"{region} mounted no part counters"
            for part_id, row in rows.items():
                part = breakdown_parts_by_id.get(part_id)
                assert part, f"{region}: {part_id} is not a real breakdown part in the payload"
                expected = _value_at(part["segments"], _ms(FROZEN))
                assert float(row["raw"]) == pytest.approx(expected["value"], rel=1e-9), part_id
                checked += 1
        assert checked >= 2, "too few breakdown parts checked for this to prove anything"
    finally:
        context.close()


@pytest.mark.parametrize("width", [1440, 390], ids=["1440px", "390px"])
def test_home2_strip_values_match_python_computed_segments_at_frozen_time(
    chromium, site, counters_by_id, width
):
    context = chromium.new_context(viewport={"width": width, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/home2.html")
        page.wait_for_selector("#financeStrip .bp-live-counter[data-counter-id]", timeout=15000)
        rows = _counter_values(page, "#financeStrip")
        assert set(rows) == set(counters_by_id), f"home strip mismatch: {sorted(rows)}"
        for counter_id, row in rows.items():
            expected = _value_at(counters_by_id[counter_id]["segments"], _ms(FROZEN))
            assert float(row["raw"]) == pytest.approx(expected["value"], rel=1e-9), counter_id
    finally:
        context.close()


# =============================== pause / resume ==============================


@pytest.mark.parametrize(
    "url_fragment,region_sel,pause_btn_id",
    [
        ("macro.html#finances-publiques", "#finance-live-strip", "finChapterPause"),
        ("home2.html", "#financeStrip", "financePause"),
    ],
)
def test_pause_freezes_values_and_resume_continues(
    chromium, site, counters_by_id, url_fragment, region_sel, pause_btn_id
):
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/{url_fragment}")
        page.wait_for_selector(region_sel + " .bp-live-counter[data-counter-id]", timeout=15000)
        btn = page.locator(f"#{pause_btn_id}")
        assert btn.get_attribute("aria-pressed") == "false"
        btn.click()
        assert btn.get_attribute("aria-pressed") == "true"
        frozen_rows = _counter_values(page, region_sel)
        page.clock.run_for(4000)
        still = _counter_values(page, region_sel)
        for counter_id, row in frozen_rows.items():
            assert still[counter_id]["raw"] == row["raw"], f"{counter_id} moved while paused"
        btn.click()
        assert btn.get_attribute("aria-pressed") == "false"
        page.clock.run_for(4000)
        now = _now_ms(page)
        resumed = _counter_values(page, region_sel)
        moved = 0
        for counter_id, row in resumed.items():
            expected = _value_at(counters_by_id[counter_id]["segments"], now)
            assert float(row["raw"]) == pytest.approx(expected["value"], rel=1e-9), counter_id
            if row["raw"] != still[counter_id]["raw"]:
                moved += 1
        assert moved, "resume did not actually restart any counter"
    finally:
        context.close()


@pytest.mark.parametrize(
    "url_fragment,region_sel",
    [
        ("macro.html#finances-publiques", "#finance-live-strip"),
        ("home2.html", "#financeStrip"),
    ],
)
def test_reduced_motion_starts_every_counter_paused(chromium, site, url_fragment, region_sel):
    context = chromium.new_context(
        viewport={"width": 1440, "height": 1000}, reduced_motion="reduce"
    )
    _set_lang(context, "en")
    page = context.new_page()
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/{url_fragment}")
        page.wait_for_selector(region_sel + " .bp-live-counter[data-counter-id]", timeout=15000)
        states = page.eval_on_selector_all(
            region_sel + " .bp-live-counter[data-counter-id]",
            "els => els.map(el => el.dataset.counterState)",
        )
        assert states, "no counters mounted"
        assert all(s == "paused" for s in states), states
    finally:
        context.close()


# ============================ year rollover / expiry =========================


def test_year_rollover_resets_flow_counters_and_since_opened_stays_non_negative(
    chromium, site, counters_by_id
):
    start = datetime(2026, 12, 31, 22, 59, 58, tzinfo=timezone.utc)
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        _freeze(page, start)
        page.goto(f"{site}/macro.html#finances-publiques")
        page.wait_for_selector('#finance-live-strip[data-state="ready"]', timeout=15000)
        page.clock.run_for(4000)
        now = _now_ms(page)
        assert (
            now == _ms(start) + 4000 == _ms(datetime(2026, 12, 31, 23, 0, 2, tzinfo=timezone.utc))
        )
        rows = _counter_values(page, "#finance-live-strip")
        for counter_id, row in rows.items():
            expected = _value_at(counters_by_id[counter_id]["segments"], now)
            assert float(row["raw"]) == pytest.approx(expected["value"], rel=1e-9), counter_id

        # A flow/difference counter's "since you opened this page" line must
        # never read negative across the reset, even though its own total
        # just dropped back toward 0 at the year boundary.
        since_text = page.eval_on_selector(
            '#finance-live-strip .bp-live-counter[data-counter-id="deficit"] .bp-live-counter-since',
            "el => el.textContent",
        )
        assert since_text is not None
        assert "-" not in since_text and "−" not in since_text, since_text

        # The YTD year label on a flow/difference counter must read the NEW
        # year, read via activeSegmentYear()'s own Brussels-calendar logic,
        # not "2026" held over from before the rollover.
        ytd_text = page.eval_on_selector(
            '#finance-live-strip .bp-live-counter[data-counter-id="deficit"] .bp-live-counter-ytd',
            "el => el ? el.textContent : null",
        )
        assert ytd_text and "2027" in ytd_text and "2026" not in ytd_text, ytd_text
    finally:
        context.close()


def test_macro_expired_state_shows_once_the_simulation_window_has_passed(chromium, site, payload):
    expired_at = datetime(2031, 1, 1, tzinfo=timezone.utc)
    assert _ms(expired_at) >= payload["valid_until_ms"], "2031 is not actually past this payload"
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        _freeze(page, expired_at)
        page.goto(f"{site}/macro.html#finances-publiques")
        page.wait_for_selector("#finance-live-strip[data-state]", timeout=15000)
        state = page.eval_on_selector("#finance-live-strip", "el => el.dataset.state")
        assert state == "unavailable", f"expired region should read 'unavailable', got {state!r}"
        assert (
            page.eval_on_selector_all("#finance-live-strip [data-counter-id]", "els => els.length")
            == 0
        ), "a counter mounted even though the whole payload has expired"
    finally:
        context.close()


def test_home2_expired_state_falls_back_honestly_with_no_badge_and_no_pause(
    chromium, site, payload
):
    """home2.html's HomeFinance module has no data-state attribute to poll
    (it is a fallback SENTENCE, not a card -- see renderFallback()); the
    observable contract is the same one the 404 scenario below checks:
    badge hidden, pause hidden, nothing mounted, and some non-empty fallback
    text written into #financeCompactText."""
    expired_at = datetime(2031, 1, 1, tzinfo=timezone.utc)
    assert _ms(expired_at) >= payload["valid_until_ms"], "2031 is not actually past this payload"
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        _freeze(page, expired_at)
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "document.getElementById('financeCompactText').textContent.trim().length > 0",
            timeout=15000,
        )
        assert page.eval_on_selector("#financeBadge", "el => el.hidden") is True
        assert page.eval_on_selector("#financePause", "el => el.hidden") is True
        assert (
            page.eval_on_selector_all("#financeStrip [data-counter-id]", "els => els.length") == 0
        )
    finally:
        context.close()


def test_expired_headline_reads_differently_from_a_genuinely_missing_payload(chromium, site):
    """The expired state and the missing-payload state must be two
    different messages (round-1 fix: both used to collapse into the same
    'Pas encore disponible' headline). Compares the ACTUAL rendered
    headline text between the two scenarios rather than a hand-typed
    expected string, so a future wording edit cannot silently make this
    test meaningless by both sides still matching each other."""
    expired_at = datetime(2031, 1, 1, tzinfo=timezone.utc)

    def _headline(route_block):
        context = chromium.new_context(viewport={"width": 1440, "height": 1000})
        _set_lang(context, "en")
        page = context.new_page()
        try:
            if route_block:
                page.route("**/live_counters.json", lambda r: r.fulfill(status=404, body=""))
            _freeze(page, expired_at)
            page.goto(f"{site}/macro.html#finances-publiques")
            page.wait_for_selector('#finance-live-strip[data-state="unavailable"]', timeout=15000)
            return page.eval_on_selector("#finStripHeadline", "el => el.textContent.trim()")
        finally:
            context.close()

    expired_headline = _headline(route_block=False)
    missing_headline = _headline(route_block=True)
    assert expired_headline and missing_headline
    assert expired_headline != missing_headline, (
        "expired and missing-payload render the SAME headline again -- "
        f"both say {expired_headline!r}"
    )


# ================================ fetch failure ===============================


def test_macro_payload_fetch_failure_falls_back_honestly_with_no_badge_and_no_pause(chromium, site):
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        page.route("**/live_counters.json", lambda route: route.fulfill(status=404, body=""))
        _freeze(page, FROZEN)
        page.goto(f"{site}/macro.html#finances-publiques")
        page.wait_for_selector('#finance-live-strip[data-state="unavailable"]', timeout=15000)
        assert (
            page.eval_on_selector_all("#finance-live-strip [data-counter-id]", "els => els.length")
            == 0
        ), "a counter mounted despite the payload fetch failing"
        assert (
            page.eval_on_selector("#finStripBadge", "el => el.hidden") is True
        ), "the simulation badge is visible with no simulation running"
        assert (
            page.eval_on_selector("#finChapterPause", "el => el.hidden") is True
        ), "the pause button is visible with nothing to pause"
    finally:
        context.close()


def test_home2_payload_fetch_failure_falls_back_honestly_with_no_badge_and_no_pause(chromium, site):
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        page.route("**/live_counters.json", lambda route: route.fulfill(status=404, body=""))
        _freeze(page, FROZEN)
        page.goto(f"{site}/home2.html")
        page.wait_for_function(
            "document.getElementById('financeCompactText').textContent.trim().length > 0",
            timeout=15000,
        )
        assert (
            page.eval_on_selector_all("#financeStrip [data-counter-id]", "els => els.length") == 0
        ), "a counter mounted despite the payload fetch failing"
        assert (
            page.eval_on_selector("#financeBadge", "el => el.hidden") is True
        ), "the simulation badge is visible with no simulation running"
        assert (
            page.eval_on_selector("#financePause", "el => el.hidden") is True
        ), "the pause button is visible with nothing to pause"
    finally:
        context.close()


# ================================ formatting ==================================


def test_french_values_use_a_non_breaking_thousands_separator(chromium, site, counters_by_id):
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "fr")
    page = context.new_page()
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/macro.html#finances-publiques")
        page.wait_for_selector('#finance-live-strip[data-state="ready"]', timeout=15000)
        texts = page.eval_on_selector_all(
            "#finance-live-strip [data-counter-id] .bp-value",
            "els => els.map(el => el.textContent)",
        )
        assert texts
        # 'debt'/'revenue'/'spending' are all well over 1000 at this instant
        # (committed payload), so at least one rendered value must group
        # thousands with the French NARROW NO-BREAK SPACE (U+202F) -- the
        # actual separator MapUI.formatValue uses, confirmed by printing the
        # live page's own rendered text's codepoints, not assumed -- never an
        # English-style comma.
        narrow_nbsp = chr(0x202F)
        assert any(narrow_nbsp in t for t in texts), [t.encode("unicode_escape") for t in texts]
        assert not any("," in t for t in texts), texts
    finally:
        context.close()


def test_french_official_figures_translate_the_pct_of_gdp_unit():
    """Regression for a real bug this batch's own mockup-fidelity check
    caught: the Official-figures cards/charts are the first thing on
    macro.html to ever display a 'pct_of_gdp' value, and macro.html's own
    unitHint() fell back to MapUI.unitSuffix(unit) with NO lang argument
    for any unit outside its small allowlist -- silently defaulting to
    English ('% of GDP') in every language. Checks the FIX directly in the
    source (not a rendered page, since this is a one-line argument-passing
    defect any future regression of the same shape would reproduce
    identically): the fallback must pass LANG through."""
    html = (REPO / "macro.html").read_text(encoding="utf-8")
    fn = re.search(r"function unitHint\(unit\)\{.*?\n  \}", html, re.DOTALL)
    assert fn, "unitHint() not found"
    assert "MapUI.unitSuffix(unit, LANG)" in fn.group(0), (
        "unitHint()'s fallback no longer passes LANG to MapUI.unitSuffix -- "
        "every non-allowlisted unit (pct_of_gdp among them) would silently "
        "render English regardless of the page's own language"
    )


def test_french_official_figures_render_pib_not_gdp(chromium, site):
    context = chromium.new_context(viewport={"width": 1440, "height": 1000})
    _set_lang(context, "fr")
    page = context.new_page()
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/macro.html#finances-publiques")
        page.wait_for_selector('#finance-official[data-state="ready"]', timeout=15000)
        texts = page.eval_on_selector_all(
            "#finOfficialGrid .bp-official-item .value, #finOfficialCharts .leadchart-eyebrow",
            "els => els.map(el => el.textContent)",
        )
        assert texts
        assert any(
            "PIB" in t for t in texts
        ), f"no French '% du PIB' rendered anywhere in the official-figures block: {texts}"
        assert not any(
            "GDP" in t for t in texts
        ), f"English 'GDP' leaked into the French page: {texts}"
    finally:
        context.close()


# ============================= layout / overflow ===============================


@pytest.mark.parametrize("url_fragment", ["macro.html#finances-publiques", "home2.html"])
def test_no_horizontal_overflow_at_390px_with_the_simulation_mounted(chromium, site, url_fragment):
    context = chromium.new_context(viewport={"width": 390, "height": 844})
    _set_lang(context, "fr")
    page = context.new_page()
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/{url_fragment}")
        page.wait_for_selector("[data-counter-id]", timeout=15000)
        overflow = page.evaluate(
            "document.documentElement.scrollWidth - document.documentElement.clientWidth"
        )
        assert overflow <= 1, f"{url_fragment} overflows its 390px viewport by {overflow}px"
        wrapped = page.eval_on_selector_all(
            "[data-counter-id] .bp-value",
            "els => els.filter(el => el.getClientRects().length > 1).map(el => el.textContent)",
        )
        assert not wrapped, f"value(s) wrapped onto a second line at 390px: {wrapped}"
    finally:
        context.close()


# ============================ behavioural guard ================================
#
# ADR 0016's policy, checked the way a reader would notice a violation: take
# a full snapshot of every non-blank text node on the page, advance the fake
# clock, and require that any node whose text actually changed sits inside a
# [data-simulated="true"] region that itself carries a visible
# .bp-simulated-badge. micro.html, which loads none of this, must show
# exactly zero changed nodes -- proving the snapshot technique itself is
# sensitive (it would catch a real violation), not merely silent.

_SNAPSHOT_JS = """
() => {
  function isTrulyVisible(el){
    if(!el || el.hidden) return false;
    const style = getComputedStyle(el);
    if(style.display === 'none' || style.visibility === 'hidden') return false;
    if(parseFloat(style.opacity) === 0) return false;
    const rect = el.getBoundingClientRect();
    if(rect.width <= 0 || rect.height <= 0) return false;
    if(rect.right <= 0 || rect.bottom <= 0) return false;
    return true;
  }
  // Snapshots LEAF ELEMENTS (el.children.length === 0 -- no element
  // children, so this element is the direct, lowest-level owner of
  // whatever text it shows), not raw text nodes. A first attempt walked
  // text nodes directly and broke on .bp-live-counter-since: that element
  // starts with `sinceEl.textContent = ''` (round-1 "never show a
  // misleading zero" rule), which leaves it with ZERO child text nodes,
  // not one empty one -- a later `sinceEl.textContent = '+123 ...'` then
  // CREATES a brand new text node rather than mutating an existing one,
  // which a text-node walker sees as the list growing by one entry (a
  // false "structural change") for every flow/difference counter, rather
  // than the ordinary content update it actually is. The ELEMENT itself
  // is never created or destroyed by a tick, only its .textContent -- so
  // leaf elements are the stable unit to diff by index.
  const leaves = Array.from(document.body.querySelectorAll('*')).filter(
    el => el.children.length === 0
  );
  return leaves.map(function(el){
    const region = el.closest('[data-simulated="true"]');
    let hasVisibleBadge = false;
    if(region){
      const badge = region.querySelector('.bp-simulated-badge');
      hasVisibleBadge = !!(badge && badge.textContent.trim() && isTrulyVisible(badge));
    }
    return {text: el.textContent, simulated: !!region, hasVisibleBadge: hasVisibleBadge};
  });
}
"""


@pytest.mark.parametrize("width", [1440, 390], ids=["1440px", "390px"])
@pytest.mark.parametrize(
    "url_fragment,expect_any_change",
    [
        ("macro.html#finances-publiques", True),
        ("home2.html", True),
        ("micro.html", False),
    ],
)
def test_behavioural_guard_changed_text_lives_only_in_a_badged_simulated_region(
    chromium, site, url_fragment, expect_any_change, width
):
    context = chromium.new_context(viewport={"width": width, "height": 1000})
    _set_lang(context, "en")
    page = context.new_page()
    try:
        # home2.html's hero video (assets/belpulse/home-film/*.mp4) is
        # unrelated to this guard and, once its JS sets a real <source src>,
        # keeps the network busy well past the point every panel (including
        # the finance strip) has settled -- blocked here so
        # wait_for_load_state("networkidle") below reflects the REST of the
        # page settling, not video buffering. Harmless on macro.html/
        # micro.html, neither of which loads a video at all.
        page.route("**/*.mp4", lambda route: route.abort())
        _freeze(page, FROZEN)
        page.goto(f"{site}/{url_fragment}")
        page.wait_for_load_state("networkidle", timeout=15000)
        # live_counters.js deliberately stops ticking a region that is
        # scrolled off-screen (IntersectionObserver -- its own file header
        # comment), to save CPU/battery on a counter nobody is looking at.
        # macro.html#finances-publiques auto-scrolls to its anchor on load,
        # so its strip is already in view; home2.html's #financeStrip sits
        # well below the hero (measured ~1550px down at this viewport) and
        # would otherwise never tick at all in this test -- correct product
        # behaviour, not something to defeat, but this guard is about WHERE
        # changed text may live, not about reproducing a reader's scroll
        # position, so it is scrolled into view here the same way a reader
        # checking on it would. A no-op where the id does not exist
        # (micro.html).
        page.evaluate(
            "document.getElementById('financeStrip') "
            "&& document.getElementById('financeStrip').scrollIntoView()"
        )
        before = page.evaluate(_SNAPSHOT_JS)
        page.clock.run_for(5000)
        after = page.evaluate(_SNAPSHOT_JS)
        assert len(before) == len(after), (
            f"{url_fragment}: text-node count changed ({len(before)} -> {len(after)}) -- "
            "structural change, not just a tick"
        )
        changed = 0
        violations = []
        for b, a in zip(before, after, strict=True):
            if b["text"] == a["text"]:
                continue
            changed += 1
            if not (a["simulated"] and a["hasVisibleBadge"]):
                violations.append((b["text"], a["text"]))
        assert not violations, f"{url_fragment}: text changed outside a badged region: {violations}"
        if expect_any_change:
            assert (
                changed
            ), f"{url_fragment}: nothing changed in 5s -- is the clock really advancing?"
        else:
            assert (
                changed == 0
            ), f"{url_fragment}: {changed} node(s) changed; this page simulates nothing"
    finally:
        context.close()


# ============== issue #309 fix round 2: boot-order safety ====================
#
# CI regression on PR #314: opening macro.html#europe at 390px threw an
# uncaught "BPLiveCounters.mount: container has no visible, non-empty
# .bp-simulated-badge" from inside FinancesPubliques.init() -- the finance
# chapter sits ABOVE #europe in the DOM, so the browser's own anchor-scroll
# lands past it before boot() ever mounts it, and isTrulyVisible() used to
# read that as "hidden" (see live_counters.js's own isTrulyVisible comment
# for the geometry). Because nothing caught the exception, every statement
# boot() had left to run -- including the Europe panel's own init a little
# further down the same synchronous chain -- silently never ran. Two
# independent fixes, tested together here: isTrulyVisible() no longer
# depends on scroll position (tests/pages/test_shared_components.py has the
# unit-level proof), and every BPLiveCounters.mount() call site now catches
# its own failure instead of letting it propagate.

EUROPE_SVG_SELECTOR = '#bpEuropeStage svg path[id^="em-nutsrg-"]'


@pytest.mark.parametrize("width", [390, 1440], ids=["390px", "1440px"])
@pytest.mark.parametrize("anchor", ["europe", "croissance", "finances-publiques"])
def test_macro_boots_cleanly_from_any_chapter_anchor(chromium, site, anchor, width):
    """Opening macro.html on ANY chapter's own anchor -- not just the one
    the reader is about to look at -- must mount the finance strip with no
    uncaught page error, at both a phone and a desktop width. #europe is
    the specific regression (its own map must also actually draw); the
    other two anchors are controls that were never broken, included so a
    future change that fixes #europe by special-casing it would still be
    caught here."""
    context = chromium.new_context(viewport={"width": width, "height": 844})
    _set_lang(context, "en")
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda exc: errors.append(str(exc)))
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/macro.html#{anchor}")
        page.wait_for_selector("#finance-live-strip[data-state]", timeout=15000)
        page.wait_for_function(
            "document.getElementById('finance-live-strip').dataset.state !== 'loading'",
            timeout=15000,
        )
        if anchor == "europe":
            page.wait_for_selector(EUROPE_SVG_SELECTOR, timeout=15000)
            count = page.eval_on_selector_all(EUROPE_SVG_SELECTOR, "els => els.length")
            assert count > 0, "opened on #europe but the Europe map drew no regions"
        assert not errors, f"uncaught page error(s) opening macro.html#{anchor}: {errors}"
        raws = page.eval_on_selector_all(
            "#finance-live-strip [data-counter-id] .bp-value",
            "els => els.map(e => e.getAttribute('data-raw'))",
        )
        assert raws and all(
            r is not None for r in raws
        ), f"finance strip did not mount its counters when opened on #{anchor}: {raws}"
    finally:
        context.close()


@pytest.mark.parametrize("width", [390, 1440], ids=["390px", "1440px"])
def test_home2_boots_cleanly_when_scrolled_to_the_bottom_before_mount(chromium, site, width):
    """Same class of regression as above, reproduced on home2.html: scroll
    to the very bottom of the document BEFORE the async payload fetch
    resolves (boot() is still awaiting it at this point), so #financeStrip
    mounts while it is nowhere near the viewport -- it must still mount
    successfully, with no uncaught page error."""
    context = chromium.new_context(viewport={"width": width, "height": 844})
    _set_lang(context, "en")
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda exc: errors.append(str(exc)))
    page.route("**/*.mp4", lambda route: route.abort())
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/home2.html")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_function(
            "document.getElementById('financeBadge') && "
            "(document.getElementById('financeBadge').hidden === false || "
            "document.getElementById('financeCompactText').textContent.trim().length > 0)",
            timeout=15000,
        )
        assert not errors, f"uncaught page error(s) on home2.html scrolled to bottom: {errors}"
        assert (
            page.eval_on_selector("#financeBadge", "el => el.hidden") is False
        ), "the strip fell back to unavailable instead of mounting -- regression, not just scroll"
        raws = page.eval_on_selector_all(
            "#financeStrip [data-counter-id] .bp-value",
            "els => els.map(e => e.getAttribute('data-raw'))",
        )
        assert raws and all(r is not None for r in raws), f"strip did not mount: {raws}"
    finally:
        context.close()


# A script that forces exactly the FIRST BPLiveCounters.mount() call to
# throw (on macro.html, that is mountStrip()'s own call), then lets every
# later call through untouched -- installed before live_counters.js runs by
# trapping the `window.BPLiveCounters =` assignment itself, so it is not a
# timing-dependent race against boot()'s own async fetch.
_FORCE_FIRST_MOUNT_FAILURE_INIT_SCRIPT = """
(function(){
  var real, failed = false;
  Object.defineProperty(window, 'BPLiveCounters', {
    configurable: true,
    get: function(){ return real; },
    set: function(v){
      if(v && typeof v.mount === 'function'){
        var originalMount = v.mount;
        v.mount = function(){
          if(!failed){ failed = true; throw new Error('forced mount failure (test)'); }
          return originalMount.apply(this, arguments);
        };
      }
      real = v;
    }
  });
})();
"""


def test_a_forced_mount_failure_falls_back_without_killing_the_rest_of_the_page(chromium, site):
    context = chromium.new_context(viewport={"width": 1440, "height": 900})
    _set_lang(context, "en")
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda exc: errors.append(str(exc)))
    page.add_init_script(_FORCE_FIRST_MOUNT_FAILURE_INIT_SCRIPT)
    try:
        _freeze(page, FROZEN)
        page.goto(f"{site}/macro.html#europe")
        page.wait_for_function(
            "document.getElementById('finance-live-strip').dataset.state !== 'loading'",
            timeout=15000,
        )
        page.wait_for_selector(EUROPE_SVG_SELECTOR, timeout=15000)
        assert not errors, f"the forced mount failure was not caught -- uncaught: {errors}"
        strip_state = page.eval_on_selector("#finance-live-strip", "el => el.dataset.state")
        assert (
            strip_state == "unavailable"
        ), f"the region whose mount() was forced to throw should read 'unavailable', got {strip_state!r}"
        europe_count = page.eval_on_selector_all(EUROPE_SVG_SELECTOR, "els => els.length")
        assert (
            europe_count > 0
        ), "the Europe map did not initialise after an earlier region's mount failed"
    finally:
        context.close()
