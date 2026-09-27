"""Browser behaviour for comparables.html (Peer Model v1, Block M).

Follows the shared-server + shared-Chromium pattern
tests/test_commune_portrait_browser.py already uses (tests/conftest.py's
`chromium` fixture, one Chromium process for the whole session, a fresh
context per test).
"""

from __future__ import annotations

import functools
import http.server
import json
import re
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
PEERS_11002 = REPO / "public" / "data" / "peers" / "11002.json"
MOBILE_WIDTH = 390


class Quiet(socketserver.TCPServer):
    allow_reuse_address = True

    def log_message(self, *args):  # pragma: no cover - silence the server
        pass

    def handle_error(self, *args):  # pragma: no cover - a closed socket is not a failure
        pass


@pytest.fixture(scope="module")
def site():
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(REPO))
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


def _context(chromium, width=1200, height=900):
    return chromium.new_context(viewport={"width": width, "height": height}, locale="en-US")


def _real_errors(errors):
    # Same convention as test_commune_portrait_browser.py: a 404'd fetch for
    # data the page is expected to degrade gracefully from is not a bug.
    return [e for e in errors if "404" not in e]


@pytest.mark.skipif(
    not (REPO / "public" / "data" / "peers").is_dir(),
    reason="public/data/peers/ not built -- run scripts/export_peer_benchmarks.py first",
)
def test_92094_renders_ten_peers_and_the_table_with_zero_console_errors(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.goto(f"{site}/comparables.html?nis=92094", wait_until="load")
        page.wait_for_selector('#bp-main[data-page-state="ready"]')

        assert page.locator("#peerStrip .peer-chip").count() == 10
        assert page.locator("#tableBody tr").count() > 0
        assert not _real_errors(errors), f"console/page errors: {_real_errors(errors)}"
    finally:
        ctx.close()


@pytest.mark.skipif(
    not (REPO / "public" / "data" / "peers").is_dir(),
    reason="public/data/peers/ not built -- run scripts/export_peer_benchmarks.py first",
)
@pytest.mark.parametrize(
    "locale,pattern",
    [
        ("en-US", re.compile(r"^\d+(st|nd|rd|th) of \d+$")),
        ("fr-BE", re.compile(r"^\d+(er|e) sur \d+$")),
        ("nl-BE", re.compile(r"^\d+e van \d+$")),
    ],
)
def test_position_column_has_no_broken_ordinal_in_any_language(chromium, site, locale, pattern):
    """Browser finding (P1, blocker): the Position column used to render a
    literal '{ord}' placeholder in English and a doubled ordinal letter in
    French/Dutch (e.g. '4th{ord} of 11', '1ere sur 11', '3ee van 11') on
    every single row. Every Position cell must now match a clean
    '<n><suffix> of/sur/van <n>' shape, with no stray '{' and no doubled
    letter, in each of the three languages."""
    ctx = chromium.new_context(viewport={"width": 1200, "height": 900}, locale=locale)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/comparables.html?nis=92094", wait_until="load")
        page.wait_for_selector('#bp-main[data-page-state="ready"]')
        cells = page.locator("#tableBody td:last-child").all_inner_texts()
        assert cells, "no Position cells rendered -- fixture likely stale"
        bad = [c for c in cells if c != "—" and not pattern.match(c)]
        assert not bad, f"[{locale}] Position cells do not match {pattern.pattern}: {bad[:10]}"
        assert not any("{" in c for c in cells), f"[{locale}] stray placeholder: {cells[:10]}"
    finally:
        ctx.close()


@pytest.mark.skipif(
    not (REPO / "public" / "data" / "peers").is_dir(),
    reason="public/data/peers/ not built -- run scripts/export_peer_benchmarks.py first",
)
def test_units_with_a_known_shared_map_gap_render_without_leaking_or_duplicating(chromium, site):
    """Regression test for two real rendering bugs found while reviewing this
    page: eur_per_month (MUN_LEASE_CHARGES_MEDIAN_HOUSING) used to render as
    "€75 /mo. eur per month" (the unit duplicated after MapUI.formatValue
    already embedded it), and per_10000_dwellings
    (HOUSE_BURGLARIES_PER_10K) leaked its raw English unit code ("per 10000
    dwellings") even on the French/Dutch page, because
    assets/commune_map.js's shared unit vocabulary has no entry for either --
    the same two gaps commune.html's own local unitSuffix() already works
    around."""
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/comparables.html?nis=92094", wait_until="load")
        page.wait_for_selector('#bp-main[data-page-state="ready"]')
        page.locator("#scopeNational").click()
        page.wait_for_function(
            "() => document.getElementById('scopeNational').getAttribute('aria-pressed') === 'true'"
        )

        body_text = page.locator("#tableBody").inner_text()
        assert "mo. eur per month" not in body_text
        assert "per 10000 dwellings" not in body_text.lower()
        assert "per_10000" not in body_text.lower()
    finally:
        ctx.close()


@pytest.mark.skipif(
    not (REPO / "public" / "data" / "peers").is_dir(),
    reason="public/data/peers/ not built -- run scripts/export_peer_benchmarks.py first",
)
def test_default_scope_is_region_and_the_switch_changes_the_peer_list(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/comparables.html?nis=92094", wait_until="load")
        page.wait_for_selector('#bp-main[data-page-state="ready"]')

        # Same-region is the default (maintainer decision, 2026-09-27).
        assert page.locator("#scopeRegion").get_attribute("aria-pressed") == "true"
        assert page.locator("#scopeNational").get_attribute("aria-pressed") == "false"
        region_names = page.locator("#peerStrip .peer-chip .name").all_inner_texts()

        page.locator("#scopeNational").click()
        page.wait_for_function(
            "() => document.getElementById('scopeNational').getAttribute('aria-pressed') === 'true'"
        )
        national_names = page.locator("#peerStrip .peer-chip .name").all_inner_texts()

        assert page.locator("#scopeRegion").get_attribute("aria-pressed") == "false"
        assert len(national_names) == 10
        assert national_names != region_names, "switching scope did not change the peer list"
    finally:
        ctx.close()


@pytest.mark.skipif(
    not PEERS_11002.is_file(),
    reason="public/data/peers/11002.json not built -- run scripts/export_peer_benchmarks.py first",
)
def test_antwerpen_internal_migration_net_shows_an_em_dash_on_the_national_scope(chromium, site):
    """11002 (Antwerpen), INTERNAL_MIGRATION_NET, national list: a real entry
    with a negative peer median -- deviation_pct is null and the table must
    show an em dash with an explanatory tooltip, never a blank cell and never
    a percentage computed against a negative or zero median."""
    payload = json.loads(PEERS_11002.read_text(encoding="utf-8"))
    entry = payload["lists"]["national"]["INTERNAL_MIGRATION_NET"]
    assert (
        entry["deviation_pct"] is None and entry["deviation_withheld"] == "median_negative"
    ), "fixture assumption changed -- re-check public/data/peers/11002.json"
    indicators = json.loads(
        (REPO / "public" / "data" / "metadata" / "indicators.json").read_text(encoding="utf-8")
    )
    name_en = next(
        i["names"]["en"]
        for i in indicators["indicators"]
        if i["indicator_code"] == "INTERNAL_MIGRATION_NET"
    )

    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        # No ?lang= support on this page (same as commune.html) -- English is
        # selected via the context's locale, which I18N.initial() reads from
        # navigator.language when localStorage carries no saved choice.
        page.goto(f"{site}/comparables.html?nis=11002", wait_until="load")
        page.wait_for_selector('#bp-main[data-page-state="ready"]')
        page.locator("#scopeNational").click()
        page.wait_for_function(
            "() => document.getElementById('scopeNational').getAttribute('aria-pressed') === 'true'"
        )

        # Exact match, not has_text's substring match: "Net internal
        # migration" is itself a substring of the engine-only sibling
        # indicator's fallback name ("Net internal migration (per 1,000
        # residents)"), which this same payload/scope also carries.
        row = page.locator("#tableBody tr").filter(
            has=page.locator("td", has_text=re.compile(rf"^{re.escape(name_en)}\*?$"))
        )
        assert row.count() == 1, f"expected exactly one row for {name_en!r}"
        dev_cell = row.locator("td.dev-cell")
        assert dev_cell.inner_text().strip() == "—"
        assert dev_cell.locator("abbr").get_attribute("title")
    finally:
        ctx.close()


def test_a_missing_payload_shows_the_unavailable_state(chromium, site):
    """A NIS with no public/data/peers/<nis>.json (or none given at all) must
    show the unavailable message, never a blank page or a stuck spinner."""
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/comparables.html?nis=00000", wait_until="load")
        page.wait_for_selector('#bp-main[data-page-state="unavailable"]')
        assert page.locator("#cpUnavail").is_visible()
        assert not page.locator("#cpBody").is_visible()
    finally:
        ctx.close()


@pytest.mark.skipif(
    not (REPO / "public" / "data" / "peers").is_dir(),
    reason="public/data/peers/ not built -- run scripts/export_peer_benchmarks.py first",
)
def test_no_nis_at_all_shows_the_unavailable_state_but_still_offers_a_working_picker(
    chromium, site
):
    """Browser finding (P2, should-fix): landing on the bare URL (no ?nis)
    used to leave the commune picker empty -- it sits outside the hidden
    .cp-body, so a reader saw a visible but non-functional <select>. The
    unavailable state must still show, but the picker must now be populated
    and choosing a commune from it must leave the unavailable state."""
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/comparables.html", wait_until="load")
        page.wait_for_selector('#bp-main[data-page-state="unavailable"]')
        assert page.locator("#cpUnavail").is_visible()
        page.wait_for_function("document.getElementById('communeSelect').options.length > 0")
        options = page.locator("#communeSelect option")
        assert options.count() > 0
        page.select_option("#communeSelect", "92094")
        page.wait_for_selector('#bp-main[data-page-state="ready"]')
        assert page.locator("#peerStrip .peer-chip").count() == 10
    finally:
        ctx.close()


@pytest.mark.skipif(
    not (REPO / "public" / "data" / "peers").is_dir(),
    reason="public/data/peers/ not built -- run scripts/export_peer_benchmarks.py first",
)
def test_no_horizontal_overflow_at_390px(chromium, site):
    ctx = _context(chromium, width=MOBILE_WIDTH, height=800)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/comparables.html?nis=92094", wait_until="load")
        page.wait_for_selector('#bp-main[data-page-state="ready"]')
        scroll_width = page.evaluate("document.documentElement.scrollWidth")
        client_width = page.evaluate("document.documentElement.clientWidth")
        assert scroll_width <= client_width + 1, (
            f"horizontal overflow at 390px: scrollWidth={scroll_width} "
            f"clientWidth={client_width}"
        )
    finally:
        ctx.close()
