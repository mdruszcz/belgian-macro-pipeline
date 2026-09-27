"""Browser behaviour for the comparable-communes layer (PR C,
docs/features/peer_model.md / docs/features/commune_portrait.md).

Follows the shared-server + Playwright pattern of
tests/test_commune_portrait_browser.py. Every figure asserted below is read
straight from the committed public/data/peers/<nis>.json and
public/data/communes/<nis>.json fixtures on disk (never hand-typed against
the spec) -- checked live with a throwaway script before this file was
written, exactly as the numbers in docs/features/peer_model.md's own test
list were derived.

U+202F (narrow no-break space) and U+00A0 (no-break space), which fmt()
inserts before a unit in fr/nl, are normalised to a plain space before any
text comparison, matching the spec's own instruction.
"""

from __future__ import annotations

import functools
import http.server
import re
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]


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


def _context(chromium, width=1122, height=900, locale="en-US"):
    return chromium.new_context(viewport={"width": width, "height": height}, locale=locale)


def _real_errors(errors):
    return [e for e in errors if "404" not in e]


def _norm(text):
    """Collapse U+202F/U+00A0 to a plain space, per the spec's own rule."""
    return re.sub("[  ]", " ", text or "")


def _chapter_for_code(page, code, select=True):
    """The <section class="chapter"> ancestor of the tile with this code --
    scoping a toggle click or a badge read to the SAME chapter, since every
    non-additive chapter on the page carries its own independent switch.
    `select` clicks the tile so this code becomes the chapter's LEAD chart
    (and hence the one .sim-badge that chapter shows); the badge otherwise
    belongs to whichever indicator happened to be the default headline."""
    tile = page.locator(f'.multiple-tile[data-code="{code}"]').first
    tile.scroll_into_view_if_needed()
    if select:
        tile.click()
    return tile, tile.locator("xpath=ancestor::section[contains(@class,'chapter')]")


def _open_income_chapter(page, nis="92094"):
    tile, chapter = _chapter_for_code(page, "AVG_NET_TAXABLE_INCOME")
    return tile, chapter


# ---------------------------------------------------------------------------
# 1-2: 92094 income, default list, then switch to national
# ---------------------------------------------------------------------------


def test_92094_income_default_region_list_then_switch_to_national(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        tile, chapter = _open_income_chapter(page)
        page.wait_for_timeout(300)

        # Default list is "same region" (maintainer decision, 2026-09-27).
        region_radio = chapter.locator('.sim-toggle input[value="region"]').first
        assert region_radio.is_checked()

        badge = chapter.locator(".sim-badge").first
        badge.wait_for(state="visible")
        line1 = _norm(badge.locator(".l1").first.text_content())
        assert line1 == "2.6% above the median of comparable communes (same region)"
        line2 = _norm(badge.locator(".l2").first.text_content())
        assert "Median €36,794" in line2
        assert "Namur: 5th highest value of 11" in line2
        assert "2023" in line2
        assert chapter.locator(".sim-badge .note", has_text="chosen partly").count() == 1
        assert chapter.locator(".sim-badge .note", has_text="Relative difference").count() == 0

        # The lead chart's own svgWrap for this chapter (there is exactly
        # one lead per chapter).
        svgwrap = chapter.locator(".leadchart-svgwrap").first
        svgwrap.scroll_into_view_if_needed()
        page.wait_for_timeout(2000)

        lines = svgwrap.locator("path.sim-line")
        assert lines.count() == 10
        nis_set = {lines.nth(i).get_attribute("data-nis") for i in range(10)}
        assert nis_set == {
            "53053",
            "57081",
            "51004",
            "25072",
            "25121",
            "61031",
            "62108",
            "62003",
            "62079",
            "64074",
        }
        for i in range(10):
            first = lines.nth(i).get_attribute("data-first")
            last = lines.nth(i).get_attribute("data-last")
            assert first == "2005", f"peer {i} starts at {first}, not 2005"
            assert last == "2023", f"peer {i} ends at {last}, not 2023"

        legend_text = _norm(chapter.locator(".sim-legend").first.text_content())
        assert "10 comparable communes (same region)" in legend_text

        tile_text = _norm(tile.locator(".mtile-sim").text_content())
        assert tile_text.startswith("2.6% above the median of comparable communes")
        assert "*" in tile_text

        # 2: switch to "All of Belgium".
        before_requests = []
        page.on("request", lambda req: before_requests.append(req.url))
        chapter.locator(".sim-toggle label", has_text="All of Belgium").first.click()
        page.wait_for_timeout(2000)

        national_radio = chapter.locator('.sim-toggle input[value="national"]').first
        assert national_radio.is_checked()

        line1b = _norm(badge.locator(".l1").first.text_content())
        assert line1b == "7.7% below the median of comparable communes (all of Belgium)"
        line2b = _norm(badge.locator(".l2").first.text_content())
        assert "Median €40,886" in line2b
        assert "8th highest value of 11" in line2b

        new_requests = [
            u
            for u in before_requests
            if re.search(r"/public/data/communes/(31005|12021|71072|24107)\.json$", u)
        ]
        assert len(set(new_requests)) == 4

        stored = page.evaluate("() => window.localStorage.getItem('belpulse-similar')")
        assert stored == "national"

        which = chapter.locator(".sim-which").first
        which.locator("summary").click()
        page.wait_for_timeout(1500)
        which_text = _norm(which.text_content())
        assert "Bruges" in which_text
    finally:
        ctx.close()


# ---------------------------------------------------------------------------
# 3: 92094 municipal debt -- region ok, national few_peers
# ---------------------------------------------------------------------------


def test_92094_debt_region_ok_national_few_peers(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        tile, chapter = _chapter_for_code(page, "MUN_DEBT_TOTAL_PER_CAPITA")
        page.wait_for_timeout(500)

        badge = chapter.locator(".sim-badge").first
        badge.wait_for(state="visible")
        line1 = _norm(badge.locator(".l1").first.text_content())
        assert line1 == "48.4% above the median of comparable communes (same region)"
        line2 = _norm(badge.locator(".l2").first.text_content())
        assert "€1,997.8" in line2 or "1,997.8" in line2
        assert "3rd highest value of 11" in line2

        which = chapter.locator(".sim-which").first
        which.locator("summary").click()
        page.wait_for_timeout(1500)
        which_text = _norm(which.text_content())
        assert "Mons" in which_text
        assert "4,096.8" in which_text

        chapter.locator(".sim-toggle label", has_text="All of Belgium").first.click()
        page.wait_for_timeout(1000)
        line1b = _norm(badge.locator(".l1").first.text_content())
        assert "No comparison for 2024" in line1b
        assert "only 6 of the 10 comparable communes have this figure" in line1b
        assert "at least 7 are needed" in line1b

        # MUN_DEBT_TOTAL_PER_CAPITA is <=2 periods (bar mode), so there is
        # no line chart and no data-nis grey line to check here; the
        # national list still lists all 10 members (rank order, not
        # filtered to those with a value -- the withheld state applies to
        # the BADGE, not to which communes are listed).
        which2 = chapter.locator(".sim-which").first
        if not which2.locator("ol").is_visible():
            which2.locator("summary").click()
            page.wait_for_timeout(500)
        assert which2.locator("ol li").count() == 10

        try_btn = chapter.locator(".sim-badge .tryother", has_text="same region")
        assert try_btn.count() == 1
    finally:
        ctx.close()


# ---------------------------------------------------------------------------
# 9: POPULATION_CHANGE_5Y -- peer_deviation none (A4)
# ---------------------------------------------------------------------------


def test_92094_population_change_5y_has_no_pct_line(chromium, site):
    """Needs PR A's exporter change (A4: peer_deviation: none written into
    metadata/indicators.json), which has not run against this repo's
    committed public/data/** fixtures yet -- routed here so this test
    exercises commune.html's OWN handling of that flag (rule 24: it reads
    meta.peer_deviation, never an indicator id) rather than waiting for a
    daily run neither PR A nor this PR controls."""
    ctx = _context(chromium)
    try:
        page = ctx.new_page()

        def _inject_peer_deviation(route):
            response = route.fetch()
            payload = response.json()
            for row in payload["indicators"]:
                if row["indicator_code"] == "POPULATION_CHANGE_5Y":
                    row["peer_deviation"] = "none"
            route.fulfill(response=response, json=payload)

        page.route("**/public/data/metadata/indicators.json", _inject_peer_deviation)
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        tile, chapter = _chapter_for_code(page, "POPULATION_CHANGE_5Y")
        page.wait_for_timeout(500)

        badge = chapter.locator(".sim-badge").first
        badge.wait_for(state="visible")
        # No above/below line at all -- peer_deviation: none means the
        # metadata carries "peer_deviation":"none" (A4) and benchmarkState
        # resolves to 'no_pct_config', which has no l1 line, only l2 +
        # the config note.
        assert badge.locator(".l1").count() == 0
        config_note = _norm(badge.text_content())
        assert "A % difference is not shown for this figure." in config_note
        assert "Median 2.19" in config_note or "Median 2,19" in config_note
        assert "highest value of 11" in config_note
        assert "chosen partly on this figure" in config_note
    finally:
        ctx.close()


# ---------------------------------------------------------------------------
# 10: Hide
# ---------------------------------------------------------------------------


def test_hide_removes_everything_and_persists(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        page.wait_for_timeout(500)
        _, chapter = _chapter_for_code(page, "AVG_NET_TAXABLE_INCOME")
        chapter.locator(".sim-toggle label", has_text="Hide").first.click()
        page.wait_for_timeout(500)

        assert page.locator("path.sim-line").count() == 0
        assert page.locator(".sim-badge").count() == 0
        assert page.locator(".mtile-sim").count() == 0
        assert page.locator(".sim-legend").count() == 0
        assert page.locator(".sim-which").count() == 0

        stored = page.evaluate("() => window.localStorage.getItem('belpulse-similar')")
        assert stored == "off"

        page.reload(wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        page.wait_for_timeout(500)
        assert page.locator(".sim-badge").count() == 0
        assert page.locator(".sim-toggle input[value='off']").first.is_checked()

        peer_requests = []
        page.on("request", lambda req: peer_requests.append(req.url))
        page.wait_for_timeout(1000)
        other_commune_requests = [
            u
            for u in peer_requests
            if re.search(r"/public/data/communes/\d+\.json$", u) and "92094" not in u
        ]
        assert not other_commune_requests
    finally:
        ctx.close()


# ---------------------------------------------------------------------------
# 12: 404
# ---------------------------------------------------------------------------


def test_peers_404_disables_the_whole_layer(chromium, site):
    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        page.on("pageerror", lambda exc: errors.append(str(exc)))
        page.route(
            "**/public/data/peers/92094.json",
            lambda route: route.fulfill(status=404, body="not found"),
        )
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        page.wait_for_timeout(1000)

        assert page.locator(".sim-toggle").count() == 0
        assert page.locator(".sim-badge").count() == 0
        assert page.locator(".mtile-sim").count() == 0
        assert not _real_errors(errors), f"unexpected console errors: {_real_errors(errors)}"
    finally:
        ctx.close()


# ---------------------------------------------------------------------------
# 17: fr wording
# ---------------------------------------------------------------------------


def test_french_badge_wording(chromium, site):
    ctx = _context(chromium, locale="fr-BE")
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.evaluate("() => window.localStorage.setItem('belpulse-lang', 'fr')")
        page.reload(wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        tile, chapter = _open_income_chapter(page)
        page.wait_for_timeout(500)
        badge = chapter.locator(".sim-badge").first
        badge.wait_for(state="visible")
        line1 = _norm(badge.locator(".l1").first.text_content())
        assert line1 == "2,6 % au-dessus de la médiane des communes comparables (même région)"
    finally:
        ctx.close()


# ---------------------------------------------------------------------------
# 18: no horizontal overflow at 390px
# ---------------------------------------------------------------------------


def test_no_overflow_at_390px(chromium, site):
    ctx = _context(chromium, width=390, height=800)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        _, chapter = _open_income_chapter(page)
        page.wait_for_timeout(500)
        overflow = page.evaluate(
            "() => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1"
        )
        assert not overflow, "horizontal scroll appeared at 390px"
        toggle_height = chapter.locator(".sim-toggle label").first.evaluate(
            "el => el.getBoundingClientRect().height"
        )
        assert toggle_height >= 36
    finally:
        ctx.close()
