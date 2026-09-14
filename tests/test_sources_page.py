"""sources.html -- Batch A3.1a (docs/features/site_unification.md).

The page is a catalogue of the nine registered sources
(public/data/metadata/sources.json), each row built at runtime from the
indicators that actually carry that source
(public/data/explorer/index.json). These tests hold two things: that no
source name, agency, dataset name or date is typed into the page's own
markup (CLAUDE.md rules 28, 36 -- the page must earn every fact it shows from
a real payload), and that the rendered table behaves the way the acceptance
criteria describe -- one row per registered source, a real "not stated"
state for a null licence note, a correct period range, a keyboard-operable
accordion, a working search, and a language switch that relabels it all.

PERIOD DATA COMES FROM public/data/explorer/index.json, NOT
public/data/metadata/indicators.json -- see sources.html's own file-header
comment for why (that index is municipal-only and several registered
sources publish national series only; explorer/index.json already carries a
deterministic period_min/period_max for both scopes). The browser tests
below compute their expected period the same way sources.html's own script
does, from that same file, so they are pinned to what the page actually
reads.
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

REPO = Path(__file__).resolve().parents[1]
SOURCES_HTML = REPO / "sources.html"
SOURCES_JSON = REPO / "public" / "data" / "metadata" / "sources.json"
EXPLORER_INDEX_JSON = REPO / "public" / "data" / "explorer" / "index.json"

MOBILE_WIDTH = 390


# --- static checks -----------------------------------------------------------


def _sources_registry() -> dict:
    return json.loads(SOURCES_JSON.read_text(encoding="utf-8"))["sources"]


def _explorer_index() -> list[dict]:
    return json.loads(EXPLORER_INDEX_JSON.read_text(encoding="utf-8"))["indicators"]


def _non_dynamic_markup() -> str:
    """sources.html's own markup, with the parts that are ALLOWED to name a
    source verbatim stripped out first: the file-header HTML comment (design
    rationale, never rendered) and the `.attribution` div (the Statbel/ONEM/
    police/Steunpunt Werk licence notice, held byte-identical to
    assets/i18n.js by tests/test_statbel_attribution.py -- a completely
    different obligation from this file's "nothing hand-typed" one).

    What is left is everything the reader actually sees outside that one
    licence block: the page chrome, the toolbar, the cards, the table
    skeleton and the client-side script that builds the table body.
    """
    text = SOURCES_HTML.read_text(encoding="utf-8")
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)
    text = re.sub(
        r'<div class="attribution".*?</div>\s*(?=<p class="src-licences-foot")',
        "",
        text,
        flags=re.DOTALL,
    )
    return text


def test_no_source_or_agency_name_is_hand_typed():
    """Every label and agency in sources.json, for every language, must be
    absent from the page's own markup -- the table is built from the fetched
    registry at runtime, never written out by hand."""
    text = _non_dynamic_markup()
    offenders = []
    for source_id, entry in _sources_registry().items():
        candidates = set(entry.get("label", {}).values())
        if entry.get("agency"):
            candidates.add(entry["agency"])
        for candidate in candidates:
            if candidate and candidate in text:
                offenders.append(f"{source_id}: {candidate!r}")
    assert not offenders, f"source/agency names hand-typed in sources.html: {offenders}"


def test_no_indicator_name_is_hand_typed():
    """Same obligation, for dataset names -- the 'Data' column and its
    expanded list are built from explorer/index.json, never written out."""
    text = _non_dynamic_markup()
    offenders = []
    for row in _explorer_index():
        for name in (row.get("names") or {}).values():
            # Short/common words ("GDP", "Belgium") would false-positive
            # against ordinary interface prose; only real multi-word
            # indicator names are informative here.
            if name and len(name.split()) >= 3 and name in text:
                offenders.append(row["indicator_code"])
    assert not offenders, f"indicator names hand-typed in sources.html: {offenders}"


def test_no_date_is_hand_typed():
    """No retrieval or period date is written into the page's own markup --
    every date the reader sees is filled in by the script from a payload."""
    text = _non_dynamic_markup()
    assert not re.search(
        r"\b20\d{2}-\d{2}-\d{2}\b", text
    ), "an ISO date is hand-typed in sources.html"


def test_the_shell_markers_are_present_exactly_once():
    text = SOURCES_HTML.read_text(encoding="utf-8")
    for zone in ("bootstrap", "header", "footer"):
        assert len(re.findall(rf"<!-- bp-shell:{zone}:start", text)) == 1
        assert len(re.findall(rf"<!-- bp-shell:{zone}:end -->", text)) == 1


def test_the_values_attribution_markers_are_intact():
    text = SOURCES_HTML.read_text(encoding="utf-8")
    assert "<!-- values-attribution:start" in text
    assert "<!-- values-attribution:end -->" in text
    assert 'id="attrUpdated"' in text


def test_every_registered_source_could_render_a_row():
    """Sanity check on the fixture data itself, so a browser test failure
    below is never mistaken for an empty registry."""
    assert len(_sources_registry()) == 9


# --- browser checks ------------------------------------------------------


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


@pytest.fixture
def page(chromium, site):
    # `locale="en-US"` pins I18N.initial()'s navigator.language fallback to
    # English regardless of the host machine's own OS locale -- without it
    # this suite is not reproducible: on a French-locale machine every test
    # here would see French chrome and text-match assertions written for
    # English would fail for a reason that has nothing to do with the page.
    context = chromium.new_context(viewport={"width": 1122, "height": 900}, locale="en-US")
    try:
        p = context.new_page()
        p.goto(f"{site}/sources.html", wait_until="load")
        p.wait_for_selector("#srcTableBody tr.src-row")
        yield p
    finally:
        context.close()


def test_every_source_has_exactly_one_row(page):
    rows = page.locator("#srcTableBody tr.src-row")
    assert rows.count() == len(_sources_registry())
    seen = set()
    for i in range(rows.count()):
        name = rows.nth(i).locator(".src-name").text_content()
        assert name not in seen, f"{name!r} rendered more than once"
        seen.add(name)


def _row_for_agency(page, substring: str):
    """A row whose visible text (name AND, where present, the secondary
    line) contains `substring`. Not scoped to `.src-agency` alone: that span
    is omitted entirely when a source's agency and name both match its
    label (see sources.html's subtitleFor()), so a locator scoped to it
    would wait forever for an element that row never renders."""
    rows = page.locator("#srcTableBody tr.src-row")
    for i in range(rows.count()):
        row = rows.nth(i)
        if substring in (row.text_content() or ""):
            return row
    raise AssertionError(f"no row found containing {substring!r}")


def test_a_null_licence_note_renders_the_not_stated_state(page):
    """fpb (Federal Planning Bureau) has licence_note: null and zero
    published indicators in sources.json/explorer index -- both must show as
    an honest state, never a blank cell and never invented wording."""
    assert _sources_registry()["fpb"]["licence_note"] is None
    row = _row_for_agency(page, "FPB")
    conditions_cell = row.locator("td").nth(3)
    assert "not stated" in (conditions_cell.text_content() or "").lower()
    assert (row.locator("td").nth(2).text_content() or "").strip() == "—"


def test_a_source_with_no_indicators_says_so_when_expanded(page):
    row = _row_for_agency(page, "FPB")
    row.locator(".src-toggle").click()
    detail_id = row.locator(".src-toggle").get_attribute("aria-controls")
    detail = page.locator(f"#{detail_id}")
    assert detail.is_visible()
    assert "no indicator" in (detail.text_content() or "").lower()


def _period_cmp_key(period: str):
    year = period[:4]
    return (year, period)


def test_a_rows_period_matches_the_min_max_computed_from_the_explorer_index(page):
    """walstat's own indicators are all plain years (no quarter/month mix),
    so the expected string is unambiguous: min(period_min), max(period_max)
    across every row explorer/index.json carries for that source."""
    rows = [r for r in _explorer_index() if r.get("source") == "walstat"]
    assert rows, "fixture drift: walstat has no indicators in explorer/index.json any more"
    first = min((r["period_min"] for r in rows), key=_period_cmp_key)
    last = max((r["period_max"] for r in rows), key=_period_cmp_key)
    expected = first if first == last else f"{first}–{last}"

    row = _row_for_agency(page, "IWEPS")
    period_cell = row.locator("td").nth(2)
    assert (period_cell.text_content() or "").strip() == expected


def test_the_accordion_opens_by_keyboard(page):
    row = page.locator("#srcTableBody tr.src-row").first
    toggle = row.locator(".src-toggle")
    detail_id = toggle.get_attribute("aria-controls")
    detail = page.locator(f"#{detail_id}")

    assert toggle.get_attribute("aria-expanded") == "false"
    assert not detail.is_visible()

    toggle.focus()
    page.keyboard.press("Enter")

    assert toggle.get_attribute("aria-expanded") == "true"
    assert detail.is_visible()

    page.keyboard.press("Enter")
    assert toggle.get_attribute("aria-expanded") == "false"
    assert not detail.is_visible()


def test_search_narrows_the_rows(page):
    total = page.locator("#srcTableBody tr.src-row").count()
    assert total == len(_sources_registry())

    page.fill("#sourceSearch", "eurostat")
    page.wait_for_function(
        "(n) => document.querySelectorAll('#srcTableBody tr.src-row').length < n", arg=total
    )
    narrowed = page.locator("#srcTableBody tr.src-row")
    assert 0 < narrowed.count() < total
    for i in range(narrowed.count()):
        text = (narrowed.nth(i).text_content() or "").lower()
        assert "eurostat" in text

    page.fill("#sourceSearch", "no such source or dataset exists at all")
    page.wait_for_function(
        "() => document.querySelectorAll('#srcTableBody tr.src-row').length === 0"
    )
    assert page.locator("#srcTableBody tr.src-row").count() == 0
    assert page.locator("#srcEmpty").is_visible()


def test_the_category_filter_narrows_by_scope(page):
    """The category filter is derived from explorer/index.json's own `scope`
    field (municipal / national), not an invented taxonomy -- see sources.html's
    file-header comment. fpb (no published indicator of either scope) is
    correctly absent from BOTH filtered views, present only under "All"."""
    total = page.locator("#srcTableBody tr.src-row").count()
    assert total == len(_sources_registry())

    # `.src-name`, not `.src-agency`: the latter is omitted entirely for a
    # source whose agency and name both match its label (see sources.html's
    # subtitleFor()), so it does not exist for every row.
    page.select_option("#categoryFilter", "national")
    page.wait_for_function(
        "(n) => document.querySelectorAll('#srcTableBody tr.src-row').length < n", arg=total
    )
    national_names = {
        page.locator("#srcTableBody tr.src-row .src-name").nth(i).text_content()
        for i in range(page.locator("#srcTableBody tr.src-row").count())
    }
    assert "FPB" not in national_names

    page.select_option("#categoryFilter", "municipal")
    page.wait_for_timeout(50)
    municipal_names = {
        page.locator("#srcTableBody tr.src-row .src-name").nth(i).text_content()
        for i in range(page.locator("#srcTableBody tr.src-row").count())
    }
    assert "FPB" not in municipal_names
    assert national_names != municipal_names

    page.select_option("#categoryFilter", "")
    page.wait_for_function(
        "(n) => document.querySelectorAll('#srcTableBody tr.src-row').length === n", arg=total
    )


def test_switching_to_nl_relabels_the_table_headers(page):
    assert page.locator(".src-table thead th").first.text_content() == "Source"
    page.click("#bp-lang-menu-btn")
    page.click('.bp-lang-switch [data-lang="nl"]')
    page.wait_for_function("document.querySelector('.src-table thead th').textContent === 'Bron'")
    headers = page.locator(".src-table thead th")
    assert headers.nth(0).text_content() == "Bron"
    assert headers.nth(1).text_content() == "Gegevens"
    assert headers.nth(2).text_content() == "Gepubliceerde periode"
    assert headers.nth(3).text_content() == "Voorwaarden"
    # The table body itself was rebuilt in the new language too, not just the
    # column heads -- e.g. a source with no indicators now says so in Dutch.
    fpb_row = _row_for_agency(page, "FPB")
    assert "nog geen" in (fpb_row.text_content() or "").lower()


def test_1122px_the_heading_lines_up_with_the_header_logo(page):
    """Regression: the generic `.wrap` in assets/belpulse/layout.css carries
    no side padding of its own (only `.bp-topbar .wrap`/`.foot .wrap` get
    one) -- without a page-local padding rule, the heading, cards, table and
    licence block sat flush against the viewport edge while the header
    above them, padded separately, did not. Pinned by comparing the h1's
    left edge to the header logo's -- the two should align exactly, the way
    a page's own heading always lines up under its site's wordmark."""
    h1_box = page.locator(".pagehead h1").bounding_box()
    # `.bp-logo` matches twice (header and footer both carry the wordmark) --
    # scoped to the header specifically, the one the heading should line up
    # under.
    logo_box = page.locator(".bp-topbar .bp-logo").bounding_box()
    assert h1_box is not None and logo_box is not None
    assert abs(h1_box["x"] - logo_box["x"]) <= 2, (
        f"h1 left edge ({h1_box['x']}) does not line up with the logo's "
        f"({logo_box['x']}) at 1122px"
    )


def test_390px_no_horizontal_scroll(chromium, site):
    context = chromium.new_context(viewport={"width": MOBILE_WIDTH, "height": 800})
    try:
        p = context.new_page()
        p.goto(f"{site}/sources.html", wait_until="load")
        p.wait_for_selector("#srcTableBody tr.src-row")
        scroll_width = p.evaluate("document.documentElement.scrollWidth")
        client_width = p.evaluate("document.documentElement.clientWidth")
        assert scroll_width <= client_width + 1, (
            f"sources.html scrolls horizontally at 390px: scrollWidth={scroll_width}, "
            f"clientWidth={client_width}"
        )
    finally:
        context.close()


def test_390px_the_heading_keeps_a_real_side_gutter(chromium, site):
    context = chromium.new_context(viewport={"width": MOBILE_WIDTH, "height": 800})
    try:
        p = context.new_page()
        p.goto(f"{site}/sources.html", wait_until="load")
        p.wait_for_selector("#srcTableBody tr.src-row")
        h1_box = p.locator(".pagehead h1").bounding_box()
        assert h1_box is not None
        assert h1_box["x"] >= 16, f"h1 sits only {h1_box['x']}px from the left edge at 390px"
    finally:
        context.close()


def test_390px_detail_rows_stay_collapsed_by_default(chromium, site):
    """Regression: the stacked-table media query sets `tr{display:block}` on
    every row, which has higher CSS specificity than the UA stylesheet's
    `[hidden]{display:none}` -- without an explicit `tr[hidden]{display:none}`
    rule, every detail row rendered open at 390px even though its toggle's
    `aria-expanded` correctly said "false". Found on a real screenshot."""
    context = chromium.new_context(viewport={"width": MOBILE_WIDTH, "height": 800})
    try:
        p = context.new_page()
        p.goto(f"{site}/sources.html", wait_until="load")
        p.wait_for_selector("#srcTableBody tr.src-row")
        detail_rows = p.locator("#srcTableBody tr.src-detail-row")
        assert detail_rows.count() == len(_sources_registry())
        for i in range(detail_rows.count()):
            assert not detail_rows.nth(
                i
            ).is_visible(), "a detail row is visible before its toggle was clicked"
    finally:
        context.close()
