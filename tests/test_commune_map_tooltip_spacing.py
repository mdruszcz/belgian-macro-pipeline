"""Commune Portrait polish (stage 2), A7: map hover tooltips.

Before this batch, the shared map engine's tooltip (.tip, assets/
commune_map.js's own .n/.v/.m spans -- name / value / period+status) had no
page-level CSS giving each span its own line, so they rendered as one run-on
line with no visible separation. Fixed page-level only (commune.html's own
CSS, see the "A7: map hover tooltips" doc comment above .tip's rule) --
assets/commune_map.css (the shared engine's default, used by other pages)
is untouched, per rule 29.

This test drives a REAL hover (a dispatched mousemove, the same event
assets/commune_map.js itself listens for) over a commune path in the lead
chapter's own map -- the one map on this page whose tooltip has a real `.v`
value line (the locator's and the neighbours map's shared placeTipHtml()
never show a value, only name + muted lines), so it is the only one that
can exercise the name/value spacing this batch's acceptance criterion is
about.
"""

from __future__ import annotations

import functools
import http.server
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


def test_map_tooltip_name_and_value_lines_do_not_overlap(chromium, site):
    ctx = chromium.new_context(viewport={"width": 1200, "height": 900}, locale="fr-FR")
    page = ctx.new_page()
    try:
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#chapters .chapter", state="attached")
        page.wait_for_timeout(500)
        # The first chapter's own lead map (demography, headline
        # POPULATION_BY_COMMUNE) -- rendered on load, no interaction needed
        # to populate it with real values.
        first_chapter = page.query_selector(".chapter")
        assert first_chapter, "no rendered chapter found"
        map_box = first_chapter.query_selector(".leadmap-box")
        assert map_box, "no .leadmap-box in the first chapter"
        page.wait_for_function(
            "el => el.querySelectorAll('path[data-nis]').length > 0",
            arg=map_box,
        )
        # Hover a commune path that actually carries a numeric value --
        # not necessarily the very first path in DOM order, which can be a
        # withheld/no-data commune (three distinct states, rule 26) with no
        # `.v` line to check spacing on. own commune (92094) is a safe bet:
        # a real profile page's own indicator is never suppressed for its
        # own commune on this build's fixture.
        own_path = map_box.query_selector('path[data-nis="92094"]')
        target = own_path or map_box.query_selector("path[data-nis]")
        assert target, "no commune path with data-nis found in the lead map"
        target.hover(force=True)
        page.wait_for_function(
            "el => !el.querySelector('.tip').classList.contains('map-hidden')",
            arg=map_box,
        )

        result = page.evaluate(
            """
            (mapBox) => {
                const tip = mapBox.querySelector('.tip');
                const n = tip.querySelector('.n');
                const v = tip.querySelector('.v');
                const m = tip.querySelector('.m');
                if (!n) return { error: 'no .n span in the tooltip' };
                const out = { nRect: n.getBoundingClientRect(), hasV: !!v, hasM: !!m };
                if (v) out.vRect = v.getBoundingClientRect();
                if (m) out.mRect = m.getBoundingClientRect();
                return out;
            }
            """,
            map_box,
        )
        assert "error" not in result, result.get("error")
        assert result["hasV"], (
            "the lead map's tooltip has no .v (value) line -- hovered a commune "
            "with no numeric value; the test needs one that has one"
        )
        n_rect, v_rect = result["nRect"], result["vRect"]
        # Each span is its own block-level line (display:block), so they
        # must not vertically overlap, and the gap between them must be at
        # least 2px -- the batch's own acceptance criterion.
        gap = v_rect["top"] - n_rect["bottom"]
        assert gap >= 2, (
            f"name and value lines are less than 2px apart (gap={gap:.2f}px): "
            f"name={n_rect} value={v_rect}"
        )
        if result["hasM"]:
            m_rect = result["mRect"]
            gap2 = m_rect["top"] - v_rect["bottom"]
            assert gap2 >= 2, (
                f"value and period/status lines are less than 2px apart "
                f"(gap={gap2:.2f}px): value={v_rect} meta={m_rect}"
            )
    finally:
        ctx.close()
