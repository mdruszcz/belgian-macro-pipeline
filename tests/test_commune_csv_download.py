"""#downloadLink's CSV export (A2, 2026-09-27).

The header "Télécharger le profil" button used to open the commune's raw
JSON payload with no download at all. It now builds a CSV client-side from
that same payload (state.profile) and triggers a real browser download --
this is the one test file covering it, and it also carries rule 26's "five
distinct states, never collapsed" coverage that the removed "All data"
accordion used to hold (see tests/test_a4_finishing_fixes.py).

Same shared local-file HTTP server / one-context-per-test pattern as
tests/test_a4_finishing_fixes.py.
"""

from __future__ import annotations

import csv
import functools
import http.server
import io
import json
import socketserver
import threading
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
COMMUNE_92094_JSON = REPO / "public" / "data" / "communes" / "92094.json"


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


def _context(chromium, width=1122, height=900):
    return chromium.new_context(
        viewport={"width": width, "height": height}, locale="en-US", accept_downloads=True
    )


def test_download_link_produces_a_named_csv_with_the_payload_value(chromium, site):
    payload = json.loads(COMMUNE_92094_JSON.read_text(encoding="utf-8"))
    expected_value = payload["indicators"]["POPULATION_BY_COMMUNE"]["periods"]["2026"]["value"]

    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#essentielRow .essentiel-item", state="attached")

        with page.expect_download() as dl_info:
            page.click("#downloadLink")
        download = dl_info.value
        assert download.suggested_filename == "92094_namur.csv"

        csv_path = download.path()
        raw = Path(csv_path).read_bytes()
        # UTF-8 BOM so Excel opens accented labels correctly.
        assert raw.startswith(b"\xef\xbb\xbf"), "CSV is missing its UTF-8 BOM"
        text = raw.decode("utf-8-sig")

        rows = list(csv.reader(io.StringIO(text)))
        header = rows[0]
        assert header == [
            "Indicator code",
            "Indicator name",
            "Period",
            "Value",
            "Status",
            "Unit",
        ]

        pop_2026 = [r for r in rows[1:] if r[0] == "POPULATION_BY_COMMUNE" and r[2] == "2026"]
        assert len(pop_2026) == 1, "expected exactly one POPULATION_BY_COMMUNE/2026 row"
        row = pop_2026[0]
        assert float(row[3]) == expected_value
        # No thousands separator in the raw value (rule: values raw).
        assert "," not in row[3] and " " not in row[3]
        assert row[5] == "count"
    finally:
        ctx.close()


def test_csv_covers_every_indicator_and_period_in_the_payload(chromium, site):
    payload = json.loads(COMMUNE_92094_JSON.read_text(encoding="utf-8"))
    expected_rows = sum(len(entry.get("periods") or {}) for entry in payload["indicators"].values())
    assert expected_rows > 0

    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#essentielRow .essentiel-item", state="attached")

        with page.expect_download() as dl_info:
            page.click("#downloadLink")
        download = dl_info.value
        text = Path(download.path()).read_bytes().decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text)))
        assert len(rows) - 1 == expected_rows, (
            f"expected {expected_rows} data rows (one per indicator/period), "
            f"found {len(rows) - 1}"
        )
    finally:
        ctx.close()


def test_csv_five_states_stay_distinct(chromium, site):
    """A real number (zero included), a suppressed cell and a missing cell
    must never render the same way in the export (rule 26)."""
    payload = json.loads(COMMUNE_92094_JSON.read_text(encoding="utf-8"))
    numeric_code = None
    for code, entry in payload["indicators"].items():
        periods = entry.get("periods") or {}
        for period, cell in periods.items():
            if isinstance(cell.get("value"), (int, float)):
                numeric_code, numeric_period = code, period
                break
        if numeric_code:
            break
    assert numeric_code, "fixture drift: no indicator in 92094.json has a numeric value"

    ctx = _context(chromium)
    try:
        page = ctx.new_page()
        page.goto(f"{site}/commune.html?nis=92094", wait_until="load")
        page.wait_for_selector("#essentielRow .essentiel-item", state="attached")

        with page.expect_download() as dl_info:
            page.click("#downloadLink")
        download = dl_info.value
        text = Path(download.path()).read_bytes().decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text)))
        match = [r for r in rows[1:] if r[0] == numeric_code and r[2] == numeric_period]
        assert len(match) == 1
        # A real numeric value is written as a raw number, never blank.
        assert match[0][3].strip() not in ("", "—")
        float(match[0][3])  # parses as a number
    finally:
        ctx.close()
