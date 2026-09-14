"""Tests for scripts/sync_site_shell.py (Batch A1.1,
docs/features/site_unification.md).

home2.html and macro.html are hand-edited HTML, not rendered from a page
document, so the shared header and footer `src/pages/shell.py` builds for
every other public page cannot simply overwrite them: a maintainer's next
edit to either page would drift the moment nobody remembered to re-run a
generator. Instead each page carries three delimited zones (bootstrap,
header, footer) that ONLY this script writes into -- these tests are the
guarantee that the sync is exact, idempotent, and never silently partial.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SYNC_SCRIPT = REPO_ROOT / "scripts" / "sync_site_shell.py"

sys.path.insert(0, str(REPO_ROOT))

from scripts.sync_site_shell import SHELL_PAGES, SyncError, synced  # noqa: E402
from src.pages.shell import NAV, render_header  # noqa: E402

LINKS_SIX = [path for path, _ in NAV]


def _read(page: str) -> str:
    return (REPO_ROOT / page).read_text(encoding="utf-8")


# --- the marker contract ------------------------------------------------------


@pytest.mark.parametrize("page, _current", SHELL_PAGES)
@pytest.mark.parametrize("zone", ["bootstrap", "header", "footer"])
def test_each_page_has_each_marker_pair_exactly_once(page, _current, zone):
    html = _read(page)
    starts = len(re.findall(rf"<!-- bp-shell:{zone}:start", html))
    ends = len(re.findall(rf"<!-- bp-shell:{zone}:end -->", html))
    assert starts == 1, f"{page} has {starts} bp-shell:{zone}:start markers, expected 1"
    assert ends == 1, f"{page} has {ends} bp-shell:{zone}:end markers, expected 1"


def test_a_page_missing_a_marker_pair_is_refused():
    stub = "<html><body>no markers here</body></html>"
    with pytest.raises(SyncError, match="bootstrap"):
        synced(stub, page="stub.html", current="stub.html")


def test_a_page_with_two_start_markers_is_refused():
    stub = (
        "<!-- bp-shell:bootstrap:start -->x<!-- bp-shell:bootstrap:end -->"
        "<!-- bp-shell:bootstrap:start -->y<!-- bp-shell:bootstrap:end -->"
        '<!-- bp-shell:header:start page="x" -->x<!-- bp-shell:header:end -->'
        "<!-- bp-shell:footer:start -->x<!-- bp-shell:footer:end -->"
    )
    with pytest.raises(SyncError, match="bootstrap"):
        synced(stub, page="stub.html", current="stub.html")


# --- determinism and idempotency (claude.md rule 35) --------------------------


def test_the_sync_is_idempotent(tmp_path):
    """Syncing an already-synced page changes nothing -- a maintainer running
    this twice by habit must not churn the file a second time."""
    for page, _current in SHELL_PAGES:
        work = tmp_path / page
        shutil.copyfile(REPO_ROOT / page, work)
        once = synced(work.read_text(encoding="utf-8"), page=page, current=_current)
        twice = synced(once, page=page, current=_current)
        assert once == twice, f"{page}: syncing a synced page changed it"


def test_check_passes_on_the_committed_tree():
    """The committed home2.html/macro.html are exactly what a fresh sync
    would produce -- the same guarantee export_page_documents.py's --check
    gives the generated pages."""
    result = subprocess.run(
        [sys.executable, str(SYNC_SCRIPT), "--check"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, (
        f"the committed pages are not in sync (run scripts/sync_site_shell.py):\n"
        f"{result.stdout}{result.stderr}"
    )


def test_syncing_a_stale_copy_reproduces_the_committed_bytes(tmp_path):
    """Deterministic in the strong sense: hand the sync a copy with the header
    zone deliberately blanked out, and it reproduces the exact bytes already
    committed -- not merely 'valid-looking' output."""
    for page, current in SHELL_PAGES:
        committed = _read(page)
        blanked = re.sub(
            r"(<!-- bp-shell:header:start[^>]*-->).*?(<!-- bp-shell:header:end -->)",
            r"\1\2",
            committed,
            count=1,
            flags=re.DOTALL,
        )
        assert blanked != committed, f"{page}: could not blank the header zone to re-derive it"
        rebuilt = synced(blanked, page=page, current=current)
        assert rebuilt == committed, f"{page}: re-syncing a blanked copy did not reproduce it"


# --- no \r anywhere in a synced fragment (rule 35's LF-only guarantee) --------


@pytest.mark.parametrize("page, _current", SHELL_PAGES)
def test_the_synced_fragments_contain_no_carriage_return(page, _current):
    assert "\r" not in _read(page), f"{page} contains a carriage return"


# --- the six links, in order, header and footer --------------------------------


@pytest.mark.parametrize("page, current", SHELL_PAGES)
def test_the_six_links_appear_in_order_in_the_header(page, current):
    html = _read(page)
    header = re.search(r'<header class="bp-topbar">(.*?)</header>', html, re.S).group(1)
    nav = re.search(r'<nav class="bp-nav"[^>]*>(.*?)</nav>', header, re.S).group(1)
    links = re.findall(r'href="([^"]+)"', nav)
    assert links == LINKS_SIX, f"{page}: header links {links} != {LINKS_SIX}"


@pytest.mark.parametrize("page, current", SHELL_PAGES)
def test_the_six_links_appear_in_order_in_the_footer(page, current):
    html = _read(page)
    footer = re.search(r'<footer class="foot">(.*?)</footer>', html, re.S).group(1)
    nav = re.search(r'<nav class="bp-nav"[^>]*>(.*?)</nav>', footer, re.S).group(1)
    links = re.findall(r'href="([^"]+)"', nav)
    assert links == LINKS_SIX, f"{page}: footer links {links} != {LINKS_SIX}"


# --- the static header matches render_header(mode="static") on a real generated page --


def test_the_static_header_in_built_about_html_matches_render_header():
    """about.html is rendered by src/pages/shell.py's wrap() (mode="static"),
    never synced by this script -- this pins the two paths to the same
    output for a page in the same static mode, so a change to render_header
    cannot drift unnoticed between "wrapped" and "synced" pages."""
    about = (REPO_ROOT / "about.html").read_text(encoding="utf-8")
    header_match = re.search(r'<header class="bp-topbar">.*?</header>', about, re.S)
    assert header_match, "about.html has no top bar to compare"
    switch_links = {
        "en": "about.html",
        "fr": "fr/about.html",
        "nl": "nl/about.html",
    }
    expected = render_header(
        lang="en", current=None, asset_prefix="", mode="static", switch_links=switch_links
    )
    expected_header = re.search(r'<header class="bp-topbar">.*?</header>', expected, re.S).group(0)
    assert header_match.group(0) == expected_header
