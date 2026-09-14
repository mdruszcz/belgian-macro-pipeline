"""The public redesign has one primary navigation, with no legacy detours."""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PAGES = {
    "home2.html": "home2.html",
    "profiles.html": "profiles.html",
    "commune.html": "profiles.html",
    "macro.html": "macro.html",
    "micro.html": "micro.html",
    "map.html": "map.html",
    "sources.html": "sources.html",
}
#: Batch A1.1 (docs/features/site_unification.md) rebuilds the header/footer
#: for home2.html and macro.html, through scripts/sync_site_shell.py -- these
#: carry the SIX destinations (Sources added). Batch A3.1a adds sources.html
#: itself to the same synced set (it is both a SHELL_PAGES entry and one of
#: the six destinations the others link to). The remaining four hand pages
#: (profiles.html, commune.html, micro.html, map.html) are unconverted until
#: a later batch and still carry the five this repo shipped with before
#: Batch A1.1; they keep the old assertion below rather than being silently
#: exempted, so a regression on them still fails loudly.
SYNCED_PAGES = ("home2.html", "macro.html", "sources.html")
UNSYNCED_PAGES = tuple(p for p in PAGES if p not in SYNCED_PAGES)
LINKS = ["home2.html", "profiles.html", "macro.html", "micro.html", "map.html"]
LINKS_SIX = LINKS + ["sources.html"]
LEGACY = {"index.html", "home.html", "communes.html", "all_data.html", "local.html"}


def _primary_nav(page: str) -> str:
    html = (REPO / page).read_text(encoding="utf-8")
    header = re.search(r'<header class="bp-topbar">(.*?)</header>', html, re.S)
    assert header, f"{page} has no shared top bar"
    nav = re.search(r'<nav class="bp-nav"[^>]*>(.*?)</nav>', header.group(1), re.S)
    assert nav, f"{page} has no primary navigation"
    return nav.group(1)


def _footer_nav(page: str) -> str:
    html = (REPO / page).read_text(encoding="utf-8")
    footer = re.search(r'<footer class="foot">(.*?)</footer>', html, re.S)
    assert footer, f"{page} has no shared footer"
    nav = re.search(r'<nav class="bp-nav"[^>]*>(.*?)</nav>', footer.group(1), re.S)
    assert nav, f"{page} has no footer navigation"
    return nav.group(1)


@pytest.mark.parametrize("page", SYNCED_PAGES)
def test_the_synced_pages_offer_all_six_destinations(page):
    """home2.html and macro.html: Batch A1.1's shared header, six links
    including Sources -- docs/features/site_unification.md section 2."""
    links = re.findall(r'href="([^"]+)"', _primary_nav(page))
    assert links == LINKS_SIX, f"{page} has a different primary navigation: {links}"
    assert not (set(links) & LEGACY)


@pytest.mark.parametrize("page", UNSYNCED_PAGES)
def test_the_unsynced_pages_still_offer_the_old_five_destinations(page):
    """profiles.html, commune.html, micro.html, map.html: not touched by
    Batch A1.1 (explicit exclusion) -- Batch A3.1 moves these to the same six
    links and markers the pilots now carry. This assertion is the old
    five-link contract, kept exactly so a regression on an unconverted page
    still fails here rather than being silently exempted."""
    links = re.findall(r'href="([^"]+)"', _primary_nav(page))
    assert links == LINKS, f"{page} has a different primary navigation: {links}"
    assert not (set(links) & LEGACY)


@pytest.mark.parametrize("page", SYNCED_PAGES)
def test_footer_lists_the_same_destinations(page):
    """The footer is not a second, independently-maintained nav -- it lists
    the identical six destinations, in the identical order, as the header
    (render_header/render_footer, src/pages/shell.py, both fed by the one
    NAV tuple)."""
    links = re.findall(r'href="([^"]+)"', _footer_nav(page))
    assert links == LINKS_SIX, f"{page}'s footer has a different navigation: {links}"
    assert not (set(links) & LEGACY)


def test_each_page_marks_exactly_one_matching_destination_current():
    for page, current in PAGES.items():
        nav = _primary_nav(page)
        marked = re.findall(r'<a href="([^"]+)"[^>]*aria-current="page"', nav)
        assert marked == [current], f"{page} marks {marked}, expected {current}"


def test_homepage_actions_do_not_send_readers_to_legacy_pages():
    html = (REPO / "home2.html").read_text(encoding="utf-8")
    for legacy in LEGACY:
        assert f'href="{legacy}"' not in html
        assert f"href = '{legacy}'" not in html
