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
#: Batch A1.1 (docs/features/site_unification.md) rebuilt the header/footer
#: for home2.html and macro.html through scripts/sync_site_shell.py; Batch
#: A3.1b moved the other four hand pages (profiles.html, commune.html,
#: micro.html, map.html) onto the same shared shell, and Batch A3.1a added sources.html. All seven PAGES entries
#: are synced now, so the old five-link contract (LINKS) that used to
#: guard the unconverted pages has nothing left to guard -- removed rather
#: than kept as a vacuous parametrize over an empty tuple, which would have
#: silently stopped testing anything the moment the last page was converted.
SYNCED_PAGES = tuple(PAGES)
LINKS_SIX = ["home2.html", "profiles.html", "macro.html", "micro.html", "map.html", "sources.html"]
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
    """Every public page: the shared header, six links including Sources --
    docs/features/site_unification.md section 2."""
    links = re.findall(r'href="([^"]+)"', _primary_nav(page))
    assert links == LINKS_SIX, f"{page} has a different primary navigation: {links}"
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
