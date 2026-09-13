"""The public redesign has one primary navigation, with no legacy detours."""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PAGES = {
    "home2.html": "home2.html",
    "profiles.html": "profiles.html",
    "commune.html": "profiles.html",
    "macro.html": "macro.html",
    "micro.html": "micro.html",
    "map.html": "map.html",
}
LINKS = ["home2.html", "profiles.html", "macro.html", "micro.html", "map.html"]
LEGACY = {"index.html", "home.html", "communes.html", "all_data.html", "local.html"}


def _primary_nav(page: str) -> str:
    html = (REPO / page).read_text(encoding="utf-8")
    header = re.search(r'<header class="bp-topbar">(.*?)</header>', html, re.S)
    assert header, f"{page} has no shared top bar"
    nav = re.search(r'<nav class="bp-nav"[^>]*>(.*?)</nav>', header.group(1), re.S)
    assert nav, f"{page} has no primary navigation"
    return nav.group(1)


def test_every_public_redesign_uses_the_same_five_destinations():
    for page in PAGES:
        links = re.findall(r'href="([^"]+)"', _primary_nav(page))
        assert links == LINKS, f"{page} has a different primary navigation: {links}"
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
