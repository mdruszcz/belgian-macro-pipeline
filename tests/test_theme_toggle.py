"""The data pages must offer a light palette, not just follow the device.

These three pages shipped with only `@media (prefers-color-scheme: dark)`,
so a visitor on a dark-mode device could not see the light palette at all
and had no control over it. Each now carries a Light / Auto / Dark switch.

What makes the switch work is three CSS rules that have to stay in agreement,
which is what these tests pin:

  :root{...light...}                                      the base
  @media (prefers-color-scheme: dark){
      :root:not([data-theme="light"]){...dark...} }        follow the device,
                                                           unless forced light
  :root[data-theme="dark"]{...dark...}                     force dark on a
                                                           light-mode device

Drop the `:not([data-theme="light"])` and forcing light silently stops
working on exactly the devices that need it. Drop the third rule and forcing
dark silently does nothing on a light-mode device. Neither failure is visible
to anyone testing on a machine whose OS setting happens to match.
"""

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

# dashboard.html, index.html and about.html have their own (older) switcher
# and are deliberately not covered here.
THEMED_PAGES = ["communes.html", "all_data.html", "local.html"]


def _page(name: str) -> str:
    return (REPO / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("page", THEMED_PAGES)
def test_page_offers_all_three_theme_choices(page):
    text = _page(page)
    for choice in ("light", "auto", "dark"):
        assert f'data-theme-choice="{choice}"' in text, f"{page} has no {choice} option"


@pytest.mark.parametrize("page", THEMED_PAGES)
def test_forcing_light_overrides_a_dark_mode_device(page):
    """Without the :not() exclusion, the media query would keep winning and
    the Light button would appear to do nothing on a dark-mode device."""
    text = re.sub(r"\s+", " ", _page(page))
    assert (
        '@media (prefers-color-scheme: dark){ :root:not([data-theme="light"])' in text
    ), f"{page}'s dark media query does not exclude a forced light theme"


@pytest.mark.parametrize("page", THEMED_PAGES)
def test_forcing_dark_works_on_a_light_mode_device(page):
    """Needs a rule outside the media query, or Dark does nothing for anyone
    whose device is set to light."""
    assert ':root[data-theme="dark"]' in _page(
        page
    ), f"{page} has no forced-dark rule outside the media query"


@pytest.mark.parametrize("page", THEMED_PAGES)
def test_theme_is_applied_before_first_paint(page):
    """The saved choice is read in <head>, not at the end of the body --
    otherwise the page renders in the wrong palette and visibly flips."""
    text = _page(page)
    head = text[: text.index("</head>")]
    assert "belpulse-theme" in head, f"{page} does not apply the saved theme before paint"


@pytest.mark.parametrize("page", THEMED_PAGES)
def test_the_choice_is_remembered_and_shared_across_the_data_pages(page):
    text = re.sub(r"\s+", " ", _page(page)).replace('"', "'")
    # The switch stores under a KEY constant rather than an inline string, so
    # assert both halves: the key it is set to, and that it is written.
    assert "var KEY = 'belpulse-theme';" in text, f"{page} does not use the shared theme key"
    assert "localStorage.setItem(KEY, choice)" in text, f"{page} does not persist the choice"
    # A distinct key from dashboard.html's 'theme' (day/soft/night). Sharing
    # it would hand these pages a data-theme value they have no rule for.
    assert "localStorage.getItem('theme')" not in text


@pytest.mark.parametrize("page", THEMED_PAGES)
def test_localstorage_access_is_guarded(page):
    """localStorage throws in some privacy modes. An unguarded read in the
    pre-paint script would abort it and, on the pages where that script runs
    first, leave the rest of the page unstyled."""
    text = re.sub(r"\s+", " ", _page(page))
    assert (
        "try{ var t = localStorage.getItem('belpulse-theme');" in text
    ), f"{page} reads localStorage before paint without a try/catch"
