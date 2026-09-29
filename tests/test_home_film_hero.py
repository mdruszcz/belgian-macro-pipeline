"""Static contract for the two-phase film hero on home2.html
(the film-first redesign, screenshots-review/home-v5/, superseding the
behind-the-cards film hero shipped in #300).

home2.html is the canonical homepage (index.html and home.html both redirect to
it -- tests/test_home2.py already pins that). This file only pins what a static
read of the page can verify: the film/poster markup and sources exist, phase 1
is the film ALONE (no map/chart cards on top of it -- the defect the maintainer
rejected in #300), the once-per-session / reduced-motion / Save-Data logic is
present in the inline script, the i18n keys exist in all three languages, and
the page still has exactly one <h1>.

Browser behaviour (phase transitions on film-end/Skip/scroll, the second-visit
banner+replay state, reduced-motion suppressing the video request, the 720p
source at mobile width, and the CTA/search being usable in phase 2) is covered
by tests/test_home_film_hero_browser.py.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PAGE = REPO / "home2.html"
FILM_DIR = REPO / "assets" / "belpulse" / "home-film"

FILM_ASSETS = (
    "assets/belpulse/home-film/film-1080.mp4",
    "assets/belpulse/home-film/film-720.mp4",
    "assets/belpulse/home-film/poster.jpg",
)


def _html() -> str:
    return PAGE.read_text(encoding="utf-8")


def test_film_assets_exist_and_are_each_under_25mb():
    for relative in FILM_ASSETS:
        f = REPO / relative
        assert f.is_file(), f"missing {relative}"
        size_mb = f.stat().st_size / (1024 * 1024)
        assert size_mb <= 25, f"{relative} is {size_mb:.1f} MB, over the 25 MB limit"


def test_home2_film_hero_references_every_film_source_and_the_poster():
    html = _html()
    for relative in FILM_ASSETS:
        assert relative in html, f"{relative} not referenced in home2.html"
    # poster.webp is a real file even though only poster.jpg needs to appear in
    # markup (the JPEG is what the <img>/<video poster> actually use here).
    assert (FILM_DIR / "poster.webp").is_file()
    # film-1440.webm (the #300 cut's optional AV1 tier) is deliberately not
    # referenced -- the approved cut this batch builds from is a 1080p H.264
    # source only, and remuxing it into .webm is not possible (-c:v copy
    # requires an actual AV1/VP9 stream). No stale reference to it remains.
    assert "film-1440.webm" not in html
    assert not (FILM_DIR / "film-1440.webm").is_file()


def test_home2_film_hero_video_is_muted_playsinline_never_autoplay_attribute():
    """Autoplay attribute is deliberately never used -- the JS decides whether
    to play (reduced-motion / Save-Data / session-already-played all suppress
    it), which a bare `autoplay` attribute could not express. muted+playsinline
    are required for iOS/Android inline playback and to avoid ever bringing up
    fullscreen or sound."""
    html = _html()
    video_tag = re.search(r"<video[^>]*id=\"filmVideo\"[^>]*>", html)
    assert video_tag, "no #filmVideo <video> element"
    tag = video_tag.group(0)
    assert "muted" in tag
    assert "playsinline" in tag
    assert "autoplay" not in tag
    assert 'preload="none"' in tag


def test_home2_phase1_is_the_film_alone_with_no_map_or_chart_cards_on_top():
    """The defect the maintainer rejected in #300: the film played BEHIND the
    hero's map/chart cards, so both fought for attention at once. Phase 1
    (`.filmhero`) must contain no `.hcard`/`.hero-map`/`.hero-mini`/map SVG --
    those all belong to `.datahero` (phase 2), a materially separate section
    that follows the film band in the DOM, not layered inside/behind it."""
    html = _html()
    film_section = re.search(
        r'<section class="filmhero"[^>]*id="filmHero"[^>]*>(.*?)</section>', html, re.DOTALL
    )
    assert film_section, "no #filmHero section"
    body = film_section.group(1)
    for forbidden in ("hero-map", "hero-mini", 'id="mapCard"', 'id="svg"', "hcard"):
        assert forbidden not in body, f"phase 1 (#filmHero) still contains {forbidden!r}"

    # #dataHero (phase 2) must exist as its own section, after #filmHero in
    # the DOM, not nested inside it.
    filmhero_at = html.index('id="filmHero"')
    datahero_at = html.index('id="dataHero"')
    assert filmhero_at < datahero_at
    assert 'id="mapCard"' in html[datahero_at:]


def test_home2_film_hero_layer_is_never_full_screen():
    html = _html()
    assert "requestFullscreen" not in html
    assert "webkitEnterFullscreen" not in html
    # ~92vh film band, next section still follows below it (never 100vh/100%).
    rule = re.search(r"\.filmhero\{([^}]+)\}", html)
    assert rule, "no .filmhero rule"
    assert "height:92vh" in rule.group(1).replace(" ", "")


def test_home2_film_hero_js_encodes_the_once_per_session_and_environment_rules():
    html = _html()
    script = re.search(r"function initHero\(\)\{(.*?)\n  \}\n\n  // Fires", html, re.DOTALL)
    assert script, "initHero() not found"
    body = script.group(1)
    assert "sessionStorage" in body
    assert "prefers-reduced-motion: reduce" in body
    assert "saveData" in body
    assert re.search(r"2g\|2g\|3g", body) or "effectiveType" in body
    assert "IntersectionObserver" in body
    assert "max-width: 768px" in body


def test_home2_film_hero_video_sources_use_data_src_not_src_until_js_wires_them():
    """No JS: the <source> elements must carry no live `src` in the markup
    (only data-src), so a no-JS browser never issues a video request and the
    poster <img> is what actually renders."""
    html = _html()
    video_block = re.search(r'<video[^>]*id="filmVideo"[^>]*>(.*?)</video>', html, re.DOTALL)
    assert video_block, "no #filmVideo video element"
    body = video_block.group(1)
    assert (
        re.search(r"<source[^>]*[^a-z-]src=", body) is None
    ), "a <source> has a live src in markup"
    assert body.count("data-src=") == 2


def test_home2_still_has_exactly_one_h1():
    body = re.search(r"<body[^>]*>(.*)</body>", _html(), re.DOTALL).group(1)
    assert len(re.findall(r"<h1\b", body)) == 1


def test_home2_i18n_keys_for_the_film_exist_in_all_three_languages():
    strings = (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
    for key in ("homeFilmReplay", "homeFilmAlt", "v5SkipFilm", "v5ReplayFilm", "v5ScrollCue"):
        assert key in _html() or key in strings  # referenced from home2.html via data-t*
        assert strings.count(f"{key}:") == 3, f"{key} missing from one language block"


def test_home2_replay_button_uses_translated_strings_not_hardcoded_text():
    """v5ReplayFilm/homeFilmReplay drive the button's text and its aria-label
    -- data-t on the <button> element itself is what applyStrings() rewrites
    (document.querySelectorAll('[data-t]').forEach(...).textContent = ...),
    the same pattern every other translated element on the page uses (e.g.
    the finance strip's <h2 data-t="homeFinanceTitle">)."""
    html = _html()
    button = re.search(r'<button[^>]*id="filmReplay"[^>]*>(.*?)</button>', html, re.DOTALL)
    assert button, "no #filmReplay button"
    opening_tag = button.group(0).split(">", 1)[0]
    assert 'data-t-aria="homeFilmReplay"' in opening_tag
    assert 'data-t="v5ReplayFilm"' in opening_tag
    assert (
        "hidden" in opening_tag
    ), "replay button must start hidden -- only shown once a play has completed"


def test_home2_skip_button_uses_translated_strings_not_hardcoded_text():
    html = _html()
    button = re.search(r'<button[^>]*id="filmSkip"[^>]*>(.*?)</button>', html, re.DOTALL)
    assert button, "no #filmSkip button"
    assert 'data-t="v5SkipFilm"' in button.group(0).split(">", 1)[0]
