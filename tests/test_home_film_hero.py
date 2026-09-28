"""Static contract for the brand-film hero on home2.html (docs/features/home_film.md).

home2.html is the canonical homepage (index.html and home.html both redirect to
it -- tests/test_home2.py already pins that). This file only pins what a static
read of the page can verify: the film/poster markup and sources exist, the
once-per-session / reduced-motion / Save-Data logic is present in the inline
script, the i18n keys exist in all three languages, and the page still has
exactly one <h1> (tests/test_home2.py's own invariant, re-checked here because
the hero markup changed materially).

Browser behaviour (autoplay, the second-visit poster+Replay state, reduced-motion
suppressing the video request, the 720p source at mobile width, and the CTA
being clickable at t=0) is covered by tests/test_home_film_hero_browser.py.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PAGE = REPO / "home2.html"
FILM_DIR = REPO / "assets" / "belpulse" / "home-film"

FILM_ASSETS = (
    "assets/belpulse/home-film/film-1080.mp4",
    "assets/belpulse/home-film/film-1440.webm",
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


def test_home2_hero_references_every_film_source_and_the_poster():
    html = _html()
    for relative in FILM_ASSETS:
        assert relative in html, f"{relative} not referenced in home2.html"
    # poster.webp is a real file even though only poster.jpg needs to appear in
    # markup (the JPEG is what the <img>/<video poster> actually use here).
    assert (FILM_DIR / "poster.webp").is_file()


def test_home2_hero_video_is_muted_playsinline_never_autoplay_attribute():
    """Autoplay attribute is deliberately never used -- the JS decides whether
    to play (reduced-motion / Save-Data / session-already-played all suppress
    it), which a bare `autoplay` attribute could not express. muted+playsinline
    are required for iOS/Android inline playback and to avoid ever bringing up
    fullscreen or sound."""
    html = _html()
    video_tag = re.search(r"<video[^>]*id=\"heroVideo\"[^>]*>", html)
    assert video_tag, "no #heroVideo <video> element"
    tag = video_tag.group(0)
    assert "muted" in tag
    assert "playsinline" in tag
    assert "autoplay" not in tag
    assert 'preload="none"' in tag


def test_home2_hero_film_layer_is_decorative_and_never_full_screen():
    html = _html()
    film = re.search(r'<div class="hero-film"[^>]*id="heroFilm"[^>]*>', html)
    assert film, "no #heroFilm layer"
    assert 'aria-hidden="true"' in film.group(0)
    # No requestFullscreen / webkitEnterFullscreen anywhere in the page script.
    assert "requestFullscreen" not in html
    assert "webkitEnterFullscreen" not in html
    # ~75vh hero band, next section still peeks below (never 100vh/100%).
    rule = re.search(r"\.home2 \.hero\{([^}]+)\}", html)
    assert rule, "no .home2 .hero rule"
    assert "min-height:75vh" in rule.group(1).replace(" ", "")


def test_home2_hero_headline_lead_and_cta_are_real_html_above_the_film():
    """The scrim + real HTML text (not baked into the video) is what makes the
    hero readable from second 0 -- z-index puts .wrap above .hero-film, and
    the headline/lead/CTA markup already exists (tests/test_home2.py pins the
    strings); this only pins the stacking that keeps them on top and outside
    the aria-hidden film layer."""
    html = _html()
    squashed = re.sub(r"\s+", "", html)
    assert re.search(r"\.hero-film\{[^}]*z-index:0", squashed)
    assert ".hero.wrap{position:relative;z-index:1}" in squashed
    # The CTA link is a sibling of .hero-film, not nested inside the
    # aria-hidden decorative layer, so it is never hidden from assistive tech.
    hero_section = re.search(r'<section class="hero">(.*?)</section>', html, re.DOTALL)
    assert hero_section
    film_block = hero_section.group(1)[: hero_section.group(1).index('class="wrap"')]
    assert 'id="heroFilm"' in film_block
    cta_at = hero_section.group(1).index('href="profiles.html"')
    wrap_at = hero_section.group(1).index('class="wrap"')
    assert cta_at > wrap_at, "CTA must be inside .wrap, after the film layer"


def test_home2_hero_js_encodes_the_once_per_session_and_environment_rules():
    html = _html()
    script = re.search(r"function initHeroFilm\(\)\{(.*?)\n  \}\n\n  // Fires", html, re.DOTALL)
    assert script, "initHeroFilm() not found"
    body = script.group(1)
    assert "sessionStorage" in body
    assert "prefers-reduced-motion: reduce" in body
    assert "saveData" in body
    assert re.search(r"2g\|2g\|3g", body) or "effectiveType" in body
    assert "IntersectionObserver" in body
    assert "max-width: 768px" in body


def test_home2_hero_video_sources_use_data_src_not_src_until_js_wires_them():
    """No JS: the <source> elements must carry no live `src` in the markup
    (only data-src), so a no-JS browser never issues a video request and the
    poster <img> is what actually renders."""
    html = _html()
    video_block = re.search(r'<video[^>]*id="heroVideo"[^>]*>(.*?)</video>', html, re.DOTALL)
    assert video_block, "no #heroVideo video element"
    body = video_block.group(1)
    assert (
        re.search(r"<source[^>]*[^a-z-]src=", body) is None
    ), "a <source> has a live src in markup"
    assert body.count("data-src=") == 3


def test_home2_still_has_exactly_one_h1():
    body = re.search(r"<body[^>]*>(.*)</body>", _html(), re.DOTALL).group(1)
    assert len(re.findall(r"<h1\b", body)) == 1


def test_home2_i18n_keys_for_the_film_exist_in_all_three_languages():
    strings = (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
    for key in ("homeFilmReplay", "homeFilmAlt"):
        assert key in _html() or key in strings  # referenced from home2.html via data-t*
        assert strings.count(f"{key}:") == 3, f"{key} missing from one language block"


def test_home2_replay_button_uses_translated_strings_not_hardcoded_text():
    html = _html()
    button = re.search(r'<button[^>]*id="heroReplay"[^>]*>(.*?)</button>', html, re.DOTALL)
    assert button, "no #heroReplay button"
    assert 'data-t-aria="homeFilmReplay"' in html
    assert 'data-t="homeFilmReplay"' in button.group(1)
    assert (
        button.group(0).split(">", 1)[0].find("hidden") != -1
    ), "replay button must start hidden -- only shown once a play has completed"
