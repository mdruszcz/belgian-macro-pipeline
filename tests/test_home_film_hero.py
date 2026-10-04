"""Static contract for the film, now that it has moved off the homepage.

Issue #312 batch 2 (docs/features/site_clarity.md) reversed the two-phase
film/data hero this file used to pin: home2.html opens directly on real
figures now (tests/test_home2.py covers that page's own contract), and the
film plays only on request, on about.html, built from a new `film` block
type (assets/belpulse/blocks/registry.json, src/pages/render.py's
_render_film). This file now pins the two halves of that reversal:

1. home2.html carries no film, no `<video>`, no film asset reference at all.
2. about.html (and its fr/nl editions) render the film block correctly:
   `preload="none"`, no `autoplay`, a labelled play button, and the click
   wiring loaded as a fixed asset (assets/belpulse/home-film/film-block.js),
   never a `<script>` written into config/pages/about/published.json itself
   (claude.md rule 22 -- a page document is data, not code).

Browser behaviour (no video request before a click, exactly one after,
keyboard operability) is covered by tests/test_home_film_hero_browser.py.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
FILM_DIR = REPO / "assets" / "belpulse" / "home-film"

FILM_ASSETS = (
    "assets/belpulse/home-film/film-1080.mp4",
    "assets/belpulse/home-film/film-720.mp4",
    "assets/belpulse/home-film/poster.webp",
)

ABOUT_PAGES = ("about.html", "fr/about.html", "nl/about.html")


def _html(relative: str) -> str:
    return (REPO / relative).read_text(encoding="utf-8")


def test_film_assets_exist_and_are_each_under_25mb():
    for relative in FILM_ASSETS:
        f = REPO / relative
        assert f.is_file(), f"missing {relative}"
        size_mb = f.stat().st_size / (1024 * 1024)
        assert size_mb <= 25, f"{relative} is {size_mb:.1f} MB, over the 25 MB limit"


def test_home2_carries_no_film_at_all():
    """The film left the homepage entirely (2026-10-04, issue #312): no
    `<video>` element, no film band, no replay control, no reference to any
    of the home-film/* assets."""
    html = _html("home2.html")
    rendered = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    assert "<video" not in rendered
    assert "filmHero" not in html
    assert "filmVideo" not in html
    assert "filmReplay" not in html
    assert "filmSkip" not in html
    for relative in FILM_ASSETS:
        assert relative not in html, f"home2.html still references {relative}"


def test_registry_declares_the_film_block_type_version_1():
    registry = json.loads(
        (REPO / "assets" / "belpulse" / "blocks" / "registry.json").read_text(encoding="utf-8")
    )
    film = registry["block_types"]["film"]
    assert film["current_version"] == 1
    assert film["supported_versions"] == [1]
    assert film["accepts_binding"] is False, "a film is never a resolved data figure"

    props = film["versions"]["1"]["props_schema"]
    assert props["additionalProperties"] is False
    assert props["required"] == ["alt"]
    for name in ("src", "src_mobile", "poster", "overlay_title", "play_label"):
        assert name in props["properties"], f"film v1 props schema missing {name!r}"

    # Site-relative only (rule 23: no remote URL in a page definition) --
    # src/poster/src_mobile all resolve to the shared relative_url $def.
    for name in ("src", "src_mobile", "poster"):
        assert props["properties"][name].get("$ref") == "#/$defs/relative_url"


def test_film_block_is_an_addition_never_a_change_to_an_existing_type():
    """registry.json's own forward_compatibility rule: a new type may be
    ADDED, but nothing already declared may be renamed, narrowed or removed.
    `photo` (the other image-ish type film was modelled on) must be
    untouched."""
    registry = json.loads(
        (REPO / "assets" / "belpulse" / "blocks" / "registry.json").read_text(encoding="utf-8")
    )
    photo = registry["block_types"]["photo"]
    assert photo["current_version"] == 1
    assert photo["supported_versions"] == [1]
    assert photo["versions"]["1"]["props_schema"]["required"] == ["alt"]


def test_about_pages_render_the_film_block_with_no_autoplay_and_preload_none():
    for page in ABOUT_PAGES:
        html = _html(page)
        block = re.search(
            r'<div class="bp-block bp-block--film"[^>]*>(.*?)</div>\s*</section>', html, re.DOTALL
        )
        assert block, f"{page}: no film block markup found"
        body = block.group(1)
        video_tag = re.search(r"<video[^>]*>", body)
        assert video_tag, f"{page}: no <video> inside the film block"
        tag = video_tag.group(0)
        assert 'preload="none"' in tag
        assert "autoplay" not in tag
        assert "muted" not in tag, "a muted autoplaying film is not what a click-to-play block is"


def test_about_pages_film_play_button_has_a_visible_accessible_name():
    """Lead review, round 2: the play button's accessible name must come
    from its own VISIBLE text, not a separate aria-label on an icon-only
    button -- so there must be no aria-label here, only real text content."""
    for page in ABOUT_PAGES:
        html = _html(page)
        button = re.search(
            r'<button type="button" class="film-play">(.*?)</button>', html, re.DOTALL
        )
        assert button, f"{page}: no .film-play button found"
        assert "aria-label" not in button.group(0)
        label = re.search(r'class="film-play-label"[^>]*>([^<]+)<', button.group(1))
        assert label and label.group(1).strip(), f"{page}: play button has no visible text"


def test_about_pages_film_block_carries_no_script_in_the_page_document():
    """Rule 22: a page document is data, not code. The film block's own
    click-to-play wiring must be a fixed asset the shell loads conditionally
    (src/pages/shell.py's _has_block_type check), never a <script> emitted
    from config/pages/about/published.json's own props."""
    for page in ABOUT_PAGES:
        html = _html(page)
        block = re.search(
            r'<div class="bp-block bp-block--film"[^>]*>(.*?)</div>\s*</section>', html, re.DOTALL
        )
        assert block, f"{page}: no film block markup found"
        assert "<script" not in block.group(1)
        # The fixed asset is still linked, conditionally, elsewhere on the page.
        assert "assets/belpulse/home-film/film-block.js" in html


def test_about_pages_film_sources_and_poster_reuse_the_existing_files():
    """No re-encode, no new binary (rule 12) -- the same files reused."""
    for page in ABOUT_PAGES:
        html = _html(page)
        assert "assets/belpulse/home-film/film-1080.mp4" in html
        assert "assets/belpulse/home-film/film-720.mp4" in html
        assert "assets/belpulse/home-film/poster.webp" in html


def test_film_block_js_never_sets_autoplay_or_fetches_before_click():
    source = (FILM_DIR / "film-block.js").read_text(encoding="utf-8")
    # Strip comments first -- this file's own prose explains what it does
    # NOT do, in words that would otherwise false-positive against a naive
    # substring check.
    code = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
    code = re.sub(r"//[^\n]*", "", code)
    assert re.search(r"\.autoplay\s*=|setAttribute\(['\"]autoplay['\"]", code) is None
    assert ".load()" not in code, "an explicit .load() call would start fetching early"
    assert "video.play()" in code, "the click handler must still start playback"
