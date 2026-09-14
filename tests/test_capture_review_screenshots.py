"""Unit tests for the pure parts of scripts/capture_review_screenshots.py --
filename slugging and argument parsing. No browser here: browser-backed
capture is exercised by hand (docs/implementation/batches/*) not by pytest,
same as scripts/capture_baseline_screenshots.py and
scripts/capture_builder_screenshots.py before it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.capture_review_screenshots import (
    DEFAULT_LANGS,
    DEFAULT_PAGES,
    DEFAULT_THEMES,
    DEFAULT_WIDTHS,
    build_filename,
    parse_args,
    slugify_page,
)


class TestSlugifyPage:
    def test_plain_page(self):
        assert slugify_page("home2.html") == "home2"

    def test_page_with_hash_fragment(self):
        assert slugify_page("macro.html#apercu") == "macro-apercu"

    def test_page_with_query_string(self):
        assert slugify_page("local.html?nis=11002") == "local-nis-11002"

    def test_nested_path(self):
        assert slugify_page("local/11002/index.html") == "local-11002-index"

    def test_is_deterministic(self):
        # Same input, same slug, every time -- other pieces of this script
        # (and a maintainer comparing two runs) rely on that.
        assert slugify_page("macro.html#europe") == slugify_page("macro.html#europe")

    def test_uppercase_is_normalised(self):
        assert slugify_page("Home2.HTML") == "home2"

    def test_empty_slug_is_rejected(self):
        with pytest.raises(ValueError):
            slugify_page("####")


class TestBuildFilename:
    def test_basic_combination(self):
        assert build_filename("home2.html", "light", "fr", 390) == "home2_light_fr_390.png"

    def test_fragment_page_combination(self):
        assert (
            build_filename("macro.html#apercu", "dark", "nl", 1122)
            == "macro-apercu_dark_nl_1122.png"
        )

    def test_distinct_combinations_never_collide(self):
        names = {
            build_filename(page, theme, lang, width)
            for page in DEFAULT_PAGES
            for theme in DEFAULT_THEMES
            for lang in DEFAULT_LANGS
            for width in DEFAULT_WIDTHS
        }
        expected_count = (
            len(DEFAULT_PAGES) * len(DEFAULT_THEMES) * len(DEFAULT_LANGS) * len(DEFAULT_WIDTHS)
        )
        assert len(names) == expected_count


class TestParseArgs:
    def test_out_is_required(self):
        with pytest.raises(SystemExit):
            parse_args([])

    def test_defaults_used_when_only_out_given(self):
        args = parse_args(["--out", "/tmp/somewhere"])
        assert args.pages == DEFAULT_PAGES
        assert args.themes == DEFAULT_THEMES
        assert args.langs == DEFAULT_LANGS
        assert args.widths == DEFAULT_WIDTHS
        assert args.out == Path("/tmp/somewhere")

    def test_out_is_parsed_as_path(self):
        args = parse_args(["--out", "/tmp/somewhere"])
        assert isinstance(args.out, Path)

    def test_widths_are_parsed_as_ints(self):
        args = parse_args(["--out", "/tmp/x", "--widths", "400", "800"])
        assert args.widths == [400, 800]
        assert all(isinstance(w, int) for w in args.widths)

    def test_overrides_are_applied(self):
        args = parse_args(
            [
                "--out",
                "/tmp/x",
                "--pages",
                "home2.html",
                "--themes",
                "light",
                "--langs",
                "fr",
                "--widths",
                "390",
            ]
        )
        assert args.pages == ["home2.html"]
        assert args.themes == ["light"]
        assert args.langs == ["fr"]
        assert args.widths == [390]
