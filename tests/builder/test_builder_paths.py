"""Batch 11 builder-service path allowlist (batch-11 spec, "Path-allowlist
decision" and Tests item 1).

Every case here proves a REJECTION actually happens -- raising `PathError` (or
refusing a write) -- never merely that a regex or a guard function exists.
Batch 10's own finding (a protocol-relative URL sailing through a
correctly-*looking* character class) is exactly the failure mode this file
exists to catch: a page_id allowlist built on a character class rather than an
anchored, closed pattern would have the same shape of hole.

No indicator id, NIS code or commune figure is used here -- page ids and
version strings are not data under claude.md rule 36, so hand-writing them is
fine; this file never touches a page *document*.
"""

from __future__ import annotations

import os

import pytest

from src.builder import paths as builder_paths
from src.builder.paths import PathError

# --- 1. page_id allowlist: every case must actually raise --------------------

INVALID_PAGE_IDS = [
    pytest.param("../etc", id="dot-dot-slash"),
    pytest.param("..%2fetc", id="percent-encoded-slash-literal"),
    pytest.param("%2e%2e/etc", id="percent-encoded-dotdot-literal"),
    pytest.param("....//etc", id="quad-dot-double-slash"),
    pytest.param("/etc/passwd", id="absolute-path"),
    pytest.param("a/b", id="embedded-slash"),
    pytest.param("a\\b", id="embedded-backslash"),
    pytest.param("bad\x00id", id="embedded-nul"),
    pytest.param(".hidden", id="leading-dot"),
    pytest.param("-leading-dash", id="leading-dash"),
    pytest.param("Uppercase", id="uppercase"),
    pytest.param("a" * 65, id="too-long-65-chars"),
    pytest.param("", id="empty"),
    pytest.param("paɡe", id="unicode-homoglyph-cyrillic-a-lookalike"),
    pytest.param(".", id="dot-only"),
    pytest.param("..", id="dot-dot-only"),
]


@pytest.mark.parametrize("bad_id", INVALID_PAGE_IDS)
def test_validate_page_id_rejects(bad_id):
    """Every listed shape must be an actual rejection, not a pass-through.

    Literal strings, not URL-decoded ones, are what a well-behaved HTTP
    library hands the query-string parser (`..%2fetc` and `%2e%2e/etc` arrive
    at `paths.py` percent-encoded or already decoded depending on layer; both
    spellings are covered here so neither an over-eager nor a missing decode
    step opens a hole).
    """
    with pytest.raises(PathError):
        builder_paths.validate_page_id(bad_id)


def test_page_id_regex_itself_rejects_every_invalid_case():
    """Belt-and-suspenders on the regex object itself (not just the wrapper
    function), since PAGE_ID_RE is the "single place any filesystem path is
    derived" the spec names -- if the regex is loose, every caller inherits
    the hole even if validate_page_id happens to add an extra check today."""
    for bad_id in [p.values[0] for p in INVALID_PAGE_IDS]:
        assert builder_paths.PAGE_ID_RE.fullmatch(bad_id) is None, bad_id


VALID_PAGE_IDS = ["a", "home", "municipality-11001", "a" * 64]


@pytest.mark.parametrize("good_id", VALID_PAGE_IDS)
def test_validate_page_id_accepts_well_formed_ids(good_id):
    """The boundary the regex documents (`{0,63}` after one leading char, so
    64 total) is a PASS, not a rejection -- proven so the too-long case above
    is known to be testing the real boundary and not an off-by-one guess."""
    builder_paths.validate_page_id(good_id)  # must not raise


# --- version allowlist: exactly four digits (rev 2 P1 #5) --------------------

INVALID_VERSIONS = [
    pytest.param("1", id="too-short"),
    pytest.param("001", id="three-digits"),
    pytest.param("00001", id="five-digits"),
    pytest.param("abcd", id="non-numeric"),
    pytest.param("-001", id="negative"),
    pytest.param("1234.0", id="decimal"),
    pytest.param("", id="empty"),
    pytest.param("12 34", id="embedded-space"),
    pytest.param("1234\n", id="trailing-newline"),
]


@pytest.mark.parametrize("bad_version", INVALID_VERSIONS)
def test_validate_version_rejects(bad_version):
    with pytest.raises(PathError):
        builder_paths.validate_version(bad_version)


def test_validate_version_accepts_exactly_four_digits():
    builder_paths.validate_version("0001")  # must not raise
    builder_paths.validate_version("9999")  # must not raise


def test_version_ordering_is_lexical_and_numeric_agreeing():
    """Rev 2 P1 #5: exactly four digits, specifically so lexical and numeric
    sort agree -- '10000' (five digits, itself invalid) must never be able to
    sort before '9999' the way rev 1's `{4,}` would have allowed."""
    assert sorted(["0002", "0010", "0100", "1000", "9999"]) == [
        "0002",
        "0010",
        "0100",
        "1000",
        "9999",
    ]
    with pytest.raises(PathError):
        builder_paths.validate_version("10000")


# --- derived paths stay inside PAGES_ROOT --------------------------------


@pytest.fixture()
def pages_root(tmp_path, monkeypatch):
    """Both `paths.PAGES_ROOT` and `store.PAGES_ROOT` must be patched:
    store.py imports the name by value (`from src.builder.paths import
    PAGES_ROOT`), so it holds its own separate binding that patching only
    `paths` does not reach -- needed here because the symlink test below
    exercises `store.save_draft`."""
    from src.builder import store as builder_store

    root = tmp_path / "pages"
    root.mkdir()
    monkeypatch.setattr(builder_paths, "PAGES_ROOT", root)
    monkeypatch.setattr(builder_store, "PAGES_ROOT", root)
    return root


def test_draft_path_is_contained_in_pages_root(pages_root):
    p = builder_paths.draft_path("home")
    assert os.path.commonpath([str(p.resolve()), str(pages_root.resolve())]) == str(
        pages_root.resolve()
    )
    assert p.name == "draft.json"


def test_published_path_is_contained_in_pages_root(pages_root):
    p = builder_paths.published_path("home")
    assert os.path.commonpath([str(p.resolve()), str(pages_root.resolve())]) == str(
        pages_root.resolve()
    )
    assert p.name == "published.json"


def test_versions_dir_is_contained_in_pages_root(pages_root):
    d = builder_paths.versions_dir("home")
    assert os.path.commonpath([str(d.resolve()), str(pages_root.resolve())]) == str(
        pages_root.resolve()
    )
    assert d.name == "versions"


def test_version_path_names_a_published_snapshot(pages_root):
    p = builder_paths.version_path("home", "0001", kind="published")
    assert p.name == "published-0001.json"
    assert os.path.commonpath([str(p.resolve()), str(pages_root.resolve())]) == str(
        pages_root.resolve()
    )


def test_version_path_rejects_a_bad_page_id_even_when_version_is_fine(pages_root):
    with pytest.raises(PathError):
        builder_paths.version_path("../escape", "0001", kind="published")


def test_version_path_rejects_a_bad_version_even_when_page_id_is_fine(pages_root):
    with pytest.raises(PathError):
        builder_paths.version_path("home", "not-a-version", kind="published")


def test_assert_contained_passes_for_a_path_inside_the_root(pages_root):
    inside = pages_root / "home" / "draft.json"
    builder_paths.assert_contained(inside)  # must not raise


def test_assert_contained_rejects_a_path_outside_the_root(pages_root, tmp_path):
    outside = tmp_path / "elsewhere" / "draft.json"
    with pytest.raises(PathError):
        builder_paths.assert_contained(outside)


# --- the symlink escape: defence in depth beyond the regex --------------------


def test_symlink_planted_inside_a_page_directory_cannot_redirect_a_write_outside(
    pages_root, tmp_path
):
    """Tests item 1's specific adversarial case: even though `page_id` itself
    is clean and passes the regex, `draft.json` inside its directory is
    replaced with a symlink pointing outside `config/pages/`. The realpath
    containment check (spec: "Every resolved path is then checked with
    os.path.realpath for containment") must refuse a write through it -- the
    regex alone cannot catch this, since the page_id was never malicious.
    """
    page_dir = pages_root / "victim-page"
    page_dir.mkdir()
    outside_target = tmp_path / "outside_secret.json"
    outside_target.write_text("PRE-EXISTING SECRET", encoding="utf-8")

    draft_symlink = page_dir / "draft.json"
    draft_symlink.symlink_to(outside_target)

    # The path helper must refuse to treat this as a legitimate contained path...
    with pytest.raises(PathError):
        builder_paths.assert_contained(draft_symlink)

    # ...and a real write attempt through the store layer must not touch the
    # symlink target: import lazily so this file still collects (and fails
    # informatively rather than erroring at collection) if store.py is not
    # yet implemented.
    from src.builder import store as builder_store

    with pytest.raises(PathError):
        builder_store.save_draft("victim-page", "{}")

    assert (
        outside_target.read_text(encoding="utf-8") == "PRE-EXISTING SECRET"
    ), "a write through the planted symlink must never reach the file it points to"
