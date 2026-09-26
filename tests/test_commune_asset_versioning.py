"""commune.html's local asset URLs carry a content-hash query string
(?v=<first 10 hex of the file's sha256>), so a stale cached copy of e.g.
assets/i18n.js is never paired with a newer commune.html.

Why this exists: GitHub Pages serves assets/** with Cache-Control:
max-age=600 (10 minutes). PR #278 shipped a rewritten commune.html together
with new assets/i18n.js keys, with no versioning on either -- for up to 10
minutes after deploy, a browser that already had assets/i18n.js cached kept
using the OLD table against the NEW page, and the maintainer saw raw i18n
keys (v4ParaValue, cpNeighboursValue, v4RankOf...) rendered as literal text.
This PR adds keys too, so without versioning the same failure mode would
repeat on every deploy that touches both files together.

This file is deliberately static and deterministic (rule 35): it hashes the
files on disk and compares against what commune.html's markup says, with no
build step and no browser. A change to any of the 9 versioned assets that
does not also bump its own ?v= in commune.html fails this test -- the
version bump is verified to happen IN THE SAME PR/commit as the content
change, not left to a maintainer to remember.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
COMMUNE_HTML = REPO / "commune.html"

#: Every LOCAL asset URL commune.html's own <link>/<script> tags load --
#: the Google Fonts stylesheet is excluded (external, not ours to version).
VERSIONED_ASSET_PATHS = (
    "assets/belpulse/tokens.css",
    "assets/belpulse/layout.css",
    "assets/belpulse/components.css",
    "assets/commune_map.css",
    "assets/i18n.js",
    "assets/belpulse/shell.js",
    "assets/belpulse/components.js",
    "assets/commune_map.js",
    "assets/belpulse/charts.js",
)


def _page_text() -> str:
    return COMMUNE_HTML.read_text(encoding="utf-8")


def _content_hash(path: str) -> str:
    data = (REPO / path).read_bytes()
    return hashlib.sha256(data).hexdigest()[:10]


def _tag_for(text: str, asset_path: str) -> str:
    """The exact <link.../> or <script...></script> tag referencing
    asset_path, wherever its href/src falls in that tag's attribute list."""
    pattern = re.compile(
        r"<(?:link|script)\b[^>]*(?:href|src)=\"" + re.escape(asset_path) + r"(\?[^\"]*)?\"[^>]*>"
    )
    m = pattern.search(text)
    assert m, f"no <link>/<script> tag found for {asset_path!r} in commune.html"
    return m.group(0)


def test_every_versioned_asset_actually_has_a_tag_in_the_page():
    """A defensive check on this test's own fixture list, not on the page:
    if a future edit removes or renames one of these 9 tags, this test's
    list must be updated in the same PR rather than silently stop checking
    anything for that asset."""
    text = _page_text()
    for asset_path in VERSIONED_ASSET_PATHS:
        _tag_for(text, asset_path)


def test_every_local_asset_url_carries_a_v_query_matching_its_own_content_hash():
    """The load-bearing check: for each of the 9 local assets commune.html
    loads, its own <link>/<script> tag's ?v= must equal the FIRST 10 HEX
    CHARACTERS of that file's own sha256 -- computed here from the file on
    disk, never hand-typed. A change to one of these files that does not
    also bump its ?v= in commune.html fails here, in the same PR."""
    text = _page_text()
    mismatches = []
    for asset_path in VERSIONED_ASSET_PATHS:
        tag = _tag_for(text, asset_path)
        m = re.search(r"\?v=([0-9a-f]+)", tag)
        if not m:
            mismatches.append(f"{asset_path}: no ?v= query on its tag: {tag!r}")
            continue
        found = m.group(1)
        expected = _content_hash(asset_path)
        if found != expected:
            mismatches.append(
                f"{asset_path}: ?v={found} in commune.html, but the file's own "
                f"sha256 gives {expected} -- bump ?v= in the same change"
            )
    assert not mismatches, "\n".join(mismatches)


def test_no_local_asset_url_is_missing_its_version_query():
    """A second, independent sweep in the opposite direction: EVERY href/src
    pointing at assets/** in commune.html must carry a ?v=, not only the 9
    this file's own fixture list names -- catches a NEW asset tag added
    later without versioning, rather than only checking the ones already
    known about."""
    text = _page_text()
    unversioned = []
    for m in re.finditer(r"<(?:link|script)\b[^>]*(?:href|src)=\"(assets/[^\"]+)\"[^>]*>", text):
        url = m.group(1)
        if url.startswith("http"):
            continue
        if "?v=" not in url:
            unversioned.append(url)
    assert not unversioned, f"local asset URL(s) with no ?v= query: {unversioned}"
