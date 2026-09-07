"""Batch 11 builder-service store layer: save/publish/restore and the
atomicity of every write (batch-11 spec, Tests items 4-6, and rev 2 P1 #5 /
P2 #11 / #16).

This file exercises `src/builder/store.py` directly -- no live socket -- so
these are filesystem-behaviour tests: given a temp `PAGES_ROOT`, does the
right byte sequence land on disk, does a mid-write crash ever leave a partial
file visible, and does `os.replace` (not a truncate-and-rewrite) perform every
swap. HTTP-shaped behaviour (422s, response bodies) is covered separately in
test_builder_api.py; this file cares only about what ends up on disk.

`bytes` and `sha256` mean "of the file bytes actually written" (rev 2 #16):
every assertion below reads the real file back off disk and hashes it, rather
than trusting a returned value at face value.
"""

from __future__ import annotations

import hashlib
import os

import pytest

from src.builder import paths as builder_paths
from src.builder import store as builder_store
from src.pages import dumps
from tests.fixtures.pages import builders


def _sha256(text_or_bytes) -> str:
    data = text_or_bytes.encode("utf-8") if isinstance(text_or_bytes, str) else text_or_bytes
    return hashlib.sha256(data).hexdigest()


def _tmp_files_in(directory) -> list:
    if not directory.exists():
        return []
    return list(directory.rglob(".tmp-*"))


@pytest.fixture()
def pages_root(tmp_path, monkeypatch):
    """Both `paths.PAGES_ROOT` and `store.PAGES_ROOT` must be patched: store.py
    imports the name by value (`from src.builder.paths import PAGES_ROOT`),
    so it holds its own separate binding that patching only `paths` does not
    reach."""
    root = tmp_path / "pages"
    root.mkdir()
    monkeypatch.setattr(builder_paths, "PAGES_ROOT", root)
    monkeypatch.setattr(builder_store, "PAGES_ROOT", root)
    return root


@pytest.fixture()
def doc_text() -> str:
    return dumps(builders.minimal_valid_document())


@pytest.fixture()
def other_doc_text() -> str:
    doc = builders.minimal_valid_document()
    doc["seo"]["title"]["en"] = "A different document entirely"
    return dumps(doc)


# --- atomic_write_text: the primitive every write goes through --------------


def test_atomic_write_text_writes_exact_bytes_and_reports_them(pages_root, doc_text):
    target = pages_root / "atomic-target.json"
    result = builder_store.atomic_write_text(target, doc_text)
    on_disk = target.read_bytes()
    assert on_disk == doc_text.encode("utf-8")
    assert result.bytes == len(on_disk)
    assert result.sha256 == hashlib.sha256(on_disk).hexdigest()


def test_atomic_write_text_leaves_no_tmp_file_on_success(pages_root, doc_text):
    target = pages_root / "atomic-target.json"
    builder_store.atomic_write_text(target, doc_text)
    assert _tmp_files_in(pages_root) == []


def test_atomic_write_text_uses_os_replace_to_perform_the_swap(pages_root, doc_text, monkeypatch):
    """Proves the swap mechanism is `os.replace`, not a truncate-and-rewrite
    of the destination in place -- spec Tests item 6's second half."""
    target = pages_root / "atomic-target.json"
    calls = []
    real_replace = os.replace

    def spy_replace(src, dst):
        calls.append((src, dst))
        return real_replace(src, dst)

    monkeypatch.setattr(builder_store.os, "replace", spy_replace)
    builder_store.atomic_write_text(target, doc_text)
    assert calls, "atomic_write_text must call os.replace to publish the temp file"
    assert calls[-1][1] == target or os.fspath(calls[-1][1]) == os.fspath(target)


def test_atomic_write_text_crash_before_replace_leaves_destination_untouched(
    pages_root, doc_text, other_doc_text, monkeypatch
):
    """Simulated crash mid-write: the write raises AFTER the temp file exists
    but BEFORE os.replace runs. The destination must be untouched (old
    content, or absent if it never existed) and no partial file visible."""
    target = pages_root / "atomic-target.json"
    target.write_text(other_doc_text, encoding="utf-8")
    original_sha = _sha256(other_doc_text)

    def exploding_replace(*_a, **_kw):
        raise OSError("simulated crash before replace")

    monkeypatch.setattr(builder_store.os, "replace", exploding_replace)

    with pytest.raises(OSError):
        builder_store.atomic_write_text(target, doc_text)

    assert (
        _sha256(target.read_bytes()) == original_sha
    ), "destination must still hold the OLD content after a crash before replace"
    # No partial/new-content file visible under any name.
    for path in pages_root.rglob("*"):
        if path.is_file() and path != target:
            assert path.read_bytes() != doc_text.encode(
                "utf-8"
            ), f"new content leaked into {path} despite the simulated crash"


def test_atomic_write_text_crash_before_replace_creates_nothing_when_no_prior_file(
    pages_root, doc_text, monkeypatch
):
    target = pages_root / "atomic-target.json"
    assert not target.exists()

    def exploding_replace(*_a, **_kw):
        raise OSError("simulated crash before replace")

    monkeypatch.setattr(builder_store.os, "replace", exploding_replace)

    with pytest.raises(OSError):
        builder_store.atomic_write_text(target, doc_text)

    assert not target.exists(), "no prior draft/published file must still not exist after a crash"


# --- save_draft: draft.json only, nothing else ------------------------------


def test_save_draft_writes_only_draft_json(pages_root, doc_text):
    result = builder_store.save_draft("home", doc_text)
    draft = builder_paths.draft_path("home")
    assert draft.read_text(encoding="utf-8") == doc_text
    assert result.sha256 == _sha256(draft.read_bytes())
    assert result.bytes == len(draft.read_bytes())
    published = builder_paths.published_path("home")
    assert not published.exists(), "save_draft must never create published.json"


def test_save_draft_overwrites_a_prior_draft_atomically(pages_root, doc_text, other_doc_text):
    builder_store.save_draft("home", other_doc_text)
    builder_store.save_draft("home", doc_text)
    draft = builder_paths.draft_path("home")
    assert draft.read_text(encoding="utf-8") == doc_text
    assert _tmp_files_in(pages_root) == []


# --- publish: snapshot-then-replace ordering (rev 2 P1 #5) -------------------


def test_first_publish_of_a_page_writes_published_with_no_prior_snapshot(pages_root, doc_text):
    """rev 2 #5: "the first publish of a page has no previous content, writes
    no snapshot, and returns version: null."""
    result, version = builder_store.publish("home", doc_text)
    published = builder_paths.published_path("home")
    assert published.read_text(encoding="utf-8") == doc_text
    assert result.sha256 == _sha256(published.read_bytes())
    assert version is None
    versions_dir = builder_paths.versions_dir("home")
    assert not versions_dir.exists() or list(versions_dir.iterdir()) == []


def test_second_publish_snapshots_the_previous_content_before_replacing(
    pages_root, doc_text, other_doc_text
):
    builder_store.publish("home", other_doc_text)  # first publish: version None
    result, version = builder_store.publish("home", doc_text)  # second publish

    published = builder_paths.published_path("home")
    assert (
        published.read_text(encoding="utf-8") == doc_text
    ), "published.json now holds the NEW content"

    assert (
        version is not None
    ), "second publish must report the snapshot holding the PREVIOUS content"
    snapshot = builder_paths.version_path("home", version, kind="published")
    assert (
        snapshot.read_text(encoding="utf-8") == other_doc_text
    ), "the version snapshot must hold the OLD (previous) content, not the new one"


def test_publish_leaves_no_tmp_file_on_success(pages_root, doc_text, other_doc_text):
    builder_store.publish("home", other_doc_text)
    builder_store.publish("home", doc_text)
    assert _tmp_files_in(pages_root) == []


def test_failed_publish_when_the_replace_itself_fails_leaves_published_byte_identical(
    pages_root, doc_text, other_doc_text, monkeypatch
):
    """The transaction was reordered after a security-red-team pass found the
    original ordering (replace, then claim the snapshot) let a FAILED claim
    -- reachable via the documented version_space_exhausted refusal -- leave
    published.json already changed while the HTTP caller was told the
    publish failed. The fix claims the snapshot BEFORE os.replace, so
    published.json is untouched by every failure up to and including a full
    versions/ directory.

    That reordering has a accepted residual case, tested here: if os.replace
    ITSELF fails, it fails AFTER the snapshot is already claimed. This is not
    a violation -- published.json still holds the OLD content (the replace
    that would have changed it didn't happen), and the snapshot correctly
    describes what is still live. So this test's own assertion changed from
    the pre-fix version: no stray snapshot is no longer the right claim,
    because a snapshot legitimately exists now. What must still hold is
    published.json's content and no leftover temp file."""
    builder_store.publish("home", other_doc_text)  # establish a real published.json
    published = builder_paths.published_path("home")
    before_sha = _sha256(published.read_bytes())
    before_bytes = published.read_bytes()

    def failing_replace(src, dst):
        raise OSError("simulated disk failure during publish")

    monkeypatch.setattr(builder_store.os, "replace", failing_replace)

    with pytest.raises(OSError):
        builder_store.publish("home", doc_text)

    assert published.read_bytes() == before_bytes
    assert _sha256(published.read_bytes()) == before_sha
    versions_dir = builder_paths.versions_dir("home")
    snapshots = sorted(p.name for p in versions_dir.iterdir()) if versions_dir.exists() else []
    assert snapshots == ["published-0001.json"], (
        "the snapshot claimed before the failed replace legitimately describes "
        "the content that is still published"
    )
    assert (versions_dir / "published-0001.json").read_bytes() == before_bytes
    assert _tmp_files_in(pages_root) == [], "a failed publish must leave no leftover temp file"


def test_publish_exhausting_four_digit_versions_refuses_loudly_rather_than_wrapping(
    pages_root, doc_text, other_doc_text
):
    """rev 2 #5: version numbers are exactly four digits; exhausting 9999 is a
    loud refusal, never a silent wrap to '0000' or a five-digit overflow.

    ALSO the exact scenario a security-red-team pass used to prove F1: this
    refusal is a real failure the store can hit in ordinary use (9999
    publishes of one page, no filesystem sabotage needed), and the first
    version of this transaction let it through AFTER published.json had
    already been replaced -- an HTTP caller told "publish failed" while the
    site had, in fact, changed. published.json must be byte-identical to
    what it was before this call, not merely "no new snapshot number"."""
    builder_store.publish("home", other_doc_text)  # first publish -> published.json exists
    published = builder_paths.published_path("home")
    before_bytes = published.read_bytes()
    versions_dir = builder_paths.versions_dir("home")
    versions_dir.mkdir(parents=True, exist_ok=True)
    (versions_dir / "published-9999.json").write_text(other_doc_text, encoding="utf-8")

    with pytest.raises(builder_store.StoreError):
        builder_store.publish("home", doc_text)

    assert (
        published.read_bytes() == before_bytes
    ), "a publish that reports failure must not have changed published.json"
    names = {p.name for p in versions_dir.iterdir()}
    assert "published-10000.json" not in names
    assert "published-0000.json" not in names, "must never silently wrap the version counter"


# --- restore: draft only, snapshotting the draft first (rev 2 #11) ----------


def test_restore_writes_draft_json_and_never_published_json(pages_root, doc_text, other_doc_text):
    _result, version = builder_store.publish("home", doc_text)  # version None, no snapshot yet
    _result2, version2 = builder_store.publish("home", other_doc_text)  # snapshots doc_text
    assert version2 is not None

    builder_store.save_draft("home", other_doc_text)  # some in-progress draft edit
    builder_store.restore("home", version2)

    draft = builder_paths.draft_path("home")
    assert (
        draft.read_text(encoding="utf-8") == doc_text
    ), "restore must bring the snapshot into draft.json"

    published = builder_paths.published_path("home")
    assert (
        published.read_text(encoding="utf-8") == other_doc_text
    ), "restore must NEVER touch published.json"


def test_restore_snapshots_the_current_draft_before_overwriting_it(
    pages_root, doc_text, other_doc_text
):
    """rev 2 #11: restore snapshots the current draft first, to
    versions/draft-NNNN.json, so an accidental restore is recoverable."""
    builder_store.publish("home", doc_text)  # first publish: published.json = doc_text, no snapshot
    builder_store.publish(
        "home", other_doc_text
    )  # second publish snapshots doc_text into versions/

    versions_dir = builder_paths.versions_dir("home")
    published_snapshots = sorted(
        p for p in versions_dir.iterdir() if p.name.startswith("published-")
    )
    assert published_snapshots, "expected a published snapshot of doc_text to restore from"
    a_version = published_snapshots[0].name.split("-")[1].split(".")[0]
    assert published_snapshots[0].read_bytes() == doc_text.encode("utf-8")

    builder_store.save_draft("home", other_doc_text)  # in-progress work about to be overwritten
    draft_before_restore = builder_paths.draft_path("home").read_bytes()

    builder_store.restore("home", a_version)

    draft_snapshots = [p for p in versions_dir.iterdir() if p.name.startswith("draft-")]
    assert draft_snapshots, "restore must snapshot the pre-restore draft before overwriting it"
    assert any(
        p.read_bytes() == draft_before_restore for p in draft_snapshots
    ), "the draft snapshot must hold exactly the content the draft had before this restore"


def test_restore_of_unknown_version_raises_rather_than_silently_creating_one(pages_root, doc_text):
    builder_store.save_draft("home", doc_text)
    with pytest.raises(builder_store.StoreError):
        builder_store.restore("home", "9999")


# --- list_pages / list_versions / read_document_text ------------------------


def test_list_pages_reports_draft_published_and_version_counts(
    pages_root, doc_text, other_doc_text
):
    builder_store.save_draft("alpha", doc_text)
    builder_store.publish("alpha", doc_text)
    builder_store.publish("alpha", other_doc_text)  # creates one snapshot

    pages = {p["page_id"]: p for p in builder_store.list_pages()}
    assert "alpha" in pages
    assert pages["alpha"]["has_draft"] is True
    assert pages["alpha"]["has_published"] is True
    assert pages["alpha"]["versions"] == 1


def test_list_pages_reports_no_draft_no_published_for_an_untouched_page(pages_root):
    pages = {p["page_id"]: p for p in builder_store.list_pages()}
    assert "never-touched" not in pages


def test_list_versions_returns_sorted_four_digit_strings(pages_root, doc_text, other_doc_text):
    builder_store.publish("home", doc_text)
    builder_store.publish("home", other_doc_text)
    builder_store.publish("home", doc_text)
    versions = builder_store.list_versions("home")
    assert versions == sorted(versions)
    for v in versions:
        assert len(v) == 4 and v.isdigit()


def test_read_document_text_reads_back_exactly_what_was_written(pages_root, doc_text):
    builder_store.save_draft("home", doc_text)
    assert builder_store.read_document_text("home", "draft") == doc_text


def test_read_document_text_of_a_missing_document_does_not_fabricate_content(pages_root):
    """A syntactically legal but never-written page id returns None -- the
    documented contract ("or None if absent") -- rather than raising or
    fabricating placeholder content."""
    assert builder_store.read_document_text("does-not-exist", "draft") is None
