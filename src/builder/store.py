"""Every filesystem write the builder makes, and the publish transaction.

Batch 11 of docs/features/page_builder.md. Three properties this module owns,
each of which is a claude.md rule rather than a preference:

* **A write is atomic** (rule 33, invariant 10). Content goes to a temp file in
  the *same directory*, is `fsync`ed, and is then moved into place with
  `os.replace`. A crash leaves either the whole old file or the whole new one;
  a half-written `published.json` is not a reachable state. Same directory
  matters: `os.replace` is only atomic within one filesystem.
* **A failed publish changes nothing** (rule 33). The ordering in `publish` is
  not the obvious one and is spelled out there.
* **Publishing never commits** (rule 34). Nothing that can spawn a process is
  imported here at all, so running a shell or a version-control command is
  unreachable
  rather than merely avoided.

Serialization is not done here. The caller hands in text produced by
`src.pages.dumps`, the one canonical form (rule 35). This module never calls
`json.dumps` on a page document.
"""

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from src.builder.paths import (
    MAX_VERSION,
    PAGE_ID_RE,
    PAGES_ROOT,
    VERSION_RE,
    PathError,
    assert_contained,
    document_path,
    draft_path,
    page_dir,
    published_path,
    validate_page_id,
    validate_version,
    version_path,
    versions_dir,
)


class StoreError(Exception):
    """A filesystem operation failed in a way the caller must not paper over.

    `.code` is a service response code. `version_space_exhausted` is a loud
    refusal by design (spec rev 2, item 5): wrapping back to `0001` would
    overwrite a snapshot, and snapshots are never reused and never deleted by
    the service.
    """

    def __init__(self, message: str, code: str = "internal_error"):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class WriteResult:
    """`bytes` and `sha256` OF THE FILE BYTES ACTUALLY WRITTEN.

    Not of the request body, not of the in-memory document: without that
    definition the transaction tests assert nothing (spec rev 2, item 16).
    """

    bytes: int
    sha256: str


def _digest(data: bytes) -> WriteResult:
    return WriteResult(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def _fsync_dir(directory: Path) -> None:
    """Durably record the rename itself, not just the file contents.

    Without this the `os.replace` can be lost in a crash even though the data
    it moved was `fsync`ed. Best-effort: some filesystems refuse `O_RDONLY`
    fsync on a directory, and failing the write for that would be worse than
    the risk.
    """
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:  # pragma: no cover - platform dependent
        return
    try:
        os.fsync(fd)
    except OSError:  # pragma: no cover - platform dependent
        pass
    finally:
        os.close(fd)


def _unlink_quietly(path: Path | str | None) -> None:
    if path is None:
        return
    try:
        os.unlink(path)
    except OSError:  # pragma: no cover - already gone
        pass


def _write_temp(directory: Path, prefix: str, data: bytes) -> str:
    """A fsynced temp file in `directory`, returned by path. Never left open."""
    assert_contained(directory)
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=prefix, suffix=".json")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        _unlink_quietly(tmp)
        raise
    return tmp


def _ensure_page_dir(page_id: str) -> Path:
    directory = page_dir(page_id)
    directory.mkdir(parents=True, exist_ok=True)
    # Re-check AFTER mkdir: the check before it saw whatever was there then,
    # and the containment property we need is the one that holds at write
    # time.
    assert_contained(directory)
    return directory


def atomic_write_text(path: Path, text: str) -> WriteResult:
    """Write `text` to `path` atomically; return the bytes actually written."""
    assert_contained(path)
    directory = Path(path).parent
    assert_contained(directory)
    data = text.encode("utf-8")
    tmp = _write_temp(directory, ".tmp-write-", data)
    try:
        os.replace(tmp, path)
    except BaseException:
        _unlink_quietly(tmp)
        raise
    _fsync_dir(directory)
    return _digest(data)


def _read_bytes(path: Path) -> bytes | None:
    """The file's bytes, or None if it does not exist.

    A symlink is refused rather than followed: `config/pages/{id}/draft.json`
    pointing at `/etc/shadow` would otherwise be readable through the API by
    anyone who can plant it.
    """
    assert_contained(path)
    try:
        if os.path.islink(path):
            raise PathError("refusing to read through a symlink")
        with open(path, "rb") as handle:
            return handle.read()
    except FileNotFoundError:
        return None
    except IsADirectoryError:
        return None


def _is_real_file(builder, page_id: str) -> bool:
    """Whether that document exists as a regular file inside the tree.

    A symlink, or a path that resolves outside the root, reports False rather
    than raising: one page with a planted `draft.json -> /etc/passwd` must not
    take down the whole listing, and reporting it as present would offer the
    UI a document no read route will serve.
    """
    try:
        path = builder(page_id)
    except PathError:
        return False
    return path.is_file() and not os.path.islink(path)


def list_pages() -> list[dict]:
    """Every page directory under the root, sorted by page id.

    A directory whose name is not a legal page id is skipped, not reported:
    the API's own vocabulary cannot express it, so offering it would produce a
    listing entry no other route accepts.
    """
    pages: list[dict] = []
    try:
        names = sorted(os.listdir(assert_contained(PAGES_ROOT)))
    except FileNotFoundError:
        return []
    for name in names:
        if not PAGE_ID_RE.fullmatch(name):
            continue
        try:
            directory = page_dir(name)
        except PathError:
            continue
        if not directory.is_dir() or os.path.islink(directory):
            continue
        pages.append(
            {
                "page_id": name,
                "has_draft": _is_real_file(draft_path, name),
                "has_published": _is_real_file(published_path, name),
                "versions": len(list_versions(name)),
            }
        )
    return pages


def _list_snapshots(page_id: str, kind: str) -> list[str]:
    directory = versions_dir(page_id)
    try:
        names = os.listdir(assert_contained(directory))
    except (FileNotFoundError, NotADirectoryError):
        return []
    prefix = f"{kind}-"
    found = []
    for name in names:
        if not name.startswith(prefix) or not name.endswith(".json"):
            continue
        number = name[len(prefix) : -len(".json")]
        if VERSION_RE.fullmatch(number):
            found.append(number)
    # Four fixed digits, so lexical order IS numeric order.
    return sorted(found)


def list_versions(page_id: str) -> list[str]:
    """The published snapshots for a page, oldest first."""
    return _list_snapshots(validate_page_id(page_id), "published")


def list_draft_versions(page_id: str) -> list[str]:
    """The draft snapshots `restore` leaves behind, oldest first."""
    return _list_snapshots(validate_page_id(page_id), "draft")


def read_document_text(page_id: str, which: str) -> str | None:
    """The raw text of `draft.json` or `published.json`, or None if absent.

    Decoding failures are surfaced, not swallowed: a `draft.json` that is not
    UTF-8 is a real problem the caller must report, and returning None would
    make it look like a missing page.
    """
    raw = _read_bytes(document_path(page_id, which))
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise StoreError(f"{which}.json is not valid UTF-8", code="validation_failed") from exc


def save_draft(page_id: str, text: str) -> WriteResult:
    """Write the draft. The ONLY write the editing path performs (rule 32)."""
    page = validate_page_id(page_id)
    _ensure_page_dir(page)
    return atomic_write_text(draft_path(page), text)


def _next_version(page_id: str, kind: str) -> str:
    existing = _list_snapshots(page_id, kind)
    nxt = int(existing[-1]) + 1 if existing else 1
    if nxt > MAX_VERSION:
        raise StoreError(
            f"{kind} snapshots for this page are exhausted at {MAX_VERSION}",
            code="version_space_exhausted",
        )
    return f"{nxt:04d}"


def _claim_snapshot(page_id: str, kind: str, tmp: str) -> str:
    """Move `tmp` into `versions/{kind}-NNNN.json`, claiming the number.

    `os.link` is the claim: it fails with `FileExistsError` if the target is
    already there, which is the same exclusivity `O_EXCL` gives and moves the
    content in one step. The service is single-threaded, so this is belt and
    braces against a second process, not against itself.
    """
    directory = versions_dir(page_id)
    directory.mkdir(parents=True, exist_ok=True)
    assert_contained(directory)
    for _ in range(MAX_VERSION):
        version = _next_version(page_id, kind)
        target = version_path(page_id, version, kind=kind)
        try:
            os.link(tmp, target)
        except FileExistsError:  # pragma: no cover - only under a race
            continue
        except OSError as exc:  # pragma: no cover - filesystem without hardlinks
            raise StoreError(f"could not claim a {kind} snapshot: {exc.strerror}") from exc
        _unlink_quietly(tmp)
        _fsync_dir(directory)
        return version
    raise StoreError(  # pragma: no cover - only under a sustained race
        f"could not claim a {kind} snapshot", code="version_space_exhausted"
    )


def publish(page_id: str, text: str) -> tuple[WriteResult, str | None]:
    """Promote `text` to `published.json`; return (result, previous version).

    THE ORDERING IS THE TRANSACTION, and this is its second revision.

      1. read the current `published.json` bytes,
      2. write the NEW content to `.tmp-publish-*` in the page directory and
         fsync it,
      3. write the OLD bytes to `.tmp-snapshot-*` and fsync it,
      4. claim `versions/published-NNNN.json` for the snapshot (moves
         `.tmp-snapshot-*` there -- `published.json` is not touched yet),
      5. `os.replace` the new content over `published.json`,
      6. unlink the publish temp.

    Claim BEFORE replace, not after. The first revision of this function
    replaced first and claimed second, on the reasoning that rule 33 is about
    a FAILED publish leaving the old version intact, not about undoing a
    completed one -- true as far as it went, but it missed that a failure at
    the claim step still answers the HTTP caller with a failure code while
    `published.json` had already changed. A security-red-team pass reproduced
    it via the documented `version_space_exhausted` refusal: the caller is
    told "publish failed" and the site is different anyway, with the
    previous version's bytes sitting in an unlinked temp file the `except`
    branch had already deleted. That is worse than "no stray snapshot" was
    good; a lost snapshot is recoverable by re-publishing, a false failure
    report is not caught by anything.

    Claiming first cannot itself corrupt `published.json`, because
    `_claim_snapshot` only names a file inside `versions/` and never opens
    `target`. So every failure up to and including a full `versions/`
    (`version_space_exhausted`) now happens before step 5, unlinks both
    temps, and leaves `published.json` byte-identical -- which is what "a
    failed publish changes nothing" has to mean if the response says it
    failed. The one case this does NOT cover is `os.replace` itself failing
    at step 5, after the snapshot is already claimed: that leaves a snapshot
    of content that is still, in fact, published. Recorded as the residual
    risk rather than hidden, because eliminating it needs a rename that is
    atomic across both files at once, which the filesystem does not offer.

    The returned version is the snapshot holding the PREVIOUS published
    content, so the FIRST publish of a page writes no snapshot and returns
    None. There is no previous content to keep, and a snapshot of nothing
    would be a lie about what can be restored.

    Publishing writes files. It does not commit, push or tag (rule 34).
    """
    page = validate_page_id(page_id)
    directory = _ensure_page_dir(page)
    target = published_path(page)

    old_bytes = _read_bytes(target)
    data = text.encode("utf-8")

    tmp_new: str | None = None
    tmp_old: str | None = None
    try:
        tmp_new = _write_temp(directory, ".tmp-publish-", data)
        if old_bytes is not None:
            tmp_old = _write_temp(directory, ".tmp-snapshot-", old_bytes)
    except BaseException:
        _unlink_quietly(tmp_new)
        _unlink_quietly(tmp_old)
        raise

    version = None
    if tmp_old is not None:
        try:
            version = _claim_snapshot(page, "published", tmp_old)
        except BaseException:
            # published.json has not been touched -- the claim runs entirely
            # inside versions/. A failure here (e.g. version_space_exhausted)
            # must leave the site exactly as it was before this call, since
            # the caller is about to be told the publish failed.
            tmp_old = None  # _claim_snapshot already unlinks tmp on any path out
            _unlink_quietly(tmp_new)
            raise

    try:
        os.replace(tmp_new, target)
    except BaseException:
        # The snapshot is already claimed at this point. Leaving it in place
        # is deliberate: published.json still holds the OLD content (this
        # replace is what was going to change it and didn't), so the
        # snapshot correctly describes what is still live. Removing it here
        # would delete the one available restore point for no reason.
        _unlink_quietly(tmp_new)
        raise
    tmp_new = None
    _fsync_dir(directory)

    return _digest(data), version


def restore(page_id: str, version: str) -> tuple[WriteResult, str]:
    """Restore a published snapshot INTO THE DRAFT. Never into published.

    Rule 32 read literally: bringing back an old version is an edit, and
    making it public again needs an explicit `publish`. `published.json` is
    not opened for writing anywhere in this function.

    The current `draft.json` is snapshotted to `versions/draft-NNNN.json`
    first (spec rev 2, item 11), so an accidental restore does not destroy
    in-progress work irreversibly.

    Returns (result, version) with the version RESTORED, echoed back for the
    response.
    """
    page = validate_page_id(page_id)
    wanted = validate_version(version)
    directory = _ensure_page_dir(page)

    snapshot = _read_bytes(version_path(page, wanted, kind="published"))
    if snapshot is None:
        raise StoreError(f"no published snapshot {wanted} for this page", code="not_found")

    current = _read_bytes(draft_path(page))
    if current is not None:
        tmp_draft = _write_temp(directory, ".tmp-draft-snapshot-", current)
        try:
            _claim_snapshot(page, "draft", tmp_draft)
        except BaseException:
            _unlink_quietly(tmp_draft)
            raise

    # The snapshot's own bytes, written back verbatim: it was produced by
    # `src.pages.dumps` when it was taken, so it is already canonical, and
    # re-serializing it here would be a second chance to change bytes that
    # rule 35 says must not change.
    return atomic_write_text(draft_path(page), snapshot.decode("utf-8")), wanted


def read_version_text(page_id: str, version: str, kind: str = "published") -> str | None:
    """The raw text of a snapshot, or None if that version does not exist."""
    raw = _read_bytes(version_path(page_id, version, kind=kind))
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise StoreError(
            f"snapshot {kind}-{version}.json is not valid UTF-8", code="validation_failed"
        ) from exc
