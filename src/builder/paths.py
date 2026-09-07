"""The one place a filesystem path is derived from a request field.

Batch 11 of docs/features/page_builder.md. Every path the builder service
opens -- for reading or for writing -- comes out of this module, and the
allowlist is enforced here and nowhere else (the batch spec's
"Path-allowlist decision"). A caller that builds a path by string
concatenation has left the allowlist behind, which is the whole failure mode
this module exists to make impossible.

Two independent layers, on purpose:

1. `validate_page_id` / `validate_version` reject anything that is not a bare
   lowercase slug or a number of exactly four numerals, BEFORE a path
   exists. Traversal is
   impossible by construction: `..`, `/`, `\\`, `.`, `%` and NUL are all
   outside the character class, so there is nothing to normalise away.
2. `assert_contained` re-checks the finished path with `os.path.realpath`
   against the realpath of `PAGES_ROOT`. Layer 1 cannot see a symlink someone
   planted at `config/pages/{page_id}/` pointing at `/etc`; layer 2 can, and
   refuses the write. Defence in depth that is redundant on purpose --
   Batch 10's protocol-relative-URL finding is what a single guard that
   "reads correctly" is worth.

Percent-decoding is NOT done here. The service decodes a query string exactly
once and hands the decoded value to `validate_page_id`, so `..%2f` and
`%2e%2e` arrive as `../` and `..` and are rejected by the regex. Decoding
twice anywhere would reintroduce the hole.

`PAGES_ROOT` is derived from this file's location, never from a request. No
request field may set, override or hint at the root.
"""

import os
import re
from pathlib import Path

#: config/pages/, derived from the repository root. Never from a request.
PAGES_ROOT = Path(__file__).resolve().parents[2] / "config" / "pages"

#: A page id is a bare slug: lowercase, digits, hyphens, 1-64 characters, and
#: it may not start with a hyphen (which would also make it look like a CLI
#: flag anywhere this value is printed).
PAGE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")

#: Exactly four digits, so lexical and numeric order agree -- `{4,}` would
#: sort `10000` before `9999` and hand the wrong snapshot back to `restore`.
VERSION_RE = re.compile(r"^[0-9]{4}$")

MAX_VERSION = 9999

DRAFT_FILENAME = "draft.json"
PUBLISHED_FILENAME = "published.json"
VERSIONS_DIRNAME = "versions"

#: The only two kinds of snapshot the service ever writes.
VERSION_KINDS = ("published", "draft")

WHICH_VALUES = ("draft", "published")


class PathError(Exception):
    """A request tried to name something outside the allowlist.

    `.code` is one of the service's response codes so the HTTP layer can map
    it without re-deciding: a bad page id, a bad version, a bad `which` and a
    containment failure are all `bad_request` -- the client asked for
    something it is not allowed to ask for, and the reply says no more than
    that.
    """

    def __init__(self, message: str, code: str = "bad_request"):
        super().__init__(message)
        self.code = code


def _require_clean_str(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise PathError(f"{field} must be a string")
    if "\x00" in value:
        # Explicit, ahead of the regex: a NUL truncates the path in any C
        # library underneath us, so `good\x00/../../etc` must never reach a
        # syscall even if some future regex change would let it through.
        raise PathError(f"{field} contains a NUL byte")
    return value


def validate_page_id(page_id: object) -> str:
    """The page id, or raise. The only accepted form is `PAGE_ID_RE`."""
    value = _require_clean_str(page_id, "page_id")
    if not PAGE_ID_RE.fullmatch(value):
        raise PathError("page_id must match ^[a-z0-9][a-z0-9-]{0,63}$")
    return value


def validate_version(version: object) -> str:
    """The zero-padded four-numeral version, or raise."""
    value = _require_clean_str(version, "version")
    if not VERSION_RE.fullmatch(value):
        raise PathError("version must be exactly four digits")
    return value


def validate_which(which: object) -> str:
    """`"draft"` or `"published"`, or raise."""
    value = _require_clean_str(which, "which")
    if value not in WHICH_VALUES:
        raise PathError("which must be 'draft' or 'published'")
    return value


def assert_contained(path: Path) -> Path:
    """Return `path` if its realpath is inside the realpath of `PAGES_ROOT`.

    Compared on `os.path.realpath` of both sides, so a symlink anywhere along
    the way -- including one planted inside `config/pages/{page_id}/` after
    the id passed layer 1 -- is resolved before the comparison rather than
    after the write.
    """
    root = os.path.realpath(PAGES_ROOT)
    resolved = os.path.realpath(path)
    if resolved != root and not resolved.startswith(root + os.sep):
        raise PathError("resolved path is outside the page root")
    return path


def page_dir(page_id: str) -> Path:
    return assert_contained(PAGES_ROOT / validate_page_id(page_id))


def draft_path(page_id: str) -> Path:
    return assert_contained(PAGES_ROOT / validate_page_id(page_id) / DRAFT_FILENAME)


def published_path(page_id: str) -> Path:
    return assert_contained(PAGES_ROOT / validate_page_id(page_id) / PUBLISHED_FILENAME)


def document_path(page_id: str, which: str) -> Path:
    """`draft.json` or `published.json`, chosen by the request's `which`."""
    kind = validate_which(which)
    return draft_path(page_id) if kind == "draft" else published_path(page_id)


def versions_dir(page_id: str) -> Path:
    return assert_contained(PAGES_ROOT / validate_page_id(page_id) / VERSIONS_DIRNAME)


def version_path(page_id: str, version: str, kind: str = "published") -> Path:
    """`versions/{kind}-NNNN.json`.

    `kind` is a caller-side constant, not a request field, and is checked
    anyway: a future caller passing a value through from a request must not be
    the moment this becomes a filename injection.
    """
    if kind not in VERSION_KINDS:
        raise PathError(f"version kind must be one of {VERSION_KINDS}")
    name = f"{kind}-{validate_version(version)}.json"
    return assert_contained(PAGES_ROOT / validate_page_id(page_id) / VERSIONS_DIRNAME / name)
