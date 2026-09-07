"""Structural validation and resource-exhaustion guards for a page document.

Also the home of this package's error primitives (`PageDocumentError`,
`PageValidationError`, `ERROR_CODES`) and its limits. They live here, and not
in `document.py`, only to keep the import graph acyclic: `metadata.py`,
`registry.py`, `semantics.py`, `serialize.py` and `migrations.py` all need
them, and `document.py` imports every one of those. `src/pages/__init__.py`
re-exports the public names, so callers never need to know which module they
were defined in.

Guard ordering matters and is not the obvious one (a byte cap applies to raw
text, a depth cap needs a parsed tree). The required order, executed by
`document.load_document`:

1. `check_raw_size()` on the raw text, before parsing at all.
2. `serialize.loads()` -- `json.loads` wrapped so CPython's `RecursionError`
   (raised somewhere around 1000 levels of nesting) becomes a domain error and
   not a crash.
3. `check_tree_limits()` on the parsed tree: depth, section count, block
   count, string length.
4. `check_schema_version()`, then `migrations.migrate()`.
5. `validate_structure()` -- jsonschema -- last.

Anything that has its own error code in `ERROR_CODES` is enforced in Python
(here or in `semantics.py`), never by the JSON Schema, because a JSON Schema
failure can only ever be reported as `schema_violation`. That is why
`page_document.schema.json` has no `enum` on `page_type`, no `minimum` on
`schema_version`, and no bounds on the grid coordinates.
"""

import json
from pathlib import Path

import jsonschema

CURRENT_SCHEMA_VERSION = 1

#: The oldest `schema_version` `migrations.migrate()` can still bring forward.
#: 1 is the first published version, so today it is the current one too.
MIN_MIGRATABLE_SCHEMA_VERSION = 1

# Resource-exhaustion caps. Chosen far above any real page (Batch 0's largest
# root page is a few dozen blocks) and far below anything that hurts a
# validator or a reviewer's diff. Recorded here rather than in a doc so the
# numbers and their justification cannot drift apart.
MAX_DOCUMENT_BYTES = 1_048_576  # 1 MiB of raw UTF-8 text
MAX_SECTIONS = 64
MAX_BLOCKS = 512  # whole document, not per section
MAX_DEPTH = 16  # nesting levels of dict/list, root counted as 1
MAX_STRING_LENGTH = 8192  # any single string value

#: Grid units, never pixels (block_contract.md). A layout survives a redesign
#: of the underlying CSS grid because it stores columns, not coordinates.
GRID_COLUMNS = {"desktop": 12, "tablet": 8, "mobile": 4}

BREAKPOINTS = ("desktop", "tablet", "mobile")

#: Closed vocabulary. Tests assert these strings, so they are contract, not
#: cosmetics: nothing may be added here without a batch spec saying so.
ERROR_CODES = frozenset(
    {
        "duplicate_block_id",
        "unknown_block_type",
        "unsupported_block_version",
        "invalid_route",
        "invalid_grid_position",
        "grid_overlap",
        "unknown_indicator",
        "invalid_nis_code",
        "retired_nis_code",
        "geo_level_mismatch",
        "forbidden_aggregation",
        "missing_accessible_name",
        "unsafe_content",
        "unknown_page_type",
        "schema_violation",
        "unsupported_schema_version",
        "document_too_large",
        "too_many_sections",
        "too_many_blocks",
        "too_deep",
        "string_too_long",
    }
)

#: Guard codes short-circuit everything after them: once a document is too big
#: or too deep, walking it further is the denial of service, not a diagnosis.
GUARD_CODES = frozenset(
    {
        "document_too_large",
        "too_many_sections",
        "too_many_blocks",
        "too_deep",
        "string_too_long",
    }
)

ROOT_PATH = "(root)"

SCHEMA_PATH = (
    Path(__file__).resolve().parents[2] / "docs" / "features" / "page_document.schema.json"
)


class PageDocumentError(Exception):
    """Base for every error this package raises."""


class PageValidationError(PageDocumentError):
    """One rejection, carrying a code from `ERROR_CODES`, a document path and
    a human-readable message.

    A rejection, never a warning: `block_contract.md`'s semantic list rejects,
    and Batch 9 is the only place these checks exist.
    """

    def __init__(self, code: str, path: str, message: str):
        if code not in ERROR_CODES:
            raise ValueError(f"unknown error code {code!r} (ERROR_CODES is closed)")
        self.code = code
        self.path = path or ROOT_PATH
        self.message = message
        super().__init__(f"{self.path}: {code} — {message}")

    @property
    def sort_key(self) -> tuple[str, str, str]:
        return (self.path, self.code, self.message)

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"PageValidationError(code={self.code!r}, path={self.path!r})"


def format_path(parts) -> str:
    """A document path as `sections/0/blocks/1/layout/desktop`, or `(root)`."""
    rendered = "/".join(str(p) for p in parts)
    return rendered or ROOT_PATH


def sort_errors(errors: list[PageValidationError]) -> list[PageValidationError]:
    """Deterministic order by (path, code, message), so two runs of the
    validator over the same document report identically (claude.md rule 35)."""
    return sorted(errors, key=lambda e: e.sort_key)


def load_schema() -> dict:
    """The page-document JSON Schema, read from docs/features/."""
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


_SCHEMA: dict = load_schema()

# One validator instance for the process: jsonschema compiles the schema on
# first use, and §16 asks for < 50 ms per document.
_STRUCTURE_VALIDATOR = jsonschema.Draft202012Validator(_SCHEMA)


def check_raw_size(text: str) -> list[PageValidationError]:
    """Guard step 1: raw UTF-8 byte length, before any parsing."""
    size = len(text.encode("utf-8"))
    if size > MAX_DOCUMENT_BYTES:
        return [
            PageValidationError(
                "document_too_large",
                ROOT_PATH,
                f"document is {size} bytes, the limit is {MAX_DOCUMENT_BYTES}",
            )
        ]
    return []


def _iter_nodes(doc):
    """Every (path, node) in the tree, depth-first, iteratively.

    Iteratively on purpose: a recursive walker would hit CPython's own
    recursion limit on exactly the deeply-nested document this guard exists to
    reject, turning a rejection into a crash.
    """
    stack = [((), doc, 1)]
    while stack:
        path, node, depth = stack.pop()
        yield path, node, depth
        if isinstance(node, dict):
            for key, value in node.items():
                stack.append(((*path, key), value, depth + 1))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                stack.append(((*path, index), value, depth + 1))


def check_tree_limits(doc) -> list[PageValidationError]:
    """Guard step 3: depth, section count, block count, string length.

    Runs before jsonschema, so it must assume nothing about the shape of what
    it was handed.
    """
    errors: list[PageValidationError] = []

    if isinstance(doc, dict):
        sections = doc.get("sections")
        if isinstance(sections, list):
            if len(sections) > MAX_SECTIONS:
                errors.append(
                    PageValidationError(
                        "too_many_sections",
                        "sections",
                        f"{len(sections)} sections, the limit is {MAX_SECTIONS}",
                    )
                )
            blocks = 0
            for section in sections:
                if isinstance(section, dict) and isinstance(section.get("blocks"), list):
                    blocks += len(section["blocks"])
            if blocks > MAX_BLOCKS:
                errors.append(
                    PageValidationError(
                        "too_many_blocks",
                        "sections",
                        f"{blocks} blocks in the document, the limit is {MAX_BLOCKS}",
                    )
                )

    deepest_path = None
    deepest = 0
    for path, node, depth in _iter_nodes(doc):
        if depth > deepest:
            deepest, deepest_path = depth, path
        if isinstance(node, str) and len(node) > MAX_STRING_LENGTH:
            errors.append(
                PageValidationError(
                    "string_too_long",
                    format_path(path),
                    f"string is {len(node)} characters, the limit is {MAX_STRING_LENGTH}",
                )
            )
    if deepest > MAX_DEPTH:
        errors.append(
            PageValidationError(
                "too_deep",
                format_path(deepest_path),
                f"nesting reaches {deepest} levels, the limit is {MAX_DEPTH}",
            )
        )

    return sort_errors(errors)


def check_schema_version(doc) -> list[PageValidationError]:
    """Guard step 4: is this a version we can understand at all?

    Too new is rejected rather than guessed at; too old is rejected only if
    `migrations.py` has no path forward from it.
    """
    if not isinstance(doc, dict):
        return [PageValidationError("schema_violation", ROOT_PATH, "document is not an object")]
    version = doc.get("schema_version")
    if not isinstance(version, int) or isinstance(version, bool):
        return [
            PageValidationError(
                "unsupported_schema_version",
                "schema_version",
                f"schema_version must be an integer, got {version!r}",
            )
        ]
    if version > CURRENT_SCHEMA_VERSION:
        return [
            PageValidationError(
                "unsupported_schema_version",
                "schema_version",
                f"schema_version {version} is newer than this build understands "
                f"({CURRENT_SCHEMA_VERSION}); refusing to guess",
            )
        ]
    if version < MIN_MIGRATABLE_SCHEMA_VERSION:
        return [
            PageValidationError(
                "unsupported_schema_version",
                "schema_version",
                f"schema_version {version} is older than the oldest migratable version "
                f"({MIN_MIGRATABLE_SCHEMA_VERSION})",
            )
        ]
    return []


def validate_structure(doc) -> list[PageValidationError]:
    """Guard step 5: the JSON Schema, last.

    Every failure is a `schema_violation` -- that is the whole reason anything
    with its own error code is checked in Python instead.
    """
    errors = [
        PageValidationError("schema_violation", format_path(e.absolute_path), e.message)
        for e in _STRUCTURE_VALIDATOR.iter_errors(doc)
    ]
    return sort_errors(errors)


def validate_against(schema: dict, instance, path_prefix: tuple) -> list[PageValidationError]:
    """Validate a sub-document (a block's props) against a registry-supplied
    schema, reporting paths relative to the whole document."""
    validator = jsonschema.Draft202012Validator(schema)
    errors = [
        PageValidationError(
            "schema_violation",
            format_path((*path_prefix, *e.absolute_path)),
            e.message,
        )
        for e in validator.iter_errors(instance)
    ]
    return sort_errors(errors)
