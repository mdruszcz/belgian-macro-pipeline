"""Load and validate a page document -- the entry point Batches 10-16 call.

Two functions, deliberately different in temperament:

`validate_document(doc, ...)` COLLECTS every finding and returns them, the
same way `src/validation/config_schema.py` accumulates `iter_errors` output
before raising once. A page author gets one list of everything wrong with
their draft, not a game of whack-a-mole.

`load_document(text, ...)` RAISES on the first finding, because its caller
wanted a usable document and does not have one.

`load_document` also owns the guard ordering from `schema.py`'s docstring: raw
byte cap, guarded parse, tree caps, version check, migrate, JSON Schema,
semantics. `validate_document` starts at the tree caps, because it is handed a
parsed object and there is no raw text left to measure.
"""

from src.pages.metadata import PageMetadata
from src.pages.migrations import migrate
from src.pages.registry import Registry
from src.pages.schema import (
    GUARD_CODES,
    ROOT_PATH,
    PageValidationError,
    check_schema_version,
    check_tree_limits,
    sort_errors,
    validate_structure,
)
from src.pages.semantics import validate_semantics
from src.pages.serialize import loads


def validate_document(
    doc: dict, *, metadata: PageMetadata, registry: Registry
) -> list[PageValidationError]:
    """Every finding for an already-parsed document, sorted by (path, code).

    Returns an empty list for a valid document. Never raises for a bad
    document -- only for a broken environment (a missing registry, malformed
    metadata), which is not the document's fault and must not be reported as
    if it were.
    """
    if not isinstance(doc, dict):
        return [
            PageValidationError(
                "schema_violation",
                ROOT_PATH,
                f"a page document must be an object, got {type(doc).__name__}",
            )
        ]

    guard_errors = check_tree_limits(doc)
    if guard_errors:
        # Stop here on purpose. Once a document is too deep or too large,
        # continuing to walk it IS the denial of service the guard exists to
        # prevent -- a fuller error list is not worth spending the work on
        # input already known to be hostile.
        return sort_errors(guard_errors)

    version_errors = check_schema_version(doc)
    if version_errors:
        return sort_errors(version_errors)

    errors = validate_structure(doc)
    errors.extend(validate_semantics(doc, metadata=metadata, registry=registry))
    return sort_errors(errors)


def load_document(text: str, *, metadata: PageMetadata, registry: Registry) -> dict:
    """Parse, migrate and validate a page document; return it, or raise.

    Raises the first `PageValidationError` in sorted order. The document
    returned is the MIGRATED one, at `CURRENT_SCHEMA_VERSION` with every block
    at its newest registered version, so a caller never has to wonder which
    shape it holds.
    """
    doc = loads(text)  # guard steps 1 and 2: raw byte cap, then guarded parse

    guard_errors = check_tree_limits(doc)
    if guard_errors:
        raise sort_errors(guard_errors)[0]

    version_errors = check_schema_version(doc)
    if version_errors:
        raise version_errors[0]

    migrated = migrate(doc)

    errors = validate_document(migrated, metadata=metadata, registry=registry)
    if errors:
        raise errors[0]
    return migrated


def is_guard_error(error: PageValidationError) -> bool:
    """Whether a finding is a resource guard rather than a content problem.

    Batch 11's HTTP API will want to answer a guard rejection differently
    (413-shaped, no detail echoed back) from a content rejection.
    """
    return error.code in GUARD_CODES
