"""Page documents: the format the BelPulse page builder reads and writes.

Batch 9 of docs/features/page_builder.md. The public surface is exactly what
is re-exported here; Batches 10-16 build against these names and nothing else,
so which module a name is defined in stays an implementation detail.

    load_document(text, *, metadata, registry) -> dict
        Parse, migrate and validate. Raises on the first finding.
    validate_document(doc, *, metadata, registry) -> list[PageValidationError]
        Collect every finding. Empty list means valid.
    dumps(doc) -> str / loads(text) -> dict
        Canonical, byte-stable serialization (claude.md rule 35).
    migrate(doc) -> dict
        Forward-only, never silently drops a field.
    load_registry(path=None) -> Registry
        assets/belpulse/blocks/registry.json, the one declaration of what a
        block type is.
    load_metadata(root=None) -> PageMetadata
        The published metadata this validator checks bindings against.
    is_guard_error(error) -> bool
        Whether a finding is a resource guard rather than a content problem,
        so Batch 11's API can answer it 413-shaped without echoing detail.
    render_document(doc, *, registry, lang, data, errors) -> str
        The one renderer (Batch 10). The public build and the builder preview
        both call this, so what is edited and what ships cannot drift.
    BlockRenderError
        One block that failed to render; collected, never raised out of a page.
    CURRENT_SCHEMA_VERSION, PageValidationError, PageDocumentError

What this package does NOT do, and must not grow into: it never resolves a
binding, opens a commune payload or computes a value (Batch 14), and it never
resolves geography -- see the boundary spelled out in `semantics.py`'s module
docstring. Batch 10 added rendering, which is why `render_document` takes
already-resolved `data` and renders what it is handed: the line it must not
cross is fetching or computing that data itself.
"""

from src.pages.document import is_guard_error, load_document, validate_document
from src.pages.metadata import load_metadata
from src.pages.migrations import migrate
from src.pages.registry import load_registry
from src.pages.render import BlockRenderError, render_document
from src.pages.schema import (
    CURRENT_SCHEMA_VERSION,
    PageDocumentError,
    PageValidationError,
)
from src.pages.serialize import dumps, loads

__all__ = [
    "CURRENT_SCHEMA_VERSION",
    "PageDocumentError",
    "PageValidationError",
    "BlockRenderError",
    "dumps",
    "is_guard_error",
    "load_document",
    "load_metadata",
    "load_registry",
    "loads",
    "migrate",
    "render_document",
    "validate_document",
]
