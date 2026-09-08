"""Forward-only migrations for page documents.

Two independent ladders, both climbed by `migrate()`:

1. **Document `schema_version`.** `DOCUMENT_MIGRATIONS` maps a version to the
   step that turns it into the next one. At `CURRENT_SCHEMA_VERSION == 2` it
   holds one step, `{1: _document_1_to_2}`, which adds the required `locked`
   flag to every block. It went in exactly as this docstring predicted it
   would: one entry here, one constant bumped, and nothing else changed.

2. **Block `version`.** `BLOCK_MIGRATIONS` maps `(block_type, version)` to the
   step that turns that block into the next version of itself. This ladder is
   populated: `kpi_card` v1 -> v2 is real, because the registry declares two
   supported `kpi_card` versions so that both the unsupported-version
   rejection and this framework are testable against the real registry rather
   than a fixture.

Forward only. There is no downgrade path, on purpose: a v2 document opened by
a build that only understands v1 is rejected with
`unsupported_schema_version` rather than silently degraded.

**Never silently drops a field.** A prop a migration does not recognise is
carried through untouched. It will then usually fail the target version's
props schema (every one is `additionalProperties: false`) -- which is the
point: an unexpected field becomes a loud `schema_violation` naming the field,
not a value that vanished between two saves.

Never write a bare `import migrations` from inside this package. The
repository has a top-level `migrations/` directory (the SQL schema), which is
an implicit namespace package on `sys.path` and would shadow this module. Use
`from src.pages import migrations`.
"""

import copy
from collections.abc import Callable

from src.pages.schema import (
    CURRENT_SCHEMA_VERSION,
    PageValidationError,
    check_schema_version,
)


def _document_1_to_2(doc: dict) -> dict:
    """schema_version 1 -> 2: every block gains a required `locked` flag.

    Defaults to False, which is the only safe direction: locking is an editing
    guard, and a document written before the flag existed had no blocks anyone
    had chosen to protect. Defaulting to True would silently freeze every block
    on every existing page and look like a broken builder.

    A block that somehow already carries the field keeps it -- a migration that
    overwrote a real value would lose an author's choice, and this ladder never
    silently drops or rewrites a field it did not add.
    """
    out = dict(doc)
    sections = out.get("sections")
    if not isinstance(sections, list):
        return out
    out["sections"] = [
        (
            section
            if not isinstance(section, dict) or not isinstance(section.get("blocks"), list)
            else {
                **section,
                "blocks": [
                    (
                        {**block, "locked": block.get("locked", False)}
                        if isinstance(block, dict)
                        else block
                    )
                    for block in section["blocks"]
                ],
            }
        )
        for section in sections
    ]
    return out


#: version -> step producing version + 1.
DOCUMENT_MIGRATIONS: dict[int, Callable[[dict], dict]] = {1: _document_1_to_2}


def _kpi_card_1_to_2(props: dict) -> dict:
    """kpi_card v1 -> v2.

    v2 renamed `show_sparkline` to `sparkline` and added `show_provenance`,
    which defaults to true because claude.md rule 28 wants a figure's source,
    unit, period and freshness shown -- a v1 card that predates the flag
    should start showing them, not stay silent.
    """
    out = dict(props)
    if "show_sparkline" in out:
        out["sparkline"] = out.pop("show_sparkline")
    out.setdefault("show_provenance", True)
    return out


def _map_1_to_2(props: dict) -> dict:
    """map v1 -> v2 (Batch 15).

    v2 adds the chrome map.html owns: zoom, an indicator picker and
    click-through. Every one defaults to FALSE, because a v1 map was authored
    without them and silently growing controls on an existing page is a change
    the author did not make. A converted map.html turns them on explicitly.
    """
    out = dict(props)
    out.setdefault("show_zoom", False)
    out.setdefault("indicator_picker", False)
    out.setdefault("click_through", False)
    return out


#: (block_type, from_version) -> step producing from_version + 1 props.
BLOCK_MIGRATIONS: dict[tuple[str, int], Callable[[dict], dict]] = {
    ("kpi_card", 1): _kpi_card_1_to_2,
    ("map", 1): _map_1_to_2,
}


def migrate_block(block: dict) -> dict:
    """Climb one block up its version ladder as far as steps exist.

    A block type with no step registered at its current version is already
    current and is returned unchanged (a copy). An unknown type is left
    completely alone -- rejecting it is `semantics.check_block_type_and_version`'s
    job, and doing it here would turn an `unknown_block_type` finding into an
    exception from the migrator.
    """
    out = copy.deepcopy(block)
    block_type = out.get("type")
    seen: set[int] = set()
    while True:
        version = out.get("version")
        if not isinstance(version, int) or isinstance(version, bool):
            return out
        if version in seen:  # pragma: no cover - guards a malformed table
            return out
        seen.add(version)
        step = BLOCK_MIGRATIONS.get((block_type, version))
        if step is None:
            return out
        props = out.get("props")
        out["props"] = step(props) if isinstance(props, dict) else props
        out["version"] = version + 1


def migrate(doc: dict) -> dict:
    """Bring a document forward to `CURRENT_SCHEMA_VERSION` and every block
    forward to its newest registered version.

    Never mutates its argument. Raises `PageValidationError` with
    `unsupported_schema_version` if the document's version is one this build
    cannot start from.
    """
    version_errors = check_schema_version(doc)
    if version_errors:
        raise version_errors[0]

    out = copy.deepcopy(doc)
    version = out["schema_version"]
    while version < CURRENT_SCHEMA_VERSION:
        step = DOCUMENT_MIGRATIONS.get(version)
        if step is None:
            raise PageValidationError(
                "unsupported_schema_version",
                "schema_version",
                f"no migration exists from schema_version {version} to {version + 1}",
            )
        out = step(out)
        version += 1
        out["schema_version"] = version

    sections = out.get("sections")
    if isinstance(sections, list):
        for section in sections:
            if not isinstance(section, dict):
                continue
            blocks = section.get("blocks")
            if not isinstance(blocks, list):
                continue
            for b_index, block in enumerate(blocks):
                if isinstance(block, dict):
                    blocks[b_index] = migrate_block(block)

    return out
