"""Accessor for the block registry, `assets/belpulse/blocks/registry.json`.

The registry is DATA, not code, and it is the single declaration of what a
block type is: its supported versions, its props schema, whether it takes a
binding, whether it is interactive. Batch 9's validator and Batch 10's
renderer read the same file, so "what the editor allows" and "what the public
site renders" cannot drift apart.

It lives under `assets/` rather than `config/` on purpose: `assets/` is
already served from the Pages root, so Batch 10's renderer can fetch the
registry in the browser with no build step and no second copy. `config/` would
have needed either a build-time copy into `public/` or a server, and the
public site stays static (claude.md rule 30).

New block types are added by editing that file and going through review,
exactly like a new indicator config. Nothing loads a block type from outside
it (block_contract.md's non-goals).
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from src.pages.schema import PageDocumentError

REGISTRY_PATH = (
    Path(__file__).resolve().parents[2] / "assets" / "belpulse" / "blocks" / "registry.json"
)

SUPPORTED_REGISTRY_VERSION = 1


class RegistryError(PageDocumentError):
    """The registry file is missing or malformed. Always fatal."""


@dataclass(frozen=True)
class Registry:
    """One loaded registry. Immutable: a validator holds it for its lifetime."""

    registry_version: int
    block_types: Mapping[str, dict]
    defs: Mapping[str, dict]

    @property
    def types(self) -> tuple[str, ...]:
        return tuple(sorted(self.block_types))

    def has_type(self, block_type: str) -> bool:
        return block_type in self.block_types

    def _entry(self, block_type: str) -> dict:
        try:
            return self.block_types[block_type]
        except KeyError:
            raise RegistryError(f"unknown block type {block_type!r}") from None

    def supported_versions(self, block_type: str) -> tuple[int, ...]:
        return tuple(self._entry(block_type)["supported_versions"])

    def supports_version(self, block_type: str, version: object) -> bool:
        if not self.has_type(block_type):
            return False
        if not isinstance(version, int) or isinstance(version, bool):
            return False
        return version in self.supported_versions(block_type)

    def current_version(self, block_type: str) -> int:
        return self._entry(block_type)["current_version"]

    def accepts_binding(self, block_type: str) -> bool:
        return bool(self._entry(block_type)["accepts_binding"])

    def requires_binding(self, block_type: str) -> bool:
        return bool(self._entry(block_type)["requires_binding"])

    def is_interactive(self, block_type: str) -> bool:
        return bool(self._entry(block_type)["interactive"])

    def props_schema(self, block_type: str, version: int) -> dict:
        """The props schema for one version of one block type, with the
        registry's shared `$defs` injected so `{"$ref": "#/$defs/..."}` inside
        it resolves when the schema is validated on its own."""
        entry = self._entry(block_type)
        versions = entry["versions"]
        key = str(version)
        if key not in versions:
            raise RegistryError(
                f"block type {block_type!r} declares no props schema for version {version}"
            )
        schema = dict(versions[key]["props_schema"])
        shared = dict(self.defs)
        shared.update(schema.get("$defs") or {})
        schema["$defs"] = shared
        return schema

    def props_schema_status(self, block_type: str, version: int) -> str:
        return self._entry(block_type)["versions"][str(version)]["props_schema_status"]


_REQUIRED_TYPE_KEYS = (
    "accepts_binding",
    "current_version",
    "interactive",
    "requires_binding",
    "supported_versions",
    "versions",
)


def _validate_shape(data: dict, path: Path) -> None:
    version = data.get("registry_version")
    if version != SUPPORTED_REGISTRY_VERSION:
        raise RegistryError(
            f"{path}: registry_version is {version!r}, this build supports "
            f"{SUPPORTED_REGISTRY_VERSION}"
        )
    block_types = data.get("block_types")
    if not isinstance(block_types, dict) or not block_types:
        raise RegistryError(f"{path}: 'block_types' must be a non-empty object")

    for name, entry in block_types.items():
        if not isinstance(entry, dict):
            raise RegistryError(f"{path}: block type {name!r} is not an object")
        missing = [k for k in _REQUIRED_TYPE_KEYS if k not in entry]
        if missing:
            raise RegistryError(f"{path}: block type {name!r} is missing {', '.join(missing)}")
        versions = entry["versions"]
        supported = entry["supported_versions"]
        if not isinstance(supported, list) or not supported:
            raise RegistryError(f"{path}: block type {name!r} supports no versions")
        if not all(isinstance(v, int) and not isinstance(v, bool) for v in supported):
            raise RegistryError(f"{path}: block type {name!r} has a non-integer version")
        if not isinstance(versions, dict):
            raise RegistryError(f"{path}: block type {name!r} 'versions' is not an object")
        for v in supported:
            declared = versions.get(str(v))
            if not isinstance(declared, dict):
                raise RegistryError(
                    f"{path}: block type {name!r} declares version {v} as supported "
                    "but ships no entry for it"
                )
            if not isinstance(declared.get("props_schema"), dict):
                raise RegistryError(
                    f"{path}: block type {name!r} version {v} has no props_schema object"
                )
            if declared.get("props_schema_status") != "provisional":
                raise RegistryError(
                    f"{path}: block type {name!r} version {v} must be marked "
                    'props_schema_status "provisional" -- no block has been rendered yet'
                )
        if entry["current_version"] not in supported:
            raise RegistryError(
                f"{path}: block type {name!r} current_version "
                f"{entry['current_version']!r} is not in supported_versions"
            )
        for flag in ("accepts_binding", "requires_binding", "interactive"):
            if not isinstance(entry[flag], bool):
                raise RegistryError(f"{path}: block type {name!r} {flag!r} is not a boolean")
        if entry["requires_binding"] and not entry["accepts_binding"]:
            raise RegistryError(
                f"{path}: block type {name!r} requires a binding but does not accept one"
            )


def load_registry(path: Path | str | None = None) -> Registry:
    """Load and structurally check the block registry.

    Raises rather than returning a partial registry: a validator holding half a
    registry would pass block types it cannot actually check.
    """
    target = Path(path) if path is not None else REGISTRY_PATH
    if not target.is_file():
        raise RegistryError(f"block registry is missing: {target}")
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RegistryError(f"block registry {target} could not be read: {exc}") from exc
    if not isinstance(data, dict):
        raise RegistryError(f"block registry {target} is not a JSON object")

    _validate_shape(data, target)
    defs = data.get("$defs") or {}
    if not isinstance(defs, dict):
        raise RegistryError(f"block registry {target} has a non-object '$defs'")

    return Registry(
        registry_version=data["registry_version"],
        block_types=MappingProxyType(dict(data["block_types"])),
        defs=MappingProxyType(dict(defs)),
    )
