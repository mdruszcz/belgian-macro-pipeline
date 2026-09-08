"""Page-document construction helpers for the Batch 9 test suite.

These build valid (or deliberately invalid) in-memory page documents against the real
contract published by builder-core: docs/features/page_document.schema.json and
assets/belpulse/blocks/registry.json, without hand-typing any indicator id, NIS code or
figure (real_data.py supplies those) and without hand-typing the exact prop shape of any
block type, which is builder-core's to define (registry.json's `props_schema_status` is
explicitly "provisional", batch spec §13). Instead, `props_for()` derives a minimally-valid
props object directly from whatever JSON Schema the real registry.json declares for a block
type -- including resolving its `$ref`s against the registry's own `$defs` -- so these tests
track builder-core's registry rather than guessing field names that would drift.

UI copy (English/French/Dutch labels, seo titles, block captions) is not an indicator value,
commune figure or mockup number under claude.md rule 36 -- it is hand-written placeholder text,
the same way any other test's fixture strings are, and is used freely below.
"""

from __future__ import annotations

import copy
from typing import Any

from src.pages.schema import CURRENT_SCHEMA_VERSION
from tests.fixtures.pages import real_data

# --- generic "smallest valid instance of a JSON Schema fragment" ---------------


def _resolve_ref(ref: str, defs: dict) -> dict:
    # Registry $refs are always local: "#/$defs/<name>".
    name = ref.rsplit("/", 1)[-1]
    assert name in defs, f"registry $defs has no entry named {name!r} (ref was {ref!r})"
    return defs[name]


def _instance_for(schema: dict, defs: dict, seed: str = "x") -> Any:
    """A minimal value satisfying `schema`. Handles the JSON-Schema shapes this
    repo's own schemas use (object/array/string/integer/number/boolean/null,
    enum, const, $ref, oneOf/anyOf/allOf) -- enough to fill in a props object
    whose exact field names are not known ahead of time, including trilingual
    {en, fr, nl} objects, which fall straight out of the generic "object"
    branch once en/fr/nl are the (resolved) schema's required properties.
    """
    if not schema:
        return {}
    if "$ref" in schema:
        return _instance_for(_resolve_ref(schema["$ref"], defs), defs, seed)
    if "const" in schema:
        return schema["const"]
    if "enum" in schema:
        return schema["enum"][0]
    for combiner in ("oneOf", "anyOf"):
        if combiner in schema and schema[combiner]:
            return _instance_for(schema[combiner][0], defs, seed)
    if "allOf" in schema:
        out: dict = {}
        for sub in schema["allOf"]:
            value = _instance_for(sub, defs, seed)
            if isinstance(value, dict):
                out.update(value)
        return out

    types = schema.get("type")
    if isinstance(types, list):
        types = next((t for t in types if t != "null"), types[0])

    if types == "object" or (types is None and "properties" in schema):
        props = schema.get("properties", {})
        required = schema.get("required", [])
        return {key: _instance_for(props.get(key, {}), defs, f"{seed}-{key}") for key in required}
    if types == "array":
        item_schema = schema.get("items", {})
        min_items = schema.get("minItems", 0)
        return [_instance_for(item_schema, defs, f"{seed}-{i}") for i in range(min_items)]
    if types == "string":
        min_len = schema.get("minLength", 0)
        text = f"Test value {seed}"
        if len(text) < min_len:
            text = text + ("x" * (min_len - len(text)))
        return text
    if types == "integer":
        return max(int(schema.get("minimum", 1)), 1)
    if types == "number":
        return float(schema.get("minimum", 1))
    if types == "boolean":
        return True
    if types == "null":
        return None
    return {}


# --- registry access -------------------------------------------------------------


def registry_entry(block_type: str) -> dict:
    raw = real_data.raw_registry()
    block_types = raw.get("block_types")
    assert isinstance(block_types, dict), "registry.json has no 'block_types' object"
    assert block_type in block_types, f"registry.json declares no block type {block_type!r}"
    entry = dict(block_types[block_type])
    entry.setdefault("type", block_type)
    return entry


def _registry_defs() -> dict:
    raw = real_data.raw_registry()
    defs = raw.get("$defs")
    assert isinstance(defs, dict), "registry.json has no top-level '$defs' object"
    return defs


def _versions_of(entry: dict) -> list[int]:
    versions = entry.get("supported_versions")
    assert versions, f"registry entry {entry.get('type')!r} declares no supported_versions"
    return sorted(int(v) for v in versions)


def props_schema_for(block_type: str, version: int) -> dict:
    entry = registry_entry(block_type)
    versions = entry.get("versions", {})
    version_entry = versions.get(str(version))
    assert (
        version_entry is not None
    ), f"registry entry {block_type!r} has no 'versions' entry for version {version}"
    schema = version_entry.get("props_schema")
    assert isinstance(schema, dict), f"{block_type!r} v{version} has no props_schema object"
    return schema


def props_for(block_type: str, version: int | None = None) -> dict:
    entry = registry_entry(block_type)
    resolved_version = version if version is not None else entry["current_version"]
    schema = props_schema_for(block_type, resolved_version)
    instance = _instance_for(schema, _registry_defs(), seed=block_type)
    return instance if isinstance(instance, dict) else {}


def accepts_binding(block_type: str) -> bool:
    return bool(registry_entry(block_type)["accepts_binding"])


def is_interactive(block_type: str) -> bool:
    return bool(registry_entry(block_type)["interactive"])


def current_version(block_type: str) -> int:
    return registry_entry(block_type)["current_version"]


def latest_version(block_type: str) -> int:
    return _versions_of(registry_entry(block_type))[-1]


def trilingual_instance(prefix: str) -> dict:
    """A minimal valid {en, fr, nl} object, resolved from the registry's own
    shared $defs (rather than a hand-typed shape) so it always matches
    whatever trilingual_text actually requires."""
    defs = _registry_defs()
    assert "trilingual_text" in defs, "registry $defs has no 'trilingual_text' entry"
    instance = _instance_for(defs["trilingual_text"], defs, seed=prefix)
    assert isinstance(instance, dict)
    return instance


# --- layout / grid ---------------------------------------------------------------


def layout(desktop=(0, 0, 4, 2), tablet=(0, 0, 4, 2), mobile=(0, 0, 4, 2)) -> dict:
    def pos(t):
        x, y, w, h = t
        return {"x": x, "y": y, "w": w, "h": h}

    return {"desktop": pos(desktop), "tablet": pos(tablet), "mobile": pos(mobile)}


def visibility(desktop=True, tablet=True, mobile=True) -> dict:
    return {"desktop": desktop, "tablet": tablet, "mobile": mobile}


def label(prefix: str) -> dict:
    """A plain, hand-written trilingual UI label -- not an indicator value or
    commune figure, so freely hand-typed per claude.md rule 36's own scope."""
    return {"en": f"{prefix} EN", "fr": f"{prefix} FR", "nl": f"{prefix} NL"}


# --- bindings -----------------------------------------------------------------
#
# Shape from docs/features/page_document.schema.json's `binding` $def, not
# docs/features/data_binding.md's illustrative (Batch 14) example -- the two
# differ (e.g. aggregation is `binding.aggregate.function == "sum"`, not an
# operation/comparison_scope combination), and the schema is the enforced
# contract.


def municipal_binding(
    indicator: str,
    *,
    nis: str | None = None,
    aggregate_over: str | None = None,
) -> dict:
    geography = {"mode": "fixed", "nis": nis} if nis else {"mode": "context"}
    out = {
        "provider": "municipal",
        "indicator": indicator,
        "geography": geography,
        "period": {"mode": "latest"},
        "operation": "aggregate" if aggregate_over else "latest",
    }
    if aggregate_over:
        out["aggregate"] = {"function": "sum", "over": aggregate_over}
    return out


def national_binding(indicator: str) -> dict:
    return {
        "provider": "national",
        "indicator": indicator,
        "geography": {"mode": "fixed", "nis": real_data.a_country_nis_code()},
        "period": {"mode": "latest"},
        "operation": "latest",
    }


# --- blocks / sections / documents ---------------------------------------------


def make_block(
    block_type: str,
    *,
    block_id: str,
    version: int | None = None,
    binding: dict | None = None,
    props: dict | None = None,
    block_layout: dict | None = None,
    block_visibility: dict | None = None,
    locked: bool = False,
) -> dict:
    resolved_version = current_version(block_type) if version is None else version
    resolved_props = props if props is not None else props_for(block_type, resolved_version)
    return {
        "id": block_id,
        "type": block_type,
        "version": resolved_version,
        "props": resolved_props,
        "binding": binding,
        "visibility": block_visibility or visibility(),
        "layout": block_layout or layout(),
        "locked": locked,
    }


def make_section(section_id: str, blocks: list[dict], *, allow_overlap: bool | None = None) -> dict:
    section = {"id": section_id, "blocks": blocks}
    if allow_overlap is not None:
        section["allow_overlap"] = allow_overlap
    return section


def minimal_valid_document(route: str = "/", page_type: str = "blank") -> dict:
    """The smallest document that should validate cleanly: one static `hero`
    block (no binding, no NIS/indicator involved at all)."""
    hero = make_block("hero", block_id="blk-hero-1", binding=None)
    return {
        "schema_version": CURRENT_SCHEMA_VERSION,
        "page_id": "test-minimal",
        "revision": 1,
        "route": route,
        "page_type": page_type,
        "theme": "light-institutional",
        "context": {},
        "seo": {"title": label("Test page"), "description": label("A test page")},
        "sections": [make_section("sec-1", [hero])],
    }


def realistic_multi_section_document() -> dict:
    """A document with several sections and several block types, including
    real bound indicators (one municipal, one national) -- closer to what
    Batch 15 will actually author."""
    hero = make_block("hero", block_id="blk-hero-1", binding=None)
    kpi = make_block(
        "kpi_card",
        block_id="blk-kpi-1",
        binding=municipal_binding(real_data.an_additive_municipal_indicator_id()),
        block_layout=layout(desktop=(0, 0, 6, 2), tablet=(0, 0, 8, 2), mobile=(0, 0, 4, 2)),
    )
    chart_props = props_for("chart")
    chart_props["accessible_name"] = trilingual_instance("chart-accessible-name")
    chart = make_block(
        "chart",
        block_id="blk-chart-1",
        binding=national_binding(real_data.national_indicator_ids()[0]),
        props=chart_props,
        block_layout=layout(desktop=(6, 0, 6, 2), tablet=(0, 2, 8, 2), mobile=(0, 2, 4, 2)),
    )
    table = make_block(
        "comparison_table",
        block_id="blk-table-1",
        binding=municipal_binding(real_data.an_additive_municipal_indicator_id()),
    )
    return {
        "schema_version": CURRENT_SCHEMA_VERSION,
        "page_id": "test-realistic",
        "revision": 2,
        "route": f"/local/{real_data.a_municipal_nis_code()}/",
        "page_type": "municipality-profile",
        "theme": "light-institutional",
        "context": {"nis": real_data.a_municipal_nis_code()},
        "seo": {"title": label("Realistic page"), "description": label("Realistic description")},
        "sections": [
            make_section("sec-hero", [hero]),
            make_section("sec-kpis", [kpi, chart]),
            make_section("sec-table", [table]),
        ],
    }


def deep_clone(doc: dict) -> dict:
    return copy.deepcopy(doc)
