"""Validates config/live_counters.yaml against
docs/features/live_counters.schema.json, plus the cross-field checks one
JSON Schema file cannot express alone -- same split as
src/validation/config_schema.py does for config/indicators/*.yaml:

  * every counter id and every breakdown part id is unique;
  * a `placements` list names only real counter ids;
  * a `difference` counter's `minuend`/`subtrahend` name real `flow`
    counters, DEFINED EARLIER in `counters` (build_payload builds the list
    in config order and can only resolve a reference it has already built);
    a stock counter paced by a counter names a real `difference` counter,
    likewise defined earlier;
  * a nested remainder's `covers` names real sibling part ids.

Raises `LiveCounterError` (src/analytics/live_counters.py's own exception,
imported rather than duplicated) for any of the above -- a config problem,
never a data one, so it fails the build loudly (CLAUDE.md rule 13).
"""

import json
from pathlib import Path

import jsonschema
import yaml

from src.analytics.live_counters import LiveCounterError

SCHEMA_PATH = (
    Path(__file__).resolve().parents[2] / "docs" / "features" / "live_counters.schema.json"
)


def _format_errors(errors: list[jsonschema.ValidationError]) -> str:
    parts = []
    for e in sorted(errors, key=str):
        field = "/".join(str(p) for p in e.absolute_path) or "(root)"
        parts.append(f"{field} — {e.message}")
    return "; ".join(parts)


def load_and_validate_live_counters(path: Path) -> dict:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(schema)
    errors = list(validator.iter_errors(data))
    if errors:
        raise LiveCounterError(f"{path.name}: {_format_errors(errors)}")

    counters = data["counters"]
    ids = [c["id"] for c in counters]
    if len(ids) != len(set(ids)):
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        raise LiveCounterError(f"{path.name}: duplicate counter id(s) {dupes}")
    by_id = {c["id"]: c for c in counters}
    index_of = {c["id"]: i for i, c in enumerate(counters)}

    for counter in counters:
        breakdown = counter.get("breakdown")
        if not breakdown:
            continue
        part_ids = [p["id"] for p in breakdown["parts"]]
        if len(part_ids) != len(set(part_ids)):
            dupes = sorted({i for i in part_ids if part_ids.count(i) > 1})
            raise LiveCounterError(
                f"{path.name}: counter {counter['id']!r} has duplicate part id(s) {dupes}"
            )
        part_id_set = set(part_ids)
        for part in breakdown["parts"]:
            for covered in part.get("covers", []):
                if covered not in part_id_set:
                    raise LiveCounterError(
                        f"{path.name}: counter {counter['id']!r} part {part['id']!r} "
                        f"covers unknown sibling part {covered!r}"
                    )

    # A SEPARATE loop, deliberately -- `deficit` has no `breakdown` at all,
    # so gating this check behind the loop above's `if not breakdown:
    # continue` would skip it entirely for the one counter it matters most
    # for.
    for counter in counters:
        if counter["kind"] != "difference":
            continue
        for role in ("minuend", "subtrahend"):
            ref = by_id.get(counter[role])
            if ref is None:
                raise LiveCounterError(
                    f"{path.name}: counter {counter['id']!r}.{role} names "
                    f"unknown counter {counter[role]!r}"
                )
            if ref["kind"] != "flow":
                raise LiveCounterError(
                    f"{path.name}: counter {counter['id']!r}.{role} "
                    f"({counter[role]!r}) is not a flow counter"
                )
            # build_payload (scripts/export_live_counters.py) builds
            # counters_out/flow_segments_by_id/deficit_pace_by_year in
            # config-list order and only ever looks a reference up in a dict
            # already populated by an EARLIER iteration -- a forward
            # reference is never an error there, it is a silent `.get()`
            # miss that degrades the whole counter to "unavailable" (audit
            # P2-3: reordering `debt` above `deficit` in the YAML makes debt
            # quietly vanish from the site with no raised error). Order is
            # therefore part of this config's contract, not a style choice,
            # and belongs under the same fail-loud check as the reference
            # itself existing at all.
            if index_of[counter[role]] >= index_of[counter["id"]]:
                raise LiveCounterError(
                    f"{path.name}: counter {counter['id']!r}.{role} "
                    f"({counter[role]!r}) must be defined BEFORE {counter['id']!r} "
                    f"in `counters` -- build_payload resolves it by config order, "
                    f"not by name lookup across the whole file"
                )

    for counter in counters:
        if counter["kind"] == "stock" and "paced_by" in counter:
            ref = by_id.get(counter["paced_by"])
            if ref is None:
                raise LiveCounterError(
                    f"{path.name}: counter {counter['id']!r}.paced_by names "
                    f"unknown counter {counter['paced_by']!r}"
                )
            if ref["kind"] != "difference":
                raise LiveCounterError(
                    f"{path.name}: counter {counter['id']!r}.paced_by "
                    f"({counter['paced_by']!r}) is not a difference counter"
                )
            if index_of[counter["paced_by"]] >= index_of[counter["id"]]:
                raise LiveCounterError(
                    f"{path.name}: counter {counter['id']!r}.paced_by "
                    f"({counter['paced_by']!r}) must be defined BEFORE "
                    f"{counter['id']!r} in `counters` -- same build-order "
                    f"contract as a difference counter's minuend/subtrahend"
                )

    for placement, members in data["placements"].items():
        unknown = [m for m in members if m not in by_id]
        if unknown:
            raise LiveCounterError(
                f"{path.name}: placement {placement!r} names unknown counter(s) {unknown}"
            )

    return data
