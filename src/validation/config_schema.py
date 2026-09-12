"""
Validates config/indicators/*.yaml and config/sources/*.yaml against
docs/features/indicator_config.schema.json and source_config.schema.json.

JSON Schema alone can't express "this indicator's source_id must exist
among the loaded sources" (a cross-file check) -- load_and_validate_all()
does that pass itself, after per-file schema validation.
"""

import json
from pathlib import Path

import jsonschema
import yaml

SCHEMA_DIR = Path(__file__).resolve().parents[2] / "docs" / "features"
INDICATOR_SCHEMA = json.loads((SCHEMA_DIR / "indicator_config.schema.json").read_text())
SOURCE_SCHEMA = json.loads((SCHEMA_DIR / "source_config.schema.json").read_text())
DERIVED_SCHEMA = json.loads((SCHEMA_DIR / "derived_indicator_config.schema.json").read_text())


class ConfigValidationError(Exception):
    pass


def _format_errors(errors: list[jsonschema.ValidationError], path: Path) -> list[str]:
    out = []
    for e in errors:
        field = "/".join(str(p) for p in e.absolute_path) or "(root)"
        out.append(f"{path.name}:{field} — {e.message}")
    return out


def validate_indicator_config(data: dict, path: Path) -> list[str]:
    validator = jsonschema.Draft202012Validator(INDICATOR_SCHEMA)
    return _format_errors(sorted(validator.iter_errors(data), key=str), path)


def validate_source_config(data: dict, path: Path) -> list[str]:
    validator = jsonschema.Draft202012Validator(SOURCE_SCHEMA)
    return _format_errors(sorted(validator.iter_errors(data), key=str), path)


def validate_derived_config(data: dict, path: Path) -> list[str]:
    validator = jsonschema.Draft202012Validator(DERIVED_SCHEMA)
    return _format_errors(sorted(validator.iter_errors(data), key=str), path)


def load_and_validate_derived(derived_dir: Path, known_indicator_ids: set[str]) -> dict:
    """Load config/indicators/derived/*.yaml.

    A separate pass because these live in a subdirectory the main loader's
    non-recursive glob would silently skip -- an unvalidated config directory
    is worse than none, since it looks checked.

    Beyond the schema, this does the cross-file check JSON Schema cannot
    express: every `derived.inputs` entry must name either a real source
    indicator or another derived one. An input nothing provides would produce
    a column that is null for every row, which is the kind of quiet emptiness
    that survives review.
    """
    errors: list[str] = []
    derived: dict[str, dict] = {}
    if not derived_dir.is_dir():
        return derived

    for path in sorted(derived_dir.glob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        errs = validate_derived_config(data, path)
        if errs:
            errors.extend(errs)
            continue
        derived[data["id"]] = data

    # An input may also be a CONFIGURED source indicator whose data has not
    # reached this store yet: a new source's ratios are checked in beside its
    # adapter, and the daily workflow exports from the committed database
    # BEFORE that day's sync loads the rows (found by Batch O's audit, P0-1 --
    # the four municipal-finance ratios stopped every export on a database
    # without WalStat rows). Such a config is DEFERRED: left out of the result
    # so the engine, which rightly refuses to compute a column that would be
    # null for every row, never sees it. A typo still fails here, because it
    # resolves nowhere -- not in the store, not in a config, not derived.
    configured = _configured_indicator_ids(derived_dir.parent)
    resolvable = known_indicator_ids | configured | set(derived)
    for ind_id, cfg in sorted(derived.items()):
        for dep in cfg["derived"]["inputs"]:
            if dep not in resolvable:
                errors.append(
                    f"{ind_id}.yaml:derived/inputs \u2014 input {dep!r} is not a known "
                    "indicator and is not itself derived"
                )

    if errors:
        raise ConfigValidationError("\n".join(errors))
    return _computable(derived, known_indicator_ids)


def _computable(derived: dict[str, dict], known_indicator_ids: set[str]) -> dict[str, dict]:
    """The derived configs whose inputs are all in the store, or derived from
    it -- iterated to a fixed point so a ratio of a deferred ratio is deferred
    too. Order is preserved."""
    available = set(known_indicator_ids)
    kept: set[str] = set()
    changed = True
    while changed:
        changed = False
        for ind_id, cfg in derived.items():
            if ind_id in kept:
                continue
            if all(dep in available for dep in cfg["derived"]["inputs"]):
                kept.add(ind_id)
                available.add(ind_id)
                changed = True
    return {ind_id: cfg for ind_id, cfg in derived.items() if ind_id in kept}


def _configured_indicator_ids(indicators_dir: Path) -> set[str]:
    """The `id` of every source-indicator config beside the derived ones."""
    ids: set[str] = set()
    if not indicators_dir.is_dir():
        return ids
    for path in sorted(indicators_dir.glob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        if isinstance(data, dict) and isinstance(data.get("id"), str):
            ids.add(data["id"])
    return ids


def load_and_validate_all(indicators_dir: Path, sources_dir: Path) -> tuple[dict, dict]:
    errors: list[str] = []

    sources: dict[str, dict] = {}
    for path in sorted(sources_dir.glob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        errs = validate_source_config(data, path)
        if errs:
            errors.extend(errs)
            continue
        sources[data["source_id"]] = data

    indicators: dict[str, dict] = {}
    for path in sorted(indicators_dir.glob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        errs = validate_indicator_config(data, path)
        if errs:
            errors.extend(errs)
            continue
        indicators[data["id"]] = data

    if not errors:
        for code, ind in indicators.items():
            if ind["source_id"] not in sources:
                errors.append(
                    f"{code}.yaml:source_id — references unknown source {ind['source_id']!r}"
                )

    if errors:
        raise ConfigValidationError("\n".join(errors))

    return indicators, sources


def is_canonical_eligible(indicator: dict, sources: dict) -> bool:
    """Whether this indicator belongs in the canonical schema/dashboard
    pipeline. Deliberately NOT "does it have a display block" -- e.g.
    EC_CONS_CONF_BE is Belgian data with no dashboard row of its own and is
    still eligible; EUROSTAT_GDP_Q_MEUR_DE has the same shape but is German
    data and is not. The real criterion is country + a fetchable adapter."""
    if indicator.get("country", "BE") != "BE":
        return False
    source = sources[indicator["source_id"]]
    return source["adapter"] in ("nbb", "dbnomics")
