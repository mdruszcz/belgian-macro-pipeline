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
