from pathlib import Path

import pytest

from src.validation.config_schema import (
    ConfigValidationError,
    load_and_validate_all,
    validate_indicator_config,
    validate_source_config,
)

VALID_SOURCE = {
    "source_id": "test_source",
    "name": "Test Source",
    "agency": "TEST",
    "adapter": "nbb",
    "base_url": "https://example.test",
    "licence": None,
    "catalog_ref": None,
    "cadence": "daily",
    "is_active": True,
}

VALID_INDICATOR = {
    "id": "TEST_IND",
    "name": {"en": "Test", "fr": "Test FR", "nl": "Test NL"},
    "unit": "percent_yy",
    "frequency": "A",
    "source_id": "test_source",
    "geo_levels": ["national"],
    "preferred_direction": "higher_is_better",
    "display": {
        "category": "gdp",
        "title": {"en": "Test title", "fr": "Titre", "nl": "Titel"},
        "sort_order": 1,
    },
}


def test_valid_indicator_config_passes():
    assert validate_indicator_config(VALID_INDICATOR, Path(__file__)) == []


def test_missing_required_field_rejected():
    bad = {k: v for k, v in VALID_INDICATOR.items() if k != "unit"}
    errors = validate_indicator_config(bad, Path("bad.yaml"))
    assert errors
    assert any("unit" in e for e in errors)


def test_wrong_enum_value_rejected():
    bad = dict(VALID_INDICATOR, frequency="weekly")
    errors = validate_indicator_config(bad, Path("bad.yaml"))
    assert errors
    assert any("frequency" in e for e in errors)


def test_valid_source_config_passes():
    assert validate_source_config(VALID_SOURCE, Path(__file__)) == []


def test_source_missing_required_field_rejected():
    bad = {k: v for k, v in VALID_SOURCE.items() if k != "adapter"}
    errors = validate_source_config(bad, Path("bad.yaml"))
    assert errors
    assert any("adapter" in e for e in errors)


def test_dangling_source_id_rejected(tmp_path):
    indicators_dir = tmp_path / "indicators"
    sources_dir = tmp_path / "sources"
    indicators_dir.mkdir()
    sources_dir.mkdir()
    import yaml

    (sources_dir / "nbb.yaml").write_text(yaml.dump(VALID_SOURCE))
    bad_indicator = dict(VALID_INDICATOR, source_id="does_not_exist")
    (indicators_dir / "TEST_IND.yaml").write_text(yaml.dump(bad_indicator))

    with pytest.raises(ConfigValidationError, match="does_not_exist"):
        load_and_validate_all(indicators_dir, sources_dir)


def test_load_and_validate_all_valid(tmp_path):
    import yaml

    indicators_dir = tmp_path / "indicators"
    sources_dir = tmp_path / "sources"
    indicators_dir.mkdir()
    sources_dir.mkdir()
    (sources_dir / "nbb.yaml").write_text(yaml.dump(VALID_SOURCE))
    (indicators_dir / "TEST_IND.yaml").write_text(yaml.dump(VALID_INDICATOR))

    indicators, sources = load_and_validate_all(indicators_dir, sources_dir)
    assert "TEST_IND" in indicators
    assert "test_source" in sources
