from pathlib import Path

import pytest

from src.validation.config_schema import (
    ConfigValidationError,
    is_multi_geo,
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


# --- fetch: three mutually exclusive shapes (international pilot PR 1) -----


def test_fetch_query_shape_is_valid():
    doc = dict(VALID_INDICATOR, fetch={"query": "/x/Q.FOO.BE?observations=true"})
    assert validate_indicator_config(doc, Path(__file__)) == []
    assert is_multi_geo(doc) is False


def test_fetch_national_dataset_shape_requires_geo_in_filters():
    doc = dict(
        VALID_INDICATOR,
        fetch={"dataset": "namq_10_gdp", "filters": {"unit": "CLV10_MEUR"}, "since": "2008"},
    )
    errors = validate_indicator_config(doc, Path("bad.yaml"))
    assert errors, "filters without geo must not validate against either dataset shape"


def test_fetch_national_dataset_shape_is_valid_with_geo():
    doc = dict(
        VALID_INDICATOR,
        fetch={
            "dataset": "namq_10_gdp",
            "filters": {"unit": "CLV10_MEUR", "geo": "BE"},
            "since": "2008",
        },
    )
    assert validate_indicator_config(doc, Path(__file__)) == []
    assert is_multi_geo(doc) is False


def test_fetch_multi_geo_shape_is_valid_and_forbids_geo_in_filters():
    doc = dict(
        VALID_INDICATOR,
        fetch={
            "dataset": "namq_10_gdp",
            "filters": {"unit": "CLV10_MEUR"},
            "geographies": "allowlist",
            "since": "2008",
        },
    )
    assert validate_indicator_config(doc, Path(__file__)) == []
    assert is_multi_geo(doc) is True

    with_geo = dict(VALID_INDICATOR, fetch={**doc["fetch"], "filters": {"geo": "BE"}})
    errors = validate_indicator_config(with_geo, Path("bad.yaml"))
    assert errors, "geographies: allowlist must forbid a geo filter"


def test_geographies_allowlist_forbids_a_country_field():
    """A multi-geo fetch describes every country at once; a `country` field
    would be meaningless -- the root-level allOf/if/then rule."""
    doc = dict(
        VALID_INDICATOR,
        country="EU27_2020",
        fetch={"dataset": "namq_10_gdp", "geographies": "allowlist"},
    )
    errors = validate_indicator_config(doc, Path("bad.yaml"))
    assert errors, "geographies: allowlist + country must be rejected"


def test_is_multi_geo_false_when_no_fetch_is_declared():
    assert is_multi_geo({}) is False
    assert is_multi_geo({"fetch": None}) is False
