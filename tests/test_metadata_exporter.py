import json

import yaml

from src.exporters.metadata import export_metadata

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


def test_dummy_indicator_reaches_exported_metadata(tmp_path):
    indicators_dir = tmp_path / "indicators"
    sources_dir = tmp_path / "sources"
    indicators_dir.mkdir()
    sources_dir.mkdir()

    (sources_dir / "test.yaml").write_text(yaml.dump(VALID_SOURCE))
    (indicators_dir / "DUMMY_TEST_IND.yaml").write_text(
        yaml.dump(
            {
                "id": "DUMMY_TEST_IND",
                "name": {"en": "Dummy", "fr": "Factice", "nl": "Dummy"},
                "unit": "percent_yy",
                "frequency": "A",
                "source_id": "test_source",
                "geo_levels": ["national"],
                "display": {
                    "category": "gdp",
                    "title": {
                        "en": "Dummy indicator",
                        "fr": "Indicateur factice",
                        "nl": "Dummy-indicator",
                    },
                    "sort_order": 999,
                },
            }
        )
    )

    out = tmp_path / "indicators.json"
    n = export_metadata(indicators_dir, sources_dir, out)

    assert n == 1
    result = json.loads(out.read_text())
    assert "DUMMY_TEST_IND" in result["indicators"]
    assert result["indicators"]["DUMMY_TEST_IND"]["display"]["title"]["en"] == "Dummy indicator"
    assert result["indicators"]["DUMMY_TEST_IND"]["source_agency"] == "TEST"


def test_hidden_indicator_has_null_display(tmp_path):
    indicators_dir = tmp_path / "indicators"
    sources_dir = tmp_path / "sources"
    indicators_dir.mkdir()
    sources_dir.mkdir()

    (sources_dir / "test.yaml").write_text(yaml.dump(VALID_SOURCE))
    (indicators_dir / "HIDDEN.yaml").write_text(
        yaml.dump(
            {
                "id": "HIDDEN",
                "name": {"en": "HIDDEN", "fr": "HIDDEN", "nl": "HIDDEN"},
                "unit": "index_2010",
                "frequency": "Q",
                "source_id": "test_source",
                "geo_levels": ["national"],
                "display": None,
            }
        )
    )

    out = tmp_path / "indicators.json"
    export_metadata(indicators_dir, sources_dir, out)
    result = json.loads(out.read_text())
    assert result["indicators"]["HIDDEN"]["display"] is None


def test_categories_present_and_ordered(tmp_path):
    indicators_dir = tmp_path / "indicators"
    sources_dir = tmp_path / "sources"
    indicators_dir.mkdir()
    sources_dir.mkdir()
    (sources_dir / "test.yaml").write_text(yaml.dump(VALID_SOURCE))

    out = tmp_path / "indicators.json"
    export_metadata(indicators_dir, sources_dir, out)
    result = json.loads(out.read_text())
    ids = [c["id"] for c in result["categories"]]
    assert "gdp" in ids
    assert "unemployment" in ids
