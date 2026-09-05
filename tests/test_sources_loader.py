import yaml

import belgian_macro_db as bmdb


def test_loader_reconstructs_gdp_quarterly_yy(tmp_path, monkeypatch):
    indicators_dir = tmp_path / "indicators"
    sources_dir = tmp_path / "sources"
    indicators_dir.mkdir()
    sources_dir.mkdir()

    (sources_dir / "nbb.yaml").write_text(
        yaml.dump(
            {
                "source_id": "nbb",
                "name": "NBB",
                "agency": "NBB",
                "adapter": "nbb",
                "base_url": "https://nsidisseminate-stat.nbb.be/rest/data/BE2",
                "licence": None,
                "catalog_ref": None,
                "cadence": "daily",
                "is_active": True,
            }
        )
    )
    (indicators_dir / "GDP_QUARTERLY_YY.yaml").write_text(
        yaml.dump(
            {
                "id": "GDP_QUARTERLY_YY",
                "name": {"en": "Quarterly GDP growth (Y-Y)", "fr": "x", "nl": "x"},
                "unit": "percent_yy",
                "frequency": "Q",
                "source_id": "nbb",
                "geo_levels": ["national"],
                "preferred_direction": "higher_is_better",
                "fetch": {"query": ",DF_QNA_DISS,1.0/Q.1.B1GM.VZ.LY.N?startPeriod=2000-Q1"},
                "display": {
                    "category": "gdp",
                    "title": {"en": "x", "fr": "x", "nl": "x"},
                    "sort_order": 1,
                },
            }
        )
    )

    monkeypatch.setattr(bmdb, "CONFIG_DIR", tmp_path)
    sources = bmdb._load_sources()

    assert sources["GDP_QUARTERLY_YY"]["frequency"] == "Q"
    assert sources["GDP_QUARTERLY_YY"]["source_agency"] == "NBB"
    assert sources["GDP_QUARTERLY_YY"]["type"] == "nbb"
    assert sources["GDP_QUARTERLY_YY"]["url"].startswith("https://nsidisseminate-stat.nbb.be")
    assert sources["GDP_QUARTERLY_YY"]["unit"] == "percent_yy"
