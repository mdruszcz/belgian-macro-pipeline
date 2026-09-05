import json

from src.fetchers.eurostat import EurostatSource

DBNOMICS_JSON = {
    "series": {
        "docs": [
            {
                "period": ["2007", "2008-Q1", "2010-Q1", "2010-Q2", "2011-Q1", "2011-Q2"],
                "value": [10.0, 100.0, 150.0, 150.0, "NA", None],
            }
        ]
    }
}


class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        pass


def test_eurostat_source_parses_dbnomics_json_identically_to_before_the_refactor(
    tmp_path, monkeypatch
):
    """DBnomicsFetcher.fetch's original behaviour, preserved verbatim:
    < "2008" filter, None/"NA" skipped, obs_status hardcoded to "A"."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(DBNOMICS_JSON).encode()),
    )

    rows = EurostatSource(source_id="eurostat").fetch(
        "https://example.test/dbnomics", cache_key="TEST_IND"
    )

    assert rows == [
        {"period": "2008-Q1", "value": 100.0, "obs_status": "A"},
        {"period": "2010-Q1", "value": 150.0, "obs_status": "A"},
        {"period": "2010-Q2", "value": 150.0, "obs_status": "A"},
    ]


def test_eurostat_source_rebases_to_2010_when_unit_requests_it(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(DBNOMICS_JSON).encode()),
    )

    rows = EurostatSource(source_id="eurostat").fetch(
        "https://example.test/dbnomics", cache_key="TEST_IND", unit="index_2010"
    )

    # 2010 average is 150 -> every value rescaled so 2010 = 100
    assert {r["period"]: r["value"] for r in rows} == {
        "2008-Q1": 66.67,
        "2010-Q1": 100.0,
        "2010-Q2": 100.0,
    }


def test_eurostat_source_raises_on_unexpected_json_shape(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(b'{"unexpected": true}'),
    )
    try:
        EurostatSource(source_id="eurostat").fetch("https://example.test/dbnomics", cache_key="X")
        raise AssertionError("expected ValueError")
    except ValueError as e:
        assert "Unexpected DBnomics JSON structure" in str(e)


def test_eurostat_source_serves_both_eurostat_and_ameco_source_ids(tmp_path, monkeypatch):
    """Same adapter class, different source_id -- config/sources/*.yaml gives
    dbnomics_eurostat and dbnomics_ameco the same `adapter: dbnomics`."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(DBNOMICS_JSON).encode()),
    )

    eurostat = EurostatSource(source_id="eurostat")
    ameco = EurostatSource(source_id="ameco_ec")
    assert eurostat.source_id == "eurostat"
    assert ameco.source_id == "ameco_ec"
    assert eurostat.adapter == ameco.adapter == "dbnomics"
