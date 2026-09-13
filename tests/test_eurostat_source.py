"""Tests for the new direct-Eurostat adapter (international pilot PR 1).

tests/fixtures/eurostat/gov_10dd_edpt1.json is a REAL recorded response
(fetched 2026-09-13 from
https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/gov_10dd_edpt1?format=JSON&lang=EN&na_item=GD&sector=S13&unit=PC_GDP&sinceTimePeriod=2023),
31 geographies (EU27_2020, three euro-area vintages, 27 member states) x 3
annual periods, no status flags -- it proves the cube-walk and the offset
arithmetic against real data. The flag-mapping and refusal branches use small
synthetic JSON-stat fragments instead: constructing a real response that
happens to carry every flag letter, an unpinned second dimension AND an
unknown flag would be less legible than building exactly the shape each test
needs.
"""

import json
from pathlib import Path

import pytest

from src.fetchers.base import FetchError
from src.fetchers.eurostat import EurostatSource, singleton_geo

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "eurostat"


class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        pass


def _cube(dims, sizes, geo_codes, time_codes, values=None, status=None):
    """A minimal JSON-stat 2.0 fragment: every non-geo/time dimension pinned
    to one code, `geo`/`time` carrying the given codes in position order."""
    dimension = {}
    for dim in dims:
        if dim == "geo":
            dimension["geo"] = {
                "category": {"index": dict(zip(geo_codes, range(len(geo_codes)), strict=True))}
            }
        elif dim == "time":
            dimension["time"] = {
                "category": {"index": dict(zip(time_codes, range(len(time_codes)), strict=True))}
            }
        else:
            dimension[dim] = {"category": {"index": {"X": 0}}}
    return {
        "version": "2.0",
        "id": dims,
        "size": sizes,
        "dimension": dimension,
        "value": values or {},
        "status": status or {},
    }


def test_build_url_appends_dataset_and_filters_and_since():
    url = EurostatSource.build_url(
        "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data",
        "gov_10dd_edpt1",
        {"na_item": "GD", "sector": "S13", "unit": "PC_GDP"},
        "2008",
    )
    assert url == (
        "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/gov_10dd_edpt1"
        "?format=JSON&lang=EN&na_item=GD&sector=S13&unit=PC_GDP&sinceTimePeriod=2008"
    )


def test_build_url_with_no_filters_still_asks_for_every_geo():
    url = EurostatSource.build_url("https://example.test", "namq_10_gdp", {}, "2008")
    assert url == "https://example.test/namq_10_gdp?format=JSON&lang=EN&sinceTimePeriod=2008"


def test_parses_every_geography_in_a_real_recorded_response(tmp_path, monkeypatch):
    raw = (FIXTURES / "gov_10dd_edpt1.json").read_bytes()
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr("src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(raw))

    rows = EurostatSource().fetch(
        "https://example.test/gov_10dd_edpt1", cache_key="DEBT", dataset="gov_10dd_edpt1"
    )

    geos = {r["geo"] for r in rows}
    assert len(geos) == 31
    assert "BE" in geos and "EU27_2020" in geos and "EA21" in geos
    # 31 geos x 3 periods, no gaps in this recording -- every cell a row.
    assert len(rows) == 93
    be_2023 = next(r for r in rows if r["geo"] == "BE" and r["period"] == "2023")
    assert be_2023["obs_status"] == "final"
    assert isinstance(be_2023["value"], float)
    for row in rows:
        assert set(row.keys()) == {"geo", "period", "value", "obs_status"}


def test_a_position_with_no_value_and_no_flag_produces_no_row(tmp_path, monkeypatch):
    cube = _cube(
        ["geo", "time"], [2, 1], ["BE", "DE"], ["2023"], values={"0": 100.0}
    )  # DE (offset 1) absent entirely
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(cube).encode()),
    )

    rows = EurostatSource().fetch("https://example.test/x", cache_key="X", dataset="x")

    assert [r["geo"] for r in rows] == ["BE"]


@pytest.mark.parametrize(
    "flag,expected_status,expected_value",
    [
        ("", "final", 100.0),
        ("p", "provisional", 100.0),
        ("e", "estimate", 100.0),
        ("f", "estimate", 100.0),
        ("b", "revised", 100.0),
        ("d", "revised", 100.0),
    ],
)
def test_flags_with_a_value_map_to_the_right_status(
    tmp_path, monkeypatch, flag, expected_status, expected_value
):
    status = {"0": flag} if flag else {}
    cube = _cube(["geo", "time"], [1, 1], ["BE"], ["2023"], values={"0": 100.0}, status=status)
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(cube).encode()),
    )

    rows = EurostatSource().fetch("https://example.test/x", cache_key="X", dataset="x")

    assert rows == [
        {"geo": "BE", "period": "2023", "value": expected_value, "obs_status": expected_status}
    ]


@pytest.mark.parametrize("flag,expected_status", [("c", "suppressed"), ("z", "na")])
def test_a_flag_with_no_value_still_produces_a_row_in_a_known_state(
    tmp_path, monkeypatch, flag, expected_status
):
    """Suppressed and na are known states, not gaps: a position Eurostat
    explicitly flagged but did not carry a number for is a row with value
    None, never simply skipped and never a fabricated zero."""
    cube = _cube(["geo", "time"], [1, 1], ["BE"], ["2023"], values={}, status={"0": flag})
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(cube).encode()),
    )

    rows = EurostatSource().fetch("https://example.test/x", cache_key="X", dataset="x")

    assert rows == [{"geo": "BE", "period": "2023", "value": None, "obs_status": expected_status}]


def test_an_unrecognized_flag_refuses_rather_than_guesses(tmp_path, monkeypatch):
    cube = _cube(["geo", "time"], [1, 1], ["BE"], ["2023"], values={"0": 1.0}, status={"0": "u"})
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(cube).encode()),
    )

    with pytest.raises(FetchError, match="unrecognized Eurostat OBS_FLAG 'u'"):
        EurostatSource().fetch("https://example.test/x", cache_key="X", dataset="x")


def test_a_non_singleton_dimension_other_than_geo_time_is_refused(tmp_path, monkeypatch):
    """A config whose filters did not pin `unit` down to one code would
    silently mix two units into one series -- refused instead."""
    cube = {
        "id": ["unit", "geo", "time"],
        "size": [2, 1, 1],
        "dimension": {
            "unit": {"category": {"index": {"EUR": 0, "USD": 1}}},
            "geo": {"category": {"index": {"BE": 0}}},
            "time": {"category": {"index": {"2023": 0}}},
        },
        "value": {"0": 1.0, "1": 2.0},
        "status": {},
    }
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(cube).encode()),
    )

    with pytest.raises(FetchError, match="dimension 'unit' has 2 code"):
        EurostatSource().fetch("https://example.test/x", cache_key="X", dataset="x")


def test_a_response_with_no_geo_or_time_dimension_is_refused(tmp_path, monkeypatch):
    cube = {
        "id": ["unit"],
        "size": [1],
        "dimension": {"unit": {"category": {"index": {"EUR": 0}}}},
        "value": {"0": 1.0},
    }
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(cube).encode()),
    )

    with pytest.raises(FetchError, match="no geo/time dimension"):
        EurostatSource().fetch("https://example.test/x", cache_key="X", dataset="x")


# --- singleton_geo: the national-fetch path -------------------------------


def test_singleton_geo_reshapes_a_single_country_response():
    rows = [
        {"geo": "BE", "period": "2023", "value": 1.0, "obs_status": "final"},
        {"geo": "BE", "period": "2024", "value": 2.0, "obs_status": "provisional"},
    ]
    assert singleton_geo(rows) == [
        {"period": "2023", "value": 1.0, "obs_status": "final"},
        {"period": "2024", "value": 2.0, "obs_status": "provisional"},
    ]


def test_singleton_geo_refuses_more_than_one_geography():
    rows = [
        {"geo": "BE", "period": "2023", "value": 1.0, "obs_status": "final"},
        {"geo": "DE", "period": "2023", "value": 2.0, "obs_status": "final"},
    ]
    with pytest.raises(FetchError, match=r"\['BE', 'DE'\]"):
        singleton_geo(rows)


def test_singleton_geo_refuses_zero_geographies():
    with pytest.raises(FetchError):
        singleton_geo([])
