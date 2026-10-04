"""
Rewritten for ADR 0017 (docs/decisions/0017-ameco-forecast-periods.md,
ACCEPTED 2026-10-04), not deleted: every pre-existing behaviour this file
pinned (the < "2008" filter, None/"NA" skipped, obs_status hardcoded to "A",
the index_2010 rebase, the unexpected-JSON-shape error, one adapter class
serving several source_ids) is still true and still tested below, each with
an `indexed_at` added to its fixture because _parse now requires one on
every call (ADR 0017 point 1: raise rather than default to "everything is
final" when the release date is missing) -- chosen far enough past each
fixture's own periods that nothing in it is newly excluded, so none of the
original assertions change.

New in this file: AMECO's own forecast-exclusion rule, reproduced from its
Reference Metadata (26 November 2025) and ADR 0017's own worked example --
"after the May 2026 release, 2026-2027 are forecasts and 2025 is the last
outturn. From mid-November 2026, it will be 2026-2028." -- so a Spring
release excludes the last 2 years and an Autumn release excludes the last 3,
with neither count hardcoded in src/fetchers/dbnomics.py: both fall out of
comparing each period's year against `_last_outturn_year(indexed_at)`.
"""

import json
import logging

import pytest

from src.fetchers.dbnomics import DBnomicsSource

# A safe indexed_at for the pre-existing fixture below: released 2012-05-20,
# so release_year=2012, last_outturn_year=2011 -- after every period this
# fixture carries (2007-2011), so nothing in it is excluded as a forecast
# and the original row-for-row assertions are unchanged.
_SAFE_INDEXED_AT = "2012-05-20T00:00:00Z"

DBNOMICS_JSON = {
    "series": {
        "docs": [
            {
                "indexed_at": _SAFE_INDEXED_AT,
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


def _annual_fixture(indexed_at: str, periods: list[str]) -> bytes:
    """A minimal real-shaped AMECO series: one value per bare year, every
    value present and parseable (the forecast-exclusion behaviour under test
    is independent of the NA/None-skipping behaviour the original fixture
    above already covers)."""
    return json.dumps(
        {
            "series": {
                "docs": [
                    {
                        "indexed_at": indexed_at,
                        "series_code": "AMECO/PLCD/BEL.3.1.99.0.PLCD",
                        "period": periods,
                        "value": [float(100 + i) for i in range(len(periods))],
                    }
                ]
            }
        }
    ).encode()


def test_dbnomics_source_parses_dbnomics_json_identically_to_before_the_refactor(
    tmp_path, monkeypatch
):
    """DBnomicsFetcher.fetch's original behaviour, preserved verbatim:
    < "2008" filter, None/"NA" skipped, obs_status hardcoded to "A"."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(DBNOMICS_JSON).encode()),
    )

    rows = DBnomicsSource(source_id="ameco_ec").fetch(
        "https://example.test/dbnomics", cache_key="TEST_IND"
    )

    assert rows == [
        {"period": "2008-Q1", "value": 100.0, "obs_status": "A"},
        {"period": "2010-Q1", "value": 150.0, "obs_status": "A"},
        {"period": "2010-Q2", "value": 150.0, "obs_status": "A"},
    ]


def test_dbnomics_source_rebases_to_2010_when_unit_requests_it(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(DBNOMICS_JSON).encode()),
    )

    rows = DBnomicsSource(source_id="ameco_ec").fetch(
        "https://example.test/dbnomics", cache_key="TEST_IND", unit="index_2010"
    )

    # 2010 average is 150 -> every value rescaled so 2010 = 100
    assert {r["period"]: r["value"] for r in rows} == {
        "2008-Q1": 66.67,
        "2010-Q1": 100.0,
        "2010-Q2": 100.0,
    }


def test_dbnomics_source_raises_on_unexpected_json_shape(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(b'{"unexpected": true}'),
    )
    try:
        DBnomicsSource(source_id="ameco_ec").fetch("https://example.test/dbnomics", cache_key="X")
        raise AssertionError("expected ValueError")
    except ValueError as e:
        assert "Unexpected DBnomics JSON structure" in str(e)


def test_dbnomics_source_serves_ameco_and_could_serve_another_source_id(tmp_path, monkeypatch):
    """Same adapter class, different source_id -- config/sources/*.yaml gives
    every `adapter: dbnomics` source its own source_id."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(json.dumps(DBNOMICS_JSON).encode()),
    )

    ameco = DBnomicsSource(source_id="ameco_ec")
    other = DBnomicsSource(source_id="some_other_dbnomics_source")
    assert ameco.source_id == "ameco_ec"
    assert other.source_id == "some_other_dbnomics_source"
    assert ameco.adapter == other.adapter == "dbnomics"


# --- ADR 0017: forecast years excluded, counted and logged -----------------


def test_a_spring_release_excludes_the_last_two_years(tmp_path, monkeypatch):
    """ADR 0017's own worked example: indexed_at the day after AMECO's real
    21/05/2026 release -> 2025 is the last outturn year, 2026-2027 (2 years)
    are forecasts and must not reach `observations`."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(
            _annual_fixture("2026-05-22T01:32:00Z", ["2024", "2025", "2026", "2027"])
        ),
    )

    rows = DBnomicsSource(source_id="ameco_ec").fetch(
        "https://example.test/dbnomics", cache_key="LABOUR_COST_BE"
    )

    assert [r["period"] for r in rows] == ["2024", "2025"]


def test_an_autumn_release_excludes_the_last_three_years(tmp_path, monkeypatch):
    """ADR 0017's own worked example continued: "From mid-November 2026, it
    will be 2026-2028" -- the last outturn year is UNCHANGED at 2025 (the
    year a release happens in is never itself outturn, Spring or Autumn),
    but the series now carries one more forecast year, so 3 are excluded,
    not 2 -- neither count is hardcoded in the adapter."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(
            _annual_fixture("2026-11-19T00:00:00Z", ["2024", "2025", "2026", "2027", "2028"])
        ),
    )

    rows = DBnomicsSource(source_id="ameco_ec").fetch(
        "https://example.test/dbnomics", cache_key="LABOUR_COST_BE"
    )

    assert [r["period"] for r in rows] == ["2024", "2025"]


def test_the_outturn_boundary_before_a_springs_own_release(tmp_path, monkeypatch):
    """A fetch in April, before THIS year's Spring release has happened,
    must read as still being under the PRIOR November's release -- not flip
    the just-ended year to final merely because the calendar turned to a new
    year (the exact risk ADR 0017 names against anchoring on the fetch date).
    indexed_at has not moved since the prior November (DBnomics only
    re-indexes when AMECO actually republishes), so it still reads
    2025-11-18 here -- release_year 2025, last outturn 2024."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(
            _annual_fixture("2025-11-18T00:00:00Z", ["2023", "2024", "2025", "2026", "2027"])
        ),
    )

    rows = DBnomicsSource(source_id="ameco_ec").fetch(
        "https://example.test/dbnomics", cache_key="LABOUR_COST_BE"
    )

    assert [r["period"] for r in rows] == ["2023", "2024"]


def test_the_outturn_boundary_right_at_a_springs_own_release(tmp_path, monkeypatch):
    """The complement of the test above: indexed_at moves to May of the SAME
    year once AMECO actually releases, and the last outturn year advances by
    one -- this is the only thing that changes it, never the clock."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(
            _annual_fixture("2026-05-20T00:00:00Z", ["2023", "2024", "2025", "2026", "2027"])
        ),
    )

    rows = DBnomicsSource(source_id="ameco_ec").fetch(
        "https://example.test/dbnomics", cache_key="LABOUR_COST_BE"
    )

    assert [r["period"] for r in rows] == ["2023", "2024", "2025"]


def test_missing_indexed_at_raises_and_names_the_series(tmp_path, monkeypatch):
    """ADR 0017 point 1: raise rather than default to "everything is
    final" when the release date is missing -- and the message names the
    series, not just a generic complaint, so a failure is actionable."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    no_indexed_at = json.dumps(
        {
            "series": {
                "docs": [
                    {
                        "series_code": "AMECO/PLCD/BEL.3.1.99.0.PLCD",
                        "period": ["2025", "2026"],
                        "value": [100.0, 101.0],
                    }
                ]
            }
        }
    ).encode()
    monkeypatch.setattr(
        "src.fetchers.base.requests.get", lambda *a, **k: _FakeResponse(no_indexed_at)
    )

    with pytest.raises(ValueError, match="AMECO/PLCD/BEL.3.1.99.0.PLCD"):
        DBnomicsSource(source_id="ameco_ec").fetch(
            "https://example.test/dbnomics", cache_key="LABOUR_COST_BE"
        )


def test_excluded_forecast_years_are_counted_and_logged(tmp_path, monkeypatch, caplog):
    """CLAUDE.md rule 13: never silently dropped. A test asserts the count
    directly (ADR 0017's own "Tests that would guard it"), so a year where
    AMECO's rule changes shows up as a failure, not a silent row change."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(
            _annual_fixture("2026-05-22T01:32:00Z", ["2024", "2025", "2026", "2027"])
        ),
    )

    with caplog.at_level(logging.INFO, logger="fetchers.dbnomics"):
        DBnomicsSource(source_id="ameco_ec").fetch(
            "https://example.test/dbnomics", cache_key="LABOUR_COST_BE"
        )

    [record] = [r for r in caplog.records if "excluded" in r.message]
    assert "excluded 2 forecast period(s)" in record.message
    assert "2026" in record.message and "2027" in record.message


def test_no_periods_excluded_means_no_exclusion_log_line(tmp_path, monkeypatch, caplog):
    """The log line is conditional on something actually having been
    excluded -- a normal run with no forecast years in the response (every
    period already at or before the last outturn year) logs nothing extra."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(_annual_fixture("2026-05-22T01:32:00Z", ["2024", "2025"])),
    )

    with caplog.at_level(logging.INFO, logger="fetchers.dbnomics"):
        rows = DBnomicsSource(source_id="ameco_ec").fetch(
            "https://example.test/dbnomics", cache_key="LABOUR_COST_BE"
        )

    assert [r["period"] for r in rows] == ["2024", "2025"]
    assert not [r for r in caplog.records if "excluded" in r.message]
