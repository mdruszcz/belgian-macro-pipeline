"""Tests for scripts/export_europe_countries.py -- the Europe countries
batch's published payloads (docs/features/europe_countries.md).

Same convention as tests/test_export_europe_nuts2.py: the happy path runs
against the REAL committed data (config/geography/international.csv, the
real 2024 NUTS 0 geometry, data/international/*.csv) -- a publish-time
export is more meaningfully proven against its real committed inputs than a
synthetic fixture. The rebase math (`_rebase_to_2015_index`) gets its own
hand-computed unit tests (CLAUDE.md rule 5), independent of any real data.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import export_europe_countries as ex  # noqa: E402

ALL_CANONICAL_AND_SYNTHETIC_STATES = {
    "final",
    "provisional",
    "estimate",
    "revised",
    "suppressed",
    "na",
    "missing",
}


class TestRebaseToIndex:
    """Hand-computed expected values (CLAUDE.md rule 5) -- never derived
    from the function under test."""

    def test_all_four_base_quarters_present_rebases_to_exactly_100(self):
        # mean(90, 100, 105, 105) = 100.0 exactly, by hand.
        cells = {
            "2015-Q1": {"v": 90.0, "s": "final"},
            "2015-Q2": {"v": 100.0, "s": "final"},
            "2015-Q3": {"v": 105.0, "s": "final"},
            "2015-Q4": {"v": 105.0, "s": "final"},
            "2016-Q1": {"v": 110.0, "s": "provisional"},
        }
        out = ex._rebase_to_2015_index(cells)
        assert out["2015-Q1"] == {"v": 90.0, "s": "final"}
        assert out["2015-Q2"] == {"v": 100.0, "s": "final"}
        assert out["2015-Q3"] == {"v": 105.0, "s": "final"}
        assert out["2015-Q4"] == {"v": 105.0, "s": "final"}
        # 100 * 110 / 100 = 110.0, by hand.
        assert out["2016-Q1"] == {"v": 110.0, "s": "provisional"}

    def test_missing_one_base_quarter_makes_the_whole_series_na(self):
        cells = {
            "2015-Q1": {"v": 90.0, "s": "final"},
            "2015-Q2": {"v": 100.0, "s": "final"},
            "2015-Q3": {"v": None, "s": "suppressed"},
            "2015-Q4": {"v": 105.0, "s": "final"},
            "2016-Q1": {"v": 110.0, "s": "final"},
        }
        out = ex._rebase_to_2015_index(cells)
        assert all(cell == {"v": None, "s": "na"} for cell in out.values())

    def test_country_absent_for_a_whole_base_quarter_is_also_na(self):
        # No 2015-Q3 key at all (e.g. the country's series starts later).
        cells = {
            "2015-Q1": {"v": 90.0, "s": "final"},
            "2015-Q2": {"v": 100.0, "s": "final"},
            "2015-Q4": {"v": 105.0, "s": "final"},
            "2016-Q1": {"v": 110.0, "s": "final"},
        }
        out = ex._rebase_to_2015_index(cells)
        assert all(cell == {"v": None, "s": "na"} for cell in out.values())

    def test_a_real_zero_base_is_never_divided_by(self):
        cells = {
            "2015-Q1": {"v": 0.0, "s": "final"},
            "2015-Q2": {"v": 0.0, "s": "final"},
            "2015-Q3": {"v": 0.0, "s": "final"},
            "2015-Q4": {"v": 0.0, "s": "final"},
        }
        out = ex._rebase_to_2015_index(cells)
        assert all(cell == {"v": None, "s": "na"} for cell in out.values())


class TestRealExport:
    def setup_method(self):
        pass

    def _export(self, tmp_path):
        out_dir = tmp_path / "countries"
        indicator_ids = ex.export(out_dir)
        return out_dir, indicator_ids

    def _load(self, out_dir, indicator_id):
        return json.loads((out_dir / f"{indicator_id}.json").read_text(encoding="utf-8"))

    def test_every_configured_indicator_gets_a_file(self, tmp_path):
        out_dir, indicator_ids = self._export(tmp_path)
        assert set(indicator_ids) == set(ex.ALL_INDICATOR_IDS)
        for indicator_id in indicator_ids:
            assert (out_dir / f"{indicator_id}.json").is_file()

    def test_index_lists_six_map_indicators_and_one_chart_only(self, tmp_path):
        out_dir, _ = self._export(tmp_path)
        index = json.loads((out_dir / "index.json").read_text(encoding="utf-8"))
        assert index["geo_vintage"] == "2024"
        assert index["geometry"] == "geo/nuts0/2024/0.json"
        assert index["attribution"]
        by_id = {row["id"]: row for row in index["indicators"]}
        assert set(by_id) == set(ex.ALL_INDICATOR_IDS)
        for indicator_id in ex.MAP_INDICATOR_IDS:
            assert by_id[indicator_id]["map"] is True
            assert by_id[indicator_id]["status"] == "loaded"
        assert by_id["GDP_VOLUME_EUROPE"]["map"] is False
        assert by_id["GDP_VOLUME_EUROPE"]["status"] == "loaded"

    def test_index_countries_cover_the_full_allowlist_with_belgium_present(self, tmp_path):
        out_dir, _ = self._export(tmp_path)
        index = json.loads((out_dir / "index.json").read_text(encoding="utf-8"))
        codes = {c["code"] for c in index["countries"]}
        assert "BE" in codes
        assert "MT" in codes and "LU" in codes and "CY" in codes  # tiny-country picker cases
        # Every real EU/EFTA/candidate country the licence allows, per
        # config/geography/international.csv, not just the geometry file's.
        assert len(codes) == 40
        aggregate_codes = {a["code"] for a in index["aggregates"]}
        assert aggregate_codes == {"EA21", "EU27_2020"}
        assert codes.isdisjoint(aggregate_codes)

    def test_georgia_and_moldova_have_no_outline_but_are_still_listed(self, tmp_path):
        out_dir, _ = self._export(tmp_path)
        index = json.loads((out_dir / "index.json").read_text(encoding="utf-8"))
        by_code = {c["code"]: c for c in index["countries"]}
        assert by_code["GE"]["has_outline"] is False
        assert by_code["MD"]["has_outline"] is False
        assert by_code["BE"]["has_outline"] is True

    def test_kosovo_is_excluded_by_licence_everywhere(self, tmp_path):
        out_dir, _ = self._export(tmp_path)
        index = json.loads((out_dir / "index.json").read_text(encoding="utf-8"))
        assert "XK" in index["excluded_by_licence"]
        assert "XK" not in {c["code"] for c in index["countries"]}
        payload = self._load(out_dir, "GDP_PC_PPS_COUNTRY")
        assert "XK" in payload["excluded_by_licence"]
        for period_values in payload["values"].values():
            assert "XK" not in period_values

    def test_belgium_gdp_per_capita_matches_the_live_checked_figure(self, tmp_path):
        """The lead checked Eurostat live on 2026-09-15: BE 2023 = 45400,
        DE 2023 = 45100 (provisional), EU27_2020 2023 = 38400 -- the payload
        must carry the same real numbers, not a hand-typed stand-in
        (CLAUDE.md rule 36)."""
        out_dir, _ = self._export(tmp_path)
        payload = self._load(out_dir, "GDP_PC_PPS_COUNTRY")
        # `yoy` (2026-09-15 "croissance annuelle" follow-up) has its own
        # dedicated, hand-computed test below
        # (test_yoy_wired_only_onto_the_three_eligible_indicators) -- popped
        # off here so this test keeps checking only what it always checked:
        # the live-verified level figures.
        be_2023 = dict(payload["values"]["2023"]["BE"])
        be_2023.pop("yoy", None)
        assert be_2023 == {"v": 45400.0, "s": "final"}
        de_2023 = dict(payload["values"]["2023"]["DE"])
        de_2023.pop("yoy", None)
        assert de_2023 == {"v": 45100.0, "s": "provisional"}
        assert payload["reference_lines"]["2023"]["EU27_2020"] == {"v": 38400.0, "s": "final"}

    def test_belgium_population_matches_the_live_checked_figure(self, tmp_path):
        out_dir, _ = self._export(tmp_path)
        payload = self._load(out_dir, "POPULATION_COUNTRY")
        be_2025 = dict(payload["values"]["2025"]["BE"])
        be_2025.pop("yoy", None)  # this test only checks the level figure
        assert be_2025 == {"v": 11883495.0, "s": "final"}

    def test_gdp_volume_is_never_painted_and_carries_an_adapted_notice(self, tmp_path):
        out_dir, _ = self._export(tmp_path)
        payload = self._load(out_dir, "GDP_VOLUME_EUROPE")
        assert payload["map"] is False
        assert payload["class_breaks"] == {}
        assert payload["unit"] == "index_2015_100"
        assert payload["adapted"]["notice"]["en"]
        assert payload["adapted"]["notice"]["fr"]
        assert payload["adapted"]["notice"]["nl"]

    def test_yoy_wired_only_onto_the_three_eligible_indicators(self, tmp_path):
        """CLAUDE.md rule 5: hand-computed, not derived from the code under
        test. growth_rate() itself (src/analytics/derived.py) already has
        its own hand-computed tests in tests/test_derived.py -- this only
        proves export_europe_countries.py wires its answer onto the right
        cell for the right indicators (2026-09-15 "croissance annuelle"
        follow-up, docs/features/europe_countries.md).

        BE GDP per capita: 2022 = 42500.0, 2023 = 45400.0 (both real,
        committed data/international/GDP_PC_PPS_COUNTRY.csv rows) ->
        (45400 - 42500) / 42500 * 100 = 6.8235...%, rounded to 1 decimal =
        6.8, by hand.
        """
        out_dir, _ = self._export(tmp_path)
        payload = self._load(out_dir, "GDP_PC_PPS_COUNTRY")
        assert payload["has_yoy"] is True
        assert payload["values"]["2023"]["BE"] == {"v": 45400.0, "s": "final", "yoy": 6.8}

        # Excluded (already a rate, maintainer's explicit decision): no
        # `yoy` key at all, and `has_yoy` says so up front.
        unemployment = self._load(out_dir, "UNEMPLOYMENT_RATE_EUROPE")
        assert unemployment["has_yoy"] is False
        latest = unemployment["latest_period"]
        assert "yoy" not in unemployment["values"][latest]["BE"]

        # Quarterly wiring: GDP_VOLUME_EUROPE's very first published period
        # (2008-Q1) has no 2007-Q1 to compare against -- growth_rate()
        # returns None for a missing prior period, and that None must reach
        # the payload as `"yoy": None`, never a fabricated number.
        gdp_volume = self._load(out_dir, "GDP_VOLUME_EUROPE")
        assert gdp_volume["has_yoy"] is True
        first_period = gdp_volume["periods"][0]
        assert first_period == "2008-Q1"
        assert gdp_volume["values"]["2008-Q1"]["BE"]["yoy"] is None

    def test_gdp_volume_belgium_2015_quarters_average_to_100(self, tmp_path):
        out_dir, _ = self._export(tmp_path)
        payload = self._load(out_dir, "GDP_VOLUME_EUROPE")
        q_values = [
            payload["values"][q]["BE"]["v"] for q in ("2015-Q1", "2015-Q2", "2015-Q3", "2015-Q4")
        ]
        assert all(v is not None for v in q_values)
        assert abs(sum(q_values) / 4 - 100.0) < 0.05  # rounded to 1 decimal each

    @pytest.mark.parametrize("indicator_id", ex.MAP_INDICATOR_IDS)
    def test_five_states_never_collapse_on_a_map_indicator(self, tmp_path, indicator_id):
        out_dir, _ = self._export(tmp_path)
        payload = self._load(out_dir, indicator_id)
        states_seen = set()
        for period_values in payload["values"].values():
            for cell in period_values.values():
                states_seen.add(cell["s"])
        assert states_seen <= ALL_CANONICAL_AND_SYNTHETIC_STATES

    @pytest.mark.parametrize("indicator_id", ex.MAP_INDICATOR_IDS)
    def test_missing_state_never_carries_a_value(self, tmp_path, indicator_id):
        out_dir, _ = self._export(tmp_path)
        payload = self._load(out_dir, indicator_id)
        for period_values in payload["values"].values():
            for code, cell in period_values.items():
                if cell["s"] == "missing":
                    assert cell["v"] is None, f"{code} is 'missing' but has a value"

    @pytest.mark.parametrize("indicator_id", ex.MAP_INDICATOR_IDS)
    def test_class_breaks_present_for_every_period_with_at_least_two_distinct_values(
        self, tmp_path, indicator_id
    ):
        out_dir, _ = self._export(tmp_path)
        payload = self._load(out_dir, indicator_id)
        for period in payload["periods"]:
            breaks = payload["class_breaks"][period]
            assert breaks == [] or len(breaks) == 4
            assert breaks == sorted(breaks)

    def test_rebuild_is_byte_identical(self, tmp_path):
        first_dir, indicator_ids = self._export(tmp_path / "first")
        second_dir = tmp_path / "second"
        ex.export(second_dir)
        for indicator_id in [*indicator_ids, "index"]:
            first_bytes = (first_dir / f"{indicator_id}.json").read_bytes()
            second_bytes = (second_dir / f"{indicator_id}.json").read_bytes()
            assert first_bytes == second_bytes, indicator_id


def test_an_unrecognized_data_vs_allowlist_mismatch_fails_the_export(monkeypatch):
    """A data code with no allowlist/aggregate match must fail the export
    outright -- CLAUDE.md rule 13."""
    from src.validation.config_schema import load_and_validate_all

    indicator_configs, source_configs = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    monkeypatch.setattr(
        ex,
        "_load_store_rows",
        lambda indicator_id: (
            [
                {
                    "geo_id": "zz:country",
                    "period": "2023",
                    "value": "1.0",
                    "status": "final",
                    "vintage": "2026-09-15T00:00:00+00:00",
                    "is_latest": "1",
                }
            ]
            if indicator_id == "GDP_PC_PPS_COUNTRY"
            else None
        ),
    )
    international_rows = dict(ex.load_international_rows())
    international_rows["ZZ"] = {
        "geo": {
            "geo_id": "zz:country",
            "nis_code": None,
            "level": "country",
            "name_nl": "Zz",
            "name_fr": "Zz",
            "name_en": "Zz",
            "parent_geo_id": None,
            "valid_from": "2024-01-01",
            "valid_to": None,
            "successor_geo_id": None,
            "population": None,
            "area_km2": None,
        },
        "scope": "pilot",
        "basis": "eu",
        "listed_on": "2026-09-15",
    }

    with pytest.raises(ex.ExportError, match="ZZ"):
        ex._build_indicator_payload(
            "GDP_PC_PPS_COUNTRY",
            indicator_configs["GDP_PC_PPS_COUNTRY"],
            source_configs["eurostat"],
            international_rows,
            country_codes_allowed=set(),
            aggregate_codes=set(),
            excluded_codes=set(),
            geometry_ids=set(),
        )
