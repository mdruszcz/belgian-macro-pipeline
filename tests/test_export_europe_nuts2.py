"""Tests for scripts/export_europe_nuts2.py -- the Europe NUTS 2 batch's
published payloads (Europe NUTS 2, batch B2, docs/features/europe_nuts2.md).

Most of these run against the REAL committed data (config/geography/nuts2.csv,
the real 2024 geometry, data/nuts2/GDP_PC_PPS_NUTS2.csv) -- this is a
publish-time export, not a computed statistic, so proving it behaves
correctly on the real committed inputs is more meaningful than a synthetic
fixture for the happy path. The one failure-mode test (an unrecognized
data-vs-geometry mismatch) uses small, hand-built stand-ins via monkeypatch,
matching this repo's own convention for edge cases a real fetch cannot be
relied on to reproduce (see tests/test_sync_international_pilot.py's
unknown-geography test).
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import export_europe_nuts2 as ex  # noqa: E402

ALL_CANONICAL_AND_SYNTHETIC_STATES = {
    "final",
    "provisional",
    "estimate",
    "revised",
    "suppressed",
    "na",
    "missing",
}


def test_quantile_breaks_are_deterministic_and_ordered():
    values = [1.0, 5.0, 3.0, 9.0, 2.0, 7.0, 4.0, 8.0, 6.0, 10.0]
    breaks = ex._quantile_breaks(values)
    assert breaks == ex._quantile_breaks(list(reversed(values)))  # order-independent
    assert breaks == sorted(breaks)
    assert len(breaks) == 4


def test_quantile_breaks_empty_below_two_distinct_values():
    assert ex._quantile_breaks([]) == []
    assert ex._quantile_breaks([5.0]) == []
    assert ex._quantile_breaks([5.0, 5.0, 5.0]) == []


def test_round_sensibly_keeps_three_significant_figures():
    assert ex._round_sensibly(76234.5) == 76200
    assert ex._round_sensibly(0.0) == 0.0
    assert ex._round_sensibly(12.345) == 12.3


def test_licence_split_classifies_by_two_letter_prefix():
    kept, excluded, unresolved = ex._licence_split({"BE21", "UKC1", "ZZ99"})
    assert kept == {"BE21"}
    assert excluded == {"UKC1"}
    assert unresolved == {"ZZ99"}


class TestRealExport:
    @pytest.fixture(autouse=True)
    def _export_once(self, tmp_path):
        self.out_dir = tmp_path / "nuts2"
        self.indicator_ids = ex.export(self.out_dir)

    def _load(self, indicator_id):
        return json.loads((self.out_dir / f"{indicator_id}.json").read_text(encoding="utf-8"))

    def test_every_configured_indicator_gets_a_file_whether_or_not_it_has_data(self):
        assert set(self.indicator_ids) == {
            "GDP_PC_PPS_NUTS2",
            "POPULATION_NUTS2",
            "UNEMPLOYMENT_RATE_NUTS2",
        }
        for indicator_id in self.indicator_ids:
            assert (self.out_dir / f"{indicator_id}.json").is_file()

    def test_index_lists_every_indicator_as_loaded(self):
        """All three configured indicators load as of 2026-09-14: the
        maintainer's empty-u-is-suppressed decision (docs/decisions/0010-
        eurostat-compound-observation-flags.md, amendment) unblocked
        UNEMPLOYMENT_RATE_NUTS2, the last one still blocked before this."""
        index = json.loads((self.out_dir / "index.json").read_text(encoding="utf-8"))
        assert index["nuts_version"] == "2024"
        assert index["geometry"] == "geo/nuts2/2024/2.json"
        assert index["attribution"]
        ids_and_status = {row["id"]: row["status"] for row in index["indicators"]}
        assert ids_and_status == {
            "GDP_PC_PPS_NUTS2": "loaded",
            "POPULATION_NUTS2": "loaded",
            "UNEMPLOYMENT_RATE_NUTS2": "loaded",
        }

    def test_unemployment_payload_is_loaded_not_blocked(self):
        payload = self._load("UNEMPLOYMENT_RATE_NUTS2")
        assert payload["status"] == "loaded"
        assert "blocked_reason" not in payload
        assert payload["years"]
        assert payload["values"]

    def test_unemployment_latest_year_is_2025_and_suppressed_cells_never_carry_a_value(self):
        payload = self._load("UNEMPLOYMENT_RATE_NUTS2")
        assert payload["latest_year"] == "2025"
        v2025 = payload["values"]["2025"]
        suppressed = [code for code, cell in v2025.items() if cell["s"] == "suppressed"]
        assert suppressed  # the maintainer's decision must actually be exercised, not vacuous
        for code in suppressed:
            assert v2025[code]["v"] is None, code
        # And a real observed case: DE22/2020 was one of the 149 originally-
        # blocking cells (flag "bu"), now suppressed.
        assert payload["values"]["2020"]["DE22"] == {"v": None, "s": "suppressed"}

    @pytest.mark.parametrize(
        "indicator_id", ["GDP_PC_PPS_NUTS2", "POPULATION_NUTS2", "UNEMPLOYMENT_RATE_NUTS2"]
    )
    def test_five_states_never_collapse_in_a_loaded_indicator(self, indicator_id):
        payload = self._load(indicator_id)
        states_seen = set()
        for year_values in payload["values"].values():
            for cell in year_values.values():
                states_seen.add(cell["s"])
        assert states_seen <= ALL_CANONICAL_AND_SYNTHETIC_STATES
        assert "missing" in states_seen  # a real, known gap must appear

    @pytest.mark.parametrize(
        "indicator_id", ["GDP_PC_PPS_NUTS2", "POPULATION_NUTS2", "UNEMPLOYMENT_RATE_NUTS2"]
    )
    def test_missing_state_never_carries_a_value(self, indicator_id):
        payload = self._load(indicator_id)
        for year_values in payload["values"].values():
            for code, cell in year_values.items():
                if cell["s"] == "missing":
                    assert cell["v"] is None, f"{code} is 'missing' but has a value"

    @pytest.mark.parametrize(
        "indicator_id", ["GDP_PC_PPS_NUTS2", "POPULATION_NUTS2", "UNEMPLOYMENT_RATE_NUTS2"]
    )
    def test_a_real_value_is_never_a_bare_zero_standing_in_for_missing(self, indicator_id):
        """Every 'missing' cell's value is null (checked above); this is the
        complementary direction -- a real, present region never gets v=0 as
        a placeholder instead of a real number or an explicit missing/null."""
        payload = self._load(indicator_id)
        for year_values in payload["values"].values():
            for cell in year_values.values():
                if cell["s"] != "missing" and cell["v"] is None:
                    assert cell["s"] in ("suppressed", "na"), cell

    def test_latest_year_is_2024_and_has_at_least_one_real_value(self):
        payload = self._load("GDP_PC_PPS_NUTS2")
        assert payload["latest_year"] == "2024"
        assert any(cell["v"] is not None for cell in payload["values"]["2024"].values())

    def test_population_latest_year_is_2025_and_has_at_least_one_real_value(self):
        payload = self._load("POPULATION_NUTS2")
        assert payload["latest_year"] == "2025"
        assert any(cell["v"] is not None for cell in payload["values"]["2025"].values())

    @pytest.mark.parametrize(
        "indicator_id", ["GDP_PC_PPS_NUTS2", "POPULATION_NUTS2", "UNEMPLOYMENT_RATE_NUTS2"]
    )
    def test_class_breaks_present_for_every_year_with_at_least_two_distinct_values(
        self, indicator_id
    ):
        payload = self._load(indicator_id)
        for year in payload["years"]:
            breaks = payload["class_breaks"][year]
            assert breaks == [] or len(breaks) == 4
            assert breaks == sorted(breaks)

    @pytest.mark.parametrize(
        "indicator_id", ["GDP_PC_PPS_NUTS2", "POPULATION_NUTS2", "UNEMPLOYMENT_RATE_NUTS2"]
    )
    def test_geometry_ids_and_no_outline_codes_account_for_every_value_key(self, indicator_id):
        """Every key in `values` is either a real 2024 geometry id or one of
        the documented no-outline codes -- the cross-check this exporter is
        required to make (docs/features/europe_nuts2.md, decision 8)."""
        payload = self._load(indicator_id)
        geometry_ids = ex._geometry_ids()
        no_outline_codes = set(ex.KNOWN_NO_OUTLINE) | set(ex.SUPERSEDED_NUTS_VINTAGE_NO_OUTLINE)
        for year_values in payload["values"].values():
            for code in year_values:
                assert code in geometry_ids or code in no_outline_codes, code

    @pytest.mark.parametrize(
        "indicator_id", ["GDP_PC_PPS_NUTS2", "POPULATION_NUTS2", "UNEMPLOYMENT_RATE_NUTS2"]
    )
    def test_excluded_by_licence_regions_never_appear_in_values(self, indicator_id):
        payload = self._load(indicator_id)
        excluded = set(payload["excluded_by_licence"])
        assert excluded  # XK00 at minimum
        for year_values in payload["values"].values():
            assert excluded.isdisjoint(year_values)

    def test_rebuild_is_byte_identical(self, tmp_path):
        second_dir = tmp_path / "second"
        ex.export(second_dir)
        for indicator_id in [*self.indicator_ids, "index"]:
            first_bytes = (self.out_dir / f"{indicator_id}.json").read_bytes()
            second_bytes = (second_dir / f"{indicator_id}.json").read_bytes()
            assert first_bytes == second_bytes, indicator_id


def test_an_unrecognized_data_vs_geometry_mismatch_fails_the_export(tmp_path, monkeypatch):
    """A data code with no geometry outline that is NOT one of the
    documented no-outline codes must fail the export outright -- CLAUDE.md
    rule 13, the known case is documented, the unknown case is refused."""
    fake_geo_dict = {
        "geo_id": "zz99:nuts2",
        "nis_code": None,
        "level": "nuts2",
        "name_nl": "Zz99",
        "name_fr": "Zz99",
        "name_en": "Zz99",
        "parent_geo_id": None,
        "valid_from": "2024-01-01",
        "valid_to": None,
        "successor_geo_id": None,
        "population": None,
        "area_km2": None,
    }
    monkeypatch.setattr(
        ex, "load_nuts2_rows", lambda: {"ZZ99": {"geo": fake_geo_dict, "belgian_geo_id": None}}
    )
    monkeypatch.setattr(
        ex,
        "_load_store_rows",
        lambda indicator_id: (
            [
                {
                    "geo_id": "zz99:nuts2",
                    "period": "2024",
                    "value": "42.0",
                    "status": "final",
                    "vintage": "2026-09-14T00:00:00+00:00",
                    "is_latest": "1",
                }
            ]
            if indicator_id == "GDP_PC_PPS_NUTS2"
            else None
        ),
    )

    from src.validation.config_schema import load_and_validate_all

    indicator_configs, source_configs = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )

    with pytest.raises(ex.ExportError, match="ZZ99"):
        ex._build_indicator_payload(
            "GDP_PC_PPS_NUTS2",
            indicator_configs["GDP_PC_PPS_NUTS2"],
            source_configs["eurostat"],
            ex.load_nuts2_rows(),
            geometry_ids_allowed=set(),
            excluded_by_licence=set(),
        )
