"""config/live_counters.yaml: schema + cross-field validation
(src/validation/live_counters_config.py)."""

import copy
from pathlib import Path

import pytest
import yaml

from src.analytics.live_counters import LiveCounterError
from src.validation.live_counters_config import load_and_validate_live_counters

REPO = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO / "config" / "live_counters.yaml"


@pytest.fixture
def base():
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))


def _write(tmp_path, data):
    path = tmp_path / "live_counters.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def test_the_real_config_validates():
    data = load_and_validate_live_counters(CONFIG_PATH)
    ids = [c["id"] for c in data["counters"]]
    assert ids == ["population", "revenue", "spending", "deficit", "debt"]
    assert data["method"] == {
        "trend_window_years": 3,
        "horizon_years": 2,
        "remainder_tolerance_meur": 0.5,
    }


def test_every_placement_names_only_real_counter_ids():
    data = load_and_validate_live_counters(CONFIG_PATH)
    ids = {c["id"] for c in data["counters"]}
    for members in data["placements"].values():
        assert set(members) <= ids


def test_every_named_part_has_an_indicator_and_no_label_of_its_own():
    # Named parts take their label from the indicator config (rule 2/24
    # extended) -- this config must never hand-type one.
    data = load_and_validate_live_counters(CONFIG_PATH)
    for counter in data["counters"]:
        breakdown = counter.get("breakdown")
        if not breakdown:
            continue
        for part in breakdown["parts"]:
            if "indicator" in part:
                assert "label" not in part, part["id"]
            else:
                assert "label" in part, part["id"]


def test_unknown_placement_reference_is_refused(tmp_path, base):
    base["placements"]["bogus"] = ["not_a_real_id"]
    with pytest.raises(LiveCounterError, match="unknown counter"):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_duplicate_counter_id_is_refused(tmp_path, base):
    base["counters"].append(copy.deepcopy(base["counters"][0]))
    with pytest.raises(LiveCounterError, match="duplicate counter id"):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_duplicate_part_id_is_refused(tmp_path, base):
    revenue = next(c for c in base["counters"] if c["id"] == "revenue")
    revenue["breakdown"]["parts"].append(copy.deepcopy(revenue["breakdown"]["parts"][0]))
    with pytest.raises(LiveCounterError, match="duplicate part id"):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_a_difference_counters_minuend_must_be_a_real_flow_counter(tmp_path, base):
    deficit = next(c for c in base["counters"] if c["id"] == "deficit")
    deficit["minuend"] = "not_a_real_id"
    with pytest.raises(LiveCounterError, match="minuend"):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_a_difference_counters_minuend_must_be_kind_flow_not_stock(tmp_path, base):
    deficit = next(c for c in base["counters"] if c["id"] == "deficit")
    deficit["minuend"] = "population"  # a real id, but kind stock, not flow
    with pytest.raises(LiveCounterError, match="not a flow counter"):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_a_stocks_paced_by_must_be_a_real_difference_counter(tmp_path, base):
    debt = next(c for c in base["counters"] if c["id"] == "debt")
    debt["paced_by"] = "revenue"  # real id, but kind flow, not difference
    with pytest.raises(LiveCounterError, match="not a difference counter"):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_a_covers_entry_must_name_a_real_sibling_part(tmp_path, base):
    revenue = next(c for c in base["counters"] if c["id"] == "revenue")
    other_taxes = next(p for p in revenue["breakdown"]["parts"] if p["id"] == "other_taxes")
    other_taxes["covers"] = ["not_a_real_sibling"]
    with pytest.raises(LiveCounterError, match="unknown sibling part"):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_a_remainder_part_with_no_label_is_refused_by_the_schema(tmp_path, base):
    revenue = next(c for c in base["counters"] if c["id"] == "revenue")
    non_tax = next(p for p in revenue["breakdown"]["parts"] if p["id"] == "non_tax_revenue")
    del non_tax["label"]
    with pytest.raises(LiveCounterError):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_a_label_missing_a_language_is_refused(tmp_path, base):
    base["counters"][0]["label"].pop("nl")
    with pytest.raises(LiveCounterError):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_missing_schema_version_is_refused(tmp_path, base):
    del base["schema_version"]
    with pytest.raises(LiveCounterError):
        load_and_validate_live_counters(_write(tmp_path, base))


def test_an_unknown_top_level_key_is_refused(tmp_path, base):
    base["something_extra"] = True
    with pytest.raises(LiveCounterError):
        load_and_validate_live_counters(_write(tmp_path, base))
