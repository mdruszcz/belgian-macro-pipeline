"""Part-of-whole groups: declared in config, verified against the data.

The age donut on commune.html waited since Batch 4 for something published
to say WHICH indicators partition a whole. config/local_sections.yaml now
says so, and the exporter refuses to publish the claim unless the data bears
it out in every commune and period it can check -- a donut of shares that do
not add up is a picture of something that is not the case.

Expected values below are hand-built fixtures, not the script's own output.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts import export_site_payloads as payloads  # noqa: E402

_check_compositions = payloads._check_compositions
_check_sections = payloads._check_sections
_sections = payloads._sections

SECTIONS_JSON = REPO / "public" / "data" / "metadata" / "sections.json"


def commune(**series):
    """A payload-shaped commune: indicator -> {period -> value}."""
    return {
        "indicators": {
            code: {"periods": {period: {"value": value} for period, value in periods.items()}}
            for code, periods in series.items()
        }
    }


def layout(**overrides):
    group = {"id": "age", "whole": "TOTAL", "parts": ["A", "B", "C"]}
    group.update(overrides)
    return {"headlines": [], "sections": [], "compositions": [group]}


def test_a_true_partition_is_accepted():
    """10 + 20 + 30 = 60, in both periods of both communes."""
    communes = {
        "1": commune(
            TOTAL={"2024": 60, "2025": 60},
            A={"2024": 10, "2025": 10},
            B={"2024": 20, "2025": 20},
            C={"2024": 30, "2025": 30},
        ),
        "2": commune(TOTAL={"2024": 6}, A={"2024": 1}, B={"2024": 2}, C={"2024": 3}),
    }
    _check_compositions(layout(), communes)  # no exception


def test_a_false_partition_is_refused_and_says_where():
    """The whole is 61 where the parts sum to 60: one commune, one period,
    and that is enough to refuse the layout for all 565."""
    communes = {
        "1": commune(TOTAL={"2024": 60}, A={"2024": 10}, B={"2024": 20}, C={"2024": 30}),
        "2": commune(TOTAL={"2024": 61}, A={"2024": 10}, B={"2024": 20}, C={"2024": 30}),
    }
    with pytest.raises(ValueError, match="commune 2, period 2024"):
        _check_compositions(layout(), communes)


def test_a_partition_nothing_can_verify_is_refused():
    """No commune carries the whole and every part together, so the claim
    is unchecked -- and an unchecked claim is not published."""
    communes = {"1": commune(TOTAL={"2024": 60}, A={"2024": 10})}
    with pytest.raises(ValueError, match="could not be verified"):
        _check_compositions(layout(), communes)


def test_a_breakdown_with_no_whole_is_not_checked_for_a_total():
    """Employed / unemployed / inactive has no declared whole; there is nothing
    to add up to, so only existence is checked (by _check_sections)."""
    communes = {"1": commune(A={"2024": 1}, B={"2024": 2}, C={"2024": 3})}
    _check_compositions(layout(whole=None), communes)  # no exception


def test_a_composition_naming_an_unknown_indicator_is_caught_with_the_sections():
    with pytest.raises(ValueError, match="names indicator"):
        _check_sections(layout(parts=["A", "NOPE"]), known={"A", "B", "C", "TOTAL"})


def test_the_real_layout_declares_the_age_bands_and_the_activity_statuses():
    """What ships: two groups, the donut with a whole and the bars without."""
    groups = {g["id"]: g for g in _sections()["compositions"]}
    assert set(groups) == {"age_structure", "activity_status"}
    assert groups["age_structure"]["chart"] == "donut"
    assert groups["age_structure"]["whole"]
    assert len(groups["age_structure"]["parts"]) == 3
    assert groups["activity_status"]["chart"] == "bars"
    assert not groups["activity_status"].get("whole")
    for group in groups.values():
        for field in ("label", "note"):
            assert set(group[field]) == {
                "en",
                "fr",
                "nl",
            }, f"{group['id']}.{field} is not trilingual"


def test_the_published_sections_carry_the_compositions():
    """The page reads sections.json, not the YAML. If the exporter dropped the
    block on the floor the panel would silently stay 'unavailable'."""
    if not SECTIONS_JSON.is_file():
        pytest.skip("site payloads not built")
    published = json.loads(SECTIONS_JSON.read_text(encoding="utf-8"))
    assert [g["id"] for g in published["compositions"]] == ["age_structure", "activity_status"]
