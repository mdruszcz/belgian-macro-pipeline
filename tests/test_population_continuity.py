"""Tests for the CONTROL C population-continuity check.

The script itself is throwaway, but its *verdict* is the only durable evidence
CONTROL C will ever produce -- "I looked at a chart and it seemed fine" is not
auditable. So the thing under test is that it actually detects a broken
crosswalk, and that it refuses to run rather than reporting a false PASS when
the data is missing.

No network, no real population files (data/raw is gitignored): these build a
tiny synthetic Statbel-shaped file set against the REAL committed crosswalk.
"""

import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import plot_population_continuity as P  # noqa: E402


def _write_population(out_dir: Path, groups: dict, chosen: list[str], drop_from: str | None):
    """Statbel-shaped: pipe-delimited, one commune split across several rows.

    drop_from -- if set, that group's successor total silently loses its first
    predecessor's people from the merger year on: exactly the crosswalk bug
    this control exists to catch.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    base = {p: 5000 + 100 * i for g in chosen for i, p in enumerate(groups[g]["predecessors"])}
    for year in range(2010, 2026):
        rows = []
        for g in chosen:
            preds = groups[g]["predecessors"]
            growth = 1 + 0.004 * (year - 2010)
            if year < groups[g]["merger_year"]:
                rows += [(p, int(base[p] * growth)) for p in preds]
            else:
                total = sum(int(base[p] * growth) for p in preds)
                if g == drop_from:
                    total -= int(base[preds[0]] * growth)
                rows.append((g, total))
        with (out_dir / f"pop_{year}.csv").open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh, delimiter="|")
            writer.writerow(["CD_REFNIS", "MS_POPULATION"])
            for nis, pop in rows:
                writer.writerow([nis, pop // 2])
                writer.writerow([nis, pop - pop // 2])


@pytest.fixture
def groups():
    return P.read_merger_groups(P.CROSSWALK)


def test_missing_data_raises_rather_than_reporting_a_false_pass(tmp_path):
    """The dangerous failure is a green result from no data at all."""
    with pytest.raises(P.MissingPopulationData, match="No population data"):
        P.read_population_by_year(tmp_path / "does-not-exist")


def test_empty_directory_names_what_it_found(tmp_path):
    (tmp_path / "pop").mkdir()
    (tmp_path / "pop" / "notes.xlsx").write_text("x")
    with pytest.raises(P.MissingPopulationData, match="no .csv/.txt"):
        P.read_population_by_year(tmp_path / "pop")


def test_unrecognised_columns_raise_with_the_real_header(tmp_path):
    pop = tmp_path / "pop"
    pop.mkdir()
    (pop / "pop_2020.csv").write_text("wrong|columns\n1|2\n", encoding="utf-8")
    with pytest.raises(P.MissingPopulationData, match="could not find a nis column"):
        P.read_population_by_year(pop)


def test_correct_crosswalk_passes(tmp_path, groups):
    chosen = P.select_groups(groups, 5)
    _write_population(tmp_path / "pop", groups, chosen, drop_from=None)
    assert P.run(tmp_path / "pop", tmp_path / "out", 5) == 0


def test_broken_crosswalk_is_detected(tmp_path, groups):
    """Drop one predecessor's population from Tongeren-Borgloon and the step
    change must be caught -- this is the exact bug the NIS6 prefix rule missed
    in Block C, and the reason this group is force-included."""
    chosen = P.select_groups(groups, 5)
    _write_population(tmp_path / "pop", groups, chosen, drop_from="73111")
    assert P.run(tmp_path / "pop", tmp_path / "out", 5) == 1


def test_tongeren_borgloon_is_always_checked(groups):
    """73111 must appear even when it does not rank in the top five, because
    its lineage was name-matched rather than derived -- see the crosswalk's
    own `note` column."""
    assert "73111" in P.select_groups(groups, 1)


def test_partial_transfers_are_excluded(groups):
    """A boundary transfer cannot be stitched by summing whole communes, so
    including one would manufacture a step this control would misattribute."""
    with P.CROSSWALK.open(encoding="utf-8", newline="") as fh:
        partial_rows = [
            r
            for r in csv.DictReader(fh)
            if (r.get("has_partial_transfer") or "").strip().lower() == "true"
        ]
    for row in partial_rows:
        predecessors = groups.get(row["new_nis"], {}).get("predecessors", [])
        assert row["old_nis"] not in predecessors, (
            f"{row['old_nis']} is a partial transfer into {row['new_nis']} but was "
            f"included as a whole-commune predecessor"
        )


def test_absent_commune_is_not_treated_as_zero():
    """A commune missing from a year's file means 'not published', not
    'nobody lives there' -- summing it as 0 would fake a discontinuity."""
    by_year = {2020: {"11001": 100}, 2021: {}}
    series = P.build_series(by_year, ["11001"])
    assert series[2020] == 100
    assert series[2021] is None


def test_step_threshold_scales_with_the_commune_s_own_volatility():
    """A fast-growing commune must not be flagged merely for growing."""
    volatile = {y: int(1000 * (1.08 ** (y - 2010))) for y in range(2010, 2026)}
    assert P.typical_yoy_pct(volatile, 2025) == pytest.approx(8.0, abs=0.5)
