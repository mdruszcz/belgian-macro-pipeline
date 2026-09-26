"""Peer model stability -- a period-shift stand-in, docs/features/peer_model.md
"Stability -- a stand-in until a second data vintage exists".

Every store behind this model currently holds one vintage only, so the model
cannot yet be tested by re-running it against a corrected LATER vintage of
the same data (the real test). The stand-in used here: recompute the model
with each variable's period shifted one period earlier WHERE an earlier
period exists for that variable in the committed data; a variable with no
earlier period (average_household_size and share_foreign_nationals -- 2021
census, one vintage; enterprise_density's own LOCAL_UNITS_BY_COMMUNE --
frozen at 2023-Q4, Statbel has not republished it) keeps its period
unchanged in the shifted run, exactly as the spec directs.

Reports the mean Jaccard overlap of the national top-10 peer sets between
the two runs, across all 565 communes, and the ten communes with the lowest
overlap by name. A test (tests/test_peer_stability.py) runs this against the
real committed data and asserts the spec's proposed threshold, mean overlap
>= 0.5. If the real number falls below that, this script does NOT lower the
threshold -- it prints the number and the least-stable communes and exits
non-zero, a finding for the maintainer, not something to paper over
(CLAUDE.md rule 13 in spirit: a model whose own stability check fails must
say so loudly).

THIS IS NOT A REAL STABILITY TEST -- docs/features/peer_model.md says so
plainly and this script's docstring repeats it deliberately: shifting a
period is not the same as re-measuring with corrected data, and this number
should never be reported to a client as a real robustness check.

Usage:  python scripts/check_peer_stability.py
            [--history-dir data/communes_history]
            [--history-csv data/communes_history.csv]
            [--areas config/geography/commune_area_km2.csv]
            [--geographies config/geography/geographies.csv]
            [--threshold 0.5]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scripts.export_peer_model as export_peer_model  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]

#: One period earlier for every variable whose store has one, per the
#: buildable variables' actual coverage (checked against the committed data
#: 2026-09-26): population, the two age shares, unemployment and property
#: tax base per resident go from 2026 to 2025 (POPULATION_BY_COMMUNE,
#: POPULATION_AGE_65_PLUS/0_14, UNEMPLOYMENT_RATE_INSURED and
#: MUN_CADASTRAL_INCOME_TOTAL all carry 2017-2026); population_change_5y
#: goes from 2026 to 2025 (POPULATION_CHANGE_5Y carries 2021-2026);
#: avg_net_taxable_income goes from 2023 to 2022 (AVG_NET_TAXABLE_INCOME
#: carries 2017-2023). population_density's own area file has no earlier
#: vintage (the geometry the area is derived from is a single dated
#: snapshot), so only its population numerator shifts.
#: enterprise_density's LOCAL_UNITS_BY_COMMUNE is frozen at 2023-Q4 (no
#: earlier period exists in the store at all), so only its population
#: denominator shifts, from 2023 to 2022 (checked: present).
#: share_foreign_nationals and average_household_size (2021 census) have no
#: earlier period anywhere in this pipeline and keep 2021 unchanged.
SHIFTED_PERIODS: dict[str, str] = {
    "POPULATION_BY_COMMUNE:2026": "2025",
    "POPULATION_AGE_65_PLUS:2026": "2025",
    "POPULATION_AGE_0_14:2026": "2025",
    "UNEMPLOYMENT_RATE_INSURED:2026": "2025",
    "MUN_CADASTRAL_INCOME_TOTAL:2026": "2025",
    "POPULATION_CHANGE_5Y:2026": "2025",
    "AVG_NET_TAXABLE_INCOME:2023": "2022",
    # enterprise_density's population denominator (build_raw_values reads
    # POPULATION_BY_COMMUNE at "2023" specifically for this ratio):
    "POPULATION_BY_COMMUNE:2023": "2022",
}

PROPOSED_THRESHOLD = 0.5


def _jaccard(a: list[str], b: list[str]) -> float:
    sa, sb = set(a), set(b)
    union = sa | sb
    if not union:
        return 1.0
    return len(sa & sb) / len(union)


def _shifted_series(index, indicator: str, period: str, communes: set[str]):
    shift_key = f"{indicator}:{period}"
    shifted_period = SHIFTED_PERIODS.get(shift_key, period)
    return export_peer_model._series(index, indicator, shifted_period, communes)


def build_shifted_raw_values(
    history_dir: Path, history_csv: Path, areas_path: Path, communes: set[str]
):
    """Same shape as export_peer_model.build_raw_values, but reading each
    variable's SHIFTED period where SHIFTED_PERIODS names one, else its
    normal period (variables with no earlier vintage: unchanged)."""
    rows = export_peer_model._read_all_history_rows(history_dir, history_csv)
    index = export_peer_model._index_by_indicator_period(rows)

    population_2026 = _shifted_series(index, "POPULATION_BY_COMMUNE", "2026", communes)
    age_65_2026 = _shifted_series(index, "POPULATION_AGE_65_PLUS", "2026", communes)
    age_0_14_2026 = _shifted_series(index, "POPULATION_AGE_0_14", "2026", communes)
    unemployment_2026 = _shifted_series(index, "UNEMPLOYMENT_RATE_INSURED", "2026", communes)
    cadastral_income_2026 = _shifted_series(index, "MUN_CADASTRAL_INCOME_TOTAL", "2026", communes)
    population_change_5y = _shifted_series(index, "POPULATION_CHANGE_5Y", "2026", communes)
    avg_income_2023 = _shifted_series(index, "AVG_NET_TAXABLE_INCOME", "2023", communes)
    # No earlier vintage available anywhere in the pipeline -- kept unchanged.
    share_foreign_2021 = export_peer_model._series(
        index, "SHARE_FOREIGN_NATIONALS", "2021", communes
    )
    household_size_2021 = export_peer_model._series(
        index, "AVERAGE_HOUSEHOLD_SIZE", "2021", communes
    )
    # Frozen source, no earlier period -- kept unchanged.
    local_units_2023q4 = export_peer_model._series(
        index, "LOCAL_UNITS_BY_COMMUNE", "2023-Q4", communes
    )
    # Enterprise density's population denominator DOES have an earlier period.
    population_2023 = _shifted_series(index, "POPULATION_BY_COMMUNE", "2023", communes)

    # Population density's area has no earlier vintage (a single dated
    # geometry snapshot); only the population numerator shifts.
    areas = export_peer_model._read_areas(areas_path, communes)

    values = export_peer_model.pd.DataFrame(index=sorted(communes))
    values["population"] = population_2026
    values["population_density"] = population_2026 / areas
    values["share_65_plus"] = (age_65_2026 / population_2026) * 100.0
    values["share_0_14"] = (age_0_14_2026 / population_2026) * 100.0
    values["population_change_5y"] = population_change_5y
    values["avg_net_taxable_income"] = avg_income_2023
    values["unemployment_rate_insured"] = unemployment_2026
    values["share_foreign_nationals"] = share_foreign_2021
    values["average_household_size"] = household_size_2021
    values["enterprise_density"] = (local_units_2023q4 / population_2023) * 1000.0
    values["property_tax_base_per_resident"] = cadastral_income_2026 / population_2026

    return values


def run_stability_check(
    history_dir: Path,
    history_csv: Path,
    areas_path: Path,
    geographies_path: Path,
) -> tuple[float, list[tuple[str, float]]]:
    """Returns (mean_overlap, ten_least_stable) where ten_least_stable is a
    list of (nis, overlap) sorted ascending by overlap."""
    communes = export_peer_model._current_municipality_nis(geographies_path)
    rows = export_peer_model._read_all_history_rows(history_dir, history_csv)
    meta = export_peer_model._commune_metadata(rows, communes)

    baseline_values = export_peer_model.build_raw_values(
        history_dir, history_csv, areas_path, communes
    )
    shifted_values = build_shifted_raw_values(history_dir, history_csv, areas_path, communes)

    baseline_model = export_peer_model.build_model(baseline_values, meta, variant="standardised")
    shifted_model = export_peer_model.build_model(shifted_values, meta, variant="standardised")

    overlaps: list[tuple[str, float]] = []
    for nis in baseline_model["communes"]:
        baseline_top10 = [e["nis"] for e in baseline_model["communes"][nis]["national"]]
        shifted_top10 = [e["nis"] for e in shifted_model["communes"][nis]["national"]]
        overlaps.append((nis, _jaccard(baseline_top10, shifted_top10)))

    mean_overlap = sum(o for _nis, o in overlaps) / len(overlaps)
    least_stable = sorted(overlaps, key=lambda pair: (pair[1], pair[0]))[:10]
    return mean_overlap, least_stable


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-dir", type=Path, default=export_peer_model.DEFAULT_HISTORY_DIR)
    parser.add_argument("--history-csv", type=Path, default=export_peer_model.DEFAULT_HISTORY_CSV)
    parser.add_argument("--areas", type=Path, default=export_peer_model.DEFAULT_AREAS)
    parser.add_argument("--geographies", type=Path, default=export_peer_model.DEFAULT_GEOGRAPHIES)
    parser.add_argument("--threshold", type=float, default=PROPOSED_THRESHOLD)
    args = parser.parse_args()

    mean_overlap, least_stable = run_stability_check(
        args.history_dir, args.history_csv, args.areas, args.geographies
    )

    print(f"mean national top-10 Jaccard overlap (one-period shift): {mean_overlap:.4f}")
    print("ten least stable communes (nis, overlap):")
    for nis, overlap in least_stable:
        print(f"  {nis}: {overlap:.4f}")

    if mean_overlap < args.threshold:
        print(
            f"BELOW proposed threshold {args.threshold} -- this is a finding for the "
            "maintainer, not something this script should paper over."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
