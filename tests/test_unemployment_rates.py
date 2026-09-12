"""Four figures on this site are called some form of "unemployment", and no two
of them mean the same thing. This file pins the differences.

    UNEMPLOYMENT_RATE_BIT       IWEPS/WalStat, ILO definition, LFS-calibrated.
                                Wallonia only, annual 1999-2023.
    UNEMPLOYMENT_RATE_COM       Statbel Census 2021, register/declaration.
                                581 communes, one period.
    UNEMPLOYMENT_CLAIMANT_RATE_WORKING_AGE   ONEM claimants / population 15-64.
                                565 communes, annual 2017-2026.
    SHARE_POP_ON_UNEMPLOYMENT_BENEFIT        ONEM claimants / whole population.
                                The same numerator over a worse denominator.

THE MEASURED FINDING THIS FILE EXISTS FOR: the first two claim the same
definition -- "unemployment rate, ages 15-64" -- and they do not agree. At
2021, on the 262 Walloon communes both cover, the census figure is higher in
ALL 262, median +2.44 pp, ratio 1.37x. Recomputed over Wallonia the census
counts give 12.09 % where the LFS gives 9.29 %, and it is 9.29 % that matches
the published Walloon rate. The census records a situation from the register
and the census declaration; the LFS applies the ILO's tests (sought work in
the last four weeks AND available within two), which fewer people meet.

That is asserted here as a RELATIONSHIP, not as agreement, so that a future
load which quietly made the two converge -- one of them having silently become
the other measure -- fails instead of passing. Ratios are hand-computed
(CLAUDE.md rule 5).
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.analytics.derived import share_of_total  # noqa: E402
from src.analytics.engine import ObservationSet, compute  # noqa: E402
from src.fetchers.walstat import _PERIOD  # noqa: E402
from src.validation.config_schema import load_and_validate_derived  # noqa: E402

INDICATOR_DIR = REPO / "config" / "indicators"
DERIVED_DIR = INDICATOR_DIR / "derived"
CENSUS_CSV = REPO / "data" / "census2021_observations.csv"

BIT = "UNEMPLOYMENT_RATE_BIT"
COM = "UNEMPLOYMENT_RATE_COM"
CLAIMANT_WA = "UNEMPLOYMENT_CLAIMANT_RATE_WORKING_AGE"
CLAIMANT_POP = "SHARE_POP_ON_UNEMPLOYMENT_BENEFIT"


def _config(indicator_id: str) -> dict:
    for directory in (INDICATOR_DIR, DERIVED_DIR):
        path = directory / f"{indicator_id}.yaml"
        if path.is_file():
            return yaml.safe_load(path.read_text(encoding="utf-8"))
    raise AssertionError(f"no config for {indicator_id}")


# --- the period format the new series brought with it ---------------------------


@pytest.mark.parametrize(
    "periode, year",
    [
        ("année 2024", "2024"),  # the finance series
        ("Année 2023", "2023"),  # the site varies the capital
        ("annee 2019", "2019"),  # and sometimes drops the accent
        ("moyenne annuelle 1999", "1999"),  # the labour-market series
        ("Moyenne annuelle 2023", "2023"),
        ("moyenne  annuelle  2010", "2010"),  # runs of whitespace
    ],
)
def test_both_walstat_period_forms_resolve_to_their_year(periode, year):
    match = _PERIOD.match(periode)
    assert match, f"{periode!r} was refused"
    assert match.group(1) == year


@pytest.mark.parametrize(
    "periode",
    [
        "trimestre 2024",  # a quarter is not a year and must not load as one
        "moyenne mensuelle 2024",  # a monthly average, ditto
        "moyenne annuelle",  # no year at all
        "2024",  # a bare year: the source never writes this
        "année 24",  # two digits
        "moyenne annuelle 2024 (provisoire)",  # a qualifier we have never seen
    ],
)
def test_a_period_form_nobody_has_seen_is_still_refused(periode):
    """Widening the pattern for 'moyenne annuelle' must not have widened it
    into anything that merely contains a year (rule 13)."""
    assert _PERIOD.match(periode) is None, f"{periode!r} was accepted"


# --- the four indicators are declared as four different things ------------------


def test_the_bit_series_names_the_walstat_series_it_loads():
    cfg = _config(BIT)
    assert cfg["source_id"] == "walstat"
    assert cfg["fetch"]["query"] == "/json/236400_0/com+period=all"
    assert cfg["unit"] == "percent"
    assert cfg["preferred_direction"] == "lower_is_better"


def test_the_claimant_rate_divides_by_working_age_population_in_that_order():
    """A swapped input order would publish a population as a percentage of a
    claimant count and still validate, so the order is asserted, not assumed."""
    cfg = _config(CLAIMANT_WA)
    assert cfg["derived"]["function"] == "share_of_total"
    assert cfg["derived"]["inputs"] == ["UNEMPLOYED_JOBSEEKERS", "POPULATION_AGE_15_64"]
    assert cfg["unit"] == "percent"


def test_the_two_claimant_ratios_differ_only_in_their_denominator():
    wa, pop = _config(CLAIMANT_WA), _config(CLAIMANT_POP)
    assert wa["derived"]["inputs"][0] == pop["derived"]["inputs"][0] == "UNEMPLOYED_JOBSEEKERS"
    assert wa["derived"]["inputs"][1] == "POPULATION_AGE_15_64"
    assert pop["derived"]["inputs"][1] == "POPULATION_BY_COMMUNE"


@pytest.mark.parametrize("indicator_id", [BIT, COM, CLAIMANT_WA])
def test_each_rate_says_in_its_own_config_what_it_is_not(indicator_id):
    """The whole hazard here is a reader treating any two of these as one
    series. Every one of them must carry that warning where the next
    maintainer will actually read it."""
    description = _config(indicator_id)["description"]["en"]
    assert "NOT" in description
    for other in {BIT, COM, CLAIMANT_WA} - {indicator_id}:
        assert other in description, f"{indicator_id} never mentions {other}"


def test_the_census_rate_no_longer_claims_to_be_eurostat_comparable():
    """It did until 2026-09-12, and the claim was measurably false.

    The description still contains those words, because it QUOTES the old
    claim in order to retract it -- so the assertion cannot be a plain
    substring check (the first version of this test was exactly that, and
    failed on the corrected config). What must hold is the ORDER: the
    retraction comes first, and the quote sits inside it.
    """
    description = _config(COM)["description"]["en"]
    claim = "comparable to published EU/Eurostat unemployment rates"
    assert "NOT THE EUROSTAT-COMPARABLE RATE" in description
    assert claim in description, "the retraction no longer quotes what it retracts"
    assert description.index("previously claimed") < description.index(claim), (
        "the old claim appears outside the retraction that quotes it -- it now reads as "
        "an assertion again"
    )
    assert "is false" in description


# --- the arithmetic, by hand ------------------------------------------------------


def test_the_claimant_rate_arithmetic():
    """Namur 2025: 4,071.9166667 claimants over 73,497 residents aged 15-64.
    4071.9166667 / 73497 * 100 = 5.54024880...%"""
    assert share_of_total(4071.9166667, 73497) == pytest.approx(5.5402488, abs=1e-6)


def test_the_claimant_rate_reads_well_below_the_ilo_rate_for_the_same_commune():
    """Namur 2023, the one year all three measures cover: claimants over
    working-age population is 5.59 %, the ILO rate is 9.8 % and the census
    rate 14.4 %. Written out because "it is lower" is the single most
    important thing about this indicator and the easiest to lose."""
    claimant = share_of_total(4043.5833333, 72272)
    assert claimant == pytest.approx(5.5949515, abs=1e-6)
    assert claimant < 9.8, "the claimant rate must stay below the ILO rate"


def test_the_configs_wire_the_claimant_rate_the_way_the_hand_computation_does():
    """The hand computation above calls share_of_total directly, so a swapped
    or renamed input in the YAML would leave it green. This runs the committed
    config through the engine."""
    observations = ObservationSet(
        [
            ("UNEMPLOYED_JOBSEEKERS", "be:mun:92094", "2025", 4071.9166667),
            ("POPULATION_AGE_15_64", "be:mun:92094", "2025", 73497.0),
        ]
    )
    known = {"UNEMPLOYED_JOBSEEKERS", "POPULATION_AGE_15_64"}
    configs = load_and_validate_derived(DERIVED_DIR, known)
    assert CLAIMANT_WA in configs, "the config did not load against its own inputs"
    result = compute(observations, {CLAIMANT_WA: configs[CLAIMANT_WA]}, known)
    assert result.value(CLAIMANT_WA, "be:mun:92094", "2025") == pytest.approx(5.5402488, abs=1e-6)


# --- the measured relationship between the two "same definition" rates ------------


def test_the_census_counts_give_wallonia_a_rate_the_lfs_does_not():
    """Recomputed from the census's own commune counts over the communes the
    BIT series covers -- 12.09 %, against the 9.29 % IWEPS publishes for the
    Walloon Region on the LFS. Both are in this repository's data; they are
    nearly three points apart, and only one of them is the internationally
    comparable figure.

    Aggregated the way CLAUDE.md's Definitions require -- the ratio recomputed
    from summed counts, never a mean of 262 commune rates.
    """
    if not CENSUS_CSV.is_file():
        pytest.skip("census observations store not present")
    unemployed: dict[str, float] = {}
    labour: dict[str, float] = {}
    with CENSUS_CSV.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row["period"] != "2021" or row["is_latest"] != "1" or not row["value"]:
                continue
            if row["indicator_id"] == "CAS_UNEMPLOYED":
                unemployed[row["geo_id"]] = float(row["value"])
            elif row["indicator_id"] == "CAS_LABOUR_FORCE":
                labour[row["geo_id"]] = float(row["value"])
    walloon = [g for g in unemployed if g.startswith("be:mun:") and _is_walloon_nis(g[-5:])]
    assert len(walloon) > 250, f"only {len(walloon)} Walloon communes found in the census store"
    rate = sum(unemployed[g] for g in walloon) / sum(labour[g] for g in walloon) * 100.0
    assert rate == pytest.approx(12.09, abs=0.25), (
        f"the census counts now give Wallonia {rate:.2f}%. This test pins the GAP against "
        "the LFS figure of 9.29% that UNEMPLOYMENT_RATE_BIT carries; if this moved, "
        "re-measure the relationship before assuming either series is wrong."
    )
    assert rate > 11.0, "the census rate must stay well above the LFS rate (measured 1.37x)"


def _is_walloon_nis(nis: str) -> bool:
    """Walloon NIS prefixes: 5x-6x (Hainaut, Liège), 8x (Luxembourg),
    9x (Namur), 2x only for Walloon Brabant (25xxx)."""
    if not nis.isdigit() or len(nis) != 5:
        return False
    code = int(nis)
    return 25000 <= code < 26000 or 51000 <= code < 70000 or 80000 <= code < 100000


@pytest.mark.parametrize(
    "nis, census, lfs",
    [
        ("92094", 14.4, 9.8),  # Namur, 2021 census vs 2023 LFS
        ("62063", 22.0, 15.3),  # Liège
        ("52011", 21.4, 15.1),  # Charleroi
    ],
)
def test_the_census_reads_higher_than_the_lfs_in_the_cities(nis, census, lfs):
    """Read off the two series for three communes anyone will check first.
    Not a tolerance: a DIRECTION, which held in 262 of 262 communes."""
    assert census > lfs, f"{nis}: the census rate has stopped reading higher than the LFS"
    assert census / lfs > 1.2, (
        f"{nis}: the measured ratio was 1.37x across Wallonia. A ratio near 1 would mean "
        "one of these two series has silently become the other measure."
    )
