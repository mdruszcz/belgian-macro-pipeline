"""Tests for the derived-indicator engine (Block G).

The two properties that carry the risk:

  - dependency order is explicit and cycles raise, naming the cycle;
  - the peer set is scoped PER PERIOD, so a percentile ranks against the
    communes that existed that year rather than today's 565.

Plus CONTROL G: no derived indicator may appear in `observations`.
"""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from src.analytics.engine import (  # noqa: E402
    CircularDependencyError,
    ObservationSet,
    UnknownFunctionError,
    UnknownInputError,
    compute,
    resolve_order,
)
from src.validation.config_schema import (  # noqa: E402
    ConfigValidationError,
    load_and_validate_derived,
)

REPO = Path(__file__).resolve().parents[1]
DERIVED_DIR = REPO / "config" / "indicators" / "derived"
POPULATION_STORE = REPO / "data" / "population_observations.csv"
# Read from config rather than hardcoded: this list had to be edited by hand
# every time a source indicator was added, which is how a fixture meant to
# describe reality drifts away from it.
SOURCE_IDS = {
    yaml.safe_load(p.read_text(encoding="utf-8"))["id"]
    for p in sorted((REPO / "config" / "indicators").glob("*.yaml"))
}


def _cfg(ind_id: str, function: str, inputs: list[str], **args) -> dict:
    return {"id": ind_id, "derived": {"function": function, "inputs": inputs, "args": args}}


# ── Dependency resolution ───────────────────────────────────────────────────


def test_inputs_are_computed_before_the_things_that_need_them():
    configs = {
        "B": _cfg("B", "five_year_change", ["A"]),
        "A": _cfg("A", "five_year_change", ["RAW"]),
    }
    assert resolve_order(configs, {"RAW"}) == ["A", "B"]


def test_order_is_deterministic():
    """An unstable order would make export diffs noisy for no reason."""
    configs = {
        "Z": _cfg("Z", "five_year_change", ["RAW"]),
        "A": _cfg("A", "five_year_change", ["RAW"]),
        "M": _cfg("M", "five_year_change", ["RAW"]),
    }
    assert resolve_order(configs, {"RAW"}) == ["A", "M", "Z"]


def test_a_cycle_raises_and_names_the_cycle():
    configs = {
        "A": _cfg("A", "five_year_change", ["B"]),
        "B": _cfg("B", "five_year_change", ["A"]),
    }
    with pytest.raises(CircularDependencyError) as exc:
        resolve_order(configs)
    message = str(exc.value)
    assert "A" in message and "B" in message and "->" in message


def test_a_three_node_cycle_raises():
    configs = {
        "A": _cfg("A", "five_year_change", ["C"]),
        "B": _cfg("B", "five_year_change", ["A"]),
        "C": _cfg("C", "five_year_change", ["B"]),
    }
    with pytest.raises(CircularDependencyError):
        resolve_order(configs)


def test_an_input_nothing_provides_raises():
    """Otherwise the column is null for every row -- quiet emptiness that
    survives review."""
    configs = {"A": _cfg("A", "five_year_change", ["NOT_A_THING"])}
    with pytest.raises(UnknownInputError, match="NOT_A_THING"):
        resolve_order(configs, {"RAW"})


def test_an_unknown_function_raises():
    obs = ObservationSet([("RAW", "be:mun:11002", "2026", 1.0)])
    configs = {"A": _cfg("A", "no_such_function", ["RAW"])}
    with pytest.raises(UnknownFunctionError, match="no_such_function"):
        compute(obs, configs, {"RAW"})


# ── The peer set is per period ──────────────────────────────────────────────


def test_peers_are_scoped_to_the_period():
    """Three communes exist in 2016; one is merged away by 2026. The 2016
    peer set must be 3 and the 2026 set 2 -- ranking a 2016 value against the
    2026 set would use the wrong denominator."""
    obs = ObservationSet(
        [
            ("POP", "be:mun:A", "2016", 100.0),
            ("POP", "be:mun:B", "2016", 200.0),
            ("POP", "be:mun:GONE", "2016", 300.0),
            ("POP", "be:mun:A", "2026", 150.0),
            ("POP", "be:mun:B", "2026", 250.0),
        ]
    )
    assert len(obs.peers("POP", "2016")) == 3
    assert len(obs.peers("POP", "2026")) == 2


def test_percentile_uses_the_period_specific_peer_set():
    """Same commune, same value, different years -> different rank, because
    the peer set differs. By hand:
      2016, peers [100, 200, 300], subject 200: below=1, equal=1, N=3
            -> 100 * 1.5 / 3 = 50.0
      2026, peers [150, 250],      subject 250: below=1, equal=1, N=2
            -> 100 * 1.5 / 2 = 75.0
    """
    obs = ObservationSet(
        [
            ("POP", "be:mun:A", "2016", 100.0),
            ("POP", "be:mun:B", "2016", 200.0),
            ("POP", "be:mun:GONE", "2016", 300.0),
            ("POP", "be:mun:A", "2026", 150.0),
            ("POP", "be:mun:B", "2026", 250.0),
        ]
    )
    configs = {"RANK": _cfg("RANK", "percentile", ["POP"])}
    out = compute(obs, configs, {"POP"})
    assert out.value("RANK", "be:mun:B", "2016") == 50.0
    assert out.value("RANK", "be:mun:B", "2026") == 75.0


def test_the_real_store_really_does_change_peer_size_by_period():
    """Guards the spec's central claim against the committed data, so a future
    reload that silently drops historical communes fails here."""
    rows = [
        (r["indicator_id"], r["geo_id"], r["period"], float(r["value"]))
        for r in csv.DictReader(POPULATION_STORE.open(encoding="utf-8"))
        if r["is_latest"] == "1" and r["indicator_id"] == "POPULATION_BY_COMMUNE"
    ]
    obs = ObservationSet(rows)
    assert len(obs.peers("POPULATION_BY_COMMUNE", "2016")) == 589
    assert len(obs.peers("POPULATION_BY_COMMUNE", "2019")) == 581
    assert len(obs.peers("POPULATION_BY_COMMUNE", "2026")) == 565


# ── Multi-input and time-series shapes ──────────────────────────────────────


def test_dependency_ratio_from_three_bands():
    """(3000 + 2000) / 10000 * 100 = 50.0 exactly."""
    obs = ObservationSet(
        [
            ("YOUNG", "be:mun:A", "2026", 3000.0),
            ("WORK", "be:mun:A", "2026", 10000.0),
            ("OLD", "be:mun:A", "2026", 2000.0),
        ]
    )
    configs = {"DR": _cfg("DR", "dependency_ratio", ["YOUNG", "WORK", "OLD"])}
    out = compute(obs, configs, {"YOUNG", "WORK", "OLD"})
    assert out.value("DR", "be:mun:A", "2026") == 50.0


def test_multi_input_does_not_cross_wire_two_communes():
    """The [REVIEW] step this guards: 'verify per-capita denominators'. A
    MULTI_INPUT function (dependency_ratio here, but the risk is identical
    for per_capita) fetches each input at the SAME geo_id -- proven by
    putting two communes with DIFFERENT band values in one ObservationSet
    and asserting each commune's own ratio, not the other's, comes back.
    A denominator silently borrowed from a neighbouring commune would still
    produce a plausible-looking number, which is exactly the failure mode
    that is invisible without a test like this one."""
    obs = ObservationSet(
        [
            ("YOUNG", "be:mun:A", "2026", 1000.0),
            ("WORK", "be:mun:A", "2026", 4000.0),
            ("OLD", "be:mun:A", "2026", 1000.0),
            ("YOUNG", "be:mun:B", "2026", 9000.0),
            ("WORK", "be:mun:B", "2026", 1000.0),
            ("OLD", "be:mun:B", "2026", 9000.0),
        ]
    )
    configs = {"DR": _cfg("DR", "dependency_ratio", ["YOUNG", "WORK", "OLD"])}
    out = compute(obs, configs, {"YOUNG", "WORK", "OLD"})
    # A: (1000+1000)/4000*100 = 50.0. B: (9000+9000)/1000*100 = 1800.0.
    # If B's WORK (a tiny 1000) leaked into A's denominator, A would come
    # back as 1800.0 too -- the two must differ, and by the right amount.
    assert out.value("DR", "be:mun:A", "2026") == 50.0
    assert out.value("DR", "be:mun:B", "2026") == 1800.0


def test_a_single_period_indicator_derives_to_null_not_zero():
    """LOCAL_UNITS_BY_COMMUNE has exactly one period, so a five-year change
    over it is undefined. Zero would read as 'no growth', which is a claim the
    data does not support."""
    obs = ObservationSet([("LU", "be:mun:A", "2023-Q4", 66381.0)])
    configs = {"LU_5Y": _cfg("LU_5Y", "five_year_change", ["LU"])}
    out = compute(obs, configs, {"LU"})
    assert out.value("LU_5Y", "be:mun:A", "2023-Q4") is None


def test_compute_does_not_mutate_the_input_set():
    obs = ObservationSet([("POP", "be:mun:A", "2026", 100.0)])
    configs = {"RANK": _cfg("RANK", "percentile", ["POP"])}
    compute(obs, configs, {"POP"})
    assert obs.value("RANK", "be:mun:A", "2026") is None


# ── The committed configs ───────────────────────────────────────────────────


def test_the_committed_derived_configs_load_and_order():
    derived = load_and_validate_derived(DERIVED_DIR, SOURCE_IDS)
    assert set(derived) == {
        "AVG_NET_TAXABLE_INCOME",
        "DEPENDENCY_RATIO",
        "POPULATION_CAGR_10Y",
        "POPULATION_CHANGE_5Y",
        "POPULATION_PERCENTILE",
        # Census 2021 shares -- counts are stored, the ratios a reader wants
        # are computed from them (CLAUDE.md rule 6).
        "AVERAGE_HOUSEHOLD_SIZE",
        "SHARE_BORN_ABROAD",
        "SHARE_DWELLINGS_UNOCCUPIED",
        "SHARE_FOREIGN_NATIONALS",
        "SHARE_SINGLE_PARENT_FAMILIES",
        "SHARE_SINGLE_PERSON_HOUSEHOLDS",
        # Block O: WalStat municipal finance, per-inhabitant series in, four
        # ratios out (the other three the roadmap names need series the
        # source does not publish -- docs/features/walstat_adapter.md).
        "MUN_DEBT_TO_REVENUE",
        "MUN_EXPENDITURE_GROWTH_1Y",
        "MUN_INVESTMENT_SHARE_OF_EXPENDITURE",
        "MUN_REVENUE_GROWTH_1Y",
        "UNEMPLOYMENT_RATE_COM",
        # AVG_HOUSE_PRICE retired: Statbel's newer real-estate file gives only
        # a median, which cannot be derived from stored components the way a
        # mean could -- MEDIAN_HOUSE_PRICE is stored directly instead (see
        # scripts/sync_realestate.py), not a derived indicator at all.
        # ONEM/RVA. Claimants over total population, NOT over the labour
        # force -- deliberately not called an unemployment rate; see the
        # config's own description.
        "SHARE_POP_ON_UNEMPLOYMENT_BENEFIT",
    }
    assert resolve_order(derived, SOURCE_IDS)


def test_a_derived_config_naming_an_unprovided_input_fails_validation(tmp_path):
    """The cross-file check JSON Schema cannot express."""
    (tmp_path / "BAD.yaml").write_text(
        yaml.dump(
            {
                "id": "BAD",
                "name": {"en": "x", "fr": "x", "nl": "x"},
                "unit": "percent",
                "frequency": "A",
                "geo_levels": ["municipal"],
                "preferred_direction": "contextual",
                "derived": {"function": "five_year_change", "inputs": ["NOPE"]},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="NOPE"):
        load_and_validate_derived(tmp_path, SOURCE_IDS)


# ── CONTROL G ───────────────────────────────────────────────────────────────


def test_control_g_no_derived_indicator_is_stored_as_an_observation():
    """Once a derived value is persisted as source data, a formula fix stops
    propagating and the database holds two contradicting truths."""
    derived_ids = set(load_and_validate_derived(DERIVED_DIR, SOURCE_IDS))

    conn = sqlite3.connect(str(REPO / "data" / "belgian_macro.db"))
    stored = {r[0] for r in conn.execute("SELECT DISTINCT indicator_id FROM observations")}
    conn.close()
    assert derived_ids.isdisjoint(
        stored
    ), f"derived indicators found in observations: {sorted(derived_ids & stored)}"

    in_store = {r["indicator_id"] for r in csv.DictReader(POPULATION_STORE.open(encoding="utf-8"))}
    assert derived_ids.isdisjoint(in_store), (
        f"derived indicators found in the committed CSV store: " f"{sorted(derived_ids & in_store)}"
    )
