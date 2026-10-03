"""scripts/export_live_counters.py -- fast tier, fixtures only, no network.

tests/fixtures/live_counters/{national,aggregates,metadata_indicators}.json
are trimmed slices of this PR's own REAL 2026-10-03 published payloads
(public/data/national.json, aggregates.json, metadata/indicators.json) --
every value is real, loaded data, never invented (rule 36/39).
config/live_counters.yaml is the real, committed config.
"""

import copy
import json
from decimal import ROUND_HALF_UP, Decimal, localcontext
from pathlib import Path

import pytest
import yaml

from orchestration.definitions import build_defs
from scripts.export_live_counters import DEFAULT_CONFIG, build_payload, export_live_counters
from src.analytics.live_counters import LiveCounterError

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "live_counters"


@pytest.fixture
def national():
    return json.loads((FIXTURES / "national.json").read_text(encoding="utf-8"))


@pytest.fixture
def aggregates():
    return json.loads((FIXTURES / "aggregates.json").read_text(encoding="utf-8"))


@pytest.fixture
def metadata():
    return json.loads((FIXTURES / "metadata_indicators.json").read_text(encoding="utf-8"))


@pytest.fixture
def config():
    from src.validation.live_counters_config import load_and_validate_live_counters

    return load_and_validate_live_counters(DEFAULT_CONFIG)


# ── end-to-end against the real fixtures (a regression pin) ────────────────


def test_build_payload_real_fixtures_headline_numbers(national, aggregates, metadata, config):
    payload = build_payload(national, aggregates, metadata, config)
    assert payload["schema_version"] == 1
    assert payload["simulated"] is True
    by_id = {c["id"]: c for c in payload["counters"]}

    assert all(c["state"] == "available" for c in by_id.values())

    # Revenue/spending 2026 pace: TR_2026 = 314736.4 * 1.0461895754,
    # TE_2026 = 347956.3 * 1.0568799130 (hand-computed in
    # tests/test_live_counters.py; reproduced here end-to-end).
    assert by_id["revenue"]["annual_meur"]["2026"] == 329273.9
    assert by_id["spending"]["annual_meur"]["2026"] == 367748.0

    # 2026 deficit = spending - revenue for that year, in EUR.
    deficit_2026 = by_id["deficit"]["segments"][0]["v1"]
    assert deficit_2026 == pytest.approx(38_474_083_392.88, abs=0.01)

    # Debt anchors on GOV_DEBT_Q_MEUR_BE 2026-Q1 (706,581.0 meur), later
    # than GOV_DEBT_MEUR_BE's own 2025 annual anchor.
    debt = by_id["debt"]
    assert debt["basis"][0]["indicator"] == "GOV_DEBT_Q_MEUR_BE"
    assert debt["segments"][0]["v0"] == 706_581_000_000.0

    # Population: pop_2026 - pop_2025 = 42,083/year.
    pop = by_id["population"]
    assert pop["segments"][0]["v0"] == 11_867_634
    assert pop["segments"][0]["v1"] == 11_909_717


def test_revenue_breakdown_parts_sum_to_the_counters_own_total(
    national, aggregates, metadata, config
):
    payload = build_payload(national, aggregates, metadata, config)
    revenue = next(c for c in payload["counters"] if c["id"] == "revenue")
    breakdown = revenue["breakdown"]
    assert breakdown["year"] == "2025"
    total_2026 = Decimal(str(revenue["segments"][0]["v1"]))
    parts_sum = sum(Decimal(str(p["segments"][0]["v1"])) for p in breakdown["parts"])
    assert parts_sum == total_2026
    assert all(p["segments"][0]["v1"] >= 0 for p in breakdown["parts"])


def test_spending_breakdown_parts_sum_to_the_counters_own_total(
    national, aggregates, metadata, config
):
    payload = build_payload(national, aggregates, metadata, config)
    spending = next(c for c in payload["counters"] if c["id"] == "spending")
    breakdown = spending["breakdown"]
    assert breakdown["year"] == "2024"
    total_2026 = Decimal(str(spending["segments"][0]["v1"]))
    parts_sum = sum(Decimal(str(p["segments"][0]["v1"])) for p in breakdown["parts"])
    assert parts_sum == total_2026


def test_named_part_names_come_from_its_own_indicator_not_hand_typed(
    national, aggregates, metadata, config
):
    payload = build_payload(national, aggregates, metadata, config)
    revenue = next(c for c in payload["counters"] if c["id"] == "revenue")
    pit = next(p for p in revenue["breakdown"]["parts"] if p["id"] == "pit")
    assert pit["names"] == national["indicators"]["GOV_TAX_PIT_BE"]["names"]


# ── D.995 fix: other_taxes' `plus` wiring (audit P2-1) ──────────────────────


def test_other_taxes_plus_wiring_adds_back_uncollected_tax(national, aggregates, metadata, config):
    """config/live_counters.yaml's `other_taxes` part reads
    `remainder_of: GOV_TAX_SSC_TOTAL_BE` `plus: [GOV_TAX_UNCOLLECTED_BE]` --
    GOV_TAX_SSC_TOTAL_BE is Eurostat's D2_D5_D91_D61_M_D995 series, NET of
    the D.995 write-off, while GOV_REVENUE_BE (TR) is not, so other_taxes'
    own `whole` must be the GROSS total (SSC_TOTAL + UNCOLLECTED) or the
    write-off silently lands in non_tax_revenue instead (docs/data_catalog.md,
    "D.995" note). This pins the wiring AND the arithmetic end to end, using
    this PR's real 2025 fixture values (tests/fixtures/live_counters/national.json,
    trimmed from the real 2026-10-03 payload, rule 36/39) -- it fails if
    `plus` is ever removed from the config, because the code would then use
    GOV_TAX_SSC_TOTAL_BE alone (282517.4, not 283412.6) as the whole and
    produce a different share and a different 2026 value than hand-computed
    here.
    """
    payload = build_payload(national, aggregates, metadata, config)
    revenue = next(c for c in payload["counters"] if c["id"] == "revenue")
    assert revenue["breakdown"]["year"] == "2025"
    other_taxes = next(p for p in revenue["breakdown"]["parts"] if p["id"] == "other_taxes")

    # basis must name BOTH series the gross total is built from, not just
    # remainder_of -- a reader has to be able to see the write-off was added
    # back, not just infer it (scripts/export_live_counters.py's
    # _breakdown_entry, the `plus` basis-entries loop).
    basis_indicators = [b["indicator"] for b in other_taxes["basis"]]
    assert basis_indicators == ["GOV_TAX_SSC_TOTAL_BE", "GOV_TAX_UNCOLLECTED_BE"]

    # Hand-computed arithmetic, all real 2025 fixture values (public/data/
    # national.json periods."2025"):
    #   social_contributions = 97512.6   (GOV_TAX_SOCIAL_CONTRIB_BE)
    #   pit                  = 75791.1   (GOV_TAX_PIT_BE)
    #   cit                  = 26308.0   (GOV_TAX_CIT_BE)
    #   vat                  = 39796.6   (GOV_TAX_VAT_BE)
    #   excise               = 11186.1   (GOV_TAX_EXCISE_BE)
    #   covered = 97512.6 + 75791.1 + 26308.0 + 39796.6 + 11186.1 = 250594.4
    #
    #   ssc_total   = 282517.4   (GOV_TAX_SSC_TOTAL_BE, NET of D.995)
    #   uncollected =    895.2   (GOV_TAX_UNCOLLECTED_BE, the D.995 write-off)
    #   gross = ssc_total + uncollected = 282517.4 + 895.2 = 283412.6
    #   (this `gross` is exactly the 283412.6 the audit's handoff names)
    #
    #   other_taxes_2025 = gross - covered = 283412.6 - 250594.4 = 32818.2
    #   share = other_taxes_2025 / GOV_REVENUE_BE_2025 = 32818.2 / 314736.4
    #         = 0.10427201937875631798546339095192040069086384669838
    #           (shares_from_breakdown uses Decimal at 50-digit precision;
    #           reproduced here the same way so the two agree to the last
    #           digit before rounding)
    #
    #   other_taxes_2026_v1 = money_round(share * revenue_2026_v1), where
    #   revenue_2026_v1 is revenue's OWN already-cent-rounded 2026 segment
    #   total (flow_segments() rounds every flow to the cent) -- apply_shares_
    #   rounded() multiplies the share against that same rounded total, not
    #   the unrounded one, so the parts always sum to the published total.
    covered = (
        Decimal("97512.6")
        + Decimal("75791.1")
        + Decimal("26308.0")
        + Decimal("39796.6")
        + Decimal("11186.1")
    )
    assert covered == Decimal("250594.4")
    gross = Decimal("282517.4") + Decimal("895.2")
    assert gross == Decimal("283412.6")
    other_taxes_2025 = gross - covered
    assert other_taxes_2025 == Decimal("32818.2")

    with localcontext() as ctx:
        ctx.prec = 50
        share = other_taxes_2025 / Decimal("314736.4")
        revenue_2026_v1 = Decimal(str(revenue["segments"][0]["v1"]))
        expected_2026_v1 = (share * revenue_2026_v1).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

    assert revenue["segments"][0]["v1"] == pytest.approx(329273940678.92, abs=0.01)
    assert other_taxes["segments"][0]["v1"] == pytest.approx(float(expected_2026_v1), abs=0.001)
    # = 34334058723.39 EUR -- i.e. "other taxes" paces toward roughly
    # EUR 34,334.1 million in 2026, the gross (D.995-inclusive) share of TR.


# ── determinism (rule 35) ───────────────────────────────────────────────────


def test_export_live_counters_is_byte_identical_on_rebuild(tmp_path):
    out1, out2 = tmp_path / "run1.json", tmp_path / "run2.json"
    export_live_counters(
        national_path=FIXTURES / "national.json",
        aggregates_path=FIXTURES / "aggregates.json",
        metadata_path=FIXTURES / "metadata_indicators.json",
        config_path=DEFAULT_CONFIG,
        out_path=out1,
    )
    export_live_counters(
        national_path=FIXTURES / "national.json",
        aggregates_path=FIXTURES / "aggregates.json",
        metadata_path=FIXTURES / "metadata_indicators.json",
        config_path=DEFAULT_CONFIG,
        out_path=out2,
    )
    assert out1.read_bytes() == out2.read_bytes()


def test_export_live_counters_never_touches_the_wall_clock(tmp_path, monkeypatch):
    """datetime.now()/time.time() both raise -- if the export still succeeds,
    nothing in it read the wall clock. fromtimestamp() (used to find the
    calendar year of a fixed anchor ms) is deliberately left alone: it is
    pure given its argument, not a clock read.

    Both src/analytics/live_counters.py and scripts/export_live_counters.py
    did `from datetime import datetime`, which binds the class directly into
    each module's OWN globals at import time. Patching the `datetime` module's
    `datetime` attribute (as this test used to do) never reaches either of
    those already-bound names -- the patch lands on an attribute nothing
    reads, so a real `datetime.now()` call in either module would sail
    straight through undetected (audit P2-2). Patching each module's own
    `datetime` name directly is the only way this guard can actually fail."""
    import datetime as datetime_module
    import time as time_module

    class _NoNow(datetime_module.datetime):
        @classmethod
        def now(cls, tz=None):
            raise AssertionError("export_live_counters must never call datetime.now()")

    monkeypatch.setattr("src.analytics.live_counters.datetime", _NoNow)
    monkeypatch.setattr("scripts.export_live_counters.datetime", _NoNow)
    monkeypatch.setattr(time_module, "time", lambda: (_ for _ in ()).throw(AssertionError("no")))

    out = tmp_path / "out.json"
    export_live_counters(
        national_path=FIXTURES / "national.json",
        aggregates_path=FIXTURES / "aggregates.json",
        metadata_path=FIXTURES / "metadata_indicators.json",
        config_path=DEFAULT_CONFIG,
        out_path=out,
    )
    assert out.is_file()


# ── config refusal propagates (rule 13) ─────────────────────────────────────


def test_a_malformed_config_refuses_the_whole_export(tmp_path, national, aggregates, metadata):
    bad_config = tmp_path / "bad.yaml"
    data = yaml.safe_load(DEFAULT_CONFIG.read_text(encoding="utf-8"))
    del data["schema_version"]
    bad_config.write_text(yaml.safe_dump(data), encoding="utf-8")

    national_path = tmp_path / "national.json"
    national_path.write_text(json.dumps(national), encoding="utf-8")
    aggregates_path = tmp_path / "aggregates.json"
    aggregates_path.write_text(json.dumps(aggregates), encoding="utf-8")
    metadata_path = tmp_path / "metadata.json"
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    with pytest.raises(LiveCounterError):
        export_live_counters(
            national_path=national_path,
            aggregates_path=aggregates_path,
            metadata_path=metadata_path,
            config_path=bad_config,
            out_path=tmp_path / "out.json",
        )


# ── data conditions degrade to "unavailable", never raise ──────────────────


def test_missing_trend_base_year_makes_the_flow_counter_unavailable_not_raised(
    national, aggregates, metadata, config
):
    broken = copy.deepcopy(national)
    del broken["indicators"]["GOV_REVENUE_BE"]["periods"]["2022"]
    payload = build_payload(broken, aggregates, metadata, config)
    revenue = next(c for c in payload["counters"] if c["id"] == "revenue")
    assert revenue["state"] == "unavailable"
    assert revenue["reason"] == "missing_year:2022"
    # Population is unaffected -- one broken counter does not block another.
    population = next(c for c in payload["counters"] if c["id"] == "population")
    assert population["state"] == "available"


def test_population_coverage_below_100_is_unavailable_not_raised(
    national, aggregates, metadata, config
):
    broken = copy.deepcopy(aggregates)
    broken["indicators"]["POPULATION_BY_COMMUNE"]["be:country"]["periods"]["2026"]["coverage"][
        "pct"
    ] = 97.2
    payload = build_payload(national, broken, metadata, config)
    population = next(c for c in payload["counters"] if c["id"] == "population")
    assert population["state"] == "unavailable"
    assert population["reason"].startswith("coverage_below_100")


def test_a_breakdown_remainder_too_negative_makes_only_the_breakdown_unavailable(
    national, aggregates, metadata, config
):
    broken = copy.deepcopy(national)
    # Inflate one named COFOG part well past the COFOG total -> the
    # (single-level) spending remainder goes far negative.
    broken["indicators"]["GOV_EXP_OLD_AGE_BE"]["periods"]["2024"]["value"] = 10_000_000.0
    payload = build_payload(broken, aggregates, metadata, config)
    spending = next(c for c in payload["counters"] if c["id"] == "spending")
    # The flow total itself (TE, unrelated to COFOG) still ticks.
    assert spending["state"] == "available"
    assert spending["breakdown"]["state"] == "unavailable"
    assert spending["breakdown"]["reason"].startswith("remainder_below_tolerance")


def test_an_unavailable_flow_cascades_to_the_difference_counter(
    national, aggregates, metadata, config
):
    broken = copy.deepcopy(national)
    del broken["indicators"]["GOV_REVENUE_BE"]["periods"]["2022"]
    payload = build_payload(broken, aggregates, metadata, config)
    deficit = next(c for c in payload["counters"] if c["id"] == "deficit")
    assert deficit["state"] == "unavailable"
    assert deficit["reason"] == "component_unavailable"
    # Debt, paced by that same unavailable deficit beyond latest_year, still
    # has the official years (<= latest_year) to work with here since its
    # anchor (2026-Q1) is already past latest_year (2025) with no official
    # pace year needed in between -- so debt only degrades if its OWN pace
    # year is missing. Assert it degrades precisely when that happens:
    debt = next(c for c in payload["counters"] if c["id"] == "debt")
    assert debt["state"] == "unavailable"


# ── wired into the export job (rule 37) ─────────────────────────────────────


def test_live_counters_payloads_is_in_the_export_selection():
    selected = {
        k.to_user_string()
        for k in build_defs().resolve_job_def("validate_and_export").asset_layer.selected_asset_keys
    }
    assert "live_counters_payloads" in selected
