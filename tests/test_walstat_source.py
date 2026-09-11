"""The WalStat adapter -- Block O, docs/features/walstat_adapter.md.

The happy path runs on rows fetched from the real IWEPS API on 2026-09-11 and
committed verbatim (tests/fixtures/walstat_sample.json: Namur and Charleroi,
nine series, 2013-2024), against a real geography load. Every refusal is a
hand-built response, one defect each, because the API publishes no unit, no
status and no masking flag: everything this adapter guarantees, it checks.

Ratios are hand-computed (CLAUDE.md rule 5), with the arithmetic written out.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import load_geography  # noqa: E402

from src.analytics.derived import growth_rate, share_of_total  # noqa: E402
from src.analytics.engine import ObservationSet, compute  # noqa: E402
from src.db import migrate  # noqa: E402
from src.fetchers.walstat import (  # noqa: E402
    WalStatCoverageError,
    WalStatSchemaError,
    WalStatSource,
    walloon_communes_on,
)
from src.pages.resolve import _format_value  # noqa: E402
from src.validation.config_schema import (  # noqa: E402
    ConfigValidationError,
    load_and_validate_derived,
)

DERIVED_DIR = Path(__file__).resolve().parents[1] / "config" / "indicators" / "derived"

FIXTURE = json.loads(
    (REPO / "tests" / "fixtures" / "walstat_sample.json").read_text(encoding="utf-8")
)
NAMUR, CHARLEROI = "92094", "52011"


@pytest.fixture(scope="module")
def geo_conn(tmp_path_factory):
    db = tmp_path_factory.mktemp("walstat") / "geo.db"
    migrate.run(db, migrations_dir=REPO / "migrations")
    load_geography.load(db, REPO / "config" / "geography", allow_unverified=True)
    conn = sqlite3.connect(str(db))
    yield conn
    conn.close()


def parse(conn, rows, **kwargs):
    raw = json.dumps(rows, ensure_ascii=False).encode("utf-8")
    return WalStatSource()._parse(raw, geo_conn=conn, **kwargs)


def row(ins="92094", name="Namur", period="année 2024", value="1.0", kind="Commune"):
    return {"ins": ins, "type_entite": kind, "entite": name, "periode": period, "valeur": value}


# --- the happy path, on real rows -------------------------------------------


def test_the_real_rows_parse_to_the_municipal_contract(geo_conn):
    rows = parse(geo_conn, FIXTURE["811500_1"], reconcile=False)
    assert len(rows) == 24
    for r in rows:
        assert set(r) == {"geo_id", "period", "value", "status"}
        assert r["status"] == "final"
        assert isinstance(r["value"], float)
    namur_2024 = next(r for r in rows if r["geo_id"] == "be:mun:92094" and r["period"] == "2024")
    assert namur_2024["value"] == 2319.7  # read off the API on 2026-09-11


def test_the_period_is_the_year_whatever_the_capital(geo_conn):
    rows = parse(geo_conn, [row(period="Année 2023"), row(period="année 2022")], reconcile=False)
    assert sorted(r["period"] for r in rows) == ["2022", "2023"]


def test_bastogne_and_bertogne_resolve_for_2024_and_their_successor_for_2025(geo_conn):
    """The merger of 2024-12-02: 82003 and 82005 are the communes of the 2024
    accounts, 82039 is the commune from 2025 on. resolve_geo decides that
    from the geography table; nothing here names the merger."""
    old = parse(geo_conn, [row("82003", "Bastogne"), row("82005", "Bertogne")], reconcile=False)
    assert {r["geo_id"] for r in old} == {"be:mun:82003", "be:mun:82005"}
    new = parse(geo_conn, [row("82039", "Bastogne", period="année 2025")], reconcile=False)
    assert new[0]["geo_id"] == "be:mun:82039"
    # The merged code for 2024, a year its parts still report: a backcast,
    # skipped and counted rather than loaded as a third commune.
    source = WalStatSource()
    assert (
        source._parse(
            json.dumps([row("82039", "Bastogne", period="année 2024")]).encode(),
            geo_conn=geo_conn,
            reconcile=False,
        )
        == []
    )
    assert source.backcast == [("82039", "2024")]


def test_a_recoded_commune_is_attributed_to_the_code_valid_that_year(geo_conn):
    """Enghien was 55010 until 2019-01-01 and is 51067 since; WalStat writes
    51067 for 2013 too. Same commune, same name, one predecessor: the 2013
    value belongs to be:mun:55010, and the substitution is recorded."""
    source = WalStatSource()
    rows = source._parse(
        json.dumps([row("51067", "Enghien", period="année 2013", value="472.8")]).encode(),
        geo_conn=geo_conn,
        reconcile=False,
    )
    assert rows == [{"geo_id": "be:mun:55010", "period": "2013", "value": 472.8, "status": "final"}]
    assert source.recoded == [("51067", "2013", "be:mun:55010")]
    # And under its own code, in a year it is valid, no substitution happens.
    later = source._parse(
        json.dumps([row("51067", "Enghien", period="année 2019", value="828.7")]).encode(),
        geo_conn=geo_conn,
        reconcile=False,
    )
    assert later[0]["geo_id"] == "be:mun:51067"
    assert source.recoded == []


def test_a_backcast_for_a_merged_commune_that_did_not_exist_is_skipped_and_counted(geo_conn):
    """WalStat gives Bastogne+Bertogne (82039, merged 2024-12-02) a figure
    for 2013, beside the real 2013 accounts of Bastogne and Bertogne. Two
    predecessors is a merger: the backcast is not a third commune, and it is
    not silently dropped either -- it is counted in rows_read."""
    source = WalStatSource()
    rows = source._parse(
        json.dumps(
            [
                row("82003", "Bastogne", period="année 2013", value="2225.7"),
                row("82005", "Bertogne", period="année 2013", value="1208.8"),
                row("82039", "Bastogne", period="année 2013", value="2045.4"),
            ]
        ).encode(),
        geo_conn=geo_conn,
        reconcile=False,
    )
    assert {r["geo_id"] for r in rows} == {"be:mun:82003", "be:mun:82005"}
    assert source.backcast == [("82039", "2013")]
    assert source._rows_read_hint(rows) == 3


def test_the_eleven_2019_recodings_all_resolve_and_reconcile_for_2013(geo_conn):
    """A 2013 response written on the 2019 grid, exactly as WalStat sends it:
    the eleven new Hainaut codes instead of the old ones, plus the Bastogne
    backcast. It reconciles to the 262 communes of 2013 by geo_id."""
    from src.fetchers.walstat import resolve_on_source_grid

    recoded = {
        "51067": "55010",
        "51068": "55039",
        "51069": "55023",
        "55085": "52063",
        "55086": "52043",
        "57096": "54007",
        "57097": "54010",
        "58001": "55022",
        "58002": "56011",
        "58003": "56085",
        "58004": "56087",
    }
    for new, old in recoded.items():
        assert resolve_on_source_grid(geo_conn, new, "2013") == f"be:mun:{old}", new
    on_2013 = walloon_communes_on(geo_conn, "2013")
    grid_2019 = [nis for nis in on_2013 if nis not in recoded.values()] + list(recoded) + ["82039"]
    rows = [row(nis, "x", period="année 2013") for nis in grid_2019]
    source = WalStatSource()
    assert len(source._parse(json.dumps(rows).encode(), geo_conn=geo_conn)) == 262
    assert len(source.recoded) == 11
    assert source.backcast == [("82039", "2013")]


def test_walloon_communes_on_counts_the_merger(geo_conn):
    assert len(walloon_communes_on(geo_conn, "2024")) == 262
    assert len(walloon_communes_on(geo_conn, "2025")) == 261
    assert "11002" not in walloon_communes_on(geo_conn, "2024")  # Antwerp is not in Wallonia


def test_a_complete_year_reconciles(geo_conn):
    complete = [row(nis, "x") for nis in walloon_communes_on(geo_conn, "2024")]
    source = WalStatSource()
    assert len(source._parse(json.dumps(complete).encode(), geo_conn=geo_conn)) == 262
    assert source.missing == {}


def test_a_commune_absent_from_a_year_is_a_missing_reading_not_a_refusal(geo_conn):
    """53068 is absent from WalStat's 2017 revenue series -- not "non
    disponible", simply not there. One absent commune is a missing reading,
    recorded and reported, and the other 261 load. Inventing it would be
    worse; refusing 261 real accounts for it would be too."""
    complete = [row(nis, "x") for nis in walloon_communes_on(geo_conn, "2024")]
    source = WalStatSource()
    rows = source._parse(json.dumps(complete[1:]).encode(), geo_conn=geo_conn)
    assert len(rows) == 261
    assert source.missing == {"2024": [f"be:mun:{complete[0]['ins']}"]}


def test_a_year_under_the_90_percent_floor_is_refused_as_partial(geo_conn):
    """The pipeline's own coverage floor for aggregates, applied to a
    response: 235 of 262 is 89.7%, a partial year, refused."""
    complete = [row(nis, "x") for nis in walloon_communes_on(geo_conn, "2024")]
    with pytest.raises(WalStatCoverageError, match="under the 90% floor"):
        parse(geo_conn, complete[:235])
    assert len(parse(geo_conn, complete[:236])) == 236  # 90.08%


# --- every refusal ------------------------------------------------------------


@pytest.mark.parametrize(
    "payload,message",
    [
        ({"data": []}, "not the documented bare array"),
        ([{"ins": "92094", "valeur": "1"}], "keys"),
        ([row(kind="Province")], "not 'Commune'"),
        ([row(period="2024")], "does not read"),
        ([row(period="T1 2024")], "does not read"),
        ([row(value="")], "is not a number"),
        ([row(value="n.d.")], "is not a number"),
        ([row(), row()], "appears twice"),
    ],
)
def test_a_malformed_response_is_refused_not_coerced(geo_conn, payload, message):
    with pytest.raises(WalStatSchemaError, match=message):
        parse(geo_conn, payload, reconcile=False)


def test_a_value_that_is_not_a_number_is_never_zero_or_suppressed(geo_conn):
    """WalStat documents no masking rule, so an unreadable value is a broken
    row -- turning it into 0 would publish a figure, turning it into
    `suppressed` would claim the source withheld it."""
    with pytest.raises(WalStatSchemaError):
        parse(geo_conn, [row(value="")], reconcile=False)


def test_non_disponible_is_a_missing_reading_not_a_zero_and_not_a_suppression(geo_conn):
    """Seen on the live API 2026-09-11: Chièvres' 2024 revenue reads the
    literal "non disponible" -- an account not yet filed. It is skipped and
    counted, never written: absence is how this pipeline says "no reading"
    (rule 26), and a zero or a `suppressed` would each be a different claim."""
    source = WalStatSource()
    raw = json.dumps([row(), row("51019", "Chièvres", value="non disponible")]).encode("utf-8")
    rows = source._parse(raw, geo_conn=geo_conn, reconcile=False)
    assert [r["geo_id"] for r in rows] == ["be:mun:92094"]
    assert source.unavailable == [("51019", "2024")]
    assert source._rows_read_hint(rows) == 2
    # Still present in the response, so a year with one unfiled account is
    # complete for coverage purposes -- the reading is missing, not the commune.
    complete = [row(nis, "x") for nis in walloon_communes_on(geo_conn, "2024")]
    complete[0]["valeur"] = "Non disponible"
    assert len(source._parse(json.dumps(complete).encode("utf-8"), geo_conn=geo_conn)) == 261
    assert len(source.unavailable) == 1
    assert source.missing == {}


def test_two_codes_resolving_to_one_commune_year_are_refused(geo_conn):
    """Audit P1-1: 55010 (Enghien's code until 2019) and 51067 (its code since)
    both resolve to Enghien for 2013. Accepting both would write two vintages
    in one run, the last row winning. Refused like any other duplicate."""
    with pytest.raises(WalStatSchemaError, match="already supplied"):
        parse(
            geo_conn,
            [
                row(ins="55010", name="Enghien", period="année 2013", value="1.0"),
                row(ins="51067", name="Enghien", period="année 2013", value="2.0"),
            ],
            reconcile=False,
        )


def test_an_unknown_nis_code_is_refused(geo_conn):
    from src.fetchers.walstat import WalStatGeographyError

    with pytest.raises(WalStatGeographyError, match="does not resolve"):
        parse(geo_conn, [row("99999", "Nowhere")], reconcile=False)


def test_a_flemish_commune_in_a_walloon_series_fails_reconciliation(geo_conn):
    complete = [row(nis, "x") for nis in walloon_communes_on(geo_conn, "2024")]
    with pytest.raises(WalStatCoverageError, match="not Walloon communes.*be:mun:11002"):
        parse(geo_conn, complete + [row("11002", "Antwerpen")])


def test_parse_without_a_geography_connection_is_refused():
    with pytest.raises(ValueError, match="geo_conn"):
        WalStatSource()._parse(b"[]")


# --- the four ratios, by hand -------------------------------------------------


def series_for(series_id: str, ins: str) -> dict[str, float]:
    return {
        r["periode"].split()[-1]: float(r["valeur"]) for r in FIXTURE[series_id] if r["ins"] == ins
    }


def test_namur_debt_change_2024_is_minus_3_64_percent():
    """(2965.5 - 3077.5) / 3077.5 x 100 = -112.0 / 3077.5 x 100 = -3.6393..."""
    assert growth_rate(series_for("811506_0", NAMUR), "2024", years=1) == pytest.approx(
        -3.6393, abs=1e-4
    )


def test_charleroi_debt_change_2024_is_plus_12_18_percent():
    """(6239.3 - 5561.7) / 5561.7 x 100 = 677.6 / 5561.7 x 100 = 12.1833..."""
    assert growth_rate(series_for("811506_0", CHARLEROI), "2024", years=1) == pytest.approx(
        12.1833, abs=1e-4
    )


def test_namur_ordinary_revenue_change_2024_is_minus_3_82_percent():
    """(2319.7 - 2411.8) / 2411.8 x 100 = -92.1 / 2411.8 x 100 = -3.8187..."""
    assert growth_rate(series_for("811500_1", NAMUR), "2024", years=1) == pytest.approx(
        -3.8187, abs=1e-4
    )


def test_namur_investment_share_2024_is_20_72_percent():
    """Extraordinary 566.4 over total 2733.5: 566.4 / 2733.5 x 100 = 20.7207..."""
    extra = series_for("811501_2", NAMUR)["2024"]
    total = series_for("811501_0", NAMUR)["2024"]
    assert (extra, total) == (566.4, 2733.5)
    assert share_of_total(extra, total) == pytest.approx(20.7207, abs=1e-4)


def test_namur_debt_to_revenue_2024_is_98_83_percent():
    """Debt 2965.5 over total revenue 3000.6: 2965.5 / 3000.6 x 100 = 98.8302..."""
    assert share_of_total(2965.5, 3000.6) == pytest.approx(98.8302, abs=1e-4)


def test_the_accounts_identity_holds_in_the_fixture():
    """Total = ordinary + extraordinary, in the source's own figures: Namur 2013
    expenditure 1722.1 = 1438.7 + 283.4. The exporter's composition check
    relies on this holding for every commune and year."""
    ordinary = series_for("811501_1", NAMUR)["2013"]
    extra = series_for("811501_2", NAMUR)["2013"]
    total = series_for("811501_0", NAMUR)["2013"]
    assert (ordinary, extra, total) == (1438.7, 283.4, 1722.1)
    assert ordinary + extra == pytest.approx(total, abs=0.05)


# --- the unit, written as a rate ---------------------------------------------


@pytest.mark.parametrize(
    "lang,expected",
    [("en", "€2,319.7 / hab."), ("fr", "€2 319,7 / hab."), ("nl", "€2.319,7 / hab.")],
)
def test_euros_per_inhabitant_is_written_as_a_rate(lang, expected):
    """Namur's 2 319,7 is what the commune raises per resident. Mapped to
    plain `eur` it would print as a total, which is a different claim."""
    assert _format_value(2319.7, {"unit": "eur_per_inhabitant", "decimals": 1}, lang) == expected


# --- the ratios THROUGH their configs, on a store with and without the rows --


def test_the_finance_ratios_are_deferred_on_a_store_without_walstat_rows():
    """Audit P0-1: the exports validate derived inputs against the indicators
    present in the store, and the daily run exports BEFORE it syncs -- so on
    a database that holds no WalStat row yet the four ratios must be left out
    quietly, not stop the build. Every other derived config is unaffected."""
    configs = load_and_validate_derived(DERIVED_DIR, {"POPULATION_BY_COMMUNE"})
    assert "POPULATION_CHANGE_5Y" in configs
    assert not {i for i in configs if i.startswith("MUN_")}


def test_the_finance_ratios_load_once_their_inputs_are_in_the_store():
    known = {
        "MUN_REVENUE_ORDINARY_PER_CAPITA",
        "MUN_EXPENDITURE_ORDINARY_PER_CAPITA",
        "MUN_EXPENDITURE_EXTRAORDINARY_PER_CAPITA",
        "MUN_EXPENDITURE_TOTAL_PER_CAPITA",
        "MUN_DEBT_TOTAL_PER_CAPITA",
        "MUN_REVENUE_TOTAL_PER_CAPITA",
    }
    configs = load_and_validate_derived(DERIVED_DIR, known)
    assert {
        "MUN_REVENUE_GROWTH_1Y",
        "MUN_EXPENDITURE_GROWTH_1Y",
        "MUN_INVESTMENT_SHARE_OF_EXPENDITURE",
        "MUN_DEBT_TO_REVENUE",
    } <= set(configs)


def test_an_input_configured_nowhere_still_fails_validation(tmp_path):
    indicators = tmp_path / "indicators"
    derived = indicators / "derived"
    derived.mkdir(parents=True)
    (derived / "X_RATIO.yaml").write_text(
        "id: X_RATIO\nname: {en: x, fr: x, nl: x}\nunit: percent\nfrequency: A\n"
        "geo_levels: [municipal]\npreferred_direction: neutral\n"
        "derived:\n  function: share_of_total\n  inputs: [NO_SUCH_SERIES, MUN_REVENUE_TOTAL_PER_CAPITA]\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigValidationError, match="NO_SUCH_SERIES"):
        load_and_validate_derived(derived, set())


def _fixture_observations() -> ObservationSet:
    import re

    import yaml

    by_series = {}
    for path in sorted(DERIVED_DIR.parent.glob("MUN_*.yaml")):
        cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
        by_series[re.search(r"/(\d{6}_\d)/", cfg["fetch"]["query"]).group(1)] = cfg["id"]
    cells = []
    for series_id, rows in FIXTURE.items():
        for r in rows:
            cells.append(
                (
                    by_series[series_id],
                    f"be:mun:{r['ins']}",
                    r["periode"].split()[-1],
                    float(r["valeur"]),
                )
            )
    return ObservationSet(cells)


def test_the_configs_wire_the_ratios_the_way_the_hand_computations_do():
    """Audit P2-2: the hand-computed tests above call the functions directly,
    so a swapped input order in a YAML would leave them green. This runs the
    committed configs through the engine on the fixture rows and checks the
    same four figures."""
    observations = _fixture_observations()
    known = {i for (i, _g, _p) in observations._by_cell}  # noqa: SLF001
    configs = load_and_validate_derived(DERIVED_DIR, known)
    result = compute(
        observations, {i: c for i, c in configs.items() if i.startswith("MUN_")}, known
    )
    namur = "be:mun:92094"
    assert result.value("MUN_DEBT_TO_REVENUE", namur, "2024") == pytest.approx(98.8302, abs=1e-4)
    assert result.value("MUN_INVESTMENT_SHARE_OF_EXPENDITURE", namur, "2024") == pytest.approx(
        20.7207, abs=1e-4
    )
    assert result.value("MUN_REVENUE_GROWTH_1Y", namur, "2024") == pytest.approx(-3.8187, abs=1e-4)
    # 811501_1 is ORDINARY expenditure, which is the series the config names --
    # reading the total here instead gives -10.04 %, a different claim.
    charleroi = series_for("811501_1", CHARLEROI)
    assert result.value("MUN_EXPENDITURE_GROWTH_1Y", "be:mun:52011", "2024") == pytest.approx(
        (charleroi["2024"] / charleroi["2023"] - 1) * 100, abs=1e-6
    )
