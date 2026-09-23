"""scripts/sync_walstat.py end to end, without the network.

`--from-dir` replays cached responses through the same parse and the same
refusals as a live fetch. The replay here is built in-test: for each of the
nine series, one JSON file covering EVERY Walloon commune valid in the year
(read from the real geography load, so the count is 262 for 2023 and 2024),
with Namur's and Charleroi's rows taken verbatim from the committed fixture
and the other communes given a synthetic value that says which series and
year it came from -- the way tests/test_sync_onem.py fakes its sheets.
"""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import load_geography  # noqa: E402
import sync_walstat  # noqa: E402

from src.db import migrate  # noqa: E402
from src.fetchers.walstat import WalStatCoverageError, walloon_communes_on  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

FIXTURE = json.loads(
    (REPO / "tests" / "fixtures" / "walstat_sample.json").read_text(encoding="utf-8")
)
YEARS = ("2023", "2024")


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "walstat.db"
    migrate.run(path, migrations_dir=REPO / "migrations")
    load_geography.load(path, REPO / "config" / "geography", allow_unverified=True)
    return path


def series_by_code(finance_only: bool = True) -> dict[str, str]:
    """indicator id -> WalStat series id, read from the configs' own fetch queries.

    FINANCE-ONLY BY DEFAULT. WalStat is no longer only a municipal-finance
    source: UNEMPLOYMENT_RATE_BIT (series 236400_0) is a PERCENTAGE RATE for
    15-64-year-olds, not a per-inhabitant euro amount, and it carries none of
    the properties the tests below assert about the nine accounts series --
    the unit, the decimals, the direction and even the period form all differ.
    Lumping it in would have made those assertions say something weaker about
    all ten rather than something exact about the nine, so the two groups are
    separated here instead.
    """
    indicators, _ = load_and_validate_all(
        REPO / "config" / "indicators", REPO / "config" / "sources"
    )
    out = {}
    for code, cfg in sync_walstat.walstat_indicators(indicators).items():
        if finance_only and not code.startswith("MUN_"):
            continue
        out[code] = cfg["fetch"]["query"].split("/")[2]  # /json/811500_1/com+period=all
    return out


def build_replay(db: Path, out_dir: Path, drop_one_from: str | None = None) -> dict[str, str]:
    """One complete file per WalStat series the configs name -- the sync reads
    the configs, so a series with no file is a FileNotFoundError, not a skip.

    Optionally drops one commune from 2024 in one series, to exercise the
    missing-reading path.

    The nine finance series are replayed from the committed API fixture where
    it has a row and filled with plausible euro amounts elsewhere.
    UNEMPLOYMENT_RATE_BIT has no fixture (it was added later) and is generated
    entirely -- with ITS OWN PERIOD FORM, "moyenne annuelle YYYY", which is
    the difference that made the adapter's period pattern need widening.
    """
    conn = sqlite3.connect(str(db))
    communes = {year: walloon_communes_on(conn, year) for year in YEARS}
    conn.close()
    codes = series_by_code(finance_only=False)
    out_dir.mkdir()
    for code, series in codes.items():
        is_rate = not code.startswith("MUN_")
        real = {(r["ins"], r["periode"].split()[-1]): r for r in FIXTURE.get(series, [])}
        rows = []
        for year in YEARS:
            for index, nis in enumerate(sorted(communes[year])):
                if (nis, year) in real:
                    rows.append(real[(nis, year)])
                    continue
                rows.append(
                    {
                        "ins": nis,
                        "type_entite": "Commune",
                        "entite": f"Commune {nis}",
                        "periode": (f"moyenne annuelle {year}" if is_rate else f"année {year}"),
                        # A rate has to look like a rate: a euro amount in a
                        # percent series would load happily and be wrong.
                        "valeur": (
                            f"{3 + index % 15}.{index % 10}"
                            if is_rate
                            else f"{1000 + index}.{int(series[-1])}"
                        ),
                    }
                )
        if code == drop_one_from:
            rows = [r for r in rows if not (r["ins"] == "93090" and r["periode"].endswith("2024"))]
        (out_dir / f"{code}.json").write_text(
            json.dumps(rows, ensure_ascii=False), encoding="utf-8"
        )
    return codes


def test_a_replayed_day_loads_every_series_for_every_walloon_commune(db, tmp_path):
    codes = build_replay(db, tmp_path / "replay")
    fetched, changed = sync_walstat.sync(db, from_dir=tmp_path / "replay")
    assert fetched == changed == len(codes) * (262 + 262)

    conn = sqlite3.connect(str(db))
    namur = dict(
        conn.execute(
            "SELECT indicator_id, value FROM observations WHERE geo_id = 'be:mun:92094' "
            "AND period = '2024' AND is_latest = 1"
        ).fetchall()
    )
    # Read off the API on 2026-09-11 and committed in the fixture.
    assert namur["MUN_REVENUE_ORDINARY_PER_CAPITA"] == 2319.7
    assert namur["MUN_EXPENDITURE_ORDINARY_PER_CAPITA"] == 2167.1
    assert namur["MUN_DEBT_TOTAL_PER_CAPITA"] == 2965.5
    assert len(namur) == len(codes)
    # The tenth series rode the same path, from a period form the nine do not
    # use -- so this asserts the widened pattern end to end, not just in a
    # regex unit test.
    assert "UNEMPLOYMENT_RATE_BIT" in namur
    assert 0 < namur["UNEMPLOYMENT_RATE_BIT"] < 100, "a rate outside 0-100 is not a rate"
    statuses = {s for (s,) in conn.execute("SELECT DISTINCT status FROM observations")}
    assert statuses == {"final"}
    assert conn.execute("SELECT COUNT(*) FROM fetch_runs WHERE source_id = 'walstat'").fetchone()[
        0
    ] == len(codes)


def test_reference_rows_say_per_inhabitant_figures_cannot_be_aggregated(db):
    sync_walstat.sync(db, reference_rows_only=True)
    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT indicator_id, unit, aggregation_method, is_additive, decimals, preferred_direction "
        "FROM indicators WHERE source_id = 'walstat' AND indicator_id LIKE 'MUN_%' "
        "ORDER BY indicator_id"
    ).fetchall()
    assert len(rows) == 9
    for code, unit, method, additive, decimals, direction in rows:
        assert unit == "eur_per_inhabitant", code
        assert method == "not_applicable", code
        assert additive == 0, code
        assert decimals == 1, code
        assert direction == ("lower_is_better" if "DEBT" in code else "contextual"), code
    source = conn.execute(
        "SELECT adapter, licence FROM sources WHERE source_id = 'walstat'"
    ).fetchone()
    assert source[0] == "walstat"
    assert "CC0" in source[1]
    assert conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 0


def test_the_unemployment_rate_is_a_percentage_and_equally_unaggregatable(db):
    """The tenth WalStat series is a RATE, so it must not inherit the euro
    unit the nine accounts series carry -- and it must still refuse to be
    aggregated, for a different reason: WalStat publishes no labour-force
    denominator, so a province figure cannot be recomputed from these rows
    (docs/decisions/0003, "refuse, do not invent")."""
    sync_walstat.sync(db, reference_rows_only=True)
    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT unit, aggregation_method, is_additive, preferred_direction "
        "FROM indicators WHERE indicator_id = 'UNEMPLOYMENT_RATE_BIT'"
    ).fetchone()
    assert row is not None, "the BIT series has no reference row"
    unit, method, additive, direction = row
    assert unit == "percent"
    assert method == "not_applicable"
    assert additive == 0
    assert direction == "lower_is_better"


def test_a_second_replay_writes_no_new_vintage(db, tmp_path):
    codes = build_replay(db, tmp_path / "replay")
    sync_walstat.sync(db, from_dir=tmp_path / "replay")
    fetched, changed = sync_walstat.sync(db, from_dir=tmp_path / "replay")
    assert fetched == len(codes) * 524
    assert changed == 0


def test_one_absent_commune_loads_the_other_261_and_leaves_a_hole(db, tmp_path, capsys):
    """Viroinval (93090) missing from the 2024 debt file: a missing reading,
    reported on the console, and the other 261 accounts load. No row is
    invented for it and no zero is written."""
    build_replay(db, tmp_path / "replay", drop_one_from="MUN_DEBT_TOTAL_PER_CAPITA")
    sync_walstat.sync(db, from_dir=tmp_path / "replay")
    conn = sqlite3.connect(str(db))
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM observations WHERE indicator_id = 'MUN_DEBT_TOTAL_PER_CAPITA' "
            "AND period = '2024'"
        ).fetchone()[0]
        == 261
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM observations WHERE indicator_id = 'MUN_DEBT_TOTAL_PER_CAPITA' "
            "AND geo_id = 'be:mun:93090' AND period = '2024'"
        ).fetchone()[0]
        == 0
    )
    assert "1 commune-year(s) absent" in capsys.readouterr().out


def test_the_gas_meter_states_are_skipped_counted_and_reported_through_sync(db, tmp_path, capsys):
    """The gas prepayment-meter series (PREPAYMENT_METERS_GAS_SHARE) end to
    end: a response mixing all seven non-numeric strings with real numbers
    loads only the numeric rows, and the sync's own console report names
    each skipped state -- not just the parser's internal counters."""
    conn = sqlite3.connect(str(db))
    communes = sorted(walloon_communes_on(conn, "2024"))
    conn.close()
    assert len(communes) >= 8
    non_numeric = [
        "pas de gaz",
        "pas  de gaz",
        "< 300 compteurs",
        "Non fiable",
        "non fiable",
        "non diffusé",
        "non disponible",
    ]
    rows = []
    for index, nis in enumerate(communes):
        value = non_numeric[index] if index < len(non_numeric) else f"{10 + index % 5}.{index % 10}"
        rows.append(
            {
                "ins": nis,
                "type_entite": "Commune",
                "entite": f"Commune {nis}",
                "periode": "31/12/2024",
                "valeur": value,
            }
        )
    out_dir = tmp_path / "replay"
    out_dir.mkdir()
    (out_dir / "PREPAYMENT_METERS_GAS_SHARE.json").write_text(
        json.dumps(rows, ensure_ascii=False), encoding="utf-8"
    )
    # The other 13 series still need files -- the sync reads every configured
    # walstat indicator, so fill them minimally, reusing build_replay's shape
    # for a year that is not being asserted on here.
    codes = series_by_code(finance_only=False)
    for code, series in codes.items():
        if code == "PREPAYMENT_METERS_GAS_SHARE":
            continue
        is_rate = not code.startswith("MUN_")
        filler = [
            {
                "ins": nis,
                "type_entite": "Commune",
                "entite": f"Commune {nis}",
                "periode": "année 2024" if not is_rate else "moyenne annuelle 2024",
                "valeur": f"{10 + i % 5}.{i % 10}" if is_rate else f"{1000 + i}.{int(series[-1])}",
            }
            for i, nis in enumerate(communes)
        ]
        (out_dir / f"{code}.json").write_text(
            json.dumps(filler, ensure_ascii=False), encoding="utf-8"
        )

    sync_walstat.sync(db, from_dir=out_dir)
    out = capsys.readouterr().out
    assert "PREPAYMENT_METERS_GAS_SHARE:" in out
    assert "'pas de gaz' (not-applicable, no gas network)" in out
    assert "'< 300 compteurs' (suppressed)" in out
    assert "'non fiable' (missing, publisher disowns)" in out
    assert "'non diffusé' (withheld)" in out

    conn = sqlite3.connect(str(db))
    written = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE indicator_id = 'PREPAYMENT_METERS_GAS_SHARE' "
        "AND period = '2024'"
    ).fetchone()[0]
    # 7 of the communes carried a non-numeric state, skipped; the rest wrote.
    assert written == len(communes) - 7
    # None of the seven non-numeric communes got a row -- not a zero, not any
    # other value -- for this indicator-period.
    seven_nis = communes[:7]
    for nis in seven_nis:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM observations WHERE indicator_id = 'PREPAYMENT_METERS_GAS_SHARE' "
                f"AND geo_id = 'be:mun:{nis}' AND period = '2024'"
            ).fetchone()[0]
            == 0
        )


def test_a_partial_file_refuses_the_series_and_marks_its_run_an_error(db, tmp_path):
    """Half a year is a partial response, not a year with gaps: refused, and
    this script's own fetch_runs row says `error` so the validation layer's
    fetch_error rule sees it."""
    codes = build_replay(db, tmp_path / "replay")
    path = tmp_path / "replay" / "MUN_DEBT_TOTAL_PER_CAPITA.json"
    rows = json.loads(path.read_text(encoding="utf-8"))
    path.write_text(json.dumps(rows[:300]), encoding="utf-8")  # 2023 complete, 2024 has 38
    with pytest.raises(WalStatCoverageError, match="under the 90% floor"):
        sync_walstat.sync(db, from_dir=tmp_path / "replay")
    conn = sqlite3.connect(str(db))
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM observations WHERE indicator_id = 'MUN_DEBT_TOTAL_PER_CAPITA'"
        ).fetchone()[0]
        == 0
    )
    assert (
        conn.execute(
            "SELECT status FROM fetch_runs WHERE source_id = 'walstat' ORDER BY fetch_run_id DESC LIMIT 1"
        ).fetchone()[0]
        == "error"
    )
    assert codes


def test_two_syncs_of_the_same_response_write_byte_identical_csvs(db, tmp_path):
    """CLAUDE.md rule 35: identical inputs must keep producing byte-identical
    output. Two independent DBs, same replayed response (including 14-decimal
    gas-share values), exported through the same CSV writer the real offload
    path uses -- every column except the two run timestamps (vintage,
    created_at) must match exactly."""
    import sys as _sys

    _sys.path.insert(0, str(REPO / "scripts"))
    from export_observations_csv import COLUMNS, export_observations

    codes = build_replay(db, tmp_path / "replay")
    sync_walstat.sync(db, from_dir=tmp_path / "replay")
    csv1 = tmp_path / "run1.csv"
    export_observations(db, csv1, list(codes))

    db2 = tmp_path / "walstat2.db"
    migrate.run(db2, migrations_dir=REPO / "migrations")
    load_geography.load(db2, REPO / "config" / "geography", allow_unverified=True)
    build_replay(db2, tmp_path / "replay2")
    sync_walstat.sync(db2, from_dir=tmp_path / "replay2")
    csv2 = tmp_path / "run2.csv"
    export_observations(db2, csv2, list(codes))

    timestamp_cols = {"vintage", "created_at"}
    keep = [i for i, c in enumerate(COLUMNS) if c not in timestamp_cols]
    rows1 = [tuple(r[i] for i in keep) for r in csv.reader(csv1.open(encoding="utf-8"))]
    rows2 = [tuple(r[i] for i in keep) for r in csv.reader(csv2.open(encoding="utf-8"))]
    assert rows1 == rows2
    assert len(rows1) > 1


def test_every_walstat_config_names_its_series():
    """`fetch.query` is what makes a series loadable; a config without one is
    a name for nothing, and the script says so rather than skipping it."""
    codes = series_by_code()
    assert len(codes) == 9
    assert set(codes.values()) == set(FIXTURE)
