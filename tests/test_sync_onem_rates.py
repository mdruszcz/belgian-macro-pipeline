"""Contracts for the ONEM published-unemployment-rate loader.

Two things are being protected here, and they are different.

The first is arithmetic we did not do. The rate is ONEM's own quotient, so
there is no formula of ours to unit-test -- what has to be tested is that the
figure arriving in the database is the figure ONEM published, cell for cell.
The values below were read off ONEM's own PDF export
(ReportUnemploymentRates, June 2026) before this loader existed, so they are
an independent check on the parse rather than a copy of its output.

The second is the five states of CLAUDE.md rule 26. This file contains one
genuine 0.00 -- Herstappe, a commune of about eighty people, in 42 of its 124
periods and a real value in the other 82 -- and no masked cells at all. That
makes "0 means nobody" true today and a liability tomorrow: if ONEM ever
starts masking small communes, a mask read as 0 would publish "no
unemployment in Herstappe". So the loader refuses a blank or non-numeric
`graad` outright, and that refusal is tested.
"""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_onem_rates as sor  # noqa: E402

from src.db import migrate  # noqa: E402

REPO = Path(__file__).resolve().parents[1]

# Straight from ONEM's PDF export for June 2026, typed in by hand.
PDF_JUNE_2026 = {
    "92094": ("Namur", 6.74),
    "62063": ("Liège", 9.02),
    "52011": ("Charleroi", 7.64),
    "11002": ("Antwerpen", 5.73),
    "21004": ("Bruxelles", 8.19),
    "21013": ("Saint-Gilles", 11.52),
    "73028": ("Herstappe", 0.00),
}


def _write(path: Path, rows: list[list], header: list[str] | None = None) -> Path:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter=";", lineterminator="\n")
        writer.writerow(header if header is not None else sor.EXPECTED_HEADER)
        writer.writerows(rows)
    return path


def _row(level="5", zone="92094", year=2025, month=6, graad="6.00", diff=""):
    return [year, month, level, zone, graad, diff]


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "rates.db"
    migrate.run(path, migrations_dir=REPO / "migrations")
    load_geography.load(path, REPO / "config" / "geography", allow_unverified=True)
    return path


@pytest.fixture
def municipalities(db):
    conn = sqlite3.connect(db)
    try:
        return sor.current_municipalities(conn)
    finally:
        conn.close()


# --------------------------------------------------------------------------
# The file's shape. Every one of these is a refusal, per CLAUDE.md rule 13.
# --------------------------------------------------------------------------


def test_a_renamed_column_is_refused_not_guessed_at(tmp_path):
    path = _write(
        tmp_path / "r.csv",
        [_row()],
        header=["jaar", "maand", "level", "zonegeog", "werkloosheidsgraad", "diff1an"],
    )
    with pytest.raises(sor.OnemRateError, match="header is"):
        sor.read_rows(path)


def test_a_reordered_header_is_refused_even_with_the_same_names(tmp_path):
    """Order matters because the rows are zipped positionally: `graad` and
    `diff1an` swapped would load a change in percentage points as a level."""
    path = _write(
        tmp_path / "r.csv",
        [_row()],
        header=["jaar", "maand", "level", "zonegeog", "diff1an", "graad"],
    )
    with pytest.raises(sor.OnemRateError, match="header is"):
        sor.read_rows(path)


def test_an_extra_field_on_a_data_row_is_refused(tmp_path):
    path = _write(tmp_path / "r.csv", [_row() + ["surprise"]])
    with pytest.raises(sor.OnemRateError, match="fields, expected"):
        sor.read_rows(path)


def test_a_header_with_no_data_is_refused(tmp_path):
    path = _write(tmp_path / "r.csv", [])
    with pytest.raises(sor.OnemRateError, match="no data rows"):
        sor.read_rows(path)


def test_an_empty_file_is_refused(tmp_path):
    path = tmp_path / "r.csv"
    path.write_text("", encoding="utf-8")
    with pytest.raises(sor.OnemRateError, match="is empty"):
        sor.read_rows(path)


def test_the_header_is_accepted_with_quotes_and_a_bom(tmp_path):
    """The real file quotes its header row and ONEM has shipped a BOM before;
    neither is a schema change."""
    path = tmp_path / "r.csv"
    path.write_text(
        '﻿"jaar";"maand";"level";"zonegeog";"graad";"diff1an"\n2025;6;5;92094;6.00;\n',
        encoding="utf-8",
    )
    assert sor.read_rows(path) == [
        {
            "jaar": "2025",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "6.00",
            "diff1an": "",
        }
    ]


def test_blank_lines_are_skipped_rather_than_parsed_as_zero(tmp_path):
    path = tmp_path / "r.csv"
    path.write_text(
        "jaar;maand;level;zonegeog;graad;diff1an\n2025;6;5;92094;6.00;\n\n",
        encoding="utf-8",
    )
    assert len(sor.read_rows(path)) == 1


# --------------------------------------------------------------------------
# Rule 26: missing, suppressed and zero must not collapse into each other.
# --------------------------------------------------------------------------


def test_a_blank_rate_is_refused_and_never_becomes_a_zero():
    with pytest.raises(sor.OnemRateError, match="must not become a zero"):
        sor._parse_cell({"jaar": "2025", "maand": "6", "graad": "", "diff1an": ""}, "test")


def test_a_masked_rate_is_refused_rather_than_coerced():
    """ONEM masks small counts as the literal `<10` in its Excel tables. If
    that ever reaches the rate file, it must stop the load, not round to 0."""
    with pytest.raises(sor.OnemRateError, match="never as 0"):
        sor._parse_cell({"jaar": "2025", "maand": "6", "graad": "<10", "diff1an": ""}, "test")


def test_a_genuine_zero_is_kept_as_an_observed_zero():
    """Herstappe's 0.00 is a real measurement: roughly eighty residents and,
    in 42 of 124 periods, no insured jobseeker at all. It must survive as the
    number 0.0, distinguishable from a blank."""
    year, month, value = sor._parse_cell(
        {"jaar": "2026", "maand": "6", "graad": "0.00", "diff1an": "0.00"}, "test"
    )
    assert (year, month) == (2026, 6)
    assert value == 0.0
    assert value is not None


def test_an_unknown_month_code_is_refused():
    """1-12 are months and 13 is the annual mean. A 14 would be a new meaning
    -- a quarter, a rolling window -- and guessing would misdate a figure."""
    with pytest.raises(sor.OnemRateError, match="maand is 14"):
        sor._parse_cell({"jaar": "2025", "maand": "14", "graad": "6.00", "diff1an": ""}, "test")


def test_month_zero_is_refused():
    with pytest.raises(sor.OnemRateError, match="maand is 0"):
        sor._parse_cell({"jaar": "2025", "maand": "0", "graad": "6.00", "diff1an": ""}, "test")


# --------------------------------------------------------------------------
# The `diff1an` identity: the file auditing our parse.
# --------------------------------------------------------------------------


def test_the_year_on_year_identity_is_checked_and_counted():
    """Hand-computed: 8.50 - 6.00 = +2.50 pp, and only the 2026 rows have a
    predecessor, so exactly one identity is checkable out of two rows."""
    rows = [
        {
            "jaar": "2025",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "6.00",
            "diff1an": "",
        },
        {
            "jaar": "2026",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "8.50",
            "diff1an": "2.50",
        },
    ]
    assert sor.check_year_on_year(rows) == 1


def test_rounding_of_one_hundredth_is_tolerated():
    """ONEM differences unrounded rates and publishes both to two decimals, so
    6.00 -> 8.50 can legitimately be printed as +2.49. Measured worst case
    over the whole 2026-09-12 file: exactly 0.0100."""
    rows = [
        {
            "jaar": "2025",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "6.00",
            "diff1an": "",
        },
        {
            "jaar": "2026",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "8.50",
            "diff1an": "2.49",
        },
    ]
    assert sor.check_year_on_year(rows) == 1


def test_a_shifted_column_is_caught_by_the_identity():
    """The scenario this check exists for: 8.50 against 6.00 is +2.50, so a
    published -0.31 means we are reading some other commune, some other month,
    or some other column."""
    rows = [
        {
            "jaar": "2025",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "6.00",
            "diff1an": "",
        },
        {
            "jaar": "2026",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "8.50",
            "diff1an": "-0.31",
        },
    ]
    with pytest.raises(sor.OnemRateError, match="12-month change"):
        sor.check_year_on_year(rows)


def test_the_identity_is_keyed_per_zone_and_never_across_communes():
    """Namur's 2026 must be differenced against Namur's 2025, not Liège's. If
    the key dropped `zonegeog`, this pair would validate against the wrong
    predecessor and the wrong one would pass."""
    rows = [
        {
            "jaar": "2025",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "6.00",
            "diff1an": "",
        },
        {
            "jaar": "2025",
            "maand": "6",
            "level": "5",
            "zonegeog": "62063",
            "graad": "9.00",
            "diff1an": "",
        },
        {
            "jaar": "2026",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "8.50",
            "diff1an": "2.50",
        },
    ]
    assert sor.check_year_on_year(rows) == 1


def test_the_identity_is_keyed_per_level_and_never_across_them():
    """Zone codes repeat across levels -- `53` is both a region and an
    arrondissement -- so the level has to be part of the key."""
    rows = [
        {
            "jaar": "2025",
            "maand": "6",
            "level": "2",
            "zonegeog": "53",
            "graad": "4.00",
            "diff1an": "",
        },
        {
            "jaar": "2026",
            "maand": "6",
            "level": "4",
            "zonegeog": "53",
            "graad": "8.50",
            "diff1an": "2.50",
        },
    ]
    assert sor.check_year_on_year(rows) == 0


def test_an_absent_diff_is_skipped_not_treated_as_zero():
    rows = [
        {
            "jaar": "2025",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "6.00",
            "diff1an": "",
        },
        {
            "jaar": "2026",
            "maand": "6",
            "level": "5",
            "zonegeog": "92094",
            "graad": "8.50",
            "diff1an": "",
        },
    ]
    assert sor.check_year_on_year(rows) == 0


# --------------------------------------------------------------------------
# Coverage: exactly today's municipalities, in both directions.
# --------------------------------------------------------------------------


def test_a_commune_absent_from_the_file_is_refused(municipalities):
    rows = [_dict_row(nis) for nis in sorted(municipalities)[:-1]]
    with pytest.raises(sor.OnemRateError, match="absent from the file"):
        sor.check_commune_coverage(rows, municipalities)


def test_a_commune_we_do_not_know_is_refused(municipalities):
    rows = [_dict_row(nis) for nis in municipalities] + [_dict_row("99999")]
    with pytest.raises(sor.OnemRateError, match="unknown code"):
        sor.check_commune_coverage(rows, municipalities)


def test_the_real_commune_set_is_exactly_our_current_municipalities(municipalities):
    """The file's 565 codes and the municipalities in force today are the same
    set, so nothing is dropped and nothing is invented."""
    assert len(municipalities) == 565
    rows = [_dict_row(nis) for nis in municipalities]
    sor.check_commune_coverage(rows, municipalities)


def test_higher_level_zones_do_not_count_towards_commune_coverage(municipalities):
    """ONEM's region and province codes look like short NIS codes. They must
    not be mistaken for communes, in either direction."""
    rows = [_dict_row(nis) for nis in municipalities]
    rows += [
        {
            "jaar": "2025",
            "maand": "6",
            "level": "2",
            "zonegeog": "53",
            "graad": "4.00",
            "diff1an": "",
        },
        {
            "jaar": "2025",
            "maand": "6",
            "level": "3",
            "zonegeog": "0",
            "graad": "4.00",
            "diff1an": "",
        },
    ]
    sor.check_commune_coverage(rows, municipalities)


def _dict_row(nis, year="2025", month="6", graad="6.00"):
    return {
        "jaar": year,
        "maand": month,
        "level": "5",
        "zonegeog": nis,
        "graad": graad,
        "diff1an": "",
    }


# --------------------------------------------------------------------------
# The annual row: a part-year mean is loadable but never `final`.
# --------------------------------------------------------------------------


def test_a_year_with_all_twelve_months_is_final():
    rows = [_dict_row("92094", year="2025", month=str(m)) for m in range(1, 13)]
    rows.append(_dict_row("92094", year="2025", month="13"))
    assert sor.annual_status(rows) == {2025: "final"}


def test_a_running_year_with_six_months_is_provisional():
    """2026's annual row is a mean over six months. For Namur it reads 8.08
    against 6.74 in June -- 1.34 pp apart -- so calling it settled would
    publish a figure the year has not finished producing."""
    rows = [_dict_row("92094", year="2026", month=str(m)) for m in range(1, 7)]
    rows.append(_dict_row("92094", year="2026", month="13"))
    assert sor.annual_status(rows) == {2026: "provisional"}


def test_a_year_missing_one_month_in_the_middle_is_provisional():
    rows = [_dict_row("92094", year="2025", month=str(m)) for m in range(1, 13) if m != 7]
    assert sor.annual_status(rows) == {2025: "provisional"}


def test_the_annual_status_ignores_higher_level_rows():
    """Belgium's months must not make a year look complete when a commune's
    are missing; the status is judged on the commune grid."""
    rows = [
        {
            "jaar": "2026",
            "maand": str(m),
            "level": "1",
            "zonegeog": "99",
            "graad": "5.00",
            "diff1an": "",
        }
        for m in range(1, 13)
    ]
    rows += [_dict_row("92094", year="2026", month=str(m)) for m in range(1, 7)]
    assert sor.annual_status(rows) == {2026: "provisional"}


# --------------------------------------------------------------------------
# The monthly window: trimmed for file size, anchored on the data.
# --------------------------------------------------------------------------


def test_the_cutoff_counts_back_from_the_files_latest_month():
    """Hand-computed: 18 months ending 2026-06 inclusive starts at 2025-01.
    June 2026 is month 1 counting back, January 2025 is month 18."""
    rows = [_dict_row("92094", year="2026", month="6")]
    assert sor.monthly_cutoff(rows, months=18) == "2025-01"


def test_the_cutoff_crosses_a_year_boundary_correctly():
    """The arithmetic a naive `year - 3` gets wrong. 12 months ending 2026-02
    starts at 2025-03, not 2025-02 and not 2026-03."""
    rows = [_dict_row("92094", year="2026", month="2")]
    assert sor.monthly_cutoff(rows, months=12) == "2025-03"


def test_a_window_of_one_keeps_only_the_latest_month():
    rows = [_dict_row("92094", year="2026", month="6")]
    assert sor.monthly_cutoff(rows, months=1) == "2026-06"


def test_the_cutoff_ignores_the_annual_row():
    """`maand` 13 is not a thirteenth month. If it counted, the window would be
    anchored one month too late and the newest real month would be dropped."""
    rows = [
        _dict_row("92094", year="2026", month="6"),
        _dict_row("92094", year="2026", month="13"),
    ]
    assert sor.monthly_cutoff(rows, months=12) == "2025-07"


def test_the_cutoff_is_anchored_on_the_data_and_not_on_today():
    """Anchoring on `date.today()` would make the same file load differently
    tomorrow, and the byte-identical rebuild checks would start failing on a
    calendar boundary rather than on a real change."""
    rows = [_dict_row("92094", year="2019", month="4")]
    assert sor.monthly_cutoff(rows, months=24) == "2017-05"


def test_the_window_is_18_months_and_the_annual_series_is_untrimmed():
    """Pinned rather than left implicit. This constant is not a preference: at
    24 months the committed database passes daily_fetch.yml's own 39.06 MB
    commit guard by -0.38 MB and the daily run fails, taking every other
    source's data down with it. Raising it requires fixing the file size
    first -- see docs/decisions/0005-onem-published-rate.md."""
    assert sor.MONTHLY_HISTORY_MONTHS == 18


def test_months_before_the_cutoff_are_not_stored_but_their_years_still_are(
    db, tmp_path, municipalities
):
    """The point of trimming only the monthly series: 2017 has to remain
    readable as an annual figure even though its months are gone."""
    rows: list[list] = []
    for nis in sorted(municipalities):
        for year in (2017, 2026):
            for month in list(range(1, 7)) + [13]:
                rows.append([year, month, "5", nis, "6.00", ""])
    path = _write(tmp_path / "old.csv", rows)
    sor.sync(db, csv_path=path)
    conn = sqlite3.connect(db)
    monthly = sorted(
        p
        for (p,) in conn.execute(
            "SELECT DISTINCT period FROM observations WHERE indicator_id = ?",
            (sor.INDICATOR_MONTHLY,),
        )
    )
    annual = sorted(
        p
        for (p,) in conn.execute(
            "SELECT DISTINCT period FROM observations WHERE indicator_id = ?",
            (sor.INDICATOR_ANNUAL,),
        )
    )
    # 18 months back from 2026-06 is 2025-01, so 2017's months fall outside it
    # while 2026's six months do not.
    assert monthly == ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"]
    assert annual == ["2017", "2026"]
    conn.close()


# --------------------------------------------------------------------------
# End to end, on the real geography.
# --------------------------------------------------------------------------


def _full_file(path: Path, municipalities: dict[str, str]) -> Path:
    """A file shaped exactly like ONEM's: every current commune, 2025 complete
    and 2026 half-done, plus each year's annual row and the Belgium series,
    plus region/province/arrondissement rows that must be ignored.
    """
    rows: list[list] = []
    for nis in sorted(municipalities):
        for month in list(range(1, 13)) + [13]:
            rows.append([2025, month, "5", nis, "6.00", ""])
        for month in list(range(1, 7)) + [13]:
            rows.append([2026, month, "5", nis, "4.50", "-1.50"])
    for month in list(range(1, 13)) + [13]:
        rows.append([2025, month, "1", "99", "6.50", ""])
    for month in list(range(1, 7)) + [13]:
        rows.append([2026, month, "1", "99", "4.40", "-2.10"])
    # Deliberately present and deliberately not loaded.
    rows.append([2025, 6, "2", "55", "9.90", ""])
    rows.append([2025, 6, "3", "29", "5.10", ""])
    rows.append([2025, 6, "4", "25", "5.20", ""])
    return _write(path, rows)


def test_the_loader_writes_both_frequencies_with_the_right_periods(db, tmp_path, municipalities):
    path = _full_file(tmp_path / "full.csv", municipalities)
    read, written = sor.sync(db, csv_path=path)
    conn = sqlite3.connect(db)

    # 565 communes x (13 + 7) rows + Belgium's 20 = 11,320 ; hand-computed.
    assert read == 565 * 20 + 20 == 11320
    assert written == read

    monthly = conn.execute(
        "SELECT COUNT(*), MIN(period), MAX(period) FROM observations " "WHERE indicator_id = ?",
        (sor.INDICATOR_MONTHLY,),
    ).fetchone()
    assert monthly == (565 * 18 + 18, "2025-01", "2026-06")

    annual = conn.execute(
        "SELECT COUNT(*), MIN(period), MAX(period) FROM observations " "WHERE indicator_id = ?",
        (sor.INDICATOR_ANNUAL,),
    ).fetchone()
    assert annual == (565 * 2 + 2, "2025", "2026")
    conn.close()


def test_the_running_year_lands_as_provisional_and_the_closed_one_as_final(
    db, tmp_path, municipalities
):
    path = _full_file(tmp_path / "full.csv", municipalities)
    sor.sync(db, csv_path=path)
    conn = sqlite3.connect(db)
    statuses = dict(
        conn.execute(
            "SELECT period, status FROM observations WHERE indicator_id = ? "
            "AND geo_id = 'be:country'",
            (sor.INDICATOR_ANNUAL,),
        )
    )
    assert statuses == {"2025": "final", "2026": "provisional"}
    conn.close()


def test_belgium_is_loaded_and_the_levels_between_are_not(db, tmp_path, municipalities):
    """Levels 2-4 carry ONEM's own zone codes on ONEM's own territories -- 55
    is Wallonia WITHOUT the German-speaking communes, which is not this
    repository's Wallonia. Publishing it under our name would describe a
    different place, and the rate's denominator is unpublished so we cannot
    build our own. Belgium (zone 99) is the one unambiguous aggregate.
    """
    path = _full_file(tmp_path / "full.csv", municipalities)
    sor.sync(db, csv_path=path)
    conn = sqlite3.connect(db)
    levels = dict(
        conn.execute(
            "SELECT g.level, COUNT(*) FROM observations o "
            "JOIN geographies g ON g.geo_id = o.geo_id "
            "WHERE o.indicator_id LIKE 'UNEMPLOYMENT_RATE_INSURED%' GROUP BY g.level"
        )
    )
    assert set(levels) == {"municipality", "country"}
    assert levels["country"] == 20
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM observations o JOIN geographies g ON g.geo_id = o.geo_id "
            "WHERE o.indicator_id LIKE 'UNEMPLOYMENT_RATE_INSURED%' AND g.level = 'region'"
        ).fetchone()[0]
        == 0
    )
    conn.close()


def test_the_indicator_rows_refuse_an_aggregate_rather_than_weighting_one(db):
    """ADR 0003 recomputes a ratio from summed parts; ONEM publishes only the
    quotient, so there are no parts. `population_weighted` is the wrong answer
    twice over -- CLAUDE.md forbids it outright, and the whole population is
    not this rate's denominator.
    """
    sor.sync(db, reference_rows_only=True)
    conn = sqlite3.connect(db)
    rows = {
        r[0]: r[1:]
        for r in conn.execute(
            "SELECT indicator_id, aggregation_method, is_additive, unit, frequency "
            "FROM indicators WHERE indicator_id LIKE 'UNEMPLOYMENT_RATE_INSURED%'"
        )
    }
    assert rows[sor.INDICATOR_ANNUAL] == ("not_applicable", 0, "percent", "A")
    assert rows[sor.INDICATOR_MONTHLY] == ("not_applicable", 0, "percent", "M")
    conn.close()


def test_reference_rows_only_needs_no_file_and_writes_no_observations(db):
    read, written = sor.sync(db, reference_rows_only=True)
    assert (read, written) == (0, 0)
    conn = sqlite3.connect(db)
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM observations WHERE indicator_id LIKE "
            "'UNEMPLOYMENT_RATE_INSURED%'"
        ).fetchone()[0]
        == 0
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM indicators WHERE indicator_id LIKE "
            "'UNEMPLOYMENT_RATE_INSURED%'"
        ).fetchone()[0]
        == 2
    )
    conn.close()


def test_both_onem_scripts_agree_on_the_source_row_in_either_order(tmp_path):
    """Both loaders write the `onem` source row with INSERT OR IGNORE, so the
    first to run wins. If they disagreed, the licence text and base URL shown
    beside a figure would depend on the order of two steps in a Makefile --
    and the attribution ONEM's own conditions require is not negotiable.
    """
    import sync_onem

    rows = []
    for first, second in ((sync_onem, sor), (sor, sync_onem)):
        path = tmp_path / f"{first.__name__}.db"
        migrate.run(path, migrations_dir=REPO / "migrations")
        load_geography.load(path, REPO / "config" / "geography", allow_unverified=True)
        first.sync(path, reference_rows_only=True)
        second.sync(path, reference_rows_only=True)
        conn = sqlite3.connect(path)
        rows.append(
            conn.execute(
                "SELECT source_id, name, agency, adapter, base_url, licence, catalog_ref, "
                "cadence, is_active FROM sources WHERE source_id = 'onem'"
            ).fetchone()
        )
        conn.close()
    assert rows[0] == rows[1]
    assert rows[0][0] == "onem"
    assert "credit ONEM/RVA as the source" in rows[0][5]


def test_the_trilingual_description_reaches_the_database(db):
    """Rule 7: the French and Dutch descriptions are what a reader on the
    commune page is shown, so they cannot be dropped on the way in."""
    sor.sync(db, reference_rows_only=True)
    conn = sqlite3.connect(db)
    nl, fr, en = conn.execute(
        "SELECT description_nl, description_fr, description_en FROM indicators "
        "WHERE indicator_id = ?",
        (sor.INDICATOR_ANNUAL,),
    ).fetchone()
    for text in (nl, fr, en):
        assert text and len(text) > 200
    assert "assurés contre le chômage" in fr
    assert "insured against" in en
    assert "verzekerde" in nl
    conn.close()


# --------------------------------------------------------------------------
# Against the real download, when it is present.
# --------------------------------------------------------------------------


REAL_FILE = sor.RAW_CACHE_DIR / sor.CSV_NAME


@pytest.mark.skipif(not REAL_FILE.is_file(), reason="the ONEM rate file is not cached")
def test_the_real_file_matches_onems_own_pdf_export(db):
    """The one test that proves we publish ONEM's number. Expected values were
    read off ONEM's PDF, not produced by this code."""
    sor.sync(db, csv_path=REAL_FILE)
    conn = sqlite3.connect(db)
    for nis, (name, expected) in PDF_JUNE_2026.items():
        row = conn.execute(
            "SELECT o.value FROM observations o JOIN geographies g ON g.geo_id = o.geo_id "
            "WHERE o.indicator_id = ? AND g.nis_code = ? AND g.valid_to IS NULL "
            "AND o.period = '2026-06'",
            (sor.INDICATOR_MONTHLY, nis),
        ).fetchone()
        assert row is not None, f"{name} ({nis}) is missing from the load"
        assert row[0] == pytest.approx(expected), name
    conn.close()


@pytest.mark.skipif(not REAL_FILE.is_file(), reason="the ONEM rate file is not cached")
def test_the_real_file_carries_the_march_2026_break(db):
    """Not a nice-to-have. Belgium sits at 6.28 in February and 4.41 in April;
    if a future file smooths that away, the break note in both indicator
    descriptions would be describing data that no longer exists.
    """
    sor.sync(db, csv_path=REAL_FILE)
    conn = sqlite3.connect(db)
    series = dict(
        conn.execute(
            "SELECT period, value FROM observations WHERE indicator_id = ? "
            "AND geo_id = 'be:country' AND period LIKE '2026-%'",
            (sor.INDICATOR_MONTHLY,),
        )
    )
    assert series["2026-02"] == pytest.approx(6.28)
    assert series["2026-03"] == pytest.approx(5.47)
    assert series["2026-04"] == pytest.approx(4.41)
    assert series["2026-02"] - series["2026-04"] > 1.5
    conn.close()


@pytest.mark.skipif(not REAL_FILE.is_file(), reason="the ONEM rate file is not cached")
def test_the_real_file_holds_herstappes_zero_as_a_zero(db):
    """42 of 124 periods at exactly 0.00, and a real value in the rest. Both
    have to survive: a zero that became NULL would read as "not published",
    and a NULL that became zero would read as "no unemployment"."""
    sor.sync(db, csv_path=REAL_FILE)
    conn = sqlite3.connect(db)
    values = [
        v
        for (v,) in conn.execute(
            "SELECT o.value FROM observations o JOIN geographies g ON g.geo_id = o.geo_id "
            "WHERE o.indicator_id = ? AND g.nis_code = '73028' AND g.valid_to IS NULL",
            (sor.INDICATOR_MONTHLY,),
        )
    ]
    assert values, "Herstappe has no monthly rate at all"
    assert None not in values
    assert 0.0 in values
    assert max(values) > 0.0
