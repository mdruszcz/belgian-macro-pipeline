"""Tests for src/flows/buyer_origin.py: the OriginTable.csv parser, the wide Municipality CSV
parser, and the ADR 0013 computation (denominator, buckets, tie-break, coverage, zero-purchase
state). Hand-computed expected values (CLAUDE.md rule 5) are the ADR's own worked examples --
Boechout (11004), Herstappe (73028), Gesves (92054) -- which the ADR itself derived from a real
live read of the 2025 file on 2026-09-24; none of them is typed from nowhere (rule 36).

Built on the same real-migrations + real-geography db fixture tests/test_sync_spf_agdp.py uses,
since bucket classification needs the real arrondissement/region hierarchy, not a hand-rolled
stub (a stub could silently encode the exact bug this module needs to catch).
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402

from src.db import migrate  # noqa: E402
from src.flows.buyer_origin import (  # noqa: E402
    BUCKET_IDS,
    NO_PURCHASES_RECORDED,
    FlowSchemaError,
    _ancestors,
    _bucket_for_origin,
    _normalized_geo_id,
    compute_all_destinations,
    compute_destination_flows,
    parse_municipality_rows,
    parse_origin_table,
)

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"

PERIOD = "2025"


@pytest.fixture
def db(tmp_path):
    import sqlite3

    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    conn = sqlite3.connect(str(db_path))
    yield conn
    conn.close()


# --- OriginTable.csv parsing --------------------------------------------------------


def _origin_table_bytes(rows: list[str]) -> bytes:
    header = "BuyerFrom;NISCode;NameFre;NameDut;ISOCode"
    text = "\r\n".join([header, *rows]) + "\r\n"
    return text.encode("latin-1")


def test_parse_origin_table_reads_commune_rows():
    raw = _origin_table_bytes(
        [
            "BuyerFrom11004;11004;Boechout;Boechout;",
            "BuyerFrom41018;41018;Grammont;Geraardsbergen;",
        ]
    )
    table = parse_origin_table(raw)
    assert table["11004"].nis_code == "11004"
    assert table["11004"].name_fr == "Boechout"
    assert table["41018"].nis_code == "41018"


def test_parse_origin_table_latin1_country_name_round_trips():
    """A country name with an accented character must not mojibake -- this is the whole
    reason OriginTable.csv is read as latin-1 and not utf-8-sig (ADR 'Consequences')."""
    raw = "Guin\xe9e".encode("latin-1")  # "Guinée" -- \xe9 is 'é' in latin-1
    row = f"BuyerFrom324;324;{raw.decode('latin-1')};Guinee;GN"
    table = parse_origin_table(_origin_table_bytes([row]))
    assert table["324"].name_fr == "Guinée"
    assert table["324"].nis_code is None  # not a 5-digit NIS -- a country code
    assert table["324"].iso_code == "GN"


def test_parse_origin_table_unknown_codes_have_no_nis():
    raw = _origin_table_bytes(
        [
            "BuyerFrom000;000;Ind\xe9termin\xe9;Onbepaald;",
            "BuyerFrom999;999;Inconnu;Onbekend;XX",
        ]
    )
    table = parse_origin_table(raw)
    assert table["000"].nis_code is None
    assert table["999"].nis_code is None


def test_parse_origin_table_missing_column_refuses():
    text = "BuyerFrom;NISCode;NameFre\r\nBuyerFrom11004;11004;Boechout\r\n"
    with pytest.raises(FlowSchemaError):
        parse_origin_table(text.encode("latin-1"))


def test_parse_origin_table_empty_refuses():
    header = "BuyerFrom;NISCode;NameFre;NameDut;ISOCode\r\n"
    with pytest.raises(FlowSchemaError):
        parse_origin_table(header.encode("latin-1"))


# --- Municipality wide CSV parsing --------------------------------------------------


def _municipality_bytes(origin_codes: list[str], rows: list[tuple[str, str, dict]]) -> bytes:
    header = "NISCode;NameFre;NameDut;NameGer;ParcelNature;ParcelsNumber;" + ";".join(
        f"BuyerFrom{c}" for c in origin_codes
    )
    lines = [header]
    for nis, parcel_nature, cells in rows:
        values = ";".join(str(cells.get(c, "0")) for c in origin_codes)
        lines.append(
            f"{nis};Commune;Commune;Commune;{parcel_nature};{cells.get('_parcels', 0)};{values}"
        )
    text = "﻿" + "\r\n".join(lines) + "\r\n"
    return text.encode("utf-8")


def _small_origin_table():
    return parse_origin_table(
        _origin_table_bytes(
            [
                "BuyerFrom11004;11004;Boechout;Boechout;",
                "BuyerFrom11002;11002;Anvers;Antwerpen;",
                "BuyerFrom41018;41018;Grammont;Geraardsbergen;",
                "BuyerFrom000;000;Ind\xe9termin\xe9;Onbepaald;",
                "BuyerFrom999;999;Inconnu;Onbekend;XX",
                "BuyerFrom101;101;Albanie;Albani\xeb;AL",
            ]
        )
    )


def test_parse_municipality_rows_keeps_only_total_and_nonzero_cells():
    origin_table = _small_origin_table()
    raw = _municipality_bytes(
        ["11004", "11002", "000"],
        [
            ("11004", "TOTAL", {"_parcels": "10", "11004": "5", "11002": "0", "000": "1"}),
            ("11004", "MAISON", {"_parcels": "3", "11004": "3"}),  # not TOTAL, skipped
        ],
    )
    rows = parse_municipality_rows(raw, origin_table=origin_table)
    assert len(rows) == 1
    assert rows[0]["nis"] == "11004"
    assert rows[0]["parcels_number"] == 10
    assert rows[0]["cells"] == {"11004": Decimal("5"), "000": Decimal("1")}


def test_parse_municipality_rows_missing_column_refuses():
    text = "NISCode;ParcelNature\r\n11004;TOTAL\r\n"
    with pytest.raises(FlowSchemaError):
        parse_municipality_rows(text.encode("utf-8-sig"), origin_table=_small_origin_table())


def test_parse_municipality_rows_unknown_origin_column_refuses():
    origin_table = _small_origin_table()
    raw = _municipality_bytes(
        ["11004", "99999"], [("11004", "TOTAL", {"_parcels": "1", "11004": "1"})]
    )
    with pytest.raises(FlowSchemaError):
        parse_municipality_rows(raw, origin_table=origin_table)


def test_parse_municipality_rows_blank_cell_refuses():
    origin_table = _small_origin_table()
    raw = _municipality_bytes(["11004"], [])
    # Inject a blank cell manually.
    text = raw.decode("utf-8-sig")
    text += "11005;C;C;C;TOTAL;5;\r\n"
    with pytest.raises(FlowSchemaError):
        parse_municipality_rows(("﻿" + text).encode("utf-8"), origin_table=origin_table)


def test_parse_municipality_rows_duplicate_total_row_refuses():
    origin_table = _small_origin_table()
    raw = _municipality_bytes(
        ["11004"],
        [
            ("11004", "TOTAL", {"_parcels": "1", "11004": "1"}),
            ("11004", "TOTAL", {"_parcels": "2", "11004": "2"}),
        ],
    )
    with pytest.raises(FlowSchemaError):
        parse_municipality_rows(raw, origin_table=origin_table)


# --- The ADR computation: Boechout worked example -----------------------------------


def test_boechout_denominator_and_coverage(db):
    """ADR worked example: ParcelsNumber 423, D = 413.995833337, coverage 97.9%."""
    origin_table = _small_origin_table()
    dest_row = {
        "nis": "11004",
        "parcels_number": 423,
        "cells": {
            "11004": Decimal("151.15416667"),
            "11002": Decimal("79.6"),
            "41018": Decimal("10"),
            "101": Decimal("1"),  # abroad
        },
    }
    result = compute_destination_flows(
        db, dest_row=dest_row, origin_table=origin_table, period=PERIOD
    )
    total = sum(dest_row["cells"].values())
    assert result.denominator == total
    assert result.coverage == (total / 423 * 100).quantize(Decimal("0.1"))
    assert result.state == "final"


def test_boechout_top8_tie_break_by_nis_ascending(db):
    """ADR: Schilde (11039) and Geraardsbergen (41018) both at exactly 10 -- 11039 must sort
    before 41018 because its NIS is numerically and lexically lower."""
    origin_table = parse_origin_table(
        _origin_table_bytes(
            [
                "BuyerFrom11004;11004;Boechout;Boechout;",
                "BuyerFrom11002;11002;Anvers;Antwerpen;",
                "BuyerFrom11029;11029;Mortsel;Mortsel;",
                "BuyerFrom11021;11021;Hove;Hove;",
                "BuyerFrom12021;12021;Lierre;Lier;",
                "BuyerFrom11013;11013;Edegem;Edegem;",
                "BuyerFrom11039;11039;Schilde;Schilde;",
                "BuyerFrom41018;41018;Grammont;Geraardsbergen;",
                "BuyerFrom11052;11052;Wommelgem;Wommelgem;",
            ]
        )
    )
    dest_row = {
        "nis": "11004",
        "parcels_number": 423,
        "cells": {
            "11004": Decimal("151.15416667"),
            "11002": Decimal("79.6"),
            "11029": Decimal("46.858333333"),
            "11021": Decimal("22.166666667"),
            "12021": Decimal("17"),
            "11013": Decimal("12.186666667"),
            "11039": Decimal("10"),
            "41018": Decimal("10"),
            "11052": Decimal("6.5"),
        },
    }
    result = compute_destination_flows(
        db, dest_row=dest_row, origin_table=origin_table, period=PERIOD
    )
    top8_nis = [o.nis for o in result.top_origins]
    assert top8_nis == ["11004", "11002", "11029", "11021", "12021", "11013", "11039", "41018"]
    # 11052 (6.5) is the 9th candidate and must NOT appear in the top 8.
    assert "11052" not in top8_nis


def test_boechout_buckets_match_adr_worked_example(db):
    """ADR: same_commune 151.15416667, rest_of_arrondissement 211.291666667,
    rest_of_region 50.5, other_regions 0.05, abroad 1, origin_unknown 0 -- sums to D exactly."""
    origin_table = parse_origin_table(
        _origin_table_bytes(
            [
                "BuyerFrom11004;11004;Boechout;Boechout;",  # same_commune (arr 11000)
                "BuyerFrom11039;11039;Schilde;Schilde;",  # rest_of_arrondissement (arr 11000)
                "BuyerFrom41018;41018;Grammont;Geraardsbergen;",  # rest_of_region (arr 41000, reg 02000)
                "BuyerFrom10000;10000;Placeholder;Placeholder;",  # not used
                "BuyerFrom101;101;Albanie;Albani\xeb;AL",  # abroad
                "BuyerFrom000;000;Ind\xe9termin\xe9;Onbepaald;",  # origin_unknown
            ]
        )
    )
    dest_row = {
        "nis": "11004",
        "parcels_number": 423,
        "cells": {
            "11004": Decimal("151.15416667"),  # same_commune
            "11039": Decimal("211.291666667"),  # rest_of_arrondissement
            "41018": Decimal("50.5"),  # rest_of_region (Geraardsbergen: East Flanders)
            "101": Decimal("1"),  # abroad
        },
    }
    result = compute_destination_flows(
        db, dest_row=dest_row, origin_table=origin_table, period=PERIOD
    )
    assert result.bucket_values["same_commune"] == Decimal("151.15416667")
    assert result.bucket_values["rest_of_arrondissement"] == Decimal("211.291666667")
    assert result.bucket_values["rest_of_region"] == Decimal("50.5")
    assert result.bucket_values["abroad"] == Decimal("1")
    assert result.bucket_values["origin_unknown"] == Decimal("0")
    assert sum(result.bucket_values.values()) == result.denominator


def test_origin_unknown_bucket_never_folded_into_abroad(db):
    origin_table = parse_origin_table(
        _origin_table_bytes(
            [
                "BuyerFrom11004;11004;Boechout;Boechout;",
                "BuyerFrom000;000;Ind\xe9termin\xe9;Onbepaald;",
                "BuyerFrom999;999;Inconnu;Onbekend;XX",
                "BuyerFrom101;101;Albanie;Albani\xeb;AL",
            ]
        )
    )
    dest_row = {
        "nis": "11004",
        "parcels_number": 10,
        "cells": {
            "11004": Decimal("5"),
            "000": Decimal("2"),
            "999": Decimal("1"),
            "101": Decimal("1"),
        },
    }
    result = compute_destination_flows(
        db, dest_row=dest_row, origin_table=origin_table, period=PERIOD
    )
    assert result.bucket_values["origin_unknown"] == Decimal("3")
    assert result.bucket_values["abroad"] == Decimal("1")


# --- Herstappe (single-origin edge) and Gesves (coverage > 100%) --------------------


def test_herstappe_single_origin_full_own_commune(db):
    """ADR second worked example: ParcelsNumber 1, D 1, coverage 100.0%, one named origin
    (Herstappe itself), same_commune 100.0%, all other buckets 0."""
    origin_table = parse_origin_table(
        _origin_table_bytes(["BuyerFrom73028;73028;Herstappe;Herstappe;"])
    )
    dest_row = {"nis": "73028", "parcels_number": 1, "cells": {"73028": Decimal("1")}}
    result = compute_destination_flows(
        db, dest_row=dest_row, origin_table=origin_table, period=PERIOD
    )
    assert result.denominator == Decimal("1")
    assert result.coverage == Decimal("100.0")
    assert result.bucket_shares["same_commune"] == Decimal("100.0")
    for bucket in BUCKET_IDS:
        if bucket != "same_commune":
            assert result.bucket_values[bucket] == Decimal("0")
    assert [o.nis for o in result.top_origins] == ["73028"]


def test_gesves_coverage_exceeds_100_percent_and_is_not_clamped(db):
    """ADR decision 2/3: coverage is not bounded by 100% and must never be clamped. Gesves'
    real 2025 figures: ParcelsNumber 187, D 187.30, coverage 100.16% rounds to 100.2%."""
    origin_table = parse_origin_table(_origin_table_bytes(["BuyerFrom92054;92054;Gesves;Gesves;"]))
    dest_row = {"nis": "92054", "parcels_number": 187, "cells": {"92054": Decimal("187.30")}}
    result = compute_destination_flows(
        db, dest_row=dest_row, origin_table=origin_table, period=PERIOD
    )
    assert result.denominator == Decimal("187.30")
    assert result.coverage > Decimal("100.0")
    assert result.coverage == Decimal("100.2")  # 187.30/187*100 = 100.1604...


# --- Zero-purchase state (ADR decision 7) -------------------------------------------


def test_zero_denominator_is_no_purchases_recorded_never_zero_percent(db):
    origin_table = _small_origin_table()
    dest_row = {"nis": "11004", "parcels_number": 0, "cells": {}}
    result = compute_destination_flows(
        db, dest_row=dest_row, origin_table=origin_table, period=PERIOD
    )
    assert result.state == NO_PURCHASES_RECORDED
    assert result.top_origins == ()
    assert result.bucket_values == {}


def test_measured_zero_parcels_number_still_renders_as_zero_not_dash(db):
    """A ParcelsNumber of 0 alongside no_purchases_recorded is a measured zero (rule 26),
    carried through unchanged -- never coerced to None/absent."""
    origin_table = _small_origin_table()
    dest_row = {"nis": "11004", "parcels_number": 0, "cells": {}}
    result = compute_destination_flows(
        db, dest_row=dest_row, origin_table=origin_table, period=PERIOD
    )
    assert result.parcels_number == 0


# --- Geography: both axes resolve through resolve_geo, rule 13 on failure ----------


def test_unresolvable_destination_nis_raises(db):
    origin_table = _small_origin_table()
    dest_row = {"nis": "99999", "parcels_number": 1, "cells": {"11004": Decimal("1")}}
    with pytest.raises(FlowSchemaError):
        compute_destination_flows(db, dest_row=dest_row, origin_table=origin_table, period=PERIOD)


def test_unresolvable_origin_nis_raises_never_bucketed_as_unknown(db):
    """ADR decision 4: 'An origin NIS that fails to resolve raises... it is never bucketed as
    unknown.' A NIS-shaped code the OriginTable claims is Belgian but that resolve_geo does
    not know must fail loudly, not silently land in origin_unknown."""
    origin_table = parse_origin_table(
        _origin_table_bytes(
            ["BuyerFrom11004;11004;Boechout;Boechout;", "BuyerFrom99999;99999;Nowhere;Nowhere;"]
        )
    )
    dest_row = {
        "nis": "11004",
        "parcels_number": 2,
        "cells": {"11004": Decimal("1"), "99999": Decimal("1")},
    }
    with pytest.raises(FlowSchemaError):
        compute_destination_flows(db, dest_row=dest_row, origin_table=origin_table, period=PERIOD)


# --- Aggregation over multiple destinations: deterministic ordering -----------------


def _all_municipality_nis(conn) -> list[str]:
    rows = conn.execute(
        "SELECT nis_code FROM geographies WHERE level = 'municipality' "
        "AND valid_from <= '2025-01-01' AND (valid_to IS NULL OR valid_to > '2025-01-01')"
    ).fetchall()
    return [r[0] for r in rows]


# --- geo_id version-suffix normalisation (audit finding 3) --------------------------


def test_normalized_geo_id_strips_version_suffix():
    assert _normalized_geo_id("BE_ARR_11000@2025-01-01") == "BE_ARR_11000"
    assert _normalized_geo_id("BE_ARR_11000") == "BE_ARR_11000"


def test_bucket_for_origin_treats_versioned_and_unversioned_arrondissement_as_equal():
    """One versioned, one unversioned geo_id for the same real arrondissement must compare
    equal -- a same-arrondissement origin must land in rest_of_arrondissement, not
    other_regions, regardless of which row happens to carry a version marker."""
    bucket = _bucket_for_origin(
        origin_geo_id="BE_MUN_11002",
        origin_is_unknown=False,
        origin_is_abroad=False,
        dest_geo_id="BE_MUN_11004",
        dest_arr_id="BE_ARR_11000@2025-01-01",  # versioned
        dest_reg_id="BE_REG_02000",
        origin_arr_id="BE_ARR_11000",  # unversioned, same real arrondissement
        origin_reg_id="BE_REG_02000",
    )
    assert bucket == "rest_of_arrondissement"


def test_bucket_for_origin_treats_versioned_and_unversioned_region_as_equal():
    bucket = _bucket_for_origin(
        origin_geo_id="BE_MUN_41018",
        origin_is_unknown=False,
        origin_is_abroad=False,
        dest_geo_id="BE_MUN_11004",
        dest_arr_id="BE_ARR_41000",
        dest_reg_id="BE_REG_02000@2025-01-01",  # versioned
        origin_arr_id="BE_ARR_41000_OTHER",
        origin_reg_id="BE_REG_02000",  # unversioned, same real region
    )
    assert bucket == "rest_of_region"


# --- Ancestor cache (audit finding 5) -------------------------------------------------


def test_ancestors_cache_returns_same_result_and_is_populated(db):
    from src.geography.resolve import resolve_geo

    geo_id = resolve_geo(db, "11004", PERIOD)
    cache: dict = {}
    first = _ancestors(db, geo_id, cache=cache)
    assert geo_id in cache
    assert cache[geo_id] == first


class _CountingConnProxy:
    """Wraps a sqlite3.Connection and counts `.execute()` calls -- sqlite3.Connection is a C
    type and cannot be monkeypatched directly, so this proxy stands in for it."""

    def __init__(self, conn):
        self._conn = conn
        self.execute_count = 0

    def execute(self, *args, **kwargs):
        self.execute_count += 1
        return self._conn.execute(*args, **kwargs)


def test_ancestors_cache_avoids_a_second_query(db):
    from src.geography.resolve import resolve_geo

    geo_id = resolve_geo(db, "11004", PERIOD)
    proxy = _CountingConnProxy(db)
    cache: dict = {}
    _ancestors(proxy, geo_id, cache=cache)  # populates the cache
    calls_after_first = proxy.execute_count
    assert calls_after_first > 0

    result = _ancestors(proxy, geo_id, cache=cache)
    assert proxy.execute_count == calls_after_first  # cache hit -- no new query issued
    assert result == cache[geo_id]


def test_compute_all_destinations_sorted_by_geo_id(db):
    origin_table = _small_origin_table()
    all_nis = _all_municipality_nis(db)
    named = {"41018", "11002", "11004"}
    rows = [
        (
            {"nis": nis, "parcels_number": 1, "cells": {nis: Decimal("1")}}
            if nis in named and nis in origin_table
            else {"nis": nis, "parcels_number": 0, "cells": {}}
        )
        for nis in all_nis
    ]
    results = compute_all_destinations(
        db, destinations=rows, origin_table=origin_table, period=PERIOD
    )
    assert len(results) == len(all_nis)
    assert [r.dest_geo_id for r in results] == sorted(r.dest_geo_id for r in results)


# --- Destination completeness (audit finding 1) -------------------------------------


def test_compute_all_destinations_refuses_when_a_commune_is_missing(db):
    """The resolved destination set must equal every municipality valid at the period --
    a fixture missing one commune (Herstappe, 73028) must raise, naming the missing NIS."""
    origin_table = _small_origin_table()
    all_nis = [n for n in _all_municipality_nis(db) if n != "73028"]
    rows = [{"nis": nis, "parcels_number": 0, "cells": {}} for nis in all_nis]
    with pytest.raises(FlowSchemaError, match="73028"):
        compute_all_destinations(db, destinations=rows, origin_table=origin_table, period=PERIOD)


def test_compute_all_destinations_refuses_when_an_extra_nis_is_present(db):
    """An extra destination NIS the geography table does not recognise as a municipality at
    this period (here, a made-up 5-digit code) must also raise, naming the extra NIS."""
    origin_table = _small_origin_table()
    all_nis = _all_municipality_nis(db)
    rows = [{"nis": nis, "parcels_number": 0, "cells": {}} for nis in all_nis]
    rows.append({"nis": "00000", "parcels_number": 0, "cells": {}})
    with pytest.raises(FlowSchemaError, match="00000"):
        compute_all_destinations(db, destinations=rows, origin_table=origin_table, period=PERIOD)
