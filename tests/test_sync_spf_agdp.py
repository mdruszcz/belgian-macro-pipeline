"""Tests for scripts/sync_spf_agdp.py: incremental fetch (the state file),
per-quarter geo resolution, the five-state write, idempotence and
determinism -- built on the same real-migrations + real-geography db fixture
tests/test_sync_bankruptcies.py uses. The per-row five-state mapping itself
is tests/test_spf_agdp_source.py's job; this file exercises the sync layer
around it with `atom_bytes=`/`version_bytes=` fixtures, no network.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_spf_agdp  # noqa: E402

from src.db import migrate  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"

_LEASES_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;RegistrationType;LessorType;TakerType;"
    "RentsNumber;RentP25;RentP50;RentP75;ChargesP25;ChargesP50;ChargesP75;"
    "TotalRentP25;TotalRentP50;TotalRentP75"
)
_TX_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;TransactionType;ParcelNature;ParcelsNumber;"
    "PriceP25;PriceP50;PriceP75;ParcelsAreaP25;ParcelsAreaP50;ParcelsAreaP75"
)


def _leases_row(nis, rents_number, rent_p50, charges_p50=None):
    """`charges_p50` defaults to a plausible non-blank figure so a row built
    with a count >= 5 does not accidentally trip the "unexpected blank
    percentile" schema guard -- callers exercising the suppression/na states
    pass an explicit blank ('') for whichever percentile they are testing."""
    if charges_p50 is None:
        charges_p50 = "" if rent_p50 == "" else "70"
    return (
        f"{nis};Commune;Commune;Commune;HousingRegistration;TOTAL;TOTAL;"
        f"{rents_number};800;{rent_p50};950;60;{charges_p50};80;900;1000;1100"
    )


def _leases_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_LEASES_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def _tx_row(nis, parcels_number):
    return f"{nis};Commune;Commune;Commune;VENTEIMMEUB;TOTAL;{parcels_number};100000;200000;300000;50;100;150"


def _tx_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_TX_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


_OWNER_HEADER = "NISCode;Fictious;NameFre;NameDut;NameGer;PersonType;HousingRightType;PersonNumber"
_DYNAMICS_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;ParcelNature;ParcelsNumber;"
    "PropertyChange0Y;PropertyDurationP25;PropertyDurationP50;PropertyDurationP75;"
    "PropertyDurationMean;PropertyRotationMean"
)


def _owner_row(nis, number, fictious="0"):
    return f"{nis};{fictious};Commune;Commune;Commune;Total;OCCUPANTPUPES;{number}"


def _owner_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_OWNER_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def _dynamics_row(nis, parcels, p50, rotation="10.0"):
    return f"{nis};Commune;Commune;Commune;TOTAL;{parcels};0;8;{p50};10;9.5;{rotation}"


def _dynamics_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_DYNAMICS_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def _atom_xml(links: list[tuple[str, str, int]]) -> bytes:
    body = "\n".join(
        f'<link rel="section" href="{href}" time="{time}" length="{length}"/>'
        for href, time, length in links
    )
    return (
        '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">' f"{body}</feed>"
    ).encode()


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    return db_path


@pytest.fixture
def state_path(tmp_path):
    return tmp_path / "spf_agdp_state.json"


def _observations(db_path, indicator_id, geo_id=None):
    conn = sqlite3.connect(str(db_path))
    q = "SELECT geo_id, period, value, status FROM observations WHERE indicator_id = ? AND is_latest = 1"
    params = [indicator_id]
    if geo_id is not None:
        q += " AND geo_id = ?"
        params.append(geo_id)
    rows = conn.execute(q + " ORDER BY geo_id, period", params).fetchall()
    conn.close()
    return rows


def _one_quarter_fixture():
    """One version each, 2026-Q1 leases and 2025-Q4 transactions, one commune
    (Antwerp, 11002) each -- the real measured values from the handoff."""
    atom_bytes = {
        "leases": _atom_xml(
            [("https://example.test/leases-2026q1.zip", "2026-03-31T00:00:00Z", 111)]
        ),
        "transactions": _atom_xml(
            [("https://example.test/tx-2025q4.zip", "2025-12-31T00:00:00Z", 222)]
        ),
    }
    version_bytes = {
        ("leases", "2026-Q1"): _leases_csv([_leases_row("11002", "3297", "895", "70")]),
        ("transactions", "2025-Q4"): _tx_csv([_tx_row("11002", "5075")]),
    }
    return atom_bytes, version_bytes


# --- reference rows ----------------------------------------------------------


def test_reference_rows_only_needs_no_network(db, state_path):
    read, written = sync_spf_agdp.sync(db, reference_rows_only=True, state_path=state_path)
    assert (read, written) == (0, 0)
    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT indicator_id, is_additive, aggregation_method FROM indicators "
        "WHERE indicator_id IN ('MUN_LEASES_NEW_HOUSING', 'MUN_LEASE_RENT_MEDIAN_HOUSING', "
        "'MUN_LEASE_CHARGES_MEDIAN_HOUSING', 'MUN_PROPERTY_SALES', 'MUN_OWNER_OCCUPIERS', "
        "'MUN_PARCELS_OWNED', 'MUN_OWNERSHIP_DURATION_MEDIAN', 'MUN_OWNERSHIP_ROTATION_MEAN') "
        "ORDER BY indicator_id"
    ).fetchall()
    conn.close()
    assert rows == [
        ("MUN_LEASES_NEW_HOUSING", 1, "sum"),
        ("MUN_LEASE_CHARGES_MEDIAN_HOUSING", 0, "not_applicable"),
        ("MUN_LEASE_RENT_MEDIAN_HOUSING", 0, "not_applicable"),
        ("MUN_OWNERSHIP_DURATION_MEDIAN", 0, "not_applicable"),
        ("MUN_OWNERSHIP_ROTATION_MEAN", 0, "not_applicable"),
        ("MUN_OWNER_OCCUPIERS", 1, "sum"),
        ("MUN_PARCELS_OWNED", 1, "sum"),
        ("MUN_PROPERTY_SALES", 1, "sum"),
    ]
    assert not state_path.exists()  # reference-rows-only never touches state


# --- measured real values -----------------------------------------------------


def test_antwerp_2026q1_leases_real_values(db, state_path):
    atom_bytes, version_bytes = _one_quarter_fixture()
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_LEASES_NEW_HOUSING", "be:mun:11002") == [
        ("be:mun:11002", "2026-Q1", 3297.0, "final")
    ]
    assert _observations(db, "MUN_LEASE_RENT_MEDIAN_HOUSING", "be:mun:11002") == [
        ("be:mun:11002", "2026-Q1", 895.0, "final")
    ]
    assert _observations(db, "MUN_LEASE_CHARGES_MEDIAN_HOUSING", "be:mun:11002") == [
        ("be:mun:11002", "2026-Q1", 70.0, "final")
    ]


def test_antwerp_2025q4_and_2016q4_property_sales_real_values(db, state_path):
    atom_bytes = {
        "leases": _atom_xml([("https://example.test/l.zip", "2026-03-31T00:00:00Z", 1)]),
        "transactions": _atom_xml(
            [
                ("https://example.test/tx2016q4.zip", "2016-12-31T00:00:00Z", 1),
                ("https://example.test/tx2025q4.zip", "2025-12-31T00:00:00Z", 2),
            ]
        ),
    }
    version_bytes = {
        ("leases", "2026-Q1"): _leases_csv([_leases_row("11002", "1", "", "")]),
        ("transactions", "2016-Q4"): _tx_csv([_tx_row("11002", "3835")]),
        ("transactions", "2025-Q4"): _tx_csv([_tx_row("11002", "5075")]),
    }
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    rows = _observations(db, "MUN_PROPERTY_SALES", "be:mun:11002")
    assert ("be:mun:11002", "2016-Q4", 3835.0, "final") in rows
    assert ("be:mun:11002", "2025-Q4", 5075.0, "final") in rows


def test_pajottegem_2026q1_real_values(db, state_path):
    """23106 (Pajottegem) exists from 2025-01-01 -- resolves fine for a
    2026-Q1 row."""
    atom_bytes, version_bytes = _one_quarter_fixture()
    version_bytes[("leases", "2026-Q1")] = _leases_csv(
        [_leases_row("11002", "3297", "895", "70"), _leases_row("23106", "56", "950", "65")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_LEASES_NEW_HOUSING", "be:mun:23106") == [
        ("be:mun:23106", "2026-Q1", 56.0, "final")
    ]
    assert _observations(db, "MUN_LEASE_RENT_MEDIAN_HOUSING", "be:mun:23106") == [
        ("be:mun:23106", "2026-Q1", 950.0, "final")
    ]


# --- count 1-4 suppressed, never zero -----------------------------------------


def test_count_one_to_four_is_suppressed_never_zero(db, state_path):
    atom_bytes, version_bytes = _one_quarter_fixture()
    version_bytes[("leases", "2026-Q1")] = _leases_csv([_leases_row("11002", "3", "", "")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    rows = _observations(db, "MUN_LEASE_RENT_MEDIAN_HOUSING", "be:mun:11002")
    assert rows == [("be:mun:11002", "2026-Q1", None, "suppressed")]


# --- geography: period-correct, 44001/23106 windows ---------------------------


def test_44001_resolves_in_2018q4_and_is_absent_in_2019q1(db, state_path):
    atom_bytes = {
        "leases": _atom_xml(
            [
                ("https://example.test/l2018q4.zip", "2018-12-31T00:00:00Z", 1),
                ("https://example.test/l2019q1.zip", "2019-03-31T00:00:00Z", 2),
            ]
        ),
        "transactions": _atom_xml([("https://example.test/t.zip", "2025-12-31T00:00:00Z", 1)]),
    }
    version_bytes = {
        ("leases", "2018-Q4"): _leases_csv([_leases_row("44001", "1", "", "")]),
        ("leases", "2019-Q1"): _leases_csv([_leases_row("11002", "1", "", "")]),
        ("transactions", "2025-Q4"): _tx_csv([_tx_row("11002", "1")]),
    }
    read, written = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    rows = _observations(db, "MUN_LEASES_NEW_HOUSING", "be:mun:44001")
    assert rows == [("be:mun:44001", "2018-Q4", 1.0, "final")]


def test_44001_in_2019q1_file_would_refuse_the_whole_run(db, state_path):
    """44001 does not resolve for 2019-Q1 (absent from the geography table
    by then) -- a file that still carried it in that quarter must refuse the
    whole run, never load a partial series."""
    atom_bytes = {
        "leases": _atom_xml([("https://example.test/l2019q1.zip", "2019-03-31T00:00:00Z", 1)]),
        "transactions": _atom_xml([("https://example.test/t.zip", "2025-12-31T00:00:00Z", 1)]),
    }
    version_bytes = {
        ("leases", "2019-Q1"): _leases_csv([_leases_row("44001", "1", "", "")]),
        ("transactions", "2025-Q4"): _tx_csv([_tx_row("11002", "1")]),
    }
    with pytest.raises(SystemExit, match="resolved to no geography row"):
        sync_spf_agdp.sync(
            db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
        )


def test_23106_absent_2024q4_present_2025q1(db, state_path):
    atom_bytes = {
        "leases": _atom_xml([("https://example.test/l.zip", "2025-03-31T00:00:00Z", 1)]),
        "transactions": _atom_xml([("https://example.test/t.zip", "2025-12-31T00:00:00Z", 1)]),
    }
    version_bytes = {
        ("leases", "2025-Q1"): _leases_csv([_leases_row("23106", "10", "900", "65")]),
        ("transactions", "2025-Q4"): _tx_csv([_tx_row("11002", "1")]),
    }
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    rows = _observations(db, "MUN_LEASES_NEW_HOUSING", "be:mun:23106")
    assert rows == [("be:mun:23106", "2025-Q1", 10.0, "final")]

    # A 2024-Q4 row for the same code must fail to resolve (23106 does not
    # exist before 2025-01-01).
    atom_bytes_2024 = {
        "leases": _atom_xml([("https://example.test/l2024q4.zip", "2024-12-31T00:00:00Z", 2)]),
        "transactions": _atom_xml([("https://example.test/t.zip", "2025-12-31T00:00:00Z", 1)]),
    }
    version_bytes_2024 = {
        ("leases", "2024-Q4"): _leases_csv([_leases_row("23106", "1", "", "")]),
        ("transactions", "2025-Q4"): _tx_csv([_tx_row("11002", "1")]),
    }
    with pytest.raises(SystemExit, match="resolved to no geography row"):
        sync_spf_agdp.sync(
            db, atom_bytes=atom_bytes_2024, version_bytes=version_bytes_2024, state_path=state_path
        )


# --- incremental fetch ---------------------------------------------------------


def test_second_run_unchanged_atom_reads_no_zip_and_adds_no_rows(db, state_path):
    atom_bytes, version_bytes = _one_quarter_fixture()
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    conn = sqlite3.connect(str(db))
    first_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    conn.close()

    # Second run: same ATOM feed, and NO version_bytes provided at all -- if
    # the incremental logic tried to re-read a zip it would KeyError.
    read, written = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes={}, state_path=state_path
    )
    assert (read, written) == (0, 0)
    conn = sqlite3.connect(str(db))
    second_count = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    conn.close()
    assert second_count == first_count


def test_changed_length_rereads_that_one_version(db, state_path):
    atom_bytes, version_bytes = _one_quarter_fixture()
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )

    # SPF republishes 2026-Q1 leases with a revised RentsNumber, signalled
    # ONLY by a changed ATOM `length` (href unchanged).
    atom_bytes_v2 = {
        "leases": _atom_xml(
            [("https://example.test/leases-2026q1.zip", "2026-03-31T00:00:00Z", 999)]
        ),
        "transactions": atom_bytes["transactions"],
    }
    version_bytes_v2 = {
        ("leases", "2026-Q1"): _leases_csv([_leases_row("11002", "4000", "900", "75")]),
    }
    read, written = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes_v2, version_bytes=version_bytes_v2, state_path=state_path
    )
    assert written > 0
    rows = _observations(db, "MUN_LEASES_NEW_HOUSING", "be:mun:11002")
    assert rows == [("be:mun:11002", "2026-Q1", 4000.0, "final")]


def test_state_file_records_length_and_is_read_back(db, state_path):
    atom_bytes, version_bytes = _one_quarter_fixture()
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["leases"]["2026-Q1"]["length"] == 111
    assert state["transactions"]["2025-Q4"]["length"] == 222


# --- idempotence and determinism ----------------------------------------------


def test_running_twice_writes_no_new_vintage_the_second_time(db, state_path):
    atom_bytes, version_bytes = _one_quarter_fixture()
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    read2, written2 = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes={}, state_path=state_path
    )
    assert written2 == 0


def test_output_is_byte_identical_across_two_runs_from_scratch(tmp_path):
    """Same inputs, two fresh databases and state files -- the state file
    itself must serialize identically (CLAUDE.md rule 35)."""
    atom_bytes, version_bytes = _one_quarter_fixture()

    db1 = tmp_path / "a.db"
    migrate.run(db1, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db1, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    state1 = tmp_path / "state_a.json"
    sync_spf_agdp.sync(db1, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state1)

    db2 = tmp_path / "b.db"
    migrate.run(db2, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db2, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    state2 = tmp_path / "state_b.json"
    sync_spf_agdp.sync(db2, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state2)

    # loaded_at timestamps differ (wall clock) -- strip those, compare the
    # rest (dataset/quarter/length) for byte-identical structure.
    s1 = json.loads(state1.read_text(encoding="utf-8"))
    s2 = json.loads(state2.read_text(encoding="utf-8"))
    for dataset in s1:
        for quarter in s1[dataset]:
            assert s1[dataset][quarter]["length"] == s2[dataset][quarter]["length"]

    conn1 = sqlite3.connect(str(db1))
    conn2 = sqlite3.connect(str(db2))
    rows1 = conn1.execute(
        "SELECT indicator_id, geo_id, period, value, status FROM observations ORDER BY 1,2,3"
    ).fetchall()
    rows2 = conn2.execute(
        "SELECT indicator_id, geo_id, period, value, status FROM observations ORDER BY 1,2,3"
    ).fetchall()
    conn1.close()
    conn2.close()
    assert rows1 == rows2


# --- Wave 5 lot A: annual datasets (owner_occupants, property_dynamics) ------


def _annual_atom_xml(links: list[tuple[str, str, int]]) -> bytes:
    body = "\n".join(
        f'<link rel="section" href="{href}" time="{time}" length="{length}"/>'
        for href, time, length in links
    )
    return (
        '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">' f"{body}</feed>"
    ).encode()


def _annual_fixture(
    *,
    owner_occupants=None,
    property_dynamics=None,
    land_use=None,
    building_condition=None,
    tax_exemptions=None,
    notifications=None,
    concentration=None,
):
    """atom_bytes/version_bytes covering only the annual dataset(s) under
    test -- the Wave 4 quarterly pair is left out of `atom_bytes` entirely,
    which `sync()`'s fixture-mode "missing dataset key" path (added
    alongside Wave 5 lot A) treats as zero new/changed versions, the same as
    Wave 4's own tests do for the datasets they don't exercise. A kwarg left
    as None is likewise left out of `atom_bytes`, not passed as an empty
    feed (an ATOM feed with zero <link rel="section"> entries is a schema
    error, not "nothing new" -- parse_atom_feed's own guard, unchanged by
    this handoff). Wave 5 lot B adds land_use/building_condition/
    tax_exemptions alongside lot A's owner_occupants/property_dynamics; Wave
    5 lot C adds notifications (frequency="AY", a calendar-year-end ATOM
    timestamp -- `_notif_atom_xml` below, not `_annual_atom_xml`) and
    concentration (frequency="A", ordinary 1-January timestamp)."""
    atom_bytes = {}
    version_bytes = {}
    if owner_occupants is not None:
        atom_bytes["owner_occupants"] = _annual_atom_xml(owner_occupants)
    if property_dynamics is not None:
        atom_bytes["property_dynamics"] = _annual_atom_xml(property_dynamics)
    if land_use is not None:
        atom_bytes["land_use"] = _annual_atom_xml(land_use)
    if building_condition is not None:
        atom_bytes["building_condition"] = _annual_atom_xml(building_condition)
    if tax_exemptions is not None:
        atom_bytes["tax_exemptions"] = _annual_atom_xml(tax_exemptions)
    if notifications is not None:
        atom_bytes["notifications"] = _annual_atom_xml(notifications)
    if concentration is not None:
        atom_bytes["concentration"] = _annual_atom_xml(concentration)
    return atom_bytes, version_bytes


# --- Wave 5 lot B fixtures: land use / building condition / tax exemptions ---

_LANDUSE_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;ParcelNature;ParcelsNumber;"
    "TotalCadastralIncome;TaxableCadastralIncome;TaxExemptCadastralIncome"
)
_BUILDING_HEADER = "NISCode;NameFre;NameDut;NameGer;ParcelNature;ParcelsNumber;CentralHeating"
_EXEMPT_HEADER = "NISCode;NameFre;NameDut;NameGer;ExemptionType;ParcelsNumber"


def _landuse_row(nis, parcels, total_ci, taxable_ci, exempt_ci="0"):
    return f"{nis};Commune;Commune;Commune;TOTAL;{parcels};{total_ci};{taxable_ci};{exempt_ci}"


def _landuse_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_LANDUSE_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def _building_row(nis, parcels, heating):
    return f"{nis};Commune;Commune;Commune;TOTAL;{parcels};{heating}"


def _building_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_BUILDING_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def _exempt_row(nis, parcels):
    return f"{nis};Commune;Commune;Commune;TOTAL;{parcels}"


def _exempt_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_EXEMPT_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_lot_b_reference_rows_only_needs_no_network(db, state_path):
    read, written = sync_spf_agdp.sync(db, reference_rows_only=True, state_path=state_path)
    assert (read, written) == (0, 0)
    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT indicator_id, is_additive, aggregation_method FROM indicators "
        "WHERE indicator_id IN ('MUN_CADASTRAL_PARCELS_TOTAL', 'MUN_CADASTRAL_INCOME_TOTAL', "
        "'MUN_CADASTRAL_INCOME_TAXABLE', 'MUN_BUILDINGS_TOTAL', "
        "'MUN_BUILDINGS_CENTRAL_HEATING', 'MUN_PARCELS_TAX_EXEMPT') ORDER BY indicator_id"
    ).fetchall()
    conn.close()
    assert rows == [
        ("MUN_BUILDINGS_CENTRAL_HEATING", 1, "sum"),
        ("MUN_BUILDINGS_TOTAL", 1, "sum"),
        ("MUN_CADASTRAL_INCOME_TAXABLE", 1, "sum"),
        ("MUN_CADASTRAL_INCOME_TOTAL", 1, "sum"),
        ("MUN_CADASTRAL_PARCELS_TOTAL", 1, "sum"),
        ("MUN_PARCELS_TAX_EXEMPT", 1, "sum"),
    ]
    assert not state_path.exists()


def test_landuse_2026_real_values_four_communes(db, state_path):
    """Hand-computed values from the handoff, land use 2026 TOTAL."""
    atom_bytes, version_bytes = _annual_fixture(
        land_use=[("https://example.test/lu2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("land_use", "2026")] = _landuse_csv(
        [
            _landuse_row("11001", "11540", "15431671", "13908911"),
            _landuse_row("44083", "46948", "30809990", "27168364"),
            _landuse_row("23106", "38624", "14644408", "13718983"),
            _landuse_row("82039", "55633", "14059868", "11787166"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    parcels = {g: v for g, p, v, s in _observations(db, "MUN_CADASTRAL_PARCELS_TOTAL")}
    total_ci = {g: v for g, p, v, s in _observations(db, "MUN_CADASTRAL_INCOME_TOTAL")}
    taxable_ci = {g: v for g, p, v, s in _observations(db, "MUN_CADASTRAL_INCOME_TAXABLE")}
    assert parcels["be:mun:11001"] == 11540.0
    assert total_ci["be:mun:11001"] == 15431671.0
    assert taxable_ci["be:mun:11001"] == 13908911.0
    assert parcels["be:mun:44083"] == 46948.0
    assert total_ci["be:mun:44083"] == 30809990.0
    assert taxable_ci["be:mun:44083"] == 27168364.0
    assert parcels["be:mun:23106"] == 38624.0
    assert total_ci["be:mun:23106"] == 14644408.0
    assert taxable_ci["be:mun:23106"] == 13718983.0
    assert parcels["be:mun:82039"] == 55633.0
    assert total_ci["be:mun:82039"] == 14059868.0
    assert taxable_ci["be:mun:82039"] == 11787166.0


def test_landuse_2017_real_values(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        land_use=[("https://example.test/lu2017.zip", "2017-01-01T00:00:00Z", 1)]
    )
    version_bytes[("land_use", "2017")] = _landuse_csv(
        [
            _landuse_row("11001", "9925", "14338902", "12000000"),
            _landuse_row("44001", "23424", "16144345", "14000000"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_CADASTRAL_PARCELS_TOTAL", "be:mun:11001") == [
        ("be:mun:11001", "2017", 9925.0, "final")
    ]
    assert _observations(db, "MUN_CADASTRAL_INCOME_TOTAL", "be:mun:11001") == [
        ("be:mun:11001", "2017", 14338902.0, "final")
    ]
    assert _observations(db, "MUN_CADASTRAL_PARCELS_TOTAL", "be:mun:44001") == [
        ("be:mun:44001", "2017", 23424.0, "final")
    ]
    assert _observations(db, "MUN_CADASTRAL_INCOME_TOTAL", "be:mun:44001") == [
        ("be:mun:44001", "2017", 16144345.0, "final")
    ]


def test_building_2026_real_values_four_communes(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        building_condition=[("https://example.test/bc2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("building_condition", "2026")] = _building_csv(
        [
            _building_row("11001", "9553", "6419"),
            _building_row("44083", "29756", "18711"),
            _building_row("23106", "13798", "9216"),
            _building_row("82039", "13716", "8505"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    total = {g: v for g, p, v, s in _observations(db, "MUN_BUILDINGS_TOTAL")}
    heating = {g: v for g, p, v, s in _observations(db, "MUN_BUILDINGS_CENTRAL_HEATING")}
    assert total["be:mun:11001"] == 9553.0
    assert heating["be:mun:11001"] == 6419.0
    assert total["be:mun:44083"] == 29756.0
    assert heating["be:mun:44083"] == 18711.0
    assert total["be:mun:23106"] == 13798.0
    assert heating["be:mun:23106"] == 9216.0
    assert total["be:mun:82039"] == 13716.0
    assert heating["be:mun:82039"] == 8505.0


def test_building_2017_real_values(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        building_condition=[("https://example.test/bc2017.zip", "2017-01-01T00:00:00Z", 1)]
    )
    version_bytes[("building_condition", "2017")] = _building_csv(
        [
            _building_row("11001", "8011", "5636"),
            _building_row("44001", "11937", "7211"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_BUILDINGS_TOTAL", "be:mun:11001") == [
        ("be:mun:11001", "2017", 8011.0, "final")
    ]
    assert _observations(db, "MUN_BUILDINGS_CENTRAL_HEATING", "be:mun:11001") == [
        ("be:mun:11001", "2017", 5636.0, "final")
    ]
    assert _observations(db, "MUN_BUILDINGS_TOTAL", "be:mun:44001") == [
        ("be:mun:44001", "2017", 11937.0, "final")
    ]
    assert _observations(db, "MUN_BUILDINGS_CENTRAL_HEATING", "be:mun:44001") == [
        ("be:mun:44001", "2017", 7211.0, "final")
    ]


def test_exempt_2026_real_values_four_communes(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        tax_exemptions=[("https://example.test/te2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("tax_exemptions", "2026")] = _exempt_csv(
        [
            _exempt_row("11001", "216"),
            _exempt_row("44083", "1210"),
            _exempt_row("23106", "775"),
            _exempt_row("82039", "2638"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    exempt = {g: v for g, p, v, s in _observations(db, "MUN_PARCELS_TAX_EXEMPT")}
    assert exempt["be:mun:11001"] == 216.0
    assert exempt["be:mun:44083"] == 1210.0
    assert exempt["be:mun:23106"] == 775.0
    assert exempt["be:mun:82039"] == 2638.0


def test_exempt_2017_real_zero_is_final_not_na(db, state_path):
    """44001 2017: 0 -- a real measured zero, final, not na (handoff)."""
    atom_bytes, version_bytes = _annual_fixture(
        tax_exemptions=[("https://example.test/te2017.zip", "2017-01-01T00:00:00Z", 1)]
    )
    version_bytes[("tax_exemptions", "2017")] = _exempt_csv(
        [_exempt_row("11001", "206"), _exempt_row("44001", "0")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_PARCELS_TAX_EXEMPT", "be:mun:11001") == [
        ("be:mun:11001", "2017", 206.0, "final")
    ]
    assert _observations(db, "MUN_PARCELS_TAX_EXEMPT", "be:mun:44001") == [
        ("be:mun:44001", "2017", 0.0, "final")
    ]


def test_lot_b_period_correct_geography_four_codes(db, state_path):
    """44083 present 2026 absent 2017; 23106 present 2026 absent 2017;
    82039 present 2026 absent 2017; 44001 absent 2026 present 2017 (handoff,
    verified against the real land-use file)."""
    atom_2026, version_2026 = _annual_fixture(
        land_use=[("https://example.test/lu2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_2026[("land_use", "2026")] = _landuse_csv(
        [
            _landuse_row("44083", "46948", "30809990", "27168364"),
            _landuse_row("23106", "38624", "14644408", "13718983"),
            _landuse_row("82039", "55633", "14059868", "11787166"),
        ]
    )
    sync_spf_agdp.sync(db, atom_bytes=atom_2026, version_bytes=version_2026, state_path=state_path)
    for nis in ("44083", "23106", "82039"):
        assert _observations(db, "MUN_CADASTRAL_PARCELS_TOTAL", f"be:mun:{nis}")

    atom_2017, version_2017 = _annual_fixture(
        land_use=[("https://example.test/lu2017.zip", "2017-01-01T00:00:00Z", 2)]
    )
    version_2017[("land_use", "2017")] = _landuse_csv([_landuse_row("44001", "23424", "1", "1")])
    sync_spf_agdp.sync(db, atom_bytes=atom_2017, version_bytes=version_2017, state_path=state_path)
    assert _observations(db, "MUN_CADASTRAL_PARCELS_TOTAL", "be:mun:44001") == [
        ("be:mun:44001", "2017", 23424.0, "final")
    ]

    # 44083/23106/82039 absent from a 2017 file would refuse the run (handoff
    # "44083 present 2026 absent 2017" etc.) -- exercised here by the mirror
    # image: an unresolved code refuses the whole run.
    atom_bad, version_bad = _annual_fixture(
        land_use=[("https://example.test/lu2017b.zip", "2017-01-01T00:00:00Z", 3)]
    )
    version_bad[("land_use", "2017")] = _landuse_csv([_landuse_row("44083", "1", "1", "1")])
    with pytest.raises(SystemExit, match="resolved to no geography row"):
        sync_spf_agdp.sync(
            db, atom_bytes=atom_bad, version_bytes=version_bad, state_path=state_path
        )


def test_no_fictitious_column_or_non_numeric_nis_in_lot_b_row_filter():
    """Lot B configs (unlike Owner Occupants) declare no row_filter -- there
    is no Fictious column and no N/A NIS placeholder in any of the three
    datasets (handoff). Asserted directly on the configs, not just by
    absence of a crash, since a silently-added row_filter would still pass
    every other test here."""
    from src.fetchers.spf_agdp import BUILDING_CONDITION, LAND_USE, TAX_EXEMPTIONS

    for config in (LAND_USE, BUILDING_CONDITION, TAX_EXEMPTIONS):
        assert config.row_filter is None
        assert "Fictious" not in config.required_columns


def test_lot_b_second_run_unchanged_reads_no_zip_and_adds_no_rows(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        tax_exemptions=[("https://example.test/te2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("tax_exemptions", "2026")] = _exempt_csv([_exempt_row("11001", "216")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    read2, written2 = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes={}, state_path=state_path
    )
    assert (read2, written2) == (0, 0)


def test_lot_b_changed_length_rereads_that_one_version(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        tax_exemptions=[("https://example.test/te2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("tax_exemptions", "2026")] = _exempt_csv([_exempt_row("11001", "216")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )

    atom_bytes2, version_bytes2 = _annual_fixture(
        tax_exemptions=[("https://example.test/te2026.zip", "2026-01-01T00:00:00Z", 2)]
    )
    version_bytes2[("tax_exemptions", "2026")] = _exempt_csv([_exempt_row("11001", "220")])
    read2, written2 = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes2, version_bytes=version_bytes2, state_path=state_path
    )
    assert read2 == 1
    assert written2 == 1
    assert _observations(db, "MUN_PARCELS_TAX_EXEMPT", "be:mun:11001") == [
        ("be:mun:11001", "2026", 220.0, "final")
    ]


def test_owner_occupiers_2026_real_values_five_communes(db, state_path):
    """Hand-computed values from the handoff, Owner Occupants 2026
    (Total, OCCUPANTPUPES)."""
    atom_bytes, version_bytes = _annual_fixture(
        owner_occupants=[("https://example.test/oo2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("owner_occupants", "2026")] = _owner_csv(
        [
            _owner_row("11001", "7722"),
            _owner_row("44083", "21377"),
            _owner_row("23106", "11764"),
            _owner_row("82039", "7764"),
            _owner_row("21004", "34470"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    expected = {
        "be:mun:11001": 7722.0,
        "be:mun:44083": 21377.0,
        "be:mun:23106": 11764.0,
        "be:mun:82039": 7764.0,
        "be:mun:21004": 34470.0,
    }
    rows = _observations(db, "MUN_OWNER_OCCUPIERS")
    by_geo = {geo_id: value for geo_id, period, value, status in rows}
    for geo_id, value in expected.items():
        assert by_geo[geo_id] == value


def test_owner_occupiers_skips_fictitious_row_end_to_end(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        owner_occupants=[("https://example.test/oo2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("owner_occupants", "2026")] = _owner_csv(
        [_owner_row("N/A", "999999", fictious="1"), _owner_row("11001", "7722")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    rows = _observations(db, "MUN_OWNER_OCCUPIERS")
    assert rows == [("be:mun:11001", "2026", 7722.0, "final")]


def test_property_dynamics_2026_real_values_five_communes(db, state_path):
    """Hand-computed values from the handoff, Dynamics 2026 TOTAL."""
    atom_bytes, version_bytes = _annual_fixture(
        property_dynamics=[("https://example.test/pd2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("property_dynamics", "2026")] = _dynamics_csv(
        [
            _dynamics_row("11001", "11538", "9.0102669405", "15.159677398"),
            _dynamics_row("44083", "46920", "7.8056125941"),
            _dynamics_row("23106", "38623", "10.220396988"),
            _dynamics_row("82039", "55633", "11.085557837"),
            _dynamics_row("21004", "122443", "8.0492813142"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    parcels = {g: v for g, p, v, s in _observations(db, "MUN_PARCELS_OWNED")}
    medians = {g: v for g, p, v, s in _observations(db, "MUN_OWNERSHIP_DURATION_MEDIAN")}
    assert parcels["be:mun:11001"] == 11538.0
    assert medians["be:mun:11001"] == pytest.approx(9.0102669405)
    assert parcels["be:mun:44083"] == 46920.0
    assert medians["be:mun:44083"] == pytest.approx(7.8056125941)
    assert parcels["be:mun:23106"] == 38623.0
    assert medians["be:mun:23106"] == pytest.approx(10.220396988)
    assert parcels["be:mun:82039"] == 55633.0
    assert medians["be:mun:82039"] == pytest.approx(11.085557837)
    assert parcels["be:mun:21004"] == 122443.0
    assert medians["be:mun:21004"] == pytest.approx(8.0492813142)
    rotation = {g: v for g, p, v, s in _observations(db, "MUN_OWNERSHIP_ROTATION_MEAN")}
    assert rotation["be:mun:11001"] == pytest.approx(15.159677398)


def test_property_dynamics_zero_parcels_writes_na_never_zero_duration(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        property_dynamics=[("https://example.test/pd.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("property_dynamics", "2026")] = _dynamics_csv(
        [_dynamics_row("11001", "0", "0", "0")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    rows = _observations(db, "MUN_OWNERSHIP_DURATION_MEDIAN", "be:mun:11001")
    assert rows == [("be:mun:11001", "2026", None, "na")]
    rows_rot = _observations(db, "MUN_OWNERSHIP_ROTATION_MEAN", "be:mun:11001")
    assert rows_rot == [("be:mun:11001", "2026", None, "na")]
    rows_parcels = _observations(db, "MUN_PARCELS_OWNED", "be:mun:11001")
    assert rows_parcels == [("be:mun:11001", "2026", 0.0, "final")]


# --- geography: annual, period-correct at 1 January ---------------------------


def test_23106_resolves_2025_raises_2024_annual(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        owner_occupants=[("https://example.test/oo2025.zip", "2025-01-01T00:00:00Z", 1)]
    )
    version_bytes[("owner_occupants", "2025")] = _owner_csv([_owner_row("23106", "100")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    rows = _observations(db, "MUN_OWNER_OCCUPIERS", "be:mun:23106")
    assert rows == [("be:mun:23106", "2025", 100.0, "final")]

    atom_bytes_2024, version_bytes_2024 = _annual_fixture(
        owner_occupants=[("https://example.test/oo2024.zip", "2024-01-01T00:00:00Z", 2)]
    )
    version_bytes_2024[("owner_occupants", "2024")] = _owner_csv([_owner_row("23106", "1")])
    with pytest.raises(SystemExit, match="resolved to no geography row"):
        sync_spf_agdp.sync(
            db, atom_bytes=atom_bytes_2024, version_bytes=version_bytes_2024, state_path=state_path
        )


def test_82039_resolves_2025_not_2024(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        owner_occupants=[("https://example.test/oo2025.zip", "2025-01-01T00:00:00Z", 1)]
    )
    version_bytes[("owner_occupants", "2025")] = _owner_csv([_owner_row("82039", "50")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    rows = _observations(db, "MUN_OWNER_OCCUPIERS", "be:mun:82039")
    assert rows == [("be:mun:82039", "2025", 50.0, "final")]


def test_23023_resolves_2024_not_2025(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        property_dynamics=[("https://example.test/pd2024.zip", "2024-01-01T00:00:00Z", 1)]
    )
    version_bytes[("property_dynamics", "2024")] = _dynamics_csv(
        [_dynamics_row("23023", "11614", "9.0")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    rows = _observations(db, "MUN_PARCELS_OWNED", "be:mun:23023")
    assert rows == [("be:mun:23023", "2024", 11614.0, "final")]


def test_44011_resolves_2018_44083_resolves_2020(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        owner_occupants=[
            ("https://example.test/oo2018.zip", "2018-01-01T00:00:00Z", 1),
            ("https://example.test/oo2020.zip", "2020-01-01T00:00:00Z", 2),
        ]
    )
    version_bytes[("owner_occupants", "2018")] = _owner_csv([_owner_row("44011", "100")])
    version_bytes[("owner_occupants", "2020")] = _owner_csv([_owner_row("44083", "200")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_OWNER_OCCUPIERS", "be:mun:44011") == [
        ("be:mun:44011", "2018", 100.0, "final")
    ]
    assert _observations(db, "MUN_OWNER_OCCUPIERS", "be:mun:44083") == [
        ("be:mun:44083", "2020", 200.0, "final")
    ]


# --- annual period bounds derivation -------------------------------------------


def test_annual_period_bounds_are_full_calendar_year(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        owner_occupants=[("https://example.test/oo2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("owner_occupants", "2026")] = _owner_csv([_owner_row("11001", "7722")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT period_start, period_end FROM observations "
        "WHERE indicator_id = 'MUN_OWNER_OCCUPIERS' AND geo_id = 'be:mun:11001' AND is_latest = 1"
    ).fetchone()
    conn.close()
    assert row == ("2026-01-01", "2026-12-31")


# --- incremental fetch, annual datasets ----------------------------------------


def test_annual_second_run_unchanged_reads_no_zip_and_adds_no_rows(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        owner_occupants=[("https://example.test/oo2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("owner_occupants", "2026")] = _owner_csv([_owner_row("11001", "7722")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )

    read2, written2 = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes={}, state_path=state_path
    )
    assert (read2, written2) == (0, 0)


# --- Wave 5 lot C fixtures: notifications / concentration ---------------------

_NOTIF_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;CadastralIncomeFiscalStatus;CadastralIncomeNature;"
    "NotificationMotivationCategory;CadastralIncomeNumber;TotalCadastralIncome;"
    "CadastralIncomeP25;CadastralIncomeP50;CadastralIncomeP75;CadastralIncomeSD"
)
_CONC_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;ParcelNature;Range;ParcelsNumber;"
    "ParcelsCumulatedNumber;HousingsNumber;HousingsCumulatedNumber;"
    "TotalCadastralIncome;CumulatedCadastralIncome"
)


def _notif_row(nis, count, total="", p25="", p50="", p75="", sd=""):
    return f"{nis};Commune;Commune;Commune;TOTAL;TOTAL;TOTAL;{count};{total};{p25};{p50};{p75};{sd}"


def _notif_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_NOTIF_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def _conc_row(nis, cumulated_ci, parcels_cum="1", nature="TOTAL", rng="Range30001", parcels="100"):
    # `parcels` (the top band's OWN ParcelsNumber, >= 5 by default so the
    # real 1-4 suppression tier does not fire) is deliberately independent
    # from `parcels_cum` (ParcelsCumulatedNumber, the running total) --
    # real measured data shows a large parcels_cum can still sit on a
    # suppressed (1-4 parcels) row (see test_spf_agdp_source.py's
    # test_concentration_parcels_1to4_suppresses_never_zero).
    return (
        f"{nis};Commune;Commune;Commune;{nature};{rng};{parcels};{parcels_cum};"
        f"{parcels_cum};{parcels_cum};1000;{cumulated_ci}"
    )


def _conc_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_CONC_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_lot_c_reference_rows_only_needs_no_network(db, state_path):
    read, written = sync_spf_agdp.sync(db, reference_rows_only=True, state_path=state_path)
    assert (read, written) == (0, 0)
    conn = sqlite3.connect(str(db))
    rows = conn.execute(
        "SELECT indicator_id, is_additive, aggregation_method FROM indicators "
        "WHERE indicator_id IN ('MUN_CI_NOTIFICATIONS', 'MUN_CI_NOTIFIED_TOTAL', "
        "'MUN_CI_NOTIFIED_MEDIAN', 'MUN_RESIDENTIAL_PARCELS_CI_TOTAL') ORDER BY indicator_id"
    ).fetchall()
    conn.close()
    assert rows == [
        ("MUN_CI_NOTIFICATIONS", 1, "sum"),
        ("MUN_CI_NOTIFIED_MEDIAN", 0, "not_applicable"),
        ("MUN_CI_NOTIFIED_TOTAL", 1, "sum"),
        ("MUN_RESIDENTIAL_PARCELS_CI_TOTAL", 1, "sum"),
    ]
    assert not state_path.exists()


def test_notifications_2025_real_values_four_communes(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        notifications=[("https://example.test/notif2025.zip", "2025-12-31T00:00:00Z", 1)]
    )
    version_bytes[("notifications", "2025")] = _notif_csv(
        [
            _notif_row("11001", 208, total="773812", p25="400", p50="826", p75="1300", sd="200"),
            _notif_row("44083", 1143, total="3501614", p25="200", p50="444", p75="700", sd="150"),
            _notif_row("23106", 414, total="278705", p25="100", p50="228", p75="400", sd="100"),
            _notif_row("82039", 712, total="479212", p25="5", p50="16", p75="30", sd="10"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    count = {g: v for g, p, v, s in _observations(db, "MUN_CI_NOTIFICATIONS")}
    total = {g: v for g, p, v, s in _observations(db, "MUN_CI_NOTIFIED_TOTAL")}
    median = {g: v for g, p, v, s in _observations(db, "MUN_CI_NOTIFIED_MEDIAN")}
    assert count["be:mun:11001"] == 208.0
    assert total["be:mun:11001"] == 773812.0
    assert median["be:mun:11001"] == 826.0
    assert count["be:mun:44083"] == 1143.0
    assert total["be:mun:44083"] == 3501614.0
    assert median["be:mun:44083"] == 444.0
    assert count["be:mun:23106"] == 414.0
    assert total["be:mun:23106"] == 278705.0
    assert median["be:mun:23106"] == 228.0
    assert count["be:mun:82039"] == 712.0
    assert total["be:mun:82039"] == 479212.0
    assert median["be:mun:82039"] == 16.0


def test_notifications_2025_44001_absent(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        notifications=[("https://example.test/notif2025.zip", "2025-12-31T00:00:00Z", 1)]
    )
    version_bytes[("notifications", "2025")] = _notif_csv(
        [_notif_row("11001", 208, total="773812", p50="826")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_CI_NOTIFICATIONS", "be:mun:44001") == []


def test_notifications_2025_herstappe_count_three_suppressed(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        notifications=[("https://example.test/notif2025.zip", "2025-12-31T00:00:00Z", 1)]
    )
    version_bytes[("notifications", "2025")] = _notif_csv([_notif_row("73028", 3)])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_CI_NOTIFICATIONS", "be:mun:73028") == [
        ("be:mun:73028", "2025", 3.0, "final")
    ]
    assert _observations(db, "MUN_CI_NOTIFIED_TOTAL", "be:mun:73028") == [
        ("be:mun:73028", "2025", None, "suppressed")
    ]
    assert _observations(db, "MUN_CI_NOTIFIED_MEDIAN", "be:mun:73028") == [
        ("be:mun:73028", "2025", None, "suppressed")
    ]


def test_notifications_2016_real_values(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        notifications=[("https://example.test/notif2016.zip", "2016-12-31T00:00:00Z", 1)]
    )
    version_bytes[("notifications", "2016")] = _notif_csv(
        [
            _notif_row("11001", 211, total="908627", p25="400", p50="926", p75="1300", sd="200"),
            _notif_row("44001", 552, total="8707921", p25="500", p50="809", p75="1000", sd="200"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_CI_NOTIFICATIONS", "be:mun:11001") == [
        ("be:mun:11001", "2016", 211.0, "final")
    ]
    assert _observations(db, "MUN_CI_NOTIFIED_TOTAL", "be:mun:11001") == [
        ("be:mun:11001", "2016", 908627.0, "final")
    ]
    assert _observations(db, "MUN_CI_NOTIFIED_MEDIAN", "be:mun:11001") == [
        ("be:mun:11001", "2016", 926.0, "final")
    ]
    assert _observations(db, "MUN_CI_NOTIFICATIONS", "be:mun:44001") == [
        ("be:mun:44001", "2016", 552.0, "final")
    ]
    # 44083/23106/82039 did not exist in 2016 -- absent, not zero.
    assert _observations(db, "MUN_CI_NOTIFICATIONS", "be:mun:44083") == []
    assert _observations(db, "MUN_CI_NOTIFICATIONS", "be:mun:23106") == []
    assert _observations(db, "MUN_CI_NOTIFICATIONS", "be:mun:82039") == []


def test_notifications_period_bounds_are_full_calendar_year(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        notifications=[("https://example.test/notif2025.zip", "2025-12-31T00:00:00Z", 1)]
    )
    version_bytes[("notifications", "2025")] = _notif_csv(
        [_notif_row("11001", 208, total="773812", p50="826")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT period_start, period_end FROM observations "
        "WHERE indicator_id = 'MUN_CI_NOTIFICATIONS' AND geo_id = 'be:mun:11001' AND is_latest = 1"
    ).fetchone()
    conn.close()
    assert row == ("2025-01-01", "2025-12-31")


def test_notifications_second_run_unchanged_reads_no_zip_and_adds_no_rows(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        notifications=[("https://example.test/notif2025.zip", "2025-12-31T00:00:00Z", 1)]
    )
    version_bytes[("notifications", "2025")] = _notif_csv(
        [_notif_row("11001", 208, total="773812", p50="826")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    read2, written2 = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes={}, state_path=state_path
    )
    assert (read2, written2) == (0, 0)


def test_notifications_changed_length_rereads_that_one_version(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        notifications=[("https://example.test/notif2025.zip", "2025-12-31T00:00:00Z", 1)]
    )
    version_bytes[("notifications", "2025")] = _notif_csv(
        [_notif_row("11001", 208, total="773812", p50="826")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )

    atom_bytes_v2, version_bytes_v2 = _annual_fixture(
        notifications=[("https://example.test/notif2025.zip", "2025-12-31T00:00:00Z", 2)]
    )
    version_bytes_v2[("notifications", "2025")] = _notif_csv(
        [_notif_row("11001", 210, total="800000", p50="830")]
    )
    read2, written2 = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes_v2, version_bytes=version_bytes_v2, state_path=state_path
    )
    assert written2 > 0
    assert _observations(db, "MUN_CI_NOTIFICATIONS", "be:mun:11001") == [
        ("be:mun:11001", "2025", 210.0, "final")
    ]


def test_concentration_2026_real_values_four_communes(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        concentration=[("https://example.test/conc2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("concentration", "2026")] = _conc_csv(
        [
            _conc_row("11001", "8428254", parcels_cum="6453"),
            _conc_row("44083", "19155664", parcels_cum="21111"),
            _conc_row("23106", "11298541", parcels_cum="10758"),
            _conc_row("82039", "8466649", parcels_cum="8998"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    ci = {g: v for g, p, v, s in _observations(db, "MUN_RESIDENTIAL_PARCELS_CI_TOTAL")}
    assert ci["be:mun:11001"] == 8428254.0
    assert ci["be:mun:44083"] == 19155664.0
    assert ci["be:mun:23106"] == 11298541.0
    assert ci["be:mun:82039"] == 8466649.0


def test_concentration_2026_herstappe_real_value(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        concentration=[("https://example.test/conc2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("concentration", "2026")] = _conc_csv(
        [_conc_row("73028", "27177", parcels_cum="30")]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_RESIDENTIAL_PARCELS_CI_TOTAL", "be:mun:73028") == [
        ("be:mun:73028", "2026", 27177.0, "final")
    ]


def test_concentration_2011_real_values(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        concentration=[("https://example.test/conc2011.zip", "2011-01-01T00:00:00Z", 1)]
    )
    version_bytes[("concentration", "2011")] = _conc_csv(
        [
            _conc_row("11001", "7473689", parcels_cum="5760"),
            _conc_row("44001", "7127074", parcels_cum="8311"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    assert _observations(db, "MUN_RESIDENTIAL_PARCELS_CI_TOTAL", "be:mun:11001") == [
        ("be:mun:11001", "2011", 7473689.0, "final")
    ]
    assert _observations(db, "MUN_RESIDENTIAL_PARCELS_CI_TOTAL", "be:mun:44001") == [
        ("be:mun:44001", "2011", 7127074.0, "final")
    ]
    # 44083/23106/82039 did not exist in 2011.
    assert _observations(db, "MUN_RESIDENTIAL_PARCELS_CI_TOTAL", "be:mun:44083") == []


def test_concentration_period_bounds_are_full_calendar_year(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        concentration=[("https://example.test/conc2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("concentration", "2026")] = _conc_csv([_conc_row("11001", "8428254")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    conn = sqlite3.connect(str(db))
    row = conn.execute(
        "SELECT period_start, period_end FROM observations "
        "WHERE indicator_id = 'MUN_RESIDENTIAL_PARCELS_CI_TOTAL' AND geo_id = 'be:mun:11001' "
        "AND is_latest = 1"
    ).fetchone()
    conn.close()
    assert row == ("2026-01-01", "2026-12-31")


def test_concentration_second_run_unchanged_reads_no_zip_and_adds_no_rows(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        concentration=[("https://example.test/conc2026.zip", "2026-01-01T00:00:00Z", 1)]
    )
    version_bytes[("concentration", "2026")] = _conc_csv([_conc_row("11001", "8428254")])
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    read2, written2 = sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes={}, state_path=state_path
    )
    assert (read2, written2) == (0, 0)


def test_lot_c_period_correct_geography_five_codes(db, state_path):
    atom_bytes, version_bytes = _annual_fixture(
        notifications=[("https://example.test/notif2025.zip", "2025-12-31T00:00:00Z", 1)],
        concentration=[("https://example.test/conc2026.zip", "2026-01-01T00:00:00Z", 1)],
    )
    version_bytes[("notifications", "2025")] = _notif_csv(
        [
            _notif_row("11001", 208, total="773812", p50="826"),
            _notif_row("44083", 1143, total="3501614", p50="444"),
            _notif_row("23106", 414, total="278705", p50="228"),
            _notif_row("82039", 712, total="479212", p50="16"),
        ]
    )
    version_bytes[("concentration", "2026")] = _conc_csv(
        [
            _conc_row("11001", "8428254"),
            _conc_row("44083", "19155664"),
            _conc_row("23106", "11298541"),
            _conc_row("82039", "8466649"),
        ]
    )
    sync_spf_agdp.sync(
        db, atom_bytes=atom_bytes, version_bytes=version_bytes, state_path=state_path
    )
    for nis in ("11001", "44083", "23106", "82039"):
        assert _observations(db, "MUN_CI_NOTIFICATIONS", f"be:mun:{nis}") != []
        assert _observations(db, "MUN_RESIDENTIAL_PARCELS_CI_TOTAL", f"be:mun:{nis}") != []
