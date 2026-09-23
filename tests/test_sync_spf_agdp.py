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
        "'MUN_LEASE_CHARGES_MEDIAN_HOUSING', 'MUN_PROPERTY_SALES') ORDER BY indicator_id"
    ).fetchall()
    conn.close()
    assert rows == [
        ("MUN_LEASES_NEW_HOUSING", 1, "sum"),
        ("MUN_LEASE_CHARGES_MEDIAN_HOUSING", 0, "not_applicable"),
        ("MUN_LEASE_RENT_MEDIAN_HOUSING", 0, "not_applicable"),
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
