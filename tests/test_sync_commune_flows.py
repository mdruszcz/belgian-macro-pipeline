"""Tests for scripts/sync_commune_flows.py: ATOM discovery (frequency="AY", reused from
src/fetchers/spf_agdp.py), incremental fetch via the flows_state.json length comparison,
the two-member zip read (Municipality wide CSV + OriginTable.csv from one ranged connection),
and the store write -- determinism (CLAUDE.md rule 35) and the byte-identical rebuild.

The per-row computation itself is tests/test_buyer_origin_source.py's job; this file exercises
the sync layer around it with `atom_bytes=`/`version_bytes=` fixtures, no network -- the same
shape tests/test_sync_spf_agdp.py uses for the AGDP leases/transactions sync.
"""

from __future__ import annotations

import json
import re
import sys
import zipfile
from decimal import ROUND_HALF_UP, Decimal
from io import BytesIO
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import load_geography  # noqa: E402
import sync_commune_flows  # noqa: E402

from src.db import migrate  # noqa: E402
from src.flows.store import read_store  # noqa: E402

REAL_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "migrations"
REAL_GEOGRAPHY_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config" / "geography"


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
    return db_path


@pytest.fixture
def state_path(tmp_path):
    return tmp_path / "flows_state.json"


@pytest.fixture
def out_dir(tmp_path):
    return tmp_path / "data"


def _atom_xml(links: list[tuple[str, str, int]]) -> bytes:
    body = "\n".join(
        f'<link rel="section" href="{href}" time="{time}" length="{length}"/>'
        for href, time, length in links
    )
    return (
        '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">' f"{body}</feed>"
    ).encode()


_ORIGIN_TABLE = (
    "BuyerFrom;NISCode;NameFre;NameDut;ISOCode\r\n"
    "BuyerFrom11004;11004;Boechout;Boechout;\r\n"
    "BuyerFrom11002;11002;Anvers;Antwerpen;\r\n"
    "BuyerFrom101;101;Albanie;Albani\xeb;AL\r\n"
    "BuyerFrom000;000;Ind\xe9termin\xe9;Onbepaald;\r\n"
).encode("latin-1")

_MUNICIPALITY_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;ParcelNature;ParcelsNumber;"
    "BuyerFrom11004;BuyerFrom11002;BuyerFrom101;BuyerFrom000"
)


def _municipality_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_MUNICIPALITY_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def _zip_bytes(municipality_csv: bytes, origin_table_csv: bytes) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv", municipality_csv)
        zf.writestr("OriginTable.csv", origin_table_csv)
        zf.writestr("DivisionWideBuyersOriginNaturalPersons_20251231.csv", b"ignored")
    return buf.getvalue()


def _all_municipality_nis(db_path) -> list[str]:
    """Every NIS the completeness check (audit finding 1) requires a destination row for,
    read from the same real geography fixture the `db` fixture loads -- never hardcoded, so
    a future merger changes this test's expectation the same way it changes production's."""
    import sqlite3

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT nis_code FROM geographies WHERE level = 'municipality' "
            "AND valid_from <= '2025-01-01' AND (valid_to IS NULL OR valid_to > '2025-01-01')"
        ).fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


def _complete_fixture(db_path, *, override_rows: dict[str, str] | None = None):
    """A full 565-destination Municipality CSV -- one TOTAL row per real municipality NIS,
    all zero-purchase (0 parcels, all-zero cells) except `override_rows`, which replaces a
    NIS's row wholesale. The completeness check (audit finding 1) requires every destination
    NIS present, so any fixture exercising the sync end-to-end must be complete, not a
    hand-picked subset."""
    override_rows = override_rows or {}
    rows = []
    for nis in _all_municipality_nis(db_path):
        if nis in override_rows:
            rows.append(override_rows[nis])
        else:
            rows.append(f"{nis};Commune;Commune;Commune;TOTAL;0;0;0;0;0")
    municipality = _municipality_csv(rows)
    return _zip_bytes(municipality, _ORIGIN_TABLE)


def _one_destination_fixture(db_path):
    return _complete_fixture(
        db_path,
        override_rows={"11004": "11004;Boechout;Boechout;Boechout;TOTAL;10;5;3;1;1"},
    )


def test_atom_frequency_is_ay_calendar_year_end():
    """The buyer-origin ATOM feed publishes at 31 December (ADR/spec: 'AY' mode) -- reusing
    src/fetchers/spf_agdp.py's parse_atom_feed with frequency='AY' must reject a 1-January
    timestamp exactly as every other AY dataset does."""
    from src.fetchers.spf_agdp import AgdpSchemaError, parse_atom_feed

    bad_xml = _atom_xml([("https://example.test/v1.zip", "2025-01-01T00:00:00Z", 100)])
    with pytest.raises(AgdpSchemaError):
        parse_atom_feed(bad_xml, dataset_label="test", frequency="AY")


def test_first_run_loads_latest_version_and_writes_store(db, state_path, out_dir):
    atom_bytes = _atom_xml(
        [
            ("https://example.test/2024.zip", "2024-12-31T00:00:00Z", 100),
            ("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200),
        ]
    )
    zip_2025 = _one_destination_fixture(db)
    with zipfile.ZipFile(BytesIO(zip_2025)) as zf:
        municipality = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
        origin = zf.read("OriginTable.csv")
    version_bytes = {"2025": (municipality, origin)}

    summary = sync_commune_flows.sync(
        db,
        out_dir=out_dir,
        state_path=state_path,
        atom_bytes=atom_bytes,
        version_bytes=version_bytes,
    )
    expected_count = len(_all_municipality_nis(db))
    assert summary == {"loaded": True, "period": "2025", "destinations": expected_count}

    path = out_dir / "flows" / "buyer_origin_2025.json"
    assert path.is_file()
    document = read_store(path)
    assert document["period"] == "2025"
    assert document["dataset"] == "buyer_origin"
    assert len(document["destinations"]) == expected_count
    boechout = next(d for d in document["destinations"] if d["dest_nis"] == "11004")
    assert boechout["state"] == "final"


def test_latest_year_only_2024_never_loaded(db, state_path, out_dir):
    """The maintainer's decision (2026-09-24): latest year only. Even though 2024 is a real
    version in the feed, only 2025 (the latest) is ever fetched -- version_bytes deliberately
    has no entry for 2024, so fetching it would KeyError."""
    atom_bytes = _atom_xml(
        [
            ("https://example.test/2024.zip", "2024-12-31T00:00:00Z", 100),
            ("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200),
        ]
    )
    zip_2025 = _one_destination_fixture(db)
    with zipfile.ZipFile(BytesIO(zip_2025)) as zf:
        municipality = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
        origin = zf.read("OriginTable.csv")
    version_bytes = {"2025": (municipality, origin)}  # no "2024" entry -- must never be touched

    summary = sync_commune_flows.sync(
        db,
        out_dir=out_dir,
        state_path=state_path,
        atom_bytes=atom_bytes,
        version_bytes=version_bytes,
    )
    assert summary["period"] == "2025"
    assert not (out_dir / "flows" / "buyer_origin_2024.json").exists()


def test_second_run_unchanged_length_does_not_reload(db, state_path, out_dir):
    atom_bytes = _atom_xml([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200)])
    zip_2025 = _one_destination_fixture(db)
    with zipfile.ZipFile(BytesIO(zip_2025)) as zf:
        municipality = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
        origin = zf.read("OriginTable.csv")
    version_bytes = {"2025": (municipality, origin)}

    sync_commune_flows.sync(
        db,
        out_dir=out_dir,
        state_path=state_path,
        atom_bytes=atom_bytes,
        version_bytes=version_bytes,
    )
    # Second run: same ATOM feed, and NO version_bytes at all -- if the incremental logic
    # tried to re-read, it would KeyError.
    summary2 = sync_commune_flows.sync(
        db, out_dir=out_dir, state_path=state_path, atom_bytes=atom_bytes, version_bytes={}
    )
    assert summary2 == {"loaded": False, "period": "2025", "destinations": 0}


def test_changed_length_rereads_the_version(db, state_path, out_dir):
    atom_bytes_v1 = _atom_xml([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200)])
    zip_2025 = _one_destination_fixture(db)
    with zipfile.ZipFile(BytesIO(zip_2025)) as zf:
        municipality = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
        origin = zf.read("OriginTable.csv")
    sync_commune_flows.sync(
        db,
        out_dir=out_dir,
        state_path=state_path,
        atom_bytes=atom_bytes_v1,
        version_bytes={"2025": (municipality, origin)},
    )

    # Republish under an unchanged href, changed length -- a revised value.
    atom_bytes_v2 = _atom_xml([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 999)])
    zip_v2 = _complete_fixture(
        db, override_rows={"11004": "11004;Boechout;Boechout;Boechout;TOTAL;20;15;3;1;1"}
    )
    with zipfile.ZipFile(BytesIO(zip_v2)) as zf:
        municipality2 = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
        origin2 = zf.read("OriginTable.csv")

    summary = sync_commune_flows.sync(
        db,
        out_dir=out_dir,
        state_path=state_path,
        atom_bytes=atom_bytes_v2,
        version_bytes={"2025": (municipality2, origin2)},
    )
    assert summary["loaded"] is True
    document = read_store(out_dir / "flows" / "buyer_origin_2025.json")
    boechout = next(d for d in document["destinations"] if d["dest_nis"] == "11004")
    assert boechout["parcels_number"] == 20


def test_state_file_records_length(db, state_path, out_dir):
    atom_bytes = _atom_xml([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200)])
    zip_2025 = _one_destination_fixture(db)
    with zipfile.ZipFile(BytesIO(zip_2025)) as zf:
        municipality = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
        origin = zf.read("OriginTable.csv")
    sync_commune_flows.sync(
        db,
        out_dir=out_dir,
        state_path=state_path,
        atom_bytes=atom_bytes,
        version_bytes={"2025": (municipality, origin)},
    )
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["2025"]["length"] == 200


def test_a_bad_nis_on_either_axis_raises_the_whole_run(db, state_path, out_dir):
    """Acceptance criterion from the handoff: a bad NIS on either axis raises. Uses a
    complete destination set (so this exercises the origin-side NIS resolve, not the
    completeness check in test_missing_destination_commune_raises below) with an extra
    origin column, BuyerFrom99999, whose NIS-shaped code the OriginTable.csv claims is a
    real commune but resolve_geo does not know -- must fail loudly, never bucketed as
    unknown (ADR 0013 decision 4)."""
    origin_table_bad = (
        "BuyerFrom;NISCode;NameFre;NameDut;ISOCode\r\n"
        "BuyerFrom11004;11004;Boechout;Boechout;\r\n"
        "BuyerFrom11002;11002;Anvers;Antwerpen;\r\n"
        "BuyerFrom101;101;Albanie;Albani\xeb;AL\r\n"
        "BuyerFrom000;000;Ind\xe9termin\xe9;Onbepaald;\r\n"
        "BuyerFrom99999;99999;Nowhere;Nowhere;\r\n"  # NIS-shaped but unresolvable
    ).encode("latin-1")
    header = _MUNICIPALITY_HEADER + ";BuyerFrom99999"
    rows = []
    for nis in _all_municipality_nis(db):
        if nis == "11004":
            rows.append("11004;Boechout;Boechout;Boechout;TOTAL;10;5;3;1;1;1")
        else:
            rows.append(f"{nis};Commune;Commune;Commune;TOTAL;0;0;0;0;0;0")
    text = "﻿" + "\r\n".join([header, *rows]) + "\r\n"
    municipality_bytes = text.encode("utf-8")
    zip_bad = _zip_bytes(municipality_bytes, origin_table_bad)
    with zipfile.ZipFile(BytesIO(zip_bad)) as zf:
        municipality = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
        origin = zf.read("OriginTable.csv")
    atom_bytes = _atom_xml([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200)])
    with pytest.raises(SystemExit):
        sync_commune_flows.sync(
            db,
            out_dir=out_dir,
            state_path=state_path,
            atom_bytes=atom_bytes,
            version_bytes={"2025": (municipality, origin)},
        )


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_committed_store_structural_invariants():
    """Audit finding 2: loads the COMMITTED data/flows/buyer_origin_2025.json (a real sync
    output, never hand-edited) and checks the structural invariants the ADR guarantees, per
    destination -- never a hardcoded figure (CLAUDE.md rule 36): the six buckets sum to the
    denominator, coverage is exactly D/ParcelsNumber (not clamped), top-8 length is at most 8
    and ordered value desc / NIS asc, and there are 565 destinations (the number of
    municipalities, a structural fact, not a content one)."""
    path = REPO_ROOT / "data" / "flows" / "buyer_origin_2025.json"
    document = read_store(path)

    destinations = document["destinations"]
    assert len(destinations) == 565

    for dest in destinations:
        if dest["state"] == "no_purchases_recorded":
            assert "buckets" not in dest
            assert "top_origins" not in dest
            continue

        assert dest["state"] == "final"
        denominator = Decimal(dest["denominator"])
        bucket_sum = sum(Decimal(b["value"]) for b in dest["buckets"].values())
        # Relative tolerance 1e-9: bucket_sum and denominator are sums of the same Decimal
        # cells in different groupings, so they must match to within float-free rounding.
        if denominator != 0:
            relative_diff = abs(bucket_sum - denominator) / denominator
            assert relative_diff <= Decimal("1e-9"), dest["dest_nis"]
        else:
            assert bucket_sum == 0

        parcels_number = dest["parcels_number"]
        assert parcels_number > 0  # state=="final" implies D(d) > 0, and D(d) <= sum of cells
        expected_coverage = (denominator / Decimal(parcels_number) * 100).quantize(
            Decimal("0.1"), rounding=ROUND_HALF_UP
        )
        actual_coverage = Decimal(dest["coverage_pct"])
        assert actual_coverage == expected_coverage, dest["dest_nis"]
        # Not clamped: no assertion that coverage <= 100 -- a real destination legitimately
        # exceeds 100% (ADR decision 2/3, Gesves worked example) and must not be capped.

        top = dest["top_origins"]
        assert len(top) <= 8
        values = [Decimal(o["value"]) for o in top]
        nis_codes = [o["nis"] for o in top]
        for i in range(len(top) - 1):
            assert values[i] >= values[i + 1], dest["dest_nis"]
            if values[i] == values[i + 1]:
                assert nis_codes[i] < nis_codes[i + 1], dest["dest_nis"]


def test_missing_destination_commune_raises(db, state_path, out_dir):
    """Audit finding 1: the resolved destination NIS set must equal every municipality valid
    at the file's reference date. A file missing one commune's TOTAL row (here, Boechout,
    11004) must raise loudly rather than silently publishing 564 destinations."""
    all_nis = [n for n in _all_municipality_nis(db) if n != "11004"]
    rows = [f"{n};Commune;Commune;Commune;TOTAL;0;0;0;0;0" for n in all_nis]
    municipality = _municipality_csv(rows)
    zip_missing = _zip_bytes(municipality, _ORIGIN_TABLE)
    with zipfile.ZipFile(BytesIO(zip_missing)) as zf:
        municipality_bytes = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
        origin_bytes = zf.read("OriginTable.csv")
    atom_bytes = _atom_xml([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200)])
    with pytest.raises(SystemExit, match="11004"):
        sync_commune_flows.sync(
            db,
            out_dir=out_dir,
            state_path=state_path,
            atom_bytes=atom_bytes,
            version_bytes={"2025": (municipality_bytes, origin_bytes)},
        )


def test_output_is_byte_identical_across_two_runs_from_scratch(tmp_path):
    """Same inputs, two fresh databases/out-dirs/state files -- the store's computed content
    must serialize identically except the provenance `fetched_at` timestamp, which legitimately
    differs per real run (CLAUDE.md rule 35)."""
    atom_bytes = _atom_xml([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200)])

    out1, out2 = tmp_path / "run1", tmp_path / "run2"
    for out in (out1, out2):
        out.mkdir(parents=True, exist_ok=True)
        db_path = out / "test.db"
        migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
        load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
        zip_2025 = _one_destination_fixture(db_path)
        with zipfile.ZipFile(BytesIO(zip_2025)) as zf:
            municipality = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
            origin = zf.read("OriginTable.csv")
        sync_commune_flows.sync(
            db_path,
            out_dir=out,
            state_path=out / "flows_state.json",
            atom_bytes=atom_bytes,
            version_bytes={"2025": (municipality, origin)},
        )

    doc1 = read_store(out1 / "flows" / "buyer_origin_2025.json")
    doc2 = read_store(out2 / "flows" / "buyer_origin_2025.json")
    doc1["provenance"]["fetched_at"] = "NORMALIZED"
    doc2["provenance"]["fetched_at"] = "NORMALIZED"
    assert doc1 == doc2

    # Byte-identical too, once the single differing field (fetched_at, which legitimately
    # varies per real run) is normalized in the raw text the same way.
    raw1 = (out1 / "flows" / "buyer_origin_2025.json").read_text(encoding="utf-8")
    raw2 = (out2 / "flows" / "buyer_origin_2025.json").read_text(encoding="utf-8")

    def _normalize(text: str) -> str:
        return re.sub(r'"fetched_at": "[^"]*"', '"fetched_at": "NORMALIZED"', text)

    assert _normalize(raw1) == _normalize(raw2)
