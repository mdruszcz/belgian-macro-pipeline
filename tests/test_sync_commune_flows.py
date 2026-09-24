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


def _one_destination_fixture():
    municipality = _municipality_csv(["11004;Boechout;Boechout;Boechout;TOTAL;10;5;3;1;1"])
    return _zip_bytes(municipality, _ORIGIN_TABLE)


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
    zip_2025 = _one_destination_fixture()
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
    assert summary == {"loaded": True, "period": "2025", "destinations": 1}

    path = out_dir / "flows" / "buyer_origin_2025.json"
    assert path.is_file()
    document = read_store(path)
    assert document["period"] == "2025"
    assert document["dataset"] == "buyer_origin"
    assert len(document["destinations"]) == 1
    assert document["destinations"][0]["dest_nis"] == "11004"


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
    zip_2025 = _one_destination_fixture()
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
    zip_2025 = _one_destination_fixture()
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
    zip_2025 = _one_destination_fixture()
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
    municipality_v2 = _municipality_csv(["11004;Boechout;Boechout;Boechout;TOTAL;20;15;3;1;1"])
    zip_v2 = _zip_bytes(municipality_v2, _ORIGIN_TABLE)
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
    assert document["destinations"][0]["parcels_number"] == 20


def test_state_file_records_length(db, state_path, out_dir):
    atom_bytes = _atom_xml([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200)])
    zip_2025 = _one_destination_fixture()
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
    """Acceptance criterion from the handoff: a bad NIS on either axis raises."""
    bad_municipality = _municipality_csv(["99999;Nowhere;Nowhere;Nowhere;TOTAL;10;5;3;1;1"])
    zip_bad = _zip_bytes(bad_municipality, _ORIGIN_TABLE)
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


def test_output_is_byte_identical_across_two_runs_from_scratch(tmp_path):
    """Same inputs, two fresh databases/out-dirs/state files -- the store's computed content
    must serialize identically except the provenance `fetched_at` timestamp, which legitimately
    differs per real run (CLAUDE.md rule 35)."""
    atom_bytes = _atom_xml([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 200)])
    zip_2025 = _one_destination_fixture()
    with zipfile.ZipFile(BytesIO(zip_2025)) as zf:
        municipality = zf.read("MunicipalityWideBuyersOriginNaturalPersons_20251231.csv")
        origin = zf.read("OriginTable.csv")
    version_bytes = {"2025": (municipality, origin)}

    out1, out2 = tmp_path / "run1", tmp_path / "run2"
    for out in (out1, out2):
        out.mkdir(parents=True, exist_ok=True)
        db_path = out / "test.db"
        migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
        load_geography.load(db_path, REAL_GEOGRAPHY_CONFIG_DIR, allow_unverified=True)
        sync_commune_flows.sync(
            db_path,
            out_dir=out,
            state_path=out / "flows_state.json",
            atom_bytes=atom_bytes,
            version_bytes=version_bytes,
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
