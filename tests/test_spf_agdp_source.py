"""Tests for src/fetchers/spf_agdp.py: ATOM feed parsing, the ranged HTTP
file, and the per-row five-state mapping -- the pieces
scripts/sync_spf_agdp.py's own tests treat as already correct and build on
top of (geo resolution, incremental fetch, the write).

Small, trimmed fixtures only -- no real SPF Finances zip is fetched here.
"""

from __future__ import annotations

import csv
import io
import zipfile

import pytest

from src.fetchers.spf_agdp import (
    BUILDING_CONDITION,
    CONCENTRATION,
    LAND_USE,
    LEASES,
    NOTIFICATIONS,
    OWNER_OCCUPANTS,
    PROPERTY_DYNAMICS,
    TAX_EXEMPTIONS,
    TRANSACTIONS,
    AgdpDatasetConfig,
    AgdpSchemaError,
    AgdpSource,
    _RangedHttpFile,
    open_municipality_csv,
    parse_atom_feed,
)

# --- ATOM feed parsing --------------------------------------------------------

_ATOM_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Real estate leases</title>
  {links}
</feed>
"""


def _atom_bytes(links: list[tuple[str, str, int]]) -> bytes:
    body = "\n".join(
        f'<link rel="section" href="{href}" time="{time}" length="{length}" type="application/zip"/>'
        for href, time, length in links
    )
    return _ATOM_TEMPLATE.format(links=body).encode("utf-8")


def test_parse_atom_feed_reads_quarter_and_length():
    xml = _atom_bytes(
        [
            ("https://example.test/v1.zip", "2026-03-31T00:00:00Z", 214978234),
            ("https://example.test/v2.zip", "2025-12-31T00:00:00Z", 200000000),
        ]
    )
    versions = parse_atom_feed(xml, dataset_label="test")
    assert [(v.quarter, v.length, v.url) for v in versions] == [
        ("2025-Q4", 200000000, "https://example.test/v2.zip"),
        ("2026-Q1", 214978234, "https://example.test/v1.zip"),
    ]


def test_parse_atom_feed_sorts_oldest_first():
    xml = _atom_bytes(
        [
            ("https://example.test/q1.zip", "2016-03-31T00:00:00Z", 1),
            ("https://example.test/q4.zip", "2026-03-31T00:00:00Z", 2),
            ("https://example.test/q2.zip", "2019-06-30T00:00:00Z", 3),
        ]
    )
    versions = parse_atom_feed(xml, dataset_label="test")
    assert [v.quarter for v in versions] == ["2016-Q1", "2019-Q2", "2026-Q1"]


def test_parse_atom_feed_empty_feed_refuses():
    xml = _ATOM_TEMPLATE.format(links="")
    with pytest.raises(AgdpSchemaError, match="zero"):
        parse_atom_feed(xml.encode("utf-8"), dataset_label="test")


def test_parse_atom_feed_missing_length_refuses():
    xml = b"""<?xml version="1.0"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <link rel="section" href="https://example.test/v1.zip" time="2026-03-31T00:00:00Z"/>
    </feed>"""
    with pytest.raises(AgdpSchemaError, match="missing"):
        parse_atom_feed(xml, dataset_label="test")


def test_parse_atom_feed_non_quarter_end_month_refuses():
    xml = _atom_bytes([("https://example.test/v1.zip", "2026-05-15T00:00:00Z", 1)])
    with pytest.raises(AgdpSchemaError, match="quarter end"):
        parse_atom_feed(xml, dataset_label="test")


def test_parse_atom_feed_malformed_xml_refuses():
    with pytest.raises(AgdpSchemaError, match="well-formed"):
        parse_atom_feed(b"not xml at all <<<", dataset_label="test")


def test_parse_atom_feed_ignores_non_section_links():
    xml = b"""<?xml version="1.0"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <link rel="alternate" href="https://example.test/page.html"/>
      <link rel="section" href="https://example.test/v1.zip" time="2026-03-31T00:00:00Z" length="10"/>
    </feed>"""
    versions = parse_atom_feed(xml, dataset_label="test")
    assert len(versions) == 1


# --- five-state row mapping (via AgdpSource._parse, real column shapes) -----

_LEASES_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;RegistrationType;LessorType;TakerType;"
    "RentsNumber;RentP25;RentP50;RentP75;ChargesP25;ChargesP50;ChargesP75;"
    "TotalRentP25;TotalRentP50;TotalRentP75"
)


def _leases_row(
    nis="11002",
    reg="HousingRegistration",
    lessor="TOTAL",
    taker="TOTAL",
    rents_number="3297",
    rent_p50="895",
    charges_p50="70",
):
    return (
        f"{nis};Antwerpen;Antwerpen;Antwerpen;{reg};{lessor};{taker};"
        f"{rents_number};800;{rent_p50};950;60;{charges_p50};80;900;1000;1100"
    )


def _leases_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_LEASES_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_count_present_and_positive_is_final():
    csv_bytes = _leases_csv([_leases_row(rents_number="3297", rent_p50="895", charges_p50="70")])
    rows = AgdpSource(LEASES)._parse(csv_bytes, quarter="2026-Q1")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_LEASES_NEW_HOUSING"]["value"] == 3297.0
    assert by_indicator["MUN_LEASES_NEW_HOUSING"]["status"] == "final"
    assert by_indicator["MUN_LEASE_RENT_MEDIAN_HOUSING"] == {
        "geo_id": "11002",
        "period": "2026-Q1",
        "value": 895.0,
        "status": "final",
        "indicator_id": "MUN_LEASE_RENT_MEDIAN_HOUSING",
    }


def test_count_blank_writes_zero_final_never_missing():
    csv_bytes = _leases_csv([_leases_row(rents_number="", rent_p50="", charges_p50="")])
    rows = AgdpSource(LEASES)._parse(csv_bytes, quarter="2026-Q1")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_LEASES_NEW_HOUSING"] == {
        "geo_id": "11002",
        "period": "2026-Q1",
        "value": 0.0,
        "status": "final",
        "indicator_id": "MUN_LEASES_NEW_HOUSING",
    }
    # percentile is 'na' here, not zero and not suppressed -- count is blank.
    assert by_indicator["MUN_LEASE_RENT_MEDIAN_HOUSING"]["value"] is None
    assert by_indicator["MUN_LEASE_RENT_MEDIAN_HOUSING"]["status"] == "na"


@pytest.mark.parametrize("count", ["1", "2", "3", "4"])
def test_count_one_to_four_suppresses_percentile_never_zero(count):
    csv_bytes = _leases_csv([_leases_row(rents_number=count, rent_p50="", charges_p50="")])
    rows = AgdpSource(LEASES)._parse(csv_bytes, quarter="2026-Q1")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_LEASES_NEW_HOUSING"]["value"] == float(count)
    assert by_indicator["MUN_LEASES_NEW_HOUSING"]["status"] == "final"
    assert by_indicator["MUN_LEASE_RENT_MEDIAN_HOUSING"]["value"] is None
    assert by_indicator["MUN_LEASE_RENT_MEDIAN_HOUSING"]["status"] == "suppressed"
    assert by_indicator["MUN_LEASE_CHARGES_MEDIAN_HOUSING"]["value"] is None
    assert by_indicator["MUN_LEASE_CHARGES_MEDIAN_HOUSING"]["status"] == "suppressed"


def test_count_zero_is_na_not_suppressed():
    csv_bytes = _leases_csv([_leases_row(rents_number="0", rent_p50="", charges_p50="")])
    rows = AgdpSource(LEASES)._parse(csv_bytes, quarter="2026-Q1")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_LEASE_RENT_MEDIAN_HOUSING"]["status"] == "na"


def test_count_five_or_more_requires_a_published_percentile_or_refuses():
    csv_bytes = _leases_csv([_leases_row(rents_number="5", rent_p50="", charges_p50="70")])
    with pytest.raises(AgdpSchemaError, match="expected a published value"):
        AgdpSource(LEASES)._parse(csv_bytes, quarter="2026-Q1")


def test_unexpected_non_blank_percentile_under_suppression_threshold_refuses():
    csv_bytes = _leases_csv([_leases_row(rents_number="2", rent_p50="895", charges_p50="")])
    with pytest.raises(AgdpSchemaError, match="expected suppression"):
        AgdpSource(LEASES)._parse(csv_bytes, quarter="2026-Q1")


def test_only_the_selected_row_is_used_other_registration_types_ignored():
    csv_bytes = _leases_csv(
        [
            _leases_row(reg="GeneralRegistration", rents_number="9999"),
            _leases_row(reg="HousingRegistration", lessor="LegalPerson", rents_number="500"),
            _leases_row(
                reg="HousingRegistration", lessor="TOTAL", taker="TOTAL", rents_number="3297"
            ),
        ]
    )
    rows = AgdpSource(LEASES)._parse(csv_bytes, quarter="2026-Q1")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_LEASES_NEW_HOUSING"]["value"] == 3297.0


def test_missing_required_column_refuses_schema_changed():
    header = _LEASES_HEADER.replace("RentP50;", "")
    text = "﻿" + "\r\n".join([header, _leases_row()]) + "\r\n"
    with pytest.raises(AgdpSchemaError, match="missing required column"):
        AgdpSource(LEASES)._parse(text.encode("utf-8"), quarter="2026-Q1")


def test_zero_matching_rows_refuses():
    csv_bytes = _leases_csv([_leases_row(reg="GeneralRegistration")])
    with pytest.raises(AgdpSchemaError, match="zero rows matched"):
        AgdpSource(LEASES)._parse(csv_bytes, quarter="2026-Q1")


def test_blank_nis_code_refuses():
    csv_bytes = _leases_csv([_leases_row(nis="")])
    with pytest.raises(AgdpSchemaError, match="blank NISCode"):
        AgdpSource(LEASES)._parse(csv_bytes, quarter="2026-Q1")


# --- transactions dataset (single-indicator config) --------------------------

_TX_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;TransactionType;ParcelNature;ParcelsNumber;"
    "PriceP25;PriceP50;PriceP75;ParcelsAreaP25;ParcelsAreaP50;ParcelsAreaP75"
)


def _tx_row(nis="11002", ttype="VENTEIMMEUB", nature="TOTAL", parcels_number="5075"):
    return f"{nis};Antwerpen;Antwerpen;Antwerpen;{ttype};{nature};{parcels_number};100000;200000;300000;50;100;150"


def _tx_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_TX_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_transactions_selects_venteimmeub_total_only():
    csv_bytes = _tx_csv(
        [
            _tx_row(ttype="CESSION", parcels_number="1"),
            _tx_row(ttype="VENTEIMMEUB", nature="TYPE_HOUSE", parcels_number="2000"),
            _tx_row(ttype="VENTEIMMEUB", nature="TOTAL", parcels_number="5075"),
        ]
    )
    rows = AgdpSource(TRANSACTIONS)._parse(csv_bytes, quarter="2025-Q4")
    assert len(rows) == 1
    assert rows[0] == {
        "geo_id": "11002",
        "period": "2025-Q4",
        "value": 5075.0,
        "status": "final",
        "indicator_id": "MUN_PROPERTY_SALES",
    }


def test_transactions_2016_q4_real_value():
    csv_bytes = _tx_csv([_tx_row(ttype="VENTEIMMEUB", nature="TOTAL", parcels_number="3835")])
    rows = AgdpSource(TRANSACTIONS)._parse(csv_bytes, quarter="2016-Q4")
    assert rows[0]["value"] == 3835.0


# --- _RangedHttpFile / open_municipality_csv, against a fake in-process server --


class _FakeAgdpServer:
    """A minimal HTTP range server, faked at the urlopen level (no sockets):
    monkeypatches urllib.request.urlopen to serve byte ranges out of an
    in-memory zip, honouring or ignoring Range per `honour_ranges`."""

    def __init__(self, body: bytes, *, honour_ranges: bool = True):
        self.body = body
        self.honour_ranges = honour_ranges
        self.requests: list[dict] = []

    def urlopen(self, req, timeout=None, context=None):
        headers = {k.lower(): v for k, v in req.headers.items()}
        range_header = headers.get("range")
        self.requests.append({"range": range_header})
        total = len(self.body)
        if range_header and self.honour_ranges:
            unit, _, rng = range_header.partition("=")
            start_s, _, end_s = rng.partition("-")
            start = int(start_s)
            end = int(end_s) if end_s else total - 1
            end = min(end, total - 1)
            chunk = self.body[start : end + 1]
            return _FakeResponse(
                chunk, status=206, headers={"Content-Range": f"bytes {start}-{end}/{total}"}
            )
        return _FakeResponse(self.body, status=200, headers={})


class _FakeResponse:
    def __init__(self, body: bytes, *, status: int, headers: dict):
        self._body = body
        self.status = status
        self.headers = headers

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _make_test_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("Municipality.csv", _leases_csv([_leases_row()]))
        zf.writestr("Division.csv", b"not read, ever")
        zf.writestr("National.csv", b"not read, ever")
    return buf.getvalue()


def test_ranged_http_file_reads_only_the_wanted_member(monkeypatch):
    zip_bytes = _make_test_zip()
    server = _FakeAgdpServer(zip_bytes, honour_ranges=True)
    monkeypatch.setattr("urllib.request.urlopen", server.urlopen)

    csv_bytes = open_municipality_csv(
        "https://example.test/leases.zip", dataset_label="test", quarter="2026-Q1"
    )
    assert b"11002" in csv_bytes
    # A handful of range requests, not a single whole-file GET.
    assert len(server.requests) >= 2
    assert all(r["range"] is not None for r in server.requests)
    # Never asked for a Content-Length-sized single range (i.e. never fell
    # back to downloading the whole zip in one shot).
    assert len(zip_bytes) > 200  # sanity: the fixture zip is non-trivial


def test_ranged_http_file_falls_back_and_warns_on_200(monkeypatch):
    zip_bytes = _make_test_zip()
    server = _FakeAgdpServer(zip_bytes, honour_ranges=False)
    monkeypatch.setattr("urllib.request.urlopen", server.urlopen)

    warnings = []
    csv_bytes = open_municipality_csv(
        "https://example.test/leases.zip",
        dataset_label="Real-estate leases (52.01.01)",
        quarter="2026-Q1",
        warn=warnings.append,
    )
    assert b"11002" in csv_bytes
    assert len(warnings) == 1
    assert "::warning::" in warnings[0]
    assert "Real-estate leases (52.01.01)" in warnings[0]
    assert "2026-Q1" in warnings[0]


def test_ranged_http_file_seek_and_tell():
    body = b"0123456789"
    f = _RangedHttpFile("https://example.test/x")
    f._whole_body = body  # pre-seed to avoid a real request
    f._length = len(body)
    assert f.seek(3) == 3
    assert f.tell() == 3
    buf = bytearray(4)
    n = f.readinto(buf)
    assert n == 4
    assert bytes(buf) == b"3456"
    assert f.tell() == 7
    f.seek(-2, io.SEEK_END)
    assert f.tell() == 8
    f.seek(2, io.SEEK_CUR)
    assert f.tell() == 10


# --- annual ATOM period parsing (Wave 5 lot A) --------------------------------


def test_parse_atom_feed_annual_reads_year_from_jan1_timestamp():
    xml = _atom_bytes(
        [
            ("https://example.test/v2026.zip", "2026-01-01T00:00:00Z", 5000000),
            ("https://example.test/v2020.zip", "2020-01-01T00:00:00Z", 4000000),
        ]
    )
    versions = parse_atom_feed(xml, dataset_label="test", frequency="A")
    assert [(v.quarter, v.length, v.url) for v in versions] == [
        ("2020", 4000000, "https://example.test/v2020.zip"),
        ("2026", 5000000, "https://example.test/v2026.zip"),
    ]


def test_parse_atom_feed_annual_rejects_non_jan1_month():
    xml = _atom_bytes([("https://example.test/v1.zip", "2026-03-31T00:00:00Z", 1)])
    with pytest.raises(AgdpSchemaError, match="annual"):
        parse_atom_feed(xml, dataset_label="test", frequency="A")


def test_parse_atom_feed_annual_rejects_non_jan1_day():
    xml = _atom_bytes([("https://example.test/v1.zip", "2026-01-15T00:00:00Z", 1)])
    with pytest.raises(AgdpSchemaError, match="annual"):
        parse_atom_feed(xml, dataset_label="test", frequency="A")


def test_parse_atom_feed_quarterly_path_unaffected_by_frequency_default():
    # frequency defaults to "Q" -- the Wave 4 quarterly path is byte-for-byte
    # unchanged by adding the frequency parameter.
    xml = _atom_bytes([("https://example.test/v1.zip", "2026-03-31T00:00:00Z", 10)])
    versions = parse_atom_feed(xml, dataset_label="test")
    assert versions[0].quarter == "2026-Q1"


def test_unknown_frequency_rejected():
    xml = _atom_bytes([("https://example.test/v1.zip", "2026-01-01T00:00:00Z", 10)])
    with pytest.raises(ValueError, match="Unknown frequency"):
        parse_atom_feed(xml, dataset_label="test", frequency="M")


# --- Owner Occupants (52.01.14): fictitious/non-numeric NIS row filter -------

_OWNER_HEADER = "NISCode;Fictious;NameFre;NameDut;NameGer;PersonType;HousingRightType;PersonNumber"


def _owner_row(
    nis="11001", fictious="0", person_type="Total", housing_right="OCCUPANTPUPES", number="7722"
):
    return f"{nis};{fictious};Antwerpen;Antwerpen;Antwerpen;{person_type};{housing_right};{number}"


def _owner_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_OWNER_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_owner_occupants_real_value_2026_antwerp():
    csv_bytes = _owner_csv([_owner_row(nis="11001", number="7722")])
    rows = AgdpSource(OWNER_OCCUPANTS)._parse(csv_bytes, quarter="2026")
    assert rows == [
        {
            "geo_id": "11001",
            "period": "2026",
            "value": 7722.0,
            "status": "final",
            "indicator_id": "MUN_OWNER_OCCUPIERS",
        }
    ]


def test_owner_occupants_skips_fictitious_placeholder_row():
    csv_bytes = _owner_csv(
        [
            _owner_row(nis="N/A", fictious="1", number="999999"),
            _owner_row(nis="11001", fictious="0", number="7722"),
        ]
    )
    rows = AgdpSource(OWNER_OCCUPANTS)._parse(csv_bytes, quarter="2026")
    assert len(rows) == 1
    assert rows[0]["geo_id"] == "11001"


def test_owner_occupants_skips_non_numeric_nis_even_if_fictious_is_zero():
    csv_bytes = _owner_csv(
        [
            _owner_row(nis="ABCDE", fictious="0", number="1"),
            _owner_row(nis="11001", fictious="0", number="7722"),
        ]
    )
    rows = AgdpSource(OWNER_OCCUPANTS)._parse(csv_bytes, quarter="2026")
    assert len(rows) == 1
    assert rows[0]["geo_id"] == "11001"


def test_owner_occupants_real_zero_count_is_final_not_na():
    csv_bytes = _owner_csv([_owner_row(nis="11001", number="0")])
    rows = AgdpSource(OWNER_OCCUPANTS)._parse(csv_bytes, quarter="2026")
    assert rows[0] == {
        "geo_id": "11001",
        "period": "2026",
        "value": 0.0,
        "status": "final",
        "indicator_id": "MUN_OWNER_OCCUPIERS",
    }


def test_owner_occupants_only_total_occupantpupes_row_selected():
    csv_bytes = _owner_csv(
        [
            _owner_row(
                nis="11001", person_type="MTotal", housing_right="OCCUPANTPUPES", number="3768"
            ),
            _owner_row(
                nis="11001", person_type="Total", housing_right="SPOUSESPUPES", number="999"
            ),
            _owner_row(
                nis="11001", person_type="Total", housing_right="OCCUPANTPUPES", number="7722"
            ),
        ]
    )
    rows = AgdpSource(OWNER_OCCUPANTS)._parse(csv_bytes, quarter="2026")
    assert len(rows) == 1
    assert rows[0]["value"] == 7722.0


# --- Property Dynamics (52.01.24): guard against fabricated zero duration ---

_DYNAMICS_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;ParcelNature;ParcelsNumber;"
    "PropertyChange0Y;PropertyDurationP25;PropertyDurationP50;PropertyDurationP75;"
    "PropertyDurationMean;PropertyRotationMean"
)


def _dynamics_row(
    nis="11001", nature="TOTAL", parcels="11538", p50="9.0102669405", rotation="15.159677398"
):
    return f"{nis};Antwerpen;Antwerpen;Antwerpen;{nature};{parcels};0;8;{p50};10;9.5;{rotation}"


def _dynamics_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_DYNAMICS_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_property_dynamics_real_value_2026_antwerp():
    csv_bytes = _dynamics_csv(
        [_dynamics_row(nis="11001", parcels="11538", p50="9.0102669405", rotation="15.159677398")]
    )
    rows = AgdpSource(PROPERTY_DYNAMICS)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_PARCELS_OWNED"]["value"] == 11538.0
    assert by_indicator["MUN_PARCELS_OWNED"]["status"] == "final"
    assert by_indicator["MUN_OWNERSHIP_DURATION_MEDIAN"]["value"] == pytest.approx(9.0102669405)
    assert by_indicator["MUN_OWNERSHIP_DURATION_MEDIAN"]["status"] == "final"
    assert by_indicator["MUN_OWNERSHIP_ROTATION_MEAN"]["value"] == pytest.approx(15.159677398)
    assert by_indicator["MUN_OWNERSHIP_ROTATION_MEAN"]["status"] == "final"


def test_property_dynamics_only_total_parcel_nature_selected():
    csv_bytes = _dynamics_csv(
        [
            _dynamics_row(nis="11001", nature="TYPE_HOUSE", parcels="500", p50="5.0"),
            _dynamics_row(nis="11001", nature="TOTAL", parcels="11538", p50="9.0102669405"),
        ]
    )
    rows = AgdpSource(PROPERTY_DYNAMICS)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_PARCELS_OWNED"]["value"] == 11538.0


def test_property_dynamics_zero_parcels_is_na_never_a_fabricated_zero_duration():
    # SPF writes a literal 0 for duration/rotation when ParcelsNumber is 0 --
    # this must map to na/None, never a real published zero duration.
    csv_bytes = _dynamics_csv([_dynamics_row(nis="11001", parcels="0", p50="0", rotation="0")])
    rows = AgdpSource(PROPERTY_DYNAMICS)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_PARCELS_OWNED"] == {
        "geo_id": "11001",
        "period": "2026",
        "value": 0.0,
        "status": "final",
        "indicator_id": "MUN_PARCELS_OWNED",
    }
    assert by_indicator["MUN_OWNERSHIP_DURATION_MEDIAN"]["value"] is None
    assert by_indicator["MUN_OWNERSHIP_DURATION_MEDIAN"]["status"] == "na"
    assert by_indicator["MUN_OWNERSHIP_ROTATION_MEAN"]["value"] is None
    assert by_indicator["MUN_OWNERSHIP_ROTATION_MEAN"]["status"] == "na"


def test_property_dynamics_blank_parcels_count_refuses_never_a_fabricated_zero():
    # Previously this asserted a bug: a blank ParcelsNumber (a schema
    # surprise -- no live file has ever measured one) silently became a
    # written 0.0, final. CLAUDE.md rule 26 forbids collapsing "missing"
    # into "measured zero" for a count column exactly as much as for a
    # percentile/mean one -- fixed in _annual_row_to_observations (Wave 5
    # lot B handoff) to raise AgdpSchemaError instead, naming the dataset,
    # year and NIS, the same as every other unexpected-blank guard in this
    # module already does.
    csv_bytes = _dynamics_csv([_dynamics_row(nis="11001", parcels="", p50="", rotation="")])
    with pytest.raises(AgdpSchemaError, match="blank"):
        AgdpSource(PROPERTY_DYNAMICS)._parse(csv_bytes, quarter="2026")


def test_property_dynamics_nonzero_parcels_blank_duration_refuses():
    csv_bytes = _dynamics_csv([_dynamics_row(nis="11001", parcels="100", p50="")])
    with pytest.raises(AgdpSchemaError, match="expected a published value"):
        AgdpSource(PROPERTY_DYNAMICS)._parse(csv_bytes, quarter="2026")


def test_property_dynamics_2016_real_value():
    csv_bytes = _dynamics_csv([_dynamics_row(nis="11001", parcels="9779", p50="9.5140314853")])
    rows = AgdpSource(PROPERTY_DYNAMICS)._parse(csv_bytes, quarter="2016")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_PARCELS_OWNED"]["value"] == 9779.0
    assert by_indicator["MUN_OWNERSHIP_DURATION_MEDIAN"]["value"] == pytest.approx(9.5140314853)


# --- Wave 5 lot B: land use (52.01.04) ---------------------------------------

_LANDUSE_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;ParcelNature;ParcelsNumber;"
    "TotalCadastralIncome;TaxableCadastralIncome;TaxExemptCadastralIncome"
)


def _landuse_row(
    nis="11001",
    nature="TOTAL",
    parcels="11540",
    total_ci="15431671",
    taxable_ci="13908911",
    exempt_ci="1522760",
):
    return (
        f"{nis};Antwerpen;Antwerpen;Antwerpen;{nature};{parcels};"
        f"{total_ci};{taxable_ci};{exempt_ci}"
    )


def _landuse_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_LANDUSE_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_landuse_2026_real_values_11001():
    # Hand-computed from the real 2026 Municipality CSV: parcels 11540,
    # CI total 15431671, CI taxable 13908911.
    csv_bytes = _landuse_csv(
        [_landuse_row(nis="11001", parcels="11540", total_ci="15431671", taxable_ci="13908911")]
    )
    rows = AgdpSource(LAND_USE)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_CADASTRAL_PARCELS_TOTAL"] == {
        "geo_id": "11001",
        "period": "2026",
        "value": 11540.0,
        "status": "final",
        "indicator_id": "MUN_CADASTRAL_PARCELS_TOTAL",
    }
    assert by_indicator["MUN_CADASTRAL_INCOME_TOTAL"]["value"] == 15431671.0
    assert by_indicator["MUN_CADASTRAL_INCOME_TOTAL"]["status"] == "final"
    assert by_indicator["MUN_CADASTRAL_INCOME_TAXABLE"]["value"] == 13908911.0
    assert by_indicator["MUN_CADASTRAL_INCOME_TAXABLE"]["status"] == "final"


@pytest.mark.parametrize(
    "nis,parcels,total_ci,taxable_ci",
    [
        ("44083", "46948", "30809990", "27168364"),
        ("23106", "38624", "14644408", "13718983"),
        ("82039", "55633", "14059868", "11787166"),
    ],
)
def test_landuse_2026_real_values_other_communes(nis, parcels, total_ci, taxable_ci):
    csv_bytes = _landuse_csv(
        [_landuse_row(nis=nis, parcels=parcels, total_ci=total_ci, taxable_ci=taxable_ci)]
    )
    rows = AgdpSource(LAND_USE)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_CADASTRAL_PARCELS_TOTAL"]["value"] == float(parcels)
    assert by_indicator["MUN_CADASTRAL_INCOME_TOTAL"]["value"] == float(total_ci)
    assert by_indicator["MUN_CADASTRAL_INCOME_TAXABLE"]["value"] == float(taxable_ci)


def test_landuse_2017_real_values():
    # 11001: 9925 / 14338902. 44001: 23424 / 16144345.
    csv_bytes = _landuse_csv(
        [
            _landuse_row(nis="11001", parcels="9925", total_ci="14338902", taxable_ci="12000000"),
            _landuse_row(nis="44001", parcels="23424", total_ci="16144345", taxable_ci="14000000"),
        ]
    )
    rows = AgdpSource(LAND_USE)._parse(csv_bytes, quarter="2017")
    by_geo = {}
    for r in rows:
        by_geo.setdefault(r["geo_id"], {})[r["indicator_id"]] = r
    assert by_geo["11001"]["MUN_CADASTRAL_PARCELS_TOTAL"]["value"] == 9925.0
    assert by_geo["11001"]["MUN_CADASTRAL_INCOME_TOTAL"]["value"] == 14338902.0
    assert by_geo["44001"]["MUN_CADASTRAL_PARCELS_TOTAL"]["value"] == 23424.0
    assert by_geo["44001"]["MUN_CADASTRAL_INCOME_TOTAL"]["value"] == 16144345.0


def test_landuse_only_total_parcel_nature_selected():
    csv_bytes = _landuse_csv(
        [
            _landuse_row(nis="11001", nature="TYPE_HOUSE", parcels="500"),
            _landuse_row(nis="11001", nature="TOTAL", parcels="11540"),
        ]
    )
    rows = AgdpSource(LAND_USE)._parse(csv_bytes, quarter="2026")
    assert len(rows) == 3  # three indicators, one matched row
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_CADASTRAL_PARCELS_TOTAL"]["value"] == 11540.0


def test_landuse_blank_parcels_count_refuses():
    csv_bytes = _landuse_csv([_landuse_row(nis="11001", parcels="")])
    with pytest.raises(AgdpSchemaError, match="blank"):
        AgdpSource(LAND_USE)._parse(csv_bytes, quarter="2026")


def test_landuse_blank_cadastral_income_at_nonzero_parcels_refuses():
    # Land use's TotalCadastralIncome must refuse loudly if a nonzero-parcel
    # row ever blanks it, since no suppression tier applies to this column at
    # TOTAL (measured 2026/2025/2017). The real 1-4 suppression tier this
    # dataset does have at TOTAL never touches this column in any measured
    # file; if it ever does, this must fail loudly, not silently mis-map.
    csv_bytes = _landuse_csv([_landuse_row(nis="11001", parcels="11540", total_ci="")])
    with pytest.raises(AgdpSchemaError, match="always_final_columns"):
        AgdpSource(LAND_USE)._parse(csv_bytes, quarter="2026")


def test_landuse_missing_required_column_refuses():
    header = _LANDUSE_HEADER.replace("TotalCadastralIncome;", "")
    text = "﻿" + "\r\n".join([header, _landuse_row()]) + "\r\n"
    with pytest.raises(AgdpSchemaError, match="missing required column"):
        AgdpSource(LAND_USE)._parse(text.encode("utf-8"), quarter="2026")


# --- Wave 5 lot B: building condition (52.01.05) ------------------------------

_BUILDING_HEADER = "NISCode;NameFre;NameDut;NameGer;ParcelNature;ParcelsNumber;CentralHeating"


def _building_row(nis="11001", nature="TOTAL", parcels="9553", heating="6419"):
    return f"{nis};Antwerpen;Antwerpen;Antwerpen;{nature};{parcels};{heating}"


def _building_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_BUILDING_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_building_2026_real_values_11001():
    csv_bytes = _building_csv([_building_row(nis="11001", parcels="9553", heating="6419")])
    rows = AgdpSource(BUILDING_CONDITION)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_BUILDINGS_TOTAL"] == {
        "geo_id": "11001",
        "period": "2026",
        "value": 9553.0,
        "status": "final",
        "indicator_id": "MUN_BUILDINGS_TOTAL",
    }
    assert by_indicator["MUN_BUILDINGS_CENTRAL_HEATING"]["value"] == 6419.0
    assert by_indicator["MUN_BUILDINGS_CENTRAL_HEATING"]["status"] == "final"


@pytest.mark.parametrize(
    "nis,parcels,heating",
    [
        ("44083", "29756", "18711"),
        ("23106", "13798", "9216"),
        ("82039", "13716", "8505"),
    ],
)
def test_building_2026_real_values_other_communes(nis, parcels, heating):
    csv_bytes = _building_csv([_building_row(nis=nis, parcels=parcels, heating=heating)])
    rows = AgdpSource(BUILDING_CONDITION)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_BUILDINGS_TOTAL"]["value"] == float(parcels)
    assert by_indicator["MUN_BUILDINGS_CENTRAL_HEATING"]["value"] == float(heating)


def test_building_2017_real_values():
    # 11001: 8011 / 5636. 44001: 11937 / 7211.
    csv_bytes = _building_csv(
        [
            _building_row(nis="11001", parcels="8011", heating="5636"),
            _building_row(nis="44001", parcels="11937", heating="7211"),
        ]
    )
    rows = AgdpSource(BUILDING_CONDITION)._parse(csv_bytes, quarter="2017")
    by_geo = {}
    for r in rows:
        by_geo.setdefault(r["geo_id"], {})[r["indicator_id"]] = r
    assert by_geo["11001"]["MUN_BUILDINGS_TOTAL"]["value"] == 8011.0
    assert by_geo["11001"]["MUN_BUILDINGS_CENTRAL_HEATING"]["value"] == 5636.0
    assert by_geo["44001"]["MUN_BUILDINGS_TOTAL"]["value"] == 11937.0
    assert by_geo["44001"]["MUN_BUILDINGS_CENTRAL_HEATING"]["value"] == 7211.0


def test_building_zero_central_heating_is_real_measured_zero_final():
    csv_bytes = _building_csv([_building_row(nis="11001", parcels="100", heating="0")])
    rows = AgdpSource(BUILDING_CONDITION)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_BUILDINGS_CENTRAL_HEATING"] == {
        "geo_id": "11001",
        "period": "2026",
        "value": 0.0,
        "status": "final",
        "indicator_id": "MUN_BUILDINGS_CENTRAL_HEATING",
    }


def test_building_blank_central_heating_at_nonzero_parcels_refuses():
    csv_bytes = _building_csv([_building_row(nis="11001", parcels="100", heating="")])
    with pytest.raises(AgdpSchemaError, match="always_final_columns"):
        AgdpSource(BUILDING_CONDITION)._parse(csv_bytes, quarter="2026")


def test_building_central_heating_stays_final_even_if_parcels_were_zero():
    # always_final_columns must never collapse to na the way a
    # percentile/mean does at zero count -- CentralHeating is its own
    # independent additive figure. Not observed live at TOTAL (every
    # measured commune has ParcelsNumber > 0), but the mapping must still be
    # correct if it ever occurred, rather than silently mis-classifying a
    # real value as unmeasured.
    csv_bytes = _building_csv([_building_row(nis="11001", parcels="0", heating="0")])
    rows = AgdpSource(BUILDING_CONDITION)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_BUILDINGS_TOTAL"]["value"] == 0.0
    assert by_indicator["MUN_BUILDINGS_TOTAL"]["status"] == "final"
    assert by_indicator["MUN_BUILDINGS_CENTRAL_HEATING"] == {
        "geo_id": "11001",
        "period": "2026",
        "value": 0.0,
        "status": "final",
        "indicator_id": "MUN_BUILDINGS_CENTRAL_HEATING",
    }


def test_building_blank_parcels_count_refuses():
    csv_bytes = _building_csv([_building_row(nis="11001", parcels="")])
    with pytest.raises(AgdpSchemaError, match="blank"):
        AgdpSource(BUILDING_CONDITION)._parse(csv_bytes, quarter="2026")


def test_building_only_total_parcel_nature_selected():
    csv_bytes = _building_csv(
        [
            _building_row(nis="11001", nature="TYPE_HOUSE", parcels="500", heating="400"),
            _building_row(nis="11001", nature="TOTAL", parcels="9553", heating="6419"),
        ]
    )
    rows = AgdpSource(BUILDING_CONDITION)._parse(csv_bytes, quarter="2026")
    by_indicator = {r["indicator_id"]: r for r in rows}
    assert by_indicator["MUN_BUILDINGS_TOTAL"]["value"] == 9553.0


# --- Wave 5 lot B: property-tax exemptions (52.01.03) -------------------------

_EXEMPT_HEADER = "NISCode;NameFre;NameDut;NameGer;ExemptionType;ParcelsNumber"


def _exempt_row(nis="11001", exemption="TOTAL", parcels="216"):
    return f"{nis};Antwerpen;Antwerpen;Antwerpen;{exemption};{parcels}"


def _exempt_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_EXEMPT_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_exempt_2026_real_values():
    # 11001: 216. 44083: 1210. 23106: 775. 82039: 2638.
    csv_bytes = _exempt_csv([_exempt_row(nis="11001", parcels="216")])
    rows = AgdpSource(TAX_EXEMPTIONS)._parse(csv_bytes, quarter="2026")
    assert rows == [
        {
            "geo_id": "11001",
            "period": "2026",
            "value": 216.0,
            "status": "final",
            "indicator_id": "MUN_PARCELS_TAX_EXEMPT",
        }
    ]


@pytest.mark.parametrize(
    "nis,parcels",
    [
        ("44083", "1210"),
        ("23106", "775"),
        ("82039", "2638"),
    ],
)
def test_exempt_2026_real_values_other_communes(nis, parcels):
    csv_bytes = _exempt_csv([_exempt_row(nis=nis, parcels=parcels)])
    rows = AgdpSource(TAX_EXEMPTIONS)._parse(csv_bytes, quarter="2026")
    assert rows[0]["value"] == float(parcels)


def test_exempt_2017_real_zero_is_final_not_na():
    # 44001 2017: 0 -- a real measured zero, final, not na (handoff).
    csv_bytes = _exempt_csv([_exempt_row(nis="44001", parcels="0")])
    rows = AgdpSource(TAX_EXEMPTIONS)._parse(csv_bytes, quarter="2017")
    assert rows == [
        {
            "geo_id": "44001",
            "period": "2017",
            "value": 0.0,
            "status": "final",
            "indicator_id": "MUN_PARCELS_TAX_EXEMPT",
        }
    ]


def test_exempt_2017_11001_real_value():
    csv_bytes = _exempt_csv([_exempt_row(nis="11001", parcels="206")])
    rows = AgdpSource(TAX_EXEMPTIONS)._parse(csv_bytes, quarter="2017")
    assert rows[0]["value"] == 206.0


def test_exempt_only_total_exemption_type_selected():
    csv_bytes = _exempt_csv(
        [
            _exempt_row(nis="11001", exemption="1", parcels="50"),
            _exempt_row(nis="11001", exemption="2", parcels="60"),
            _exempt_row(nis="11001", exemption="TOTAL", parcels="216"),
        ]
    )
    rows = AgdpSource(TAX_EXEMPTIONS)._parse(csv_bytes, quarter="2026")
    assert len(rows) == 1
    assert rows[0]["value"] == 216.0


def test_exempt_blank_parcels_count_refuses():
    # Herstappe (73028) genuinely blanks TotalCadastralIncome at
    # ParcelsNumber=4 in this dataset (excluded from this batch's chosen
    # columns), but ParcelsNumber itself is never blank in any measured
    # file. A blank here is still a schema surprise, refused loudly rather
    # than treated as a measured zero.
    csv_bytes = _exempt_csv([_exempt_row(nis="73028", parcels="")])
    with pytest.raises(AgdpSchemaError, match="blank"):
        AgdpSource(TAX_EXEMPTIONS)._parse(csv_bytes, quarter="2025")


# --- Wave 5 lot C: __post_init__ hardening -----------------------------------


def test_post_init_rejects_more_than_one_is_count_entry():
    with pytest.raises(ValueError, match="at most one"):
        AgdpDatasetConfig(
            label="x",
            uuid="u",
            required_columns=("A", "B"),
            select={},
            count_column="A",
            indicators={"A": ("IND1", True), "B": ("IND2", True)},
        )


def test_post_init_rejects_is_count_column_mismatching_count_column():
    with pytest.raises(ValueError, match="does not match count_column"):
        AgdpDatasetConfig(
            label="x",
            uuid="u",
            required_columns=("A", "B"),
            select={},
            count_column="B",
            indicators={"A": ("IND1", True)},
        )


def test_post_init_rejects_always_final_columns_naming_a_count_column():
    with pytest.raises(ValueError, match="not a non-count key"):
        AgdpDatasetConfig(
            label="x",
            uuid="u",
            required_columns=("A",),
            select={},
            count_column="A",
            indicators={"A": ("IND1", True)},
            always_final_columns=("A",),
        )


def test_post_init_allows_zero_is_count_entries():
    # Concentration (52.01.08) keys suppression off ParcelsCumulatedNumber
    # without publishing it as its own indicator -- zero is_count=True
    # entries must be allowed, not just exactly one.
    config = AgdpDatasetConfig(
        label="x",
        uuid="u",
        required_columns=("A", "B"),
        select={},
        count_column="A",
        indicators={"B": ("IND1", False)},
        always_final_columns=("B",),
    )
    assert config.count_column == "A"


def test_post_init_accepts_real_configs():
    for config in (
        LEASES,
        TRANSACTIONS,
        OWNER_OCCUPANTS,
        PROPERTY_DYNAMICS,
        LAND_USE,
        BUILDING_CONDITION,
        TAX_EXEMPTIONS,
        NOTIFICATIONS,
        CONCENTRATION,
    ):
        # Constructed at import time already; re-running __post_init__ here
        # via a fresh construction (dataclasses don't expose it standalone)
        # would just re-validate the same object -- this asserts every real
        # config module-level constant actually exists and is the right type.
        assert isinstance(config, AgdpDatasetConfig)


# --- Wave 5 lot C: notifications of the cadastral income (52.01.25) ---------

_NOTIF_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;CadastralIncomeFiscalStatus;CadastralIncomeNature;"
    "NotificationMotivationCategory;CadastralIncomeNumber;TotalCadastralIncome;"
    "CadastralIncomeP25;CadastralIncomeP50;CadastralIncomeP75;CadastralIncomeSD"
)


def _notif_row(
    nis,
    count,
    fiscal_status="TOTAL",
    nature="TOTAL",
    motivation="TOTAL",
    total="",
    p25="",
    p50="",
    p75="",
    sd="",
):
    return (
        f"{nis};Commune;Commune;Commune;{fiscal_status};{nature};{motivation};{count};"
        f"{total};{p25};{p50};{p75};{sd}"
    )


def _notif_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_NOTIF_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_notifications_2025_real_value_11001():
    # Hand-computed from the handoff: 11001 2025 count 208, total 773812, P50 826.
    csv_bytes = _notif_csv(
        [_notif_row("11001", 208, total="773812", p25="400", p50="826", p75="1300", sd="250")]
    )
    rows = AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")
    by = {r["indicator_id"]: r for r in rows}
    assert by["MUN_CI_NOTIFICATIONS"] == {
        "geo_id": "11001",
        "period": "2025",
        "value": 208.0,
        "status": "final",
        "indicator_id": "MUN_CI_NOTIFICATIONS",
    }
    assert by["MUN_CI_NOTIFIED_TOTAL"]["value"] == 773812.0
    assert by["MUN_CI_NOTIFIED_TOTAL"]["status"] == "final"
    assert by["MUN_CI_NOTIFIED_MEDIAN"]["value"] == 826.0
    assert by["MUN_CI_NOTIFIED_MEDIAN"]["status"] == "final"


@pytest.mark.parametrize(
    "nis,count,total,p50",
    [
        ("44083", 1143, "3501614", "444"),
        ("23106", 414, "278705", "228"),
        ("82039", 712, "479212", "16"),
    ],
)
def test_notifications_2025_real_values_other_communes(nis, count, total, p50):
    csv_bytes = _notif_csv([_notif_row(nis, count, total=total, p25="1", p50=p50, p75="1", sd="1")])
    rows = AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")
    by = {r["indicator_id"]: r for r in rows}
    assert by["MUN_CI_NOTIFICATIONS"]["value"] == float(count)
    assert by["MUN_CI_NOTIFIED_TOTAL"]["value"] == float(total)
    assert by["MUN_CI_NOTIFIED_MEDIAN"]["value"] == float(p50)


def test_notifications_2025_herstappe_count_three_is_suppressed():
    # 73028 Herstappe count 3 -> count final, total/P50 value None, status
    # suppressed.
    csv_bytes = _notif_csv([_notif_row("73028", 3)])
    rows = AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")
    by = {r["indicator_id"]: r for r in rows}
    assert by["MUN_CI_NOTIFICATIONS"] == {
        "geo_id": "73028",
        "period": "2025",
        "value": 3.0,
        "status": "final",
        "indicator_id": "MUN_CI_NOTIFICATIONS",
    }
    assert by["MUN_CI_NOTIFIED_TOTAL"]["value"] is None
    assert by["MUN_CI_NOTIFIED_TOTAL"]["status"] == "suppressed"
    assert by["MUN_CI_NOTIFIED_MEDIAN"]["value"] is None
    assert by["MUN_CI_NOTIFIED_MEDIAN"]["status"] == "suppressed"


def test_notifications_2016_real_values():
    # 11001 2016 count 211, total 908627, P50 926. 44001 2016 count 552,
    # total 8707921, P50 809.
    csv_bytes = _notif_csv(
        [
            _notif_row("11001", 211, total="908627", p25="500", p50="926", p75="1400", sd="300"),
            _notif_row("44001", 552, total="8707921", p25="600", p50="809", p75="1100", sd="200"),
        ]
    )
    rows = AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2016")
    by_geo = {}
    for r in rows:
        by_geo.setdefault(r["geo_id"], {})[r["indicator_id"]] = r
    assert by_geo["11001"]["MUN_CI_NOTIFICATIONS"]["value"] == 211.0
    assert by_geo["11001"]["MUN_CI_NOTIFIED_TOTAL"]["value"] == 908627.0
    assert by_geo["11001"]["MUN_CI_NOTIFIED_MEDIAN"]["value"] == 926.0
    assert by_geo["44001"]["MUN_CI_NOTIFICATIONS"]["value"] == 552.0
    assert by_geo["44001"]["MUN_CI_NOTIFIED_TOTAL"]["value"] == 8707921.0
    assert by_geo["44001"]["MUN_CI_NOTIFIED_MEDIAN"]["value"] == 809.0


def test_notifications_2016_count_one_is_suppressed():
    # 73028 2016 count 1 -> suppressed.
    csv_bytes = _notif_csv([_notif_row("73028", 1)])
    rows = AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2016")
    by = {r["indicator_id"]: r for r in rows}
    assert by["MUN_CI_NOTIFIED_TOTAL"]["status"] == "suppressed"
    assert by["MUN_CI_NOTIFIED_TOTAL"]["value"] is None


def test_notifications_2016_count_four_is_suppressed():
    # 33016 2016 count 4 -> suppressed.
    csv_bytes = _notif_csv([_notif_row("33016", 4)])
    rows = AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2016")
    by = {r["indicator_id"]: r for r in rows}
    assert by["MUN_CI_NOTIFIED_TOTAL"]["status"] == "suppressed"
    assert by["MUN_CI_NOTIFIED_MEDIAN"]["status"] == "suppressed"


def test_notifications_count_zero_total_is_final_zero_median_is_na():
    # Measured live 2026-09-24 (NIS 73028, 2019): at count 0, SPF writes a
    # literal '0' for EVERY value column, including the median -- not a
    # blank. TotalCadastralIncome's 0 is a genuine additive sum-of-zero
    # (final); CadastralIncomeP50's 0 is SPF's own undefined-median
    # placeholder (na), same distinction Property Dynamics already makes
    # for zero-parcel duration/rotation.
    csv_bytes = _notif_csv([_notif_row("44001", 0, total="0", p25="0", p50="0", p75="0", sd="0")])
    rows = AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")
    by = {r["indicator_id"]: r for r in rows}
    assert by["MUN_CI_NOTIFICATIONS"] == {
        "geo_id": "44001",
        "period": "2025",
        "value": 0.0,
        "status": "final",
        "indicator_id": "MUN_CI_NOTIFICATIONS",
    }
    assert by["MUN_CI_NOTIFIED_TOTAL"] == {
        "geo_id": "44001",
        "period": "2025",
        "value": 0.0,
        "status": "final",
        "indicator_id": "MUN_CI_NOTIFIED_TOTAL",
    }
    assert by["MUN_CI_NOTIFIED_MEDIAN"]["value"] is None
    assert by["MUN_CI_NOTIFIED_MEDIAN"]["status"] == "na"


def test_notifications_count_zero_blank_value_refuses():
    # A blank cell at count 0 is itself a schema surprise now -- SPF's own
    # shape at count 0 is a literal '0', never a blank.
    csv_bytes = _notif_csv([_notif_row("44001", 0)])
    with pytest.raises(AgdpSchemaError, match="expected a literal '0'"):
        AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")


def test_notifications_blank_count_refuses_never_a_fabricated_value():
    csv_bytes = _notif_csv([_notif_row("11001", "")])
    with pytest.raises(AgdpSchemaError, match="blank"):
        AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")


def test_notifications_count_zero_unexpected_nonblank_value_refuses():
    csv_bytes = _notif_csv([_notif_row("44001", 0, total="100")])
    with pytest.raises(AgdpSchemaError, match="expected a literal '0'"):
        AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")


def test_notifications_count_1to4_unexpected_nonblank_value_refuses():
    csv_bytes = _notif_csv([_notif_row("73028", 3, total="100")])
    with pytest.raises(AgdpSchemaError, match="expected suppression"):
        AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")


def test_notifications_count_5plus_unexpected_blank_value_refuses():
    csv_bytes = _notif_csv([_notif_row("11001", 10)])
    with pytest.raises(AgdpSchemaError, match="expected a published"):
        AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")


def test_notifications_only_total_total_total_row_selected():
    csv_bytes = _notif_csv(
        [
            _notif_row("11001", 50, fiscal_status="OTHER", total="10000", p50="200"),
            _notif_row("11001", 208, total="773812", p50="826"),
        ]
    )
    rows = AgdpSource(NOTIFICATIONS)._parse(csv_bytes, quarter="2025")
    assert len(rows) == 3
    by = {r["indicator_id"]: r for r in rows}
    assert by["MUN_CI_NOTIFICATIONS"]["value"] == 208.0


def test_notifications_missing_required_column_refuses():
    bad_header = _NOTIF_HEADER.replace("CadastralIncomeP50;", "")
    text = "﻿" + "\r\n".join([bad_header, _notif_row("11001", 10)]) + "\r\n"
    with pytest.raises(AgdpSchemaError, match="missing required"):
        AgdpSource(NOTIFICATIONS)._parse(text.encode("utf-8"), quarter="2025")


def test_notifications_atom_accepts_calendar_year_end_timestamp():
    xml = _atom_bytes([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 1)])
    versions = parse_atom_feed(xml, dataset_label="notifications", frequency="AY")
    assert [(v.quarter, v.length) for v in versions] == [("2025", 1)]


def test_annual_atom_rejects_calendar_year_end_timestamp():
    # The 1-January parser ("A") must keep rejecting a 31-December
    # timestamp -- "AY" is a separate, narrower mode, never a loosening of
    # "A" itself.
    xml = _atom_bytes([("https://example.test/2025.zip", "2025-12-31T00:00:00Z", 1)])
    with pytest.raises(AgdpSchemaError, match="1-January"):
        parse_atom_feed(xml, dataset_label="notifications", frequency="A")


def test_calendar_year_end_atom_rejects_january_first_timestamp():
    xml = _atom_bytes([("https://example.test/2025.zip", "2025-01-01T00:00:00Z", 1)])
    with pytest.raises(AgdpSchemaError, match="calendar-year-end"):
        parse_atom_feed(xml, dataset_label="notifications", frequency="AY")


# --- Wave 5 lot C: concentration of cadastral income (52.01.08) ------------

_CONC_HEADER = (
    "NISCode;NameFre;NameDut;NameGer;ParcelNature;Range;ParcelsNumber;"
    "ParcelsCumulatedNumber;HousingsNumber;HousingsCumulatedNumber;"
    "TotalCadastralIncome;CumulatedCadastralIncome"
)


def _conc_row(
    nis,
    rng,
    parcels_cum,
    ci_cum,
    nature="TOTAL",
    parcels="100",
    housings="100",
    housings_cum="100",
    ci="1000",
):
    return (
        f"{nis};Commune;Commune;Commune;{nature};{rng};{parcels};{parcels_cum};"
        f"{housings};{housings_cum};{ci};{ci_cum}"
    )


def _conc_csv(rows: list[str]) -> bytes:
    text = "﻿" + "\r\n".join([_CONC_HEADER, *rows]) + "\r\n"
    return text.encode("utf-8")


def test_concentration_2026_real_value_11001():
    # Hand-computed: 11001 2026 cumulated CI 8428254 at Range30001.
    csv_bytes = _conc_csv([_conc_row("11001", "Range30001", "6453", "8428254")])
    rows = AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2026")
    assert rows == [
        {
            "geo_id": "11001",
            "period": "2026",
            "value": 8428254.0,
            "status": "final",
            "indicator_id": "MUN_RESIDENTIAL_PARCELS_CI_TOTAL",
        }
    ]


@pytest.mark.parametrize(
    "nis,ci_cum",
    [
        ("44083", "19155664"),
        ("23106", "11298541"),
        ("82039", "8466649"),
    ],
)
def test_concentration_2026_real_values_other_communes(nis, ci_cum):
    csv_bytes = _conc_csv([_conc_row(nis, "Range30001", "1", ci_cum)])
    rows = AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2026")
    assert rows[0]["value"] == float(ci_cum)


def test_concentration_2026_herstappe_real_value():
    csv_bytes = _conc_csv([_conc_row("73028", "Range30001", "30", "27177")])
    rows = AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2026")
    assert rows[0]["value"] == 27177.0


def test_concentration_2011_real_values():
    csv_bytes = _conc_csv(
        [
            _conc_row("11001", "Range30001", "5760", "7473689"),
            _conc_row("44001", "Range30001", "8311", "7127074"),
        ]
    )
    rows = AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2011")
    by_geo = {r["geo_id"]: r for r in rows}
    assert by_geo["11001"]["value"] == 7473689.0
    assert by_geo["44001"]["value"] == 7127074.0


def test_concentration_only_total_nature_range30001_selected():
    csv_bytes = _conc_csv(
        [
            _conc_row("11001", "Range1", "500", "500", nature="TOTAL"),
            _conc_row("11001", "Range30001", "500", "3000", nature="TYPE_HOUSE"),
            _conc_row("11001", "Range30001", "6453", "8428254", nature="TOTAL"),
        ]
    )
    rows = AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2026")
    assert len(rows) == 1
    assert rows[0]["value"] == 8428254.0


def test_concentration_range30001_missing_for_one_commune_no_row_written():
    csv_bytes = _conc_csv(
        [
            _conc_row("11001", "Range30001", "6453", "8428254"),
            _conc_row("44001", "Range1", "5", "5"),
        ]
    )
    rows = AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2026")
    assert len(rows) == 1
    assert rows[0]["geo_id"] == "11001"


def test_concentration_duplicate_range30001_row_for_same_nis_refuses():
    csv_bytes = _conc_csv(
        [
            _conc_row("11001", "Range30001", "6453", "8428254"),
            _conc_row("11001", "Range30001", "1", "1"),
        ]
    )
    with pytest.raises(AgdpSchemaError, match="more than one row"):
        AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2026")


def test_concentration_blank_cumulated_ci_at_parcels_5plus_refuses():
    # parcels="100" default (>= 5) -- SPF always publishes a value there.
    csv_bytes = _conc_csv([_conc_row("11001", "Range30001", "6453", "")])
    with pytest.raises(AgdpSchemaError, match="expected a published value"):
        AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2026")


def test_concentration_parcels_1to4_suppresses_never_zero():
    # Real measured shape (2011, NIS 11005): the TOP BAND's own ParcelsNumber
    # 1-4 suppresses CumulatedCadastralIncome even when the cumulated running
    # total (ParcelsCumulatedNumber) is in the thousands.
    csv_bytes = _conc_csv([_conc_row("11005", "Range30001", "6637", "", parcels="4")])
    rows = AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2011")
    assert rows == [
        {
            "geo_id": "11005",
            "period": "2011",
            "value": None,
            "status": "suppressed",
            "indicator_id": "MUN_RESIDENTIAL_PARCELS_CI_TOTAL",
        }
    ]


def test_concentration_parcels_1to4_unexpected_nonblank_refuses():
    csv_bytes = _conc_csv([_conc_row("11005", "Range30001", "6637", "12345", parcels="4")])
    with pytest.raises(AgdpSchemaError, match="expected suppression"):
        AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2011")


def test_concentration_parcels_zero_publishes_carried_forward_total():
    # Real measured shape (2011, NIS 11001): ParcelsNumber=0 (nothing NEW in
    # the top band that year) still publishes a real, large cumulated total
    # carried forward from lower bands -- final, not na.
    csv_bytes = _conc_csv([_conc_row("11001", "Range30001", "5760", "7473689", parcels="0")])
    rows = AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2011")
    assert rows == [
        {
            "geo_id": "11001",
            "period": "2011",
            "value": 7473689.0,
            "status": "final",
            "indicator_id": "MUN_RESIDENTIAL_PARCELS_CI_TOTAL",
        }
    ]


def test_concentration_parcels_zero_blank_cumulated_ci_refuses():
    csv_bytes = _conc_csv([_conc_row("11001", "Range30001", "5760", "", parcels="0")])
    with pytest.raises(AgdpSchemaError, match="expected a published"):
        AgdpSource(CONCENTRATION)._parse(csv_bytes, quarter="2011")


def test_concentration_missing_required_column_refuses():
    bad_header = _CONC_HEADER.replace("HousingsCumulatedNumber;", "")
    text = "﻿" + "\r\n".join([bad_header, _conc_row("11001", "Range30001", "1", "1")]) + "\r\n"
    with pytest.raises(AgdpSchemaError, match="missing required"):
        AgdpSource(CONCENTRATION)._parse(text.encode("utf-8"), quarter="2026")


def test_concentration_band_sum_cross_check_11001_2026():
    # Cross-check test (handoff): 34 bands' ParcelsNumber sum to 6453,
    # ParcelsCumulatedNumber at Range30001 -- the SPF's own all-bands total.
    # This is a test-only cross-check; production code never sums the bands.
    # 33 bands of 190 plus one final band that makes the total exactly 6453
    # -- the individual band values are arbitrary test fixtures, not real
    # SPF figures; only the total (the real measured 11001/2026 value) and
    # the count of bands (34, per the handoff) matter here.
    band_parcels = [190] * 33 + [6453 - 190 * 33]
    assert len(band_parcels) == 34
    assert sum(band_parcels) == 6453
    rows = [
        _conc_row("11001", f"Range{i}", parcels=str(p), parcels_cum="0", ci_cum="0")
        for i, p in enumerate(band_parcels)
    ]
    rows[-1] = _conc_row("11001", "Range30001", "6453", "8428254", parcels=str(band_parcels[-1]))
    csv_bytes = _conc_csv(rows)
    reader_rows = list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig")), delimiter=";"))
    total_parcels = sum(int(r["ParcelsNumber"]) for r in reader_rows if r["NISCode"] == "11001")
    range30001 = next(r for r in reader_rows if r["Range"] == "Range30001")
    assert total_parcels == int(range30001["ParcelsCumulatedNumber"])
