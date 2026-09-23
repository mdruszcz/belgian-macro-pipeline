"""Tests for src/fetchers/spf_finances.py.

Fixture workbooks are built in-memory with openpyxl, reproducing the real
file's exact shape (one sheet 'Liste communes', header
('VILLE OU COMMUNE', 'Taux (%)')) -- measured directly against the real
2024/2025/2026 files, 2026-09-23. The specific rate values used below
(Aalst 7.5, Aartselaar 5.0, Knokke-Heist 0.0, Saint-Nicolas 8.5,
Sint-Niklaas 7.5, all tax year 2026) are hand-copied from those real files,
not invented.
"""

import io
import sys
from pathlib import Path

import openpyxl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.fetchers.spf_finances import (
    EXPECTED_HEADER,
    SHEET_NAME,
    IppRateSchemaError,
    IppRateSource,
)


def _make_workbook(
    rows: list[tuple], *, sheet_name: str = SHEET_NAME, header=EXPECTED_HEADER
) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = sheet_name
    if header is not None:
        ws.append(list(header))
    for row in rows:
        ws.append(list(row))
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# Hand-copied from the real 2026 file (docs/features/ipp_rate.md,
# measured 2026-09-23).
REAL_2026_ROWS = [
    ("Aalst", 7.5),
    ("Aartselaar", 5.0),
    ("Knokke-Heist", 0.0),
    ("Saint-Nicolas", 8.5),
    ("Sint-Niklaas", 7.5),
]


def test_parses_the_real_shape_and_carries_a_real_measured_zero():
    raw = _make_workbook(REAL_2026_ROWS)
    rows = IppRateSource()._parse(raw, tax_year="2026")

    by_name = {r["geo_id"]: r for r in rows}
    assert by_name["Aalst"]["value"] == 7.5
    assert by_name["Aartselaar"]["value"] == 5.0
    assert by_name["Saint-Nicolas"]["value"] == 8.5
    assert by_name["Sint-Niklaas"]["value"] == 7.5

    # Knokke-Heist's 0 is a real published rate, not a missing/na value --
    # CLAUDE.md rule 26. status is still 'final', value is exactly 0.0, not
    # None and not skipped.
    knokke = by_name["Knokke-Heist"]
    assert knokke["value"] == 0.0
    assert knokke["status"] == "final"

    for row in rows:
        assert row["period"] == "2026"
        assert row["status"] == "final"
        assert isinstance(row["value"], float)


def test_every_row_carries_the_base_contract_keys():
    raw = _make_workbook(REAL_2026_ROWS)
    rows = IppRateSource()._parse(raw, tax_year="2026")
    assert rows
    for row in rows:
        assert {"geo_id", "period", "value", "status"} <= set(row.keys())
        assert isinstance(row["geo_id"], str)
        assert isinstance(row["period"], str)
        assert row["status"] in {"final", "provisional", "estimate", "revised", "suppressed", "na"}


def test_period_is_the_tax_year_exactly_as_given_not_derived():
    raw = _make_workbook(REAL_2026_ROWS)
    rows = IppRateSource()._parse(raw, tax_year="2024")
    assert all(r["period"] == "2024" for r in rows)


def test_wrong_sheet_name_refuses():
    raw = _make_workbook(REAL_2026_ROWS, sheet_name="Sheet1")
    with pytest.raises(IppRateSchemaError):
        IppRateSource()._parse(raw, tax_year="2026")


def test_wrong_header_refuses():
    raw = _make_workbook(REAL_2026_ROWS, header=("Commune", "Rate"))
    with pytest.raises(IppRateSchemaError):
        IppRateSource()._parse(raw, tax_year="2026")


def test_missing_header_entirely_refuses():
    raw = _make_workbook([], header=None)
    with pytest.raises(IppRateSchemaError):
        IppRateSource()._parse(raw, tax_year="2026")


def test_non_numeric_rate_refuses():
    raw = _make_workbook([("Aalst", "not a number")])
    with pytest.raises(IppRateSchemaError):
        IppRateSource()._parse(raw, tax_year="2026")


def test_blank_commune_name_refuses():
    raw = _make_workbook([("", 5.0)])
    with pytest.raises(IppRateSchemaError):
        IppRateSource()._parse(raw, tax_year="2026")


def test_zero_data_rows_refuses():
    raw = _make_workbook([])
    with pytest.raises(IppRateSchemaError):
        IppRateSource()._parse(raw, tax_year="2026")


def test_blank_trailer_rows_are_skipped_not_treated_as_data():
    rows_with_blank = list(REAL_2026_ROWS) + [(None, None)]
    raw = _make_workbook(rows_with_blank)
    rows = IppRateSource()._parse(raw, tax_year="2026")
    assert len(rows) == len(REAL_2026_ROWS)


def test_determinism_same_bytes_same_output():
    raw = _make_workbook(REAL_2026_ROWS)
    first = IppRateSource()._parse(raw, tax_year="2026")
    second = IppRateSource()._parse(raw, tax_year="2026")
    assert first == second
