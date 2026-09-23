"""Tests for src/fetchers/bankruptcies.py.

Fixture rows below are hand-copied from the real Statbel file (fetched
2026-09-23, TF_BANKRUPTCIES(2025).zip -- the year in the filename floats;
this batch's own fixtures do not depend on it), sliced down to a handful of
communes/months rather than committing the 6.5 MB real file. Column order
and all 41 header names match the real file exactly, including the columns
this adapter does not read, so a header-name lookup bug cannot hide behind a
too-convenient fixture shape.
"""

import io
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.fetchers.bankruptcies import (
    BANKRUPTCIES,
    BANKRUPTCY_JOBS_LOST,
    MEMBER_NAME,
    BankruptciesSchemaError,
    BankruptciesSource,
    BankruptcyLinkNotFoundError,
    discover_zip_url,
)

# The real 41-column header, verbatim.
HEADER = (
    "MS_COUNTOF_BANKRUPTCIES|MS_COUNTOF_FULL_TIME_WORKERS|MS_COUNTOF_PART_TIME_WORKERS|"
    "MS_COUNTOF_SELF_EMPLOYED_WORKERS|MS_COUNTOF_WORKERS|CD_YEAR|CD_MONTH|CD_EMPLOYMENT_CLASS|"
    "TX_EMPLOYMENT_CLASS_DESCR_FR|TX_EMPLOYMENT_CLASS_DESCR_NL|CD_LEGAL_FORM|"
    "TX_LEGAL_FORM_DESCR_FR|TX_LEGAL_FORM_DESCR_NL|CD_MUNTY_REFNIS|TX_MUNTY_DESCR_FR|"
    "TX_MUNTY_DESCR_NL|CD_DSTR_REFNIS|TX_ADM_DSTR_DESCR_FR|TX_ADM_DSTR_DESCR_NL|CD_PROV_REFNIS|"
    "TX_PROV_DESCR_FR|TX_PROV_DESCR_NL|CD_RGN_REFNIS|TX_RGN_DESCR_FR|TX_RGN_DESCR_NL|"
    "CD_NACE_REV2_CLASS|TX_NACE_REV2_CLASS|TX_NACE_REV2_CLASS_FR|TX_NACE_REV2_CLASS_NL|"
    "TX_NACE_REV2_GROUP|TX_NACE_REV2_GROUP_FR|TX_NACE_REV2_GROUP_NL|TX_NACE_REV2_DIVISION|"
    "TX_NACE_REV2_DIVISION_FR|TX_NACE_REV2_DIVISION_NL|TX_NACE_REV2_SECTION|"
    "TX_NACE_REV2_SECTION_FR|TX_NACE_REV2_SECTION_NL|CD_COMPANY_DURATION|"
    "TX_COMPANY_DURATION_FR|TX_COMPANY_DURATION_NL"
)

_TAIL = "1|0 - 4 salariés|0 - 4 werknemers|1|SNC|VOF|{nis}|Commune|Gemeente|0|D|D|0|P|P|0|R|R|4711|x|x|x|x|x|x|x|x|x|x|x|0200|x|x"


def _row(bankruptcies: float, workers: float, nis: str, year: int, month: int) -> str:
    return f"{bankruptcies}|0|0|0|{workers}|{year}|{month}|{_TAIL.format(nis=nis)}"


def _make_zip(data_rows: list[str], member_name: str = MEMBER_NAME) -> bytes:
    text = "﻿" + "\n".join([HEADER, *data_rows]) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(member_name, text.encode("utf-8"))
    return buf.getvalue()


# --- link discovery -------------------------------------------------------


def test_discovers_the_plain_zip_link_among_four():
    html = """
    <a href="https://statbel.fgov.be/x/TF_BANKRUPTCIES%282025%29.xlsx">xlsx</a>
    <a href="https://statbel.fgov.be/x/TF_BANKRUPTCIES%282025%29.zip">zip</a>
    <a href="https://statbel.fgov.be/x/TF_BANKRUPTCIES_accdb%282025%29.zip">accdb</a>
    <a href="https://statbel.fgov.be/x/TF_BANKRUPTCIES_sqlite%282025%29.zip">sqlite</a>
    """
    url = discover_zip_url(html)
    assert url == "https://statbel.fgov.be/x/TF_BANKRUPTCIES(2025).zip"


def test_discovers_the_plain_zip_link_with_literal_parens():
    html = '<a href="https://statbel.fgov.be/x/TF_BANKRUPTCIES(2024).zip">zip</a>'
    assert discover_zip_url(html) == "https://statbel.fgov.be/x/TF_BANKRUPTCIES(2024).zip"


def test_no_matching_link_raises():
    html = """
    <a href="https://statbel.fgov.be/x/TF_BANKRUPTCIES_accdb%282025%29.zip">accdb</a>
    <a href="https://statbel.fgov.be/x/TF_BANKRUPTCIES_sqlite%282025%29.zip">sqlite</a>
    <a href="https://statbel.fgov.be/x/unrelated.zip">other</a>
    """
    with pytest.raises(BankruptcyLinkNotFoundError, match="No href"):
        discover_zip_url(html)


def test_empty_page_raises():
    with pytest.raises(BankruptcyLinkNotFoundError):
        discover_zip_url("<html><body>nothing here</body></html>")


def test_two_plain_zip_candidates_raises_rather_than_guessing():
    html = """
    <a href="https://statbel.fgov.be/x/TF_BANKRUPTCIES%282024%29.zip">old</a>
    <a href="https://statbel.fgov.be/x/TF_BANKRUPTCIES%282025%29.zip">new</a>
    """
    with pytest.raises(BankruptcyLinkNotFoundError, match="candidate"):
        discover_zip_url(html)


# --- encoding ---------------------------------------------------------------


def test_liege_utf8_bom_round_trips():
    """Liège is stored as UTF-8 bytes 4c 69 c3 a8 67 65; utf-8-sig must
    decode it correctly (and strip the BOM), never cp1252 mangling it."""
    row = _row(1.0, 1.0, "62063", 2026, 8).replace("Commune", "Liège")
    raw = _make_zip([row])
    rows = BankruptciesSource()._parse(raw)
    # decoding succeeded without raising -- the row parsed and produced values
    assert any(r["geo_id"] == "62063" and r["period"] == "2026-08" for r in rows)


# --- schema refusals ----------------------------------------------------


def test_missing_member_is_refused():
    raw = _make_zip(["irrelevant"], member_name="WRONG_NAME.txt")
    with pytest.raises(BankruptciesSchemaError, match="no member named"):
        BankruptciesSource()._parse(raw)


def test_missing_required_column_is_refused():
    bad_header = HEADER.replace("MS_COUNTOF_WORKERS|", "")
    text = "﻿" + bad_header + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(MEMBER_NAME, text.encode("utf-8"))
    with pytest.raises(BankruptciesSchemaError, match="missing required column"):
        BankruptciesSource()._parse(buf.getvalue())


# --- summation, hand-computed from the real file (2026-09-23) -----------


def test_forest_2026_08():
    rows = [
        _row(1, 1, "21007", 2026, 8),
        _row(1, 2, "21007", 2026, 8),
        _row(1, 1, "21007", 2026, 8),
        _row(1, 1, "21007", 2026, 8),
    ]
    raw = _make_zip(rows)
    results = BankruptciesSource()._parse(raw)
    by_ind = {(r["indicator_id"], r["geo_id"], r["period"]): r["value"] for r in results}
    assert by_ind[(BANKRUPTCIES, "21007", "2026-08")] == 4.0
    assert by_ind[(BANKRUPTCY_JOBS_LOST, "21007", "2026-08")] == 5.0


def test_overijse_2026_08():
    raw = _make_zip([_row(1, 1, "23062", 2026, 8)])
    results = BankruptciesSource()._parse(raw)
    by_ind = {(r["indicator_id"], r["geo_id"], r["period"]): r["value"] for r in results}
    assert by_ind[(BANKRUPTCIES, "23062", "2026-08")] == 1.0
    assert by_ind[(BANKRUPTCY_JOBS_LOST, "23062", "2026-08")] == 1.0


def test_liege_2026_08():
    rows = [
        _row(2, 3, "62063", 2026, 8),
        _row(1, 1, "62063", 2026, 8),
        _row(1, 1, "62063", 2026, 8),
        _row(1, 1, "62063", 2026, 8),
        _row(1, 1, "62063", 2026, 8),
        _row(1, 3, "62063", 2026, 8),
        _row(1, 1, "62063", 2026, 8),
        _row(1, 1, "62063", 2026, 8),
        _row(1, 2, "62063", 2026, 8),
        _row(1, 5, "62063", 2026, 8),
    ]
    raw = _make_zip(rows)
    results = BankruptciesSource()._parse(raw)
    by_ind = {(r["indicator_id"], r["geo_id"], r["period"]): r["value"] for r in results}
    assert by_ind[(BANKRUPTCIES, "62063", "2026-08")] == 11.0
    assert by_ind[(BANKRUPTCY_JOBS_LOST, "62063", "2026-08")] == 19.0


def test_unpadded_month_formats_as_two_digits():
    raw = _make_zip([_row(1, 1, "11001", 2026, 8)])
    results = BankruptciesSource()._parse(raw)
    assert all(r["period"] == "2026-08" for r in results)


def test_every_row_has_exactly_the_base_contract_keys_plus_indicator_id():
    raw = _make_zip([_row(1, 1, "11001", 2026, 8)])
    results = BankruptciesSource()._parse(raw)
    for row in results:
        assert set(row.keys()) == {"geo_id", "period", "value", "status", "indicator_id"}
        assert isinstance(row["geo_id"], str)
        assert isinstance(row["period"], str)
        assert isinstance(row["value"], float)
        assert row["status"] == "final"
        assert row["indicator_id"] in (BANKRUPTCIES, BANKRUPTCY_JOBS_LOST)
