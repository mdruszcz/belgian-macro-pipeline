"""SPF Finances' communal additional personal-income-tax rate (taxe communale
additionnelle a l'IPP / aanvullende gemeentebelasting op de personenbelasting)
-- MUN_IPP_ADDITIONAL_RATE, docs/features/ipp_rate.md.

ONE XLSX PER TAX YEAR, at a fixed URL pattern:
`https://fin.belgium.be/sites/default/files/media/documents/taux-taxe-communale-{year}.xlsx`.
Measured 2026-09-23: 2024, 2025 and 2026 are live; 2018-2023 return 404 as
XLSX (PDF-only for those years, out of scope -- see
scripts/sync_ipp_rate.py's module docstring for the year-discovery loop that
tells "not yet published" apart from "the URL pattern changed").

ONE SHEET, 'Liste communes', header ('VILLE OU COMMUNE', 'Taux (%)') --
`_parse` checks both the sheet name and the header by name, never position,
and refuses on any drift (CLAUDE.md rule 13). Measured row counts: 581 for
2024 (pre-2025 merger commune count), 565 for 2025 and 2026.

CONTRACT DEVIATION, same shape as src/fetchers/bankruptcies.py and
src/fetchers/population_movement.py: `_parse` resolves nothing. `geo_id`
here is not even a raw NIS code -- the file carries no NIS codes at all, only
a commune NAME -- so it is the raw name string exactly as published, and
`period` is the tax year exactly as published (a string, e.g. "2026"), per
the handoff: the tax year is stored as-is, not translated to an income
year. The tax-year-to-income-year mapping itself is decided (maintainer,
2026-09-26: tax year T taxes income year T-1 -- see docs/features/
ipp_rate.md) but nothing in this adapter uses it; `period` stays the tax
year as published. scripts/sync_ipp_rate.py resolves name -> NIS against the
commune map valid at f"{tax_year}-01-01" (after accent/case/punctuation
normalisation), then NIS -> geo_id via resolve_geo(conn, nis, tax_year) --
the same adapter/sync split bankruptcies.py and population_movement.py use,
for the same reason: name resolution needs a live db connection this layer
does not have. tests/test_source_contract.py filters this adapter's rows to
the base four keys before asserting on them, the same bankruptcies-case
pattern.

STATUS IS ALWAYS 'final': the file publishes the year's settled rate, no
provisional marker anywhere in it. A rate of exactly 0 (Knokke-Heist, 2026)
is a real measured zero, not missing and not 'na' (CLAUDE.md rule 26) --
`_parse` does not special-case it; the value simply passes through.

NO NIS CODES, NO PER-ROW FILTERING TRAP unlike population_movement.py: every
row in 'Liste communes' is a commune name and a rate, nothing else to
classify or drop. A row whose rate cell is not numeric, or whose name cell
is blank, is a schema violation and fails the whole parse (CLAUDE.md rule
13) rather than being silently skipped.
"""

from __future__ import annotations

import io

import openpyxl

from src.fetchers.base import MunicipalTimeSeriesSource

SHEET_NAME = "Liste communes"
EXPECTED_HEADER = ("VILLE OU COMMUNE", "Taux (%)")

#: URL pattern for one tax year's file -- interpolated by
#: scripts/sync_ipp_rate.py's year-discovery loop, never a single hard-coded
#: year (CLAUDE.md rule 13).
URL_PATTERN = (
    "https://fin.belgium.be/sites/default/files/media/documents/taux-taxe-communale-{year}.xlsx"
)

INDICATOR_ID = "MUN_IPP_ADDITIONAL_RATE"


class IppRateSchemaError(ValueError):
    """The workbook is not the documented shape."""


class IppRateSource(MunicipalTimeSeriesSource):
    """Contract deviation, documented in the module docstring and tested in
    tests/test_source_contract.py: `geo_id` is the raw commune NAME as
    published (there is no NIS code in this file at all), not a resolved
    `be:mun:` id -- name-to-NIS-to-geo_id resolution is
    scripts/sync_ipp_rate.py's job, since it needs a live db connection this
    layer does not have.
    """

    source_id = "spf_finances"
    adapter = "spf_finances"
    raw_extension = "xlsx"

    def _parse(self, raw: bytes, *, tax_year: str, **kwargs) -> list[dict]:
        workbook = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        try:
            if SHEET_NAME not in workbook.sheetnames:
                raise IppRateSchemaError(
                    f"Expected a sheet named {SHEET_NAME!r}. Sheets found: "
                    f"{workbook.sheetnames}. Refusing to guess a replacement "
                    "(CLAUDE.md rule 13)."
                )
            sheet = workbook[SHEET_NAME]
            rows_iter = sheet.iter_rows(values_only=True)
            try:
                header = next(rows_iter)
            except StopIteration as exc:
                raise IppRateSchemaError(
                    f"Sheet {SHEET_NAME!r} is empty -- no header row."
                ) from exc

            header_pair = tuple(header[:2]) if header else ()
            if header_pair != EXPECTED_HEADER:
                raise IppRateSchemaError(
                    f"Expected header {EXPECTED_HEADER!r} in sheet {SHEET_NAME!r}, found "
                    f"{header_pair!r}. Refusing to guess a replacement (CLAUDE.md rule 13)."
                )

            results: list[dict] = []
            for row in rows_iter:
                if row is None or all(cell is None for cell in row):
                    continue
                if len(row) < 2:
                    raise IppRateSchemaError(
                        f"Row {row!r} in sheet {SHEET_NAME!r} has fewer than 2 columns."
                    )
                name, rate = row[0], row[1]
                if not isinstance(name, str) or not name.strip():
                    raise IppRateSchemaError(
                        f"Row {row!r} in sheet {SHEET_NAME!r}: commune name is not a "
                        "non-empty string. Refusing to guess (CLAUDE.md rule 13)."
                    )
                if not isinstance(rate, (int, float)) or isinstance(rate, bool):
                    raise IppRateSchemaError(
                        f"Row for {name!r} in sheet {SHEET_NAME!r}: rate is {rate!r}, not "
                        "numeric. Refusing to guess (CLAUDE.md rule 13)."
                    )
                results.append(
                    {
                        "geo_id": name.strip(),
                        "period": tax_year,
                        "value": float(rate),
                        "status": "final",
                    }
                )

            if not results:
                raise IppRateSchemaError(
                    f"Sheet {SHEET_NAME!r} produced zero data rows for tax year {tax_year!r}."
                )
            return results
        finally:
            workbook.close()
