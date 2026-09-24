"""Statbel's population-movement workbook -- BIRTHS, DEATHS,
INTERNAL_MIGRATION_NET, INTERNATIONAL_MIGRATION_NET,
INTERNAL_MIGRATION_IN, INTERNAL_MIGRATION_OUT, Block NS3 (+ this batch,
2026-09-24, internal migration arrivals/departures).

TWO STEPS, same discipline as src/fetchers/bankruptcies.py: the theme page
(https://statbel.fgov.be/fr/themes/population/mouvement-de-la-population)
links one XLSX -- `discover_xlsx_url()` reads the href off the page rather
than ever hard-coding the filename, and fails loudly if it finds zero or
more than one candidate (CLAUDE.md rule 13).

ONE SHEET PER YEAR, 1992 down to 2025 as of 2026-09-23 (measured), named
by the bare year ("2025", "2024", ...). Header spans rows 2-4 (merged
cells); data starts at row 5. `_parse` validates the four column labels it
reads by name, not position: D/NAISSANCES, E/DECES, I/SOLDE (under
G/ENTREES-H/SORTIES-I/SOLDE, MOUVEMENT MIGRATOIRE INTERNE), and
P/SOLDE (under J-L/ENTREES-M-O/SORTIES-P/SOLDE, MOUVEMENT MIGRATOIRE
INTERNATIONAL). A header that has moved is refused, never silently
re-indexed.

GEOGRAPHY IS PERIOD-CORRECT, NOT PINNED -- the opposite convention from
police.be/real-estate/bankruptcies, and load-bearing here. Each row resolves
against the commune map in effect at the START of its OWN sheet year --
resolve_geo(conn, nis, sheet_year), the ordinary rule (CLAUDE.md rule 3) --
NOT at sheet_year + 1. An earlier version of this adapter resolved at
sheet_year + 1 on the theory that column W's own label, "POPULATION AU 31
DECEMBRE (SOIT AU 1/1 DE L'ANNEE SUIVANTE)", meant the whole sheet's commune
grid was dated to the following 1 January; that reading was measured against
the real file and rejected (see docs/features/population_movement.md) --
resolving at sheet_year + 1 makes a transition-year sheet's forward-mapped
codes (see below) look valid for THAT sheet's own period, which silently
manufactures rows for communes that did not yet exist when the events being
counted happened, and corrupts every downstream coverage calculation that
compares this indicator's per-period geo_id set against the calendar map
(scripts/export_aggregates_csv.py's `_universe_resolver`).

TRANSITION SHEETS carry forward-mapped codes ahead of their own merger date.
Measured directly (2026-09-23) against the real file: the sheet named "2018"
already carries the NEW codes the 2019-01-01 merger created (12041, 44083-
85, 45068, 51067-69, 55085-86, 57096-97, 58001-04, 72042-43 -- eighteen of
them) -- and NOT their pre-merger predecessor codes, which are simply absent
from that sheet. Likewise sheet "2024" already carries 82039
(Bastogne+Bertogne, valid_from 2024-12-02, inside sheet "2024"'s own
calendar year) and the twelve other 2025-01-01 merger codes (thirteen
total), again with no predecessor rows. Resolved at the sheet's own period,
none of these thirty-one rows resolves (the commune did not exist yet at
that sheet-year's 1 January), so scripts/sync_population_movement.py drops
them -- loudly, counted and logged per sheet, and checked against an
explicit expected set, per the handoff: Statbel publishes the transition
year only on the post-merger map, so on that year's own map these communes'
figures genuinely do not exist for that one year (the missing state, not a
zero) -- neither the predecessor (whose code the sheet does not carry) nor
the successor (which did not yet legally exist) has a defensible row.

ROW-FILTERING TRAP, per the handoff: do not select commune rows by "5-digit
code not ending in 000" -- that includes 20001/20002 (the two Brabant
provinces) alongside the real communes. `_parse` does not filter rows at
all; every row with a 5-digit numeric CODE INS is emitted with its raw code,
unfiltered, together with the sheet's own year string as `period` (this
adapter resolves nothing itself; resolve_geo needs a live db connection,
which is scripts/sync_population_movement.py's job, the same adapter/sync
split src/fetchers/bankruptcies.py uses for its own, different reason).
scripts/sync_population_movement.py does the real filtering, by checking
each code resolves to a `municipality` level in the geographies table at
the row's own sheet-year as-of date -- a code that resolves to a different
level (province, arrondissement, region, country) is dropped as an expected
non-municipal row; a code that resolves to NOTHING is either an expected
transition-sheet exclusion (above) or, outside that expected set, causes
the whole run to be refused (never partial), the same sync_police.py
pattern.

SIX INDICATORS, ONE FETCH: like bankruptcies.py, one row in the source
produces up to six output rows, one per indicator, each carrying a fifth
key `indicator_id` beyond the base MunicipalTimeSeriesSource four. All six
values (NAISSANCES, DECES, internal SOLDE, international SOLDE, and --
added 2026-09-24 -- internal ENTREES/SORTIES, columns G and H) are the
publisher's own totals -- nothing here recomputes or derives a value
(CLAUDE.md rule 6). INTERNAL_MIGRATION_IN (G/ENTREES) and
INTERNAL_MIGRATION_OUT (H/SORTIES) are published alongside I/SOLDE under the
same "MOUVEMENT MIGRATOIRE INTERNE" group; INTERNAL_MIGRATION_NET =
INTERNAL_MIGRATION_IN - INTERNAL_MIGRATION_OUT, as published by Statbel
(not recomputed here -- verified equal to the existing SOLDE column for
every row parsed, but the sync writes I/SOLDE unchanged, and IN/OUT are
each written unchanged too; three independent published numbers, not two
plus a derivation).

Status is 'final' on every row: this file publishes settled annual
demographic accounts, no provisional marker anywhere in it.
"""

from __future__ import annotations

import io
import re

import openpyxl

from src.fetchers.base import MunicipalTimeSeriesSource

BIRTHS = "BIRTHS"
DEATHS = "DEATHS"
INTERNAL_MIGRATION_NET = "INTERNAL_MIGRATION_NET"
INTERNATIONAL_MIGRATION_NET = "INTERNATIONAL_MIGRATION_NET"
INTERNAL_MIGRATION_IN = "INTERNAL_MIGRATION_IN"
INTERNAL_MIGRATION_OUT = "INTERNAL_MIGRATION_OUT"

ALL_INDICATORS = (
    BIRTHS,
    DEATHS,
    INTERNAL_MIGRATION_NET,
    INTERNATIONAL_MIGRATION_NET,
    INTERNAL_MIGRATION_IN,
    INTERNAL_MIGRATION_OUT,
)

#: Column letters -> 0-based index, for readability at the call sites below.
_COL_CODE_INS = 0
_COL_NAISSANCES = 3  # D
_COL_DECES = 4  # E
_COL_INTERNAL_ENTREES = 6  # G
_COL_INTERNAL_SORTIES = 7  # H
_COL_INTERNAL_SOLDE = 8  # I
_COL_INTERNATIONAL_SOLDE = 15  # P

#: The exact row-4 label expected at each column above, plus the row-2/row-3
#: group labels that must sit above it -- checked together so a Statbel
#: reorder inside a group (e.g. swapping ENTREES/SORTIES) is caught even
#: though row 4 alone would still say "ENTREES"/"SORTIES" at a shifted
#: position.
#: Row 2/3 group labels are only written to the merge's TOP-LEFT cell by
#: openpyxl (read_only mode does not fan a merged value out across the
#: whole span) -- measured directly against the real file, 2026-09-23:
#: row 2 col G ("MOUVEMENT MIGRATOIRE INTERNE") is the merge's origin for
#: columns G-I, so column I itself reads None at row 2, not a repeat of the
#: label. Checked at the merge's own origin column, not at the data column.
_COL_INTERNAL_GROUP_ORIGIN = 6  # G

_EXPECTED_HEADER = {
    _COL_NAISSANCES: ("MOUVEMENT NATUREL", None, "NAISSANCES"),
    _COL_DECES: (None, None, "DECES"),
    _COL_INTERNAL_ENTREES: (None, None, "ENTREES"),
    _COL_INTERNAL_SORTIES: (None, None, "SORTIES"),
    _COL_INTERNAL_SOLDE: (None, None, "SOLDE"),
    _COL_INTERNATIONAL_SOLDE: (None, "SOLDE", None),
}

#: Matches the one href this page carries: a `.xlsx` link whose path contains
#: "pop1992-mov" (the workbook's own filename stem, stable across the
#: URL-encoded space in "5.2 Loop van de bevolking").
_XLSX_HREF = re.compile(r'href="([^"]*pop1992-mov[^"]*\.xlsx)"', re.IGNORECASE)

_YEAR_SHEET = re.compile(r"^(19|20)\d{2}$")


class PopulationMovementLinkNotFoundError(Exception):
    """The theme page has no href matching the population-movement workbook."""


class PopulationMovementSchemaError(ValueError):
    """The workbook is not the documented shape."""


def discover_xlsx_url(html: str) -> str:
    """The href for the population-movement XLSX on the Statbel theme page.

    Raises PopulationMovementLinkNotFoundError -- never falls back to a
    cached or hard-coded URL -- if zero or more than one link matches
    (CLAUDE.md rule 13).
    """
    candidates = _XLSX_HREF.findall(html)
    if not candidates:
        raise PopulationMovementLinkNotFoundError(
            "No href on the theme page matches a pop1992-mov*.xlsx link. Refusing to fall "
            "back to a cached or hard-coded URL (CLAUDE.md rule 13)."
        )
    unique = sorted(set(candidates))
    if len(unique) > 1:
        raise PopulationMovementLinkNotFoundError(
            f"{len(unique)} distinct candidate links matched: {unique}. Refusing to guess "
            "which one is current."
        )
    return unique[0].replace("&amp;", "&")


def _check_header(sheet_name: str, row2: tuple, row3: tuple, row4: tuple) -> None:
    for col, (expect_r2, expect_r3, expect_r4) in _EXPECTED_HEADER.items():
        if expect_r2 is not None and row2[col] != expect_r2:
            raise PopulationMovementSchemaError(
                f"Sheet {sheet_name!r}: expected row 2 column {col} to be {expect_r2!r}, "
                f"found {row2[col]!r}. Refusing to guess a replacement (CLAUDE.md rule 13)."
            )
        if expect_r3 is not None and row3[col] != expect_r3:
            raise PopulationMovementSchemaError(
                f"Sheet {sheet_name!r}: expected row 3 column {col} to be {expect_r3!r}, "
                f"found {row3[col]!r}. Refusing to guess a replacement (CLAUDE.md rule 13)."
            )
        if expect_r4 is not None and row4[col] != expect_r4:
            raise PopulationMovementSchemaError(
                f"Sheet {sheet_name!r}: expected row 4 column {col} to be {expect_r4!r}, "
                f"found {row4[col]!r}. Refusing to guess a replacement (CLAUDE.md rule 13)."
            )
    if row2[_COL_CODE_INS] != "CODE INS":
        raise PopulationMovementSchemaError(
            f"Sheet {sheet_name!r}: expected column A row 2 to be 'CODE INS', found "
            f"{row2[_COL_CODE_INS]!r}. Refusing to guess a replacement (CLAUDE.md rule 13)."
        )
    if row2[_COL_INTERNAL_GROUP_ORIGIN] != "MOUVEMENT MIGRATOIRE INTERNE":
        raise PopulationMovementSchemaError(
            f"Sheet {sheet_name!r}: expected row 2 column {_COL_INTERNAL_GROUP_ORIGIN} to be "
            f"'MOUVEMENT MIGRATOIRE INTERNE', found {row2[_COL_INTERNAL_GROUP_ORIGIN]!r}. "
            "Refusing to guess a replacement (CLAUDE.md rule 13)."
        )


class PopulationMovementSource(MunicipalTimeSeriesSource):
    """Contract deviation, documented in the module docstring and tested in
    tests/test_source_contract.py: `geo_id` is the raw NIS string (never
    resolved inline) and `period` is the sheet's own year string, exactly as
    published -- resolve_geo(conn, nis, period) against this same sheet
    year is scripts/sync_population_movement.py's job, since resolving needs
    a live db connection this layer does not have. Each row also carries a
    fifth key, `indicator_id`.
    """

    source_id = "statbel"
    adapter = "population_movement"
    raw_extension = "xlsx"

    def _parse(self, raw: bytes, **kwargs) -> list[dict]:
        workbook = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        year_sheets = [name for name in workbook.sheetnames if _YEAR_SHEET.match(name)]
        if not year_sheets:
            raise PopulationMovementSchemaError(
                f"No sheet name matches a bare 4-digit year. Sheets found: "
                f"{workbook.sheetnames}. Refusing to guess a replacement (CLAUDE.md rule 13)."
            )

        results: list[dict] = []
        for sheet_name in year_sheets:
            sheet = workbook[sheet_name]
            header_rows = list(sheet.iter_rows(min_row=2, max_row=4, values_only=True))
            if len(header_rows) < 3:
                raise PopulationMovementSchemaError(
                    f"Sheet {sheet_name!r} has fewer than 4 rows -- no header to validate."
                )
            row2, row3, row4 = header_rows
            _check_header(sheet_name, row2, row3, row4)

            for row in sheet.iter_rows(min_row=5, values_only=True):
                code = row[_COL_CODE_INS]
                if code is None:
                    continue
                code_str = str(code).strip()
                if not code_str.isdigit() or len(code_str) != 5:
                    # Footer/attribution rows ("Source: Statbel ...") and any
                    # blank trailer -- not a data row, not an error either;
                    # every real data row's CODE INS is a 5-digit string.
                    continue

                values = {}
                for col, indicator_id in (
                    (_COL_NAISSANCES, BIRTHS),
                    (_COL_DECES, DEATHS),
                    (_COL_INTERNAL_SOLDE, INTERNAL_MIGRATION_NET),
                    (_COL_INTERNATIONAL_SOLDE, INTERNATIONAL_MIGRATION_NET),
                    (_COL_INTERNAL_ENTREES, INTERNAL_MIGRATION_IN),
                    (_COL_INTERNAL_SORTIES, INTERNAL_MIGRATION_OUT),
                ):
                    cell = row[col]
                    if not isinstance(cell, (int, float)):
                        raise PopulationMovementSchemaError(
                            f"Sheet {sheet_name!r}, CODE INS {code_str!r}: column for "
                            f"{indicator_id} is {cell!r}, not numeric. Refusing to guess "
                            "(CLAUDE.md rule 13)."
                        )
                    values[indicator_id] = float(cell)

                for indicator_id, value in values.items():
                    results.append(
                        {
                            "geo_id": code_str,
                            "period": sheet_name,
                            "value": value,
                            "status": "final",
                            "indicator_id": indicator_id,
                        }
                    )
        workbook.close()
        return results
