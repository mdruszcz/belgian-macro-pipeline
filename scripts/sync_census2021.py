"""Load Statbel's Census 2021 commune tables -- the first CURRENT municipal
data beyond population and fiscal income.

WHY THIS IS A MANUAL SOURCE. Statbel publishes ~140 Census 2021 open datasets
under CC BY 4.0, but only on `statbel.fgov.be`, which automation cannot read
(see docs/data_catalog.md). The Bestat API does carry Census 2021 -- datasources
IM_SOC_GEO_IND_CENSUS_2021 and IM_SOC_GEO_NUC_CENSUS_2021 -- but all 174 of
their views were probed across four locales and every one stops at province or
arrondissement. None reaches commune. So these files are hand-downloaded, the
same route as TF_SOC_POP_STRUCT and TF_PSNL_INC_TAX_MUNTY, and land in the
committed CSV store rather than the daily database (ADR 0002).

THESE FILES ARE EASIER THAN THE OTHER STATBEL SOURCES, in one specific way:
they carry a real NIS code in CD_REFNIS_LVL_4, so resolve_geo(nis, period)
applies directly. The Bestat business-units feed names communes only in French
text and needs the compound (name, arrondissement) match in
src/fetchers/statbel.py; nothing like that is needed here.

PERIOD IS 2021, and it is not an assumption. Summing HC03_1 gives 11,521,238
people over 581 communes, and the pipeline's independent population series
(TF_SOC_POP_STRUCT, reference date 1 January) gives the identical total for
2021 with all 581 communes matching EXACTLY, commune by commune. Two unrelated
Statbel products agreeing to the person is what fixes the reference date.

WHAT IS DELIBERATELY NOT LOADED. Total population is not stored again: it is
already POPULATION_BY_COMMUNE, the exact match above proves the two are
interchangeable, and a second copy would be a second source of truth for one
fact. Shares are not stored either -- they are derived from these counts and
that population at export time, so a revision to either recomputes them
(CLAUDE.md rule 6).
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.geography.resolve import UnknownGeographyError, resolve_geo  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
DEFAULT_CENSUS_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "statbel" / "census2021"

PERIOD = "2021"

# Which slice of which file becomes which indicator.
#
# `filters` selects rows by their coded columns; a column absent from the
# filter is summed over. Coded values are used, never the French labels: the
# labels are prose that could be re-worded in a future publication, the codes
# are the file's own keys.
#
# Every indicator here is a COUNT, deliberately. Statbel publishes these as
# counts and the shares a reader wants (share of foreign nationals, vacancy
# rate, average household size) are computed from them by the derived engine,
# so a correction to a count propagates instead of leaving a stale ratio.
EXTRACTS = {
    "POP_FOREIGN_NATIONALS": {
        "file": "TF_CENSUS_2021_HC08_1.xlsx",
        "measure": "MS_POP",
        "filters": {"CD_COC_LVL_1": {"FOR"}},
    },
    "POP_NON_EU_NATIONALS": {
        "file": "TF_CENSUS_2021_HC08_1.xlsx",
        "measure": "MS_POP",
        "filters": {"CD_COC_LVL_2": {"NEU"}},
    },
    "POP_BORN_ABROAD": {
        "file": "TF_CENSUS_2021_HC08_2.xlsx",
        "measure": "MS_POP",
        "filters": {"CD_POB_LVL_1": {"FOR"}},
    },
    "POP_FEMALE": {
        "file": "TF_CENSUS_2021_HC03_1.xlsx",
        "measure": "MS_POP",
        "filters": {"CD_SEX": {"F"}},
    },
    "POP_MARRIED": {
        "file": "TF_CENSUS_2021_HC03_3.xlsx",
        "measure": "MS_POP",
        "filters": {"CD_LMS": {"MAR"}},
    },
    "HOUSEHOLDS_PRIVATE": {
        "file": "TF_CENSUS_2021_HC35_1.xlsx",
        "measure": "MS_MENAGES",
        "filters": {},
    },
    "HOUSEHOLDS_SINGLE_PERSON": {
        "file": "TF_CENSUS_2021_HC35_1.xlsx",
        "measure": "MS_MENAGES",
        "filters": {"CD_TPH_LVL_2": {"P1"}},
    },
    "FAMILY_NUCLEI": {
        "file": "TF_CENSUS_2021_HC36_1.xlsx",
        "measure": "MS_NOYAUX",
        "filters": {},
    },
    "FAMILY_NUCLEI_SINGLE_PARENT": {
        "file": "TF_CENSUS_2021_HC36_1.xlsx",
        "measure": "MS_NOYAUX",
        # Single fathers and single mothers, summed: the split by sex of the
        # lone parent is a different question from "how many single-parent
        # families are there", which is the one a commune asks.
        "filters": {"CD_TFN": {"M1_CH", "F1_CH"}},
    },
    "DWELLINGS_OCCUPIED": {
        "file": "TF_CENSUS_2021_HC38_1.xlsx",
        "measure": "MS_LOGEMENTS",
        "filters": {"CD_OCS": {"DW_OC"}},
    },
    "DWELLINGS_TOTAL": {
        "file": "TF_CENSUS_2021_HC38_1.xlsx",
        "measure": "MS_LOGEMENTS",
        # No filter: occupied and unoccupied, every building type. The
        # denominator for the unoccupied share -- which must be the whole
        # stock, not just the occupied part.
        "filters": {},
    },
    "DWELLINGS_VACANT": {
        "file": "TF_CENSUS_2021_HC38_1.xlsx",
        "measure": "MS_LOGEMENTS",
        "filters": {"CD_OCS": {"DW_NOC"}},
    },
    "DWELLINGS_IN_SINGLE_UNIT_BUILDING": {
        "file": "TF_CENSUS_2021_HC38_1.xlsx",
        "measure": "MS_LOGEMENTS",
        "filters": {"CD_TOB_LVL_2": {"RES1"}},
    },
}

# T01_CAS_AGE_BE_NL.XLSX is a DIFFERENT file shape from the hypercube EXTRACTS
# above -- a per-table workbook, one NIS block of three rows (Mannen/Vrouwen/
# Totaal) per geography, rather than one flat row per (geo, category). Despite
# the "BE" in the filename (Statbel's convention for "national report", not
# "national-only geography" -- misleading, and worth knowing for the next
# maintainer download) it drills to commune: 583 NIS-6 codes in the sheet.
#
# CAS = "arbeidsmarktsituatie" (labour-market situation), not "civil status"
# as the maintainer's naming-convention note first guessed -- corrected here
# from the file's own contents, not from documentation.
#
# The 15-64 sheet is used, matching the EU Labour Force Survey's standard
# working-age population, so the resulting unemployment rate is comparable to
# published EU/Eurostat figures rather than an unusual age cut. Verified
# against the file's own Belgium total before trusting any commune row: 8.61%
# (462,991 unemployed of 5,376,113 in the labour force) -- the correct order
# of magnitude for 2021, a year still affected by the pandemic.
CAS_TABLE = {
    "file": "T01_CAS_AGE_BE_NL.XLSX",
    "sheet": "CENSUS_T01_2021_BE_CAS1564_2021",
    # column header (as it appears in the file) -> indicator_id
    "columns": {
        "1. Beroepsactieven": "CAS_LABOUR_FORCE",
        "1.1. Werkzame personen": "CAS_EMPLOYED",
        "1.2. Werklozen": "CAS_UNEMPLOYED",
        "2. Inactieven": "CAS_INACTIVE",
    },
}

NIS_COLUMN = "CD_REFNIS_LVL_4"


def _read_extract(path: Path, measure: str, filters: dict) -> dict[str, float]:
    """Sum one measure per commune, over the rows matching `filters`."""
    import openpyxl

    workbook = openpyxl.load_workbook(path, read_only=True)
    sheet = workbook[workbook.sheetnames[0]]
    rows = sheet.iter_rows(values_only=True)
    header = list(next(rows))

    missing = [c for c in (*filters, NIS_COLUMN, measure) if c not in header]
    if missing:
        raise ValueError(
            f"{path.name} is missing expected column(s) {missing}. Its columns are "
            f"{header}. Refusing to guess a replacement (CLAUDE.md rule 13) -- if "
            "Statbel restructured this file, the mapping in EXTRACTS must be "
            "re-derived from the new layout, not patched."
        )

    idx = {name: header.index(name) for name in header if name}
    totals: dict[str, float] = {}
    for row in rows:
        nis = row[idx[NIS_COLUMN]]
        if nis is None:
            continue
        if any(row[idx[column]] not in allowed for column, allowed in filters.items()):
            continue
        value = row[idx[measure]]
        if value is None:
            continue
        totals[str(nis)] = totals.get(str(nis), 0.0) + float(value)
    workbook.close()
    return totals


def _commune_nis_codes(conn: sqlite3.Connection) -> set[str]:
    """Every NIS code that has ever named a municipality -- current or
    historical, since Census 2021 uses the 2019-2024 geography and this
    table's block filter must recognise those codes too."""
    return {
        row[0]
        for row in conn.execute("SELECT nis_code FROM geographies WHERE level = 'municipality'")
    }


def _read_block_table(
    path: Path, sheet_name: str, columns: dict[str, str], commune_nis_codes: set[str]
) -> dict[str, dict]:
    """Parse T01_CAS_AGE_BE_NL.XLSX's block layout: one geography per THREE
    rows (Mannen, Vrouwen, Totaal), with the NIS code and geography name
    given only on the first row of each block.

    Returns indicator_id -> {nis: value}, taking only the 'Totaal' row of
    each block -- the per-sex breakdown is not loaded, matching the rest of
    this file's raw-counts-only, no-second-source-of-truth approach.
    """
    import openpyxl

    workbook = openpyxl.load_workbook(path, read_only=True)
    if sheet_name not in workbook.sheetnames:
        raise ValueError(
            f"{path.name} has no sheet {sheet_name!r}. Its sheets are "
            f"{workbook.sheetnames}. Refusing to guess a replacement (CLAUDE.md rule 13)."
        )
    sheet = workbook[sheet_name]
    rows = list(sheet.iter_rows(values_only=True))

    # Row 3 (0-indexed) holds the column headers, offset by three leading
    # blank/label columns (NIS, name, sex) -- verified against this file's
    # actual layout, not assumed from a generic table shape.
    header = rows[3]
    missing = [c for c in columns if c not in header]
    if missing:
        raise ValueError(
            f"{path.name}!{sheet_name} is missing expected column(s) {missing}. Its "
            f"columns are {header}. Refusing to guess a replacement (CLAUDE.md rule 13) "
            "-- if Statbel restructured this table, CAS_TABLE must be re-derived."
        )
    col_idx = {label: header.index(label) for label in columns}

    out: dict[str, dict[str, float]] = {indicator_id: {} for indicator_id in columns.values()}
    current_nis: str | None = None
    for row in rows[4:]:
        nis, _name, sex = row[0], row[1], row[2]
        if nis is not None:
            # This table reports EVERY geography level in one sheet --
            # country, region, province, arrondissement AND commune -- unlike
            # the hypercube EXTRACTS files, which are pre-filtered to commune
            # rows only. Verified: 638 NIS blocks total, only 583 of them
            # commune-shaped. Without this filter, resolve_geo happily
            # resolves the higher-level codes too (they are valid NIS codes,
            # just not communes) and silently writes an arrondissement's
            # figure into an indicator this pipeline treats as commune-only
            # everywhere else -- caught by inspecting the loaded rows before
            # trusting the count, not by a crash: 57 non-commune rows landed
            # under CAS_LABOUR_FORCE on the first run.
            #
            # Filtered against the geographies table's own commune codes,
            # NOT a "last three digits are zero" heuristic: that heuristic
            # missed Flemish/Walloon Brabant's split province codes 20001 and
            # 20002, which do not end in 000 and were caught only by
            # inspecting the resolved rows' actual geography level.
            nis_str = str(nis) if isinstance(nis, str) and nis.isdigit() else None
            current_nis = nis_str if nis_str in commune_nis_codes else None
        if current_nis is None or sex != "Totaal":
            continue
        for label, indicator_id in columns.items():
            value = row[col_idx[label]]
            if value is not None:
                out[indicator_id][current_nis] = float(value)
    workbook.close()
    return out


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('statbel', 'Statbel Bestat API', 'Statbel', 'statbel',
                'https://bestat.statbel.fgov.be/bestat/api', ?,
                'docs/data_catalog.md -- Census indicators', 'decennial (manual)', 1)
        """,
        (
            'See docs/data_catalog.md "Statbel licence" -- two Statbel documents, both '
            "confirmed to grant commercial reuse",
        ),
    )
    for indicator_id in (*EXTRACTS, *CAS_TABLE["columns"].values()):
        ind = indicator_configs[indicator_id]
        conn.execute(
            """
            INSERT OR IGNORE INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'statbel', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'sum', 1, 0, ?, 1)
            """,
            (
                indicator_id,
                ind["name"]["nl"],
                ind["name"]["fr"],
                ind["name"]["en"],
                ind.get("description", {}).get("en", ""),
                ind["frequency"],
                ind["unit"],
                ind["preferred_direction"],
                f"config/indicators/{indicator_id}.yaml",
            ),
        )
    conn.commit()


def sync(db_path: Path, census_dir: Path, reference_rows_only: bool = False) -> tuple[int, int]:
    indicator_configs, _sources = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, indicator_configs)
    if reference_rows_only:
        conn.close()
        return 0, 0

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("statbel", "census2021", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    period_start, period_end = derive_period_bounds(PERIOD, "A")
    rows_read = rows_written = 0
    unresolved: list[tuple[str, str]] = []

    for indicator_id, spec in EXTRACTS.items():
        path = census_dir / spec["file"]
        if not path.is_file():
            raise FileNotFoundError(
                f"{path} not found. Census 2021 files are hand-downloaded from "
                "statbel.fgov.be -- see docs/features/manual_sources.md."
            )
        totals = _read_extract(path, spec["measure"], spec["filters"])
        rows_read += len(totals)

        for nis, value in sorted(totals.items()):
            try:
                geo_id = resolve_geo(conn, nis, PERIOD)
            except UnknownGeographyError:
                unresolved.append((nis, indicator_id))
                continue
            rows_written += upsert_observation(
                conn,
                indicator_id=indicator_id,
                geo_id=geo_id,
                period=PERIOD,
                period_start=period_start,
                period_end=period_end,
                value=value,
                status="final",
                vintage=now,
                fetch_run_id=fetch_run_id,
            )
        print(f"  {indicator_id:36} {len(totals):>4} communes")

    # T01_CAS_AGE_BE_NL.XLSX: a different file, a different parser
    # (_read_block_table), but the same load/resolve/write path -- one
    # dataset added to the loop, not a second copy of it.
    cas_path = census_dir / CAS_TABLE["file"]
    if not cas_path.is_file():
        raise FileNotFoundError(
            f"{cas_path} not found. Census 2021 files are hand-downloaded from "
            "statbel.fgov.be -- see docs/features/manual_sources.md."
        )
    cas_totals = _read_block_table(
        cas_path, CAS_TABLE["sheet"], CAS_TABLE["columns"], _commune_nis_codes(conn)
    )
    for indicator_id, totals in cas_totals.items():
        rows_read += len(totals)
        for nis, value in sorted(totals.items()):
            try:
                geo_id = resolve_geo(conn, nis, PERIOD)
            except UnknownGeographyError:
                unresolved.append((nis, indicator_id))
                continue
            rows_written += upsert_observation(
                conn,
                indicator_id=indicator_id,
                geo_id=geo_id,
                period=PERIOD,
                period_start=period_start,
                period_end=period_end,
                value=value,
                status="final",
                vintage=now,
                fetch_run_id=fetch_run_id,
            )
        print(f"  {indicator_id:36} {len(totals):>4} communes")

    conn.execute(
        "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
        "WHERE fetch_run_id = ?",
        (datetime.now(timezone.utc).isoformat(), rows_read, rows_written, fetch_run_id),
    )
    conn.commit()
    conn.close()

    if unresolved:
        raise SystemExit(
            f"::error::{len(unresolved)} (NIS, indicator) pairs did not resolve to a "
            f"geography for {PERIOD}, e.g. {unresolved[:5]}. Refusing to load a partial "
            "census: a commune silently missing from a count would understate every "
            "aggregate built on it."
        )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load Statbel Census 2021 commune tables (manual, not daily)"
    )
    ap.add_argument("--db", required=True)
    ap.add_argument("--census-dir", type=Path, default=DEFAULT_CENSUS_DIR)
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no workbook and no network",
    )
    args = ap.parse_args()
    read, written = sync(args.db and Path(args.db), args.census_dir, args.reference_rows_only)
    if args.reference_rows_only:
        all_ids = sorted({*EXTRACTS, *CAS_TABLE["columns"].values()})
        print(f"Reference rows ensured for: {', '.join(all_ids)}")
    else:
        print(f"Read {read} commune values, wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
