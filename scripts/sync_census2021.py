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
    for indicator_id in EXTRACTS:
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
        print(f"Reference rows ensured for: {', '.join(sorted(EXTRACTS))}")
    else:
        print(f"Read {read} commune values, wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
