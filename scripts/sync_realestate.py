"""Load Statbel's commune-level real-estate transaction file -- the housing
price data docs/data_catalog.md previously said did not exist at commune
level. It does; it was in the wrong corner of Statbel's site (a hand-supplied
bulk file, not a Bestat API view), and it was found alongside the Census 2021
workbooks in the same manual download.

MANUAL ONLY, same reason as population and fiscal income: statbel.fgov.be is
unreachable from this pipeline's network context. The file is
`immo_by_municipality_2010-2019.xlsx` under data/raw/statbel/census2021/
(the directory name predates knowing this file belonged there; not moved,
to avoid invalidating an already-verified download path).

SCOPED TO ORDINARY HOUSES ONLY ("gewone woonhuizen"), of the file's four
property types (houses, apartments, villas, building land). Measured
coverage per year: houses 588-589 of 589 communes, apartments 527-546,
villas 577-579, building land 583-587 (2010-2014 only; absent 2015-2017
entirely). Houses is both the best-covered type and the one a reader means
by "housing price" without qualification. The other three are real data,
correctly available, and simply not loaded by this first pass -- extending
EXTRACTS below to add them is the same shape of change as any other
indicator here, not a rewrite.

ANNUAL PERIOD ONLY (`CD_PERIOD == 'Y'`), not the quarterly/semestral rows the
file also carries: the rest of this pipeline's municipal indicators are
annual, and quarterly commune-level transaction counts are small enough that
most communes would be sparse or suppressed most quarters.

STORED AS TOTALS, NOT AS THE FILE'S OWN MEAN. MS_TOTAL_TRANSACTIONS and
MS_TOTAL_PRICE are additive; MS_MEAN_PRICE is not, and averaging Statbel's
own means across communes would be the exact "average of averages" error
Block L's aggregation rule exists to prevent (docs/decisions/0003). The mean
is recomputed downstream by the derived engine (mean_from_total), the same
pattern AVG_NET_TAXABLE_INCOME already uses.

PERIOD-AWARE GEOGRAPHY, not fixed like sync_fiscal_income.py. The fiscal file
back-casts one commune vintage across all its years (see that script's own
docstring); this file's coverage count is stable across 2010-2017 (589,
588-589 communes per year) with no Belgian merger wave in that window, so
each year is resolved against its own geography via resolve_geo(nis, year) --
the same approach sync_population.py uses.

NO EXPLICIT ZEROS anywhere in the annual house rows (verified: 0 of 4,705).
An absent (commune, year) is a suppressed or genuinely empty cell, and stays
absent rather than being written as a 0 transaction count.
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
DEFAULT_SOURCE_FILE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "raw"
    / "statbel"
    / "census2021"
    / "immo_by_municipality_2010-2019.xlsx"
)

PROPERTY_TYPE = "gewone woonhuizen"  # ordinary houses -- see module docstring
TRANSACTIONS_INDICATOR = "HOUSE_SALES_TRANSACTIONS"
TOTAL_PRICE_INDICATOR = "HOUSE_SALES_TOTAL_PRICE"


def _read_annual_house_sales(path: Path) -> dict[str, dict[str, tuple[float, float]]]:
    """{year: {nis: (transactions, total_price)}} for annual house-sale rows."""
    import openpyxl

    workbook = openpyxl.load_workbook(path, read_only=True)
    sheet = workbook[workbook.sheetnames[0]]
    rows = sheet.iter_rows(values_only=True)
    header = list(next(rows))

    required = (
        "CD_YEAR",
        "CD_TYPE_NL",
        "CD_REFNIS",
        "CD_PERIOD",
        "MS_TOTAL_TRANSACTIONS",
        "MS_TOTAL_PRICE",
    )
    missing = [c for c in required if c not in header]
    if missing:
        raise ValueError(
            f"{path.name} is missing expected column(s) {missing}. Its columns are "
            f"{header}. Refusing to guess a replacement (CLAUDE.md rule 13)."
        )
    idx = {name: header.index(name) for name in required}

    out: dict[str, dict[str, tuple[float, float]]] = {}
    for row in rows:
        if row[idx["CD_TYPE_NL"]] != PROPERTY_TYPE or row[idx["CD_PERIOD"]] != "Y":
            continue
        year = str(row[idx["CD_YEAR"]])
        nis = str(row[idx["CD_REFNIS"]])
        transactions = row[idx["MS_TOTAL_TRANSACTIONS"]]
        total_price = row[idx["MS_TOTAL_PRICE"]]
        if transactions is None or total_price is None:
            continue
        out.setdefault(year, {})[nis] = (float(transactions), float(total_price))
    workbook.close()
    return out


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('statbel', 'Statbel Bestat API', 'Statbel', 'statbel',
                'https://bestat.statbel.fgov.be/bestat/api', ?,
                'docs/data_catalog.md -- Census indicators', 'annual (manual)', 1)
        """,
        (
            'See docs/data_catalog.md "Statbel licence" -- two Statbel documents, both '
            "confirmed to grant commercial reuse",
        ),
    )
    for indicator_id in (TRANSACTIONS_INDICATOR, TOTAL_PRICE_INDICATOR):
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


def sync(
    db_path: Path, source_file: Path = DEFAULT_SOURCE_FILE, reference_rows_only: bool = False
) -> tuple[int, int]:
    indicator_configs, _sources = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, indicator_configs)
    if reference_rows_only:
        conn.close()
        return 0, 0

    if not source_file.is_file():
        raise FileNotFoundError(
            f"{source_file} not found. Hand-downloaded from statbel.fgov.be -- see "
            "docs/features/manual_sources.md."
        )
    by_year = _read_annual_house_sales(source_file)

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("statbel", "realestate", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    rows_read = rows_written = 0
    unresolved: list[tuple[str, str]] = []

    for year in sorted(by_year):
        period_start, period_end = derive_period_bounds(year, "A")
        for nis, (transactions, total_price) in sorted(by_year[year].items()):
            rows_read += 1
            try:
                geo_id = resolve_geo(conn, nis, year)
            except UnknownGeographyError:
                unresolved.append((nis, year))
                continue
            for indicator_id, value in (
                (TRANSACTIONS_INDICATOR, transactions),
                (TOTAL_PRICE_INDICATOR, total_price),
            ):
                rows_written += upsert_observation(
                    conn,
                    indicator_id=indicator_id,
                    geo_id=geo_id,
                    period=year,
                    period_start=period_start,
                    period_end=period_end,
                    value=value,
                    status="final",
                    vintage=now,
                    fetch_run_id=fetch_run_id,
                )

    conn.execute(
        "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
        "WHERE fetch_run_id = ?",
        (datetime.now(timezone.utc).isoformat(), rows_read, rows_written, fetch_run_id),
    )
    conn.commit()
    conn.close()

    if unresolved:
        raise SystemExit(
            f"::error::{len(unresolved)} (NIS, year) pairs did not resolve to a "
            f"geography, e.g. {unresolved[:5]}. Refusing to load a partial series."
        )
    print(f"Read {rows_read} (commune, year) rows across {len(by_year)} years.")
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load Statbel's commune-level ordinary-house sale prices (manual, not daily)"
    )
    ap.add_argument("--db", required=True)
    ap.add_argument("--source-file", type=Path, default=DEFAULT_SOURCE_FILE)
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no workbook and no network",
    )
    args = ap.parse_args()
    read, written = sync(Path(args.db), args.source_file, args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for: {TRANSACTIONS_INDICATOR}, {TOTAL_PRICE_INDICATOR}")
    else:
        print(f"Wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
