"""Load Statbel's commune-level real-estate transaction file -- the housing
price data on /local.

MANUAL ONLY, same reason as population and fiscal income: statbel.fgov.be is
unreachable from this pipeline's network context. The file is
`FR_immo_statbel_trimestre_par_commune.xlsx` under
data/raw/statbel/census2021/ (the directory name predates knowing this file
belonged there; not moved, to avoid invalidating an already-verified
download path).

REPLACES `immo_by_municipality_2010-2019.xlsx`, THE ANNUAL FILE THIS SCRIPT
USED TO READ. That file gave a TOTAL price (additive, from which a true mean
was recomputed downstream -- CLAUDE.md rule 6) for "gewone woonhuizen"
(ordinary houses, i.e. closed + semi-closed only), 2010-2019, annual. This
one gives QUARTERLY data through the file's own present, 2010-2026 so far,
for a slightly BROADER category -- "Toutes les maisons avec 2, 3, 4 ou plus
de façades (excl. appartements)", i.e. every house type except apartments,
including open/detached houses the old file's "ordinary houses" scope
excluded -- but only a MEDIAN price and its quartiles, no total. A median
cannot be reconstructed from parts (see docs/features/fiscal_income.md's
same point about a different Statbel file), so this is a genuine trade:
wider category, far more current, but the true recomputable mean this
pipeline used to publish is no longer possible from what Statbel supplies.
AVG_HOUSE_PRICE (mean_from_total) and HOUSE_SALES_TOTAL_PRICE are RETIRED
along with the old file; MEDIAN_HOUSE_PRICE replaces AVG_HOUSE_PRICE as the
housing headline, stored directly rather than derived, since there is
nothing to derive it FROM.

MEASURED, NOT ASSUMED, THAT THIS IS A DIFFERENT CATEGORY, NOT JUST A REVISION
OF THE SAME ONE: Aartselaar 2017 read 108 ordinary-house transactions in the
old file; the new file's closed+semi-closed subset alone reads 113 for the
same commune-year (a plausible late-registration revision, not investigated
further), and its broader "all houses" total -- what this script actually
loads -- reads 146, pulled up by open/detached houses the old scope never
counted. HOUSE_SALES_TRANSACTIONS' history therefore steps up around 30-40%
at the 2019/refresh boundary for a reason that has nothing to do with the
housing market; documented here and in the indicator's own description so a
reader does not mistake a scope change for a trend.

EVERY YEAR USES ONE FIXED, CURRENT (POST-2025-MERGER) GEOGRAPHY -- 565
distinct `refnis` codes, identical across 2010 and 2024 alike. Proven the
same way ONEM and police.be were: Kruisem (NIS 45068, created by the 2019
merger) carries a real value in this file's own 2010 rows. So every
observation here resolves against a SINGLE PINNED PERIOD, "2025" -- deliberately
the OPPOSITE choice from police.be's "2024" pin, because THIS file's fixed
code set matches the map AFTER the 2025 mergers (565 codes), not before it
(police.be's 581).

TWO SEPARATE "MOST RECENT PERIOD IS INCOMPLETE" PROBLEMS, handled
differently because they are different kinds of incompleteness:

  1. HOUSE_SALES_TRANSACTIONS (annual, additive). 2026 has only a Q1 row --
     summing it and calling the result "2026" would be exactly ONEM's
     part-year-total mistake (an ~75% undercount presented as a full year).
     Years with fewer than 4 quarters are SKIPPED entirely for this
     indicator, not summed partially.

  2. MEDIAN_HOUSE_PRICE (quarterly, not a total). Every quarter through
     2026-Q1 is loaded, since a quarter's own median is a complete,
     meaningful figure for that quarter regardless of what comes after it --
     there is no "partial quarter" analogue to ONEM's partial year here.

STATUS: the single most recent period in EACH indicator's own series is
'provisional' (2025 for the annual transactions count, 2026-Q1 for the
quarterly median), every earlier period 'final' -- the same rule ONEM and
police.be use, applied here because real-estate registrations can lag and
revise a recent period upward even after it looks complete.

NOT AGGREGATABLE, MEDIAN_HOUSE_PRICE: a median has no additive components to
recompute a province/region/Belgium figure from, the same reasoning as every
police.be rate. `is_additive=0`, `aggregation_method='not_applicable'`.
HOUSE_SALES_TRANSACTIONS remains additive and aggregatable, unchanged.
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
    / "FR_immo_statbel_trimestre_par_commune.xlsx"
)
SHEET_NAME = "Par commune"
CATEGORY_LABEL = "Toutes les maisons avec 2, 3, 4 ou plus de façades (excl. appartements)"

TRANSACTIONS_INDICATOR = "HOUSE_SALES_TRANSACTIONS"
MEDIAN_PRICE_INDICATOR = "MEDIAN_HOUSE_PRICE"

# See module docstring: the OPPOSITE pin from police.be's "2024", because
# this file's 565-code geography matches the map AFTER the 2025 mergers.
PINNED_PERIOD = "2025"


def _read_quarterly_rows(path: Path) -> dict[str, dict[str, tuple[float | None, float | None]]]:
    """{period ("YYYY-Qn"): {nis: (transactions, median_price)}}.

    Reads the "Toutes les maisons ... (excl. appartements)" block by its own
    row-1 label, not a fixed column position -- the file repeats the same
    four column names ("nombre transactions", "prix médian(€)", ...) once
    per property category, so a position would silently read the wrong
    category if Statbel ever reordered the blocks.
    """
    import openpyxl

    workbook = openpyxl.load_workbook(path, read_only=True)
    if SHEET_NAME not in workbook.sheetnames:
        raise ValueError(
            f"{path.name} has no sheet {SHEET_NAME!r}. Its sheets are "
            f"{workbook.sheetnames}. Refusing to guess a replacement (CLAUDE.md rule 13)."
        )
    sheet = workbook[SHEET_NAME]
    rows = list(sheet.iter_rows(values_only=True))
    category_row, columns_row = rows[1], rows[2]

    try:
        block_start = category_row.index(CATEGORY_LABEL)
    except ValueError as exc:
        raise ValueError(
            f"{path.name}!{SHEET_NAME} has no {CATEGORY_LABEL!r} category block. Its row-1 "
            f"labels are {[c for c in category_row if c]}. Refusing to guess a replacement "
            "(CLAUDE.md rule 13)."
        ) from exc
    block = columns_row[block_start : block_start + 4]
    if block[0] != "nombre transactions" or block[1] != "prix médian(€)":
        raise ValueError(
            f"{path.name}!{SHEET_NAME}: the columns under {CATEGORY_LABEL!r} are {block}, not "
            "the expected ('nombre transactions', 'prix médian(€)', ...). Refusing to guess "
            "a replacement (CLAUDE.md rule 13)."
        )
    transactions_col, median_col = block_start, block_start + 1

    required = ("refnis", "année", "période")
    missing = [c for c in required if c not in columns_row]
    if missing:
        raise ValueError(
            f"{path.name}!{SHEET_NAME} is missing expected column(s) {missing}. Its columns "
            f"are {columns_row}. Refusing to guess a replacement (CLAUDE.md rule 13)."
        )
    idx = {name: columns_row.index(name) for name in required}

    out: dict[str, dict[str, tuple[float | None, float | None]]] = {}
    for row in rows[3:]:
        nis = row[idx["refnis"]]
        if nis is None:
            continue
        year, quarter = row[idx["année"]], row[idx["période"]]
        period = f"{year}-{quarter}"
        transactions = row[transactions_col]
        median_price = row[median_col]
        out.setdefault(period, {})[str(nis)] = (
            float(transactions) if transactions is not None else None,
            float(median_price) if median_price is not None else None,
        )
    workbook.close()
    return out


def _annual_transaction_totals(
    by_period: dict[str, dict[str, tuple]],
) -> dict[str, dict[str, float]]:
    """Sum quarterly transaction counts into an annual total, but ONLY for a
    year every one of whose four quarters is present in the file -- see
    module docstring's ONEM-part-year-total parallel."""
    quarters_seen: dict[str, set[str]] = {}
    totals: dict[str, dict[str, float]] = {}
    for period, by_nis in by_period.items():
        year, quarter = period.split("-")
        quarters_seen.setdefault(year, set()).add(quarter)
        year_totals = totals.setdefault(year, {})
        for nis, (transactions, _median) in by_nis.items():
            if transactions is None:
                continue
            year_totals[nis] = year_totals.get(nis, 0.0) + transactions

    return {
        year: values
        for year, values in totals.items()
        if quarters_seen[year] == {"Q1", "Q2", "Q3", "Q4"}
    }


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('statbel', 'Statbel Bestat API', 'Statbel', 'statbel',
                'https://bestat.statbel.fgov.be/bestat/api', ?,
                'docs/data_catalog.md -- Census indicators', 'quarterly (manual)', 1)
        """,
        (
            'See docs/data_catalog.md "Statbel licence" -- two Statbel documents, both '
            "confirmed to grant commercial reuse",
        ),
    )
    for indicator_id, aggregation_method, is_additive in (
        (TRANSACTIONS_INDICATOR, "sum", 1),
        (MEDIAN_PRICE_INDICATOR, "not_applicable", 0),
    ):
        ind = indicator_configs[indicator_id]
        conn.execute(
            """
            INSERT INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'statbel', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, 0, ?, 1)
            ON CONFLICT(indicator_id) DO UPDATE SET
                name_nl = excluded.name_nl,
                name_fr = excluded.name_fr,
                name_en = excluded.name_en,
                description_en = excluded.description_en,
                frequency = excluded.frequency,
                unit = excluded.unit,
                preferred_direction = excluded.preferred_direction,
                aggregation_method = excluded.aggregation_method,
                is_additive = excluded.is_additive
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
                aggregation_method,
                is_additive,
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
    by_period = _read_quarterly_rows(source_file)
    annual_transactions = _annual_transaction_totals(by_period)

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("statbel", "realestate", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    rows_read = rows_written = 0
    unresolved: list[str] = []

    latest_year = max(annual_transactions) if annual_transactions else None
    for year, by_nis in sorted(annual_transactions.items()):
        period_start, period_end = derive_period_bounds(year, "A")
        status = "provisional" if year == latest_year else "final"
        for nis, total in sorted(by_nis.items()):
            rows_read += 1
            try:
                geo_id = resolve_geo(conn, nis, PINNED_PERIOD)
            except UnknownGeographyError:
                unresolved.append(nis)
                continue
            rows_written += upsert_observation(
                conn,
                indicator_id=TRANSACTIONS_INDICATOR,
                geo_id=geo_id,
                period=year,
                period_start=period_start,
                period_end=period_end,
                value=total,
                status=status,
                vintage=now,
                fetch_run_id=fetch_run_id,
            )

    latest_period = max(by_period) if by_period else None
    for period, by_nis in sorted(by_period.items()):
        period_start, period_end = derive_period_bounds(period, "Q")
        status = "provisional" if period == latest_period else "final"
        for nis, (_transactions, median_price) in sorted(by_nis.items()):
            if median_price is None:
                continue
            rows_read += 1
            try:
                geo_id = resolve_geo(conn, nis, PINNED_PERIOD)
            except UnknownGeographyError:
                unresolved.append(nis)
                continue
            rows_written += upsert_observation(
                conn,
                indicator_id=MEDIAN_PRICE_INDICATOR,
                geo_id=geo_id,
                period=period,
                period_start=period_start,
                period_end=period_end,
                value=median_price,
                status=status,
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
            f"::error::{len(unresolved)} NIS code(s) did not resolve at the pinned period "
            f"{PINNED_PERIOD!r}, e.g. {sorted(set(unresolved))[:5]}. Refusing to load a "
            "partial series."
        )
    print(
        f"Read {rows_read} rows: {len(annual_transactions)} annual periods for "
        f"{TRANSACTIONS_INDICATOR}, {len(by_period)} quarterly periods for "
        f"{MEDIAN_PRICE_INDICATOR}."
    )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load Statbel's commune-level house transactions and median price "
        "(manual, not daily)"
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
        print(f"Reference rows ensured for: {TRANSACTIONS_INDICATOR}, {MEDIAN_PRICE_INDICATOR}")
    else:
        print(f"Wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
