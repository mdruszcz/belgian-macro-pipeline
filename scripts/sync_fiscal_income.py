"""
Load Statbel's fiscal-income-by-commune file into the canonical `observations`
table -- docs/features/fiscal_income.md.

MANUAL ONLY, like sync_population.py and for the same reason: statbel.fgov.be
is unreachable from this pipeline's network context, so the maintainer
downloads TF_PSNL_INC_TAX_MUNTY.xlsx by hand into
data/raw/statbel/REVENUS/ and runs this against a local, disposable database.
The result is committed as a CSV, not as rows in the daily database
(docs/decisions/0002-split-committed-stores.md).

WHY THE COMMUNE FILE AND NOT THE SECTOR FILE. Statbel publishes the same
statistics by statistical sector (TF_PSNL_INC_TAX_SECTOR.xlsx, 378,416 rows,
20,156 sectors). That file is NOT the source here, and the reason is
measured, not stylistic:

  * 15.9% of its sectors are suppressed in 2023, and the suppression is
    concentrated in small ones. Herstappe (73028) has BOTH its sectors
    suppressed -- aggregating sectors to a commune would publish EUR 0 for
    it. In this file Herstappe is complete: 41 returns, EUR 1,849,504,
    EUR 45,110 average. That is exactly the "if suppression looks like zero
    you will publish 'median income EUR 0' for a small commune" failure the
    data model spec warns about, with a name attached.
  * Its headline measure is a MEDIAN, and a median cannot be reconstructed
    from parts. Sector medians cannot be turned into a commune median by any
    arithmetic.

So commune figures come from the commune file. The sector file remains useful
only for genuinely sub-communal work, which would need a geography level this
pipeline does not have.

SUPPRESSION. Statbel marks a withheld cell with a literal asterisk, not an
empty cell -- which means a naive numeric read does not fail, it produces a
string where a number belongs. Those become value NULL with status
'suppressed', never 0 (CLAUDE.md rule 13). None of the four measures loaded
here is suppressed anywhere in 2005-2023; the handling exists because six
other columns of the same file are, and because a future year could be.

A ZERO HERE IS REAL. MS_TOT_MUNICIP_TAXES is exactly 0 for Knokke-Heist and
Koksijde in all 19 years and De Panne from 2007: those communes levy no
municipal surcharge at all. Suppressed and zero must not be conflated in
either direction.
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.geography.resolve import UnknownGeographyError, resolve_geo  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
DEFAULT_FILE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "raw"
    / "statbel"
    / "REVENUS"
    / "TF_PSNL_INC_TAX_MUNTY.xlsx"
)

# Statbel's column -> our indicator. Deliberately a handful, not all 49:
# every column here is one a municipal finance officer recognises, and each
# is either additive or a count, so all of them survive a commune merger by
# summation. See docs/features/fiscal_income.md for what was left out and why.
COLUMN_TO_INDICATOR = {
    "MS_TOT_NET_TAXABLE_INC": "FISCAL_TOT_NET_TAXABLE_INC",
    "MS_NBR_NON_ZERO_INC": "FISCAL_NBR_NON_ZERO_INC",
    "MS_TOT_TAXES": "FISCAL_TOT_TAXES",
    "MS_TOT_MUNICIP_TAXES": "FISCAL_TOT_MUNICIP_TAXES",
}

YEAR_COLUMN = "CD_YEAR"
NIS_COLUMN = "CD_MUNTY_REFNIS"

# Statbel's marker for a withheld cell.
SUPPRESSED_MARKER = "*"

# THE GEOGRAPHY IN THIS FILE IS A FIXED VINTAGE, NOT THE BOUNDARIES OF THE
# TIME -- the opposite of sync_population.py, and getting it wrong silently
# drops rows rather than failing.
#
# Measured: the set of 581 commune codes is IDENTICAL in every year from 2005
# to 2023, and it resolves 581/581 only at a reference period of 2019-2024.
# At 2005 it resolves 563/581: the missing 18 are 2019-wave creations
# (12041 Puurs-Sint-Amands, 44083 Deinze, 44084 Aalter, 44085 Lievegem,
# 45068 Kruisem, 51067 Enghien, ...) that did not exist in 2005. Statbel has
# back-cast the 2019 commune structure across the whole history, so a 2005 row
# labelled 12041 means "the territory of today's Puurs-Sint-Amands in 2005",
# not an entity that existed then.
#
# Resolving each row at its own period -- correct for the population files,
# which really are per-year snapshots -- therefore dropped 252 rows (2.3%)
# here, exactly the silent mismapping Block C's audit exists to catch.
GEOGRAPHY_REFERENCE_PERIOD = "2024"


class MissingFiscalData(Exception):
    """Raised with instructions rather than a stack trace, because the fix is
    a manual download and the person reading this may not be the person who
    wrote the script."""


def _read_rows(path: Path) -> list[dict]:
    if not path.is_file():
        raise MissingFiscalData(
            f"{path} not found.\n\n"
            "This source cannot be fetched: statbel.fgov.be is unreachable from\n"
            "this pipeline's network context. Download 'Fiscal statistics on income'\n"
            "(data_catalog.md row 1) by hand from\n"
            "  https://statbel.fgov.be/en/open-data/fiscal-statistics-income\n"
            f"and place TF_PSNL_INC_TAX_MUNTY.xlsx in {path.parent}."
        )

    import openpyxl

    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    ws = wb.active
    rows = ws.iter_rows(values_only=True)
    header = next(rows)
    index = {name: i for i, name in enumerate(header)}

    missing = [c for c in (YEAR_COLUMN, NIS_COLUMN, *COLUMN_TO_INDICATOR) if c not in index]
    if missing:
        # A renamed column is exactly the silent schema change the validation
        # layer exists to catch. Fail here rather than load a short file.
        raise MissingFiscalData(
            f"{path.name} is missing expected column(s): {missing}. "
            "Statbel may have changed the file layout; refusing to load a "
            "partial result."
        )

    out = []
    for row in rows:
        if row[index[YEAR_COLUMN]] is None:
            continue
        record = {
            "year": int(row[index[YEAR_COLUMN]]),
            "nis": str(row[index[NIS_COLUMN]]).strip(),
        }
        for column, indicator_id in COLUMN_TO_INDICATOR.items():
            record[indicator_id] = row[index[column]]
        out.append(record)
    wb.close()
    return out


def _coerce(raw, indicator_id: str, nis: str, year: int) -> tuple[float | None, str]:
    """Statbel cell -> (value, status).

    Three cases and no fourth: a number is a number, the asterisk means
    withheld, and anything else is a surprise that must stop the load rather
    than be guessed at.
    """
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw), "final"
    if isinstance(raw, str) and raw.strip() == SUPPRESSED_MARKER:
        return None, "suppressed"
    raise ValueError(
        f"Unexpected value {raw!r} for {indicator_id} in commune {nis}, year {year}. "
        f"Expected a number or the suppression marker {SUPPRESSED_MARKER!r}. "
        "Refusing to guess (CLAUDE.md rule 13)."
    )


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    """INSERT OR IGNORE the sources/indicators rows -- same pattern as
    sync_population.py, and safe to run whichever script goes first."""
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('statbel', 'Statbel Bestat API', 'Statbel', 'statbel',
                'https://bestat.statbel.fgov.be/bestat/api', ?,
                'docs/data_catalog.md rows 1, 1b', 'annual (manual)', 1)
        """,
        (
            'See docs/data_catalog.md "Statbel licence" -- two Statbel documents, both '
            "confirmed to grant commercial reuse",
        ),
    )
    for indicator_id in COLUMN_TO_INDICATOR.values():
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


def _assert_geography_vintage(
    conn: sqlite3.Connection, records: list[dict], geography_period: str
) -> None:
    """Every commune code must resolve at the reference period, or refuse to load.

    A hard check rather than a warning, and before any write. If Statbel
    restates the file onto a different structure -- the 2025 wave took Belgium
    from 581 communes to 565 -- resolution would start failing for whole
    communes, and a partial load of a fiscal panel is worse than no load: it
    looks complete.
    """
    codes = sorted({r["nis"] for r in records})
    bad = []
    for code in codes:
        try:
            resolve_geo(conn, code, geography_period)
        except UnknownGeographyError:
            bad.append(code)
    if bad:
        raise MissingFiscalData(
            f"{len(bad)} of {len(codes)} commune codes do not resolve at the reference "
            f"period {geography_period!r}: {bad[:10]}"
            + (" ..." if len(bad) > 10 else "")
            + "\n\nThis file's geography is a fixed vintage, not the boundaries of each "
            "year. If Statbel has restated it onto a newer commune structure, pass the "
            "matching --geography-period rather than loading a partial panel."
        )


def ensure_reference_rows_only(db_path: Path) -> None:
    """Insert the sources/indicators rows and nothing else.

    Needed because the OBSERVATIONS live in a committed CSV while their
    name/unit metadata is read from the `indicators` table -- one metadata
    path, per export_communes_csv.py. CI can run this (it only reads config),
    where it cannot run the loader (that needs the hand-downloaded workbook).
    Idempotent: INSERT OR IGNORE throughout.
    """
    indicator_configs, _ = load_and_validate_all(CONFIG_DIR / "indicators", CONFIG_DIR / "sources")
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_reference_rows(conn, indicator_configs)
    conn.close()


def sync(
    db_path: Path,
    source_file: Path,
    geography_period: str = GEOGRAPHY_REFERENCE_PERIOD,
) -> tuple[int, int, int, int]:
    """Returns (rows_read, rows_written, suppressed, unresolved)."""
    indicator_configs, _ = load_and_validate_all(CONFIG_DIR / "indicators", CONFIG_DIR / "sources")

    records = _read_rows(source_file)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_reference_rows(conn, indicator_configs)

    _assert_geography_vintage(conn, records, geography_period)

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("statbel", "statbel", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    rows_read = rows_written = suppressed = 0
    unresolved: list[tuple[str, str]] = []

    for record in records:
        period = str(record["year"])
        period_start, period_end = derive_period_bounds(period, "A")
        rows_read += 1
        try:
            geo_id = resolve_geo(conn, record["nis"], geography_period)
        except UnknownGeographyError:
            unresolved.append((record["nis"], period))
            continue

        for indicator_id in COLUMN_TO_INDICATOR.values():
            value, status = _coerce(
                record[indicator_id], indicator_id, record["nis"], record["year"]
            )
            if status == "suppressed":
                suppressed += 1
            rows_written += upsert_observation(
                conn,
                indicator_id=indicator_id,
                geo_id=geo_id,
                period=period,
                period_start=period_start,
                period_end=period_end,
                value=value,
                status=status,
                vintage=now,
                fetch_run_id=fetch_run_id,
            )

    if unresolved:
        print(
            f"WARNING: {len(unresolved)} (NIS, period) pairs did not resolve to a "
            f"geography and were skipped, not guessed: {unresolved[:10]}"
            + (" ..." if len(unresolved) > 10 else ""),
            file=sys.stderr,
        )

    conn.execute(
        "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
        "WHERE fetch_run_id = ?",
        (datetime.now(timezone.utc).isoformat(), rows_read, rows_written, fetch_run_id),
    )
    conn.commit()
    conn.close()
    return rows_read, rows_written, suppressed, len(unresolved)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Statbel fiscal income by commune into observations (manual, not daily)"
    )
    parser.add_argument("--db", required=True, help="Path to the SQLite DB file")
    parser.add_argument("--source-file", type=Path, default=DEFAULT_FILE)
    parser.add_argument(
        "--geography-period",
        default=GEOGRAPHY_REFERENCE_PERIOD,
        help=(
            "Commune structure the file is expressed in (NOT the period of each row). "
            f"Default {GEOGRAPHY_REFERENCE_PERIOD}; see the module docstring."
        ),
    )
    parser.add_argument(
        "--reference-rows-only",
        action="store_true",
        help=(
            "Insert only the sources/indicators reference rows and exit. Lets CI keep "
            "the metadata path intact without the hand-downloaded workbook."
        ),
    )
    args = parser.parse_args()
    if args.reference_rows_only:
        ensure_reference_rows_only(Path(args.db))
        print("Reference rows ensured for: " + ", ".join(sorted(COLUMN_TO_INDICATOR.values())))
        return
    try:
        rows_read, rows_written, suppressed, unresolved = sync(
            Path(args.db), args.source_file, args.geography_period
        )
    except MissingFiscalData as exc:
        print(f"\nCANNOT RUN -- data missing.\n\n{exc}\n", file=sys.stderr)
        raise SystemExit(2) from exc
    print(
        f"Read {rows_read} (commune, year) rows, wrote {rows_written} new vintage(s), "
        f"{suppressed} suppressed cell(s), {unresolved} unresolved."
    )


if __name__ == "__main__":
    main()
