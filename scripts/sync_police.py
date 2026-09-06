"""Load the federal police's stats-pol commune rate files -- docs/data_catalog.md's
police.be row, the pipeline's 14th dataset and its first from stats-pol.

MANUAL ONLY, and for a different reason than Statbel/ONEM. onem.be and
statbel.fgov.be fail at the network layer (TCP handshake timeout); police.be
responds, but with an HTTP 403 "Maintenance" page and no session cookie, to
every request this pipeline's own network context and GitHub Actions' runner
both send (see scripts/fetch_police_raw.py). The maintainer fetched these
files from their own browser instead, where the same requests return 200.

FOUR CATEGORIES, one directory each under data/raw/police/, one file per
period named literally by its year:

    cambriolage/{2000,2017..2025}      -> HOUSE_BURGLARIES_PER_10K
    vol de voiture/2025                -> CAR_THEFT_PER_10K
    Vol dans ou sur un véhicule/2025   -> THEFT_FROM_VEHICLE_PER_10K
    Violence intrafamiliale/2025       -> DOMESTIC_VIOLENCE_PER_10K

Only `cambriolage` has a real multi-year history so far; the other three
have one file each. Adding a year to any category is dropping one more
year-named file in its directory -- nothing here is hardcoded to today's
set of files.

THE DENOMINATORS ARE NOT WHAT AN EARLIER VERSION OF THIS SCRIPT ASSUMED.
The maintainer-supplied filename for the burglary file said "par 10000hab"
(per 10,000 inhabitants), and the first version of this loader took that at
face value. The maintainer later found police.be's own citation for each
rate's denominator: `HOUSE_BURGLARIES_PER_10K` is per 10,000 DWELLINGS
("Parc de bâtiments" - SPF Economie/Statbel), not per capita. Corrected here
rather than left as a plausible-sounding wrong unit -- the filename was the
person who downloaded it describing what they saw, not police.be's own
methodology.

EVERY FILE SHARES ONE FIXED GEOGRAPHY -- 587 `geo_code`s, identical set
across every category and every year, including "2000". Proven, not assumed:
Kruisem (NIS 45068) was FORMED by the 2019 merger wave and did not exist as
that code before 2019-01-01, yet it carries a real, distinct value in the
`cambriolage/2000` file. police.be's own historical tool backcasts its
current (pre-2025-merger) municipal grid onto every year it shows, the same
move ONEM makes for its own history. So every row of every file, regardless
of the year in its filename, is resolved against ONE FIXED PERIOD ("2024",
the last day before the 2025 mergers) -- not `resolve_geo(nis, that file's
own year)`, which would raise for merger-created communes in a pre-merger
year and would be wrong regardless: the "z" value already reflects
police.be's own current-grid attribution, not a true historical one.

SIX OF THE 587 `geo_code`S ARE NOT REAL GEOGRAPHY, under any period: the
three negative placeholders -1/-3/-4 (a residual/unknown bucket in the
source's own export) plus 21020, 23095 and 31999, which resemble NIS codes
but match no geography row at all. All six are always paired with `z: 0` in
every file checked. Skipped as a GENERAL rule (any code unresolvable at the
fixed period, carrying a value of exactly 0), not a hardcoded list, so a
future file's own garbage rows are handled the same way without an edit
here. A nonzero value on an unresolvable code is a different situation and
raises.

STATUS: every year is 'final' except the MOST RECENT year in each
category's own file set, which is 'provisional'. Measured, not assumed, for
`cambriolage`: national totals across the real 2000/2017-2024 series are
35,138-46,000-ish per year with ordinary year-to-year variation, and 2025's
35,138 sits inside that range rather than reading like a two-months-only
partial total (ONEM's euro files fall to ~17% of a full year when genuinely
partial -- this does not). That is evidence 2025 may already be a complete
period, not proof, since nothing in any file states whether "2025" means a
completed calendar year or a still-open rolling window. 'provisional'
records that remaining uncertainty rather than asserting a finality nobody
has confirmed.

NOT AGGREGATABLE, ON PURPOSE, all four. Every one is a rate with no
underlying count in the source to derive it from -- CLAUDE.md rule 6 governs
deriving a ratio FROM stored additive components, and there are none here.
`is_additive=0`, `aggregation_method='not_applicable'` for all four;
export_aggregates_csv.py's methods_from_metadata() then refuses them, so
they show at commune level only, with no province/region/Belgium row
manufactured by averaging a rate across communes (docs/decisions/0003).
"""

import argparse
import json
import re
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
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "police"

# The period every file's geo_code is resolved against, regardless of the
# year in the file's own name -- see module docstring: police.be's own tool
# backcasts its current (pre-2025-merger) grid onto every year it publishes,
# proven by Kruisem (2019-merger-created) carrying a value in the "2000" file.
PINNED_PERIOD = "2024"

_YEAR_FILENAME = re.compile(r"^\d{4}$")

# indicator_id -> source directory name under data/raw/police/
DATASETS = {
    "HOUSE_BURGLARIES_PER_10K": "cambriolage",
    "CAR_THEFT_PER_10K": "vol de voiture",
    "THEFT_FROM_VEHICLE_PER_10K": "Vol dans ou sur un véhicule",
    "DOMESTIC_VIOLENCE_PER_10K": "Violence intrafamiliale",
}


def _years_available(directory: Path) -> list[str]:
    if not directory.is_dir():
        return []
    years = [p.name for p in directory.iterdir() if _YEAR_FILENAME.match(p.name)]
    return sorted(years)


def _read_rates(path: Path) -> dict[str, float]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("error"):
        raise ValueError(f"{path} itself reports error=true: {payload}")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError(
            f"{path} has no usable 'data' list. Its top-level keys are "
            f"{list(payload.keys())}. Refusing to guess a replacement (CLAUDE.md rule 13)."
        )
    return {str(row["geo_code"]): float(row["z"]) for row in rows}


def _resolve(conn: sqlite3.Connection, nis: str) -> str:
    return resolve_geo(conn, nis, PINNED_PERIOD)


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    # ON CONFLICT ... DO UPDATE, not INSERT OR IGNORE: this indicator's own
    # name and unit were WRONG on first load (per-inhabitant, guessed from a
    # filename) and corrected once the real denominator was found. IGNORE
    # would have left the stale name sitting in an already-loaded database
    # forever, since --reference-rows-only is the only thing that ever
    # touches this row and a fix to config/indicators/*.yaml would then
    # silently not reach it. Matches scripts/sync_to_canonical.py's own
    # pattern, not sync_realestate.py/sync_census2021.py's IGNORE, which
    # share this same latent gap -- not fixed here, out of scope for this
    # source.

    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('police', 'Police Fédérale / Federale Politie — stats-pol',
                "Police Fédérale — Direction de l'information policière et des moyens ICT",
                'police', 'https://www.police.be/statistiques/', ?,
                'docs/data_catalog.md -- police.be row', 'annual (manual, ad hoc)', 1)
        """,
        (
            "Attribution required, maintainer-supplied 2026-09-06 -- see "
            "docs/data_catalog.md's police.be row for the exact text and its caveats.",
        ),
    )
    for indicator_id in DATASETS:
        ind = indicator_configs[indicator_id]
        conn.execute(
            """
            INSERT INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'police', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'not_applicable', 0, 1, ?, 1)
            ON CONFLICT(indicator_id) DO UPDATE SET
                name_nl = excluded.name_nl,
                name_fr = excluded.name_fr,
                name_en = excluded.name_en,
                description_en = excluded.description_en,
                unit = excluded.unit,
                preferred_direction = excluded.preferred_direction
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
    db_path: Path, raw_dir: Path = RAW_DIR, reference_rows_only: bool = False
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

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("police", "police", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    rows_read = rows_written = 0
    skipped_zero_placeholder = 0
    unresolved: list[tuple[str, str, str]] = []

    for indicator_id, dirname in DATASETS.items():
        directory = raw_dir / dirname
        years = _years_available(directory)
        if not years:
            raise FileNotFoundError(
                f"{directory} has no year-named file. Hand-fetched from police.be -- see "
                "docs/features/manual_sources.md."
            )
        latest_year = years[-1]  # every year but the latest is a closed period

        for year in years:
            status = "provisional" if year == latest_year else "final"
            period_start, period_end = derive_period_bounds(year, "A")
            rates = _read_rates(directory / year)
            rows_read += len(rates)

            for nis, value in sorted(rates.items()):
                try:
                    geo_id = _resolve(conn, nis)
                except UnknownGeographyError:
                    if value == 0:
                        # See module docstring: a code that names no commune
                        # at the pinned period AND carries a value of exactly
                        # 0 is one of the source's own placeholder rows, not
                        # a real geography this pipeline failed to load.
                        skipped_zero_placeholder += 1
                        continue
                    unresolved.append((indicator_id, year, nis))
                    continue
                rows_written += upsert_observation(
                    conn,
                    indicator_id=indicator_id,
                    geo_id=geo_id,
                    period=year,
                    period_start=period_start,
                    period_end=period_end,
                    value=value,
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
            f"::error::{len(unresolved)} (indicator, year, NIS) triple(s) did not resolve at "
            f"the pinned period {PINNED_PERIOD!r}, e.g. {unresolved[:5]}. Refusing to load a "
            "partial series."
        )
    print(
        f"Read {rows_read} (indicator, commune, year) rates across {len(DATASETS)} categories "
        f"({skipped_zero_placeholder} zero-valued non-geography code(s) skipped)."
    )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load the federal police's stats-pol commune rate files (manual, not daily)"
    )
    ap.add_argument("--db", required=True)
    ap.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no source files",
    )
    args = ap.parse_args()
    read, written = sync(Path(args.db), args.raw_dir, args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for: {', '.join(DATASETS)}")
    else:
        print(f"Wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
