"""
Load Statbel's population-by-commune bulk file into the canonical
`observations` table -- Block F, docs/features/statbel_adapter.md (the
"Demography" item, previously blocked on data access).

NOT part of the automated daily pipeline, unlike sync_statbel.py.
sync_statbel.py performs a live network fetch every run; this cannot, because
statbel.fgov.be is unreachable from this pipeline's own network context
(confirmed repeatedly -- see scripts/plot_population_continuity.py's
docstring) and the source file has no reachable API equivalent (Bestat's only
commune-level population view is Census 2011, a single year). The maintainer
must download TF_SOC_POP_STRUCT_<year>.zip by hand for each year and place it
(zipped or already extracted) in data/raw/statbel/population/ -- this script
is then run manually, and only produces new observations for years actually
present there.

Reuses read_population_by_year from plot_population_continuity.py rather
than re-implementing the zip/encoding/delimiter handling a second time --
both scripts read the exact same files. Each year's per-commune total is
resolved to a geo_id via resolve_geo(nis, period) (Block C), which is
period-aware: a pre-merger year's NIS code resolves to the historical
predecessor entity, not today's successor, which is the correct meaning of
"population of Kruibeke in 2016" now that Kruibeke has been merged away.

Vintage/is_latest discipline matches sync_statbel.py: insert-only-on-change,
UPDATE the previous is_latest row rather than deleting it, hard rowcount
check on insert.
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from plot_population_continuity import (  # noqa: E402
    AGE_BANDS,
    DEFAULT_POP_DIR,
    MissingPopulationData,
    read_population_by_age_band,
)
from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.geography.resolve import UnknownGeographyError, resolve_geo  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
INDICATOR_ID = "POPULATION_BY_COMMUNE"

# The three age bands load as ordinary raw counts, one indicator each. The
# dependency ratio itself is NOT stored -- it is computed from these by the
# Block G derived engine, so a revision to any band recomputes it instead of
# leaving a stale figure behind (CLAUDE.md rule 6).
BAND_INDICATOR_IDS = {band: f"POPULATION_AGE_{band}" for band, _, _ in AGE_BANDS}


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    """INSERT OR IGNORE the sources/indicators rows this script depends on --
    same pattern as sync_statbel.py's _ensure_reference_rows. The statbel
    source row may already exist (sync_statbel.py creates it too); INSERT OR
    IGNORE makes running either script first equally safe."""
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('statbel', 'Statbel Bestat API', 'Statbel', 'statbel',
                'https://bestat.statbel.fgov.be/bestat/api', ?,
                'docs/data_catalog.md rows 3, 3b', 'annual (manual)', 1)
        """,
        (
            'See docs/data_catalog.md "Statbel licence" -- two Statbel documents, both '
            "confirmed to grant commercial reuse",
        ),
    )
    for indicator_id in [INDICATOR_ID, *BAND_INDICATOR_IDS.values()]:
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


def _upsert_observation(
    conn: sqlite3.Connection,
    *,
    indicator_id: str,
    geo_id: str,
    period: str,
    period_start: str,
    period_end: str,
    value: int,
    vintage: str,
    fetch_run_id: int,
) -> int:
    """Insert-only-on-change. Returns 1 if a new vintage was written, else 0.

    Extracted so the four indicators this script now writes (the total plus
    three age bands) share one implementation of the vintage/is_latest rule
    rather than four copies of it.
    """
    current = conn.execute(
        """SELECT value FROM observations
           WHERE indicator_id = ? AND geo_id = ? AND period = ? AND is_latest = 1""",
        (indicator_id, geo_id, period),
    ).fetchone()
    if current is not None and current[0] == value:
        return 0  # unchanged -- no new vintage

    if current is not None:
        conn.execute(
            """UPDATE observations SET is_latest = 0
               WHERE indicator_id = ? AND geo_id = ? AND period = ? AND is_latest = 1""",
            (indicator_id, geo_id, period),
        )

    cur = conn.execute(
        """
        INSERT OR IGNORE INTO observations
            (indicator_id, geo_id, period, vintage, value, status,
             period_start, period_end, is_latest, fetch_run_id, created_at)
        VALUES (?, ?, ?, ?, ?, 'final', ?, ?, 1, ?, ?)
        """,
        (
            indicator_id,
            geo_id,
            period,
            vintage,
            value,
            period_start,
            period_end,
            fetch_run_id,
            vintage,
        ),
    )
    if cur.rowcount != 1:
        raise RuntimeError(
            f"Vintage collision writing {indicator_id}/{geo_id}/{period}/{vintage}: "
            "a row with this exact key already exists. Refusing to silently drop "
            "the new value (CLAUDE.md rule 13)."
        )
    return 1


def sync(db_path: Path, pop_dir: Path) -> tuple[int, int, int]:
    """Returns (rows_read, rows_written, rows_unresolved).

    rows_unresolved counts NIS codes read from a file but not present in
    `geographies` for that period -- logged and skipped, not raised, because
    a handful of these are expected (e.g. a code that changed on a date this
    pipeline's crosswalk does not carry to the day, only the year -- see
    docs/features/geography.md). A wholesale failure (more than a token few)
    is exactly the silent-mismapping risk Block C's own RED audit exists to
    catch, so it is surfaced as a printed warning with the actual codes, not
    swallowed.
    """
    indicator_configs, _ = load_and_validate_all(CONFIG_DIR / "indicators", CONFIG_DIR / "sources")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_reference_rows(conn, indicator_configs)

    by_year = read_population_by_age_band(pop_dir)

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("statbel", "statbel", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    rows_read = 0
    rows_written = 0
    unresolved: list[tuple[str, str]] = []

    for year, communes in sorted(by_year.items()):
        period = str(year)
        period_start, period_end = derive_period_bounds(period, "A")

        for nis, bands in communes.items():
            rows_read += 1
            try:
                geo_id = resolve_geo(conn, nis, period)
            except UnknownGeographyError:
                unresolved.append((nis, period))
                continue

            # The total is the sum of the bands rather than a second pass over
            # a 100 MB file. Every row falls in exactly one band, so this must
            # equal what a total-only read produces -- verified against the
            # real 2026 file for all 565 communes before this landed.
            values = {INDICATOR_ID: sum(bands.values())}
            for band, indicator_id in BAND_INDICATOR_IDS.items():
                values[indicator_id] = bands.get(band, 0)

            for indicator_id, value in values.items():
                rows_written += _upsert_observation(
                    conn,
                    indicator_id=indicator_id,
                    geo_id=geo_id,
                    period=period,
                    period_start=period_start,
                    period_end=period_end,
                    value=value,
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
    return rows_read, rows_written, len(unresolved)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Statbel population-by-commune data into observations (manual, not daily)"
    )
    parser.add_argument("--db", required=True, help="Path to the SQLite DB file")
    parser.add_argument("--population-dir", type=Path, default=DEFAULT_POP_DIR)
    args = parser.parse_args()
    try:
        rows_read, rows_written, unresolved = sync(Path(args.db), args.population_dir)
    except MissingPopulationData as exc:
        print(f"\nCANNOT RUN -- data missing.\n\n{exc}\n", file=sys.stderr)
        raise SystemExit(2) from exc
    print(
        f"Read {rows_read} (commune, year) rows, wrote {rows_written} new vintage(s), "
        f"{unresolved} unresolved."
    )


if __name__ == "__main__":
    main()
