"""Load Statbel's population-movement workbook -- BIRTHS, DEATHS,
INTERNAL_MIGRATION_NET, INTERNATIONAL_MIGRATION_NET, Block NS3.

DAILY, AUTOMATIC. Discovered live off the theme page
(https://statbel.fgov.be/fr/themes/population/mouvement-de-la-population),
the same discipline scripts/sync_bankruptcies.py uses for its own landing
page: the href is read off the page's HTML, never hard-coded, never a
cached fallback (CLAUDE.md rule 13).

GEOGRAPHY IS PER-ROW, NOT PINNED -- the opposite of sync_bankruptcies.py /
sync_police.py / sync_realestate.py, and the reason this script exists
separately rather than reusing their loop. Every (indicator, nis, sheet
year) cell resolves via resolve_geo(conn, nis, as_of_period), where
`as_of_period` is the row's OWN sheet year PLUS ONE -- not the sheet year
itself. See src/fetchers/population_movement.py's module docstring for the
measurement behind this: column W's own label is "POPULATION AU 31
DECEMBRE (SOIT AU 1/1 DE L'ANNEE SUIVANTE)", and sheet "2018" already
carries the eighteen commune codes the 2019-01-01 merger created (verified
directly against the geographies table, 2026-09-23) -- so a sheet named
YYYY reports its commune grid as of 1 January of YYYY+1, while the
OBSERVATION itself is still written under period=YYYY (the births/deaths/
migration happened during calendar year YYYY; only the grid lookup needs
the following year's date).

An unresolved (nis, as_of_period) is collected, never causes a partial
load: every row in the file is checked before the run is either fully
committed or fully refused (sync_police.py's own pattern, CLAUDE.md rule
13). A code that resolves to a NON-municipality level (province,
arrondissement, region, country -- Belgium itself is a row, "01000") is
silently skipped, expected and by design: the row-filtering trap in the
handoff is exactly this -- a naive "5-digit code not ending in 000" filter
would also catch 20001/20002 (the two Brabant provinces), which resolve to
level='province' here and are dropped, never counted as unresolved.

FOUR INDICATORS, ALL ADDITIVE COUNTS (is_additive=1, aggregation_method=
'sum'), summed the ordinary way to province/region/Belgium over the
geographies that existed in that period -- the existing aggregation engine
does this; nothing here touches it. NAISSANCES and DECES are always
non-negative; the two migration SOLDE columns can be negative (net outflow)
-- see config/indicators/INTERNAL_MIGRATION_NET.yaml and
INTERNATIONAL_MIGRATION_NET.yaml's own description for how this batch
resolves the conflict with the `counts_non_negative` validation rule
(src/validation/rules.py), which currently rejects any negative value where
unit='count'. This script does NOT weaken that rule; see the PR body.

RATES ARE OUT OF SCOPE (per-1,000 birth/death/migration rates would need
src/analytics/derived.py, gated behind an ADR per CLAUDE.md rule 19) -- this
script writes only the four raw counts.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import ssl
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.fetchers.population_movement import (  # noqa: E402
    ALL_INDICATORS,
    PopulationMovementSource,
    discover_xlsx_url,
)
from src.geography.resolve import UnknownGeographyError, resolve_geo  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

THEME_PAGE_URL = "https://statbel.fgov.be/fr/themes/population/mouvement-de-la-population"

_YEAR = re.compile(r"^(19|20)\d{2}$")

#: Non-municipality levels expected in the file (Belgium, regions, provinces,
#: arrondissements) -- rows at these levels are dropped, not treated as
#: unresolved. Any level string outside this set alongside 'municipality' is
#: still accepted implicitly (the check below is "not municipality -> skip"),
#: this set exists only for the log message.
_EXPECTED_NON_MUNICIPAL_LEVELS = {"country", "region", "province", "arrondissement"}


def _fetch_theme_page_html() -> str:
    """A plain GET for the theme page HTML -- not through DataSource.fetch(),
    which caches raw bytes under the ADAPTER's own source_id/raw_extension,
    and this page is not the xlsx. This machine's TLS interception means a
    live GET here needs an unverified SSL context, exactly like every other
    live network call this pipeline makes from this environment.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(THEME_PAGE_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    # INSERT OR IGNORE, same as scripts/sync_bankruptcies.py's own
    # _ensure_reference_rows: whichever store's reference-rows script runs
    # first (config/stores.yaml's declaration order) creates this row, every
    # later one is a harmless no-op against the same source_id.
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('statbel', 'Statbel Bestat API', 'Statbel', 'statbel',
                'https://bestat.statbel.fgov.be/bestat/api', ?,
                'docs/data_catalog.md -- Population movement indicators', 'annual (automatic)', 1)
        """,
        (
            'See docs/data_catalog.md "Statbel licence" -- two Statbel documents, both '
            "confirmed to grant commercial reuse",
        ),
    )
    for indicator_id in ALL_INDICATORS:
        ind = indicator_configs[indicator_id]
        conn.execute(
            """
            INSERT INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'statbel', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'sum', 1, 0, ?, 1)
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
    db_path: Path,
    reference_rows_only: bool = False,
    xlsx_bytes: bytes | None = None,
) -> tuple[int, int]:
    """Returns (rows_read, rows_written).

    `xlsx_bytes`, when given, replaces the theme-page discovery and live
    download entirely -- what tests use to run the real parse/resolve/write
    logic against a fixture with no network. Production (main()) always
    discovers and downloads live.
    """
    indicator_configs, _sources = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, indicator_configs)
    if reference_rows_only:
        conn.close()
        return 0, 0

    source = PopulationMovementSource()
    now = datetime.now(timezone.utc).isoformat()

    if xlsx_bytes is not None:
        rows = source._parse(xlsx_bytes)
        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            ("statbel", "population_movement", now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    else:
        html = _fetch_theme_page_html()
        xlsx_url = discover_xlsx_url(html)
        rows = source.fetch(xlsx_url, cache_key="population_movement", conn=conn)
        fetch_run_id = conn.execute(
            "SELECT fetch_run_id FROM fetch_runs WHERE source_id = 'statbel' AND "
            "adapter = 'population_movement' ORDER BY fetch_run_id DESC LIMIT 1"
        ).fetchone()[0]

    if not rows:
        raise ValueError(
            "Statbel's population-movement workbook produced zero rows after parsing -- "
            "refusing to load an empty series (CLAUDE.md rule 13)."
        )

    # PASS 1: resolve every (indicator, nis, sheet year) cell against the
    # commune grid as of 1 January of sheet_year + 1 (module docstring).
    # Collect every failure and refuse the whole run at the end rather than
    # loading a partial series. A code that resolves to a NON-municipality
    # level is dropped silently -- expected (Belgium, regions, provinces,
    # arrondissements all appear in the file).
    unresolved: list[tuple[str, str, str]] = []
    to_write: list[tuple[str, str, str, float, str]] = (
        []
    )  # (indicator_id, geo_id, period, value, status)
    rows_read = 0

    for row in rows:
        rows_read += 1
        indicator_id = row["indicator_id"]
        nis = row["geo_id"]
        sheet_year = row["period"]
        if not _YEAR.match(sheet_year):
            raise ValueError(
                f"Unexpected period {sheet_year!r} from the adapter -- expected a bare "
                "4-digit year. Refusing to guess (CLAUDE.md rule 13)."
            )
        as_of_period = str(int(sheet_year) + 1)

        level_row = conn.execute(
            "SELECT level FROM geographies WHERE nis_code = ? AND valid_from <= ? "
            "AND (valid_to IS NULL OR valid_to > ?)",
            (nis, f"{as_of_period}-01-01", f"{as_of_period}-01-01"),
        ).fetchone()

        if level_row is None:
            unresolved.append((indicator_id, sheet_year, nis))
            continue
        if level_row[0] != "municipality":
            # Belgium, a region, a province or an arrondissement row -- the
            # row-filtering trap the handoff calls out explicitly. Not an
            # error, not counted as unresolved.
            continue

        try:
            geo_id = resolve_geo(conn, nis, as_of_period)
        except UnknownGeographyError:
            unresolved.append((indicator_id, sheet_year, nis))
            continue

        to_write.append((indicator_id, geo_id, sheet_year, row["value"], row["status"]))

    if unresolved:
        raise SystemExit(
            f"::error::{len(unresolved)} (indicator, sheet year, NIS) triple(s) resolved to a "
            f"municipality-level code with no covering geography row (or no geography row at "
            f"all) as of 1 January of the following year, e.g. {unresolved[:5]}. Refusing to "
            "load a partial series."
        )

    rows_written = 0
    for indicator_id, geo_id, period, value, status in sorted(
        to_write, key=lambda t: (t[0], t[1], t[2])
    ):
        period_start, period_end = derive_period_bounds(period, "A")
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

    conn.execute(
        "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
        "WHERE fetch_run_id = ?",
        (datetime.now(timezone.utc).isoformat(), rows_read, rows_written, fetch_run_id),
    )
    conn.commit()
    conn.close()

    print(
        f"Read {rows_read} (indicator, commune, year) cells from the workbook; wrote "
        f"{rows_written} new vintage(s) across {len({g for _, g, _, _, _ in to_write})} "
        "communes."
    )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load Statbel's population-movement workbook (daily, automatic)"
    )
    ap.add_argument("--db", required=True)
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no network",
    )
    args = ap.parse_args()
    read, written = sync(Path(args.db), args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for: {', '.join(ALL_INDICATORS)}")
    else:
        print(f"Wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
