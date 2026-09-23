"""Load Statbel's monthly bankruptcies-by-NACE open-data file --
BANKRUPTCIES and BANKRUPTCY_JOBS_LOST, docs/features/bankruptcies.md.

NOT PART OF THE DAILY AUTOMATIC FETCH SINCE 2026-09-23. Unlike police.be and
the real-estate workbook (both MANUAL from the start: statbel.fgov.be itself
is unreachable from this pipeline's network context for those), this file
used to be discovered and downloaded live: the landing page's own href for
`TF_BANKRUPTCIES(<year>).zip` (see src/fetchers/bankruptcies.py's
discover_zip_url), a year that changes as Statbel republishes -- never
hard-coded, never a cached fallback. That discovery-and-download logic is
unchanged and still runs when no `--from-file` is given.

WHY NOT DAILY ANY MORE. Diagnosed 2026-09-23 on the scheduled run
(2026-09-23T18:09): statbel.fgov.be answers EVERY request from a GitHub
Actions runner with a CAPTCHA challenge page (HTTP 200, text/html, ~46 KB,
"This question is for testing whether you are a human visitor... What code
is in the image?", carrying a support ID) instead of the landing page or the
zip. A probe run confirmed this is runner-specific: the same URLs still
return the real page/file from a maintainer's own machine. This pipeline
does not solve or evade CAPTCHAs. So orchestration/commands.py's
`bankruptcies_observations` Command carries no `workflow_step` and is absent
from TRACKED / the daily fetch_sources job; the committed store
(config/stores.yaml `bankruptcies`, mode: in_db) is untouched and keeps
flowing into every export.

HOW TO REFRESH. Two ways, both from a machine that still passes the CAPTCHA
(this one, as of 2026-09-23):
  1. Live, unattended:  python scripts/sync_bankruptcies.py --db data/belgian_macro.db
  2. From a hand-downloaded file, when even this machine gets challenged:
     open the landing page (LANDING_PAGE_URL below) in a browser (which
     passes the CAPTCHA interactively), follow its `TF_BANKRUPTCIES(<year>).zip`
     link, save the zip, then:
       python scripts/sync_bankruptcies.py --db data/belgian_macro.db \
           --from-file TF_BANKRUPTCIES_2026.zip
     `--from-file` runs the exact same parse/resolve/zero-fill path as the
     live fetch (BankruptciesSource._parse, then the same PINNED_PERIOD
     resolution and zero-fill below) -- only the transport differs.
The store's `max_age_days` / the `staleness` validation rule is what flags
when a refresh is actually due -- this source's own fetch_window_days no
longer applies since it is not in the daily gate, but staleness still
checks the DATA's own age regardless of how it arrived.

GEOGRAPHY: PINNED at "2026", the same pattern scripts/sync_police.py and
scripts/sync_realestate.py already use, and for the same reason -- the file
is published on TODAY'S 565-commune map (measured 2026-09-23: 31
post-merger codes, e.g. 44083, 23106, 37021, carry real rows in years before
those communes existed, so the file backcasts the current grid onto its
whole history the same way police.be and the real-estate file do). Every
row resolves via resolve_geo(conn, nis, PINNED_PERIOD); an unresolved code
is collected and the whole run refused at the end (never a partial load),
exactly as sync_police.py's `unresolved` list does.

THE ZERO RULE IS THE POINT OF THIS SCRIPT. A commune with no row in the file
for a given month had ZERO bankruptcies that month -- the maintainer's own
words: "une commune sans faillite un mois donné compte pour 0". So after
summing the file's own rows, this script builds the full commune x month
grid (every commune LIVE on the pinned map, at PINNED_PERIOD -- level =
'municipality' AND valid_to IS NULL, the same "current live map" query
export_communes_csv.py and friends already use) x every month the FILE
itself covers, and writes an explicit `value=0.0, status='final'` row for
every cell that has no entry from the summed totals. This is why the
zero-fill iterates the GEOGRAPHY map, not the file's own distinct NIS codes
-- Herstappe (73028) never appears in the file across 21 years, and would be
silently dropped if the fill iterated the file's codes instead.

Two boundaries on the fill, both load-bearing:
  * NEVER a month beyond the file's own last observed month. The range is
    derived from min/max of the periods actually present in the summed
    totals, never from today's date -- a September run must not invent an
    August zero-row that Statbel has not published data for yet in EITHER
    direction (a real nonzero row or an explicit absence meaning zero).
  * NEVER a month before the file's own first observed month either, for the
    same reason -- the fill covers exactly [min period, max period] as
    observed, nothing invented outside that window.

is_additive=1, aggregation_method='sum' for both indicators: BANKRUPTCIES is
a plain count and BANKRUPTCY_JOBS_LOST is a count of workers at bankrupt
firms, both summable to province/region/Belgium the normal way (unlike
police.be's rates or WalStat's per-capita euros, which have no underlying
total to recompute from).
"""

from __future__ import annotations

import argparse
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
from src.fetchers.bankruptcies import (  # noqa: E402
    BANKRUPTCIES,
    BANKRUPTCY_JOBS_LOST,
    BankruptciesSource,
    discover_zip_url,
)
from src.geography.resolve import UnknownGeographyError, resolve_geo  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

LANDING_PAGE_URL = "https://statbel.fgov.be/fr/open-data/evolution-mensuelle-des-faillites-par-nace"

# The period every row resolves against, regardless of the month in the
# row's own CD_YEAR/CD_MONTH -- see module docstring: the file backcasts
# today's commune grid onto its whole history, the same convention
# scripts/sync_police.py (2024 pin) and scripts/sync_realestate.py (2025
# pin) already follow for their own sources. This source's fixed code set
# matches the map as of 2026, the most current pin of the three.
PINNED_PERIOD = "2026"

DATASETS = (BANKRUPTCIES, BANKRUPTCY_JOBS_LOST)


def _fetch_landing_page_html() -> str:
    """A plain GET for the landing page HTML -- not through DataSource.fetch(),
    since that caches raw bytes under one (source_id, cache_key) keyed by the
    ADAPTER's own source_id/raw_extension, and this page is not the zip. This
    machine's TLS interception means a live GET here needs an unverified SSL
    context, exactly like every other live network call this pipeline makes
    from this environment; nothing about that is specific to Statbel.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(LANDING_PAGE_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _live_communes(conn: sqlite3.Connection) -> dict[str, str]:
    """{geo_id: geo_id} for every commune live on the pinned (current) map --
    the same `level = 'municipality' AND valid_to IS NULL` query
    export_communes_csv.py and friends use for "today's grid". Returned as a
    dict for a cheap membership test in the fill loop below."""
    rows = conn.execute(
        "SELECT geo_id FROM geographies WHERE level = 'municipality' AND valid_to IS NULL"
    ).fetchall()
    return {geo_id: geo_id for (geo_id,) in rows}


def _months_between(start: str, end: str) -> list[str]:
    """Every "YYYY-MM" from `start` to `end` inclusive."""
    start_year, start_month = (int(x) for x in start.split("-"))
    end_year, end_month = (int(x) for x in end.split("-"))
    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            month = 1
            year += 1
    return months


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    # INSERT OR IGNORE, same as scripts/sync_realestate.py's own
    # _ensure_reference_rows: whichever store's reference-rows script runs
    # first (config/stores.yaml's declaration order) creates this row, every
    # later one is a harmless no-op against the same source_id. Values match
    # config/sources/statbel.yaml, which this batch does not touch.
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('statbel', 'Statbel Bestat API', 'Statbel', 'statbel',
                'https://bestat.statbel.fgov.be/bestat/api', ?,
                'docs/data_catalog.md -- Bankruptcies indicators', 'monthly (automatic)', 1)
        """,
        (
            'See docs/data_catalog.md "Statbel licence" -- two Statbel documents, both '
            "confirmed to grant commercial reuse",
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
            VALUES (?, 'statbel', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'sum', 1, 1, ?, 1)
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
    zip_bytes: bytes | None = None,
) -> tuple[int, int]:
    """Returns (rows_read, rows_written).

    `zip_bytes`, when given, replaces the landing-page discovery and live
    download entirely -- what tests use to run the real parse/fill/resolve
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

    source = BankruptciesSource()
    now = datetime.now(timezone.utc).isoformat()

    if zip_bytes is not None:
        rows = source._parse(zip_bytes)
        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            ("statbel", "bankruptcies", now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    else:
        html = _fetch_landing_page_html()
        zip_url = discover_zip_url(html)
        rows = source.fetch(zip_url, cache_key="bankruptcies", conn=conn)
        fetch_run_id = conn.execute(
            "SELECT fetch_run_id FROM fetch_runs WHERE source_id = 'statbel' AND "
            "adapter = 'bankruptcies' ORDER BY fetch_run_id DESC LIMIT 1"
        ).fetchone()[0]

    if not rows:
        raise ValueError(
            "Statbel's bankruptcies file produced zero rows after parsing -- refusing to "
            "load an empty series (CLAUDE.md rule 13)."
        )

    # PASS 1: resolve every (indicator, nis, period) cell at the pinned
    # period, exactly as sync_police.py / sync_realestate.py do. Collect
    # every failure and refuse the whole run at the end rather than loading
    # a partial series.
    unresolved: list[tuple[str, str, str]] = []
    by_indicator_geo: dict[str, dict[str, dict[str, float]]] = {
        indicator_id: {} for indicator_id in DATASETS
    }
    periods_seen: set[str] = set()

    for row in rows:
        indicator_id = row["indicator_id"]
        nis = row["geo_id"]
        period = row["period"]
        periods_seen.add(period)
        try:
            geo_id = resolve_geo(conn, nis, PINNED_PERIOD)
        except UnknownGeographyError:
            unresolved.append((indicator_id, period, nis))
            continue
        by_indicator_geo[indicator_id].setdefault(geo_id, {})[period] = row["value"]

    if unresolved:
        raise SystemExit(
            f"::error::{len(unresolved)} (indicator, period, NIS) triple(s) did not resolve "
            f"at the pinned period {PINNED_PERIOD!r}, e.g. {unresolved[:5]}. Refusing to load "
            "a partial series."
        )

    if not periods_seen:
        raise ValueError("No periods found in the parsed rows -- nothing to load or zero-fill.")
    period_min, period_max = min(periods_seen), max(periods_seen)
    all_months = _months_between(period_min, period_max)
    live_communes = _live_communes(conn)

    rows_read = len(rows)
    rows_written = 0
    zero_filled = 0

    for indicator_id in DATASETS:
        by_geo = by_indicator_geo[indicator_id]
        for geo_id in sorted(live_communes):
            values_by_period = by_geo.get(geo_id, {})
            for period in all_months:
                if period in values_by_period:
                    value = values_by_period[period]
                    status = "final"
                else:
                    # See module docstring: no row for this commune-month
                    # means zero bankruptcies that month, a real measured
                    # zero -- not missing, not na (CLAUDE.md rule 26).
                    value = 0.0
                    status = "final"
                    zero_filled += 1
                period_start, period_end = derive_period_bounds(period, "M")
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
        f"Read {rows_read} summed (indicator, commune, month) cells from the file; wrote "
        f"{rows_written} vintage(s) across {len(live_communes)} live communes and "
        f"{len(all_months)} months ({period_min}..{period_max}), {zero_filled} of them "
        "explicit zero-fills."
    )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Load Statbel's monthly bankruptcies-by-NACE file (not part of the daily "
            "automatic fetch since 2026-09-23 -- see module docstring)"
        )
    )
    ap.add_argument("--db", required=True)
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no network",
    )
    ap.add_argument(
        "--from-file",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "Load a hand-downloaded TF_BANKRUPTCIES(<year>).zip instead of discovering and "
            "fetching live. Same parse/resolve/zero-fill path as the live fetch; only the "
            "transport differs. No network."
        ),
    )
    args = ap.parse_args()
    if args.from_file:
        read, written = sync(
            Path(args.db), args.reference_rows_only, zip_bytes=args.from_file.read_bytes()
        )
    else:
        read, written = sync(Path(args.db), args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for: {', '.join(DATASETS)}")
    else:
        print(f"Wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
