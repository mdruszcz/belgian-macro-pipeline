"""
Fetch WalStat (IWEPS) municipal-finance series and write them into the
canonical `observations` table -- Block O, docs/features/walstat_adapter.md.

Same shape as scripts/sync_statbel.py, the other DataSource-driven municipal
sync: one live fetch per indicator through the adapter (which caches the raw
response under data/raw/walstat/{date}/ and logs `fetch_runs` itself), then
`upsert_observation` per row -- the one shared insert-only-on-change
implementation (Block I), keyed per commune geo_id.

Which series this script loads is decided by the indicator configs, not by a
list here: every config/indicators/*.yaml with `source_id: walstat` carries
its own `fetch.query`, the API path for its series. Adding a series is a
config change (CLAUDE.md rule 2), never an edit to this file.

NOT AGGREGATABLE, ON PURPOSE. Every WalStat series here is euros PER
INHABITANT, published as such by the SPW; there is no underlying total in
the source to sum. So `is_additive=0`, `aggregation_method='not_applicable'`
-- the same call scripts/sync_police.py makes for its rates, for the same
reason (docs/decisions/0003): a province figure averaged from per-head
commune figures would be exactly the error that ADR exists to prevent.

Usage:  python scripts/sync_walstat.py --db data/belgian_macro.db
        python scripts/sync_walstat.py --db X --from-dir tests/fixtures/... (no network)
        python scripts/sync_walstat.py --db X --reference-rows-only
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.fetchers.walstat import WalStatSource  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

#: One decimal, as IWEPS publishes them (2319.7 euros per inhabitant).
DECIMALS = 1


def walstat_indicators(indicator_configs: dict) -> dict[str, dict]:
    """The configs this script owns: `source_id: walstat`, each with a fetch query."""
    mine = {
        code: cfg
        for code, cfg in sorted(indicator_configs.items())
        if cfg.get("source_id") == "walstat"
    }
    for code, cfg in mine.items():
        if not (cfg.get("fetch") or {}).get("query"):
            raise ValueError(
                f"config/indicators/{code}.yaml has source_id walstat but no fetch.query -- "
                "there is nothing to fetch and nothing to guess"
            )
    return mine


def _ensure_reference_rows(conn: sqlite3.Connection, source: dict, indicators: dict) -> None:
    """INSERT OR IGNORE the sources row, and UPSERT each indicator's metadata
    from its validated YAML -- names and direction may be corrected in config
    and must follow (sync_police.py's pattern), the id and its statistics
    never change from here."""
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES (?, ?, ?, 'walstat', ?, ?, ?, ?, 1)
        """,
        (
            source["source_id"],
            source["name"],
            source["agency"],
            source.get("base_url"),
            source.get("licence"),
            source.get("catalog_ref"),
            source.get("cadence"),
        ),
    )
    for code, ind in indicators.items():
        conn.execute(
            """
            INSERT INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'walstat', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'not_applicable', 0, ?, ?, 1)
            ON CONFLICT(indicator_id) DO UPDATE SET
                name_nl = excluded.name_nl,
                name_fr = excluded.name_fr,
                name_en = excluded.name_en,
                description_en = excluded.description_en,
                unit = excluded.unit,
                preferred_direction = excluded.preferred_direction
            """,
            (
                code,
                ind["name"]["nl"],
                ind["name"]["fr"],
                ind["name"]["en"],
                ind.get("description", {}).get("en", ""),
                ind["frequency"],
                ind["unit"],
                ind["preferred_direction"],
                DECIMALS,
                f"config/indicators/{code}.yaml",
            ),
        )
    conn.commit()


def sync(
    db_path: Path,
    from_dir: Path | None = None,
    reference_rows_only: bool = False,
) -> tuple[int, int]:
    """Returns (rows_fetched, rows_changed).

    `from_dir` reads `{code}.json` files instead of the network -- what the
    tests use, and what a maintainer uses to replay a cached day. The parse
    and every refusal are identical on both paths; only the transport differs.
    """
    indicator_configs, source_configs = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    source_meta = source_configs["walstat"]
    indicators = walstat_indicators(indicator_configs)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, source_meta, indicators)
    if reference_rows_only:
        conn.close()
        return 0, 0

    now = datetime.now(timezone.utc).isoformat()
    fetched = changed = 0
    source = WalStatSource()

    for code, ind in indicators.items():
        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            ("walstat", "walstat", now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        try:
            if from_dir is not None:
                raw = (from_dir / f"{code}.json").read_bytes()
                rows = source._parse(raw, geo_conn=conn)
            else:
                url = source_meta["base_url"] + ind["fetch"]["query"]
                # `conn` is for the adapter's own fetch_runs logging; `geo_conn`
                # is the parse-time geography lookup. Same connection, two roles.
                rows = source.fetch(url, cache_key=code, conn=conn, geo_conn=conn)
        except Exception as exc:
            # This script's own run row must not say `ok` for a series that
            # refused: the fetch_error validation rule reads the latest run.
            conn.execute(
                "UPDATE fetch_runs SET finished_at = ?, status = 'error', message = ? "
                "WHERE fetch_run_id = ?",
                (datetime.now(timezone.utc).isoformat(), str(exc)[:500], fetch_run_id),
            )
            conn.commit()
            raise
        fetched += len(rows)
        absent = sum(len(v) for v in source.missing.values())
        print(
            f"  {code}: {len(rows)} readings; {len(source.unavailable)} 'non disponible', "
            f"{len(source.backcast)} backcast rows for a not-yet-existing commune skipped, "
            f"{len(source.recoded)} attributed to a re-coded commune's earlier code, "
            f"{absent} commune-year(s) absent from the response"
        )

        for row in rows:
            period_start, period_end = derive_period_bounds(row["period"], ind["frequency"])
            changed += upsert_observation(
                conn,
                indicator_id=code,
                geo_id=row["geo_id"],
                period=row["period"],
                period_start=period_start,
                period_end=period_end,
                value=row["value"],
                status=row["status"],
                vintage=now,
                fetch_run_id=fetch_run_id,
            )

        conn.execute(
            "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
            "WHERE fetch_run_id = ?",
            (
                datetime.now(timezone.utc).isoformat(),
                source._rows_read_hint(rows) or len(rows),
                len(rows),
                fetch_run_id,
            ),
        )
        conn.commit()

    conn.close()
    return fetched, changed


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Sync WalStat (IWEPS) municipal finance series (Block O)"
    )
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file")
    ap.add_argument(
        "--from-dir",
        type=Path,
        default=None,
        help="Read {INDICATOR_ID}.json files from here instead of the network (tests, replays)",
    )
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no network",
    )
    args = ap.parse_args()
    fetched, changed = sync(Path(args.db), args.from_dir, args.reference_rows_only)
    if args.reference_rows_only:
        print("Reference rows ensured for the WalStat indicators.")
    else:
        print(f"Fetched {fetched} observations, {changed} new vintage(s) written.")


if __name__ == "__main__":
    main()
