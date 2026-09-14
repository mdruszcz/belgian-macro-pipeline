"""
Fetch the international pilot's five direct-Eurostat, every-country-at-once
indicators and write them into the canonical `observations` table --
international pilot PR 1 (docs/features/international.md).

Same shape as scripts/sync_walstat.py, the other DataSource-driven sync: one
live fetch per indicator through EurostatSource (which caches the raw
response under data/raw/eurostat/{date}/ and logs its own fetch_runs row),
then `upsert_observation` per row -- the shared insert-only-on-change
implementation (Block I), keyed per country/aggregate geo_id.

Which indicators this script owns is decided by the indicator configs, not a
list here: every config/indicators/*.yaml with `source_id: eurostat` and a
multi-geo `fetch` (fetch.geographies: allowlist, is_multi_geo() in
src/validation/config_schema.py). The eight OTHER eurostat-sourced indicators
(single-country, `fetch.filters.geo` pinned) are a different, pre-existing
path -- belgian_macro_db.py's fetch_all / scripts/port_existing_indicators.py
/ scripts/sync_to_canonical.py -- and are not touched by this script.

GEOGRAPHY, PER ROW (config/geography/international.csv +
international_excluded.csv, src/geography/international.py):
  - allowlisted, scope "pilot"  -> written, under that row's geo_id.
  - allowlisted, scope "legacy" -> recognized, but NOT written: today only
    bare "EA" (Eurostat's dynamic "current euro area"), kept allowlisted
    solely so EUROSTAT_GDP_Q_MEUR_EA's national fetch can still resolve it,
    not so the pilot re-publishes a second, ambiguous-vintage euro-area
    total beside EA21.
  - explicitly excluded (international_excluded.csv)              -> skipped,
    not an error (UK, XK, US, JP, and Eurostat's superseded/ambiguous
    aggregate codes).
  - in NEITHER file -> FAILS THE FETCH for that indicator (rule 13): a
    country this pipeline has made no licence decision about must not
    silently start being published.
  - allowlisted (scope pilot) but ABSENT from this dataset's response -> the
    run still ends 'ok'; every such code is named in the fetch_runs message
    rather than the run status turning 'partial' (deliberate deviation from
    a literal reading of the spec -- 'partial' is a FAIL in
    src/validation/rules.py's fetch_error rule, which would fail every daily
    run merely because, say, Ukraine has no unemployment series yet).

Usage:  python scripts/sync_international.py --db data/belgian_macro.db
        python scripts/sync_international.py --db X --from-dir tests/fixtures/... (no network)
        python scripts/sync_international.py --db X --reference-rows-only
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
from src.fetchers.base import FetchError  # noqa: E402
from src.fetchers.eurostat import EurostatSource  # noqa: E402
from src.geography.international import (  # noqa: E402
    GEO_COLUMNS,
    load_excluded,
    load_international_rows,
)
from src.validation.config_schema import is_multi_geo, load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

#: Rounded to what these five indicators actually publish (Eurostat's own
#: decimals -- a balance, a percentage, a volume in millions of euro).
DECIMALS = 2


class SyncInternationalError(Exception):
    """A pilot indicator's fetch could not be resolved to canonical rows."""


def pilot_indicators(indicator_configs: dict) -> dict[str, dict]:
    """The configs this script owns: `source_id: eurostat` and a multi-geo
    fetch -- the five pilot indicators, never the eight single-country ones."""
    return {
        code: cfg
        for code, cfg in sorted(indicator_configs.items())
        if cfg.get("source_id") == "eurostat" and is_multi_geo(cfg)
    }


def _ensure_source_row(conn: sqlite3.Connection, source: dict) -> None:
    """UPSERT (not INSERT OR IGNORE): the committed `sources` row for
    'eurostat' predates this pilot and still says adapter='dbnomics',
    catalog_ref='docs/data_catalog.md (pending)' -- the cutover this script
    drives must correct it, not leave the stale copy in place."""
    conn.execute(
        """
        INSERT INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES (?, ?, ?, 'eurostat', ?, ?, ?, ?, 1)
        ON CONFLICT(source_id) DO UPDATE SET
            name = excluded.name,
            agency = excluded.agency,
            adapter = excluded.adapter,
            base_url = excluded.base_url,
            licence = excluded.licence,
            catalog_ref = excluded.catalog_ref,
            cadence = excluded.cadence
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


def _ensure_indicator_rows(conn: sqlite3.Connection, indicators: dict[str, dict]) -> None:
    for code, ind in indicators.items():
        conn.execute(
            """
            INSERT INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'eurostat', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'not_applicable', 0, ?, ?, 1)
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


def _ensure_geography_rows(conn: sqlite3.Connection, rows: dict[str, dict]) -> None:
    """Every allowlisted geography (every scope): INSERT OR IGNORE, never an
    UPDATE -- a geography already loaded some other way (be:country, from the
    Belgian hierarchy) is left exactly as it is."""
    for row in rows.values():
        geo = row["geo"]
        conn.execute(
            f"""
            INSERT OR IGNORE INTO geographies ({", ".join(GEO_COLUMNS)})
            VALUES ({", ".join("?" for _ in GEO_COLUMNS)})
            """,
            tuple(geo[c] for c in GEO_COLUMNS),
        )


def _ensure_reference_rows(
    conn: sqlite3.Connection, source: dict, indicators: dict[str, dict], geo_rows: dict[str, dict]
) -> None:
    _ensure_source_row(conn, source)
    _ensure_indicator_rows(conn, indicators)
    _ensure_geography_rows(conn, geo_rows)
    conn.commit()


def _resolve_geo(
    code: str, geo_rows: dict[str, dict], excluded: dict[str, str]
) -> tuple[str, str] | None:
    """(geo_id, scope) for a fetched Eurostat geo code, or raises if this
    pipeline has made no licence decision about it at all.

    Returns None for a code this pipeline has explicitly decided NOT to
    publish (excluded, or allowlisted only for the legacy national path) --
    the caller skips those rows without treating them as an error.
    """
    if code in geo_rows:
        row = geo_rows[code]
        if row["scope"] != "pilot":
            return None
        return row["geo"]["geo_id"], row["scope"]
    if code in excluded:
        return None
    raise SyncInternationalError(
        f"geography {code!r} is neither allowlisted (config/geography/international.csv) "
        f"nor excluded (config/geography/international_excluded.csv). Refusing to guess "
        "whether it may be published (CLAUDE.md rule 13) -- add it to one file or the other."
    )


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
    source_meta = source_configs["eurostat"]
    indicators = pilot_indicators(indicator_configs)
    geo_rows = load_international_rows()
    excluded = load_excluded()

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, source_meta, indicators, geo_rows)
    if reference_rows_only:
        conn.close()
        return 0, 0

    now = datetime.now(timezone.utc).isoformat()
    fetched = changed = 0
    source = EurostatSource()

    for code, ind in indicators.items():
        dataset = ind["fetch"]["dataset"]
        filters = ind["fetch"].get("filters") or {}
        since = ind["fetch"].get("since", "2008")

        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            ("eurostat", "eurostat", now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        try:
            if from_dir is not None:
                raw = (from_dir / f"{code}.json").read_bytes()
                geo_rows_fetched = source._parse(raw, dataset=dataset)
            else:
                # Deliberately no `conn=` here (audit SHOULD-FIX 7): this
                # loop already opened and will close its own fetch_runs row
                # per indicator, exactly like
                # scripts/port_existing_indicators.py's one-off port()
                # already does for the same reason ("a second, adapter-level
                # row would describe the same network call from a different
                # angle and is not needed"). Passing `conn` here made
                # DataSource.fetch()'s own `finally` block log a SECOND row,
                # always 'ok' even when this loop's OWN row was later marked
                # 'error' -- so fetch_error (which reads the highest
                # fetch_run_id per source) saw the adapter's later 'ok' row
                # and missed the failure entirely. Ten eurostat rows for five
                # fetches in the committed db was the symptom.
                url = EurostatSource.build_url(source_meta["base_url"], dataset, filters, since)
                geo_rows_fetched = source.fetch(url, cache_key=code, dataset=dataset)

            resolved: list[tuple[dict, str]] = []
            seen_codes: set[str] = set()
            for row in geo_rows_fetched:
                seen_codes.add(row["geo"])
                target = _resolve_geo(row["geo"], geo_rows, excluded)
                if target is None:
                    continue
                geo_id, _scope = target
                resolved.append((row, geo_id))

            absent = sorted(
                code_
                for code_, row in geo_rows.items()
                if row["scope"] == "pilot" and code_ not in seen_codes
            )
        except (FetchError, SyncInternationalError) as exc:
            conn.execute(
                "UPDATE fetch_runs SET finished_at = ?, status = 'error', message = ? "
                "WHERE fetch_run_id = ?",
                (datetime.now(timezone.utc).isoformat(), str(exc)[:500], fetch_run_id),
            )
            conn.commit()
            raise

        fetched += len(resolved)
        message = (
            f"{len(resolved)} row(s) across {len({g for _, g in resolved})} geograph(y/ies)"
            + (f"; allowlisted but absent from this dataset: {', '.join(absent)}" if absent else "")
        )
        print(f"  {code}: {message}")

        for row, geo_id in resolved:
            period_start, period_end = derive_period_bounds(row["period"], ind["frequency"])
            changed += upsert_observation(
                conn,
                indicator_id=code,
                geo_id=geo_id,
                period=row["period"],
                period_start=period_start,
                period_end=period_end,
                value=row["value"],
                status=row["obs_status"],
                vintage=now,
                fetch_run_id=fetch_run_id,
            )

        conn.execute(
            "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ?, message = ? "
            "WHERE fetch_run_id = ?",
            (
                datetime.now(timezone.utc).isoformat(),
                len(geo_rows_fetched),
                len(resolved),
                message,
                fetch_run_id,
            ),
        )
        conn.commit()

    conn.close()
    return fetched, changed


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Sync the international pilot's five direct-Eurostat indicators"
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
        help="Insert only the sources/indicators/geographies rows; needs no network",
    )
    args = ap.parse_args()
    fetched, changed = sync(Path(args.db), args.from_dir, args.reference_rows_only)
    if args.reference_rows_only:
        print("Reference rows ensured for the international pilot indicators.")
    else:
        print(f"Fetched {fetched} observations, {changed} new vintage(s) written.")


if __name__ == "__main__":
    main()
