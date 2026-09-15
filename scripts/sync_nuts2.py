"""
Fetch the Europe NUTS 2 batch's three regional Eurostat indicators and write
them into the canonical `observations` table (Europe NUTS 2, batch B2,
docs/features/europe_nuts2.md; docs/decisions/0009-nuts2-regional-geography.md
and its 2026-09-14 amendment).

A PARALLEL script to scripts/sync_international.py, not a change to it (the
lead's decision, docs/decisions/0009's amendment, chose the parallel-loader
option: the pluggable-resolver alternative would have meant threading a
second allowlist file and a second geo-code shape through
is_multi_geo()/_resolve_geo(), a wider change to already-tested code, for a
loader that fetches only three indicators today). The ONE change made to
sync_international.py itself is pilot_indicators() excluding
`geo_levels: [nuts2]` configs, so the two scripts' indicator sets never
overlap -- see that function's own docstring.

GEOGRAPHY, PER ROW (config/geography/nuts2.csv + nuts2_excluded.csv,
src/geography/nuts2.py; the licence allowlist is still
config/geography/international.csv / international_excluded.csv, read by
2-letter country prefix):
  - a code that is not 4-character NUTS-2-shaped (is_nuts2_code()) is
    skipped outright -- these regional datasets return every NUTS level
    (0-3) mixed into the same `geo` dimension, and only level 2 is this
    loader's concern.
  - a pseudo-region code ('..ZZ' extra-regio, '..XX' not-regionalised) is
    skipped, not an error -- not a real, mappable area (CLAUDE.md rule 26:
    this is neither 'missing' nor 'suppressed', it never existed as a
    region at all, so it gets no geo_id and is never counted anywhere).
  - a non-region aggregate code (nuts2_excluded.csv: EA21, EFTA, ...) is
    skipped, not an error -- same reasoning, Eurostat's grouping code, not
    a place.
  - a NUTS-2-shaped, non-pseudo, non-aggregate code whose 2-letter prefix
    is licence-excluded (international_excluded.csv, e.g. UK*) is
    EXCLUDED_BY_LICENCE: not written, but counted and reported so
    scripts/export_europe_nuts2.py can list it for the map to grey out --
    this is NOT 'suppressed' (that means the source withheld a figure;
    here Eurostat published one and this pipeline chooses not to
    redistribute it -- CLAUDE.md rule 26 keeps these distinct).
  - a NUTS-2-shaped, non-pseudo, non-aggregate code whose prefix is
    licence-allowed but who has no nuts2.csv row -- OR whose prefix is in
    NEITHER international.csv nor international_excluded.csv -- FAILS THE
    FETCH (CLAUDE.md rule 13): a region this pipeline has made no decision
    about must not silently start being published, exactly like
    sync_international.py's whole-country version of the same rule.

ADAPTER GAP, FOUND WHILE BUILDING THIS SCRIPT (2026-09-14, live fetch): a
genuine minority of cells in lfst_r_lfu3rt (149 of 12,116 flagged-or-valued
cells; e.g. geo=DE22, period=2020, flag "bu") and one cell in
demo_r_pjanaggr3 (geo=PL912, period=2010, flag "b") carry an OBS_FLAG with
NO value at all -- EurostatSource._parse refuses these outright, because
the resulting canonical status ('estimate'/'final') is not in
NULLABLE_STATUSES and a None value would fail the observations table's own
CHECK constraint. This is a DIFFERENT gap from the compound-flag/`u`-mapping
fix already merged (#168): those made every FLAG letter and combination
resolvable; this is about a flag attached to a position that was never
assigned a value in the first place. src/fetchers/eurostat.py is NOT
touched by this batch (out of scope, no ADR covers it) -- so
UNEMPLOYMENT_RATE_NUTS2 and POPULATION_NUTS2 are configured (their .yaml
exists, approved by the maintainer, matching the spec's coverage tables) but
their sync FAILS loudly here, exactly as CLAUDE.md rule 13 requires, rather
than silently loading a truncated dataset. GDP_PC_PPS_NUTS2
(nama_10r_2gdp) has zero such cells and loads cleanly. See
docs/features/europe_nuts2.md, "Coverage" / "Measured at load, 2026-09-14".

Usage:  python scripts/sync_nuts2.py --db data/local/working.db
        python scripts/sync_nuts2.py --db X --from-dir tests/fixtures/... (no network)
        python scripts/sync_nuts2.py --db X --reference-rows-only
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.fetchers.base import FetchError  # noqa: E402
from src.fetchers.eurostat import EurostatSource  # noqa: E402
from src.geography.international import load_excluded as load_international_excluded  # noqa: E402
from src.geography.international import load_international_rows  # noqa: E402
from src.geography.nuts2 import (  # noqa: E402
    EXCLUDED_BY_LICENCE,
    GEO_COLUMNS,
    KEPT,
    NON_REGION_AGGREGATE,
    PSEUDO_REGION,
    Nuts2GeographyError,
    check_belgian_cross_references,
    classify_nuts2_code,
    is_nuts2_code,
    load_nuts2_rows,
)
from src.geography.nuts2 import (
    load_excluded as load_nuts2_excluded,
)
from src.validation.config_schema import is_multi_geo, load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

#: Display metadata only -- never applied to a stored value. Matches
#: sync_international.py's own DECIMALS constant (that pilot uses one
#: uniform value for all five indicators regardless of unit; kept
#: consistent here rather than inventing a per-indicator table).
DECIMALS = 2


class SyncNuts2Error(Exception):
    """A NUTS 2 indicator's fetch could not be resolved to canonical rows."""


def nuts2_indicators(indicator_configs: dict) -> dict[str, dict]:
    """The configs this script owns: `source_id: eurostat`, a multi-geo
    fetch, and `geo_levels: [nuts2]` -- the three Europe NUTS 2 indicators,
    never the five country-level pilot ones or the eight single-country
    ones."""
    return {
        code: cfg
        for code, cfg in sorted(indicator_configs.items())
        if cfg.get("source_id") == "eurostat"
        and is_multi_geo(cfg)
        and "nuts2" in (cfg.get("geo_levels") or [])
    }


def _ensure_source_row(conn: sqlite3.Connection, source: dict) -> None:
    """UPSERT, matching sync_international.py's own -- both scripts share
    the same 'eurostat' sources row, so whichever runs first writes it and
    the other's UPSERT is a no-op change."""
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


#: Whether summing this indicator across regions is a defensible operation,
#: independent of whether anything currently DOES that sum -- a structural
#: fact about the indicator, per CLAUDE.md's own aggregate rules (additive
#: counts/totals: SUMMED; a ratio/average: recomputed, never averaged;
#: neither: no defensible aggregate). A population count is additive;
#: GDP-per-capita and a rate/percentage are not. Nothing aggregates NUTS 2
#: geographies today (no config/no exporter reads geo_levels: nuts2 for a
#: province/region/country-style rollup), so this is inert either way, but
#: it should still say what is actually true about the indicator rather
#: than a blanket placeholder (PR #174 audit, NIT).
IS_ADDITIVE = {
    "GDP_PC_PPS_NUTS2": 0,
    "POPULATION_NUTS2": 1,
    "UNEMPLOYMENT_RATE_NUTS2": 0,
    # Eurostat additional domains batch (docs/data_catalog.md, 2026-09-15).
    "VALUE_ADDED_GROWTH_NUTS2": 0,  # a chain-linked volume INDEX -- not additive
    "EMPLOYMENT_RATE_NUTS2": 0,  # a rate -- not additive
    # RD_INTENSITY_NUTS2 (rd_e_gerdreg) DROPPED, not built -- live fetch
    # 2026-09-15 hit an unrecognized Eurostat OBS_FLAG ('C', uppercase,
    # within a compound flag) at geo=BE10 (Brussels-Capital Region), a real,
    # wanted Belgian geography -- unlike the country-level drops in this
    # same batch, this one cannot be worked around by excluding a geography,
    # since BE10 must be published. Extending FLAG_STATUS is a source-adapter
    # change (CLAUDE.md rule 19: separate ADR + maintainer approval), out of
    # scope here. See docs/data_catalog.md.
    "HOUSEHOLD_INCOME_TOTAL_NUTS2": 1,  # a TOTAL (million PPS) -- additive
}


def _ensure_indicator_rows(conn: sqlite3.Connection, indicators: dict[str, dict]) -> None:
    for code, ind in indicators.items():
        conn.execute(
            """
            INSERT INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'eurostat', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'not_applicable', ?, ?, ?, 1)
            ON CONFLICT(indicator_id) DO UPDATE SET
                name_nl = excluded.name_nl,
                name_fr = excluded.name_fr,
                name_en = excluded.name_en,
                description_en = excluded.description_en,
                unit = excluded.unit,
                preferred_direction = excluded.preferred_direction,
                is_additive = excluded.is_additive
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
                IS_ADDITIVE[code],
                DECIMALS,
                f"config/indicators/{code}.yaml",
            ),
        )


def _ensure_geography_rows(conn: sqlite3.Connection, nuts2_rows: dict[str, dict]) -> None:
    """Every nuts2.csv row: INSERT OR IGNORE, never UPDATE -- matches
    sync_international.py's own reasoning (a geography already loaded some
    other way is left exactly as it is)."""
    for row in nuts2_rows.values():
        geo = row["geo"]
        conn.execute(
            f"""
            INSERT OR IGNORE INTO geographies ({", ".join(GEO_COLUMNS)})
            VALUES ({", ".join("?" for _ in GEO_COLUMNS)})
            """,
            tuple(geo[c] for c in GEO_COLUMNS),
        )


def _ensure_reference_rows(
    conn: sqlite3.Connection, source: dict, indicators: dict[str, dict], nuts2_rows: dict[str, dict]
) -> None:
    _ensure_source_row(conn, source)
    _ensure_indicator_rows(conn, indicators)
    _ensure_geography_rows(conn, nuts2_rows)
    conn.commit()


def sync(
    db_path: Path,
    from_dir: Path | None = None,
    reference_rows_only: bool = False,
) -> tuple[int, int]:
    """Returns (rows_fetched, rows_changed).

    `from_dir` reads `{code}.json` files instead of the network -- what the
    tests use. The parse and every classification/refusal are identical on
    both paths; only the transport differs.
    """
    indicator_configs, source_configs = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    source_meta = source_configs["eurostat"]
    indicators = nuts2_indicators(indicator_configs)
    nuts2_rows = load_nuts2_rows()
    # The loader itself checks the Belgian belgian_geo_id cross-references
    # (not only tests/test_nuts2_geography.py at PR time) -- a boundary
    # change to geographies.csv that silently broke one would otherwise only
    # be caught on the next PR that happens to touch this file, not on the
    # next real sync (PR #174 audit, SHOULD-FIX 4).
    cross_ref_problems = check_belgian_cross_references(nuts2_rows)
    if cross_ref_problems:
        raise Nuts2GeographyError(
            "nuts2.csv's Belgian belgian_geo_id cross-references no longer match "
            f"geographies.csv: {cross_ref_problems}"
        )
    nuts2_excluded = load_nuts2_excluded()
    international_allowed_prefixes = {
        code
        for code, row in load_international_rows().items()
        if row["scope"] == "pilot" and len(code) == 2
    }
    international_excluded_prefixes = {c for c in load_international_excluded() if len(c) == 2}

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, source_meta, indicators, nuts2_rows)
    if reference_rows_only:
        conn.close()
        return 0, 0

    now = datetime.now(timezone.utc).isoformat()
    fetched = changed = 0
    source = EurostatSource()

    for code, ind in indicators.items():
        dataset = ind["fetch"]["dataset"]
        filters = ind["fetch"].get("filters") or {}
        since = ind["fetch"].get("since", "2000")

        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            ("eurostat", "eurostat", now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        try:
            # geo_filter=is_nuts2_code: this loader only ever wants 4-character
            # NUTS 2-shaped codes, so a cell for a NUTS 0/1/3 geography this
            # fetch never asked for (e.g. demo_r_pjanaggr3's PL912, a NUTS 3
            # code) is never even flag/value-validated -- see
            # EurostatSource._parse's own docstring. A cell for a geography
            # THIS loader does want is still validated exactly as before,
            # including refusing loudly on a flag with no value.
            if from_dir is not None:
                raw = (from_dir / f"{code}.json").read_bytes()
                geo_rows_fetched = source._parse(raw, dataset=dataset, geo_filter=is_nuts2_code)
            else:
                url = EurostatSource.build_url(source_meta["base_url"], dataset, filters, since)
                geo_rows_fetched = source.fetch(
                    url, cache_key=code, dataset=dataset, geo_filter=is_nuts2_code
                )

            resolved: list[tuple[dict, str]] = []
            seen_codes: set[str] = set()
            classification_counts: Counter[str] = Counter()
            excluded_by_licence: set[str] = set()

            for row in geo_rows_fetched:
                geo_code = row["geo"]
                if not is_nuts2_code(geo_code):
                    continue  # NUTS 0/1/3 -- not this loader's concern
                seen_codes.add(geo_code)
                classification, geo_id = classify_nuts2_code(
                    geo_code,
                    nuts2_rows,
                    nuts2_excluded,
                    international_allowed_prefixes,
                    international_excluded_prefixes,
                )
                classification_counts[classification] += 1
                if classification == KEPT:
                    resolved.append((row, geo_id))
                elif classification == EXCLUDED_BY_LICENCE:
                    excluded_by_licence.add(geo_code)
                # PSEUDO_REGION / NON_REGION_AGGREGATE: skipped silently,
                # counted only.

            absent = sorted(set(nuts2_rows) - seen_codes)
        except (FetchError, Nuts2GeographyError) as exc:
            conn.execute(
                "UPDATE fetch_runs SET finished_at = ?, status = 'error', message = ? "
                "WHERE fetch_run_id = ?",
                (datetime.now(timezone.utc).isoformat(), str(exc)[:500], fetch_run_id),
            )
            conn.commit()
            raise

        fetched += len(resolved)
        message = (
            f"{len(resolved)} row(s) across {len({g for _, g in resolved})} region(s); "
            f"licence-excluded geo codes seen: {len(excluded_by_licence)} "
            f"({classification_counts[EXCLUDED_BY_LICENCE]} cell(s)); "
            f"pseudo-region cells skipped: {classification_counts[PSEUDO_REGION]}; "
            f"aggregate cells skipped: {classification_counts[NON_REGION_AGGREGATE]}"
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
        description="Sync the Europe NUTS 2 batch's three regional Eurostat indicators"
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
        print("Reference rows ensured for the Europe NUTS 2 indicators.")
    else:
        print(f"Fetched {fetched} observations, {changed} new vintage(s) written.")


if __name__ == "__main__":
    main()
