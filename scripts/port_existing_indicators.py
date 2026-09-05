"""
One-off port of the legacy SOURCES dict (belgian_macro_db.py) into the
canonical five-table model (docs/features/data_model.md, frozen by
docs/decisions/0001-data-model.md).

Requires live network access: re-fetches every ported indicator via the
existing NBBFetcher/DBnomicsFetcher classes (reused, not reimplemented) to
get real per-row status information, rather than trusting the old DB's
mostly non-informative obs_status column. This is a deliberate, accepted
cost of a one-off script -- the daily pipeline does not do this.

SCOPE: eligibility (which indicators are Belgium-only, canonical-eligible)
and preferred_direction both come from config/indicators/*.yaml
(src.validation.config_schema.is_canonical_eligible), not a hardcoded list.
A non-Belgium indicator (config `country` != "BE", e.g. the DE/FR/NL/ES/EA
feeder series) is skipped with a logged reason; the canonical model's
geo_id convention has no codes for those yet.

The OBS_STATUS mapping below is a plausible reading of the SDMX
CL_OBS_STATUS codelist as commonly used by NBB's SDMX 2.1 API. It has NOT
been independently re-verified against a live NBB response in this pass --
spot-check it against real fetched data before trusting the port's status
values, and add any code encountered but missing from the table rather
than guessing at its meaning.
"""

import calendar
import logging
import sqlite3
import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from belgian_macro_db import CONFIG_DIR, SOURCES, DBnomicsFetcher, NBBFetcher  # noqa: E402
from src.validation.config_schema import is_canonical_eligible, load_and_validate_all  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("port_existing_indicators")

BE_COUNTRY_GEO = {
    "geo_id": "be:country",
    "nis_code": None,
    "level": "country",
    "name_nl": "België",
    "name_fr": "Belgique",
    "name_en": "Belgium",
    "parent_geo_id": None,
    "valid_from": "1830-01-01",
    "valid_to": None,
    "successor_geo_id": None,
    "population": None,
    "area_km2": None,
}

# SDMX CL_OBS_STATUS -> canonical status enum. Any code not listed here is a
# hard error, never a silent default (CLAUDE.md rule 13: fail loudly).
OBS_STATUS_MAP = {
    "A": "final",  # Normal value
    "P": "provisional",  # Provisional value
    "E": "estimate",  # Estimated value
    "B": "revised",  # Break in series -- weakest mapping here; re-verify if seen
    "M": "na",  # Missing value
    "S": "suppressed",  # Statistical disclosure control, if ever encountered
}


def map_obs_status(raw: str) -> str:
    raw = (raw or "").strip()
    if raw not in OBS_STATUS_MAP:
        raise ValueError(
            f"Unrecognized SDMX OBS_STATUS code {raw!r}. Refusing to guess "
            "(CLAUDE.md rule 13: fail loudly, never silently coerce). "
            "Add it to OBS_STATUS_MAP after confirming its meaning."
        )
    return OBS_STATUS_MAP[raw]


def derive_period_bounds(period: str, frequency: str) -> tuple[str, str]:
    """Map a period string to (period_start, period_end) per
    docs/features/data_model.md's period format table."""
    if frequency == "A":
        year = int(period)
        return f"{year:04d}-01-01", f"{year:04d}-12-31"
    if frequency == "Q":
        year_s, q_s = period.split("-Q")
        year, q = int(year_s), int(q_s)
        start_month = (q - 1) * 3 + 1
        end_month = start_month + 2
        last_day = calendar.monthrange(year, end_month)[1]
        return f"{year:04d}-{start_month:02d}-01", f"{year:04d}-{end_month:02d}-{last_day:02d}"
    if frequency == "M":
        year, month = (int(x) for x in period.split("-"))
        last_day = calendar.monthrange(year, month)[1]
        return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"
    if frequency == "D":
        return period, period
    raise ValueError(f"Unknown frequency {frequency!r} for period {period!r}")


def source_id_for(agency: str) -> str:
    return agency.lower().replace("/", "_").replace(" ", "_")


def port(db_path: Path, run_date: str | None = None) -> None:
    run_date = run_date or date.today().isoformat()
    now = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")

    conn.execute(
        """
        INSERT OR IGNORE INTO geographies
            (geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id,
             valid_from, valid_to, successor_geo_id, population, area_km2)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        tuple(BE_COUNTRY_GEO.values()),
    )

    indicator_configs, source_configs = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )

    ported = 0
    for code, meta in SOURCES.items():
        ind_config = indicator_configs[code]
        if not is_canonical_eligible(ind_config, source_configs):
            log.warning(f"SKIP {code}: non-Belgium series, out of scope this port (see docstring)")
            continue
        preferred_direction = ind_config["preferred_direction"]

        agency = meta["source_agency"]
        source_id = source_id_for(agency)
        conn.execute(
            """
            INSERT OR IGNORE INTO sources
                (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
            (
                source_id,
                agency,
                agency,
                meta["type"],
                None,
                None,
                "docs/data_catalog.md (pending)",
                None,
            ),
        )

        conn.execute(
            "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
            (source_id, meta["type"], now, "ok"),
        )
        fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        conn.execute(
            """
            INSERT OR IGNORE INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'population_weighted', 0, 1, ?, 1)
        """,
            (
                code,
                source_id,
                meta["name"],
                meta["name"],
                meta["name"],
                meta.get("description", ""),
                meta["frequency"],
                meta["unit"],
                preferred_direction,
                f"scripts/port_existing_indicators.py::{code}",
            ),
        )

        log.info(f"Fetching {code} ({meta['type']})...")
        if meta["type"] == "nbb":
            rows = NBBFetcher.fetch(meta["url"])
            mapped = [(r["period"], r["value"], map_obs_status(r["obs_status"])) for r in rows]
        else:
            rows = DBnomicsFetcher.fetch(meta["url"], meta.get("unit", ""))
            # DBnomicsFetcher hardcodes obs_status="A" for every row -- there
            # is no real per-row status from this source today. Carried
            # forward as 'final': an inherited simplification, not a new
            # claim asserted by this migration.
            mapped = [(r["period"], r["value"], "final") for r in rows]

        for period, value, status in mapped:
            period_start, period_end = derive_period_bounds(period, meta["frequency"])
            conn.execute(
                """
                INSERT OR IGNORE INTO observations
                    (indicator_id, geo_id, period, vintage, value, status,
                     period_start, period_end, is_latest, fetch_run_id, created_at)
                VALUES (?, 'be:country', ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
                (
                    code,
                    period,
                    run_date,
                    value,
                    status,
                    period_start,
                    period_end,
                    fetch_run_id,
                    now,
                ),
            )

        conn.execute(
            "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
            "WHERE fetch_run_id = ?",
            (datetime.now(timezone.utc).isoformat(), len(rows), len(mapped), fetch_run_id),
        )
        log.info(f"  OK {code}: {len(mapped)} rows")
        ported += 1

    conn.commit()
    conn.close()
    log.info(f"Ported {ported} indicators, skipped {len(SOURCES) - ported}.")


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="One-off port of legacy indicators into the new model")
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file (already migrated)")
    args = ap.parse_args()
    port(Path(args.db))


if __name__ == "__main__":
    main()
