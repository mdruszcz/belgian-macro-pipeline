"""
Belgian Macroeconomic Database
==============================
Fetches GDP and confidence data from NBB SDMX and DBnomics,
stores in SQLite, and exports to CSV/JSON.

Runs daily via GitHub Actions.
"""

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent / "scripts"))

from rename_legacy_tables import rename_legacy_tables  # noqa: E402

from src.db.migrate import run as run_migrations  # noqa: E402
from src.fetchers.eurostat import EurostatSource  # noqa: E402
from src.fetchers.fpb import FPB_XLSX_URL, FPBSource  # noqa: E402
from src.fetchers.nbb import NBBSource  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

# ─── Configuration ────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "data" / "belgian_macro.db"

CONFIG_DIR = Path(__file__).parent / "config"


def source_id_for(agency: str) -> str:
    """Duplicated from scripts/port_existing_indicators.py rather than
    imported: that script imports NBBSource/EurostatSource/FPBSource-adjacent
    names from *this* module, so importing the other way would be circular.
    Both copies must stay identical -- it is one line, and belgian_macro_db.py
    is what this repo already treats as upstream of scripts/."""
    return agency.lower().replace("/", "_").replace(" ", "_")


def _load_sources() -> dict:
    """Rebuild the SOURCES-dict shape fetch_all()/upsert_indicator() expect,
    from config/indicators/*.yaml + config/sources/*.yaml, per
    docs/features/indicator_config.md. Indicators whose source's adapter is
    not "nbb"/"dbnomics" (i.e. adapter "fpb", the forecast pseudo-indicators)
    are excluded -- forecasts are fetched by FPBSource directly, exactly as
    before, and never belonged in this dict."""
    indicators, sources = load_and_validate_all(CONFIG_DIR / "indicators", CONFIG_DIR / "sources")
    out = {}
    for code, ind in indicators.items():
        source = sources[ind["source_id"]]
        if source["adapter"] not in ("nbb", "dbnomics"):
            continue
        out[code] = {
            "name": ind["name"]["en"],
            "url": source["base_url"] + ind["fetch"]["query"],
            "frequency": ind["frequency"],
            "unit": ind["unit"],
            "source_agency": source["agency"],
            "description": ind.get("description", {}).get("en", ""),
            "type": source["adapter"],
        }
    return out


SOURCES = _load_sources()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("belgian_macro")


# ─── Database ─────────────────────────────────────────────────────


class MacroDatabase:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self):
        # Order matters: rename any legacy-shaped tables out of the way first,
        # then apply canonical migrations, then create the (now non-colliding,
        # renamed) legacy tables if they don't exist yet. See
        # scripts/rename_legacy_tables.py and docs/decisions/0001-data-model.md.
        rename_legacy_tables(self.conn)
        run_migrations(self.db_path)
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS legacy_indicators (
                code          TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                frequency     TEXT NOT NULL,
                unit          TEXT NOT NULL,
                source_agency TEXT NOT NULL,
                description   TEXT,
                api_url       TEXT
            );
            CREATE TABLE IF NOT EXISTS legacy_observations (
                indicator_code TEXT NOT NULL,
                period         TEXT NOT NULL,
                value          REAL NOT NULL,
                obs_status     TEXT,
                fetched_at     TEXT NOT NULL,
                PRIMARY KEY (indicator_code, period),
                FOREIGN KEY (indicator_code) REFERENCES legacy_indicators(code)
            );
            CREATE TABLE IF NOT EXISTS legacy_fetch_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator_code TEXT NOT NULL,
                fetched_at     TEXT NOT NULL,
                rows_upserted  INTEGER NOT NULL,
                status         TEXT NOT NULL,
                message        TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_obs_period
                ON legacy_observations(indicator_code, period DESC);
            CREATE TABLE IF NOT EXISTS forecasts (
                institution    TEXT NOT NULL,
                indicator      TEXT NOT NULL,
                year           TEXT NOT NULL,
                value          REAL,
                updated_at     TEXT,
                fetched_at     TEXT NOT NULL,
                PRIMARY KEY (institution, indicator, year)
            );
        """)
        self.conn.commit()

    def upsert_indicator(self, code: str, meta: dict):
        self.conn.execute(
            """
            INSERT INTO legacy_indicators (code, name, frequency, unit, source_agency, description, api_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name, frequency=excluded.frequency,
                unit=excluded.unit, source_agency=excluded.source_agency,
                description=excluded.description, api_url=excluded.api_url
        """,
            (
                code,
                meta["name"],
                meta["frequency"],
                meta["unit"],
                meta["source_agency"],
                meta.get("description", ""),
                meta.get("url", ""),
            ),
        )
        self.conn.commit()

    def upsert_observations(self, indicator_code: str, rows: list[dict]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        for row in rows:
            self.conn.execute(
                """
                INSERT INTO legacy_observations (indicator_code, period, value, obs_status, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(indicator_code, period) DO UPDATE SET
                    value=excluded.value, obs_status=excluded.obs_status,
                    fetched_at=excluded.fetched_at
            """,
                (indicator_code, row["period"], row["value"], row.get("obs_status", ""), now),
            )
        self.conn.commit()
        return len(rows)

    def log_fetch(self, code: str, count: int, status: str, msg: str = ""):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "INSERT INTO legacy_fetch_log (indicator_code, fetched_at, rows_upserted, status, message) VALUES (?,?,?,?,?)",
            (code, now, count, status, msg),
        )
        self.conn.commit()

    def get_latest(self, code: str) -> dict | None:
        cur = self.conn.execute(
            """
            SELECT o.period, o.value, o.obs_status, o.fetched_at, i.name, i.unit
            FROM legacy_observations o JOIN legacy_indicators i ON o.indicator_code = i.code
            WHERE o.indicator_code = ? ORDER BY o.period DESC LIMIT 1
        """,
            (code,),
        )
        r = cur.fetchone()
        if not r:
            return None
        return {
            "indicator_code": code,
            "period": r[0],
            "value": r[1],
            "obs_status": r[2],
            "fetched_at": r[3],
            "name": r[4],
            "unit": r[5],
        }

    def get_all_latest(self) -> list[dict]:
        codes = [
            r[0] for r in self.conn.execute("SELECT code FROM legacy_indicators ORDER BY code")
        ]
        return [latest for c in codes if (latest := self.get_latest(c))]

    def get_all_observations(self) -> pd.DataFrame:
        return pd.read_sql_query(
            """
            SELECT o.indicator_code, i.name, o.period, o.value,
                   o.obs_status, i.unit, i.source_agency, o.fetched_at
            FROM legacy_observations o JOIN legacy_indicators i ON o.indicator_code = i.code
            ORDER BY o.indicator_code, o.period
        """,
            self.conn,
        )

    def get_fetch_history(self, n: int = 20) -> list[dict]:
        cur = self.conn.execute(
            "SELECT indicator_code, fetched_at, rows_upserted, status, message FROM legacy_fetch_log ORDER BY id DESC LIMIT ?",
            (n,),
        )
        return [{"code": r[0], "at": r[1], "rows": r[2], "status": r[3], "msg": r[4]} for r in cur]

    def close(self):
        self.conn.close()

    def upsert_forecasts(self, rows: list[dict]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        for r in rows:
            self.conn.execute(
                """
                INSERT INTO forecasts (institution, indicator, year, value, updated_at, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(institution, indicator, year) DO UPDATE SET
                    value=excluded.value, updated_at=excluded.updated_at,
                    fetched_at=excluded.fetched_at
            """,
                (
                    r["institution"],
                    r["indicator"],
                    r["year"],
                    r.get("value"),
                    r.get("updated_at", ""),
                    now,
                ),
            )
        self.conn.commit()
        return len(rows)

    def get_all_forecasts(self) -> pd.DataFrame:
        return pd.read_sql_query(
            """
            SELECT institution, indicator, year, value, updated_at, fetched_at
            FROM forecasts ORDER BY indicator, year, institution
        """,
            self.conn,
        )


# ─── Fetchers ─────────────────────────────────────────────────────
#
# NBBSource, EurostatSource, FPBSource (src/fetchers/) replace the bare
# NBBFetcher/DBnomicsFetcher/FPBFetcher staticmethods that used to live here.
# See docs/features/source_adapter.md (Block D) -- the parsing logic in each
# is unchanged; what moved into the shared DataSource base class is the HTTP
# GET-with-retry, raw-response caching to data/raw/{source_id}/{date}/, and
# fetch_runs logging, none of which existed before this refactor.


# ─── Orchestration ────────────────────────────────────────────────


def fetch_all(db: MacroDatabase) -> bool:
    """Fetch every configured source. Returns False if any source failed.

    Each call passes db.conn as `conn` so every adapter logs a fetch_runs row
    in the same connection the legacy tables already use (Block D). The
    all_ok / db.log_fetch("OK"|"ERROR") contract below is unchanged from
    before that block -- fetch_runs is new observability alongside it, not a
    replacement (tests/test_fetch_all.py pins this down).

    source_id for fetch_runs is derived via source_id_for(agency), matching
    what scripts/sync_to_canonical.py already put in the canonical `sources`
    table ("eurostat", "ameco_ec") -- NOT the source_id used in
    config/sources/*.yaml ("dbnomics_eurostat", "dbnomics_ameco"). Those two
    registries disagree; this uses whichever one is actually populated today,
    rather than silently reconciling a pre-existing inconsistency that is out
    of scope for this refactor. See docs/features/source_adapter.md.
    """
    all_ok = True
    for code, meta in SOURCES.items():
        db.upsert_indicator(code, meta)
        source_id = source_id_for(meta["source_agency"])
        try:
            if meta.get("type") == "nbb":
                source = NBBSource()
                rows = source.fetch(meta["url"], cache_key=code, conn=db.conn)
            else:
                source = EurostatSource(source_id=source_id)
                rows = source.fetch(
                    meta["url"], cache_key=code, conn=db.conn, unit=meta.get("unit", "")
                )
            n = db.upsert_observations(code, rows)
            db.log_fetch(code, n, "OK")
            log.info(f"  OK {code}: {n} rows")
        except Exception as e:
            log.error(f"  FAIL {code}: {e}")
            db.log_fetch(code, 0, "ERROR", str(e))
            all_ok = False
    try:
        fc_rows = FPBSource().fetch(FPB_XLSX_URL, cache_key="FPB_FORECASTS", conn=db.conn)
        n = db.upsert_forecasts(fc_rows)
        db.log_fetch("FPB_FORECASTS", n, "OK")
    except Exception as e:
        log.error(f"  FAIL FPB_FORECASTS: {e}")
        db.log_fetch("FPB_FORECASTS", 0, "ERROR", str(e))
        all_ok = False
    return all_ok


def show_latest(db: MacroDatabase):
    latest = db.get_all_latest()
    if not latest:
        return
    print("\n" + "=" * 60)
    print("  BELGIAN MACRO DATABASE — Latest")
    print("=" * 60 + "\n")
    for e in latest:
        print(f"  {e['name']:<40} | {e['period']:<10} | {e['value']:>8.1f} {e['unit']}")


def export_data(db: MacroDatabase, fmt: str):
    df = db.get_all_observations()
    if df.empty:
        return
    out = Path(__file__).parent / "data"
    out.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        df.to_csv(out / "belgian_macro_export.csv", index=False)
    elif fmt == "json":
        df.to_json(out / "belgian_macro_export.json", orient="records", indent=2)
    fc = db.get_all_forecasts()
    if not fc.empty:
        if fmt == "csv":
            fc.to_csv(out / "belgian_forecasts.csv", index=False)
        elif fmt == "json":
            fc.to_json(out / "belgian_forecasts.json", orient="records", indent=2)


def main():
    ap = argparse.ArgumentParser(description="Belgian Macro DB Pipeline CLI")
    ap.add_argument("--fetch", action="store_true", help="Fetch data from APIs")
    ap.add_argument("--latest", action="store_true", help="Show latest data")
    ap.add_argument("--dump", action="store_true", help="Print all data")
    ap.add_argument("--export", action="append", choices=["csv", "json"], help="Export files")
    ap.add_argument("--history", action="store_true", help="Show fetch logs")
    ap.add_argument("--db", default=str(DB_PATH), help="DB path")
    args = ap.parse_args()

    db = MacroDatabase(Path(args.db))
    if not any([args.fetch, args.latest, args.dump, args.export, args.history]):
        args.fetch = args.latest = True

    fetch_ok = True
    try:
        if args.fetch:
            log.info(f"DB: {db.db_path}")
            fetch_ok = fetch_all(db)
            if not fetch_ok:
                log.error("One or more sources failed to fetch — see fetch_log / --history")
        if args.latest:
            show_latest(db)
        if args.dump:
            df = db.get_all_observations()
            for code in df["indicator_code"].unique():
                s = df[df["indicator_code"] == code]
                print(f"\n{s.iloc[0]['name']} ({code})")
                for _, row in s.iterrows():
                    print(f"  {row['period']:<10} {row['value']:>8.1f}")
        if args.export:
            for f in args.export:
                export_data(db, f)
        if args.history:
            for e in db.get_fetch_history():
                print(f"{e['code']:<22} | {e['at'][:19]} | {e['rows']:>4} rows | {e['status']}")
    finally:
        db.close()

    if not fetch_ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
