"""
Belgian Macroeconomic Database
==============================
Fetches GDP data from the NBB SDMX dissemination API and DBnomics,
stores in SQLite, and exports to CSV/JSON.

Runs daily via GitHub Actions — data committed back to the repo.
"""

import sqlite3
import csv
import io
import json
import logging
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# ─── Configuration ────────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "data" / "belgian_macro.db"

NBB_BASE = "https://nsidisseminate-stat.nbb.be/rest/data/BE2,DF_QNA_DISS,1.0"
NBB_CSV_HEADER = {"Accept": "application/vnd.sdmx.data+csv;version=2.0.0"}

SOURCES = {
    "GDP_QUARTERLY_YY": {
        "name": "Quarterly GDP Growth (Y-Y)",
        "url": f"{NBB_BASE}/Q.1.B1GM.VZ.LY.N?startPeriod=2000-Q1&dimensionAtObservation=AllDimensions",
        "frequency": "Q",
        "unit": "percent_yy",
        "source_agency": "NBB",
        "description": "Year-on-year volume change of GDP, quarterly, first estimate",
        "type": "nbb"
    },
    "GDP_ANNUAL_CY": {
        "name": "Annual GDP Growth (contribution)",
        "url": f"{NBB_BASE}/A.2.B1GM.VZ.CY.N?startPeriod=2006&dimensionAtObservation=AllDimensions",
        "frequency": "A",
        "unit": "pp_contribution",
        "source_agency": "NBB",
        "description": "GDP total contribution to volume change, Y-Y, non-adjusted",
        "type": "nbb"
    },
    "PRIV_CONSUMPTION_CY": {
        "name": "Private Final Consumption (contribution)",
        "url": f"{NBB_BASE}/A.2.P31_S14_S15.VZ.CY.N?startPeriod=2006&dimensionAtObservation=AllDimensions",
        "frequency": "A",
        "unit": "pp_contribution",
        "source_agency": "NBB",
        "description": "Private final consumption, contribution to GDP volume change",
        "type": "nbb"
    },
    "GOV_CONSUMPTION_CY": {
        "name": "Gov. Consumption Expenditure (contribution)",
        "url": f"{NBB_BASE}/A.2.P3_S13.VZ.CY.N?startPeriod=2006&dimensionAtObservation=AllDimensions",
        "frequency": "A",
        "unit": "pp_contribution",
        "source_agency": "NBB",
        "description": "Final consumption expenditure of general government, contribution to GDP volume change",
        "type": "nbb"
    },
    "GFCF_ENTERPRISES_CY": {
        "name": "GFCF Enterprises (contribution)",
        "url": f"{NBB_BASE}/A.2.P51_ENT.VZ.CY.N?startPeriod=2006&dimensionAtObservation=AllDimensions",
        "frequency": "A",
        "unit": "pp_contribution",
        "source_agency": "NBB",
        "description": "Gross fixed capital formation by enterprises, contribution to GDP volume change",
        "type": "nbb"
    },
    "GFCF_DWELLINGS_CY": {
        "name": "GFCF Dwellings (contribution)",
        "url": f"{NBB_BASE}/A.2.P51_DWE.VZ.CY.N?startPeriod=2006&dimensionAtObservation=AllDimensions",
        "frequency": "A",
        "unit": "pp_contribution",
        "source_agency": "NBB",
        "description": "Gross fixed capital formation in dwellings, contribution to GDP volume change",
        "type": "nbb"
    },
    "GFCF_PUBLIC_CY": {
        "name": "GFCF Public Admin (contribution)",
        "url": f"{NBB_BASE}/A.2.P51_PAD.VZ.CY.N?startPeriod=2006&dimensionAtObservation=AllDimensions",
        "frequency": "A",
        "unit": "pp_contribution",
        "source_agency": "NBB",
        "description": "Gross fixed capital formation by public administrations, contribution to GDP volume change",
        "type": "nbb"
    },
    "CHG_STOCKS_CY": {
        "name": "Changes in Stocks (contribution)",
        "url": f"{NBB_BASE}/A.2.P52.VZ.CY.N?startPeriod=2006&dimensionAtObservation=AllDimensions",
        "frequency": "A",
        "unit": "pp_contribution",
        "source_agency": "NBB",
        "description": "Changes in inventories, contribution to GDP volume change",
        "type": "nbb"
    },
    "NET_EXPORTS_CY": {
        "name": "Net Exports (contribution)",
        "url": f"{NBB_BASE}/A.2.B11.VZ.CY.N?startPeriod=2006&dimensionAtObservation=AllDimensions",
        "frequency": "A",
        "unit": "pp_contribution",
        "source_agency": "NBB",
        "description": "External balance of goods and services, contribution to GDP volume change",
        "type": "nbb"
    },
    "EUROSTAT_GDP_Q_MEUR": {
        "name": "Eurostat GDP (Chain linked volumes, MEUR)",
        "url": "https://api.db.nomics.world/v22/series/Eurostat/namq_10_gdp/Q.CLV10_MEUR.SCA.B1GQ.BE?start_period=2008-Q1",
        "frequency": "Q",
        "unit": "MEUR",
        "source_agency": "Eurostat/DBnomics",
        "description": "Gross domestic product at market prices, chain linked volumes (2010), seasonally and calendar adjusted",
        "type": "dbnomics"
    },
    "EUROSTAT_GDP_Q_MEUR_ES": {
        "name": "Eurostat GDP Spain (MEUR)",
        "url": "https://api.db.nomics.world/v22/series/Eurostat/namq_10_gdp/Q.CLV10_MEUR.SCA.B1GQ.ES?start_period=2008-Q1",
        "frequency": "Q",
        "unit": "MEUR",
        "source_agency": "Eurostat/DBnomics",
        "description": "Spain GDP, chain linked volumes, seasonally and calendar adjusted",
        "type": "dbnomics"
    },
    "EUROSTAT_GDP_Q_MEUR_DE": {
        "name": "Eurostat GDP Germany (MEUR)",
        "url": "https://api.db.nomics.world/v22/series/Eurostat/namq_10_gdp/Q.CLV10_MEUR.SCA.B1GQ.DE?start_period=2008-Q1",
        "frequency": "Q",
        "unit": "MEUR",
        "source_agency": "Eurostat/DBnomics",
        "description": "Germany GDP, chain linked volumes, seasonally and calendar adjusted",
        "type": "dbnomics"
    },
    "EUROSTAT_GDP_Q_MEUR_FR": {
        "name": "Eurostat GDP France (MEUR)",
        "url": "https://api.db.nomics.world/v22/series/Eurostat/namq_10_gdp/Q.CLV10_MEUR.SCA.B1GQ.FR?start_period=2008-Q1",
        "frequency": "Q",
        "unit": "MEUR",
        "source_agency": "Eurostat/DBnomics",
        "description": "France GDP, chain linked volumes, seasonally and calendar adjusted",
        "type": "dbnomics"
    },
    "EUROSTAT_GDP_Q_MEUR_NL": {
        "name": "Eurostat GDP Netherlands (MEUR)",
        "url": "https://api.db.nomics.world/v22/series/Eurostat/namq_10_gdp/Q.CLV10_MEUR.SCA.B1GQ.NL?start_period=2008-Q1",
        "frequency": "Q",
        "unit": "MEUR",
        "source_agency": "Eurostat/DBnomics",
        "description": "Netherlands GDP, chain linked volumes, seasonally and calendar adjusted",
        "type": "dbnomics"
    },
    "EUROSTAT_GDP_Q_MEUR_EA": {
        "name": "Eurostat GDP Euro Area 20 (MEUR)",
        "url": "https://api.db.nomics.world/v22/series/Eurostat/namq_10_gdp/Q.CLV10_MEUR.SCA.B1GQ.EA20?start_period=2008-Q1",
        "frequency": "Q",
        "unit": "MEUR",
        "source_agency": "Eurostat/DBnomics",
        "description": "Euro Area 20 GDP, chain linked volumes, seasonally and calendar adjusted",
        "type": "dbnomics"
    }
}

REQUEST_TIMEOUT = 30

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
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS indicators (
                code          TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                frequency     TEXT NOT NULL,
                unit          TEXT NOT NULL,
                source_agency TEXT NOT NULL,
                description   TEXT,
                api_url       TEXT
            );
            CREATE TABLE IF NOT EXISTS observations (
                indicator_code TEXT NOT NULL,
                period         TEXT NOT NULL,
                value          REAL NOT NULL,
                obs_status     TEXT,
                fetched_at     TEXT NOT NULL,
                PRIMARY KEY (indicator_code, period),
                FOREIGN KEY (indicator_code) REFERENCES indicators(code)
            );
            CREATE TABLE IF NOT EXISTS fetch_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                indicator_code TEXT NOT NULL,
                fetched_at     TEXT NOT NULL,
                rows_upserted  INTEGER NOT NULL,
                status         TEXT NOT NULL,
                message        TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_obs_period
                ON observations(indicator_code, period DESC);
        """)
        self.conn.commit()

    def upsert_indicator(self, code: str, meta: dict):
        self.conn.execute("""
            INSERT INTO indicators (code, name, frequency, unit, source_agency, description, api_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                name=excluded.name, frequency=excluded.frequency,
                unit=excluded.unit, source_agency=excluded.source_agency,
                description=excluded.description, api_url=excluded.api_url
        """, (code, meta["name"], meta["frequency"], meta["unit"],
              meta["source_agency"], meta.get("description", ""), meta.get("url", "")))
        self.conn.commit()

    def upsert_observations(self, indicator_code: str, rows: list[dict]) -> int:
        now = datetime.now(timezone.utc).isoformat()
        for row in rows:
            self.conn.execute("""
                INSERT INTO observations (indicator_code, period, value, obs_status, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(indicator_code, period) DO UPDATE SET
                    value=excluded.value, obs_status=excluded.obs_status,
                    fetched_at=excluded.fetched_at
            """, (indicator_code, row["period"], row["value"], row.get("obs_status", ""), now))
        self.conn.commit()
        return len(rows)

    def log_fetch(self, code: str, count: int, status: str, msg: str = ""):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "INSERT INTO fetch_log (indicator_code, fetched_at, rows_upserted, status, message) VALUES (?,?,?,?,?)",
            (code, now, count, status, msg))
        self.conn.commit()

    def get_latest(self, code: str) -> Optional[dict]:
        cur = self.conn.execute("""
            SELECT o.period, o.value, o.obs_status, o.fetched_at, i.name, i.unit
            FROM observations o JOIN indicators i ON o.indicator_code = i.code
            WHERE o.indicator_code = ? ORDER BY o.period DESC LIMIT 1
        """, (code,))
        r = cur.fetchone()
        if not r: return None
        return {"indicator_code": code, "period": r[0], "value": r[1],
                "obs_status": r[2], "fetched_at": r[3], "name": r[4], "unit": r[5]}

    def get_all_latest(self) -> list[dict]:
        codes = [r[0] for r in self.conn.execute("SELECT code FROM indicators ORDER BY code")]
        return [l for c in codes if (l := self.get_latest(c))]

    def get_all_observations(self) -> pd.DataFrame:
        return pd.read_sql_query("""
            SELECT o.indicator_code, i.name, o.period, o.value,
                   o.obs_status, i.unit, i.source_agency, o.fetched_at
            FROM observations o JOIN indicators i ON o.indicator_code = i.code
            ORDER BY o.indicator_code, o.period
        """, self.conn)

    def get_fetch_history(self, n: int = 20) -> list[dict]:
        cur = self.conn.execute(
            "SELECT indicator_code, fetched_at, rows_upserted, status, message FROM fetch_log ORDER BY id DESC LIMIT ?", (n,))
        return [{"code": r[0], "at": r[1], "rows": r[2], "status": r[3], "msg": r[4]} for r in cur]

    def close(self):
        self.conn.close()


# ─── Fetchers ─────────────────────────────────────────────────────

class NBBFetcher:
    @staticmethod
    def fetch(url: str) -> list[dict]:
        log.info(f"GET {url[:90]}...")
        resp = requests.get(url, headers=NBB_CSV_HEADER, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        seen: dict[str, dict] = {}
        for row in csv.DictReader(io.StringIO(resp.text)):
            period = row.get("TIME_PERIOD", "").strip()
            raw = row.get("OBS_VALUE", "").strip()
            status = row.get("OBS_STATUS", "").strip()
            if not period or not raw: continue
            try: val = float(raw)
            except ValueError: continue
            seen[period] = {"period": period, "value": val, "obs_status": status}
        data = sorted(seen.values(), key=lambda x: x["period"])
        if data:
            log.info(f"  {len(data)} obs: {data[0]['period']} → {data[-1]['period']}")
        return data

class DBnomicsFetcher:
    @staticmethod
    def fetch(url: str) -> list[dict]:
        log.info(f"GET {url[:90]}...")
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        
        try:
            data = resp.json()
            series = data["series"]["docs"][0]
            periods = series["period"]
            values = series["value"]
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"Unexpected DBnomics JSON structure or decoding failed: {e}")

        results = []
        for p, v in zip(periods, values):
            # Client-side fallback filter
            if str(p) < "2008-Q1":
                continue
            if v is None or v == "NA":
                continue
            try:
                val = float(v)
                # DBnomics doesn't universally provide granular observation status flags in the main array, defaulting to Actual
                results.append({"period": str(p), "value": val, "obs_status": "A"})
            except ValueError:
                continue
                
        if results:
            log.info(f"  {len(results)} obs: {results[0]['period']} → {results[-1]['period']}")
        return results


# ─── Orchestration ────────────────────────────────────────────────

def fetch_all(db: MacroDatabase) -> dict[str, int]:
    results = {}
    for code, meta in SOURCES.items():
        db.upsert_indicator(code, meta)
        try:
            src_type = meta.get("type", "nbb")
            if src_type == "nbb":
                rows = NBBFetcher.fetch(meta["url"])
            elif src_type == "dbnomics":
                rows = DBnomicsFetcher.fetch(meta["url"])
            else:
                raise ValueError(f"Unknown source type specified: {src_type}")
                
            n = db.upsert_observations(code, rows)
            db.log_fetch(code, n, "OK")
            results[code] = n
            log.info(f"  OK {code}: {n} rows")
        except Exception as e:
            log.error(f"  FAIL {code}: {e}")
            db.log_fetch(code, 0, "ERROR", str(e))
            results[code] = 0
    return results


def show_latest(db: MacroDatabase):
    latest = db.get_all_latest()
    if not latest:
        log.warning("Empty DB. Run --fetch first.")
        return
    print("\n" + "=" * 70)
    print("  BELGIAN MACRO DATABASE — Latest")
    print("=" * 70 + "\n")
    for e in latest:
        s = {"A": "Actual", "P": "Provisional"}.get(e["obs_status"], e["obs_status"])
        print(f"  {e['name']}")
        print(f"    {e['period']}  →  {e['value']}  ({s})")
        print(f"    fetched {e['fetched_at'][:16]}\n")


def export_data(db: MacroDatabase, fmt: str) -> Optional[Path]:
    df = db.get_all_observations()
    if df.empty:
        log.warning("Nothing to export.")
        return None
    out = Path(__file__).parent / "data"
    out.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        p = out / "belgian_macro_export.csv"
        df.to_csv(p, index=False)
    elif fmt == "json":
        p = out / "belgian_macro_export.json"
        df.to_dict(orient="records")
        with open(p, "w") as f:
            json.dump(df.to_dict(orient="records"), f, indent=2, default=str)
    else:
        return None
    log.info(f"Exported {len(df)} rows → {p.name}")
    return p


# ─── CLI ──────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Belgian Macro DB — APIs → SQLite → CSV")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--latest", action="store_true")
    ap.add_argument("--dump", action="store_true")
    ap.add_argument("--export", action="append", choices=["csv", "json"])
    ap.add_argument("--history", action="store_true")
    ap.add_argument("--db", default=str(DB_PATH))
    args = ap.parse_args()

    db = MacroDatabase(Path(args.db))

    if not any([args.fetch, args.latest, args.dump, args.export, args.history]):
        args.fetch = args.latest = True

    try:
        if args.fetch:
            log.info(f"DB: {db.db_path}")
            r = fetch_all(db)
            log.info(f"Total: {sum(r.values())} rows upserted")

        if args.latest:
            show_latest(db)

        if args.dump:
            df = db.get_all_observations()
            for code in df["indicator_code"].unique():
                s = df[df["indicator_code"] == code]
                print(f"\n  {s.iloc[0]['name']} ({code})")
                for _, row in s.iterrows():
                    st = {"A": "Act", "P": "Prov", "B": "Break"}.get(row["obs_status"], row["obs_status"])
                    print(f"    {row['period']:<10} {row['value']:>12.1f}  {st}")

        if args.export:
            for fmt in args.export:
                export_data(db, fmt)

        if args.history:
            for e in db.get_fetch_history():
                print(f"  {e['code']:<22} {e['at'][:19]}  {e['rows']:>4} rows  {e['status']}")
    finally:
        db.close()


if __name__ == "__main__":
    main()