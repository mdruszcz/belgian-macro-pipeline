# 🇧🇪 Belgian Macroeconomic Database

Automated pipeline that fetches Belgian economic data from the **National Bank of Belgium (NBB)** SDMX API, **Eurostat (via DBnomics)**, and **Federal Planning Bureau (FPB)**, stores it in SQLite, exports CSV/JSON, and powers a live dashboard — running daily on GitHub Actions for free.

```
NBB + Eurostat + FPB  →  SQLite DB  →  CSV + JSON  →  Live Dashboard
                  (all committed to this repo daily)
```

## 📊 Live Dashboard

**[→ Open Dashboard](https://mdruszcz.github.io/belgian-macro-pipeline/dashboard.html)**

The dashboard fetches data directly from this repo's CSV files and displays all indicators with sparklines, trend arrows, and auto-generated commentary.

## Indicators

| # | Code | Name | Source |
|---|------|------|--------|
| 1 | `GDP_QUARTERLY_YY` | Quarterly GDP Growth (Y-Y) | NBB |
| 2 | `GDP_ANNUAL_CY` | Annual GDP Growth (contribution) | NBB |
| 3 | `PRIV_CONSUMPTION_CY` | Private Final Consumption (contribution) | NBB |
| 4 | `GOV_CONSUMPTION_CY` | Gov. Consumption Expenditure (contribution) | NBB |
| 5 | `GFCF_ENTERPRISES_CY` | GFCF Enterprises (contribution) | NBB |
| 6 | `GFCF_DWELLINGS_CY` | GFCF Dwellings (contribution) | NBB |
| 7 | `GFCF_PUBLIC_CY` | GFCF Public Admin (contribution) | NBB |
| 8 | `CHG_STOCKS_CY` | Changes in Stocks (contribution) | NBB |
| 9 | `NET_EXPORTS_CY` | Net Exports (contribution) | NBB |
| 10 | `EUROSTAT_GDP_Q_MEUR` | Eurostat GDP (Index 2010=100) | Eurostat/DBnomics |
| 11 | `EUROSTAT_GDP_Q_MEUR_ES` | Eurostat GDP Spain (Index 2010=100) | Eurostat/DBnomics |
| 12 | `EUROSTAT_GDP_Q_MEUR_DE` | Eurostat GDP Germany (Index 2010=100) | Eurostat/DBnomics |
| 13 | `EUROSTAT_GDP_Q_MEUR_FR` | Eurostat GDP France (Index 2010=100) | Eurostat/DBnomics |
| 14 | `EUROSTAT_GDP_Q_MEUR_NL` | Eurostat GDP Netherlands (Index 2010=100) | Eurostat/DBnomics |
| 15 | `EUROSTAT_GDP_Q_MEUR_EA` | Eurostat GDP Euro Area 20 (Index 2010=100) | Eurostat/DBnomics |
| 16 | `BE_CONSUMER_CONFIDENCE` | Consumer Confidence (BE) | Eurostat/DBnomics |
| 17 | `EU_CONSUMER_CONFIDENCE` | Consumer Confidence (EU27) | Eurostat/DBnomics |

*Note: FPB Forecasts are also fetched and stored.*

Data sources:
- [NBB SDMX Dissemination API](https://nsidisseminate-stat.nbb.be/) — dataflow `DF_QNA_DISS` (Quarterly National Accounts).
- [DBnomics](https://db.nomics.world/) for Eurostat indicators.
- [Federal Planning Bureau (FPB)](https://www.plan.be/) for economic forecasts.

Updated daily at 06:00 CET via GitHub Actions.

## Data Files

| File | Description |
|------|-------------|
| [`data/belgian_macro_export.csv`](data/belgian_macro_export.csv) | Full time series — viewable directly in GitHub |
| [`data/belgian_macro_export.json`](data/belgian_macro_export.json) | Full time series in JSON format |
| [`data/belgian_forecasts.csv`](data/belgian_forecasts.csv) | Multi-institution economic forecasts |
| [`data/belgian_macro.db`](data/belgian_macro.db) | SQLite database with observations + fetch log |
| [`dashboard.html`](dashboard.html) | Self-contained dashboard (also hosted via GitHub Pages) |

## How It Works

1. **GitHub Actions** triggers daily at 06:00 CET (`.github/workflows/daily_fetch.yml`)
2. **Python script** fetches SDMX CSV data from NBB, JSON from DBnomics, and XLSX from FPB
3. **SQLite** upserts observations (idempotent — safe to re-run)
4. **CSV + JSON** exported to `data/`
5. **Git commit** pushes updated files back to this repo
6. **Dashboard** reads the data via raw GitHub URL — always up to date

## Adding More Indicators

Edit the `SOURCES` dict in `belgian_macro_db.py` to add NBB or DBnomics sources. For NBB:

```python
"YOUR_INDICATOR": {
    "name": "Display Name",
    "url": f"{NBB_BASE}/A.2.INDICATOR_CODE.VZ.LY.N?startPeriod=2000&dimensionAtObservation=AllDimensions",
    "frequency": "A",          # A=Annual, Q=Quarterly, M=Monthly
    "unit": "percent_yy",
    "source_agency": "NBB",
    "description": "What this measures",
    "type": "nbb"
},
```

For DBnomics sources, change `type` to `"dbnomics"` and provide the API URL.

Then add a matching entry in `dashboard.html`'s `INDICATOR_META` object (if you want to display it on the dashboard) to control display order and section grouping.

## Local Usage

```bash
pip install -r requirements.txt

python belgian_macro_db.py                # fetch + show latest
python belgian_macro_db.py --dump         # full database dump
python belgian_macro_db.py --export csv   # export CSV
python belgian_macro_db.py --export json  # export JSON
python belgian_macro_db.py --history      # fetch log
```

Exit code is non-zero if any source failed to fetch — check `--history` for details.

## Development

```bash
pip install -e ".[dev]"   # installs ruff, black, pytest on top of the runtime deps
pre-commit install        # run ruff/black/large-file checks on every commit

pytest                    # run the test suite
ruff check .
black --check .
```

## File Structure

```
├── .github/workflows/
│   └── daily_fetch.yml          ← GitHub Actions daily schedule
├── data/
│   ├── belgian_macro.db         ← SQLite database (auto-updated)
│   ├── belgian_macro_export.csv ← CSV export (auto-updated)
│   ├── belgian_macro_export.json← JSON export (auto-updated)
│   └── belgian_forecasts.csv    ← Forecasts CSV export (auto-updated)
├── belgian_macro_db.py          ← Python ETL script
├── dashboard.html               ← Live dashboard (GitHub Pages)
├── requirements.txt
└── README.md
```
