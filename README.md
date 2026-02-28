# 🇧🇪 Belgian Macroeconomic Database

Automated pipeline that fetches Belgian economic data from the **National Bank of Belgium (NBB)** SDMX API, stores it in SQLite, exports CSV, and powers a live dashboard — running daily on GitHub Actions for free.

```
NBB SDMX API  →  SQLite DB  →  CSV + JSON  →  Live Dashboard
                  (all committed to this repo daily)
```

## 📊 Live Dashboard

**[→ Open Dashboard](https://mdruszcz.github.io/belgian-macro-pipeline/dashboard.html)**

The dashboard fetches data directly from this repo's CSV and displays all indicators with sparklines, trend arrows, and auto-generated commentary.

## Indicators

| # | Code | Name | Latest | Source |
|---|------|------|--------|--------|
| 1 | `GDP_QUARTERLY_YY` | Quarterly GDP Growth (Y-Y) | 2025-Q4 | NBB |
| 2 | `GDP_ANNUAL_YY` | Annual GDP Growth | 2025 | NBB |
| 3 | `PRIV_CONSUMPTION_YY` | Private Final Consumption | 2025 | NBB |
| 4 | `GOV_CONSUMPTION_YY` | Gov. Consumption Expenditure | 2025 | NBB |
| 5 | `GFCF_ENTERPRISES_YY` | GFCF — Enterprises | 2025 | NBB |
| 6 | `GFCF_DWELLINGS_YY` | GFCF — Dwellings | 2025 | NBB |
| 7 | `GFCF_PUBLIC_YY` | GFCF — Public Admin | 2025 | NBB |
| 8 | `CHG_STOCKS_YY` | Changes in Stocks | 2025 | NBB |
| 9 | `NET_EXPORTS_YY` | Net Exports | 2025 | NBB |

All data sourced from the [NBB SDMX Dissemination API](https://nsidisseminate-stat.nbb.be/) — dataflow `DF_QNA_DISS` (Quarterly National Accounts).

Updated daily at 06:00 CET via GitHub Actions.

## Data Files

| File | Description |
|------|-------------|
| [`data/belgian_macro_export.csv`](data/belgian_macro_export.csv) | Full time series — viewable directly in GitHub |
| [`data/belgian_macro.db`](data/belgian_macro.db) | SQLite database with observations + fetch log |
| [`dashboard.html`](dashboard.html) | Self-contained dashboard (also hosted via GitHub Pages) |

## How It Works

1. **GitHub Actions** triggers daily at 06:00 CET (`.github/workflows/daily_fetch.yml`)
2. **Python script** fetches SDMX CSV data from NBB for each indicator
3. **SQLite** upserts observations (idempotent — safe to re-run)
4. **CSV + JSON** exported to `data/`
5. **Git commit** pushes updated files back to this repo
6. **Dashboard** reads the CSV via raw GitHub URL — always up to date

## Adding More Indicators

Edit the `SOURCES` dict in `belgian_macro_db.py`:

```python
"YOUR_INDICATOR": {
    "name": "Display Name",
    "url": f"{NBB_BASE}/A.2.INDICATOR_CODE.VZ.LY.N?startPeriod=2000&dimensionAtObservation=AllDimensions",
    "frequency": "A",          # A=Annual, Q=Quarterly, M=Monthly
    "unit": "percent_yy",
    "source_agency": "NBB",
    "description": "What this measures",
},
```

Then add a matching entry in `dashboard.html`'s `INDICATOR_META` object to control display order and section grouping.

Available NBB SDMX indicator codes for `DF_QNA_DISS` (Account=2, expenditure breakdown):

| Code | Component |
|------|-----------|
| `B1GM` | GDP total |
| `P31_S14_S15` | Private final consumption |
| `P3_S13` | Government consumption |
| `P51_ENT` | GFCF enterprises |
| `P51_DWE` | GFCF dwellings |
| `P51_PAD` | GFCF public admin |
| `P52` | Changes in stocks |
| `B11` | Net exports |
| `P6` | Exports |
| `P7` | Imports |

Price types: `LY` = Y-Y volume change (%), `CY` = Contribution to volume change (pp), `V` = Current prices, `L` = Chained volumes.

## Local Usage

```bash
pip install -r requirements.txt

python belgian_macro_db.py                # fetch + show latest
python belgian_macro_db.py --dump         # full database dump
python belgian_macro_db.py --export csv   # export CSV
python belgian_macro_db.py --history      # fetch log
```

## File Structure

```
├── .github/workflows/
│   └── daily_fetch.yml          ← GitHub Actions daily schedule
├── data/
│   ├── belgian_macro.db         ← SQLite database (auto-updated)
│   └── belgian_macro_export.csv ← CSV export (auto-updated)
├── belgian_macro_db.py          ← Python ETL script
├── dashboard.html               ← Live dashboard (GitHub Pages)
├── requirements.txt
└── README.md
```
