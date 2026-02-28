# 🇧🇪 Belgian Macroeconomic Database

Automated pipeline that fetches Belgian GDP data from the **National Bank of Belgium (NBB)** SDMX API, stores it in SQLite, and exports CSV — running daily on GitHub Actions for free.

```
NBB SDMX API  →  SQLite DB  →  CSV + JSON
                  (all committed to this repo daily)
```

## Live Data

| File | What's in it |
|------|-------------|
| [`data/belgian_macro_export.csv`](data/belgian_macro_export.csv) | Full time series, viewable in GitHub |
| [`data/belgian_macro_export.json`](data/belgian_macro_export.json) | Same data as JSON |
| [`data/belgian_macro.db`](data/belgian_macro.db) | SQLite database |

## Indicators

| Code | Name | Frequency | Obs | Source |
|------|------|-----------|-----|--------|
| `GDP_QUARTERLY_YY` | Quarterly GDP Growth (Y-Y) | Quarterly | 104 | NBB |
| `GDP_ANNUAL_YY` | Annual GDP Growth | Annual | 26 | NBB |

Updated daily at 06:00 CET via GitHub Actions.

## Adding more indicators

Edit `SOURCES` in `belgian_macro_db.py`:

```python
"YOUR_INDICATOR": {
    "name": "Display Name",
    "url": "https://nsidisseminate-stat.nbb.be/rest/data/BE2,DF_QNA_DISS,1.0/...",
    "frequency": "Q",
    "unit": "percent_yy",
    "source_agency": "NBB",
    "description": "What this measures",
},
```
