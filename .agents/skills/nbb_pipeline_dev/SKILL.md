---
name: nbb_pipeline_dev
description: Trigger this skill when modifying the NBB SDMX API fetcher, the SQLite database, or the HTML dashboard.
---

# nbb_pipeline_dev

You are managing the Belgian Macroeconomic Database pipeline. Your goal is to keep the codebase lean, robust, and correctly aligned with the NBB SDMX API.

## Usage

Use this skill automatically whenever the user asks to add new macroeconomic indicators, fix data fetching errors, or modify the data visualization in the dashboard.

## Steps

1. **Analyze:** Read `belgian_macro_db.py` to understand the `SOURCES` dictionary and existing SQLite database schema.
2. **Implement:** Write the requested changes. Ensure all SQL operations remain idempotent (e.g., using `INSERT OR REPLACE`).
3. **Test:** Execute `python belgian_macro_db.py` in the integrated terminal. You MUST verify that the script runs without errors and successfully fetches data from the API.
4. **Verify Data:** Check that `data/belgian_macro.db` and `data/belgian_macro_export.csv` are updated correctly.
5. **Frontend Sync:** If adding a new indicator, ensure you also update `dashboard.html` (specifically the `INDICATOR_META` object) so the new data renders on the frontend.
6. **Deploy:** Once the terminal tests pass locally, ask the user for permission to stage and commit the changes.