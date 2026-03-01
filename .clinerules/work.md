You are operating within the Belgian Macroeconomic Database pipeline. Adhere to these architectural constraints at all times:

1. **Frontend Purity:** No heavy JavaScript frameworks (React, Vue, Angular). No build steps (Webpack, Vite). Modify `dashboard.html` using strictly vanilla JavaScript, CSS, and HTML. 
2. **Backend Simplicity:** Do not introduce ORMs (like SQLAlchemy). Execute database operations using raw, optimized SQL queries via Python's built-in `sqlite3` module.
3. **Data Integrity:** Assume the GitHub Action will trigger multiple times or fail halfway. All data ingestion logic must be strictly idempotent (e.g., use `INSERT OR IGNORE` or `ON CONFLICT DO UPDATE`).
4. **Dependency Minimalism:** Do not add libraries to `requirements.txt` unless absolutely mandatory for data extraction (like `requests` or `pandas`). Refuse any request to add bloated dependencies.