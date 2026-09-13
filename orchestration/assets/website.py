"""website: the published CSVs, JSON payloads and generated pages, with the
dependencies `make exports` runs them in."""

from orchestration.assets import script_asset

VALIDATED = "validated_working_database"

national_csv = script_asset(
    "national_csv",
    group="website",
    deps=[VALIDATED],
    kinds={"csv"},
    description="data/belgian_macro_export.csv, canonical schema (scripts/export_canonical_csv.py).",
)
communes_csv = script_asset(
    "communes_csv",
    group="website",
    deps=[VALIDATED],
    kinds={"csv"},
    description="data/communes_export.csv (scripts/export_communes_csv.py).",
)
communes_history_full_csv = script_asset(
    "communes_history_full_csv",
    group="website",
    deps=[VALIDATED],
    kinds={"csv"},
    description="Full commune history, gitignored intermediate (export_communes_history_csv.py --all-periods).",
)
communes_history_csv = script_asset(
    "communes_history_csv",
    group="website",
    deps=[VALIDATED],
    kinds={"csv"},
    description="Committed commune history, last 10 years (scripts/export_communes_history_csv.py).",
)
indicator_metadata_json = script_asset(
    "indicator_metadata_json",
    group="website",
    deps=[VALIDATED],
    kinds={"json"},
    description="data/metadata/indicators.json (python -m src.exporters.metadata).",
)
site_payloads = script_asset(
    "site_payloads",
    group="website",
    deps=[
        "communes_history_full_csv",
        "communes_csv",
        "national_csv",
        "aggregates_csv",
        "percentiles_csv",
        "indicator_metadata_json",
    ],
    kinds={"json"},
    description="public/data/** and manifest.json (scripts/export_site_payloads.py).",
)
explorer_payloads = script_asset(
    "explorer_payloads",
    group="website",
    deps=["communes_history_csv", "national_csv", "site_payloads"],
    kinds={"json"},
    description="explorer.html's per-indicator shards (scripts/export_explorer_payloads.py).",
)
local_pages = script_asset(
    "local_pages",
    group="website",
    deps=["site_payloads"],
    kinds={"html"},
    description="The permanent /local/{nis} pages (scripts/export_local_pages.py).",
)
page_documents = script_asset(
    "page_documents",
    group="website",
    deps=["local_pages", "explorer_payloads", "commune_adjacency", "commune_typology"],
    kinds={"html"},
    description="Published builder pages, en/fr/nl (scripts/export_page_documents.py).",
)
site_index = script_asset(
    "site_index",
    group="website",
    deps=["page_documents", "local_pages"],
    kinds={"xml"},
    description="Sitemap index (scripts/export_site_index.py).",
)

ASSETS = [
    national_csv,
    communes_csv,
    communes_history_full_csv,
    communes_history_csv,
    indicator_metadata_json,
    site_payloads,
    explorer_payloads,
    local_pages,
    page_documents,
    site_index,
]
