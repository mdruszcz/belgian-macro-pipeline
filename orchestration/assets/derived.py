"""derived: aggregates, peer positions and the communes table -- computed by
the existing exporters, under the aggregation rules they already implement."""

from orchestration.assets import function_asset, script_asset

VALIDATED = "validated_working_database"

aggregates_csv = function_asset(
    "aggregates_csv",
    group="derived",
    deps=[VALIDATED],
    kinds={"csv"},
    description=(
        "Province/region/Belgium aggregates: export_aggregates_csv() from "
        "scripts/export_aggregates_csv.py, called in process."
    ),
)
percentiles_csv = script_asset(
    "percentiles_csv",
    group="derived",
    deps=[VALIDATED],
    kinds={"csv"},
    description="National and regional peer positions (scripts/export_percentiles_csv.py).",
)
communes_table_json = script_asset(
    "communes_table_json",
    group="derived",
    deps=[VALIDATED, "communes_history_full_csv"],
    kinds={"json"},
    description="communes.html's table payload (scripts/export_communes_table_json.py).",
)

ASSETS = [aggregates_csv, percentiles_csv, communes_table_json]
