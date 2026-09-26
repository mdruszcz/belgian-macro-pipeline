"""reference_data: the assembled working database, and the two geography
payloads derived from committed reference files."""

from dagster import AssetExecutionContext, MaterializeResult, asset

from orchestration import run
from orchestration.assets import script_asset
from orchestration.paths import DEFAULT_STORES, PipelinePaths


@asset(
    group_name="reference_data",
    kinds={"sqlite"},
    description=(
        "data/local/working.db, assembled by scripts/build_staging_db.py from the committed "
        "database plus every in_db store CSV. Every sync, check and export reads it."
    ),
)
def staging_db(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
    # The workflow's own command line; the two overrides exist for tests that
    # assemble from a small registry and database.
    extra = ()
    if paths.source_db:
        extra += ("--source-db", paths.source_db)
    if paths.stores != DEFAULT_STORES:
        extra += ("--stores", paths.stores)
    run.run_script(context, paths, "staging_db", extra)
    return MaterializeResult(metadata=run.output_metadata(paths, "staging_db"))


# After site_payloads, as in `make exports`: both write under public/data/metadata.
commune_adjacency = script_asset(
    "commune_adjacency",
    group="reference_data",
    deps=["site_payloads"],
    kinds={"json"},
    description="Which communes share a border (scripts/export_commune_adjacency.py).",
)
commune_typology = script_asset(
    "commune_typology",
    group="reference_data",
    deps=["site_payloads"],
    kinds={"json"},
    description="Belfius socio-economic clusters (scripts/export_commune_typology.py).",
)
# Depends on site_payloads (for geographies.json's trilingual names) AND
# commune_flows_buyer_origin (the committed flows store PR 1 writes) --
# docs/features/commune_flows.md, PR 2.
commune_flows_export = script_asset(
    "commune_flows_export",
    group="reference_data",
    deps=["site_payloads", "commune_flows_buyer_origin"],
    kinds={"json"},
    description=(
        "One buyer-origin flows payload per commune, reshaped from the committed "
        "flows store (scripts/export_commune_flows.py)."
    ),
)

# Schools ISE (docs/features/schools_ise.md). schools_ise_export fetches both ODWB
# datasets live and resolves each site's commune -- no dependency on site_payloads,
# unlike commune_flows_export, since it needs no geographies.json (the commune
# display names come straight from geographies.csv inside the script itself).
# schools_by_commune_export reads schools_ise_export's own output, so it depends on it.
schools_ise_export = script_asset(
    "schools_ise_export",
    group="reference_data",
    deps=[],
    kinds={"json"},
    description=(
        "The FWB school-site ISE-class snapshot, joined to each site's commune NIS "
        "code (scripts/export_schools_ise.py)."
    ),
)
schools_by_commune_export = script_asset(
    "schools_by_commune_export",
    group="reference_data",
    deps=["schools_ise_export"],
    kinds={"json"},
    description=(
        "Per-commune summary of the ISE-class snapshot -- site counts and mean "
        "class per formula (scripts/export_schools_by_commune.py)."
    ),
)

ASSETS = [
    staging_db,
    commune_adjacency,
    commune_typology,
    commune_flows_export,
    schools_ise_export,
    schools_by_commune_export,
]
