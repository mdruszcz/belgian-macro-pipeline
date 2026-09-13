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

ASSETS = [staging_db, commune_adjacency, commune_typology]
