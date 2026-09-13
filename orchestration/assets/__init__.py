"""The asset graph, one module per group.

sources_api / sources_manual / reference_data
    -> canonical_observations -> validated_working_database (+ checks)
    -> derived -> website
"""

from dagster import AssetExecutionContext, MaterializeResult, asset

from orchestration import run
from orchestration.paths import PipelinePaths


def script_asset(name: str, *, group: str, deps: list, description: str, **kwargs):
    """An asset that runs COMMANDS[name] and reports its outputs. Every
    wrapper in this package is this, or this plus metadata."""

    @asset(name=name, group_name=group, deps=deps, description=description, **kwargs)
    def _asset(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
        run.run_script(context, paths, name)
        return MaterializeResult(metadata=run.output_metadata(paths, name))

    return _asset
