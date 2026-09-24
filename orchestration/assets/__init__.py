"""The asset graph, one module per group.

sources_api / sources_manual / reference_data
    -> canonical_observations -> validated_working_database (+ checks)
    -> derived -> website
"""

from collections.abc import Callable

from dagster import AssetExecutionContext, MaterializeResult, asset

from orchestration import run
from orchestration.commands import COMMANDS
from orchestration.paths import PipelinePaths
from src.stores import resolve_extra_observations


def script_asset(name: str, *, group: str, deps: list, description: str, **kwargs):
    """An asset that runs COMMANDS[name] and reports its outputs. Every
    wrapper in this package is this, or this plus metadata."""

    @asset(name=name, group_name=group, deps=deps, description=description, **kwargs)
    def _asset(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
        run.run_script(context, paths, name)
        return MaterializeResult(metadata=run.output_metadata(paths, name))

    return _asset


# Every flag an exporter's command line may carry, and what it becomes in the
# call to its function. A flag in none of these has no translation.
EXPORTER_PATHS = {
    "--db": "db_path",
    "--out": "out_path",
    "--communes-history": "csv_path",
}
EXPORTER_STORES = "--stores"
EXPORTER_SWITCHES = {"--all-periods": "all_periods"}


def exporter_arguments(name: str, paths: PipelinePaths) -> dict:
    """What an exporter's main() hands its function, read off the command line
    in orchestration/commands.py, so the call cannot drift from what the
    Makefile runs:

        --db X                db_path=X
        --out X               out_path=X
        --communes-history X  csv_path=X
        --stores X            extra_observations=resolve_extra_observations([], X)
        --all-periods         all_periods=True

    every path made absolute. Every other parameter keeps its default, which
    is the script's argparse default. A command line with any other flag, a
    flag without its value, a switch followed by a value, a flag given twice,
    or no --db and --out is refused rather than half-translated: it needs a
    translation of its own."""
    script, *tokens = paths.render(COMMANDS[name].argv)
    refused = ValueError(f"{name}: `{script} {' '.join(tokens)}` has no exporter translation")
    values: dict[str, str] = {}
    switches: set[str] = set()
    i = 0
    while i < len(tokens):
        flag = tokens[i]
        takes_value = flag in EXPORTER_PATHS or flag == EXPORTER_STORES
        if flag in EXPORTER_SWITCHES and flag not in switches:
            switches.add(flag)
            i += 1
        elif (
            takes_value
            and flag not in values
            and i + 1 < len(tokens)
            and not tokens[i + 1].startswith("--")
        ):
            values[flag] = tokens[i + 1]
            i += 2
        else:
            raise refused
    if not {"--db", "--out"} <= set(values):
        raise refused

    arguments: dict = {
        EXPORTER_PATHS[flag]: paths.resolve(value)
        for flag, value in values.items()
        if flag in EXPORTER_PATHS
    }
    if EXPORTER_STORES in values:
        given = values[EXPORTER_STORES]
        registry = str(paths.resolve(given)) if given else ""
        arguments["extra_observations"] = resolve_extra_observations([], registry)
        if name in ("communes_history_csv", "communes_history_full_csv"):
            # export_communes_history_csv's history_shard split reads the same
            # registry --stores already names -- '' disables it exactly like it
            # disables extra_observations above, so one flag controls both.
            arguments["stores_path"] = registry
    for switch in sorted(switches):
        arguments[EXPORTER_SWITCHES[switch]] = True
    return arguments


def function_asset(
    name: str,
    *,
    group: str,
    deps: list,
    description: str,
    arguments: Callable[[str, PipelinePaths], dict] = exporter_arguments,
    **kwargs,
):
    """The in-process twin of script_asset: calls COMMANDS[name].function with
    `arguments(name, paths)` and reports the rows it returned and its outputs."""

    @asset(name=name, group_name=group, deps=deps, description=description, **kwargs)
    def _asset(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
        rows = run.call_function(context, name, **arguments(name, paths))
        metadata = {"rows written": rows, **run.output_metadata(paths, name)}
        return MaterializeResult(metadata=metadata)

    return _asset
