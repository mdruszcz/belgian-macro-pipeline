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


EXPORTER_FLAGS = ("--db", "--out", "--stores")


def exporter_arguments(name: str, paths: PipelinePaths) -> dict:
    """What an exporter's main() hands its function, read off the command line
    in orchestration/commands.py, so the call cannot drift from what the
    Makefile runs:

        --db X      db_path=X
        --out X     out_path=X
        --stores X  extra_observations=resolve_extra_observations([], X)

    every path made absolute. Every other parameter keeps its default, which
    is the script's argparse default. A command line with any other flag is
    refused rather than half-translated: it needs a translation of its own."""
    script, *tokens = paths.render(COMMANDS[name].argv)
    flags = dict(zip(tokens[::2], tokens[1::2], strict=False))
    if (
        len(tokens) != 2 * len(flags)
        or set(flags) - set(EXPORTER_FLAGS)
        or any(value.startswith("--") for value in flags.values())
        or not {"--db", "--out"} <= set(flags)
    ):
        raise ValueError(f"{name}: `{script} {' '.join(tokens)}` is not --db/--out[/--stores]")
    arguments = {
        "db_path": paths.resolve(flags["--db"]),
        "out_path": paths.resolve(flags["--out"]),
    }
    if "--stores" in flags:
        registry = str(paths.resolve(flags["--stores"])) if flags["--stores"] else ""
        arguments["extra_observations"] = resolve_extra_observations([], registry)
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
