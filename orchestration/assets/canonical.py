"""canonical: the validated working database, the two things that only make
sense once validation has passed, and the committed stores, written last.

validated_working_database does no work. It is the point every source feeds
into and every export hangs off, and the asset the validation checks are
attached to. Its metadata says which source run it was built after -- only
the run it was explicitly given, never whatever manifest happens to be on disk.

committed_stores is the offload: data/belgian_macro.db and every in_db store's
CSV, written by offload() from scripts/offload_stores.py after everything the
export job builds, in that same run, and only when the coordinator allows it.
"""

import json
import sys
from pathlib import Path

from dagster import AssetExecutionContext, Config, Failure, MaterializeResult, MetadataValue, asset

from orchestration import checks, manifest, run
from orchestration.assets import derived, reference_data, website
from orchestration.assets.sources_manual import SPECS as MANUAL_SPECS
from orchestration.commands import COMMANDS, TRACKED
from orchestration.paths import PipelinePaths


class SourceRunConfig(Config):
    # Both set by the coordinator (orchestration/daily.py); empty on a manual run.
    source_manifest: str = ""
    coordinator_run_id: str = ""


def load_coordinator_manifest(source_manifest: str, coordinator_run_id: str) -> dict:
    """The manifest a coordinator run handed an asset, refused if it is gone or
    belongs to another run -- never attributed to the wrong day."""
    path = Path(source_manifest)
    if not path.is_file():
        # The coordinator wrote it before starting this run; if it is gone,
        # something broke, and "no source run" would be a lie.
        raise Failure(f"source manifest {path} does not exist")
    data = manifest.load(path)
    if coordinator_run_id and data.get("run_id") != coordinator_run_id:
        raise Failure(
            f"source manifest {path} belongs to run {data.get('run_id')}, "
            f"not {coordinator_run_id}"
        )
    return data


def source_run_metadata(config: SourceRunConfig) -> dict:
    if not config.source_manifest:
        return {"source_run": manifest.NO_SOURCE_RUN}
    data = load_coordinator_manifest(config.source_manifest, config.coordinator_run_id)
    red = manifest.red_sources(data)
    return {
        "source_run": data["run_id"],
        "red_sources": len(red),
        "red_source_names": ", ".join(red) or "none",
        "source_statuses": MetadataValue.json(data["sources"]),
    }


@asset(
    group_name="canonical",
    deps=["staging_db", *TRACKED, *(spec.key for spec in MANUAL_SPECS)],
    kinds={"sqlite"},
    description=(
        "The working database once every source has run. Carries the validation checks; "
        "every export is built from it."
    ),
)
def validated_working_database(
    context: AssetExecutionContext, config: SourceRunConfig, paths: PipelinePaths
) -> MaterializeResult:
    db = paths.resolve(paths.working_db)
    if not db.is_file():
        raise Failure(f"No working database at {db}. Materialise staging_db first.")
    source_ids = tuple(dict.fromkeys(s for n in TRACKED for s in COMMANDS[n].source_ids))
    metadata = {"working_db": str(db), **source_run_metadata(config)}
    metadata.update(run.source_snapshot(db, source_ids))
    context.log.info(json.dumps({k: str(v) for k, v in metadata.items()}, indent=2))
    return MaterializeResult(metadata=metadata)


@asset(
    group_name="canonical",
    deps=["validated_working_database"],
    description=(
        "Today's is_latest count per indicator, the baseline for tomorrow's row_collapse rule. "
        "Only recorded when every blocking check passed (validate_data.py --record-volume)."
    ),
)
def volume_history(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
    n = checks.record_volume(paths)
    return MaterializeResult(metadata={"indicators recorded": n})


REPORT_LINES_IN_METADATA = 100


def revisions_arguments(paths: PipelinePaths) -> dict:
    """report_revisions()'s arguments, read off the command line production ran
    (--db and --since, {today} being today's UTC date), every path absolute. Any
    other shape is refused rather than half-translated."""
    script, *tokens = paths.render(COMMANDS["revisions_report"].argv, today=run.today())
    flags = dict(zip(tokens[::2], tokens[1::2], strict=False))
    if len(tokens) != 4 or set(flags) != {"--db", "--since"}:
        raise ValueError(f"revisions_report: `{script} {' '.join(tokens)}` is not --db/--since")
    return {"db_path": paths.resolve(flags["--db"]), "since": flags["--since"]}


@asset(
    group_name="canonical",
    deps=["validated_working_database"],
    description=(
        "Values revised today: report_revisions() from scripts/revisions_report.py, called in "
        "process with --since today (UTC). Logs the report; writes no file."
    ),
)
def revisions_report(context: AssetExecutionContext, paths: PipelinePaths) -> MaterializeResult:
    revisions, report = run.call_function(context, "revisions_report", **revisions_arguments(paths))
    lines = report.splitlines()
    for line in lines:
        context.log.info(line)
    shown = "\n".join(lines[:REPORT_LINES_IN_METADATA])
    if len(lines) > REPORT_LINES_IN_METADATA:
        shown += f"\n... {len(lines) - REPORT_LINES_IN_METADATA} more line(s) in the run log"
    return MaterializeResult(
        metadata={"revisions": len(revisions), "report": MetadataValue.md(f"```\n{shown}\n```")}
    )


# Everything validate_and_export builds. committed_stores depends on all of it;
# tests/test_orchestration.py holds this list to that job's selection.
EXPORTED = [
    validated_working_database,
    volume_history,
    revisions_report,
    *derived.ASSETS,
    *website.ASSETS,
    reference_data.commune_adjacency,
    reference_data.commune_typology,
    reference_data.commune_flows_export,
    reference_data.schools_ise_export,
    reference_data.schools_by_commune_export,
]


class CommittedWritesConfig(Config):
    # All three are set by the coordinator (orchestration/daily.py), for its own
    # run. Without them committed_stores refuses, so materialising it from the
    # UI or with `dagster job execute` can never write a committed file.
    allow_committed_writes: bool = False
    source_manifest: str = ""
    coordinator_run_id: str = ""


def _as_git_names_it(paths: PipelinePaths, path: Path) -> str:
    try:
        return path.resolve().relative_to(paths.root.resolve()).as_posix()
    except ValueError:
        return str(path)


PARTIAL_NOTICE_OPENING = "committed_stores could not put every committed file back."

# For any error offload() raised that is neither a refusal nor a partial
# publication. Deliberately claims no outcome: the error may have come before
# publishing (nothing changed), during it (rolled back), or after it (the new
# files are in place).
UNREPORTED_ERROR_NOTICE = (
    "committed_stores stopped with {error}. offload() did not report a partial publication "
    "(a publish that stops part-way and cannot be put back raises PartialPublication and "
    "names its files). An error before publishing changed nothing; an error while publishing "
    "was rolled back from this run's backups; an error after publishing leaves the new "
    "committed files in place. Nothing was committed and the run is red; `git status` shows "
    "which of these happened."
)


def partial_publication_notice(paths: PipelinePaths, left_new, backup_dir: Path) -> str:
    """What to do after offload() raised PartialPublication: exactly the files
    still holding this run's version, one per line as git names them, and where
    their previous versions are kept. Single files only, never a directory."""
    files = "".join(f"  {_as_git_names_it(paths, Path(f))}\n" for f in left_new)
    return (
        f"{PARTIAL_NOTICE_OPENING} offload() stopped while publishing, put back every file "
        "it could, and these still hold this run's version:\n"
        f"{files}"
        f"The previous version of each is kept in {backup_dir}, whose manifest names the file "
        "each backup belongs to; nothing there is deleted automatically. Nothing was "
        "committed and the run is red. Restore each file above, one at a time, from that "
        "directory or with git checkout -- <file>. Never restore a whole directory: it also "
        "holds files this run did not write."
    )


@asset(
    group_name="canonical",
    deps=[a.key for a in EXPORTED],
    kinds={"sqlite", "csv"},
    description=(
        "data/belgian_macro.db and every in_db store's CSV (config/stores.yaml), written "
        "last by offload() from scripts/offload_stores.py, the function `make offload` runs. "
        "Only the coordinator (python -m orchestration.daily) allows it; materialised from "
        "here it refuses. A refusal changes nothing, including the refusal while a -wal or "
        "-shm file sits beside the committed database (a check before writing, not a lock). "
        "An exception while it replaces its files puts back every one already replaced, from "
        "backups taken first; a file it cannot put back is named, with the backup directory, "
        "which is kept. A killed process or a power cut mid-publish restores nothing: that "
        "run's backup directory is what remains, for a restore by hand."
    ),
)
def committed_stores(
    context: AssetExecutionContext, config: CommittedWritesConfig, paths: PipelinePaths
) -> MaterializeResult:
    refused = "committed_stores: refused -- "
    unchanged = " Nothing was changed."
    if not config.allow_committed_writes:
        raise Failure(
            refused + "the committed database and the in_db CSVs are written only by the "
            "coordinator (python -m orchestration.daily), for its own run." + unchanged
        )
    if not (config.coordinator_run_id and config.source_manifest):
        raise Failure(
            refused
            + "allow_committed_writes needs the coordinator's run id and manifest."
            + unchanged
        )
    state = load_coordinator_manifest(config.source_manifest, config.coordinator_run_id)
    assembled = state.get("assemble", {}).get("status")
    if assembled != manifest.SUCCESS:
        raise Failure(
            refused + f"manifest {config.source_manifest} records assemble={assembled}, "
            "not success." + unchanged
        )
    if not paths.writes_into_repo and paths.targets_repository_committed_files:
        raise Failure(
            refused + f"out_root is {paths.out_root}, but committed_db ({paths.committed_db}) "
            f"or stores ({paths.stores}) is the repository's own; a redirected run must not "
            "write the committed files." + unchanged
        )
    try:
        counts = run.call_function(
            context,
            "committed_stores",
            working_db=paths.resolve(paths.working_db),
            committed_db=paths.resolve(paths.committed_db),
            stores_path=paths.resolve(paths.stores),
        )
    except Failure as refusal:
        # offload()'s own refusals all come before its first os.replace.
        raise Failure(f"{refusal.description} No committed file was changed.") from refusal
    except Exception as exc:
        # Let the error through unchanged, saying what it can mean. The script's
        # module is the one call_function already imported; never imported here.
        module = sys.modules.get(COMMANDS["committed_stores"].function.partition(":")[0])
        partial = getattr(module, "PartialPublication", None)
        if partial is not None and isinstance(exc, partial):
            context.log.error(partial_publication_notice(paths, exc.left_new, exc.backup_dir))
        else:
            context.log.error(UNREPORTED_ERROR_NOTICE.format(error=type(exc).__name__))
        raise
    return MaterializeResult(
        metadata={
            "source_run": state["run_id"],
            **run.output_metadata(paths, "committed_stores"),
            **{f"{store} rows": rows for store, rows in counts.items()},
        }
    )


ASSETS = [validated_working_database, volume_history, revisions_report, committed_stores]
