"""Running an existing script, and describing what it left behind.

Everything here is plumbing: a subprocess and file/database metadata for the
UI. Nothing computes, transforms or stores a figure.
"""

import os
import sqlite3
import subprocess
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from dagster import Failure, MetadataValue

from orchestration.commands import COMMANDS
from orchestration.paths import PipelinePaths

TAIL_LINES = 40


def run_script(context, paths: PipelinePaths, name: str, extra: tuple[str, ...] = ()) -> str:
    """Run COMMANDS[name] exactly as the Makefile does, from the repository
    root, with this interpreter. Raises Failure on a non-zero exit, so the
    asset turns red; returns the last lines of output."""
    command = COMMANDS[name]
    if command.writes_repo_only and not paths.writes_into_repo:
        raise Failure(
            f"{name}: {command.argv[0]} can only write into the repository, "
            f"and out_root is {paths.out_root}"
        )
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    argv = [sys.executable, *paths.render(command.argv, today=today), *extra]
    context.log.info("$ " + " ".join(argv[1:]))

    # UTF-8 for the child's own stdout only: a script printing an em dash must
    # not crash on a Windows console code page. File writes are unaffected.
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
    tail: deque[str] = deque(maxlen=TAIL_LINES)
    with subprocess.Popen(
        argv,
        cwd=paths.root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    ) as proc:
        for line in proc.stdout:
            line = line.rstrip("\n")
            tail.append(line)
            context.log.info(line)
        code = proc.wait()
    output = "\n".join(tail)
    if code != 0:
        raise Failure(
            f"{name}: {command.argv[-1] if command.argv[0] == '-m' else command.argv[0]} "
            f"exited with code {code}",
            metadata={"last_output": MetadataValue.md(f"```\n{output}\n```")},
        )
    return output


def output_metadata(paths: PipelinePaths, name: str) -> dict:
    """Size and row count of each declared output -- for the UI only."""
    metadata = {}
    for template in COMMANDS[name].outputs:
        target = paths.resolve(template.format(**paths.placeholders()))
        label = template.format(**paths.placeholders())
        if target.is_dir():
            metadata[f"{label} files"] = sum(1 for p in target.rglob("*") if p.is_file())
        elif target.is_file():
            metadata[f"{label} bytes"] = target.stat().st_size
            if target.suffix == ".csv":
                with target.open("rb") as fh:
                    metadata[f"{label} rows"] = max(sum(1 for _ in fh) - 1, 0)
        else:
            metadata[f"{label}"] = "missing"
    return metadata


def source_snapshot(db_path: Path, source_ids: tuple[str, ...]) -> dict:
    """Latest-row count, latest period and last fetch run per source, read
    from the working database. Metadata for the UI; never the source of truth
    for whether a run succeeded (the coordinator's manifest is)."""
    if not source_ids:
        return {}
    if not db_path.is_file():
        return {"working_db": "missing"}
    snapshot = {}
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        for source_id in source_ids:
            rows = conn.execute(
                "SELECT COUNT(*) FROM observations o JOIN indicators i USING (indicator_id) "
                "WHERE i.source_id = ? AND o.is_latest = 1",
                (source_id,),
            ).fetchone()[0]
            latest = conn.execute(
                "SELECT o.period FROM observations o JOIN indicators i USING (indicator_id) "
                "WHERE i.source_id = ? AND o.is_latest = 1 "
                "ORDER BY o.period_end DESC LIMIT 1",
                (source_id,),
            ).fetchone()
            run = conn.execute(
                # Not adapter 'rebuild': that is build_staging_db.py reloading a
                # committed CSV at assemble time, not a fetch. Counting it would
                # show a source as fetched today when nothing was fetched.
                "SELECT status, COALESCE(finished_at, started_at) FROM fetch_runs "
                "WHERE source_id = ? AND adapter != 'rebuild' "
                "ORDER BY started_at DESC LIMIT 1",
                (source_id,),
            ).fetchone()
            snapshot[f"{source_id} latest rows"] = rows
            snapshot[f"{source_id} latest period"] = latest[0] if latest else "none"
            snapshot[f"{source_id} last fetch run"] = f"{run[0]} at {run[1]}" if run else "none"
    except sqlite3.Error as exc:
        snapshot["snapshot error"] = str(exc)
    finally:
        conn.close()
    return snapshot
