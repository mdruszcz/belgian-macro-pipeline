"""The per-run source manifest: what the fetch half of a coordinator run did.

One file per run, data/local/dagster_runs/{run_id}/sources.json, never a fixed
path: a manifest left behind by an earlier run must never be mistaken for the
current state. The export half is handed its run's path explicitly, and a run
started without one reports "no source run attached".
"""

import json
import secrets
import shutil
from datetime import datetime, timezone
from pathlib import Path

from orchestration.commands import COMMANDS, TRACKED

NOT_RUN = "not_run"
SUCCESS = "success"
FAILED = "failed"
SKIPPED = "skipped"
NO_SOURCE_RUN = "no source run attached"


def new_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return f"{now:%Y%m%dT%H%M%SZ}-{secrets.token_hex(4)}"


def path_for(runs_dir: Path, run_id: str) -> Path:
    return runs_dir / run_id / "sources.json"


def initial(run_id: str, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    return {
        "run_id": run_id,
        "started_at": now.isoformat(),
        "assemble": {"status": NOT_RUN},
        "sources": {
            name: {"status": NOT_RUN, "workflow_step": COMMANDS[name].workflow_step}
            for name in TRACKED
        },
        "validate_and_export": {"status": NOT_RUN},
    }


def write(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def red_sources(manifest: dict) -> list[str]:
    """Every tracked outcome that is not a success -- a skipped
    canonical_observations counts, exactly as a skipped step would today."""
    return sorted(n for n, s in manifest["sources"].items() if s["status"] != SUCCESS)


def prune(runs_dir: Path, keep: int = 30) -> None:
    """Keep the newest `keep` run directories. Run ids start with a UTC
    timestamp, so name order is age order."""
    if not runs_dir.is_dir():
        return
    runs = sorted(p for p in runs_dir.iterdir() if p.is_dir())
    for old in runs[: max(len(runs) - keep, 0)]:
        shutil.rmtree(old, ignore_errors=True)
