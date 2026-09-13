"""The per-run source manifest: what the fetch half of a coordinator run did.

One file per run, data/local/dagster_runs/{run_id}/sources.json, never a fixed
path: a manifest left behind by an earlier run must never be mistaken for the
current state. The export half is handed its run's path explicitly, and a run
started without one reports "no source run attached".

It is also what daily_fetch.yml's auto-merge gate reads:

    python -m orchestration.manifest data/local/dagster_runs/<run_id>/sources.json

prints `key=value` lines for $GITHUB_OUTPUT (see gate()).
"""

import argparse
import json
import secrets
import shutil
import sys
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


def gate(manifest: dict) -> dict[str, str]:
    """The auto-merge decision for one run, as the workflow's outputs.

    One entry per tracked outcome under its old workflow step id (fetch_macro,
    sync_canonical, ...), `summary` with all eight on one line, and `all_ok`:
    "true" only when the working database was assembled, every tracked
    outcome is a success and validate_and_export succeeded. Anything missing
    from the file counts as not run, so an incomplete manifest never merges.
    """
    sources = manifest.get("sources", {})
    statuses = {
        COMMANDS[name].workflow_step: sources.get(name, {}).get("status", NOT_RUN)
        for name in TRACKED
    }
    all_ok = (
        manifest.get("assemble", {}).get("status") == SUCCESS
        and all(status == SUCCESS for status in statuses.values())
        and manifest.get("validate_and_export", {}).get("status") == SUCCESS
    )
    return {
        **statuses,
        "summary": ", ".join(f"{step}={status}" for step, status in statuses.items()),
        "all_ok": "true" if all_ok else "false",
    }


def prune(runs_dir: Path, keep: int = 30) -> None:
    """Keep the newest `keep` run directories. Run ids start with a UTC
    timestamp, so name order is age order."""
    if not runs_dir.is_dir():
        return
    runs = sorted(p for p in runs_dir.iterdir() if p.is_dir())
    for old in runs[: max(len(runs) - keep, 0)]:
        shutil.rmtree(old, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Print a run manifest's auto-merge gate")
    ap.add_argument("manifest", help="data/local/dagster_runs/<run_id>/sources.json")
    args = ap.parse_args(argv)
    path = Path(args.manifest)
    if not args.manifest or not path.is_file():
        # No manifest means no evidence the day went well: refuse, never "ok".
        print(f"No source manifest at {args.manifest!r}.", file=sys.stderr)
        return 2
    for key, value in gate(load(path)).items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
