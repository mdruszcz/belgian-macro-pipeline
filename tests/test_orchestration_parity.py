"""Dagster produces the same outputs as the scripts it wraps -- the reduced
check that fits in the test suite.

Everything writes to temporary directories; the worktree is never touched.
Four parts: the assembled working database, the validation results, the
exporters that accept an output path, and the offload. An exporter whose asset calls the
script's function in process (Command.function) is held to the same test: its
command line as a subprocess on one side, the asset on the other, from a
different working directory, so a path that only resolves from the repository
root shows up here. The offload is compared on tests/test_offload_stores.py's
small committed database, never the real one: with the repository's own paths
it would write the committed files. The full comparison -- every file
`make assemble exports offload` writes, including the pages, the scripts that
can only write into the repository and the committed database -- is
scripts/verify_dagster_parity.py, run on a committed HEAD.

Marked slow: real subprocesses against the real assembled data.
"""

import re
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest
import yaml

dg = pytest.importorskip("dagster")

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from build_staging_db import build  # noqa: E402
from test_build_staging_db import _extra_csv_only, _fresh_source_db  # noqa: E402
from test_offload_stores import build_pipeline  # noqa: E402

from orchestration import checks, manifest  # noqa: E402
from orchestration.commands import COMMANDS  # noqa: E402
from orchestration.definitions import build_defs  # noqa: E402
from orchestration.paths import PipelinePaths  # noqa: E402

pytestmark = pytest.mark.slow

VALIDATED = "validated_working_database"


def _materialize(paths: PipelinePaths, names: list[str]):
    job = build_defs(paths).resolve_implicit_global_asset_job_def()
    return job.execute_in_process(
        instance=dg.DagsterInstance.ephemeral(),
        raise_on_error=False,
        asset_selection=[dg.AssetKey(n) for n in names],
    )


def test_the_assembled_database_is_byte_identical(tmp_path):
    source = _fresh_source_db(tmp_path / "source.db")
    registry = _extra_csv_only(tmp_path)

    by_script = build(
        source_db=source, working_db=tmp_path / "a" / "working.db", stores_path=registry
    )
    paths = PipelinePaths(
        working_db=str(tmp_path / "b" / "working.db"), source_db=str(source), stores=str(registry)
    )
    assert _materialize(paths, ["staging_db"]).success

    assert by_script.read_bytes() == (tmp_path / "b" / "working.db").read_bytes()


def _cli_violations(db: Path) -> Counter:
    result = subprocess.run(
        [sys.executable, "scripts/validate_data.py", "--db", str(db)],
        cwd=REPO,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode in (0, 1), result.stderr
    lines = (result.stdout + result.stderr).splitlines()
    return Counter(
        m.group(1)
        for line in lines
        if (m := re.match(r"::(?:error|warning)::\[\w+\] (\w+):", line))
    )


def test_the_checks_report_exactly_what_validate_data_reports(working_db):
    cli = _cli_violations(working_db)

    paths = PipelinePaths(working_db=str(working_db))
    assert Counter(v.rule for v in checks.run_validation(paths)) == cli

    result = _materialize(paths, [VALIDATED])
    failed = {e.check_name for e in result.get_asset_check_evaluations() if not e.passed}
    assert failed == set(cli)
    assert len(result.get_asset_check_evaluations()) == len(checks.SEVERITIES)


EXPORTERS = [
    "national_csv",
    "communes_csv",
    "aggregates_csv",
    "percentiles_csv",
    "communes_history_full_csv",
    "communes_history_csv",
]


@pytest.mark.parametrize("name", EXPORTERS)
def test_the_exporters_write_identical_files(working_db, tmp_path, monkeypatch, name):
    by_script = PipelinePaths(working_db=str(working_db), out_root=str(tmp_path / "a"))
    subprocess.run([sys.executable, *by_script.render(COMMANDS[name].argv)], cwd=REPO, check=True)

    by_dagster = PipelinePaths(working_db=str(working_db), out_root=str(tmp_path / "b"))
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    result = _materialize(by_dagster, [name])
    assert result.success

    written = sorted(p.relative_to(tmp_path / "a") for p in (tmp_path / "a").rglob("*.csv"))
    assert len(written) == 1
    for relative in written:
        assert (tmp_path / "a" / relative).read_bytes() == (tmp_path / "b" / relative).read_bytes()


def test_the_offload_writes_identical_committed_files(tmp_path, monkeypatch):
    """offload() called by the asset versus scripts/offload_stores.py run with
    `make offload`'s command line, on two copies of one assembled state."""
    a = build_pipeline(tmp_path / "a")
    shutil.copytree(tmp_path / "a", tmp_path / "b")
    b = {key: tmp_path / "b" / path.relative_to(tmp_path / "a") for key, path in a.items()}
    registry = yaml.safe_load(b["registry"].read_text(encoding="utf-8"))
    registry["stores"]["walstat"]["path"] = str(b["csv"])
    b["registry"].write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    before = a["committed_db"].read_bytes()

    by_script = PipelinePaths(
        working_db=str(a["working"]), committed_db=str(a["committed_db"]), stores=str(a["registry"])
    )
    argv = by_script.render(COMMANDS["committed_stores"].argv)
    subprocess.run([sys.executable, *argv], cwd=REPO, check=True)

    run_id = "20260914T050000Z-0a1b2c3d"
    runs = tmp_path / "b" / "runs"
    state = manifest.initial(run_id)
    state["assemble"]["status"] = manifest.SUCCESS
    manifest.write(manifest.path_for(runs, run_id), state)
    by_dagster = PipelinePaths(
        out_root=str(tmp_path / "b" / "out"),
        working_db=str(b["working"]),
        committed_db=str(b["committed_db"]),
        stores=str(b["registry"]),
        runs_dir=str(runs),
    )
    config = {
        "allow_committed_writes": True,
        "source_manifest": str(manifest.path_for(runs, run_id)),
        "coordinator_run_id": run_id,
    }
    monkeypatch.chdir(tmp_path)
    result = (
        build_defs(by_dagster)
        .resolve_implicit_global_asset_job_def()
        .execute_in_process(
            instance=dg.DagsterInstance.ephemeral(),
            raise_on_error=False,
            asset_selection=[dg.AssetKey("committed_stores")],
            run_config={"ops": {"committed_stores": {"config": config}}},
        )
    )
    assert result.success

    assert a["committed_db"].read_bytes() != before, "the offload wrote nothing"
    assert a["committed_db"].read_bytes() == b["committed_db"].read_bytes()
    assert a["csv"].read_bytes() == b["csv"].read_bytes()
    for side in (a, b):
        assert [p.name for p in side["working"].parent.iterdir() if p.suffix == ".tmp"] == []
