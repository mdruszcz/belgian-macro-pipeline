"""Dagster produces the same outputs as the scripts it wraps -- the reduced
check that fits in the test suite.

Everything writes to temporary directories; the worktree is never touched.
Three parts: the assembled working database, the validation results, and the
exporters that accept an output path. The full comparison -- every file
`make assemble exports` writes, including the pages and the scripts that can
only write into the repository -- is scripts/verify_dagster_parity.py, run on
a committed HEAD.

Marked slow: real subprocesses against the real assembled data.
"""

import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

dg = pytest.importorskip("dagster")

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from build_staging_db import build  # noqa: E402
from test_build_staging_db import _extra_csv_only, _fresh_source_db  # noqa: E402

from orchestration import checks  # noqa: E402
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


EXPORTERS = ["national_csv", "communes_csv", "aggregates_csv", "percentiles_csv"]


def test_the_exporters_write_identical_files(working_db, tmp_path):
    by_script = PipelinePaths(working_db=str(working_db), out_root=str(tmp_path / "a"))
    for name in EXPORTERS:
        subprocess.run(
            [sys.executable, *by_script.render(COMMANDS[name].argv)], cwd=REPO, check=True
        )

    by_dagster = PipelinePaths(working_db=str(working_db), out_root=str(tmp_path / "b"))
    result = _materialize(by_dagster, EXPORTERS)
    assert result.success

    written = sorted(p.relative_to(tmp_path / "a") for p in (tmp_path / "a").rglob("*.csv"))
    assert len(written) == len(EXPORTERS)
    for relative in written:
        assert (tmp_path / "a" / relative).read_bytes() == (tmp_path / "b" / relative).read_bytes()
