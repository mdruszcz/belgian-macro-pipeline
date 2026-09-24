"""Tests for .github/workflows/slow_inert.sh -- CI's decision to skip the
`slow` tests (build_staging_db, committed-store consistency, exports,
offload, orchestration parity, validation). The failure that matters is a
false "inert": a change that skips these tests could merge a broken
pipeline. So every doubtful case must answer false."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "slow_inert.sh"
BASH = shutil.which("bash")

pytestmark = pytest.mark.skipif(BASH is None, reason="needs bash")

SLOW_MARKED_TEST = """import pytest

pytestmark = pytest.mark.slow


def test_something():
    assert True
"""

NON_SLOW_TEST = """import pytest


def test_something():
    assert True
"""

DECORATOR_SLOW_TEST = """import pytest


@pytest.mark.slow
def test_something():
    assert True
"""


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def _verdict(tmp_path, changed_files, base_override=None):
    """changed_files: dict of {relative path: content} to add/modify, committed
    on top of a base commit. An empty dict means an empty diff."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.test")
    _git(repo, "config", "user.name", "t")
    (repo / "seed.txt").write_text("seed\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "base")
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout.strip()
    for rel, content in changed_files.items():
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    if changed_files:
        _git(repo, "add", ".")
        _git(repo, "commit", "-q", "-m", "change")
    out = tmp_path / "out.txt"
    env = {**os.environ, "GITHUB_OUTPUT": str(out), "GITHUB_EVENT_NAME": ""}
    env["SLOW_INERT_BASE"] = base if base_override is None else base_override
    subprocess.run(
        [BASH, str(SCRIPT)],
        cwd=repo,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return out.read_text().strip()


@pytest.mark.parametrize(
    "changed_files",
    [
        {"docs/features/spf_agdp.md": "x\n"},
        {"README.md": "x\n"},
        {"assets/commune_map.js": "x\n"},
        {"assets/i18n/fr.json": "x\n"},
        {"commune.html": "x\n"},
        {"tests/test_something_ui.py": NON_SLOW_TEST},
    ],
)
def test_changes_the_slow_tests_dont_read_are_inert(tmp_path, changed_files):
    assert _verdict(tmp_path, changed_files) == "slow_inert=true"


@pytest.mark.parametrize(
    "changed_files",
    [
        {"data/belgian_macro.db": "x\n"},
        {"config/indicators/NEW.yaml": "x\n"},
        {"config/stores.yaml": "x\n"},
        {"scripts/export_local_pages.py": "x\n"},
        {"src/validation/rules.py": "x\n"},
        {"orchestration/commands.py": "x\n"},
        {"migrations/0001_init.sql": "x\n"},
        {"Makefile": "x\n"},
        {"pyproject.toml": "x\n"},
        {"requirements.txt": "x\n"},
        {".github/workflows/ci.yml": "x\n"},
        {"tests/conftest.py": "x\n"},
        {"tests/test_build_staging_db.py": SLOW_MARKED_TEST},
        {"tests/test_build_staging_db.py": DECORATOR_SLOW_TEST},
    ],
)
def test_anything_the_slow_tests_could_read_is_not_inert(tmp_path, changed_files):
    assert _verdict(tmp_path, changed_files) == "slow_inert=false"


def test_no_usable_base_is_not_inert(tmp_path):
    assert _verdict(tmp_path, {"docs/x.md": "x\n"}, base_override="0" * 40) == "slow_inert=false"


def test_an_empty_diff_is_not_inert(tmp_path):
    assert _verdict(tmp_path, {}) == "slow_inert=false"
