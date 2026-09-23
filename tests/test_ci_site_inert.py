"""Tests for .github/workflows/site_inert.sh -- CI's decision to skip the
generated_site sweeps. The failure that matters is a false "inert": a page
change that skips the sweeps could merge a broken page. So every doubtful
case must answer false."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "site_inert.sh"
BASH = shutil.which("bash")

pytestmark = pytest.mark.skipif(BASH is None, reason="needs bash")


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def _verdict(tmp_path, changed_paths, base_override=None):
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
    for rel in changed_paths:
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n")
    if changed_paths:
        _git(repo, "add", ".")
        _git(repo, "commit", "-q", "-m", "change")
    out = tmp_path / "out.txt"
    env = {**os.environ, "GITHUB_OUTPUT": str(out), "GITHUB_EVENT_NAME": ""}
    env["SITE_INERT_BASE"] = base if base_override is None else base_override
    subprocess.run([BASH, str(SCRIPT)], cwd=repo, env=env, check=True, capture_output=True)
    return out.read_text().strip()


@pytest.mark.parametrize(
    "paths",
    [
        ["src/fetchers/spf_agdp.py", "scripts/sync_spf_agdp.py", "tests/test_sync_spf_agdp.py"],
        ["orchestration/commands.py", "config/stores.yaml", "config/sources/spf_finances.yaml"],
        ["docs/features/spf_agdp.md", "README.md"],
        ["src/validation/rules.py", ".github/workflows/daily_fetch.yml"],
    ],
)
def test_changes_no_page_generator_reads_are_inert(tmp_path, paths):
    assert _verdict(tmp_path, paths) == "site_inert=true"


@pytest.mark.parametrize(
    "paths",
    [
        ["src/fetchers/x.py", "data/belgian_macro.db"],  # export_local_pages reads the db
        ["config/indicators/NEW.yaml"],
        ["scripts/export_local_pages.py"],
        ["src/pages/resolve.py"],
        ["assets/commune_map.js"],
        ["commune.html"],
        ["public/data/manifest.json"],
        ["tests/pages/test_commune_loop.py"],  # changing a sweep must run it
        ["tests/conftest.py"],
        ["tests/test_export_local_pages.py"],
        ["Makefile"],
        [".github/workflows/ci.yml"],
    ],
)
def test_anything_a_page_could_read_is_not_inert(tmp_path, paths):
    assert _verdict(tmp_path, paths) == "site_inert=false"


def test_no_usable_base_is_not_inert(tmp_path):
    assert _verdict(tmp_path, ["src/fetchers/x.py"], base_override="0" * 40) == "site_inert=false"


def test_an_empty_diff_is_not_inert(tmp_path):
    assert _verdict(tmp_path, []) == "site_inert=false"
