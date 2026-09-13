"""Full proof that the Dagster route publishes exactly what `make` publishes.

Checks out the committed HEAD twice, in two temporary git worktrees, and
rebuilds every export in each from what is committed -- no network:

  make side     `make assemble exports` (the Makefile's own recipes; read from
                the Makefile and run through bash where `make` is not installed)
  dagster side  the assemble_working_database job, then validate_and_export

then compares every file of the two trees, byte for byte, with the one field
that is a wall-clock timestamp (manifest.json's build_date) masked.

Refuses a dirty working tree. Both worktrees are HEAD: uncommitted changes
would be silently left out of the comparison, so a run before committing would
compare two copies of the previous commit and prove nothing.

    python scripts/verify_dagster_parity.py            (make verify-dagster-parity)

Exit 0: identical. Exit 1: differences, listed. Exit 2: refused.
"""

import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILD_ID = "parity"
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", ".ruff_cache", "node_modules"}
SKIP_PREFIXES = ("data/local/",)


def _git(*args: str, cwd: Path = REPO_ROOT) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True, encoding="utf-8"
    ).stdout


def _makefile_recipes(text: str) -> dict[str, list[str]]:
    recipes: dict[str, list[str]] = {}
    current = None
    for line in text.replace("\\\n", " ").splitlines():
        if line.startswith("\t"):
            if current is not None and line.strip() and not line.strip().startswith("#"):
                recipes[current].append(line.strip())
            continue
        match = re.match(r"^([A-Za-z0-9_.-]+):(?!=)", line)
        current = match.group(1) if match else None
        if current:
            recipes[current] = []
    return recipes


def _run(argv: list[str], cwd: Path, env: dict) -> None:
    print("  $ " + " ".join(argv), flush=True)
    subprocess.run(argv, cwd=cwd, env=env, check=True)


def _make(targets: list[str], cwd: Path, env: dict) -> None:
    python = Path(sys.executable).as_posix()
    if shutil.which("make"):
        _run(["make", f"PYTHON={python}", *targets], cwd, env)
        return
    bash = shutil.which("bash")
    if not bash:
        raise SystemExit("Neither make nor bash is available to run the Makefile recipes.")
    recipes = _makefile_recipes((cwd / "Makefile").read_text(encoding="utf-8"))
    variables = {
        "$(PYTHON)": shlex.quote(python),
        "$(COMMITTED_DB)": "data/belgian_macro.db",
        "$(DB)": "data/local/working.db",
        "$(STORES)": "config/stores.yaml",
    }

    def run_target(target: str) -> None:
        for line in recipes[target]:
            nested = re.fullmatch(r"\$\(MAKE\) (\S+)", line)
            if nested:
                run_target(nested.group(1))
                continue
            for name, value in variables.items():
                line = line.replace(name, value)
            _run([bash, "-c", line.replace("$$", "$")], cwd, env)

    for target in targets:
        run_target(target)


def _dagster(cwd: Path, env: dict) -> None:
    for job in ("assemble_working_database", "validate_and_export"):
        _run(
            [sys.executable, "-m", "dagster", "job", "execute", "-m", "orchestration", "-j", job],
            cwd,
            env,
        )


def _digest(path: Path) -> str:
    if path.name == "manifest.json":
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        else:
            if isinstance(data, dict):
                data.pop("build_date", None)
                return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tree(root: Path) -> dict[str, str]:
    files = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for filename in filenames:
            path = Path(dirpath) / filename
            relative = path.relative_to(root).as_posix()
            if filename == ".git" or relative.startswith(SKIP_PREFIXES):
                continue
            files[relative] = _digest(path)
    return files


def main() -> int:
    dirty = _git("status", "--porcelain").strip()
    if dirty:
        print("Refusing: the working tree has uncommitted changes. Both comparison trees are")
        print("checked out from HEAD, so these would be left out and the comparison would prove")
        print("nothing about them. Commit (or stash) first:\n")
        print(dirty)
        return 2

    head = _git("rev-parse", "HEAD").strip()
    print(f"Comparing both routes on HEAD {head}")
    scratch = Path(tempfile.mkdtemp(prefix="dagster-parity-"))
    trees = {"make": scratch / "make", "dagster": scratch / "dagster"}
    try:
        for tree in trees.values():
            # Byte-for-byte what is committed, as CI's Linux runner checks it out.
            # With core.autocrlf=true (the Windows default) a fresh checkout gets
            # CRLF migrations, whose checksums no longer match the ones recorded
            # in the committed database, and the assemble step refuses to run.
            _git("-c", "core.autocrlf=false", "worktree", "add", "--detach", str(tree), head)

        env = {**os.environ, "BUILD_ID": BUILD_ID, "PYTHONIOENCODING": "utf-8"}
        env.pop("WORKING_DB", None)

        try:
            print("\n[make] assemble exports")
            _make(["assemble", "exports"], trees["make"], env)

            print("\n[dagster] assemble_working_database, validate_and_export")
            home = trees["dagster"] / "data" / "local" / "dagster_home"
            home.mkdir(parents=True, exist_ok=True)
            _dagster(trees["dagster"], {**env, "DAGSTER_HOME": str(home)})
        except subprocess.CalledProcessError as exc:
            # A build that did not finish proves nothing either way: say which
            # step stopped, and do not compare half-built trees.
            print(f"\nNot compared: a build step exited with code {exc.returncode}:")
            print("  " + " ".join(str(a) for a in exc.cmd))
            return 1

        make_files, dagster_files = _tree(trees["make"]), _tree(trees["dagster"])
        only_make = sorted(set(make_files) - set(dagster_files))
        only_dagster = sorted(set(dagster_files) - set(make_files))
        differ = sorted(
            f for f in set(make_files) & set(dagster_files) if make_files[f] != dagster_files[f]
        )
        print(f"\nCompared {len(make_files)} files on HEAD {head}.")
        for label, names in (
            ("Only written by make", only_make),
            ("Only written by Dagster", only_dagster),
            ("Different content", differ),
        ):
            if names:
                print(f"{label} ({len(names)}):")
                for name in names[:50]:
                    print(f"  {name}")
        if only_make or only_dagster or differ:
            return 1
        print("Identical: every file matches (manifest.json build_date masked).")
        return 0
    finally:
        for tree in trees.values():
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(tree)],
                cwd=REPO_ROOT,
                capture_output=True,
            )
        _git("worktree", "prune")
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
