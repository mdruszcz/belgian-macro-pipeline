"""
Ensure every committed store's sources/indicators reference rows exist in
`--db`, driven by config/stores.yaml -- replaces the six `--reference-rows-only`
calls that used to be duplicated across the Makefile's `reference` target,
`.github/workflows/daily_fetch.yml` and `.github/workflows/manual_sources.yml`.

WHY A SUBPROCESS PER STORE, rather than importing each script's `sync()` and
calling it directly: the six scripts do not share a common signature (some
take --source-file, others --census-dir, --raw-dir or --csv, with different
defaults), and `reference_rows.args` in the registry already IS a CLI
invocation, declared once per store rather than re-encoded here as a second
copy of each script's argument shape.

Fails loudly and stops at the first failing store (CLAUDE.md rule 13): a
partial reference-row set is exactly the state that made manual_sources.yml
crash on its first run (docs/decisions/0002), so continuing past a failure
here would just move that same risk one script over.
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.stores import DEFAULT_STORES_PATH, load_stores  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]


def ensure_all(db_path: Path, stores_path: Path = DEFAULT_STORES_PATH) -> list[str]:
    """Run every registered store's reference-rows script against db_path.

    Returns the names (with their recorded reason) of stores that declare no
    script at all -- printed by the caller rather than silently doing
    nothing, so "this store has no reference-rows step" stays visible instead
    of looking identical to "it ran and there was nothing to do".
    """
    stores = load_stores(stores_path)
    skipped: list[str] = []
    for name, store in stores.items():
        if store.reference_rows is None:
            skipped.append(f"{name}: {store.reference_rows_reason}")
            continue
        cmd = [
            sys.executable,
            str(REPO_ROOT / store.reference_rows.script),
            "--db",
            str(db_path),
            *store.reference_rows.args,
        ]
        print(f"-- {name}: {' '.join(str(c) for c in cmd[1:])}")
        result = subprocess.run(cmd, cwd=REPO_ROOT)
        if result.returncode != 0:
            raise SystemExit(
                f"ensure_reference_rows: {name}'s reference-rows script failed "
                f"(exit {result.returncode}): {' '.join(str(c) for c in cmd)}"
            )
    return skipped


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Ensure every committed store's reference rows exist, driven by config/stores.yaml"
    )
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file")
    ap.add_argument("--stores", default=str(DEFAULT_STORES_PATH))
    args = ap.parse_args()

    skipped = ensure_all(Path(args.db), Path(args.stores))

    if skipped:
        print("\nSkipped (no reference-rows script declared in the registry):")
        for s in skipped:
            print(f"  {s}")


if __name__ == "__main__":
    main()
