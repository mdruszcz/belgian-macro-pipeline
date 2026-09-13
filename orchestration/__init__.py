"""BelPulse's Dagster layer (docs/features/orchestration.md).

A thin layer over the existing scripts: every asset runs a command the
Makefile runs, or one daily_fetch.yml ran itself before step 2. Production runs
it too, inside the GitHub Actions runner (python -m orchestration.daily); those
runs are not visible in the local UI.

    dagster dev -m orchestration      (make dagster)
    python -m orchestration.daily     (make dagster-daily)
"""

from orchestration.definitions import build_defs

defs = build_defs()
