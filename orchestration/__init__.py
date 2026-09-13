"""BelPulse's Dagster layer (docs/features/orchestration.md).

A thin layer over the existing scripts: every asset runs a command the
Makefile or daily_fetch.yml already runs. Local only for now; production runs
stay on GitHub Actions and are not visible in the local UI.

    dagster dev -m orchestration      (make dagster)
    python -m orchestration.daily     (make dagster-daily)
"""

from orchestration.definitions import build_defs

defs = build_defs()
