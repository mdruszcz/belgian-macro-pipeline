"""Freshness, derived from the config that already declares it.

A source's `fetch_window_days` (config/sources/*.yaml) is the editorial
statement that CI is expected to fetch it within that many days; the
validation rule `fetch_silence` reads it through validate_data._fetch_windows,
and so does this module. A source without one -- every hand-loaded store, and
the market data, which has no source config -- gets no freshness policy.

Per-indicator `max_age_days` is NOT turned into a policy: it is about the
reference period of the data, not about when an asset last ran, and it is
already enforced by the `staleness` check.

No automation condition is declared anywhere in this package: opening the
local UI never materialises anything by itself.
"""

from datetime import timedelta
from pathlib import Path

from dagster import FreshnessPolicy

from orchestration.commands import COMMANDS
from orchestration.paths import REPO_ROOT
from orchestration.scripts import import_script


def fetch_windows(sources_dir: Path = REPO_ROOT / "config" / "sources") -> dict[str, int]:
    return import_script("validate_data")._fetch_windows(sources_dir)


def window_days(name: str, windows: dict[str, int]) -> int | None:
    days = [windows[s] for s in COMMANDS[name].source_ids if s in windows]
    return min(days) if days else None


def freshness_policy(name: str, windows: dict[str, int]) -> FreshnessPolicy | None:
    days = window_days(name, windows)
    return FreshnessPolicy.time_window(fail_window=timedelta(days=days)) if days else None
