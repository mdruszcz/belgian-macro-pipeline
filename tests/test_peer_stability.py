"""scripts/check_peer_stability.py against the real committed data.

Asserts the spec's proposed acceptance threshold (mean Jaccard overlap of
the national top-10 across a one-period shift >= 0.5,
docs/features/peer_model.md "Stability"). If the real measured overlap ever
falls below 0.5, THIS TEST MUST FAIL, not have its threshold lowered --
that is the whole point of writing the number down rather than tuning it
away (CLAUDE.md rule 13 in spirit).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import scripts.check_peer_stability as check_peer_stability  # noqa: E402
import scripts.export_peer_model as export_peer_model  # noqa: E402

pytestmark = pytest.mark.slow  # rebuilds the real model twice from committed CSVs

AREAS_CSV = REPO_ROOT / "config" / "geography" / "commune_area_km2.csv"


def test_mean_overlap_meets_the_proposed_threshold():
    if not AREAS_CSV.exists():
        pytest.skip(f"{AREAS_CSV} not built -- run scripts/derive_commune_area.py first")
    mean_overlap, least_stable = check_peer_stability.run_stability_check(
        export_peer_model.DEFAULT_HISTORY_DIR,
        export_peer_model.DEFAULT_HISTORY_CSV,
        export_peer_model.DEFAULT_AREAS,
        export_peer_model.DEFAULT_GEOGRAPHIES,
    )
    assert len(least_stable) == 10
    assert mean_overlap >= check_peer_stability.PROPOSED_THRESHOLD, (
        mean_overlap,
        least_stable,
    )


def test_least_stable_list_is_sorted_ascending_by_overlap():
    if not AREAS_CSV.exists():
        pytest.skip(f"{AREAS_CSV} not built -- run scripts/derive_commune_area.py first")
    _mean_overlap, least_stable = check_peer_stability.run_stability_check(
        export_peer_model.DEFAULT_HISTORY_DIR,
        export_peer_model.DEFAULT_HISTORY_CSV,
        export_peer_model.DEFAULT_AREAS,
        export_peer_model.DEFAULT_GEOGRAPHIES,
    )
    overlaps = [o for _nis, o in least_stable]
    assert overlaps == sorted(overlaps)
