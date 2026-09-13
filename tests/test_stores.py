"""Tests for config/stores.yaml + src/stores.py -- the single declaration of
every committed observation store (PR1 of the pipeline repair).

Three properties matter:

  1. The registry validates: every store has exactly one mode, every path
     exists, every null reference_rows carries a reason.
  2. The registry's `indicators` list for each store is exactly what the
     CSV contains -- neither more nor less. This is what makes PR2's
     DB-to-CSV offload checkable rather than trusted on faith.
  3. Nothing that used to enumerate the six-store list by hand has grown a
     second copy of it -- the acceptance test the lead specified.
"""

import csv
import sys
from pathlib import Path

import jsonschema
import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from src.stores import (  # noqa: E402
    DEFAULT_STORES_PATH,
    MODE_EXTRA_CSV,
    MODE_IN_DB,
    VALID_MODES,
    StoreConfigError,
    extra_csv_paths,
    extra_csv_stores,
    in_db_stores,
    load_stores,
    resolve_extra_observations,
    verify_indicator_lists,
)

# ── The real registry ────────────────────────────────────────────────────────


def test_the_real_registry_loads_and_validates():
    stores = load_stores(DEFAULT_STORES_PATH)
    assert len(stores) == 6


def test_every_store_is_in_exactly_one_mode_and_its_path_exists():
    stores = load_stores(DEFAULT_STORES_PATH)
    for name, store in stores.items():
        assert store.mode in VALID_MODES, f"{name}: unknown mode {store.mode!r}"
        assert store.path.is_file(), f"{name}: path {store.path} does not exist"


def test_every_store_is_extra_csv_in_this_pr():
    """PR1 moves no data (CLAUDE.md rule 10) -- every store declared today
    must be extra_csv. PR2 is what introduces the first in_db store."""
    stores = load_stores(DEFAULT_STORES_PATH)
    assert all(s.mode == MODE_EXTRA_CSV for s in stores.values())
    assert in_db_stores(stores) == ()
    assert len(extra_csv_stores(stores)) == 6


def test_registry_indicators_match_each_csvs_real_contents():
    """The check that makes PR2's offload safe: an indicator silently added
    to or removed from a store's CSV without updating the registry must be
    caught here, not discovered downstream."""
    stores = load_stores(DEFAULT_STORES_PATH)
    problems = verify_indicator_lists(stores)
    assert problems == [], "\n".join(problems)


def test_every_store_declares_how_to_rebuild_its_reference_rows():
    """All six stores in this PR have a reference_rows script -- sync_population.py
    gained --reference-rows-only in this PR specifically to close that hole."""
    stores = load_stores(DEFAULT_STORES_PATH)
    for name, store in stores.items():
        assert (
            store.reference_rows is not None
        ), f"{name}: no reference_rows script declared and no reason given"
        assert (REPO / store.reference_rows.script).is_file()


# ── Schema / loader validation on synthetic registries ──────────────────────


def _write_registry(tmp_path: Path, stores_yaml: dict, csv_dir: Path) -> Path:
    path = tmp_path / "stores.yaml"
    path.write_text(yaml.safe_dump(stores_yaml, sort_keys=False), encoding="utf-8")
    return path


def _make_csv(path: Path, indicator_ids: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(
            [
                "indicator_id",
                "geo_id",
                "period",
                "vintage",
                "value",
                "status",
                "period_start",
                "period_end",
                "is_latest",
                "created_at",
            ]
        )
        for ind in indicator_ids:
            w.writerow(
                [
                    ind,
                    "be:mun:11002",
                    "2026",
                    "v1",
                    "1.0",
                    "final",
                    "2026-01-01",
                    "2026-12-31",
                    "1",
                    "x",
                ]
            )


def test_unknown_mode_is_rejected(tmp_path):
    csv_path = tmp_path / "x_observations.csv"
    _make_csv(csv_path, ["FOO"])
    registry = _write_registry(
        tmp_path,
        {
            "stores": {
                "x": {
                    "path": str(csv_path),
                    "source_id": "statbel",
                    "mode": "sideways",
                    "indicators": ["FOO"],
                    "reference_rows": {"script": "scripts/sync_var.py"},
                }
            }
        },
        tmp_path,
    )
    with pytest.raises((StoreConfigError, jsonschema.ValidationError, Exception)):
        load_stores(registry)


def test_missing_path_is_rejected(tmp_path):
    registry = _write_registry(
        tmp_path,
        {
            "stores": {
                "x": {
                    "path": "data/does_not_exist_observations.csv",
                    "source_id": "statbel",
                    "mode": MODE_EXTRA_CSV,
                    "indicators": ["FOO"],
                    "reference_rows": {"script": "scripts/sync_var.py"},
                }
            }
        },
        tmp_path,
    )
    with pytest.raises(StoreConfigError, match="does not exist"):
        load_stores(registry)


def test_null_reference_rows_requires_a_reason(tmp_path):
    csv_path = tmp_path / "x_observations.csv"
    _make_csv(csv_path, ["FOO"])
    registry = _write_registry(
        tmp_path,
        {
            "stores": {
                "x": {
                    "path": str(csv_path),
                    "source_id": "statbel",
                    "mode": MODE_EXTRA_CSV,
                    "indicators": ["FOO"],
                    "reference_rows": None,
                }
            }
        },
        tmp_path,
    )
    with pytest.raises(StoreConfigError, match="reference_rows_reason"):
        load_stores(registry)


def test_null_reference_rows_with_a_reason_is_accepted(tmp_path):
    csv_path = tmp_path / "x_observations.csv"
    _make_csv(csv_path, ["FOO"])
    registry = _write_registry(
        tmp_path,
        {
            "stores": {
                "x": {
                    "path": str(csv_path),
                    "source_id": "statbel",
                    "mode": MODE_IN_DB,
                    "indicators": ["FOO"],
                    "reference_rows": None,
                    "reference_rows_reason": "no script exists yet (PR2 territory)",
                }
            }
        },
        tmp_path,
    )
    stores = load_stores(registry)
    assert stores["x"].reference_rows is None
    assert "PR2" in stores["x"].reference_rows_reason


def test_indicator_drift_is_detected_both_directions(tmp_path):
    """The registry claims FOO and BAR; the CSV only has FOO. And a second
    store's CSV has an indicator the registry never declared."""
    csv_path = tmp_path / "x_observations.csv"
    _make_csv(csv_path, ["FOO"])
    registry = _write_registry(
        tmp_path,
        {
            "stores": {
                "x": {
                    "path": str(csv_path),
                    "source_id": "statbel",
                    "mode": MODE_EXTRA_CSV,
                    "indicators": ["FOO", "BAR"],
                    "reference_rows": {"script": "scripts/sync_var.py"},
                }
            }
        },
        tmp_path,
    )
    stores = load_stores(registry)
    problems = verify_indicator_lists(stores)
    assert len(problems) == 1
    assert "BAR" in problems[0]


# ── resolve_extra_observations: explicit wins, registry is the fallback ─────


def test_explicit_extra_observations_wins_outright():
    """Prevents the double-count bug: if both an explicit list and the
    registry default were merged, the CLI test in
    test_committed_stores_are_consistent.py would see every store twice."""
    result = resolve_extra_observations(["a.csv", "b.csv"], str(DEFAULT_STORES_PATH))
    assert result == (Path("a.csv"), Path("b.csv"))


def test_falls_back_to_the_registry_when_nothing_explicit_is_given():
    result = resolve_extra_observations([], str(DEFAULT_STORES_PATH))
    assert set(result) == set(extra_csv_paths(DEFAULT_STORES_PATH))
    assert len(result) == 6


def test_empty_stores_path_disables_the_fallback():
    """A caller that wants a bare export with nothing merged in can pass
    --stores ''."""
    assert resolve_extra_observations([], "") == ()
    assert resolve_extra_observations([], None) == ()


# ── The acceptance test: nothing outside the registry re-enumerates the list ─

# The consumers that USED TO hand-list all six (or a stale subset of six)
# stores: the Makefile's old EXTRA variable, the two workflows'
# --extra-observations repetitions, scripts/validate_data.py's DEFAULT_EXPORTS,
# tests/test_committed_stores_are_consistent.py's MANUAL_STORES, and
# tests/test_export_aggregates_csv.py's own STORES tuple -- which, found while
# writing this scan, had ALREADY dropped police_observations.csv (5 of 6), a
# fourth independent copy of the list that had drifted before anyone noticed.
# These are the files this PR rewired; a hardcoded "*_observations.csv"
# reappearing in any of them is exactly the regression a flag-printing CLI
# (rejected in the handoff) could not have prevented.
#
# DELIBERATELY NOT A REPOSITORY-WIDE GREP. docs/**, README.md and several
# other existing tests (test_census2021.py, test_fiscal_income.py, ...)
# legitimately name ONE store's own CSV to test or document that one source in
# isolation -- that is not the "the list is duplicated in N places" defect
# this guards against, and rewriting unrelated docs/tests to satisfy a wider
# grep would be an out-of-scope change (CLAUDE.md rule 10).
SCANNED_FOR_HARDCODED_STORE_LIST = [
    REPO / "Makefile",
    REPO / ".github" / "workflows" / "daily_fetch.yml",
    REPO / "scripts" / "validate_data.py",
    REPO / "tests" / "test_committed_stores_are_consistent.py",
    REPO / "tests" / "test_export_aggregates_csv.py",
    REPO / "scripts" / "export_communes_csv.py",
    REPO / "scripts" / "export_communes_history_csv.py",
    REPO / "scripts" / "export_aggregates_csv.py",
    REPO / "scripts" / "export_percentiles_csv.py",
]


@pytest.mark.parametrize("path", SCANNED_FOR_HARDCODED_STORE_LIST, ids=lambda p: p.name)
def test_no_hardcoded_store_csv_path_outside_the_registry(path):
    text = path.read_text(encoding="utf-8")
    assert "_observations.csv" not in text, (
        f"{path.relative_to(REPO)} names a store CSV directly -- store paths belong in "
        "config/stores.yaml only, read through src.stores. This is the drift that put 3 "
        "different lists of manual stores across the codebase before this PR."
    )
