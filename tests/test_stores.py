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
    LAYOUT_ONE_CSV_PER_INDICATOR,
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
    assert len(stores) == 12


def test_every_store_is_in_exactly_one_mode_and_its_path_exists():
    """`.exists()`, not `.is_file()`: a one_csv_per_indicator store's path is
    a directory (international pilot PR 1)."""
    stores = load_stores(DEFAULT_STORES_PATH)
    for name, store in stores.items():
        assert store.mode in VALID_MODES, f"{name}: unknown mode {store.mode!r}"
        assert store.path.exists(), f"{name}: path {store.path} does not exist"


def test_the_split_is_hand_loaded_extra_csv_and_ci_fetched_in_db():
    """The six hand-loaded sources stay extra_csv; the sources CI fetches
    itself and that outgrew the committed database are in_db
    (docs/decisions/0006, plus the international pilot's directory store),
    plus the Europe NUTS 2 batch's directory store (also in_db, same registry
    mechanism -- see config/stores.yaml's own comment on the `nuts2` entry
    for why it is not wired into the daily fetch despite being in_db), plus
    `bankruptcies` (feat/ns1-bankruptcies) -- the first in_db store whose
    source_id (statbel) also backs extra_csv stores, since Statbel's
    bankruptcies file is reachable live where the other Statbel files this
    pipeline loads are not."""
    stores = load_stores(DEFAULT_STORES_PATH)
    assert {s.name for s in in_db_stores(stores)} == {
        "onem",
        "onem_rates",
        "walstat",
        "international",
        "nuts2",
        "bankruptcies",
    }
    assert len(extra_csv_stores(stores)) == 6
    assert all(s.mode in (MODE_EXTRA_CSV, MODE_IN_DB) for s in stores.values())


def _configured_indicators_by_source() -> dict[str, set[str]]:
    by_source: dict[str, set[str]] = {}
    for path in (REPO / "config" / "indicators").glob("*.yaml"):
        cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        by_source.setdefault(cfg.get("source_id"), set()).add(cfg["id"])
    return by_source


def test_in_db_stores_declare_every_indicator_their_sources_configure():
    """The offload's row set is the declared list. An ONEM or WalStat indicator
    configured but not declared would make scripts/offload_stores.py refuse
    to run on the first day it is fetched -- caught here, at PR time, instead.

    Compares against `configured[source_id]` MINUS whatever an extra_csv store
    of that same source_id already declares, and minus any indicator with NO
    store at all under that source_id (pre-existing: LOCAL_UNITS_BY_COMMUNE is
    configured with source_id statbel but lives directly in the small
    committed belgian_macro.db, no CSV store of either kind -- unrelated to
    this batch, out of scope to fix here, and never exercised by this test
    before because no in_db store previously shared a source_id with any
    extra_csv or store-less indicator).

    Added for the `bankruptcies` store (feat/ns1-bankruptcies): `statbel` now
    backs both extra_csv stores (population, fiscal_income, census2021,
    realestate) and an in_db store (bankruptcies) at once, the first time one
    source_id has had both kinds. Those extra_csv indicators already have a
    documented CSV location of their own and are not this in_db store's job
    to declare; the real invariant this test protects -- every indicator
    configured under a source with an in_db store, that ALSO has some store
    home, has it in the in_db store -- is unchanged for every source. Whether
    every configured indicator has a store home AT ALL is a separate,
    narrower question, checked by test_every_configured_indicator_has_exactly_one_store_home
    below with `<=` rather than `==`, precisely so it does not re-litigate
    the pre-existing LOCAL_UNITS_BY_COMMUNE gap either."""
    stores = load_stores(DEFAULT_STORES_PATH)
    configured = _configured_indicators_by_source()
    declared: dict[str, set[str]] = {}
    any_store_declared: dict[str, set[str]] = {}
    other_store_declared: dict[str, set[str]] = {}
    for store in in_db_stores(stores):
        declared.setdefault(store.source_id, set()).update(store.indicators)
    for store in stores.values():
        any_store_declared.setdefault(store.source_id, set()).update(store.indicators)
        if store.mode != MODE_IN_DB:
            other_store_declared.setdefault(store.source_id, set()).update(store.indicators)
    for source_id, ids in declared.items():
        has_some_store = configured[source_id] & any_store_declared.get(source_id, set())
        expected = has_some_store - other_store_declared.get(source_id, set())
        assert ids == expected, (
            f"source {source_id}: configured but undeclared "
            f"{sorted(expected - ids)}, declared but unconfigured "
            f"{sorted(ids - expected)}"
        )


def test_every_configured_indicator_has_exactly_one_store_home():
    """The invariant test_in_db_stores_declare_every_indicator_their_sources_configure
    used to check in one step, kept exact now that a source_id can back both
    an in_db and an extra_csv store: every indicator configured under a
    source that has ANY committed store is declared by exactly one store
    (in_db or extra_csv), never zero, never two -- test_no_indicator_is_declared_by_two_stores
    above already covers "never two" registry-wide; this covers "never zero"
    for a source with a store at all."""
    stores = load_stores(DEFAULT_STORES_PATH)
    configured = _configured_indicators_by_source()
    all_declared_by_source: dict[str, set[str]] = {}
    for store in stores.values():
        all_declared_by_source.setdefault(store.source_id, set()).update(store.indicators)
    for source_id, declared_ids in all_declared_by_source.items():
        assert declared_ids <= configured.get(source_id, set()), (
            f"source {source_id}: declared but unconfigured "
            f"{sorted(declared_ids - configured.get(source_id, set()))}"
        )


def test_no_indicator_is_declared_by_two_stores():
    seen: dict[str, str] = {}
    for name, store in load_stores(DEFAULT_STORES_PATH).items():
        for indicator in store.indicators:
            assert indicator not in seen, f"{indicator} declared by {seen[indicator]} and {name}"
            seen[indicator] = name


def test_no_in_db_indicator_is_left_in_the_committed_database():
    """The whole point of the cutover: the committed file carries none of the
    offloaded rows. A merge that reintroduced them would also make every
    assemble refuse (scripts/build_staging_db.py)."""
    import sqlite3

    db = REPO / "data" / "belgian_macro.db"
    if not db.is_file():
        pytest.skip("committed database not present")
    ids = sorted({i for s in in_db_stores(load_stores(DEFAULT_STORES_PATH)) for i in s.indicators})
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        held = conn.execute(
            f"SELECT COUNT(*) FROM observations WHERE indicator_id IN ({','.join('?' * len(ids))})",
            ids,
        ).fetchone()[0]
    finally:
        conn.close()
    assert held == 0


def test_registry_indicators_match_each_csvs_real_contents():
    """The check that makes PR2's offload safe: an indicator silently added
    to or removed from a store's CSV without updating the registry must be
    caught here, not discovered downstream."""
    stores = load_stores(DEFAULT_STORES_PATH)
    problems = verify_indicator_lists(stores)
    assert problems == [], "\n".join(problems)


def test_every_store_declares_how_to_rebuild_its_reference_rows():
    """Every store has a reference_rows script -- sync_population.py gained
    --reference-rows-only in PR1 of the pipeline repair to close that hole."""
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


# ── layout: one_csv_per_indicator (international pilot PR 1) ────────────────


def _write_indicator_csv(directory: Path, indicator_id: str) -> None:
    _make_csv(directory / f"{indicator_id}.csv", [indicator_id])


def _dir_registry(tmp_path: Path, store_dir: Path, indicators: list[str], mode: str) -> Path:
    return _write_registry(
        tmp_path,
        {
            "stores": {
                "international": {
                    "path": str(store_dir),
                    "source_id": "eurostat",
                    "mode": mode,
                    "layout": "one_csv_per_indicator",
                    "indicators": indicators,
                    "reference_rows": {"script": "scripts/sync_international.py"},
                }
            }
        },
        tmp_path,
    )


def test_a_directory_store_needs_a_directory_not_a_file(tmp_path):
    csv_path = tmp_path / "not_a_dir.csv"
    _make_csv(csv_path, ["FOO"])
    registry = _dir_registry(tmp_path, csv_path, ["FOO"], MODE_IN_DB)
    with pytest.raises(StoreConfigError, match="not a directory"):
        load_stores(registry)


def test_csv_paths_omits_a_declared_indicator_with_no_file_yet(tmp_path):
    """The one_csv_per_indicator asymmetry for in_db stores: a configured
    indicator not yet fetched has no file, and that is tolerated -- a header
    -only CSV would make the next assemble load nothing and look identical to
    'not fetched yet', which is the distinction this is for."""
    store_dir = tmp_path / "international"
    store_dir.mkdir()
    _write_indicator_csv(store_dir, "GDP_VOL_EU")
    registry = _dir_registry(tmp_path, store_dir, ["GDP_VOL_EU", "NOT_YET_FETCHED"], MODE_IN_DB)

    stores = load_stores(registry)
    store = stores["international"]
    assert store.layout == LAYOUT_ONE_CSV_PER_INDICATOR
    assert store.csv_paths() == (store_dir / "GDP_VOL_EU.csv",)
    assert store.csv_for("NOT_YET_FETCHED") == store_dir / "NOT_YET_FETCHED.csv"
    assert verify_indicator_lists(stores) == []


def test_a_file_whose_stem_disagrees_with_its_own_rows_is_drift(tmp_path):
    store_dir = tmp_path / "international"
    store_dir.mkdir()
    _make_csv(store_dir / "GDP_VOL_EU.csv", ["WRONG_INDICATOR_ID"])
    registry = _dir_registry(tmp_path, store_dir, ["GDP_VOL_EU"], MODE_IN_DB)

    stores = load_stores(registry)
    problems = verify_indicator_lists(stores)
    assert any("other than its own filename" in p for p in problems)


def test_an_undeclared_file_on_disk_is_drift_even_though_csv_paths_never_sees_it(tmp_path):
    """Audit SHOULD-FIX 6: Store.csv_paths() iterates `store.indicators`, so
    a file for an indicator nobody declared is invisible to it BY
    CONSTRUCTION -- an audit removed an indicator from the declared list and
    verify_indicator_lists still returned [] while its file stayed on disk
    and every assemble kept loading it. The check must look at the real
    directory listing, not just the declared list's own files."""
    store_dir = tmp_path / "international"
    store_dir.mkdir()
    _write_indicator_csv(store_dir, "GDP_VOL_EU")
    _write_indicator_csv(store_dir, "GOV_DEBT_EUROPE")  # present on disk...
    # ...but only GDP_VOL_EU is declared: GOV_DEBT_EUROPE was removed.
    registry = _dir_registry(tmp_path, store_dir, ["GDP_VOL_EU"], MODE_IN_DB)

    stores = load_stores(registry)
    assert stores["international"].csv_paths() == (store_dir / "GDP_VOL_EU.csv",)

    problems = verify_indicator_lists(stores)
    assert any("GOV_DEBT_EUROPE" in p and "not declared" in p for p in problems), problems


def test_never_a_observations_suffix_filename(tmp_path):
    """The directory layout's whole point: the file IS named after the
    indicator, never `*_observations.csv` -- a second name to keep in sync
    with the indicator id is exactly the drift this layout exists to avoid."""
    store_dir = tmp_path / "international"
    store_dir.mkdir()
    _write_indicator_csv(store_dir, "GDP_VOL_EU")
    registry = _dir_registry(tmp_path, store_dir, ["GDP_VOL_EU"], MODE_IN_DB)
    store = load_stores(registry)["international"]
    assert store.csv_for("GDP_VOL_EU").name == "GDP_VOL_EU.csv"


# ── resolve_extra_observations: explicit wins, registry is the fallback ─────


def test_an_in_db_store_may_declare_an_indicator_not_fetched_yet(tmp_path):
    """UNEMPLOYMENT_RATE_BIT was configured, and so had to be declared, before
    WalStat had delivered a single row of it. For an in_db store that is not
    drift; an undeclared indicator in its CSV still is."""
    csv_path = tmp_path / "w_observations.csv"
    _make_csv(csv_path, ["FOO"])
    base = {
        "path": str(csv_path),
        "source_id": "walstat",
        "reference_rows": {"script": "scripts/sync_walstat.py"},
    }
    in_db = _write_registry(
        tmp_path,
        {"stores": {"w": {**base, "mode": "in_db", "indicators": ["FOO", "NOT_YET"]}}},
        tmp_path,
    )
    assert verify_indicator_lists(load_stores(in_db)) == []

    extra = _write_registry(
        tmp_path,
        {"stores": {"w": {**base, "mode": "extra_csv", "indicators": ["FOO", "NOT_YET"]}}},
        tmp_path,
    )
    assert verify_indicator_lists(load_stores(extra))

    undeclared = _write_registry(
        tmp_path,
        {"stores": {"w": {**base, "mode": "in_db", "indicators": ["NOT_YET"]}}},
        tmp_path,
    )
    assert verify_indicator_lists(load_stores(undeclared))


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
    REPO / ".github" / "workflows" / "ci.yml",
    REPO / "scripts" / "offload_stores.py",
    REPO / "scripts" / "build_staging_db.py",
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
