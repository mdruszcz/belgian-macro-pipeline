"""The committed stores must be able to produce the exports on their own.

Data lives in two committed places: a small `data/belgian_macro.db`, and CSV
files for every store config/stores.yaml declares -- the hand-loaded sources
(docs/decisions/0002-split-committed-stores.md) and, since the cutover, ONEM
and WalStat (docs/decisions/0006-stores-split-by-volume.md). Observations live in the CSVs but their name and unit are
read from the database's `indicators` table, so there is exactly one metadata
path -- and the exporters REFUSE to guess when a referenced indicator is
missing, rather than inventing a label.

That refusal is correct, but it means the two stores can drift into a state
where the repo cannot export itself. It happened: the fiscal store was
committed while the four `FISCAL_*` reference rows were not, and
`manual_sources.yml` crashed on its first run. CI passed throughout, because
nothing in CI ran an exporter against the committed stores.

This closes that hole at PR time instead of at workflow time.
"""

import csv
import sqlite3
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
DB = REPO / "data" / "belgian_macro.db"

sys.path.insert(0, str(REPO))

from src.stores import DEFAULT_STORES_PATH, extra_csv_stores, load_stores  # noqa: E402

# Every store's CSV FILE(S) (config/stores.yaml), read from the one place
# that list is spelled out -- src/stores.py. Used to be a hand-maintained
# list of 3 of the (then) 6 manual stores, under a comment claiming it matched
# the workflows. in_db stores are included: their reference rows live in the
# committed database exactly like the extra_csv stores'.
#
# store.csv_paths(), NOT store.path: a one_csv_per_indicator store's `path`
# is a DIRECTORY (international pilot PR 1), which is never a file, so
# building this list from bare `.path` would make every test below silently
# `pytest.skip` the whole store on its `is_file()` guard -- exactly the kind
# of quiet gap this file exists to close. csv_paths() expands it to the
# individual per-indicator files that actually exist.
MANUAL_STORES = [
    path for _, s in sorted(load_stores(DEFAULT_STORES_PATH).items()) for path in s.csv_paths()
]
EXTRA_CSV_STORES = [s.path for s in extra_csv_stores(load_stores(DEFAULT_STORES_PATH))]


def _indicators_in_db() -> set[str]:
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        return {row[0] for row in conn.execute("SELECT indicator_id FROM indicators")}
    finally:
        conn.close()


@pytest.mark.parametrize("store", MANUAL_STORES, ids=lambda p: p.name)
def test_every_indicator_in_a_manual_store_has_a_reference_row(store):
    """The exact failure that broke manual_sources.yml.

    An observation in a committed CSV whose indicator has no row in the
    committed database is unexportable: the exporter raises rather than guess
    its name and unit.
    """
    if not store.is_file():
        pytest.skip(f"{store.name} is not committed")

    with store.open(encoding="utf-8", newline="") as fh:
        referenced = {row["indicator_id"] for row in csv.DictReader(fh)}

    missing = sorted(referenced - _indicators_in_db())
    assert not missing, (
        f"{store.name} references indicator(s) with no row in the committed "
        f"indicators table: {missing}. The exporters refuse to guess a name and "
        f"unit, so the repo cannot export itself. Fix with:\n"
        f"  python scripts/sync_fiscal_income.py --db data/belgian_macro.db --reference-rows-only"
    )


@pytest.mark.parametrize("store", MANUAL_STORES, ids=lambda p: p.name)
def test_every_geography_in_a_manual_store_exists(store):
    """The same class of drift, one column over. A geo_id with no row in
    `geographies` would fail the exporters' joins or silently drop rows
    depending on the path, which is worse than failing.
    """
    if not store.is_file():
        pytest.skip(f"{store.name} is not committed")

    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        known = {row[0] for row in conn.execute("SELECT geo_id FROM geographies")}
    finally:
        conn.close()

    with store.open(encoding="utf-8", newline="") as fh:
        referenced = {row["geo_id"] for row in csv.DictReader(fh)}

    missing = sorted(referenced - known)
    assert not missing, f"{store.name} references unknown geo_id(s): {missing[:10]}"


@pytest.mark.slow
def test_the_exporters_actually_run_against_the_committed_stores(tmp_path, working_db):
    """End-to-end, the guard the two tests above only approximate: run the
    real snapshot exporter the way both workflows do -- against the working
    database assembled from the committed stores. If this passes, the repo
    can export itself from a fresh clone with no network."""
    import subprocess
    import sys

    args = [
        sys.executable,
        str(REPO / "scripts" / "export_communes_csv.py"),
        "--db",
        str(working_db),
        "--out",
        str(tmp_path / "communes_export.csv"),
    ]
    for store in EXTRA_CSV_STORES:
        if store.is_file():
            args += ["--extra-observations", str(store)]

    result = subprocess.run(
        args, capture_output=True, text=True, encoding="utf-8", cwd=REPO, timeout=300
    )
    assert (
        result.returncode == 0
    ), f"the committed stores cannot produce communes_export.csv:\n{result.stderr}"
    assert (tmp_path / "communes_export.csv").is_file()
