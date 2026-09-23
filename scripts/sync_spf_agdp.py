"""Load SPF Finances' AGDP real-estate leases and transactions datasets --
MUN_LEASES_NEW_HOUSING, MUN_LEASE_RENT_MEDIAN_HOUSING,
MUN_LEASE_CHARGES_MEDIAN_HOUSING, MUN_PROPERTY_SALES (Block Wave 4) -- plus,
Wave 5 lot A, two annual datasets: owner occupants (52.01.14) and real-estate
property dynamics (52.01.24), MUN_OWNER_OCCUPIERS, MUN_PARCELS_OWNED,
MUN_OWNERSHIP_DURATION_MEDIAN, MUN_OWNERSHIP_ROTATION_MEAN.
docs/features/spf_agdp.md.

DAILY, AUTOMATIC. Four ATOM feeds (one per dataset): the Wave 4 pair, each
version a fixed one-quarter zip, and the Wave 5 annual pair, each version a
fixed one-year (1-January snapshot) zip -- src/fetchers/spf_agdp.py's module
docstring covers feed discovery, the ranged zip read and the per-row
five-state parsing; this script is the layer that knows about the database:
incremental-fetch state, per-period geo resolution, and the write.

INCREMENTAL FETCH, NOT A FULL RE-READ EVERY DAY. Re-downloading and
re-parsing all ~40 versions of both datasets daily would mean the
Transactions dataset alone reading its ~205 MB zip (even ranged, a fresh zip
central-directory fetch) 40 times for one new quarter. A small JSON state
file, `data/spf_agdp_state.json`, committed next to the store (least
invasive per the handoff -- no schema change, no new table), records per
(dataset, quarter) the ATOM `length` and the ISO timestamp of the run that
loaded it. A run reads a version's zip IFF: (a) that quarter is not yet in
the state file for that dataset, OR (b) it IS in the state file but the
feed's current `length` differs from the recorded one (SPF Finances
republished that quarter under an unchanged href -- the only republish
signal ATOM exposes, per the handoff). Every other already-loaded version is
skipped without a network call. The one-time backfill is simply "every
version is new" on an empty/missing state file -- no separate code path.

FIVE STATES: src/fetchers/spf_agdp.py's `_row_to_observations` already
maps a CSV row to {value, status}; this script's only remaining state
decision is per-NIS geography resolution (below) -- it never re-derives a
value or a status.

GEOGRAPHY IS PERIOD-CORRECT, PER ROW'S OWN PERIOD -- resolve_geo(conn, nis,
period), the ordinary rule (CLAUDE.md rule 3), like
scripts/sync_population_movement.py and UNLIKE sync_bankruptcies.py /
sync_ipp_rate.py's pinned-period resolution: every commune alive in a given
quarter/year already has rows for every (RegistrationType x LessorType x
TakerType) / (TransactionType x ParcelNature) / (PersonType x
HousingRightType) / ParcelNature combination that period's file publishes,
so there is no zero-fill grid to build here (unlike bankruptcies.py, which
DOES need one because an absent commune-month there is a genuine unpublished
zero) -- a NIS code with no row in a live period's file is not expected and
is treated as a schema surprise by src/fetchers/spf_agdp.py's "zero matched
rows" guard, not handled here. For the annual datasets, resolve_geo(conn,
nis, "YYYY") resolves at YYYY-01-01, matching the ATOM feed's own 1-January
snapshot semantics exactly -- an ordinary, unremarkable resolve_geo() call,
never pinned to a different date.

An NIS code that fails resolve_geo() for its OWN quarter -- not covered by
any geography row at that date -- is collected and refuses the WHOLE run at
the end (sync_population_movement.py / sync_bankruptcies.py's own
`unresolved` pattern), never a partial load.

Usage:  python scripts/sync_spf_agdp.py --db data/belgian_macro.db
        python scripts/sync_spf_agdp.py --db X --reference-rows-only
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import ssl
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.fetchers.spf_agdp import (  # noqa: E402
    ATOM_URL_PATTERN,
    LEASES,
    OWNER_OCCUPANTS,
    PROPERTY_DYNAMICS,
    TRANSACTIONS,
    AgdpDatasetConfig,
    AgdpSource,
    AgdpVersion,
    open_municipality_csv,
    parse_atom_feed,
)
from src.geography.resolve import UnknownGeographyError, resolve_geo  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
STATE_PATH = Path(__file__).resolve().parents[1] / "data" / "spf_agdp_state.json"

#: dataset config key (state file / CLI) -> AgdpDatasetConfig. Wave 5 lot A
#: adds the two annual datasets (owner_occupants, property_dynamics) to the
#: quarterly pair loaded in Wave 4 -- same state file, same store, only the
#: config and `derive_period_bounds` frequency differ per dataset.
DATASETS: dict[str, AgdpDatasetConfig] = {
    "leases": LEASES,
    "transactions": TRANSACTIONS,
    "owner_occupants": OWNER_OCCUPANTS,
    "property_dynamics": PROPERTY_DYNAMICS,
}


def _load_state(path: Path) -> dict:
    """{dataset_key: {quarter: {"length": int, "loaded_at": iso}}}. A
    missing or empty file means "nothing loaded yet" -- the one-time
    backfill case -- never an error."""
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    return json.loads(text)


def _save_state(path: Path, state: dict) -> None:
    # Sorted keys -> deterministic byte-identical output for unchanged state
    # (CLAUDE.md rule 35), and a stable diff when a quarter is added.
    path.write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _fetch_atom_bytes(uuid: str) -> bytes:
    """Live GET for one dataset's ATOM feed. This machine's TLS interception
    means a live GET needs an unverified SSL context, exactly like every
    other live network call this pipeline makes from this environment."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    url = ATOM_URL_PATTERN.format(uuid=uuid)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
        return resp.read()


def _versions_to_load(versions: list[AgdpVersion], state_for_dataset: dict) -> list[AgdpVersion]:
    """Every version that is new, or whose recorded `length` no longer
    matches the feed's current `length` (a same-href republish) -- the
    incremental-fetch rule from the module docstring."""
    to_load = []
    for v in versions:
        recorded = state_for_dataset.get(v.quarter)
        if recorded is None or recorded.get("length") != v.length:
            to_load.append(v)
    return to_load


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict, source: dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES (?, ?, ?, 'spf_finances', ?, ?, ?, ?, 1)
        """,
        (
            source["source_id"],
            source["name"],
            source["agency"],
            source.get("base_url"),
            source.get("licence"),
            source.get("catalog_ref"),
            source.get("cadence"),
        ),
    )
    for indicator_id, is_additive, aggregation_method in (
        ("MUN_LEASES_NEW_HOUSING", 1, "sum"),
        ("MUN_LEASE_RENT_MEDIAN_HOUSING", 0, "not_applicable"),
        ("MUN_LEASE_CHARGES_MEDIAN_HOUSING", 0, "not_applicable"),
        ("MUN_PROPERTY_SALES", 1, "sum"),
        ("MUN_OWNER_OCCUPIERS", 1, "sum"),
        ("MUN_PARCELS_OWNED", 1, "sum"),
        ("MUN_OWNERSHIP_DURATION_MEDIAN", 0, "not_applicable"),
        ("MUN_OWNERSHIP_ROTATION_MEAN", 0, "not_applicable"),
    ):
        ind = indicator_configs[indicator_id]
        conn.execute(
            """
            INSERT INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'spf_finances', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 1)
            ON CONFLICT(indicator_id) DO UPDATE SET
                name_nl = excluded.name_nl,
                name_fr = excluded.name_fr,
                name_en = excluded.name_en,
                description_nl = excluded.description_nl,
                description_fr = excluded.description_fr,
                description_en = excluded.description_en,
                unit = excluded.unit,
                preferred_direction = excluded.preferred_direction
            """,
            (
                indicator_id,
                ind["name"]["nl"],
                ind["name"]["fr"],
                ind["name"]["en"],
                ind.get("description", {}).get("nl", ""),
                ind.get("description", {}).get("fr", ""),
                ind.get("description", {}).get("en", ""),
                ind["frequency"],
                ind["unit"],
                ind["preferred_direction"],
                aggregation_method,
                is_additive,
                f"config/indicators/{indicator_id}.yaml",
            ),
        )
    conn.commit()


def sync(
    db_path: Path,
    reference_rows_only: bool = False,
    *,
    state_path: Path | None = None,
    atom_bytes: dict[str, bytes] | None = None,
    version_bytes: dict[tuple[str, str], bytes] | None = None,
) -> tuple[int, int]:
    """Returns (rows_read, rows_written).

    `atom_bytes` ({"leases": xml, "transactions": xml}) and `version_bytes`
    ({(dataset_key, quarter): municipality_csv_bytes}) replace live ATOM
    discovery and the ranged zip download entirely -- what tests use to run
    the real parse/resolve/write/incremental-state logic against fixtures
    with no network. Production (main()) always discovers and downloads
    live. `state_path` defaults to STATE_PATH; tests override it to use an
    isolated tmp_path file.
    """
    state_path = state_path or STATE_PATH
    indicator_configs, sources = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, indicator_configs, sources["spf_finances"])
    if reference_rows_only:
        conn.close()
        return 0, 0

    state = _load_state(state_path)
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("spf_finances", "spf_agdp", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    all_rows: list[dict] = []
    versions_loaded: dict[str, list[str]] = {}
    # indicator_id -> derive_period_bounds() frequency ("A"/"Q"), so PASS 2's
    # write loop can derive the right period_start/period_end per row without
    # threading frequency through `all_rows` itself.
    indicator_frequency: dict[str, str] = {
        indicator_id: config.frequency
        for config in DATASETS.values()
        for indicator_id, _is_count in config.indicators.values()
    }

    for dataset_key, config in DATASETS.items():
        if atom_bytes is not None:
            if dataset_key not in atom_bytes:
                # Fixture mode only (production always fetches live for every
                # configured dataset): a test exercising only a subset of
                # DATASETS -- e.g. Wave 4's tests, which predate Wave 5's
                # owner_occupants/property_dynamics and only ever build a
                # {"leases": ..., "transactions": ...} atom_bytes dict -- skips
                # the untested dataset entirely rather than KeyError'ing,
                # exactly as if that feed had zero new/changed versions.
                versions_loaded[dataset_key] = []
                continue
            xml = atom_bytes[dataset_key]
        else:
            xml = _fetch_atom_bytes(config.uuid)
        versions = parse_atom_feed(xml, dataset_label=config.label, frequency=config.frequency)

        state_for_dataset = state.setdefault(dataset_key, {})
        to_load = _versions_to_load(versions, state_for_dataset)
        versions_loaded[dataset_key] = [v.quarter for v in to_load]

        source = AgdpSource(config)
        for v in to_load:
            if version_bytes is not None:
                csv_bytes = version_bytes[(dataset_key, v.quarter)]
            else:
                # The ranged zip read already IS the network fetch (it opens
                # the URL itself, unlike DataSource.fetch()'s plain GET) --
                # src/fetchers/base.py's fetch() is not reused here for that
                # reason, the same deviation
                # scripts/sync_population_movement.py's theme-page GET makes
                # for its own two-step discovery. The extracted Municipality
                # CSV bytes are cached the same way base.py's own
                # `_cache_raw` would, so the raw artefact on disk still
                # matches what `_parse` actually saw.
                csv_bytes = open_municipality_csv(
                    v.url, dataset_label=config.label, quarter=v.quarter
                )
                source._cache_raw(csv_bytes, f"{dataset_key}_{v.quarter}")
            rows = source._parse(csv_bytes, quarter=v.quarter)
            all_rows.extend(rows)
            state_for_dataset[v.quarter] = {"length": v.length, "loaded_at": now}

    if not any(versions_loaded.values()):
        # Nothing new in either feed -- a legitimate no-op day, not a failure
        # (unlike the other syncs' "zero rows total" guard, which fires on a
        # source that produced nothing AT ALL, not on "nothing changed").
        conn.execute(
            "UPDATE fetch_runs SET finished_at = ?, rows_read = 0, rows_written = 0, "
            "message = ? WHERE fetch_run_id = ?",
            (
                datetime.now(timezone.utc).isoformat(),
                "No new or changed AGDP versions in either feed.",
                fetch_run_id,
            ),
        )
        conn.commit()
        conn.close()
        print("No new or changed AGDP versions in either feed -- 0 rows read, 0 written.")
        return 0, 0

    # PASS 1: resolve every (indicator, nis, quarter) row against the commune
    # grid as of that ROW'S OWN quarter (module docstring; CLAUDE.md rule 3).
    # Collect every unresolved failure and refuse the whole run at the end
    # rather than loading a partial series.
    unresolved: list[tuple[str, str, str]] = []  # (indicator_id, quarter, nis)
    to_write: list[tuple[str, str, str, float | None, str]] = (
        []
    )  # (indicator_id, geo_id, quarter, value, status)
    rows_read = len(all_rows)

    for row in all_rows:
        indicator_id = row["indicator_id"]
        nis = row["geo_id"]
        quarter = row["period"]
        try:
            geo_id = resolve_geo(conn, nis, quarter)
        except UnknownGeographyError:
            unresolved.append((indicator_id, quarter, nis))
            continue
        to_write.append((indicator_id, geo_id, quarter, row["value"], row["status"]))

    if unresolved:
        raise SystemExit(
            f"::error::{len(unresolved)} (indicator, quarter, NIS) triple(s) resolved to no "
            f"geography row covering that NIS code as of its own quarter, e.g. "
            f"{unresolved[:5]}. Refusing to load a partial series (CLAUDE.md rule 13)."
        )

    rows_written = 0
    for indicator_id, geo_id, period, value, status in sorted(
        to_write, key=lambda t: (t[0], t[1], t[2])
    ):
        period_start, period_end = derive_period_bounds(period, indicator_frequency[indicator_id])
        rows_written += upsert_observation(
            conn,
            indicator_id=indicator_id,
            geo_id=geo_id,
            period=period,
            period_start=period_start,
            period_end=period_end,
            value=value,
            status=status,
            vintage=now,
            fetch_run_id=fetch_run_id,
        )

    conn.execute(
        "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
        "WHERE fetch_run_id = ?",
        (datetime.now(timezone.utc).isoformat(), rows_read, rows_written, fetch_run_id),
    )
    conn.commit()
    conn.close()

    _save_state(state_path, state)

    print(
        f"Loaded {sum(len(q) for q in versions_loaded.values())} new/changed version(s) "
        f"({versions_loaded}): read {rows_read} rows, wrote {rows_written} new vintage(s) "
        f"across {len({g for _, g, *_ in to_write})} communes."
    )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load SPF Finances' AGDP leases and transactions datasets (daily, automatic)"
    )
    ap.add_argument("--db", required=True)
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no network",
    )
    args = ap.parse_args()
    read, written = sync(Path(args.db), args.reference_rows_only)
    if args.reference_rows_only:
        print(
            "Reference rows ensured for: MUN_LEASES_NEW_HOUSING, "
            "MUN_LEASE_RENT_MEDIAN_HOUSING, MUN_LEASE_CHARGES_MEDIAN_HOUSING, "
            "MUN_PROPERTY_SALES, MUN_OWNER_OCCUPIERS, MUN_PARCELS_OWNED, "
            "MUN_OWNERSHIP_DURATION_MEDIAN, MUN_OWNERSHIP_ROTATION_MEAN"
        )
    else:
        print(f"Wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
