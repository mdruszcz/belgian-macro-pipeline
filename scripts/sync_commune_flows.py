"""Load SPF Finances' commune-to-commune buyer-origin flows -- the committed flows store
(data/flows/buyer_origin_<year>.json), NOT the observations table (rule 18).
docs/features/commune_flows.md, docs/decisions/0013-commune-flow-shares-and-buckets.md.

PR 1 SCOPE: 52.01.21 (origin of buyers, natural persons), LATEST YEAR ONLY (maintainer's
decision, 2026-09-24). Owners' origin (52.01.16) and any earlier year are out of scope.

REUSES THE AGDP ATOM/RANGED-ZIP MACHINERY (src/fetchers/spf_agdp.py) exactly as every other
AGDP dataset does -- discovery and the ranged read are unchanged; only the per-row shape
differs, which src/flows/buyer_origin.py's own reader covers.

TWO MEMBERS PER ZIP READ, not one -- unlike every other spf_agdp.py dataset. This dataset's zip
also carries OriginTable.csv (35 KB, the origin code lookup), read alongside the Municipality
wide CSV via a second `zf.read()` on the same already-open ZipFile so only one ranged
connection is opened per version.

INCREMENTAL VIA THE ATOM VERSION, same pattern as sync_spf_agdp.py's own state file: a small
JSON file, data/flows_state.json, records the loaded period's ATOM `length`; a run re-reads
IFF the period is not yet recorded, or the feed's current length differs from what is
recorded (a same-href republish). LATEST YEAR ONLY means this compares only the single most
recent period the feed reports -- an EARLIER period appearing in the feed's own history is
never loaded (out of scope, not a bug).

GEOGRAPHY NEEDS A DATABASE CONNECTION (resolve_geo, both axes) but this script never WRITES to
any database -- geographies is read-only reference data already present in the committed
database, so `--db` here opens read-only (mode=ro) and the script has no write-path at all
into observations/fetch_runs. This is a deliberate departure from every other sync_*.py script
in this pipeline, which all write observations; this one writes only the JSON store on disk.

Usage:  python scripts/sync_commune_flows.py --db data/belgian_macro.db --out-dir data
"""

from __future__ import annotations

import argparse
import io
import json
import sqlite3
import ssl
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.fetchers.spf_agdp import (  # noqa: E402
    _MUNICIPALITY_MEMBER,
    _RANGE_BUFFER_SIZE,
    ATOM_URL_PATTERN,
    AgdpSchemaError,
    _RangedHttpFile,
    parse_atom_feed,
)
from src.flows.buyer_origin import (  # noqa: E402
    FlowSchemaError,
    compute_all_destinations,
    parse_municipality_rows,
    parse_origin_table,
)
from src.flows.store import build_store, store_path, write_store  # noqa: E402

BUYER_ORIGIN_UUID = "b90b50be-9dfc-11f0-99e9-00be432db085"
DATASET_LABEL = "SPF Finances buyer origin, natural persons (52.01.21)"

STATE_PATH = Path(__file__).resolve().parents[1] / "data" / "flows_state.json"
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def _fetch_atom_bytes(uuid: str) -> bytes:
    """Live GET for the ATOM feed. This machine's TLS interception means a live GET needs an
    unverified SSL context, exactly like every other live network call this pipeline makes
    from this environment (scripts/sync_spf_agdp.py's own _fetch_atom_bytes)."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    url = ATOM_URL_PATTERN.format(uuid=uuid)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
        return resp.read()


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return {}
    return json.loads(text)


def _save_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def open_municipality_and_origin_csv(
    url: str, *, dataset_label: str, quarter: str, warn=print
) -> tuple[bytes, bytes]:
    """(municipality_csv_bytes, origin_table_csv_bytes) read from the SAME ranged zip
    connection -- one `_RangedHttpFile` open, two `zf.read()` calls, so this dataset (unlike
    every other AGDP one, which reads a single member) still opens only one ranged HTTP
    session per version. Falls back to a whole-zip GET, with the same loud warning
    open_municipality_csv() gives, the moment ranging is unavailable."""
    fell_back = False

    def _on_fallback():
        nonlocal fell_back
        fell_back = True
        warn(
            f"::warning::{dataset_label} {quarter}: server did not honour a ranged request "
            f"for {url} (200 instead of 206) -- falling back to a whole-zip download."
        )

    ranged = _RangedHttpFile(url, on_fallback=_on_fallback)
    buffered = io.BufferedReader(ranged, buffer_size=_RANGE_BUFFER_SIZE)
    with zipfile.ZipFile(buffered) as zf:
        names = zf.namelist()
        members = [n for n in names if _MUNICIPALITY_MEMBER.match(n.split("/")[-1])]
        if len(members) != 1:
            raise AgdpSchemaError(
                f"{dataset_label} {quarter}: expected exactly one Municipality*.csv member, "
                f"found {members} among {names}. Refusing to guess (CLAUDE.md rule 13)."
            )
        origin_members = [n for n in names if n.split("/")[-1] == "OriginTable.csv"]
        if len(origin_members) != 1:
            raise AgdpSchemaError(
                f"{dataset_label} {quarter}: expected exactly one OriginTable.csv member, "
                f"found {origin_members} among {names}. Refusing to guess (CLAUDE.md rule 13)."
            )
        municipality_bytes = zf.read(members[0])
        origin_bytes = zf.read(origin_members[0])
    return municipality_bytes, origin_bytes


def sync(
    db_path: Path,
    *,
    out_dir: Path | None = None,
    state_path: Path | None = None,
    atom_bytes: bytes | None = None,
    version_bytes: dict[str, tuple[bytes, bytes]] | None = None,
) -> dict:
    """Returns a small summary dict: {"loaded": bool, "period": str | None,
    "destinations": int}. `atom_bytes` / `version_bytes` replace live network for tests, the
    same shape scripts/sync_spf_agdp.py uses. Production (main()) always fetches live.
    """
    out_dir = out_dir or DEFAULT_DATA_DIR
    state_path = state_path or STATE_PATH
    state = _load_state(state_path)
    now = datetime.now(timezone.utc).isoformat()

    xml = atom_bytes if atom_bytes is not None else _fetch_atom_bytes(BUYER_ORIGIN_UUID)
    versions = parse_atom_feed(xml, dataset_label=DATASET_LABEL, frequency="AY")
    latest = versions[-1]  # parse_atom_feed sorts oldest first; latest year only (in scope)

    recorded = state.get(latest.quarter)
    if recorded is not None and recorded.get("length") == latest.length:
        print(
            f"{DATASET_LABEL}: {latest.quarter} unchanged (length {latest.length}) -- "
            "nothing to do."
        )
        return {"loaded": False, "period": latest.quarter, "destinations": 0}

    if version_bytes is not None:
        municipality_bytes, origin_bytes = version_bytes[latest.quarter]
    else:
        municipality_bytes, origin_bytes = open_municipality_and_origin_csv(
            latest.url, dataset_label=DATASET_LABEL, quarter=latest.quarter
        )

    origin_table = parse_origin_table(origin_bytes)
    destinations_raw = parse_municipality_rows(municipality_bytes, origin_table=origin_table)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        try:
            results = compute_all_destinations(
                conn,
                destinations=destinations_raw,
                origin_table=origin_table,
                period=latest.quarter,
            )
        except FlowSchemaError as exc:
            raise SystemExit(f"::error::{DATASET_LABEL} {latest.quarter}: {exc}") from exc
    finally:
        conn.close()

    document = build_store(
        period=latest.quarter,
        source_uuid=BUYER_ORIGIN_UUID,
        version_length=latest.length,
        fetched_at=now,
        destinations=results,
    )
    path = store_path(out_dir, latest.quarter)
    write_store(path, document)

    state[latest.quarter] = {"length": latest.length, "loaded_at": now}
    _save_state(state_path, state)

    print(f"{DATASET_LABEL}: loaded {latest.quarter} ({len(results)} destinations) -> {path}")
    return {"loaded": True, "period": latest.quarter, "destinations": len(results)}


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load SPF Finances' commune-to-commune buyer-origin flows into the "
        "committed flows store (data/flows/buyer_origin_<year>.json)."
    )
    ap.add_argument("--db", required=True, help="Path to a database with `geographies` populated.")
    ap.add_argument(
        "--out-dir", default=None, help="Data directory root (default: repository's data/)."
    )
    args = ap.parse_args()
    out_dir = Path(args.out_dir) if args.out_dir else None
    summary = sync(Path(args.db), out_dir=out_dir)
    if not summary["loaded"]:
        print("No new or changed buyer-origin version -- 0 destinations written.")


if __name__ == "__main__":
    main()
