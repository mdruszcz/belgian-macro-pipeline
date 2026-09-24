"""The committed commune-flows store -- NOT the observations table (rule 18: no origin
dimension in that PK, and reshaping the canonical schema for this is out of bounds).
docs/features/commune_flows.md ("Storage"), docs/decisions/0013-commune-flow-shares-and-buckets.md.

One JSON file per dataset per year: data/flows/buyer_origin_<year>.json. Deterministic key
ordering and a trailing newline (CLAUDE.md rule 35, byte-identical rebuilds) -- `json.dumps`
with `sort_keys=True` plus destinations already sorted by geo_id
(src.flows.buyer_origin.compute_all_destinations) makes two runs over the same real input
produce the same bytes.

Values are serialised as strings (via `str(Decimal)`), never `float`, so a re-read and a
re-dump round-trip exactly -- json.dumps on a float can print differently across runs/platforms
for some values, which rule 35 does not tolerate.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from src.flows.buyer_origin import BUCKET_IDS, NO_PURCHASES_RECORDED, DestinationFlows

#: docs/features/commune_flows.md names the dataset; this constant is what both the store
#: filename and its own "dataset" field use, so the two can never drift apart.
DATASET_ID = "buyer_origin"

STORE_SCHEMA_VERSION = 1


def _dec_str(value: Decimal) -> str:
    return format(value, "f")


def destination_to_dict(d: DestinationFlows) -> dict:
    if d.state == NO_PURCHASES_RECORDED:
        return {
            "dest_geo_id": d.dest_geo_id,
            "dest_nis": d.dest_nis,
            "parcels_number": d.parcels_number,
            "state": NO_PURCHASES_RECORDED,
        }
    return {
        "dest_geo_id": d.dest_geo_id,
        "dest_nis": d.dest_nis,
        "parcels_number": d.parcels_number,
        "state": "final",
        "denominator": _dec_str(d.denominator),
        "coverage_pct": _dec_str(d.coverage),
        "top_origins": [
            {
                "nis": o.nis,
                "value": _dec_str(o.value),
                "share_pct": _dec_str(o.share),
            }
            for o in d.top_origins
        ],
        "buckets": {
            bucket_id: {
                "value": _dec_str(d.bucket_values[bucket_id]),
                "share_pct": _dec_str(d.bucket_shares[bucket_id]),
            }
            for bucket_id in BUCKET_IDS
        },
    }


def build_store(
    *,
    period: str,
    source_uuid: str,
    version_length: int,
    fetched_at: str,
    destinations: list[DestinationFlows],
) -> dict:
    """The full document for one year's committed store file. `destinations` must already be
    sorted by dest_geo_id (compute_all_destinations does this) -- this function does not
    re-sort, so a caller-introduced ordering bug shows up as a non-deterministic diff rather
    than being silently masked here."""
    return {
        "schema_version": STORE_SCHEMA_VERSION,
        "dataset": DATASET_ID,
        "source_uuid": source_uuid,
        "period": period,
        "provenance": {
            "version_length": version_length,
            "fetched_at": fetched_at,
        },
        "destinations": [destination_to_dict(d) for d in destinations],
    }


def store_path(data_dir: Path, period: str) -> Path:
    return data_dir / "flows" / f"{DATASET_ID}_{period}.json"


def write_store(path: Path, document: dict) -> None:
    """Atomic write (CLAUDE.md's builder-rule on atomic writes) -- write to a temp file in the
    same directory, then replace, so a crash mid-write never leaves a truncated store."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(document, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def read_store(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))
