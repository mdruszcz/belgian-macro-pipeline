"""Publish the committed buyer-origin flows store as one small static payload per
commune -- docs/features/commune_flows.md ("Storage", "Where it plugs in"),
docs/decisions/0013-commune-flow-shares-and-buckets.md (the formulas this file only
reshapes, never recomputes).

READS data/flows/buyer_origin_<year>.json (src/flows/store.py, PR 1 -- the committed
flows store, NOT the observations table, rule 18) plus
public/data/metadata/geographies.json (already published by export_site_payloads.py,
which is why this script runs after site_payloads in the Makefile/orchestration
order) for trilingual commune names -- rule 36, no hand-typed name anywhere.

WRITES public/data/flows/buyer_origin/<nis>.json, one destination per file, fetched
on demand by the commune page's new block (rule 30: static, GitHub-Pages compatible).
NEVER committed by this script's own run -- the daily run regenerates public/data
and nothing here calls git.

BYTE-IDENTICAL REBUILD (rule 35). The source store already carries deterministic key
ordering and Decimal-as-string values (src/flows/store.py); this script reshapes those
same fields without re-deriving anything, adds resolved names read verbatim from
geographies.json, and writes with sort_keys + a fixed separator + a trailing newline +
LF-only line endings, exactly like src/flows/store.py's own write_store. The one field
that legitimately differs between two runs over the same input is the store's own
`fetched_at` provenance timestamp, carried through unchanged (never re-stamped here).

NO SHARE IS RECOMPUTED HERE. Every number in the output payload is copied verbatim
from the store (as a string, still Decimal-precise) or is a resolved name/label --
this script never sums, divides or rounds a flow value itself (ADR 0013's formulas
live in src/flows/buyer_origin.py alone).

Usage:  python scripts/export_commune_flows.py
            [--flows-dir data/flows]
            [--geographies public/data/metadata/geographies.json]
            [--out-dir public/data/flows/buyer_origin]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FLOWS_DIR = REPO_ROOT / "data" / "flows"
DEFAULT_GEOGRAPHIES = REPO_ROOT / "public" / "data" / "metadata" / "geographies.json"
DEFAULT_OUT_DIR = REPO_ROOT / "public" / "data" / "flows" / "buyer_origin"

DATASET_ID = "buyer_origin"
SOURCE_ID = "spf_finances"

_STORE_NAME_RE = re.compile(r"^buyer_origin_(\d{4})\.json$")

NO_PURCHASES_RECORDED = "no_purchases_recorded"

BUCKET_IDS = (
    "same_commune",
    "rest_of_arrondissement",
    "rest_of_region",
    "other_regions",
    "abroad",
    "origin_unknown",
)


class FlowExportError(ValueError):
    """The committed store or the geography payload is not the shape this exporter
    requires -- refused rather than guessed (CLAUDE.md rule 13)."""


def latest_store_path(flows_dir: Path) -> Path:
    """The single most recent data/flows/buyer_origin_<year>.json -- PR 1 writes one
    file per year it has loaded, latest-year-only for now (ADR 0013 decision 9), but
    this exporter does not assume there is exactly one file: it picks the highest
    year present, so an eventual second year (PR 3 / a later backfill) does not need
    this script rewritten."""
    candidates: list[tuple[int, Path]] = []
    for path in flows_dir.glob("buyer_origin_*.json"):
        match = _STORE_NAME_RE.match(path.name)
        if match:
            candidates.append((int(match.group(1)), path))
    if not candidates:
        raise FlowExportError(
            f"no data/flows/buyer_origin_<year>.json store found under {flows_dir} -- "
            "run scripts/sync_commune_flows.py first (CLAUDE.md rule 13)."
        )
    candidates.sort(key=lambda t: t[0])
    return candidates[-1][1]


def load_store(path: Path) -> dict:
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("dataset") != DATASET_ID:
        raise FlowExportError(
            f"{path}: dataset {document.get('dataset')!r}, expected {DATASET_ID!r}. "
            "Refusing to guess (CLAUDE.md rule 13)."
        )
    if not document.get("period"):
        raise FlowExportError(f"{path}: no `period`.")
    if not isinstance(document.get("destinations"), list) or not document["destinations"]:
        raise FlowExportError(f"{path}: no destinations to publish.")
    return document


def load_geography_names(path: Path) -> dict[str, dict[str, str]]:
    """{geo_id: {en, fr, nl}} for every municipality row -- read verbatim, never
    typed by hand (rule 36). Missing or malformed rows are refused, not skipped:
    a name the block cannot show is a schema surprise for this dataset, since
    every origin/destination NIS in the store already resolved through
    resolve_geo() in PR 1."""
    if not path.is_file():
        raise FlowExportError(f"no geography payload at {path} -- run `make exports` first.")
    document = json.loads(path.read_text(encoding="utf-8"))
    names: dict[str, dict[str, str]] = {}
    for row in document.get("geographies", []):
        if row.get("level") != "municipality":
            continue
        nis = row.get("nis_code")
        name = row.get("name") or {}
        if not nis or not all(name.get(lang) for lang in ("en", "fr", "nl")):
            raise FlowExportError(
                f"{path}: municipality row {row!r} is missing a NIS code or a trilingual "
                "name (CLAUDE.md rule 7). Refusing to guess."
            )
        names[nis] = {"en": name["en"], "fr": name["fr"], "nl": name["nl"]}
    if not names:
        raise FlowExportError(f"{path}: zero municipality rows.")
    return names


def _name_for(nis: str, names: dict[str, dict[str, str]]) -> dict[str, str]:
    resolved = names.get(nis)
    if resolved is None:
        raise FlowExportError(
            f"origin/destination NIS {nis!r} has no entry in geographies.json, but the "
            "flows store already resolved it through resolve_geo() in PR 1 -- the two "
            "payloads have drifted (CLAUDE.md rule 13)."
        )
    return resolved


def build_commune_payload(
    destination: dict, *, period: str, source_uuid: str, fetched_at: str, names: dict
) -> dict:
    """One commune's public payload, reshaping the store's own fields (never
    recomputing a share, a bucket or the denominator -- ADR 0013's formulas stay in
    src/flows/buyer_origin.py alone)."""
    dest_nis = destination["dest_nis"]
    base = {
        "schema_version": 1,
        "dataset": DATASET_ID,
        "source_id": SOURCE_ID,
        "source_uuid": source_uuid,
        "period": period,
        "fetched_at": fetched_at,
        "dest_nis": dest_nis,
        "dest_geo_id": destination["dest_geo_id"],
        "dest_name": _name_for(dest_nis, names),
        "parcels_number": destination["parcels_number"],
    }

    if destination.get("state") == NO_PURCHASES_RECORDED:
        base["state"] = NO_PURCHASES_RECORDED
        return base

    if destination.get("state") != "final":
        raise FlowExportError(
            f"destination {dest_nis!r}: unrecognised state {destination.get('state')!r}. "
            "Refusing to guess (CLAUDE.md rule 13)."
        )

    missing = [
        key
        for key in ("denominator", "coverage_pct", "top_origins", "buckets")
        if key not in destination
    ]
    if missing:
        raise FlowExportError(f"destination {dest_nis!r}: missing field(s) {missing}.")

    buckets = destination["buckets"]
    if set(buckets) != set(BUCKET_IDS):
        raise FlowExportError(
            f"destination {dest_nis!r}: bucket ids {sorted(buckets)} do not match the six "
            f"ADR 0013 buckets {sorted(BUCKET_IDS)}. Refusing to guess (CLAUDE.md rule 13)."
        )

    base["state"] = "final"
    base["denominator"] = destination["denominator"]
    base["coverage_pct"] = destination["coverage_pct"]
    base["top_origins"] = [
        {
            "nis": origin["nis"],
            "name": _name_for(origin["nis"], names),
            "value": origin["value"],
            "share_pct": origin["share_pct"],
        }
        for origin in destination["top_origins"]
    ]
    base["buckets"] = {
        bucket_id: {
            "value": buckets[bucket_id]["value"],
            "share_pct": buckets[bucket_id]["share_pct"],
        }
        for bucket_id in BUCKET_IDS
    }
    return base


def build_all(document: dict, names: dict[str, dict[str, str]]) -> dict[str, dict]:
    """{nis: payload} for every destination in the store, sorted iteration order
    (the store's own destinations are already sorted by dest_geo_id -- rule 35)."""
    period = document["period"]
    source_uuid = document.get("source_uuid", "")
    fetched_at = document.get("provenance", {}).get("fetched_at", "")
    out: dict[str, dict] = {}
    for destination in document["destinations"]:
        nis = destination["dest_nis"]
        if nis in out:
            raise FlowExportError(f"destination NIS {nis!r} appears twice in the store.")
        out[nis] = build_commune_payload(
            destination,
            period=period,
            source_uuid=source_uuid,
            fetched_at=fetched_at,
            names=names,
        )
    return out


def write_payload(path: Path, payload: dict) -> None:
    """Atomic, deterministic write -- same discipline as src/flows/store.py's
    write_store: sort_keys, a trailing newline, LF endings so the same run on
    Windows and on a GitHub Actions (Linux) runner produces byte-identical bytes
    (rule 35)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    tmp.replace(path)


def export(flows_dir: Path, geographies_path: Path, out_dir: Path) -> int:
    store_path = latest_store_path(flows_dir)
    document = load_store(store_path)
    names = load_geography_names(geographies_path)
    payloads = build_all(document, names)

    out_dir.mkdir(parents=True, exist_ok=True)
    # Remove any commune file left over from a year this store no longer serves
    # (e.g. a merged commune's old NIS), so a stale file never survives a rebuild --
    # part of what "byte-identical rebuild" means for a directory of many files.
    keep = {f"{nis}.json" for nis in payloads}
    for existing in out_dir.glob("*.json"):
        if existing.name not in keep:
            existing.unlink()

    for nis, payload in sorted(payloads.items()):
        write_payload(out_dir / f"{nis}.json", payload)

    print(
        f"{store_path.name}: {len(payloads)} commune payloads -> {out_dir} "
        f"(period {document['period']})"
    )
    return len(payloads)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flows-dir", type=Path, default=DEFAULT_FLOWS_DIR)
    parser.add_argument("--geographies", type=Path, default=DEFAULT_GEOGRAPHIES)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args(argv)

    try:
        export(args.flows_dir, args.geographies, args.out_dir)
    except FlowExportError as exc:
        print(f"refusing to publish commune flows: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
