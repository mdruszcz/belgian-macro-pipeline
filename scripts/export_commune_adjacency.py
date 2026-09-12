"""Which communes border which -- derived from the boundary file, nothing else.

This pipeline held no commune-adjacency table, and every page that wanted a
"neighbouring communes" panel said so instead of showing one. The table was
never missing from the data, only never derived: `data/geo/communes.geojson`
is built over a SHARED-ARC topology (scripts/build_commune_boundaries.py,
point 2), which means two communes that share a border share the SAME vertex
coordinates along it, byte for byte. Adjacency is therefore a set
intersection over vertices, in pure Python, with no geometry library -- the
geo stack is deliberately not part of `make all`.

THE RULE IS TWO SHARED VERTICES, NOT ONE. A single shared vertex is where
three communes meet at a point; Belgium has 1,014 of those. Two shared
vertices mean a shared edge, which is what "borders" means. Measured on the
committed file: 27,022 vertices belong to exactly two communes, 1,014 to
three and 7 to four.

Deterministic (rule 35): sorted keys, sorted neighbours, no clock. Refuses to
write an empty or lopsided table rather than publishing one (rule 13).

Usage:  python scripts/export_commune_adjacency.py
            [--geojson data/geo/communes.geojson]
            [--out public/data/metadata/adjacency.json]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GEOJSON = REPO_ROOT / "data" / "geo" / "communes.geojson"
DEFAULT_OUT = REPO_ROOT / "public" / "data" / "metadata" / "adjacency.json"

#: How many vertices two outlines must share before they count as bordering.
#: One is a corner where three communes touch; two is an edge.
SHARED_VERTICES_FOR_AN_EDGE = 2


def _rings(geometry: dict):
    """Every ring of a Polygon or MultiPolygon, outer and holes alike."""
    kind = geometry.get("type")
    if kind == "Polygon":
        yield from geometry["coordinates"]
    elif kind == "MultiPolygon":
        for polygon in geometry["coordinates"]:
            yield from polygon
    else:
        raise ValueError(f"unsupported geometry type {kind!r}")


def adjacency(features: list[dict], nis_key: str = "nis") -> dict[str, list[str]]:
    """`{nis: [neighbour nis, ...]}`, symmetric, every list sorted.

    A commune with no neighbour at all is kept with an empty list rather than
    dropped: absence from the table would read as "not computed", and an
    empty list reads as what it is.
    """
    owners: dict[tuple[float, float], set[str]] = defaultdict(set)
    codes: list[str] = []
    for feature in features:
        nis = str(feature["properties"][nis_key])
        codes.append(nis)
        for ring in _rings(feature["geometry"]):
            for x, y in ring:
                owners[(x, y)].add(nis)

    shared: dict[tuple[str, str], int] = defaultdict(int)
    for members in owners.values():
        if len(members) < 2:
            continue
        ordered = sorted(members)
        for i, a in enumerate(ordered):
            for b in ordered[i + 1 :]:
                shared[(a, b)] += 1

    table: dict[str, set[str]] = {nis: set() for nis in codes}
    for (a, b), count in shared.items():
        if count >= SHARED_VERTICES_FOR_AN_EDGE:
            table[a].add(b)
            table[b].add(a)
    return {nis: sorted(table[nis]) for nis in sorted(table)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    if not args.geojson.is_file():
        print(f"no boundary file at {args.geojson}; run `make boundaries` first", file=sys.stderr)
        return 1
    collection = json.loads(args.geojson.read_text(encoding="utf-8"))
    features = collection.get("features") or []
    if not features:
        print(
            f"{args.geojson} holds no features; refusing to write an empty table", file=sys.stderr
        )
        return 1

    table = adjacency(features)
    isolated = [nis for nis, neighbours in table.items() if not neighbours]
    # Belgium is one connected landmass with no island commune, so an
    # isolated one means the topology broke, not that the commune did.
    if isolated:
        print(
            f"{len(isolated)} commune(s) border nothing at all: {isolated[:5]} -- the "
            "boundary file is not a shared-arc topology any more; refusing to publish",
            file=sys.stderr,
        )
        return 1

    args.out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "_note": (
            "Derived from data/geo/communes.geojson by scripts/export_commune_adjacency.py: "
            "two communes are neighbours when their outlines share an edge. Statbel "
            "boundaries, Licence open data 2015-10-22; see the attribution on any map page."
        ),
        "neighbours": table,
    }
    args.out.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    edges = sum(len(v) for v in table.values()) // 2
    print(f"{args.out}: {len(table)} communes, {edges} shared borders")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
