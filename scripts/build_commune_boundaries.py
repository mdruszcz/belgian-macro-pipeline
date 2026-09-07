"""Build the commune boundary file the map page draws.

WHY THIS SCRIPT EXISTS: Statbel publishes boundaries as ~20,800 STATISTICAL
SECTORS in a 227 MB GeoJSON, projected in Belgian Lambert 2008 (EPSG:3812,
metres). A browser can use none of that directly. This turns it into the 565
communes the rest of the pipeline is keyed on, in WGS84 lon/lat, small enough
to ship.

THREE THINGS HERE ARE NOT INCIDENTAL:

1. The source CRS is READ FROM THE FILE, never assumed. Statbel ships some
   layers in EPSG:31370 (Lambert 72) and some in 3812; they differ by ~1 km at
   the wrong guess, which would look plausible and be wrong. If the file does
   not declare a CRS, this script refuses rather than guessing (CLAUDE.md
   rule 13).

2. Simplification runs over a SHARED-ARC TOPOLOGY, not per polygon. Simplifying
   each commune on its own moves a shared border twice, in two directions, and
   opens visible gaps and overlaps between neighbours. topojson simplifies each
   border once, so neighbours still fit together afterwards.

3. Every NIS code is resolved through resolve_geo() like any other source. The
   boundary file is reference geometry, but it is still keyed on NIS, and an
   unknown code here means the map would silently drop a commune.

Output is data/geo/communes.geojson -- committed, because it is derived from a
hand-downloaded source under gitignored data/raw-style storage and the site
cannot be rebuilt without it (the same reason the manual observation CSVs are
committed, ADR 0002).
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

from shapely.geometry import mapping, shape
from shapely.ops import unary_union

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.geography.resolve import resolve_geo  # noqa: E402

# The vintage the boundary file describes. Statbel stamps it on every feature as
# dt_situation; we assert it rather than read it so that dropping in a file from
# a different year is a loud failure, not a silent change of the commune map.
EXPECTED_SITUATION = "2026-01-01"

# The period NIS codes are resolved against. The 2026-01-01 file is the
# post-2025-merger map, which is what "2025" resolves to.
RESOLVE_PERIOD = "2025"

# Metres. Chosen by measuring: 25 m -> 1.9 MB, 50 m -> 1.2 MB, 100 m -> 0.7 MB,
# 200 m -> 0.45 MB. At 50 m a commune border is accurate to about half a city
# block, which is beyond what a national choropleth can show, and the file still
# fits in a single sub-2 MB download.
DEFAULT_TOLERANCE_M = 50

# Five decimal places is ~1 m at Belgian latitudes -- far finer than a 50 m
# simplification, so it throws away nothing and cuts the file roughly in half
# against full float repr.
COORD_PRECISION = 5

PROPERTY_NIS = "cd_munty_refnis"
PROPERTY_NAME_NL = "tx_munty_descr_nl"
PROPERTY_NAME_FR = "tx_munty_descr_fr"
PROPERTY_SITUATION = "dt_situation"


def read_declared_crs(path: Path) -> str:
    """Return the EPSG code the file declares, or fail.

    Read from the head of the file: the crs member is a sibling of "features"
    and appears before it, so this never loads the 227 MB body.
    """
    head = path.open(encoding="utf-8").read(4096)
    match = re.search(r"urn:ogc:def:crs:EPSG::(\d+)", head)
    if not match:
        match = re.search(r'"EPSG:(\d+)"', head)
    if not match:
        raise SystemExit(
            f"{path.name} declares no CRS. Belgian boundary files ship in both "
            "EPSG:31370 and EPSG:3812 and the two are about a kilometre apart, "
            "so this script will not guess one. Re-export the file with its CRS."
        )
    return f"EPSG:{match.group(1)}"


def load_sectors(path: Path) -> tuple[dict[str, list], dict[str, tuple[str, str]]]:
    """Stream the sector features, grouped by commune.

    Streamed line by line rather than json.load()ed because the file is 227 MB
    of text and parsing it whole costs several gigabytes of resident memory for
    no benefit -- every feature is independent and is consumed once.
    """
    groups: dict[str, list] = defaultdict(list)
    names: dict[str, tuple[str, str]] = {}
    situations: set[str] = set()

    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip().rstrip(",")
            if not line.startswith('{ "type": "Feature"'):
                continue
            feature = json.loads(line)
            props = feature["properties"]
            nis = props[PROPERTY_NIS]
            groups[nis].append(shape(feature["geometry"]))
            names.setdefault(nis, (props[PROPERTY_NAME_NL], props[PROPERTY_NAME_FR]))
            situations.add(props[PROPERTY_SITUATION])

    if situations != {EXPECTED_SITUATION}:
        raise SystemExit(
            f"expected every feature stamped {EXPECTED_SITUATION}, found "
            f"{sorted(situations)}. A different vintage means a different commune "
            "map; update EXPECTED_SITUATION deliberately after checking what changed."
        )
    if not groups:
        raise SystemExit(f"no features parsed from {path} -- is it one feature per line?")
    return groups, names


def dissolve(groups: dict[str, list]) -> dict[str, object]:
    """Merge each commune's sectors into one shape.

    buffer(0) after the union repairs the self-touching rings that fall out of
    unioning hundreds of polygons whose edges were digitised independently.
    """
    dissolved = {}
    for nis, geoms in groups.items():
        merged = unary_union(geoms)
        if not merged.is_valid:
            merged = merged.buffer(0)
        dissolved[nis] = merged
    return dissolved


def simplify_shared(order: list[str], geoms: list, tolerance: float) -> list:
    """Simplify over a shared-arc topology so neighbours still line up.

    Imported here rather than at module scope so that --help and the reference
    checks work without the geo stack installed.
    """
    import topojson

    topo = topojson.Topology(geoms, prequantize=False, shared_coords=False)
    simplified = json.loads(topo.toposimplify(tolerance).to_geojson())
    features = simplified["features"]
    if len(features) != len(order):
        raise SystemExit(
            f"simplification returned {len(features)} shapes for {len(order)} communes"
        )
    return [shape(f["geometry"]) for f in features]


def reproject(geom, transformer):
    """Reproject a shapely geometry's coordinates, rounding as we go."""

    def walk(coords):
        if coords and isinstance(coords[0], (int, float)):
            x, y = transformer.transform(coords[0], coords[1])
            return [round(x, COORD_PRECISION), round(y, COORD_PRECISION)]
        return [walk(part) for part in coords]

    geojson = mapping(geom)
    geojson["coordinates"] = walk(geojson["coordinates"])
    return geojson


def build(src: Path, out: Path, db: Path, tolerance: float) -> None:
    source_crs = read_declared_crs(src)
    print(f"source CRS declared by the file: {source_crs}")

    groups, names = load_sectors(src)
    print(f"parsed sectors for {len(groups)} communes")

    conn = sqlite3.connect(db)
    try:
        geo_ids = {nis: resolve_geo(conn, nis, RESOLVE_PERIOD) for nis in groups}
    finally:
        conn.close()

    dissolved = dissolve(groups)
    order = sorted(dissolved)
    simplified = simplify_shared(order, [dissolved[n] for n in order], tolerance)

    from pyproj import Transformer

    transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)

    features = []
    for nis, geom in zip(order, simplified, strict=True):
        if not geom.is_valid:
            geom = geom.buffer(0)
        if geom.is_empty:
            raise SystemExit(f"commune {nis} simplified away to nothing at {tolerance} m")
        name_nl, name_fr = names[nis]
        features.append(
            {
                "type": "Feature",
                # Kept deliberately minimal: the map joins on geo_id and reads
                # every value from the payloads. Duplicating figures into the
                # boundary file would give the map a second, staler copy of the
                # data (CLAUDE.md rule 6 in spirit -- one source per number).
                "properties": {
                    "geo_id": geo_ids[nis],
                    "nis": nis,
                    "name_nl": name_nl,
                    "name_fr": name_fr,
                },
                "geometry": reproject(geom, transformer),
            }
        )

    payload = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "source": {
            "file": src.name,
            "situation": EXPECTED_SITUATION,
            "source_crs": source_crs,
            "simplify_tolerance_m": tolerance,
            "attribution": "Statbel (Directorate-general Statistics - Statistics Belgium)",
        },
        "features": features,
    }

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    print(f"wrote {len(features)} communes to {out} ({out.stat().st_size / 1e6:.2f} MB)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--src",
        type=Path,
        default=Path(
            "data/raw/statbel/sectors/" "sh_statbel_statistical_sectors_3812_20260101.geojson"
        ),
    )
    parser.add_argument("--out", type=Path, default=Path("data/geo/communes.geojson"))
    parser.add_argument("--db", type=Path, default=Path("data/belgian_macro.db"))
    parser.add_argument("--tolerance", type=float, default=DEFAULT_TOLERANCE_M)
    args = parser.parse_args()

    if not args.src.exists():
        raise SystemExit(
            f"{args.src} not found. This is the Statbel statistical-sectors "
            "GeoJSON, hand-downloaded like the other manual sources."
        )
    build(args.src, args.out, args.db, args.tolerance)


if __name__ == "__main__":
    main()
