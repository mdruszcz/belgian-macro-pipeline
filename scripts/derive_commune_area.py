"""Derive each of today's 565 communes' land area, in km², for the peer
model's population-density variable (docs/features/peer_model.md, variable
2; ADR 0015, Decision 3).

ONE-OFF SCRIPT. Needs `pip install -r requirements-geo.txt` (shapely, pyproj)
-- deliberately NOT in requirements.txt, same reasoning as
build_commune_boundaries.py: the compiled GEOS/PROJ stack is not needed by
the daily pipeline, only to (re)build this one committed file.

Two sources, in order of preference:

1. The raw Statbel statistical-sector geometry
   (data/raw/statbel/sectors/sh_statbel_statistical_sectors_3812_20260101.geojson),
   already EPSG:3812 (Belgian Lambert 2008, projected metres) -- the same
   hand-downloaded file build_commune_boundaries.py dissolves for the map.
   Dissolved by commune NIS the same way (unary_union + buffer(0) repair),
   then measured directly: no reprojection needed since it is already metres.

2. If that 227 MB file is not present locally (it is gitignored, hand-
   downloaded, and most machines running this script will not have it),
   `data/geo/communes.geojson` -- the committed, already-dissolved, 50 m-
   simplified 565-commune file, in CRS84 (WGS84 lon/lat) -- reprojected to
   EPSG:3812 with pyproj and measured there. NEVER measured in lon/lat
   degrees; a degree is not a fixed distance at this latitude and doing so
   would silently misstate every area.

Output: config/geography/commune_area_km2.csv, columns
`nis,area_km2,geometry_source,situation`, area rounded to 3 decimals, sorted
by nis, `\n` line endings (not the platform default -- this file is read by
future exporters expecting the pipeline's usual convention, and Windows'
default `\r\n` would silently double the apparent line count in a naive
`wc -l`).

Refuses (CLAUDE.md rule 13) unless the output covers EXACTLY the 565 NIS
codes that are today's municipalities in config/geography/geographies.csv
(level == "municipality", valid_to empty) -- no more, no fewer -- and every
area is > 0.

Usage:
    python scripts/derive_commune_area.py
        [--sectors data/raw/statbel/sectors/sh_statbel_statistical_sectors_3812_20260101.geojson]
        [--communes-geojson data/geo/communes.geojson]
        [--geographies config/geography/geographies.csv]
        [--out config/geography/commune_area_km2.csv]
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_SECTORS = (
    REPO_ROOT
    / "data"
    / "raw"
    / "statbel"
    / "sectors"
    / ("sh_statbel_statistical_sectors_3812_20260101.geojson")
)
DEFAULT_COMMUNES_GEOJSON = REPO_ROOT / "data" / "geo" / "communes.geojson"
DEFAULT_GEOGRAPHIES = REPO_ROOT / "config" / "geography" / "geographies.csv"
DEFAULT_OUT = REPO_ROOT / "config" / "geography" / "commune_area_km2.csv"

# Same field name build_commune_boundaries.py reads from the raw sectors file.
SECTOR_NIS_PROPERTY = "cd_munty_refnis"
SECTOR_SITUATION_PROPERTY = "dt_situation"
SECTOR_EXPECTED_SITUATION = "2026-01-01"

# communes.geojson's own "situation" metadata (build_commune_boundaries.py's
# EXPECTED_SITUATION). Read from the file's `source` block, never assumed --
# a mismatch would mean this script is silently describing the wrong vintage.
COMMUNES_GEOJSON_SITUATION_KEY = "situation"


def current_municipality_nis(geographies_path: Path) -> set[str]:
    """Today's 565 NIS codes: level == municipality, valid_to empty."""
    codes: set[str] = set()
    with geographies_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["level"] == "municipality" and not row["valid_to"]:
                codes.add(row["nis_code"])
    return codes


def read_declared_crs(path: Path) -> str:
    """EPSG code declared in a GeoJSON's `crs` member. Same rule as
    build_commune_boundaries.py: never guessed, because Belgian boundary
    files ship in both EPSG:31370 and EPSG:3812, about a kilometre apart."""
    head = path.open(encoding="utf-8").read(4096)
    match = re.search(r"urn:ogc:def:crs:EPSG::(\d+)", head)
    if not match:
        match = re.search(r'"EPSG:(\d+)"', head)
    if not match:
        raise SystemExit(f"{path.name} declares no CRS -- refusing to guess (CLAUDE.md rule 13).")
    return f"EPSG:{match.group(1)}"


def areas_from_sectors(path: Path) -> tuple[dict[str, float], str]:
    """Dissolve the raw statistical-sector geometry by commune NIS and
    measure each dissolved polygon's area, in its native EPSG:3812 metres.

    Mirrors build_commune_boundaries.py's load_sectors()/dissolve(): stream
    line by line (the file is 227 MB), group polygons by
    cd_munty_refnis, union, buffer(0) to repair self-touching rings, then
    .area (m^2) -> km^2.
    """
    from shapely.geometry import shape
    from shapely.ops import unary_union

    source_crs = read_declared_crs(path)
    if source_crs != "EPSG:3812":
        raise SystemExit(
            f"{path.name} declares {source_crs}, not EPSG:3812 -- this script measures "
            "area directly in the file's own units and refuses to assume metres for a "
            "different CRS. Re-export or reproject deliberately."
        )

    groups: dict[str, list] = defaultdict(list)
    situations: set[str] = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip().rstrip(",")
            if not line.startswith('{ "type": "Feature"'):
                continue
            feature = json.loads(line)
            props = feature["properties"]
            nis = props[SECTOR_NIS_PROPERTY]
            groups[nis].append(shape(feature["geometry"]))
            situations.add(props[SECTOR_SITUATION_PROPERTY])

    if not groups:
        raise SystemExit(f"no features parsed from {path} -- is it one feature per line?")
    if situations != {SECTOR_EXPECTED_SITUATION}:
        raise SystemExit(
            f"expected every feature stamped {SECTOR_EXPECTED_SITUATION}, found "
            f"{sorted(situations)}."
        )

    areas: dict[str, float] = {}
    for nis, geoms in groups.items():
        merged = unary_union(geoms)
        if not merged.is_valid:
            merged = merged.buffer(0)
        areas[nis] = merged.area / 1_000_000.0  # m^2 -> km^2
    return areas, f"statbel_statistical_sectors:{SECTOR_EXPECTED_SITUATION}"


def areas_from_communes_geojson(path: Path) -> tuple[dict[str, float], str]:
    """Reproject the committed, already-dissolved 565-commune file to
    EPSG:3812 and measure area there. NEVER measured in the file's native
    CRS84 lon/lat -- a degree of longitude is not a fixed distance, and
    doing so would silently misstate every commune's area."""
    from pyproj import Transformer
    from shapely.geometry import shape
    from shapely.ops import transform as shapely_transform

    payload = json.loads(path.read_text(encoding="utf-8"))
    crs_name = payload.get("crs", {}).get("properties", {}).get("name", "")
    if "CRS84" not in crs_name and "4326" not in crs_name:
        raise SystemExit(
            f"{path.name} declares CRS {crs_name!r}, not CRS84/WGS84 -- this script's "
            "fallback path assumes lon/lat input to reproject from. Refusing to guess."
        )
    situation = payload.get("source", {}).get(COMMUNES_GEOJSON_SITUATION_KEY, "")
    if not situation:
        raise SystemExit(f"{path.name} carries no source.situation -- cannot stamp provenance.")

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3812", always_xy=True)

    def project(x, y, z=None):
        return transformer.transform(x, y)

    areas: dict[str, float] = {}
    for feature in payload["features"]:
        nis = feature["properties"]["nis"]
        geom = shape(feature["geometry"])
        projected = shapely_transform(project, geom)
        if not projected.is_valid:
            projected = projected.buffer(0)
        areas[nis] = projected.area / 1_000_000.0  # m^2 -> km^2

    return areas, f"communes_geojson_reprojected:{situation}"


def write_csv(out_path: Path, areas: dict[str, float], geometry_source: str) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="\n") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(["nis", "area_km2", "geometry_source", "situation"])
        situation = geometry_source.split(":", 1)[1]
        source_label = geometry_source.split(":", 1)[0]
        for nis in sorted(areas):
            writer.writerow([nis, f"{round(areas[nis], 3):.3f}", source_label, situation])


def derive(
    sectors_path: Path,
    communes_geojson_path: Path,
    geographies_path: Path,
    out_path: Path,
) -> tuple[int, str, float]:
    if sectors_path.exists():
        print(f"using raw statistical-sector geometry: {sectors_path}")
        areas, geometry_source = areas_from_sectors(sectors_path)
    else:
        print(
            f"{sectors_path} not found (hand-downloaded, gitignored) -- falling back to "
            f"{communes_geojson_path}"
        )
        areas, geometry_source = areas_from_communes_geojson(communes_geojson_path)

    current = current_municipality_nis(geographies_path)
    present = set(areas)
    if present != current:
        missing = current - present
        extra = present - current
        raise SystemExit(
            "commune area coverage does not match today's 565 municipalities exactly -- "
            f"missing {sorted(missing)}, unexpected {sorted(extra)}"
        )
    non_positive = {nis: a for nis, a in areas.items() if not (a > 0)}
    if non_positive:
        raise SystemExit(f"non-positive area for: {non_positive}")

    write_csv(out_path, areas, geometry_source)
    total = sum(areas.values())
    return len(areas), geometry_source, total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sectors", type=Path, default=DEFAULT_SECTORS)
    parser.add_argument("--communes-geojson", type=Path, default=DEFAULT_COMMUNES_GEOJSON)
    parser.add_argument("--geographies", type=Path, default=DEFAULT_GEOGRAPHIES)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    try:
        n, source, total = derive(args.sectors, args.communes_geojson, args.geographies, args.out)
    except ImportError as exc:
        raise SystemExit(
            f"missing geo dependency ({exc}) -- run "
            "`python -m pip install -r requirements-geo.txt` first (local only; do not add "
            "these to requirements.txt)."
        ) from exc
    print(f"wrote {n} communes to {args.out}, source={source}, Belgium total={total:.3f} km2")


if __name__ == "__main__":
    sys.exit(main())
