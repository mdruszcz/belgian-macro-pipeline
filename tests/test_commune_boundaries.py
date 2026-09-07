"""The boundary file map.html draws, and the script that builds it.

Two different things are checked here. The first group is the BUILDER's
refusals -- it must not guess a projection or silently accept a boundary file
from a different year, because either would produce a map that looks entirely
convincing and is wrong. The second group is the COMMITTED artifact itself:
data/geo/communes.geojson ships to readers, so its keys have to match the
payloads it will be joined against.
"""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BOUNDARIES = REPO / "data" / "geo" / "communes.geojson"
BUILDER = REPO / "scripts" / "build_commune_boundaries.py"

sys.path.insert(0, str(REPO))


def _load_builder():
    pytest.importorskip("shapely", reason="the geo stack is only needed to REBUILD boundaries")
    import importlib.util

    spec = importlib.util.spec_from_file_location("build_commune_boundaries", BUILDER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --- the builder's refusals ------------------------------------------------


def test_crs_is_read_from_the_file_not_assumed(tmp_path):
    """Statbel ships boundaries in both EPSG:31370 and EPSG:3812. Reading the
    wrong one shifts every commune by about a kilometre -- still a map-shaped
    map, just the wrong place."""
    module = _load_builder()
    path = tmp_path / "sectors.geojson"
    path.write_text(
        '{\n"type": "FeatureCollection",\n'
        '"crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:EPSG::31370" } },\n'
        '"features": []\n}',
        encoding="utf-8",
    )
    assert module.read_declared_crs(path) == "EPSG:31370"


def test_a_file_without_a_declared_crs_is_refused(tmp_path):
    module = _load_builder()
    path = tmp_path / "sectors.geojson"
    path.write_text('{\n"type": "FeatureCollection",\n"features": []\n}', encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        module.read_declared_crs(path)
    assert "will not guess" in str(excinfo.value)


def test_a_different_boundary_vintage_is_refused(tmp_path):
    """A file stamped with another date is a different commune map. Loading it
    quietly would re-key every shape against geography that no longer matches."""
    module = _load_builder()
    feature = {
        "type": "Feature",
        "properties": {
            "cd_munty_refnis": "11001",
            "tx_munty_descr_nl": "Aartselaar",
            "tx_munty_descr_fr": "Aartselaar",
            "dt_situation": "2019-01-01",
        },
        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
    }
    path = tmp_path / "sectors.geojson"
    path.write_text(
        '{\n"type": "FeatureCollection",\n"features": [\n'
        + json.dumps(feature).replace('{"type": "Feature"', '{ "type": "Feature"')
        + "\n]\n}",
        encoding="utf-8",
    )
    with pytest.raises(SystemExit) as excinfo:
        module.load_sectors(path)
    assert "2026-01-01" in str(excinfo.value)


def test_sectors_are_grouped_by_commune(tmp_path):
    """Many sectors, one commune: the whole point of the dissolve step."""
    module = _load_builder()

    def feature(nis, x):
        return {
            "type": "Feature",
            "properties": {
                "cd_munty_refnis": nis,
                "tx_munty_descr_nl": f"Commune {nis}",
                "tx_munty_descr_fr": f"Commune {nis}",
                "dt_situation": module.EXPECTED_SITUATION,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[x, 0], [x + 1, 0], [x + 1, 1], [x, 1], [x, 0]]],
            },
        }

    lines = [
        json.dumps(f).replace('{"type": "Feature"', '{ "type": "Feature"')
        for f in (feature("11001", 0), feature("11001", 1), feature("11002", 5))
    ]
    path = tmp_path / "sectors.geojson"
    path.write_text(
        '{\n"type": "FeatureCollection",\n"features": [\n' + ",\n".join(lines) + "\n]\n}",
        encoding="utf-8",
    )
    groups, names = module.load_sectors(path)
    assert sorted(groups) == ["11001", "11002"]
    assert len(groups["11001"]) == 2

    dissolved = module.dissolve(groups)
    # The two touching squares become one shape, not two.
    assert dissolved["11001"].geom_type == "Polygon"
    assert dissolved["11001"].area == pytest.approx(2.0)


# --- the committed artifact ------------------------------------------------


@pytest.fixture(scope="module")
def boundaries():
    if not BOUNDARIES.exists():
        pytest.skip(f"{BOUNDARIES} not built")
    return json.loads(BOUNDARIES.read_text(encoding="utf-8"))


def test_every_current_commune_has_a_shape(boundaries):
    features = boundaries["features"]
    assert len(features) == 565
    assert len({f["properties"]["nis"] for f in features}) == 565


def test_boundary_keys_match_the_indicator_payloads(boundaries):
    """The map joins shapes to values on nis. A shape whose key appears in no
    payload is a commune that can never be coloured, and a payload key with no
    shape is a value that is silently never drawn."""
    payload = REPO / "public" / "data" / "indicators" / "POPULATION_BY_COMMUNE.json"
    if not payload.exists():
        pytest.skip("site payloads not built")
    values = set(json.loads(payload.read_text(encoding="utf-8"))["communes"])
    shapes = {f["properties"]["nis"] for f in boundaries["features"]}
    assert values - shapes == set(), "values with no shape to draw them on"
    assert shapes - values == set(), "shapes with no population figure"


def test_coordinates_are_longitude_latitude_over_belgium(boundaries):
    """Guards the reprojection. Left in the source's Lambert metres the numbers
    would be in the hundreds of thousands, and every one of them would still
    draw a perfectly plausible-looking map."""
    lons, lats = [], []

    def walk(coords):
        if coords and isinstance(coords[0], (int, float)):
            lons.append(coords[0])
            lats.append(coords[1])
            return
        for part in coords:
            walk(part)

    for feature in boundaries["features"]:
        walk(feature["geometry"]["coordinates"])

    # Belgium's real extent, with a small margin.
    assert 2.5 <= min(lons) and max(lons) <= 6.5, (min(lons), max(lons))
    assert 49.4 <= min(lats) and max(lats) <= 51.6, (min(lats), max(lats))


def test_the_file_is_small_enough_to_send_to_a_browser(boundaries):
    """The unsimplified dissolve is 55 MB. Shipping that to a phone would make
    the map unusable on exactly the connection most readers have."""
    size_mb = BOUNDARIES.stat().st_size / 1e6
    assert size_mb < 2.5, f"{size_mb:.1f} MB is too large to send on page load"


def test_the_file_records_where_it_came_from(boundaries):
    """Provenance travels with the artifact, so a reader who downloads the
    GeoJSON alone still knows its source, vintage and how much it was
    generalised."""
    source = boundaries["source"]
    assert source["situation"] == "2026-01-01"
    assert source["source_crs"] == "EPSG:3812"
    assert source["simplify_tolerance_m"] > 0
    assert "Statbel" in source["attribution"]
