"""Which communes border which, derived from the boundary file.

Every page that wanted a "neighbouring communes" panel said the pipeline held
no adjacency table. It never needed one: the boundary file is a shared-arc
topology, so two communes that border each other share the same vertex
coordinates along the border, and adjacency is a set intersection.

The expected neighbours below are NOT read from the script's own output. They
are Namur's actual neighbours, written down from the map -- the rule for a
derived table is the same as for a derived statistic (rule 5): the expected
value is worked out independently, or the test only proves the code agrees
with itself.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.export_commune_adjacency import adjacency  # noqa: E402

GEOJSON = REPO / "data" / "geo" / "communes.geojson"
PUBLISHED = REPO / "public" / "data" / "metadata" / "adjacency.json"

#: Namur's ten neighbours, from the map of the province -- not from the file.
NAMUR = "92094"
NAMUR_NEIGHBOURS = {
    "Andenne",
    "Assesse",
    "Eghezée",
    "Fernelmont",
    "Floreffe",
    "Gembloux",
    "Gesves",
    "Jemeppe-sur-Sambre",
    "La Bruyère",
    "Profondeville",
}


def square(nis, x0, y0, size=1.0):
    """A closed square ring as a GeoJSON feature, vertices in a fixed order."""
    ring = [[x0, y0], [x0 + size, y0], [x0 + size, y0 + size], [x0, y0 + size], [x0, y0]]
    return {"properties": {"nis": nis}, "geometry": {"type": "Polygon", "coordinates": [ring]}}


def test_two_squares_sharing_an_edge_are_neighbours():
    """Left and right unit squares share the edge x=1: two vertices."""
    table = adjacency([square("A", 0, 0), square("B", 1, 0)])
    assert table == {"A": ["B"], "B": ["A"]}


def test_two_squares_touching_at_one_corner_are_not_neighbours():
    """Diagonal squares meet at a single point, (1,1). One shared vertex is a
    corner where communes touch, not a border -- Belgium has 1,014 such
    tripoints, and counting them would give every commune phantom
    neighbours across every corner."""
    table = adjacency([square("A", 0, 0), square("B", 1, 1)])
    assert table == {"A": [], "B": []}


def test_a_hole_counts_too():
    """An enclave sits inside another commune's HOLE ring: the outer commune's
    hole and the enclave's outer ring are the same vertices, so they border."""
    outer = {
        "properties": {"nis": "OUT"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[0, 0], [3, 0], [3, 3], [0, 3], [0, 0]],
                [[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]],
            ],
        },
    }
    table = adjacency([outer, square("IN", 1, 1)])
    assert table == {"IN": ["OUT"], "OUT": ["IN"]}


@pytest.fixture(scope="module")
def real():
    if not GEOJSON.is_file():
        pytest.skip("no boundary file; run `make boundaries`")
    return adjacency(json.loads(GEOJSON.read_text(encoding="utf-8"))["features"])


@pytest.fixture(scope="module")
def names():
    features = json.loads(GEOJSON.read_text(encoding="utf-8"))["features"]
    return {str(f["properties"]["nis"]): f["properties"]["name_fr"] for f in features}


def test_namur_borders_exactly_its_real_neighbours(real, names):
    assert {names[n] for n in real[NAMUR]} == NAMUR_NEIGHBOURS


def test_the_table_is_symmetric_and_complete(real):
    """If A borders B then B borders A, and no commune borders nothing --
    Belgium is one landmass with no island commune, so an isolated one means
    the topology broke."""
    assert len(real) == 565
    for nis, neighbours in real.items():
        assert neighbours, f"{nis} borders nothing"
        assert nis not in neighbours, f"{nis} lists itself"
        for other in neighbours:
            assert nis in real[other], f"{nis} -> {other} is not mirrored"


def test_the_published_table_matches_a_fresh_derivation(real):
    """What is committed is what the script produces from the committed
    boundaries -- rule 35's byte-identical rebuild, for this file."""
    if not PUBLISHED.is_file():
        pytest.skip("adjacency.json not built; run `make exports`")
    published = json.loads(PUBLISHED.read_text(encoding="utf-8"))["neighbours"]
    assert published == real


def test_the_exporter_runs_as_a_script(tmp_path):
    """The Makefile calls it, and an importable module is not the same thing
    as a runnable one (Batch 17a found `make pages` broken by exactly that)."""
    out = tmp_path / "adjacency.json"
    result = subprocess.run(
        [sys.executable, str(REPO / "scripts" / "export_commune_adjacency.py"), "--out", str(out)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=str(REPO),
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert "565 communes" in result.stdout
    assert out.is_file()
