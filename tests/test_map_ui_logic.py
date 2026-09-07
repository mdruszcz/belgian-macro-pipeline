"""Logic tests for map.html's DOM-free helpers (MapUI), run under Node -- the
same method test_local_ui_logic.py uses for local.html.

The classification is the part worth testing rather than eyeballing: a
choropleth is a picture, and a picture of a wrong break table looks exactly as
convincing as a picture of a right one. Rendering and DOM wiring are covered by
the browser check recorded in docs/steps.
"""

import json
import re
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
MAP_HTML = REPO / "map.html"


def _extract_map_ui_js() -> str:
    """Pull the MapUI <script> block out of map.html, so the test exercises the
    exact code the page ships rather than a copy that could drift from it."""
    text = MAP_HTML.read_text(encoding="utf-8")
    for script in re.findall(r"<script>(.*?)</script>", text, re.DOTALL):
        if "MapUI" in script and "module.exports" in script:
            return script
    raise AssertionError("Could not find the MapUI <script> block in map.html")


def _run_node(js_body: str):
    harness = _extract_map_ui_js() + "\n" + js_body
    result = subprocess.run(["node", "-e", harness], capture_output=True, text=True, timeout=10)
    if result.returncode != 0:
        raise AssertionError(f"node failed:\n{result.stderr}")
    return json.loads(result.stdout)


def test_quantile_breaks_split_evenly():
    """Seven bins over 700 evenly spread values put 100 communes in each."""
    out = _run_node("""
        const values = Array.from({length: 700}, (_, i) => i + 1);
        const {breaks, method} = MapUI.classify(values, 7);
        const counts = {};
        for(const v of values){
          const b = MapUI.bandFor(v, breaks);
          counts[b] = (counts[b] || 0) + 1;
        }
        console.log(JSON.stringify({breaks, counts, method}));
        """)
    assert out["method"] == "quantile"
    assert len(out["breaks"]) == 6
    assert sorted(out["counts"]) == ["0", "1", "2", "3", "4", "5", "6"]
    assert all(count == 100 for count in out["counts"].values())


def test_no_band_is_left_empty():
    """Every band the legend draws must be one a commune can actually fall in.

    A quantile break landing exactly on the minimum makes the bottom band
    unreachable, so the legend would advertise a colour that appears nowhere on
    the map and one fewer real distinction than the reader is told there is.
    """
    out = _run_node("""
        const cases = {
          skewed:  Array(90).fill(5).concat(Array(10).fill(9)),
          zeros:   Array(400).fill(0).concat(Array.from({length: 150}, (_, i) => i + 1)),
          spread:  Array.from({length: 500}, (_, i) => i * 3),
        };
        const out = {};
        for(const [name, values] of Object.entries(cases)){
          values.sort((a, b) => a - b);
          const {breaks, method} = MapUI.classify(values, 7);
          const used = new Set(values.map(v => MapUI.bandFor(v, breaks)));
          out[name] = {bands: breaks.length + 1, used: used.size, method};
        }
        console.log(JSON.stringify(out));
        """)
    for name, result in out.items():
        if result["method"] == "quantile":
            # A quantile band is defined as "this many communes", so an empty
            # one is a band that should never have been drawn.
            assert (
                result["used"] == result["bands"]
            ), f"{name}: {result['bands']} quantile bands but only {result['used']} reachable"
        else:
            # An equal-interval band is a range of VALUES, so an empty one is a
            # real statement -- no commune falls in that range. What must not
            # happen is the whole map collapsing to one colour.
            assert result["used"] >= 2, f"{name}: fell back to {result['method']} and still flat"


def test_flat_quantiles_fall_back_so_the_map_is_not_one_colour():
    """When most communes share a value, quantiles collapse to a single band
    and 90% and 10% of the country would be painted identically. The page
    switches to equal intervals rather than draw that, and reports it."""
    out = _run_node("""
        const values = Array(90).fill(5).concat(Array(10).fill(9));
        values.sort((a, b) => a - b);
        const {breaks, method} = MapUI.classify(values, 7);
        console.log(JSON.stringify({
          method,
          bands: breaks.length + 1,
          low:  MapUI.bandFor(5, breaks),
          high: MapUI.bandFor(9, breaks),
        }));
        """)
    assert out["method"] == "equal"
    assert out["bands"] >= 3
    assert out["low"] != out["high"], "the two distinct values must not share a colour"


def test_an_indicator_with_one_value_everywhere_gets_one_band():
    """Not a failure -- there is genuinely nothing to distinguish, and inventing
    bands for it would be inventing variation."""
    out = _run_node("""
        const values = Array(50).fill(7);
        const {breaks, method} = MapUI.classify(values, 7);
        console.log(JSON.stringify({breaks, method}));
        """)
    assert out["breaks"] == []
    assert out["method"] == "uniform"


def test_band_for_is_inclusive_at_the_lower_edge():
    """A value sitting exactly on a break belongs to the band above it, so a
    commune is never dropped between two bands."""
    out = _run_node("""
        const breaks = [10, 20, 30];
        console.log(JSON.stringify([9.9, 10, 19.9, 20, 30, 31].map(v => MapUI.bandFor(v, breaks))));
        """)
    assert out == [0, 1, 1, 2, 3, 3]


def test_colour_index_spans_the_whole_ramp():
    """However many bands survive, the first uses the palest colour and the
    last the strongest -- otherwise a three-band map is drawn in three
    near-identical shades."""
    out = _run_node("""
        const spans = {};
        for(const bands of [1, 2, 3, 7]){
          spans[bands] = Array.from({length: bands}, (_, b) => MapUI.colourIndex(b, bands, 7));
        }
        console.log(JSON.stringify(spans));
        """)
    assert out["1"] == [6]
    assert out["7"] == [0, 1, 2, 3, 4, 5, 6]
    for bands in ("2", "3", "7"):
        assert out[bands][0] == 0 and out[bands][-1] == 6
        assert out[bands] == sorted(out[bands])


def test_default_indicator_prefers_config_over_anything_hardcoded():
    """The opening indicator comes from the section config's headline list, or
    the URL, and never from a name written into the page."""
    out = _run_node("""
        const available = ['AAA', 'BBB', 'CCC'];
        console.log(JSON.stringify({
          url_wins:      MapUI.defaultIndicator('CCC', ['BBB'], available),
          unknown_url:   MapUI.defaultIndicator('ZZZ', ['BBB'], available),
          headline:      MapUI.defaultIndicator(null, ['ZZZ', 'BBB'], available),
          no_headline:   MapUI.defaultIndicator(null, [], available),
          nothing:       MapUI.defaultIndicator(null, [], []),
        }));
        """)
    assert out == {
        "url_wins": "CCC",
        "unknown_url": "BBB",
        "headline": "BBB",
        "no_headline": "AAA",
        "nothing": None,
    }


def test_units_are_formatted_the_way_communes_html_formats_them():
    out = _run_node("""
        console.log(JSON.stringify({
          eur:     MapUI.formatValue(445000, 'eur', 0),
          percent: MapUI.formatValue(12.5, 'percent', 1),
          count:   MapUI.formatValue(565615, 'count', 0),
          missing: MapUI.formatValue(null, 'count', 0),
          suffix_rate:  MapUI.unitSuffix('per_10000_cars'),
          suffix_count: MapUI.unitSuffix('count'),
        }));
        """)
    assert out["eur"].startswith("€")
    assert out["percent"].endswith("%")
    assert out["missing"] == "—"
    assert out["suffix_rate"] == " per 10000 cars"
    assert out["suffix_count"] == ""


def test_projection_keeps_belgium_from_stretching_sideways():
    """Longitude must be scaled by cos(latitude); without it the country is
    drawn about 1.6x too wide."""
    out = _run_node("""
        const features = [{geometry: {type: 'Polygon', coordinates: [[[2.5, 49.5], [6.4, 51.5]]]}}];
        const lat = MapUI.meanLatitude(features);
        console.log(JSON.stringify({lat, scale: MapUI.lonScale(lat)}));
        """)
    assert 49.5 <= out["lat"] <= 51.5
    assert 0.6 < out["scale"] < 0.65


def test_geometry_to_path_handles_multipolygons_and_closes_every_ring():
    """A commune with islands or exclaves is a MultiPolygon; every ring of it
    has to be closed or the fill leaks into its neighbour."""
    out = _run_node("""
        const geom = {type: 'MultiPolygon', coordinates: [
          [[[0, 0], [1, 0], [1, 1]]],
          [[[5, 5], [6, 5], [6, 6]]],
        ]};
        const d = MapUI.geometryToPath(geom, 1);
        console.log(JSON.stringify({d, moves: (d.match(/M/g) || []).length,
                                    closes: (d.match(/Z/g) || []).length}));
        """)
    assert out["moves"] == 2
    assert out["closes"] == 2


def test_page_names_no_indicator():
    """The 50% gate's rule: adding an indicator is a config change, never an
    edit to this page. An indicator id appearing here would silently make one
    dataset special."""
    text = MAP_HTML.read_text(encoding="utf-8")
    # Indicator ids are SCREAMING_SNAKE_CASE with at least two segments.
    suspects = set(re.findall(r"\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+){2,}\b", text))
    # Names that belong to the page's own vocabulary, not to any dataset.
    allowed = {"DRAG_THRESHOLD_PX", "MIN_USEFUL_BANDS"}
    assert suspects <= allowed, f"map.html names indicators directly: {suspects - allowed}"
