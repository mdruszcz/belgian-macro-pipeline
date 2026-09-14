"""Logic tests for BPCharts' pure hit-computation maths (charts.js), run
under Node -- the same method test_map_ui_logic.py uses for the shared map
component.

`computeLineLayout`/`alignPeriods`/`nearestHit` take plain data and plain
numbers, no <canvas> and no `document`, precisely so this arithmetic has a
test surface that does not require mocking a browser (docs/features/
site_unification.md, Batch A1.3: "hand-computed expected coordinates for a
tiny dataset").

The expected pixel coordinates below are independently re-derived from the
documented formula (padding = 15% of the value range; `yOf(v) = pT +
((vMax-v)/vR) * plotHeight`), not read back from the function under test.
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CHARTS_JS = REPO / "assets" / "belpulse" / "charts.js"


def _run_node(js_body: str):
    """Run the harness through a temp FILE -- Windows caps a whole command
    line well under what `node -e <harness>` can need, the same reasoning
    tests/test_map_ui_logic.py documents for the map component."""
    harness = CHARTS_JS.read_text(encoding="utf-8") + "\n" + js_body
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as handle:
        handle.write(harness)
        script = handle.name
    try:
        result = subprocess.run(
            ["node", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=10,
        )
    finally:
        os.unlink(script)
    if result.returncode != 0:
        raise AssertionError(f"node failed:\n{result.stderr}")
    return json.loads(result.stdout)


# The tiny dataset every test below shares: two series on three periods, one
# of them (HICP) missing the middle period -- so alignment has to invent a
# 2021 row for HICP from the UNION of periods, not from HICP's own two rows.
DATASET = """
var series = [
  {label: 'GDP', points: [
    {period: '2020', value: 0},
    {period: '2021', value: 50},
    {period: '2022', value: 100}
  ]},
  {label: 'HICP', points: [
    {period: '2020', value: 0},
    {period: '2022', value: 100}
  ]}
];
"""


def test_align_periods_is_the_sorted_union_not_either_series_own_list():
    result = _run_node(DATASET + """
        console.log(JSON.stringify(BPCharts.alignPeriods(series)));
        """)
    assert result == ["2020", "2021", "2022"]


def test_missing_period_yields_a_null_value_hit_never_a_zero():
    """HICP has no 2021 row. The aligned row for HICP at 2021 must carry
    value:null and status:'missing' -- collapsing it to 0 would be a real
    number that was never measured (rule 26/36)."""
    result = _run_node(DATASET + """
        var layout = BPCharts.computeLineLayout(series, 42, 24, {compact: true});
        var hicp2021 = layout.hits.find(function(h){
          return h.point.series === 'HICP' && h.point.period === '2021';
        });
        console.log(JSON.stringify(hicp2021));
        """)
    assert result["point"]["value"] is None
    assert result["point"]["status"] == "missing"
    # Never silently a real number.
    assert result["point"]["value"] != 0


def test_hit_pixel_coordinates_match_the_documented_formula_by_hand():
    """W=42, H=24, compact padding (pL=pR=2, pT=pB=4) -> plot is 38x16 CSS
    px, slW = 38/2 = 19, so xOf(i) = 2 + 19*i = [2, 21, 40].

    Values across both series range 0..100; padding = (100-0)*0.15 = 15, so
    vMin=-15, vMax=115, vR=130, and
      yOf(v) = 4 + ((115-v)/130)*16.
    yOf(0)   = 4 + (115/130)*16 = 18.153846153846153
    yOf(50)  = 4 + (65/130)*16  = 12
    yOf(100) = 4 + (15/130)*16  = 5.846153846153846
    A missing point is drawn at the vertical mid-point of the plot instead
    (never at a fabricated value's position): midY = 4 + 16/2 = 12.
    """
    result = _run_node(DATASET + """
        var layout = BPCharts.computeLineLayout(series, 42, 24, {compact: true});
        console.log(JSON.stringify(layout.hits));
        """)
    by_key = {(h["point"]["series"], h["point"]["period"]): h for h in result}

    def close(a, b):
        return abs(a - b) < 1e-9

    gdp_2020 = by_key[("GDP", "2020")]
    assert close(gdp_2020["x"], 2)
    assert close(gdp_2020["y"], 18.153846153846153)

    gdp_2021 = by_key[("GDP", "2021")]
    assert close(gdp_2021["x"], 21)
    assert close(gdp_2021["y"], 12)

    gdp_2022 = by_key[("GDP", "2022")]
    assert close(gdp_2022["x"], 40)
    assert close(gdp_2022["y"], 5.846153846153846)

    hicp_2021 = by_key[("HICP", "2021")]
    assert close(hicp_2021["x"], 21)
    assert close(hicp_2021["y"], 12)  # midY, not yOf(anything)
    assert hicp_2021["point"]["value"] is None


def test_nearest_hit_picks_the_closest_point_by_x_with_first_wins_on_a_tie():
    """Pointer at x=19 sits 2px from both series' x=21 column (period 2021)
    and 17px from the x=2 column -- nearer to 2021 either way. Two hits tie
    exactly on that column (GDP and HICP); nearestHit is a stable left-to-
    right scan, so the first one built (GDP, since it is series[0]) wins."""
    result = _run_node(DATASET + """
        var layout = BPCharts.computeLineLayout(series, 42, 24, {compact: true});
        var hit = BPCharts.nearestHit(layout.hits, 19);
        console.log(JSON.stringify(hit.point));
        """)
    assert result["series"] == "GDP"
    assert result["period"] == "2021"
    assert result["value"] == 50

    # Sanity: a pointer far to the right should land on the last period,
    # whichever series happens to own that hit object.
    result2 = _run_node(DATASET + """
        var layout = BPCharts.computeLineLayout(series, 42, 24, {compact: true});
        var hit = BPCharts.nearestHit(layout.hits, 999);
        console.log(JSON.stringify(hit.point));
        """)
    assert result2["period"] == "2022"


def test_single_series_alignment_is_a_no_op_so_existing_single_series_charts_are_unaffected():
    """The vast majority of today's callers pass exactly one series whose
    points are already sorted and gap-free -- alignPeriods must return
    precisely that series' own period list for them, or every existing
    drawLine() caller's pixel output would shift."""
    result = _run_node("""
        var one = [{label: 'Solo', points: [
          {period: '2019', value: 1}, {period: '2020', value: 2}, {period: '2021', value: 3}
        ]}];
        console.log(JSON.stringify(BPCharts.alignPeriods(one)));
        """)
    assert result == ["2019", "2020", "2021"]
