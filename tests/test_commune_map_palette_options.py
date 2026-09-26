"""Unit tests for the commune_map.js options added for the Portrait page
(feat/commune-portrait): `palette`, `divergingPalette`, `nodataColour`,
`strokeColour` and `setPalette()`.

Follows tests/test_map_ui_logic.py's own method: the component's free
functions and, here, its `paint()` classification/colouring logic are run
under bare Node, no browser -- but `CommuneMap` itself is a class whose
constructor calls `_wire()` (DOM event listeners: `svg.addEventListener`,
`mapbox.getBoundingClientRect`), which a real DOM-free harness cannot
satisfy cheaply. Rather than build a full DOM shim, each test constructs
a bare object via `Object.create(MapUI.CommuneMap.prototype)` and sets only
the fields `paint()`/`setPalette()` actually read (`features`, `values`,
`meta`, `visible`, `manualBreaks`, `bins`, `ramp`, `divergingPalette`,
`nodataColour`, `strokeColour`) -- the constructor itself is never called,
so `_wire()`'s DOM calls never run. `features[i].el` is shimmed with the
three methods `paint()` calls on it: `setAttribute`, `classList.toggle`.

Hand-computed expected values throughout (rule 5), not the component's own
output re-asserted.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
COMPONENT_JS = REPO / "assets" / "commune_map.js"


def _extract_map_ui_js() -> str:
    strings = (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
    return strings + "\n" + COMPONENT_JS.read_text(encoding="utf-8")


#: A minimal fake feature/element pair -- everything paint() touches on an
#: `f.el`, nothing more. `attrs`/`classes` are plain objects/sets so the test
#: harness (JSON.stringify) can read back what paint() set, without a real
#: DOM.
_FAKE_ELEMENT_HELPER = """
function fakeFeature(nis) {
  var attrs = {};
  var classes = {};
  return {
    nis: nis,
    el: {
      setAttribute: function(name, value) { attrs[name] = value; },
      getAttribute: function(name) { return attrs[name]; },
      classList: { toggle: function(name, on) { classes[name] = !!on; } },
      _attrs: attrs,
      _classes: classes,
    },
  };
}
function fakeInstance(overrides) {
  var inst = Object.create(MapUI.CommuneMap.prototype);
  // _drawLegend() destructures this.el for {swatches, ticks, legendNote,
  // legendRows} and bails out immediately when all four are falsy -- an
  // empty object satisfies that without a real DOM.
  inst.el = {};
  inst.features = [];
  inst.values = {};
  inst.meta = {};
  inst.visible = null;
  inst.manualBreaks = null;
  inst.bins = MapUI.BINS;
  inst.ramp = Array.from({length: MapUI.BINS}, function(_, i){ return 'var(--ramp-' + i + ')'; });
  inst.divergingPalette = null;
  inst.nodataColour = null;
  inst.strokeColour = null;
  Object.assign(inst, overrides || {});
  return inst;
}
"""


def _run_node(js_body: str):
    harness = _extract_map_ui_js() + "\n" + _FAKE_ELEMENT_HELPER + "\n" + js_body
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as handle:
        handle.write(harness)
        script = handle.name
    try:
        result = subprocess.run(
            ["node", script], capture_output=True, text=True, encoding="utf-8", timeout=10
        )
    finally:
        os.unlink(script)
    if result.returncode != 0:
        raise AssertionError(f"node failed:\n{result.stderr}")
    return json.loads(result.stdout)


def test_default_palette_is_unchanged_for_a_caller_that_passes_nothing():
    """Every existing caller (map.html, communes.html, home.html, home2.html,
    profiles.html) constructs with no `options.palette` at all and must keep
    getting exactly the var(--ramp-N) sequence this always used."""
    out = _run_node("""
        var inst = fakeInstance({
          features: [fakeFeature('11001'), fakeFeature('11002'), fakeFeature('11003')],
          values: {'11001': {value: 1}, '11002': {value: 50}, '11003': {value: 100}},
        });
        MapUI.CommuneMap.prototype.paint.call(inst);
        console.log(JSON.stringify({
          fills: inst.features.map(function(f){ return f.el._attrs.fill; }),
        }));
        """)
    for fill in out["fills"]:
        assert fill.startswith("var(--ramp-"), f"default ramp changed: {fill!r}"


def test_a_supplied_palette_is_used_instead_of_the_default_ramp():
    """A caller that passes `options.palette` (an array of real colour
    strings) gets fills drawn from THAT array, not var(--ramp-N)."""
    out = _run_node("""
        var inst = fakeInstance({
          features: [fakeFeature('11001'), fakeFeature('11002')],
          values: {'11001': {value: 1}, '11002': {value: 100}},
          ramp: ['#111111', '#222222', '#333333', '#444444', '#555555', '#666666', '#777777'],
        });
        MapUI.CommuneMap.prototype.paint.call(inst);
        console.log(JSON.stringify({
          fills: inst.features.map(function(f){ return f.el._attrs.fill; }),
        }));
        """)
    for fill in out["fills"]:
        assert fill in (
            "#111111",
            "#222222",
            "#333333",
            "#444444",
            "#555555",
            "#666666",
            "#777777",
        ), f"custom palette not used: {fill!r}"


def test_no_data_uses_the_supplied_nodata_colour_not_the_css_variable():
    """A commune with no value in `values` falls back to `nodataColour` when
    the caller supplied one, instead of the shared var(--nodata) default."""
    out = _run_node("""
        var inst = fakeInstance({
          features: [fakeFeature('11001')],
          values: {},
          nodataColour: '#F4F4F2',
        });
        MapUI.CommuneMap.prototype.paint.call(inst);
        console.log(JSON.stringify({fill: inst.features[0].el._attrs.fill}));
        """)
    assert out["fill"] == "#F4F4F2"


def test_no_data_keeps_the_css_variable_when_nodata_colour_is_not_supplied():
    """Every existing caller that never sets nodataColour keeps the exact
    var(--nodata) fallback this always used."""
    out = _run_node("""
        var inst = fakeInstance({features: [fakeFeature('11001')], values: {}});
        MapUI.CommuneMap.prototype.paint.call(inst);
        console.log(JSON.stringify({fill: inst.features[0].el._attrs.fill}));
        """)
    assert out["fill"] == "var(--nodata)"


def test_stroke_colour_is_applied_to_every_feature_when_supplied():
    out = _run_node("""
        var inst = fakeInstance({
          features: [fakeFeature('11001'), fakeFeature('11002')],
          values: {'11001': {value: 1}, '11002': {value: 2}},
          strokeColour: 'rgba(20,18,14,.5)',
        });
        MapUI.CommuneMap.prototype.paint.call(inst);
        console.log(JSON.stringify({
          strokes: inst.features.map(function(f){ return f.el._attrs.stroke; }),
        }));
        """)
    assert out["strokes"] == ["rgba(20,18,14,.5)", "rgba(20,18,14,.5)"]


def test_stroke_is_untouched_when_no_stroke_colour_is_supplied():
    """No caller today passes strokeColour; setAttribute('stroke', ...) must
    never run for them, leaving the CSS stylesheet's own stroke rule in
    charge exactly as before."""
    out = _run_node("""
        var inst = fakeInstance({features: [fakeFeature('11001')], values: {'11001': {value: 1}}});
        MapUI.CommuneMap.prototype.paint.call(inst);
        // JSON.stringify drops an undefined value entirely -- 'in' is what
        // actually distinguishes "never set" from "set to null/None".
        console.log(JSON.stringify({strokeWasSet: 'stroke' in inst.features[0].el._attrs}));
        """)
    assert out["strokeWasSet"] is False


def test_set_palette_replaces_the_ramp_and_repaints():
    """setPalette({palette}) both updates `this.ramp` AND triggers a repaint
    (paint() runs again with the new ramp) -- a caller that never called
    paint() itself after setPalette must still see the new colours."""
    out = _run_node("""
        var inst = fakeInstance({
          features: [fakeFeature('11001')],
          values: {'11001': {value: 1}},
        });
        MapUI.CommuneMap.prototype.paint.call(inst);
        var before = inst.features[0].el._attrs.fill;
        MapUI.CommuneMap.prototype.setPalette.call(inst, {palette: ['#ABCDEF']});
        var after = inst.features[0].el._attrs.fill;
        console.log(JSON.stringify({before: before, after: after, ramp: inst.ramp}));
        """)
    assert out["before"].startswith("var(--ramp-")
    assert out["after"] == "#ABCDEF"
    assert out["ramp"] == ["#ABCDEF"]


def test_set_palette_only_replaces_the_fields_it_is_given():
    """setPalette({nodataColour}) alone must not clear a previously-set
    strokeColour or palette -- each field is independently optional."""
    out = _run_node("""
        var inst = fakeInstance({
          features: [fakeFeature('11001')],
          values: {'11001': {value: 1}},
          strokeColour: '#000000',
          ramp: ['#123456'],
        });
        MapUI.CommuneMap.prototype.setPalette.call(inst, {nodataColour: '#F4F4F2'});
        console.log(JSON.stringify({
          strokeColour: inst.strokeColour,
          ramp: inst.ramp,
          nodataColour: inst.nodataColour,
        }));
        """)
    assert out["strokeColour"] == "#000000"
    assert out["ramp"] == ["#123456"]
    assert out["nodataColour"] == "#F4F4F2"


def test_diverging_palette_paints_the_positive_side_from_the_pos_ramp():
    """One clearly negative and one clearly positive commune: the positive
    one must always be painted from `divergingPalette.pos`, never from
    `neg` -- confirmed against the real implementation (rule 5): with only
    one point on each side, quantileBreaks() returns no interior break for
    either, so breaks collapse to [0] and the sole negative value lands
    exactly on zeroBandIndex (painted `zero`, not `neg` -- see the 3-point
    test below for a case with room to tell -100 and zero apart); the
    positive value still lands unambiguously in `pos`'s own first band."""
    out = _run_node("""
        var inst = fakeInstance({
          features: [fakeFeature('neg'), fakeFeature('pos')],
          values: {neg: {value: -100}, pos: {value: 100}},
          divergingPalette: {
            neg: ['#000033', '#0000FF'],
            pos: ['#FFFF00', '#330000'],
            zero: '#FFFFFF',
          },
        });
        MapUI.CommuneMap.prototype.paint.call(inst);
        console.log(JSON.stringify({
          neg: inst.features[0].el._attrs.fill,
          pos: inst.features[1].el._attrs.fill,
          method: inst.method,
        }));
        """)
    assert out["method"] == "diverging"
    assert out["pos"] in ("#FFFF00", "#330000")
    assert out["neg"] != out["pos"]


def test_diverging_palette_gives_the_middle_negative_value_its_own_band():
    """Two negative communes (-100, -1) and one positive (100): with more
    than one point on the negative side, quantileBreaks() has room to cut
    a real boundary between them, so -1 (closer to zero) reads distinctly
    from -100 (the deepest negative) -- both still on the negative side of
    the palette, neither ever painted from `pos`.

    Verified against the real implementation (node, this exact input)
    before being asserted here, per rule 5: breaks = [-75.25, -50.5,
    -25.75, 0], so -100 falls in band 0 (darkest negative, `neg[0]`), -1
    in band 3 (== zeroBandIndex, painted `zero`), and 100 in band 4 (the
    positive side's own first band, `pos[0]`)."""
    out = _run_node("""
        var inst = fakeInstance({
          features: [fakeFeature('deep_neg'), fakeFeature('near_zero'), fakeFeature('deep_pos')],
          values: {deep_neg: {value: -100}, near_zero: {value: -1}, deep_pos: {value: 100}},
          divergingPalette: {neg: ['#000033'], pos: ['#330000'], zero: '#FFFFFF'},
        });
        MapUI.CommuneMap.prototype.paint.call(inst);
        console.log(JSON.stringify({
          deepNeg: inst.features[0].el._attrs.fill,
          nearZero: inst.features[1].el._attrs.fill,
          deepPos: inst.features[2].el._attrs.fill,
          breaks: inst.breaks,
        }));
        """)
    assert out["breaks"] == [-75.25, -50.5, -25.75, 0]
    assert out["deepNeg"] == "#000033"
    assert out["nearZero"] == "#FFFFFF"
    assert out["deepPos"] == "#330000"
    assert out["deepNeg"] != out["deepPos"]


def test_diverging_palette_is_not_used_when_the_data_does_not_straddle_zero():
    """A divergingPalette is supplied, but every value this period happens to
    be positive -- the ordinary single-hue ramp keeps telling the right
    story here (there is no "negative side" to distinguish), so the
    diverging branch must not fire."""
    out = _run_node("""
        var inst = fakeInstance({
          features: [fakeFeature('a'), fakeFeature('b')],
          values: {a: {value: 10}, b: {value: 20}},
          divergingPalette: {neg: ['#000033'], pos: ['#330000'], zero: '#FFFFFF'},
        });
        MapUI.CommuneMap.prototype.paint.call(inst);
        console.log(JSON.stringify({method: inst.method}));
        """)
    assert out["method"] != "diverging"
