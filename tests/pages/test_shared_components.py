"""Tests for Batch 2 (docs/features/page_builder.md): the shared presentation
component library -- assets/belpulse/layout.css, components.css, charts.js,
components.js, and the gallery that demonstrates them,
docs/design-references/component-gallery.html.

Static analysis only, matching this repo's existing convention for CSS/JS
that has no test-time DOM (tests/pages/test_design_tokens.py,
tests/test_local_ui_logic.py): canvas rendering and DOM wiring need a real
browser and are covered by the manual headless check recorded in the batch
report, not here.
"""

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
BELPULSE_DIR = REPO / "assets" / "belpulse"
TOKENS_CSS = BELPULSE_DIR / "tokens.css"
LAYOUT_CSS = BELPULSE_DIR / "layout.css"
COMPONENTS_CSS = BELPULSE_DIR / "components.css"
CHARTS_JS = BELPULSE_DIR / "charts.js"
COMPONENTS_JS = BELPULSE_DIR / "components.js"
# Issue #309 PR 2's generic simulated-counter engine (macro.html, home2.html).
# Exercised here under Node the same way charts.js/components.js already are
# (pure-logic functions need no DOM; mount()'s own refusal guards need only
# the minimal fake element this file already builds for components.js).
LIVE_COUNTERS_JS = BELPULSE_DIR / "live_counters.js"
GALLERY_HTML = REPO / "docs" / "design-references" / "component-gallery.html"


@pytest.fixture(scope="module")
def tokens_css() -> str:
    return TOKENS_CSS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def layout_css() -> str:
    return LAYOUT_CSS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def components_css() -> str:
    return COMPONENTS_CSS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def charts_js() -> str:
    return CHARTS_JS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def components_js() -> str:
    return COMPONENTS_JS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def gallery_html() -> str:
    return GALLERY_HTML.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def live_counters_js() -> str:
    return LIVE_COUNTERS_JS.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def defined_tokens(tokens_css) -> set:
    """Every --bp-* name defined anywhere in tokens.css (light or dark
    block) -- used to check layout.css/components.css never reference a
    token that doesn't actually exist."""
    return set(re.findall(r"(--bp-[\w-]+)\s*:", tokens_css))


# --- files exist and are non-trivial ----------------------------------------


def test_all_batch_2_deliverables_exist():
    for path in (LAYOUT_CSS, COMPONENTS_CSS, CHARTS_JS, COMPONENTS_JS, GALLERY_HTML):
        assert path.exists(), f"missing Batch 2 deliverable: {path}"
        assert path.stat().st_size > 200, f"{path} looks like a stub"


# --- token discipline: no raw colour outside the documented placeholder ------


def _var_references(css: str) -> set:
    return set(re.findall(r"var\((--bp-[\w-]+)", css))


def test_layout_css_only_references_tokens_that_exist(layout_css, defined_tokens):
    referenced = _var_references(layout_css)
    missing = referenced - defined_tokens
    assert not missing, f"layout.css references undefined tokens: {missing}"


def test_components_css_only_references_tokens_that_exist(components_css, defined_tokens):
    referenced = _var_references(components_css)
    missing = referenced - defined_tokens
    assert not missing, f"components.css references undefined tokens: {missing}"


def _raw_hex_outside_placeholder_logo(css: str) -> list:
    """Every colour in the shared component library must come from a --bp-*
    token (claude.md: no raw colour outside tokens.css) with exactly one
    documented exception: the placeholder tricolour logo mark, called out in
    layout.css's own comment as the one place slated to change."""
    logo_block = re.search(r"\.bp-logo-mark span:nth-child.*?(?=\n\n|\Z)", css, re.DOTALL)
    logo_text = logo_block.group(0) if logo_block else ""
    stripped = css.replace(logo_text, "")
    return re.findall(r"#[0-9a-fA-F]{3,8}\b", stripped)


def test_layout_css_has_no_undocumented_raw_colour(layout_css):
    hits = _raw_hex_outside_placeholder_logo(layout_css)
    assert not hits, f"raw colour(s) outside the documented placeholder logo: {hits}"


def test_components_css_has_no_raw_colour(components_css):
    hits = re.findall(r"#[0-9a-fA-F]{3,8}\b", components_css)
    assert not hits, f"components.css should use --bp-* tokens only, found: {hits}"


# --- rule 36: no fabricated indicator/commune values in a design asset ------


def test_no_shared_component_file_names_an_indicator(
    layout_css, components_css, charts_js, components_js
):
    indicator_shape = re.compile(r"\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+){2,}\b")
    for label, text in (
        ("layout.css", layout_css),
        ("components.css", components_css),
        ("charts.js", charts_js),
        ("components.js", components_js),
    ):
        hits = indicator_shape.findall(text)
        assert not hits, f"{label} names something indicator-shaped: {hits}"


# --- the seven data states ---------------------------------------------------


REQUIRED_STATES = ["loading", "missing", "suppressed", "unavailable", "error"]


def test_components_css_styles_every_required_state(components_css):
    # "ready" carries an explicit zero by design (no separate selector --
    # see the file's own header comment), so it is not asserted as a
    # standalone data-state selector like the other six.
    for state in REQUIRED_STATES:
        pattern = f'[data-state="{state}"]'
        assert pattern in components_css, f"no CSS targets {pattern}"


def test_missing_and_unavailable_render_identically_but_are_distinct_selectors(components_css):
    """Both mean 'no data' to a reader and must look the same; they must
    still be two distinct data-state values, never collapsed to one, per
    claude.md rule 26."""
    assert '[data-state="missing"]' in components_css
    assert '[data-state="unavailable"]' in components_css
    # They are combined in a shared selector list rather than duplicated --
    # confirm both values appear together wherever the message is shown.
    combined = re.search(
        r'\.bp-state\[data-state="missing"\][^{]*data-state="unavailable"[^{]*\{',
        components_css,
    )
    assert combined, "missing/unavailable are not styled as siblings sharing one rule"


def test_the_gallery_demonstrates_every_state_for_kpi_and_chart_components(gallery_html):
    for state in REQUIRED_STATES:
        assert (
            gallery_html.count(f"'{state}'") + gallery_html.count(f'"{state}"') >= 1
        ), f"gallery never sets up the {state} state"


# --- component classes exist for everything Batch 2's spec names ------------


# Issue #309 PR 2 restyled .bp-live-counter/.bp-simulated-badge here (the
# light Portrait-card look, docs/decisions/0016-simulated-live-counters.md)
# but did NOT add classes to this generic component library for the new,
# FEATURE-SCOPED vocabulary it needed on top (.bp-sim-region, .bp-sim-strip,
# .bp-sim-pause, .bp-simulated-badge--prominent, .bp-share-*, .bp-official-*,
# .bp-finance-row). Those live in assets/belpulse/live_counters.css instead,
# each documented at its own definition -- they are specific to the
# Finances publiques chapter's own layout, not reusable the way a KPI tile
# or a ranking list is, so they do not belong in docs/design-references/
# component-gallery.html's generic catalogue (the same reasoning already
# keeps europe_map.css's and macro_portrait.css's own classes out of it).
REQUIRED_COMPONENT_CLASSES = [
    "bp-kpi",
    "bp-kpi--compact",
    "bp-kpi--mini",
    "bp-kpi-grid",
    "bp-stat-tile",
    "bp-list-panel",
    "bp-chart-card",
    "bp-compare-table",
    "bp-ranking-list",
    "bp-live-counter",
    "bp-simulated-badge",
    "bp-pull-quote",
    "bp-news-card",
    "bp-commune-card",
    "bp-feature-tile",
    "bp-chips",
    "bp-grade",
    "bp-freshness",
    "bp-map-card",
]


@pytest.mark.parametrize("class_name", REQUIRED_COMPONENT_CLASSES)
def test_required_component_class_is_styled(components_css, class_name):
    assert f".{class_name}" in components_css, f"no rule for .{class_name} in components.css"


@pytest.mark.parametrize("class_name", REQUIRED_COMPONENT_CLASSES)
def test_required_component_appears_in_the_gallery(gallery_html, class_name):
    assert (
        class_name in gallery_html
    ), f".{class_name} is styled but never demonstrated in the gallery"


# --- the simulated-live-counter guard ----------------------------------------


def test_simulated_counter_is_never_a_silent_data_source(components_js):
    """A real metric must never be wired to the live-counter animation by
    accident -- the JS itself refuses to run without the data-simulated
    attribute the CSS's .bp-simulated-badge depends on."""
    fn = re.search(r"function initSimulatedCounter\(.*?\n  \}", components_js, re.DOTALL)
    assert fn, "initSimulatedCounter not found"
    assert "data-simulated" in fn.group(0)
    assert "throw" in fn.group(0)


def _run_node(js_body: str, *scripts: str):
    """Run the harness through a temp FILE, not `node -e`.

    Windows caps a whole command line at about 32 KB, and these harnesses are
    whole asset files concatenated. Linux allows a far larger argument list,
    which is why CI never sees the limit.

    delete=False plus an explicit unlink because Windows will not let node open
    a NamedTemporaryFile that Python still holds open.
    """
    harness = "\n".join(scripts) + "\n" + js_body
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
    return result.stdout


def test_simulated_counter_appears_in_no_public_page():
    """components.js's initSimulatedCounter() is a GALLERY-ONLY demo helper
    (see test_simulated_counter_is_never_a_silent_data_source above). The
    real simulated counters this site ships (issue #309: macro.html and
    home2.html) are built by the separate, generic assets/belpulse/
    live_counters.js engine instead -- a public page calling the gallery's
    own demo helper would be exactly the "silent data source" risk that
    function's own guard exists to prevent, just one level up."""
    from src.site.routes import root_routes

    checked = 0
    for route in root_routes():
        page = REPO / route.lstrip("/")
        if not page.is_file():
            continue
        checked += 1
        text = page.read_text(encoding="utf-8")
        assert "initSimulatedCounter(" not in text, f"{route} calls the gallery-only demo helper"
    assert checked, "no root page resolved to a real file, so this test would prove nothing"


def test_simulated_counter_actually_throws_without_the_attribute(components_js):
    fake_el = """
    const el = { attrs: {}, getAttribute(n){ return this.attrs[n] || null; } };
    let threw = false;
    try { BPComponents.initSimulatedCounter(el); } catch(e) { threw = true; }
    console.log(JSON.stringify(threw));
    """
    out = _run_node(fake_el, components_js)
    assert json.loads(out.strip()) is True


# --- charts.js: pure logic (niceSteps) run under Node, no DOM needed --------


def test_nice_steps_produces_sane_gridlines(charts_js):
    body = """
    const steps = BPCharts.niceSteps(0, 10, 4);
    console.log(JSON.stringify(steps));
    """
    out = _run_node(body, charts_js)
    steps = json.loads(out.strip())
    assert steps == sorted(steps)
    assert all(0 <= s <= 10 for s in steps)
    assert len(steps) >= 2


def test_charts_module_exports_all_four_renderers(charts_js):
    body = """
    console.log(JSON.stringify({
      line: typeof BPCharts.drawLine,
      bar: typeof BPCharts.drawBar,
      donut: typeof BPCharts.drawDonut,
      ranking: typeof BPCharts.drawRanking,
    }));
    """
    out = _run_node(body, charts_js)
    kinds = json.loads(out.strip())
    assert all(v == "function" for v in kinds.values()), kinds


# --- reuse, not reinvention --------------------------------------------------


def test_charts_js_reads_colour_only_from_bp_chart_tokens(charts_js):
    """The whole point of generalising local.html's drawSeries() is that a
    chart never hardcodes a colour -- every series colour must come from
    chartColour(), which reads --bp-chart-N."""
    assert "--bp-chart-" in charts_js
    hits = re.findall(r"#[0-9a-fA-F]{3,8}\b", charts_js)
    assert not hits, f"charts.js should use tokens only, found raw colour: {hits}"


# --- live_counters.js: pure segment math + mount()'s refusal guards ---------
#
# Issue #309 PR 2. valueAt/sinceOpened are pure functions of plain segment
# objects -- no DOM needed, same as charts.js's niceSteps above. mount()'s
# four refusal checks (missing data-simulated, missing badge, empty badge,
# invisible badge) all throw BEFORE the function ever touches
# document.createElement, so a minimal fake container/badge (no real DOM, no
# jsdom dependency) is enough to exercise them -- the same style as
# test_simulated_counter_actually_throws_without_the_attribute's fake_el
# above.


def test_value_at_before_the_first_segment_is_not_started(live_counters_js):
    body = """
    const segments = [{start_ms: 1000, end_ms: 2000, v0: 5, v1: 15, rate_per_ms: 0.01}];
    console.log(JSON.stringify(BPLiveCounters.valueAt(segments, 500)));
    """
    out = json.loads(_run_node(body, live_counters_js).strip())
    assert out == {"state": "not-started", "value": 5, "ratePerMs": 0.01}


def test_value_at_inside_a_segment_is_the_linear_interpolation(live_counters_js):
    body = """
    const segments = [{start_ms: 1000, end_ms: 2000, v0: 5, v1: 15, rate_per_ms: 0.01}];
    console.log(JSON.stringify(BPLiveCounters.valueAt(segments, 1500)));
    """
    out = json.loads(_run_node(body, live_counters_js).strip())
    assert out["state"] == "running"
    # v0 + rate_per_ms*(t-start_ms) = 5 + 0.01*500 = 10, hand-computed.
    assert out["value"] == pytest.approx(10.0)
    assert out["ratePerMs"] == 0.01


def test_value_at_after_the_last_segment_is_expired_at_its_final_value(live_counters_js):
    body = """
    const segments = [{start_ms: 1000, end_ms: 2000, v0: 5, v1: 15, rate_per_ms: 0.01}];
    console.log(JSON.stringify(BPLiveCounters.valueAt(segments, 9999)));
    """
    out = json.loads(_run_node(body, live_counters_js).strip())
    assert out == {"state": "expired", "value": 15, "ratePerMs": 0.01}


def test_value_at_in_a_gap_between_two_segments_holds_the_earlier_value(live_counters_js):
    """Two declared segments with a gap in between (e.g. a payload that
    skips a period) must never invent a number inside the gap -- valueAt
    reports 'expired' at the earlier segment's own v1, not a fabricated
    interpolation toward the next segment's v0."""
    body = """
    const segments = [
      {start_ms: 1000, end_ms: 2000, v0: 5, v1: 15, rate_per_ms: 0.01},
      {start_ms: 5000, end_ms: 6000, v0: 50, v1: 60, rate_per_ms: 0.01},
    ];
    console.log(JSON.stringify(BPLiveCounters.valueAt(segments, 3000)));
    """
    out = json.loads(_run_node(body, live_counters_js).strip())
    assert out == {"state": "expired", "value": 15, "ratePerMs": 0.01}


def test_value_at_with_no_segments_is_not_started_with_no_fabricated_value(live_counters_js):
    body = """
    console.log(JSON.stringify(BPLiveCounters.valueAt([], 1234)));
    console.log(JSON.stringify(BPLiveCounters.valueAt(null, 1234)));
    """
    out = _run_node(body, live_counters_js).strip().splitlines()
    assert json.loads(out[0]) == {"state": "not-started", "value": None, "ratePerMs": None}
    assert json.loads(out[1]) == {"state": "not-started", "value": None, "ratePerMs": None}


def test_since_opened_never_goes_negative_across_a_year_boundary_reset(live_counters_js):
    """The real-world case this guards: a flow counter (revenue, spending)
    resets to v0=0 at every year boundary. Computed the naive way --
    valueAt(now) minus valueAt(openedAt) -- a reader who opened the page
    late in one year (a large accumulated value) and is still watching a
    couple of seconds into the next (reset near 0) would see a large
    NEGATIVE "since you opened this page" figure. Hand-computed expected
    value: segment 1 contributes 1ms*rate 1 = 1 (the overlap of
    [openedAt=999, now=1001) with [0,1000)); segment 2 contributes
    1ms*rate 1 = 1 (the overlap with [1000,2000)); total 2 -- never the
    naive diff (valueAt(1001)=1, valueAt(999)=999, diff = -998)."""
    body = """
    const segments = [
      {start_ms: 0, end_ms: 1000, v0: 0, v1: 1000, rate_per_ms: 1},
      {start_ms: 1000, end_ms: 2000, v0: 0, v1: 1000, rate_per_ms: 1},
    ];
    console.log(JSON.stringify({
      sinceOpened: BPLiveCounters.sinceOpened(segments, 999, 1001),
      naiveDiff: BPLiveCounters.valueAt(segments, 1001).value - BPLiveCounters.valueAt(segments, 999).value,
    }));
    """
    out = json.loads(_run_node(body, live_counters_js).strip())
    assert out["sinceOpened"] == pytest.approx(2.0)
    assert out["sinceOpened"] >= 0
    assert out["naiveDiff"] < 0, "the scenario should reproduce the bug the real formula avoids"


def test_since_opened_is_zero_when_now_is_not_after_opened(live_counters_js):
    body = """
    const segments = [{start_ms: 0, end_ms: 1000, v0: 0, v1: 1000, rate_per_ms: 1}];
    console.log(JSON.stringify([
      BPLiveCounters.sinceOpened(segments, 500, 500),
      BPLiveCounters.sinceOpened(segments, 500, 100),
      BPLiveCounters.sinceOpened(null, 0, 1000),
    ]));
    """
    out = json.loads(_run_node(body, live_counters_js).strip())
    assert out == [0, 0, 0]


def _fake_container_js(data_simulated="true", badge="good"):
    """badge: 'good' (visible, non-empty), 'missing' (no element at all),
    'empty' (element present, blank text), or 'hidden' (non-empty text but
    el.hidden true -- isTrulyVisible's cheapest-to-fake failure mode)."""
    badge_literal = {
        "good": "{textContent: 'Simulation', hidden: false}",
        "missing": "null",
        "empty": "{textContent: '   ', hidden: false}",
        "hidden": "{textContent: 'Simulation', hidden: true}",
    }[badge]
    attrs_literal = (
        "{}" if data_simulated is None else "{'data-simulated': " + json.dumps(data_simulated) + "}"
    )
    return (
        "const attrs = " + attrs_literal + ";\n"
        "const badge = " + badge_literal + ";\n"
        "const container = {\n"
        "  getAttribute(n){ return attrs[n] === undefined ? null : attrs[n]; },\n"
        "  querySelector(sel){ return sel === '.bp-simulated-badge' ? badge : null; },\n"
        "};\n"
    )


def _mount_throws(js_setup: str, scripts) -> str:
    body = js_setup + """
    let message = null;
    try { BPLiveCounters.mount(container, {schema_version: 1, simulated: true, counters: [], placements: {}}, {}); }
    catch(e) { message = e.message; }
    console.log(JSON.stringify(message));
    """
    out = _run_node(body, *scripts)
    return json.loads(out.strip())


def test_mount_refuses_a_container_without_data_simulated_true(live_counters_js):
    message = _mount_throws(
        _fake_container_js(data_simulated=None, badge="good"), [live_counters_js]
    )
    assert message and "data-simulated" in message


def test_mount_refuses_a_container_with_no_badge_element_at_all(live_counters_js):
    message = _mount_throws(_fake_container_js(badge="missing"), [live_counters_js])
    assert message and "bp-simulated-badge" in message


def test_mount_refuses_a_container_with_an_empty_badge(live_counters_js):
    message = _mount_throws(_fake_container_js(badge="empty"), [live_counters_js])
    assert message and "bp-simulated-badge" in message


def test_mount_refuses_a_container_with_an_invisible_badge(live_counters_js):
    message = _mount_throws(_fake_container_js(badge="hidden"), [live_counters_js])
    assert message and "bp-simulated-badge" in message


def test_mount_accepts_a_container_with_data_simulated_and_a_visible_badge(live_counters_js):
    """The positive control for the four refusal tests above: change
    nothing except making the badge visible and non-empty, and mount()
    must get PAST its own guard (it will still fail shortly after, inside
    resolveCounters/buildNode, because this fake container has no real DOM
    -- caught and reported here as a DIFFERENT error, proving the guard
    itself did not fire)."""
    message = _mount_throws(_fake_container_js(badge="good"), [live_counters_js])
    assert message is not None, "mount() did not throw at all against a fake, DOM-less container"
    assert (
        "data-simulated" not in message and "bp-simulated-badge" not in message
    ), f"the guard itself rejected a valid container: {message}"


def test_gallery_loads_tokens_before_layout_before_components(gallery_html):
    """Cascade order matters: components.css assumes layout's box-sizing
    reset and tokens.css's custom properties are already in scope."""
    order = [
        gallery_html.find("tokens.css"),
        gallery_html.find("layout.css"),
        gallery_html.find("components.css"),
    ]
    assert all(i != -1 for i in order), "gallery is missing one of the three stylesheets"
    assert order == sorted(order), "stylesheets are not linked in cascade order"
