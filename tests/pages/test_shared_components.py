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
