"""explorer.html -- the Batch 8a data explorer.

A noindexed preview beside the live all_data.html. Same contract as the other
redesigned pages: the page is a renderer, not a data file. It must name no
indicator, print no figure, reach no third party, and say honestly which of
the five observation states each cell is in.

The tests that matter most here are the two the roadmap line asks for --
"usable at full data scale" (the table is windowed, and the window is
announced to assistive technology rather than silently lying about how many
rows exist) and "never disagree with a download" (every figure on screen
comes from a payload sharded out of the very CSV the page offers).
"""

import json
import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PAGE = REPO / "explorer.html"
I18N_JS = REPO / "assets" / "i18n.js"
INDEX = REPO / "public" / "data" / "explorer" / "index.json"
PUBLISHED_INDICATORS = REPO / "public" / "data" / "metadata" / "indicators.json"
NATIONAL = REPO / "public" / "data" / "national.json"

# The figures a reader would see if the page ever hardcoded an example. There
# is no design mockup for this page, so this list is the handful of real
# values a developer might be tempted to paste while laying out a column.
FORBIDDEN_FIGURES = ["565,615", "11 867 634", "40 125", "1 397 565"]


@pytest.fixture(scope="module")
def page() -> str:
    return PAGE.read_text(encoding="utf-8")


def _strings() -> dict:
    """The three language tables out of assets/i18n.js, parsed the way
    tests/test_i18n.py parses them: the file is JS, not JSON, so each table is
    lifted by name and read key by key."""
    source = I18N_JS.read_text(encoding="utf-8")
    tables = {}
    for lang in ("en", "fr", "nl"):
        match = re.search(rf"\n  {lang}: \{{(.*?)\n  \}},?\n", source, re.DOTALL)
        assert match, f"no {lang} table in assets/i18n.js"
        tables[lang] = set(re.findall(r"^\s{4}([A-Za-z][A-Za-z0-9]*)\s*:", match.group(1), re.M))
    return tables


def _indicator_codes() -> set[str]:
    """Every code this site publishes, municipal and national. Batch 9's
    lesson: the universe is two files, not one -- metadata/indicators.json is
    municipal-only and the national codes live inside national.json."""
    codes: set[str] = set()
    if PUBLISHED_INDICATORS.is_file():
        data = json.loads(PUBLISHED_INDICATORS.read_text(encoding="utf-8"))
        codes |= {row["indicator_code"] for row in data["indicators"]}
    if NATIONAL.is_file():
        codes |= set(json.loads(NATIONAL.read_text(encoding="utf-8"))["indicators"])
    return codes


# --- the page names no indicator and prints no figure -----------------------


def test_the_page_names_no_indicator_anywhere(page):
    """Rule 2, extended to the builder by rule 24. This cannot be satisfied by
    renaming a constant: the codes come from what is actually published."""
    codes = _indicator_codes()
    if not codes:
        pytest.skip("payloads not built")
    assert len(codes) > 50, "the code set is suspiciously small"
    named = sorted(code for code in codes if code in page)
    assert not named, f"explorer.html names indicators: {named}"


def test_the_page_prints_no_figure(page):
    for figure in FORBIDDEN_FIGURES:
        assert figure not in page, f"a real figure is typed into the page: {figure}"


def test_the_page_loads_no_third_party_asset(page):
    """Rule 30 and the CSP the other redesigned pages keep: fonts only.

    ASSETS, not links. The licence notice has to link statbel.fgov.be,
    creativecommons.org, onem.be and police.be by name -- that is the
    condition under which the data may be published at all -- and an <a href>
    is not something the browser loads."""
    loaded = re.findall(r'<script[^>]+src="(https?://[^"]+)"', page)
    loaded += re.findall(r'<link[^>]+href="(https?://[^"]+)"', page)
    bad = [url for url in loaded if not url.startswith("https://fonts.googleapis.com/")]
    assert not bad, f"explorer.html loads third-party assets: {bad}"


# --- it reads only published payloads ---------------------------------------


def test_every_fetch_is_a_published_payload(page):
    """No raw CSV, no database, no source file. The page is a renderer."""
    fetched = re.findall(r"fetch\(\s*[`'\"]([^`'\"?]+)", page)
    fetched += re.findall(r"fetch\(\s*`([^`?]+)", page)
    allowed_prefixes = ("public/data/", "data/geo/")
    bad = [url for url in fetched if not url.startswith(allowed_prefixes)]
    assert not bad, f"explorer.html fetches something that is not a payload: {bad}"


def test_the_download_offered_is_the_file_the_figures_came_from(page):
    """The roadmap line for this batch: the explorer must never disagree with
    a download. The payloads are sharded out of these two CSVs, so these are
    the two files the button may offer -- and the exporter test proves the
    shards match them row for row."""
    hrefs = set(re.findall(r'href="(data/[^"]+)"', page))
    assert hrefs, "the page offers no download at all"
    assert hrefs <= {
        "data/belgian_macro_export.csv",
        "data/communes_history.csv",
    }, f"the page offers a download its figures did not come from: {sorted(hrefs)}"


def test_the_index_carries_both_scopes():
    """A single catalogue drives every filter, so the page never has to guess
    what exists before fetching a payload."""
    if not INDEX.is_file():
        pytest.skip("payloads not built")
    entries = json.loads(INDEX.read_text(encoding="utf-8"))["indicators"]
    scopes = {entry["scope"] for entry in entries}
    assert scopes == {"municipal", "national"}, scopes
    for entry in entries:
        assert entry["names"]["en"] and entry["names"]["fr"] and entry["names"]["nl"]


# --- the five states stay distinct ------------------------------------------


def test_every_status_letter_has_its_own_word(page):
    """Rule 26. The six letters the canonical exporter now emits -- Batch 8a
    extended it from two -- must each reach the reader as something other than
    a bare letter, and a suppressed cell must never read as a zero."""
    pill = re.search(r"function statusPill\(s\)\{(.*?)\n\}", page, re.DOTALL)
    assert pill, "statusPill() not found"
    body = pill.group(1)
    for letter in ("A", "P", "R", "E", "S", "N"):
        assert f"'{letter}'" in body or f'"{letter}"' in body, f"status {letter} has no branch"
    assert "derived" in body, "a derived value has no pill of its own"
    # A cell summed from the communes that existed before a merger. It must
    # have its own branch rather than falling through to the raw-word default:
    # `derived` is computed from this commune's own figures, `reconstructed`
    # from a commune that no longer exists, and rule 26 keeps those distinct.
    assert "reconstructed" in body, "a reconstructed value has no pill of its own"


def test_a_reconstructed_cell_is_named_in_the_readers_own_language(page):
    """Same contract as communes.html and map.html: `reconstructed` is
    resolved through T() rather than added to MAP_STATUS_WORDS, because
    commune_map.js prints that table's words into the tooltip as raw text with
    no i18n lookup (rule 7). Kept distinct from `derived` (rule 26)."""
    assert "function mapStatusWord(" in page
    assert "T('reconstructedSuffix')" in page
    assert "mapStatusWord(cell[1])" in page
    assert "MAP_STATUS_WORDS[cell[1]]" not in page
    table = re.search(r"const MAP_STATUS_WORDS = \{(.*?)\}", page, re.DOTALL)
    assert table, "MAP_STATUS_WORDS not found"
    assert "reconstructed" not in table.group(1)


def test_a_suppressed_cell_is_never_drawn_as_a_number(page):
    """ONEM withholds counts under 10: those arrive with a status and no
    value. The map must pass that through as withheld rather than as absent,
    the way communes.html does -- the two are different facts."""
    assert "suppressed" in page, "the page never mentions the suppressed state"
    assert re.search(r"MAP_STATUS_WORDS", page), "no status vocabulary for the map"


# --- the windowed table is honest about itself ------------------------------


def test_the_table_is_windowed(page):
    """'Usable at full data scale': 178,128 municipal rows cannot all be in
    the DOM. Only the visible slice plus an overscan margin is rendered."""
    assert "const ROW_HEIGHT" in page, "no fixed row height, so no window arithmetic"
    assert "OVERSCAN" in page, "no overscan margin"
    assert "scrollTop" in page and "clientHeight" in page


def test_the_window_declares_the_true_row_count(page):
    """A windowed table that reports only what it rendered would tell a screen
    reader there are forty rows when there are forty thousand."""
    assert "aria-rowcount" in page, "the table never declares its true length"
    assert "aria-rowindex" in page, "a rendered row never declares its true position"


def test_the_row_height_in_the_script_matches_the_stylesheet(page):
    """The window arithmetic is pixel arithmetic: if CSS and JS disagree about
    a row's height, rows drift out of the viewport as you scroll."""
    js = re.search(r"const ROW_HEIGHT = (\d+)", page)
    assert js, "ROW_HEIGHT not found"
    css = re.search(r"table\.vt tbody td\{[^}]*?height:(\d+)px", page, re.DOTALL)
    assert css, "no declared row height in the stylesheet"
    assert js.group(1) == css.group(1), f"JS says {js.group(1)}px, CSS says {css.group(1)}px"


def test_a_theme_change_repaints_the_rendered_rows(page):
    """Rows are built as HTML strings, so a palette swap does not reach them
    on its own -- assets/belpulse/blocks.js watches the same two signals."""
    assert "MutationObserver" in page, "a theme attribute change is not observed"
    assert "matchMedia" in page, "a system theme change is not observed"


# --- the map is the shared one ----------------------------------------------


def test_the_map_is_the_shared_component(page):
    """Rule 29. Loading the component and never redefining its internals is
    what 'no second map implementation' means in practice."""
    assert 'src="assets/commune_map.js"' in page
    assert 'href="assets/commune_map.css"' in page
    assert "MapUI.quantileBreaks =" not in page, "the page forks the map engine"
    assert "new MapUI.CommuneMap" in page


def test_the_map_is_offered_only_where_a_map_means_something(page):
    """A national series has one geography. Drawing it on 565 communes would
    be an invented map, so the mode says why instead."""
    assert re.search(
        r"scope\s*[=!]==?\s*['\"]national['\"]", page
    ), "the page never distinguishes a national indicator when choosing a mode"


# --- chrome, translation, routing -------------------------------------------


def test_every_key_the_page_asks_for_exists_in_all_three_languages(page):
    tables = _strings()
    used = set()
    for attribute in ("data-t", "data-t-html", "data-t-title", "data-t-aria", "data-t-placeholder"):
        used |= set(re.findall(rf'{attribute}="([A-Za-z][A-Za-z0-9]*)"', page))
    used |= set(re.findall(r"T\('([A-Za-z][A-Za-z0-9]*)'", page))
    # status_* keys are composed at runtime from the letter, like map.html's.
    used = {key for key in used if not key.startswith("status")}
    assert used, "the page marks nothing for translation"
    for lang in ("en", "fr", "nl"):
        missing = sorted(used - tables[lang])
        assert not missing, f"{lang} is missing {missing}"


def test_the_english_stays_in_the_markup(page):
    """A reader whose JavaScript never runs still gets a page, and the licence
    notice is a condition of publishing at all."""
    assert re.search(r'data-t="explorerTitle">\s*\S{4,}', page), "the heading is empty without JS"
    assert "statbel.fgov.be" in page, "no licence notice without JavaScript"


def test_it_is_registered_noindexed_and_in_no_sitemap(page):
    from src.site.routes import ROOT_PAGES, is_indexable, sitemap_routes

    routes = {entry.route for entry in ROOT_PAGES}
    assert "/explorer.html" in routes, "the page is not in the route inventory"
    assert not is_indexable("/explorer.html"), "a preview must not be indexable"
    assert "/explorer.html" not in set(sitemap_routes()), "a preview must not be submitted"
    assert (
        '<meta name="robots" content="noindex">' in page
    ), "robots.txt alone is not enough -- a linked page can still be indexed"


def test_every_link_and_script_resolves(page):
    for reference in re.findall(r'(?:src|href)="([^"#?]+)"', page):
        if reference.startswith(("http", "mailto:", "data:")):
            continue
        assert (REPO / reference).exists(), f"explorer.html points at a missing file: {reference}"


def test_the_url_carries_the_view_without_stacking_history(page):
    """A researcher's use case is sending someone a link to a slice. Every
    keystroke pushing a history entry would make the back button useless --
    map.html settled this the same way."""
    assert "history.replaceState" in page
    # Comments stripped first: the page explains in prose why it does NOT use
    # pushState, and the word appearing in that sentence is not a call.
    code = re.sub(r"<!--.*?-->", "", page, flags=re.DOTALL)
    code = re.sub(r"/\*.*?\*/", "", code, flags=re.DOTALL)
    code = re.sub(r"^\s*//.*$", "", code, flags=re.MULTILINE)
    assert "pushState" not in code
    for parameter in ("scope", "indicator", "mode"):
        assert f"'{parameter}'" in page, f"the URL never carries {parameter}"
