"""The card view and the mobile filter drawer on communes.html (Batch 5).

Same rule as the map panel (tests/test_communes_map_panel.py): the card view
draws the SAME rows as the table -- same filters, same sort, same selected
year -- so what matters here is that it cannot become a second, disagreeing
presentation of the data, and that adding it did not put the table itself at
risk. Rendering pixels is covered by the headless check recorded in
docs/steps; this file covers the wiring and the two pieces of real branching
logic (cardFigureCodes, cardCellHtml) that are worth running rather than
eyeballing.
"""

import json
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
COMMUNES_HTML = REPO / "communes.html"
INDICATORS_JSON = REPO / "public" / "data" / "metadata" / "indicators.json"

NEW_I18N_KEYS = ["filtersToggle", "viewMode", "viewTable", "viewCards", "cardAllIndicators"]


@pytest.fixture(scope="module")
def page() -> str:
    return COMMUNES_HTML.read_text(encoding="utf-8")


def _extract_fn(page: str, name: str) -> str:
    """One top-level function's exact source, from `function NAME(` to the
    closing brace that sits at column 0 -- true for every function this file
    adds or reuses, none of which has a nested block also closing at column 0.
    """
    m = re.search(rf"\nfunction {name}\(.*?\n\}}\n", page, re.DOTALL)
    assert m, f"{name}() not found in communes.html"
    return m.group(0)


def _run_node(js_body: str):
    result = subprocess.run(
        ["node", "-e", js_body],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _node_i18n(expression: str):
    result = subprocess.run(
        ["node", "-e", f"const I=require('./assets/i18n.js');process.stdout.write({expression})"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


# --- the view toggle ---------------------------------------------------------


def test_the_card_view_exists_and_is_togglable(page):
    assert 'id="cardsWrap"' in page, "no card container"
    assert 'class="cards-wrap' in page
    seg = re.search(r'<div class="view-toggle" id="viewSeg"[^>]*>(.*?)</div>', page, re.DOTALL)
    assert seg, "no view-toggle segmented control"
    assert 'data-view="table"' in seg.group(1)
    assert 'data-view="cards"' in seg.group(1)
    assert "function setView(" in page
    assert "addEventListener('click', () => setView(b.dataset.view, true))" in page


def test_the_view_choice_is_remembered_under_its_own_key_and_guarded(page):
    """A new key, not one of the existing theme/language keys -- and wrapped in
    try/catch, since localStorage can throw in private browsing."""
    assert "belpulse-communes-view" in page
    set_view = _extract_fn(page, "setView")
    assert "try{" in set_view and "localStorage.setItem('belpulse-communes-view'" in set_view


def test_cards_are_the_default_below_the_drawer_breakpoint_with_no_explicit_choice(page):
    """Decision 7: an explicit choice always wins; with none yet, cards are the
    default under the same 720px breakpoint the drawer itself uses."""
    assert re.search(
        r"matchMedia\('\(max-width:\s*720px\)'\)\.matches\)\s*VIEW\s*=\s*'cards'", page
    ), "no narrow-screen default to cards"


def test_the_view_toggle_is_not_a_url_parameter(page):
    """Decision 7: this is a per-viewer convenience in localStorage, not a URL
    state -- a query parameter here would be a URL rule 31 then has to keep
    valid forever. The page carries NO URL state at all today (no
    URLSearchParams, no pushState, no hash handling); this batch must not
    introduce the first one."""
    assert "URLSearchParams" not in page
    assert "pushState" not in page


# --- headline figures are config, not hardcoded -----------------------------


def test_headline_codes_are_read_from_sections_json(page):
    assert "public/data/metadata/sections.json" in page
    assert "sections.headlines" in page
    # Filtered against what this table actually has -- the same defensive
    # filter profiles.html applies to the same field.
    assert (
        "HEADLINE_CODES = (sections.headlines || []).filter(code => INDICATOR_CODES.includes(code))"
        in page
    )


def test_no_indicator_code_is_hardcoded_in_the_page(page):
    """Checked against the ACTUAL published indicator list, exactly as
    tests/test_map_ui_logic.py::test_no_page_names_an_indicator does, so this
    cannot be satisfied by renaming a constant and grows on its own as
    indicators are added."""
    if not INDICATORS_JSON.exists():
        pytest.skip("site payloads not built")
    codes = {
        row["indicator_code"]
        for row in json.loads(INDICATORS_JSON.read_text(encoding="utf-8"))["indicators"]
    }
    assert codes, "the indicator index is empty, so this test would prove nothing"
    named = sorted(code for code in codes if code in page)
    assert not named, f"communes.html names indicators directly: {named}"


def test_the_sorted_indicator_is_never_dropped_from_the_card(page):
    """Decision 4, run for real rather than eyeballed: cardFigureCodes() must
    add the sorted indicator when it is not already a headline, leave the
    headline list alone when it already is, and invent nothing when both are
    empty (decision 9 -- no 'first few codes' fallback)."""
    fn = _extract_fn(page, "cardFigureCodes")
    harness = f"""
    var HEADLINE_CODES, sortKey, INDICATOR_CODES;
    {fn}
    function run(headlines, key, codes){{
      HEADLINE_CODES = headlines; sortKey = key; INDICATOR_CODES = codes;
      return cardFigureCodes();
    }}
    console.log(JSON.stringify({{
      notSortedByIndicator: run(['A','B'], 'name_en', ['A','B','C']),
      sortedByExistingHeadline: run(['A','B'], 'ind:B', ['A','B','C']),
      sortedByNonHeadline: run(['A','B'], 'ind:C', ['A','B','C']),
      noHeadlinesNoSort: run([], 'name_en', ['A','B','C']),
      noHeadlinesSorted: run([], 'ind:C', ['A','B','C']),
    }}));
    """
    out = _run_node(harness)
    assert out["notSortedByIndicator"] == ["A", "B"]
    assert out["sortedByExistingHeadline"] == [
        "A",
        "B",
    ], "the sorted headline must not be duplicated"
    assert out["sortedByNonHeadline"] == [
        "C",
        "A",
        "B",
    ], "the sorted-but-not-headline column must be added"
    assert (
        out["noHeadlinesNoSort"] == []
    ), "no invented fallback list when there are no headlines to show"
    assert out["noHeadlinesSorted"] == [
        "C"
    ], "the sorted indicator must still show with no headlines loaded"


# --- the five states stay distinct on a card --------------------------------


def test_a_suppressed_card_cell_goes_through_status_pill(page):
    """Same three-way distinction as render()'s table cells (absent vs
    suppressed-with-no-value vs a real value), run for real: a suppressed cell
    (empty value, status letter) must render the Suppressed pill, never a bare
    empty string and never the generic n/a a truly-absent cell gets."""
    status_pill = _extract_fn(page, "statusPill")
    format_value = _extract_fn(page, "formatValue")
    cell_html = _extract_fn(page, "cardCellHtml")
    harness = f"""
    var LANG = 'en';
    {format_value}
    {status_pill}
    {cell_html}
    console.log(JSON.stringify({{
      absent: cardCellHtml(undefined, 'count'),
      suppressed: cardCellHtml({{value: '', status: 'S'}}, 'count'),
      real: cardCellHtml({{value: '42', status: 'A'}}, 'count'),
    }}));
    """
    out = _run_node(harness)
    assert "pill na" in out["absent"] and "n/a" in out["absent"]
    assert (
        "pill suppressed" in out["suppressed"]
    ), "a suppressed cell did not render the Suppressed pill"
    assert (
        "pill na" not in out["suppressed"]
    ), "a suppressed cell must not collapse into the absent n/a pill"
    assert "42" in out["real"] and "pill final" in out["real"]


def test_card_cell_html_never_bypasses_the_shared_formatters(page):
    body = _extract_fn(page, "cardCellHtml")
    assert "statusPill(cell.status)" in body
    assert "formatValue(cell.value, unit)" in body
    assert "cell.value === ''" in body, "suppressed and absent must be told apart before formatting"


# --- the expander is built lazily -------------------------------------------


def test_the_all_indicators_expander_is_not_built_eagerly(page):
    """565 cards x 69 rows built up front is 39,000 nodes for a panel most
    readers never open. cardHTML() -- the per-card template used on every
    render() -- must only place the empty <details> shell."""
    card_html = _extract_fn(page, "cardHTML")
    assert "INDICATOR_CODES" not in card_html, "cardHTML() eagerly lists every indicator"
    assert '<details class="cc-expand"' in card_html
    assert "cc-all" not in card_html, "the expander's row markup is already present at render time"


def test_the_expander_is_filled_in_on_first_open_only(page):
    m = re.search(r"addEventListener\('toggle', e => \{.*?\}, true\);", page, re.DOTALL)
    assert m, "no delegated 'toggle' listener for the expander"
    body = m.group(0)
    assert "det.dataset.built" in body, "no guard against rebuilding on every open"
    assert "INDICATOR_CODES" in body, "the expander does not list every indicator"
    assert "cc-all" in body


# --- the mobile filter drawer ------------------------------------------------


def test_the_drawer_button_carries_aria_state_and_the_ids_it_names_exist(page):
    button = re.search(r'<button[^>]*id="filterToggle"[^>]*>', page)
    assert button, "no #filterToggle button"
    tag = button.group(0)
    assert 'aria-expanded="false"' in tag
    controls = re.search(r'aria-controls="([A-Za-z]+)"', tag)
    assert controls, "#filterToggle carries no aria-controls"
    assert (
        f'id="{controls.group(1)}"' in page
    ), f"aria-controls names {controls.group(1)}, which does not exist"


def test_status_stays_outside_the_drawer(page):
    """A reader must always be able to see how many communes the filters left,
    even while the drawer holding the filters themselves is collapsed."""
    drawer = re.search(
        r'<div class="filter-drawer drawer-hidden" id="filterDrawer">(.*?)\n    </div>',
        page,
        re.DOTALL,
    )
    assert drawer, "filter-drawer not found"
    assert 'id="status"' not in drawer.group(1)
    assert 'id="status"' in page, "status disappeared entirely"


def test_the_drawer_closes_on_escape(page):
    handler = re.search(r"addEventListener\('keydown', e => \{.*?\}\);", page, re.DOTALL)
    assert handler, "no keydown handler"
    assert "Escape" in handler.group(0)
    assert "setDrawerOpen(false)" in handler.group(0)


def test_the_drawer_is_a_panel_not_a_modal(page):
    """Decision 8: no focus trap, no scroll lock."""
    assert "trapFocus" not in page
    set_drawer = re.search(r"function setDrawerOpen\(open\)\{.*?\n\}", page, re.DOTALL)
    assert set_drawer, "setDrawerOpen not found"
    assert "body.style" not in set_drawer.group(
        0
    ), "the drawer touches document scrolling like a modal would"
    assert "overflow" not in set_drawer.group(0), "the drawer locks page scroll like a modal would"


def test_map_toggle_still_wires_to_the_map_panel_from_inside_the_drawer(page):
    """Moving #mapToggle inside the new drawer wrapper must not change what it
    points at."""
    assert 'id="mapToggle" aria-expanded="false" aria-controls="mapPanel"' in page
    assert 'id="mapPanel">' in page


# --- translations ------------------------------------------------------------


def test_new_i18n_keys_exist_in_all_three_languages():
    strings = json.loads(_node_i18n("JSON.stringify(I.STRINGS)"))
    for lang in ("en", "fr", "nl"):
        for key in NEW_I18N_KEYS:
            assert key in strings[lang], f"{lang} is missing {key}"
            assert strings[lang][key], f"{lang}.{key} is empty"


def test_new_i18n_keys_are_actually_referenced_by_the_page(page):
    used = set(re.findall(r'data-t(?:-\w+)?="([A-Za-z]+)"', page)) | set(
        re.findall(r"T\('([A-Za-z]+)'", page)
    )
    for key in NEW_I18N_KEYS:
        assert key in used, f"{key} was added to i18n.js but communes.html never asks for it"


def test_the_reused_profile_link_key_still_exists():
    """The card's link to local/{nis}/ reuses profiles.html's own 'See the
    profile' string rather than inventing a duplicate."""
    strings = json.loads(_node_i18n("JSON.stringify(I.STRINGS)"))
    for lang in ("en", "fr", "nl"):
        assert "pfSeeProfile" in strings[lang]


# --- nothing else about the page changed -------------------------------------


def test_the_table_itself_is_untouched(page):
    assert 'id="tableScroll"' in page
    assert 'id="tbody"' in page
    assert 'id="headRow"' in page


def test_the_page_still_carries_its_licence_attribution(page):
    assert '<div class="attribution" id="attribution">' in page
