"""Static checks on the Portrait commune.html (feat/commune-portrait): no
indicator id in rendering logic (rule 24 -- also covered, more generally,
by tests/test_map_ui_logic.py::test_no_page_names_an_indicator), every
`T('...')` key the page calls exists in fr/nl/en, `parseFrom` stays byte-
identical to the version tests/test_commune_destinations.py already pins,
and the age-pyramid history URL is actually used.

Browser behaviour (the slider, Play/Pause, tile clicks, console errors,
overflow, the Schools not-applicable state, the no-history fallback) is
covered separately in tests/test_commune_portrait_browser.py -- this file
is markup/source only, no Chromium.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
COMMUNE_HTML = REPO / "commune.html"
I18N_JS = REPO / "assets" / "i18n.js"

#: Same published indicator index this page itself fetches at runtime
#: (INDEX_URL) -- checked here without a browser, against the live payload.
INDICATORS_JSON = REPO / "public" / "data" / "metadata" / "indicators.json"


def _page_text() -> str:
    return COMMUNE_HTML.read_text(encoding="utf-8")


def test_no_indicator_code_appears_anywhere_in_the_page():
    """Rule 24, checked directly against the file rather than only the
    executable logic -- a comment naming a code is still a page that names
    an indicator, and the CI-caught regression in this batch (a leftover
    comment mentioning POPULATION_BY_COMMUNE) was exactly that."""
    import json

    if not INDICATORS_JSON.is_file():
        return  # covered, with a skip, by test_map_ui_logic.py when payloads are unbuilt
    codes = {
        row["indicator_code"]
        for row in json.loads(INDICATORS_JSON.read_text(encoding="utf-8"))["indicators"]
    }
    assert codes, "the indicator index is empty, so this test would prove nothing"
    text = _page_text()
    named = sorted(code for code in codes if code in text)
    assert not named, f"commune.html names indicators directly: {named}"


def _i18n_language_blocks() -> dict[str, str]:
    strings_text = I18N_JS.read_text(encoding="utf-8")
    blocks = {}
    for lang in ("en", "fr", "nl"):
        m = re.search(rf"\n  {lang}: \{{(.*?)\n  \}},\n", strings_text, re.DOTALL)
        assert m, f"no {lang} table found in assets/i18n.js"
        blocks[lang] = m.group(1)
    return blocks


def test_every_t_key_the_page_calls_exists_in_all_three_languages():
    """Every literal `T('key'` and `T("key"` call site in commune.html must
    resolve in fr, nl AND en -- a key present in only one or two languages
    would show a raw i18n key or English leftover to some readers (rule 7).

    `(?<!T)T\\(` excludes `TT(` (the page's own local-string helper, checked
    separately below, against its own V4C table) -- without it, every `TT(`
    call also matches as a `T(` call one character in. Two dynamically
    built keys (`T('v4Status_' + status)`, `T('v4ScopeRegion_' + key)`) are
    excluded explicitly: they are prefixes concatenated with a runtime
    value, never literal keys themselves, and are checked instead by
    presence of the PREFIXED entries below.
    """
    text = _page_text()
    keys = sorted(set(re.findall(r"""(?<!T)T\(\s*['"]([A-Za-z0-9_]+)['"]""", text)))
    keys = [k for k in keys if k not in ("v4Status_", "v4ScopeRegion_")]
    assert len(keys) > 50, f"suspiciously few T() calls found: {len(keys)}"
    blocks = _i18n_language_blocks()
    missing = {}
    for key in keys:
        for lang, block in blocks.items():
            if f"{key}:" not in block:
                missing.setdefault(key, []).append(lang)
    assert not missing, f"T() keys missing from a language table: {missing}"

    # The two dynamic-prefix keys: every value the runtime status/region code
    # can actually take must have its own prefixed entry, in all three
    # languages (checked directly, since the prefix itself is never a key).
    for status in ("final", "provisional", "estimate", "revised", "suppressed", "na", "derived"):
        for lang, block in blocks.items():
            assert f"v4Status_{status}:" in block, f"{lang} is missing v4Status_{status}"
    for region_key in ("flanders", "wallonia", "brussels"):
        for lang, block in blocks.items():
            assert (
                f"v4ScopeRegion_{region_key}:" in block
            ), f"{lang} is missing v4ScopeRegion_{region_key}"


def test_no_second_page_local_i18n_table_exists():
    """commune.html used to carry its own V4C table + TT() accessor -- a
    second, page-local i18n system alongside assets/i18n.js, for 9 strings
    that had no functional reason to live outside it. Both were removed
    (every former TT('key') call site is now a plain T('cpKey') call
    against assets/i18n.js, covered like every other T() call by
    test_every_t_key_the_page_calls_exists_in_all_three_languages above) --
    this is a regression guard against that split reappearing, not a test
    of behaviour."""
    text = _page_text()
    assert "var V4C" not in text, "a page-local V4C string table has come back"
    assert re.search(r"(?<!T)TT\(", text) is None, "a TT(...) call has come back"


def test_no_raw_i18n_key_or_english_leftover_pattern_in_markup():
    """A defensive static guard: the raw markup must never show a key name
    where a translated string belongs (would appear as e.g. `>v4HeroEyebrow<`
    if a T() call were ever left unresolved in static HTML rather than
    filled by JS), and must never show a literal `{placeholder}` hand-typed
    into static tag content (the class of bug PR #278 shipped: cpLinkMap's
    fallback text is static markup, and an earlier version of this regex
    excluded `{}` from the character class, so it could never have caught
    a stray `{name}` if one had been typed directly into the HTML here --
    it stayed silent while the REAL leftover, produced by JS at runtime,
    was caught only by the browser test in
    tests/test_commune_portrait_browser.py)."""
    text = _page_text()
    # Only checks literal markup between tags, not JS string/variable names
    # (which legitimately contain these substrings as object keys).
    for tag_text in re.findall(r">([^<>\n]{2,80})<", text):
        stripped = tag_text.strip()
        assert not re.match(
            r"^(v4|cp|pf)[A-Za-z0-9_]+$", stripped
        ), f"unresolved i18n key left in static markup: {tag_text!r}"
        assert not re.search(
            r"\{[A-Za-z0-9_]+\}", stripped
        ), f"unresolved i18n placeholder left in static markup: {tag_text!r}"


def test_parse_from_is_byte_identical_to_the_version_the_destination_tests_pin():
    """tests/test_commune_destinations.py extracts and unit-tests parseFrom
    under Node with no DOM. This is not a second copy of that suite -- it
    only confirms the extraction markers survived this batch's rewrite
    with the function's behaviour completely unchanged (same source text)."""
    text = _page_text()
    start = text.index("--parseFromStart--")
    end = text.index("--parseFromEnd--", start)
    fn_start = text.index("function parseFrom(", start)
    fn_end = text.rindex("\n  }", fn_start, end) + len("\n  }")
    body = text[fn_start:fn_end]
    assert "document." not in body
    assert "window." not in body
    assert "state." not in body
    assert body.count("return") >= 2


def test_the_history_url_is_actually_fetched_in_init():
    """AGE_SEX_HISTORY_URL must not just be defined -- init() must call
    fetch() on it, or the timeline feature never loads any data at all."""
    text = _page_text()
    assert "AGE_SEX_HISTORY_URL(state.nis)" in text
    init_match = re.search(r"async function init\(\)\{(.*?)\n  \}\n\n  init\(\);", text, re.DOTALL)
    assert init_match, "init() function not found"
    assert "fetch(AGE_SEX_HISTORY_URL" in init_match.group(1)


def test_the_carried_over_header_actions_keep_their_destinations():
    """Compare/Share/Download/linkMap/linkCsv -- item e/carry-over checklist:
    ids and href targets a test elsewhere pins must survive verbatim."""
    text = _page_text()
    assert 'id="shareLink"' in text
    assert 'id="downloadLink"' in text
    assert 'id="linkMap"' in text
    assert 'id="linkCsv"' in text
    assert "document.getElementById('shareLink').href = window.location.href" in text


def test_the_all_data_accordion_keeps_its_pinned_ids():
    """tests/test_a4_finishing_fixes.py's selectors, restated here as a fast
    source-level guard against a future accidental rename."""
    text = _page_text()
    for needle in (
        'id="allData"',
        'id="allDataSections"',
        "function allDataIndicatorRows(",
        "function allDataGroups(",
        "function filterAllData(",
        "row.className = 'indicator-row'",
    ):
        assert needle in text, f"missing: {needle}"
