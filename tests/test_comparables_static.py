"""Static checks on comparables.html (Peer Model v1, Block M): no indicator id
in the renderer's generic logic (rule 24), every T('...') key the page calls
(directly or through its reason -> key lookup tables) exists in en/fr/nl, and
the route is registered exactly as ecoles-ise.html was.

Browser behaviour (peer rendering, the scope switch, the withheld/unavailable
states, console errors, overflow) is covered separately in
tests/test_comparables_browser.py -- this file is markup/source only, no
Chromium.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
COMPARABLES_HTML = REPO / "comparables.html"
INDICATORS_JSON = REPO / "public" / "data" / "metadata" / "indicators.json"


def _page_text() -> str:
    return COMPARABLES_HTML.read_text(encoding="utf-8")


def _script_body() -> str:
    text = _page_text()
    m = re.search(r"<script>\s*\(function\(\)\{.*?\}\)\(\);\s*</script>", text, re.DOTALL)
    assert m, "comparables.html's inline renderer script was not found"
    return m.group(0)


def test_no_indicator_code_appears_anywhere_in_the_page():
    """Rule 24: comparables.html reads indicator ids only as data (object
    keys from the fetched payload/metadata), never names one directly in its
    own markup or script body."""
    if not INDICATORS_JSON.is_file():
        return  # nothing to check against if the payload has never been built
    codes = {
        row["indicator_code"]
        for row in json.loads(INDICATORS_JSON.read_text(encoding="utf-8"))["indicators"]
    }
    assert codes, "the indicator index is empty, so this test would prove nothing"
    text = _page_text()
    named = sorted(code for code in codes if code in text)
    assert not named, f"comparables.html names indicators directly: {named}"


def _inline_strings_table() -> dict[str, dict[str, str]]:
    """Parses the page's own inline `var STRINGS = {en: {...}, fr: {...}, nl:
    {...}}` object -- comparables.html keeps its strings inline rather than in
    assets/i18n.js, so this reads the same source the page itself runs, not a
    second, hand-copied list that could drift from it."""
    text = _page_text()
    m = re.search(r"var STRINGS\s*=\s*\{(.*?)\n\};\n", text, re.DOTALL)
    assert m, "no inline STRINGS table found in comparables.html"
    body = m.group(1) + "\n"  # the outer capture strips the final block's trailing newline
    blocks: dict[str, dict[str, str]] = {}
    for lang in ("en", "fr", "nl"):
        lang_m = re.search(rf"\n  {lang}:\s*\{{(.*?)\n  \}}\s*,?\s*\n", body, re.DOTALL)
        assert lang_m, f"no {lang} table found in comparables.html's STRINGS"
        keys = re.findall(r"(\w+):\s*'", lang_m.group(1))
        assert keys, f"{lang} table parsed as empty"
        blocks[lang] = dict.fromkeys(keys, "")
    return blocks


#: Keys reached only through a lookup table (titleKey / devWithheld*), not by
#: a literal T('key') call -- found by reading renderWithheld/formatDeviation.
_INDIRECT_KEYS = {
    "reasonExcluded",
    "reasonNoCurrentValue",
    "reasonSuppressed",
    "reasonNa",
    "reasonFewPeers",
}


def _direct_t_calls() -> set[str]:
    return set(re.findall(r"T\('([A-Za-z0-9]+)'", _script_body()))


def test_every_t_call_key_exists_in_all_three_languages():
    tables = _inline_strings_table()
    used = _direct_t_calls() | _INDIRECT_KEYS
    assert used, "no T(...) calls found -- parser likely broken, not the page"
    for lang, table in tables.items():
        missing = sorted(k for k in used if k not in table)
        assert not missing, f"{lang} is missing STRINGS keys used by the page: {missing}"


def test_the_three_language_tables_carry_the_same_key_set():
    tables = _inline_strings_table()
    en, fr, nl = tables["en"], tables["fr"], tables["nl"]
    assert set(en) == set(fr), f"fr differs from en: {set(en) ^ set(fr)}"
    assert set(en) == set(nl), f"nl differs from en: {set(en) ^ set(nl)}"


def test_ordinal_suffix_keys_are_present_for_every_language():
    # ordinal() falls back to T('ordN') for anything not 1st/2nd/3rd -- a
    # missing key here would render the literal string "ordN" in the table.
    tables = _inline_strings_table()
    for lang, table in tables.items():
        for key in ("ord1", "ord2", "ord3", "ordN"):
            assert key in table, f"{lang} is missing {key}"


def test_positionof_template_has_no_double_suffix_and_no_stray_placeholder():
    # Browser/wording finding (P1, blocker): ordinal() already appends the
    # language's suffix (ord1/ord2/ord3/ordN) to {pos} before positionOf is
    # composed, so the template itself must carry NEITHER a literal '{ord}'
    # placeholder (which T() never substitutes -- it is only ever called
    # with {pos, of}) NOR its own trailing ordinal letter, which would
    # double the suffix ordinal() already added (e.g. French '1ere', '10ee').
    tables = _inline_strings_table()
    text = _page_text()
    for lang in ("en", "fr", "nl"):
        m = re.search(rf"\n  {lang}:\s*\{{(.*?)\n  \}}\s*,?\s*\n", text, re.DOTALL)
        assert m, f"no {lang} table found"
        pos_m = re.search(r"positionOf:\s*'([^']*)'", m.group(1))
        assert pos_m, f"{lang} has no positionOf template"
        template = pos_m.group(1)
        assert "{ord}" not in template, f"{lang} positionOf still has a stray {{ord}}: {template!r}"
        assert template in (
            "{pos} of {of}",
            "{pos} sur {of}",
            "{pos} van {of}",
        ), f"{lang} positionOf changed shape unexpectedly: {template!r}"
    assert tables  # keeps the fixture call meaningful if the loop above is edited


def test_no_similarity_score_or_distance_is_rendered():
    # The handoff bans a SIMILARITY SCORE or DISTANCE shown for a peer --
    # not the word "similarity" describing how peers are chosen in general
    # (the model footnote explains the model in exactly those terms, and the
    # STRINGS table's own copy uses the word legitimately). Checked against
    # the renderer's CODE only (STRINGS's string literals stripped first): it
    # must never read a "similarity" or "distance" field off a peer/entry
    # object such as `entry.similarity` or `e.distance`.
    script = _script_body()
    code_only = re.sub(r"'(?:[^'\\]|\\.)*'", "''", script)  # blank out '...' string literals
    assert "similarity" not in code_only.lower()
    assert "distance" not in code_only.lower()


def test_route_is_registered_like_ecoles_ise():
    from src.pages import semantics
    from src.site import routes

    root_paths = {p.route for p in routes.ROOT_PAGES}
    assert "/comparables.html" in root_paths
    assert "/ecoles-ise.html" in root_paths
    assert "/comparables.html" in semantics.ROUTE_EXACT
    assert "/ecoles-ise.html" in semantics.ROUTE_EXACT


def test_page_carries_a_noindex_meta_tag():
    # comparables.html is reachable only with ?nis=, same reasoning as
    # commune.html -- its bare URL carries no NIS for a crawler to index.
    assert '<meta name="robots" content="noindex">' in _page_text()


def test_unit_suffix_wrapper_avoids_the_known_shared_map_gaps():
    # assets/commune_map.js's shared MapUI.unitSuffix has two known gaps --
    # eur_per_month/eur_per_inhabitant (already embedded in formatValue's own
    # number, so appending its label duplicates it: "€75 /mo. eur per
    # month") and per_10000_* (no vocabulary entry, so it leaks the raw
    # English unit code regardless of page language: "49.7 per 10000
    # dwellings" even on the French page). commune.html patches around both
    # with its own local unitSuffix() wrapper; comparables.html must do the
    # same rather than calling MapUI.unitSuffix directly from formatValue.
    script = _script_body()
    m = re.search(r"function unitSuffix\(unit\)\{(.*?)\n\}\n", script, re.DOTALL)
    assert m, "no local unitSuffix(unit) wrapper found in comparables.html"
    wrapper_body = m.group(1)
    assert "eur_per_month" in wrapper_body and "eur_per_inhabitant" in wrapper_body
    assert "per_10000_" in wrapper_body

    fmt_m = re.search(r"function formatValue\(value, meta\)\{(.*?)\n\}\n", script, re.DOTALL)
    assert fmt_m, "no formatValue(value, meta) found in comparables.html"
    assert "unitSuffix(unit)" in fmt_m.group(1)
    assert "MapUI.unitSuffix" not in fmt_m.group(1)


def test_default_scope_is_region_per_maintainer_decision():
    # 2026-09-27 maintainer decision: same-region is the default view, with a
    # switch to all of Belgium.
    text = _page_text()
    assert 'data-scope="region" aria-pressed="true"' in text
    assert 'data-scope="national" aria-pressed="false"' in text
    assert re.search(r"scope:\s*'region'", _script_body())
