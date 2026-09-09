"""The site's interface language (Block X).

Roadmap: "Mediocre French or Dutch signals a foreign product to a Belgian
public administration, which is fatal in this market." So the tests here are
about COMPLETENESS and CONSISTENCY rather than taste -- a key that exists in
English and not in Dutch falls back silently to English, and a page half
translated reads worse than one not translated at all.
"""

import json
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
I18N_JS = REPO / "assets" / "i18n.js"
COMPONENT_JS = REPO / "assets" / "commune_map.js"

# Pages that carry the language switcher and read the shared strings.
TRANSLATED_PAGES = ["communes.html", "map.html", "local.html"]

LANGS = ("en", "fr", "nl")


def _node(expression: str):
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


@pytest.fixture(scope="module")
def strings():
    return json.loads(_node("JSON.stringify(I.STRINGS)"))


# --- completeness ----------------------------------------------------------


def test_every_key_exists_in_every_language(strings):
    """LocalUI.t and I18N.t both fall back to English for a missing key, so an
    omission is invisible in testing and visible only to a French reader."""
    assert sorted(strings) == sorted(LANGS)
    keys = set(strings["en"])
    assert len(keys) > 40, "the table is suspiciously small"
    for lang in LANGS:
        missing = sorted(keys - set(strings[lang]))
        extra = sorted(set(strings[lang]) - keys)
        assert not missing, f"{lang} is missing {missing}"
        assert not extra, f"{lang} has keys English does not: {extra}"


def test_no_prose_string_is_left_untranslated(strings):
    """Catches a copy-paste left in English.

    Short interface words legitimately match across languages -- "Communes",
    "Province", "Auto" -- so only multi-word prose is checked, which is where
    an untranslated string would actually read as foreign.
    """
    untranslated = []
    for key, english in strings["en"].items():
        if len(english.split()) < 4:
            continue
        for lang in ("fr", "nl"):
            if strings[lang][key] == english:
                untranslated.append(f"{lang}.{key}")
    assert not untranslated, f"still in English: {untranslated}"


def test_placeholders_survive_translation(strings):
    """A translation that drops {n} renders a sentence with a hole in it, and a
    translation that invents {total} renders the brace to the reader."""
    for key, english in strings["en"].items():
        expected = set(re.findall(r"\{(\w+)\}", english))
        for lang in ("fr", "nl"):
            got = set(re.findall(r"\{(\w+)\}", strings[lang][key]))
            assert (
                got == expected
            ), f"{lang}.{key}: placeholders {sorted(got)} != {sorted(expected)}"


def test_the_licence_notice_is_complete_in_every_language(strings):
    """It is a licence condition, not copy. Each language must name all three
    sources and must not claim CC BY covers the two it does not."""
    for lang in LANGS:
        notice = strings[lang]["attribution"]
        assert "statbel.fgov.be" in notice, f"{lang} does not credit Statbel"
        assert "onem.be" in notice, f"{lang} does not credit ONEM"
        assert "police.be" in notice, f"{lang} does not credit the federal police"
        assert 'id="attrUpdated"' in notice, f"{lang} has no date-of-last-update slot"
        # CC BY is named once as granted and twice as NOT applying.
        assert notice.count("CC BY 4.0") >= 3, f"{lang} does not disclaim CC BY for the other two"


# --- language selection ----------------------------------------------------


def test_an_explicit_choice_beats_the_browser():
    out = json.loads(
        _node(
            "JSON.stringify({"
            "saved: I.initial({getItem: () => 'nl'}, 'fr-BE'),"
            "browser: I.initial({getItem: () => null}, 'fr-BE'),"
            "unknownBrowser: I.initial({getItem: () => null}, 'de-DE'),"
            "junkSaved: I.initial({getItem: () => 'xx'}, 'nl-BE'),"
            "})"
        )
    )
    assert out["saved"] == "nl", "a saved choice must win"
    assert out["browser"] == "fr", "a Belgian French browser should open in French"
    assert out["unknownBrowser"] == "en", "an unsupported language falls back to English"
    assert out["junkSaved"] == "nl", "a junk stored value must not be trusted"


def test_the_language_key_is_the_one_already_in_use():
    """local.html has persisted the reader's language under this key since it
    was the only translated page. Changing it would silently reset every
    existing reader's choice."""
    assert "belpulse-lang" in _node("I.STORAGE_KEY")


def test_substitution_and_fallback():
    out = json.loads(
        _node(
            "JSON.stringify({"
            "subbed: I.t('fr', 'nOfCommunes', {n: 19, total: 565}),"
            "missingKey: I.t('nl', 'noSuchKey'),"
            "unknownLang: I.t('de', 'statCommunes'),"
            "})"
        )
    )
    assert out["subbed"] == "19 communes sur 565"
    assert out["missingKey"] == "noSuchKey", "a missing key must be visible, not blank"
    assert out["unknownLang"] == "Communes", "an unknown language falls back to English"


# --- the pages -------------------------------------------------------------


@pytest.mark.parametrize("page", TRANSLATED_PAGES)
def test_a_translated_page_loads_the_shared_strings(page):
    text = (REPO / page).read_text(encoding="utf-8")
    assert 'src="assets/i18n.js"' in text, f"{page} does not load the shared strings"
    assert 'id="langSeg"' in text, f"{page} has no language switcher"
    for lang in LANGS:
        assert f'data-lang="{lang}"' in text, f"{page} cannot switch to {lang}"


@pytest.mark.parametrize("page", ["communes.html", "map.html"])
def test_every_key_a_page_asks_for_actually_exists(page, strings):
    """A typo in a data-t attribute renders the key itself on screen. Silent in
    English too, because the fallback is the key."""
    text = (REPO / page).read_text(encoding="utf-8")
    used = set()
    for attribute in ("data-t", "data-t-html", "data-t-title", "data-t-aria", "data-t-placeholder"):
        used |= set(re.findall(rf'{attribute}="([A-Za-z]+)"', text))
    # ...and the keys the page's own script asks for.
    used |= set(re.findall(r"T\('([A-Za-z]+)'", text))
    assert used, f"{page} marks nothing for translation"
    unknown = sorted(used - set(strings["en"]))
    assert not unknown, f"{page} asks for keys that do not exist: {unknown}"


@pytest.mark.parametrize("page", ["communes.html", "map.html"])
def test_the_english_stays_in_the_markup(page):
    """A reader whose JavaScript never runs must still get a complete page --
    including the licence notice, which is a condition of publishing at all.
    The strings file REPLACES text; it does not supply it."""
    text = (REPO / page).read_text(encoding="utf-8")
    assert re.search(
        r'data-t="communesTitle">Commune Data<|data-t="mapTitle">Commune Map<', text
    ), f"{page}'s heading is empty without JavaScript"
    assert "statbel.fgov.be" in text, f"{page} has no licence notice without JavaScript"


def test_the_map_component_holds_no_english_of_its_own():
    """It is loaded by pages in three languages, so a sentence left inside it
    would appear in English on all of them."""
    component = COMPONENT_JS.read_text(encoding="utf-8")
    # Strip comments: the reasoning is meant to be in English.
    body = re.sub(r"/\*.*?\*/", "", component, flags=re.DOTALL)
    body = re.sub(r"^\s*//.*$", "", body, flags=re.MULTILINE)
    # Only things that look like a SENTENCE: letters, spaces and prose
    # punctuation, nothing else. Matching any long quoted run picks up code
    # fragments caught between a template literal and an apostrophe.
    prose = [
        literal
        for literal in re.findall(r"'([A-Za-z][A-Za-z ,.;:'\u2019\u2014-]{17,})'", body)
        if len(literal.split()) >= 4
    ]
    assert not prose, f"user-facing English left in the component: {prose}"


# --- one language for the whole site ---------------------------------------


LEGACY_PAGES = ["index.html", "dashboard.html"]


@pytest.mark.parametrize("page", LEGACY_PAGES)
def test_the_older_pages_write_the_canonical_language_key(page):
    """THE BUG THIS FIXES, reproduced in a browser before it was fixed.

    index.html and dashboard.html have had their own translation tables and
    their own switcher since before the shared module existed, persisting to
    plain `lang` while local.html used `belpulse-lang`. Two language systems on
    one site, writing to two different keys: choosing French on the dashboard
    and clicking through to the commune table gave you English.
    """
    text = (REPO / page).read_text(encoding="utf-8")
    assert "belpulse-lang" in text, f"{page} does not write the site-wide language key"
    assert not re.search(
        r"setItem\(\s*['\"]lang['\"]", text
    ), f"{page} still writes the old key, so its choice will not carry"
    # ...and still READS the old one, so nobody loses a choice they made.
    assert re.search(
        r"getItem\(\s*['\"]lang['\"]", text
    ), f"{page} no longer reads the legacy key, so existing readers are reset"


def test_a_language_already_chosen_under_the_old_key_is_honoured():
    """A reader who picked Dutch on the dashboard last week must not be reset
    to English by this change."""
    out = json.loads(
        _node(
            "JSON.stringify({"
            "legacyOnly: I.initial({getItem: k => k === 'lang' ? 'nl' : null}, 'en-GB'),"
            "canonicalWins: I.initial({getItem: k => k === 'lang' ? 'nl' : 'fr'}, 'en-GB'),"
            "})"
        )
    )
    assert out["legacyOnly"] == "nl", "a choice under the old key is ignored"
    assert out["canonicalWins"] == "fr", "the canonical key must win when both exist"


@pytest.mark.parametrize("page", ["all_data.html"])
def test_the_thin_pages_are_translated_too(page):
    """Pages translated IN THE BROWSER: one URL, strings swapped by i18n.js.

    about.html was in this list until Batch 15d cut it over. It is still
    trilingual -- more so than before, since its body prose used to sit in a
    private `translations` table this test never checked -- but it is now
    trilingual by a DIFFERENT MECHANISM, three server-rendered URLs, so these
    assertions no longer describe it. The property is asserted for it below
    rather than dropped: a test removed because the page changed shape is how
    a guarantee quietly disappears.
    """
    text = (REPO / page).read_text(encoding="utf-8")
    assert 'src="assets/i18n.js"' in text, f"{page} does not load the shared strings"
    assert 'id="langSeg"' in text, f"{page} has no language switcher"
    assert "data-t=" in text, f"{page} marks nothing for translation"


@pytest.mark.parametrize("page", ["about.html"])
def test_a_page_translated_by_url_publishes_all_three(page):
    """Pages translated BY URL: three files, one per language, linked to each
    other. The stronger form -- a crawler and a reader with JavaScript off both
    get the French page, which the browser-swapped pages cannot offer.
    """
    english = (REPO / page).read_text(encoding="utf-8")
    assert 'class="bp-lang-switch"' in english, f"{page} has no language switcher"

    rendered = {"en": english}
    for lang in ("fr", "nl"):
        sibling = REPO / lang / page
        assert sibling.is_file(), f"{page} has no {lang} edition at /{lang}/{page}"
        rendered[lang] = sibling.read_text(encoding="utf-8")
        assert f'<html lang="{lang}">' in rendered[lang]

    assert len(set(rendered.values())) == 3, f"{page} renders identically in all three languages"
    for lang, text in rendered.items():
        for other in ("en", "fr", "nl"):
            href = f"{other}/{page}" if other != "en" else page
            assert href in text, f"the {lang} edition does not link to {other}"
