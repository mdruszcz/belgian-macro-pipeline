"""Every page publishing Statbel-derived municipal data must carry Statbel's
required attribution.

This is a licence condition, not a style preference. Statbel's *Licentie open
data* of 22 October 2015 terminates the grant AUTOMATICALLY on
non-compliance (clause 6), and the CC BY 4.0 terms in Statbel's *Conditions
générales d'utilisation* carry their own attribution and "changes were made"
obligations. Both were confirmed to grant commercial reuse; we satisfy the
union of their obligations until Statbel says which governs. See
docs/data_catalog.md.

The obligations are on PUBLISHED OUTPUT, so the only way to keep them true is
to assert them against the published pages -- a doc note is what let this gap
open in the first place (data_catalog.md tracked it as a "Block K/J item"
while communes.html was already live).
"""

import json
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

# Pages that render municipal (Statbel-derived) figures. all_data.html and
# dashboard.html are national-only (NBB / Eurostat / FPB) and so are not
# listed; add a page here the moment it starts showing commune data.
MUNICIPAL_PAGES = ["communes.html", "local.html", "map.html", "home.html"]


def _rendered_strings() -> str:
    """Every language's interface strings from assets/i18n.js, as a browser
    gets them -- escapes resolved, not the source literals."""
    result = subprocess.run(
        [
            "node",
            "-e",
            # Values joined plainly, NOT JSON-stringified: stringifying escapes
            # the quotes in markup like id="attrUpdated", and the assertions
            # below look for the markup a reader's browser receives.
            "const I=require('./assets/i18n.js');"
            "process.stdout.write(I.LANGS.map(l => "
            "Object.values(I.STRINGS[l]).join('\\n')).join('\\n'))",
        ],
        capture_output=True,
        text=True,
        cwd=REPO,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def _page(name: str) -> str:
    """THE LICENCE SURFACE OF A PAGE: its own markup, plus the shared strings
    it renders.

    The notice used to be written out on every page, so reading the file was
    enough. It now lives once in assets/i18n.js, and a page that loads that
    module publishes those words just as surely as if they were inline --
    local.html renders the notice from it. Checking only the file would let
    every obligation below pass vacuously on a page whose notice had been
    deleted from the module.

    Runs of whitespace are collapsed because HTML collapses them when
    rendered: a phrase a reader sees as "does not endorse" may be split across
    source lines, and a compliance test that fires on a reflow is one that gets
    weakened.
    """
    text = (REPO / name).read_text(encoding="utf-8")
    if 'src="assets/i18n.js"' in text:
        text += "\n" + _rendered_strings()
    return re.sub(r"\s+", " ", text)


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_credits_statbel_as_the_source(page):
    """Obligation 1: attribution naming the producer."""
    text = _page(page)
    assert re.search(
        r"<strong>Source:</strong>\s*<a[^>]*statbel\.fgov\.be", text
    ), f"{page} publishes Statbel municipal data without a source credit"
    assert "Statistics Belgium" in text


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_links_the_licence(page):
    """Obligation 2: a link to the licence itself, not just its name."""
    assert "creativecommons.org/licenses/by/4.0/" in _page(page)


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_states_that_changes_were_made(page):
    """Obligation 3 (CC BY 4.0 §5.1). Naming the source is not enough -- we
    reshape, re-key and derive, and the notice must say so."""
    text = _page(page)
    assert "Changes were made" in text
    # ...and say what changed, rather than asserting it abstractly.
    assert "canonical" in text and "merger" in text


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_carries_a_date_of_last_update(page):
    """Obligation with teeth: the 2015 licence requires "de datum van de
    laatste bijwerking" in the attribution. The element must exist AND be
    filled from the data, never left as the em-dash placeholder."""
    text = _page(page)
    assert "Data last updated:" in text
    assert 'id="attrUpdated"' in text
    # Populated from the data, however that is expressed -- directly, or via
    # a variable holding the element. Asserting one exact syntax made this
    # fail on a refactor that kept the obligation perfectly intact.
    assert re.search(r"attrUpdated[^;]{0,120}textContent\s*=", text) or re.search(
        r"getElementById\('attrUpdated'\)[\s\S]{0,200}?textContent\s*=", text
    ), f"{page} has the update-date element but never populates it from the data"


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_disclaims_endorsement(page):
    """Obligation 4 (both documents): nothing may imply Statbel backs this."""
    assert "does not endorse" in _page(page)


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_labels_english_names_as_unofficial(page):
    """Obligation 5 (2015 licence §3): no Statbel file supplies English
    commune names, so every one shown here is our own translation and must
    not be presented as an official Statbel label."""
    text = _page(page)
    assert "unofficial" in text.lower()


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_attribution_is_not_hidden(page):
    """A notice that is present in the markup but invisible satisfies
    nothing. Guards the obvious ways it could be neutralised without
    deleting it."""
    text = _page(page)
    block = re.search(r'<div class="attribution"[^>]*>', text)
    assert block, f"{page} has no .attribution block"
    assert "display:none" not in block.group(0).replace(" ", "")
    assert "hidden" not in block.group(0)


# ── ONEM/RVA ────────────────────────────────────────────────────────────────
#
# A SECOND SOURCE WITH DIFFERENT OBLIGATIONS, on the same pages. ONEM's own
# reuse conditions (quoted in full in docs/data_catalog.md) permit commercial
# reuse and require two things: name the source, and state the date of the
# information used. They are NOT CC BY 4.0 -- there is no "changes were made"
# clause and no no-endorsement clause -- so the tests above must not be
# reused for it, and the page must not imply CC BY covers it either.


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_credits_onem_as_a_source(page):
    """ONEM obligation 1: "en mentionneront la source"."""
    text = _page(page)
    assert re.search(
        r"<a[^>]*onem\.be", text
    ), f"{page} publishes ONEM unemployment data without a source credit"
    assert "ONEM" in text and "Arbeidsvoorziening" in text


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_states_the_date_of_the_onem_information(page):
    """ONEM obligation 2: "indiqueront la date des informations utilisées".

    A different obligation from Statbel's date-of-last-update, and satisfied
    separately -- one date does not stand in for the other, because the two
    sources are refreshed on entirely different cadences.
    """
    text = _page(page)
    assert re.search(r"[Dd]ate (of the information used|des informations)", text) or (
        "gebruikte informatie" in text
    ), f"{page} credits ONEM but never states the date of the data used"


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_does_not_claim_cc_by_covers_onem(page):
    """The one way this could go quietly wrong: ONEM's data sitting under a
    CC BY notice it was never released under. The page carries CC BY for
    Statbel, so the ONEM credit has to say explicitly that it does not apply.
    """
    text = _page(page)
    assert re.search(
        r"not</em>\s*under\s*CC BY|<em>non</em>\s*sous CC BY|niet</em> onder CC BY", text
    ), f"{page} shows ONEM data alongside a CC BY notice without excluding it"


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_says_masked_cells_are_not_zero(page):
    """Not a licence condition -- a truthfulness one, and the reason it is
    tested here is that it is a claim about published output like the rest.
    ONEM withholds counts under 10, those cells are stored with a NULL value
    and status 'suppressed', and a reader must not read a blank as a zero.
    """
    text = _page(page)
    assert "suppressed" in text.lower() or "supprim" in text.lower() or "onderdrukt" in text.lower()
    assert "never as zero" in text or "jamais comme z" in text or "nooit als nul" in text


# ── police.be ────────────────────────────────────────────────────────────────
#
# A THIRD SOURCE, thinner than either above. police.be's own condition
# (quoted in full in docs/data_catalog.md) asks only that the source be
# credited correctly -- it says nothing about permitted uses, unlike
# Statbel's CC BY 4.0 or ONEM's explicit "commercial reuse permitted"
# clause. The page must not claim more than that condition actually grants.


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_credits_police_as_a_source(page):
    """police.be obligation: correctly indicate the source."""
    text = _page(page)
    assert re.search(
        r"<a[^>]*police\.be", text
    ), f"{page} publishes police.be data without a source credit"
    assert "Police" in text and ("ICT" in text or "polici" in text.lower())


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_does_not_claim_a_reuse_grant_police_never_made(page):
    """The one way this could go quietly wrong: implying police.be permits
    commercial reuse the way ONEM's text explicitly does, or sits under CC
    BY the way Statbel's does. Its own condition says neither."""
    text = _page(page)
    assert re.search(
        r"not.{0,20}stated grant|<em>non</em> sous CC BY.{0,20}sans octroi|"
        r"<em>niet</em> onder CC BY.{0,40}zonder uitdrukkelijke",
        text,
    ), f"{page} shows police.be data without disclaiming a reuse grant it never got"
    assert re.search(
        r"CC BY 4\.0, which does not apply|CC BY 4\.0.{0,10}ne s.applique pas|"
        r"CC BY 4\.0.{0,10}niet van toepassing",
        text,
    ), f"{page} shows police.be data without disclaiming CC BY"


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_flags_the_stale_geography_and_unstated_period(page):
    """Not a licence condition -- a truthfulness one. The house-burglary
    figure uses the pre-2025-merger commune map and an unconfirmed reporting
    window; both are in the module docstring of scripts/sync_police.py and
    must also reach the reader, not stay a code comment."""
    text = _page(page)
    assert "through 2024" in text or "jusqu'en 2024" in text or "tot en met 2024" in text
    assert (
        "provisional" in text.lower() or "provisoire" in text.lower() or "voorlopig" in text.lower()
    )


def _canonical_attribution() -> str:
    """The one wording of the licence notice, from assets/i18n.js.

    Rendered through node rather than regexed out of the file, so the test
    compares what a browser would actually get -- escapes resolved -- and not
    the source literal.
    """
    result = subprocess.run(
        [
            "node",
            "-e",
            "process.stdout.write(require('./assets/i18n.js').STRINGS.en.attribution)",
        ],
        capture_output=True,
        text=True,
        cwd=REPO,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


# Pages that carry the notice in their own MARKUP. local.html is deliberately
# absent: it is a JavaScript application, and its no-JavaScript story is the 565
# static /local/{nis} pages, which lift this same markup from communes.html.
HTML_ATTRIBUTION_PAGES = ["communes.html", "map.html"]


@pytest.mark.parametrize("page", HTML_ATTRIBUTION_PAGES)
def test_every_page_carries_the_one_canonical_licence_notice(page):
    """Replaces a weaker test that compared two pages to each other.

    The licence notice was written out four times: three languages in
    local.html plus an English copy each in communes.html and map.html, held in
    step only by a byte comparison between the last two. Four copies of a
    LICENCE CONDITION drifting apart is not untidiness -- it is the notice
    becoming wrong on some pages and not others.

    There is now one string, in assets/i18n.js, and every page's HTML must
    match it exactly. The HTML keeps the English rendering rather than being
    injected by script, because a reader with JavaScript disabled must still
    see the notice and scripts/export_local_pages.py lifts it from
    communes.html's markup to put on all 565 static pages.
    """
    canonical = _canonical_attribution()
    text = (REPO / page).read_text(encoding="utf-8")

    # A page that also draws the map carries a SECOND notice (the boundary
    # licence) in the same block, so it marks off the values notice explicitly
    # and that marked span is what must match. Keyed on the marker being
    # present rather than on a filename, so the next map-bearing page is held
    # to the same standard without editing this test.
    block = re.search(
        r"<!-- values-attribution:start.*?-->(.*?)<!-- values-attribution:end -->",
        text,
        re.DOTALL,
    ) or re.search(r'<div class="attribution" id="attribution">(.*?)</div>', text, re.DOTALL)
    assert block, f"{page} has no attribution block"

    def normalise(value):
        return re.sub(r"\s+", " ", value).strip()

    assert normalise(block.group(1)) == normalise(canonical), (
        f"{page}'s licence notice has drifted from assets/i18n.js. "
        "Change the string there, not the page."
    )


def test_the_canonical_notice_exists_in_all_three_languages():
    """A licence condition met only in English is met only for English readers,
    and the whole point of this file is that the condition is met."""
    result = subprocess.run(
        [
            "node",
            "-e",
            "const I=require('./assets/i18n.js');"
            "process.stdout.write(JSON.stringify("
            "I.LANGS.map(l => (I.STRINGS[l].attribution || '').length)))",
        ],
        capture_output=True,
        text=True,
        cwd=REPO,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    lengths = json.loads(result.stdout)
    assert len(lengths) == 3
    assert all(n > 2000 for n in lengths), f"a language has a truncated notice: {lengths}"


@pytest.mark.parametrize("page", MUNICIPAL_PAGES)
def test_page_says_a_derived_figures_date_belongs_to_its_inputs(page):
    """The ruling recorded in docs/features/provenance.md, asserted on the page.

    Point 2 of Statbel's 2015 licence requires the date of last update of the
    information reused, and the information reused in a derived figure IS its
    inputs -- they are published inside that number. Point 5 forbids
    misleading a reader about the update date. Both are satisfied only if the
    date is shown AND attributed to the inputs rather than to the figure, so
    the page has to say which it is showing.
    """
    text = _page(page)
    assert re.search(
        r"computed from these data carries the date its INPUTS", text
    ), f"{page} shows no derived-input date statement"
    assert (
        "computed, not retrieved" in text
    ), f"{page} does not distinguish a computed figure from a retrieved one"


def test_the_statement_exists_in_all_three_languages():
    """A licence condition met only in English is met only for English
    readers. Asserted against the shared strings, which is where all three
    now live."""
    text = _rendered_strings()
    assert "carries the date its INPUTS were last updated" in text
    assert "de dernière mise à jour de ses DONNÉES SOURCES" in text
    assert "waarop de BRONCIJFERS voor het laatst zijn bijgewerkt" in text


def test_the_commune_app_renders_the_notice_from_the_shared_module():
    """local.html held the notice three times, once per language. It now holds
    it zero times and renders it from assets/i18n.js, so the wording cannot
    differ between the app and the pages around it."""
    text = (REPO / "local.html").read_text(encoding="utf-8")
    assert "I18N.t(LANG, 'attribution')" in text, "the app does not render the shared notice"
    assert 'src="assets/i18n.js"' in text, "the app does not load the shared strings"
    # And keeps no copy of its own.
    assert "attribution: '" not in text, "local.html still carries its own copy of the notice"
    assert "Licentie open data" not in text, "licence prose is still inlined in local.html"
