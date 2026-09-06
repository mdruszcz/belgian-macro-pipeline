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

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

# Pages that render municipal (Statbel-derived) figures. all_data.html and
# dashboard.html are national-only (NBB / Eurostat / FPB) and so are not
# listed; add a page here the moment it starts showing commune data.
MUNICIPAL_PAGES = ["communes.html", "local.html"]


def _page(name: str) -> str:
    """Page source with runs of whitespace collapsed to single spaces.

    HTML collapses whitespace when rendered, so a phrase the reader sees as
    "does not endorse" may be split across source lines. Asserting on the raw
    bytes would make these tests fail on a reflow that changed nothing a
    visitor sees -- and a compliance test that fires on cosmetics is one that
    gets weakened.
    """
    return re.sub(r"\s+", " ", (REPO / name).read_text(encoding="utf-8"))


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
