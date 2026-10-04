"""Static regression tests for the 2026-09-29 site-audit fixes batch.

An independent audit of the live public site found no wrong NUMBERS, but
several display defects a visitor actually sees: an untranslated raw i18n
key on screen, a page that scrolls sideways on a phone, no visible keyboard
focus on search boxes, facts drawn in a colour documented as decoration-only,
a growth rate labelled with the wrong unit ("pp"), euros placed
English-style on the French/Dutch pages, and English-only page titles even
after switching language. Each fix below has its own test; the shared
behaviour ones (i18n completeness, the euro placement logic, the
commune.html asset-version pins) live in tests/test_i18n.py,
tests/test_map_ui_logic.py and tests/test_commune_asset_versioning.py
instead of being duplicated here.

Static and deterministic throughout (rule 35): every check here reads a file
on disk and asserts on its text, with no build step and no browser -- the
browser-only checks (390px overflow, visible focus, title changing on a
language switch) are done by hand against a locally served copy before
pushing (see the PR body) rather than automated here, matching this repo's
existing balance between tests/test_*.py and the handful of
tests/test_*_browser.py files that actually launch Chromium.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]


# --- fix 2: profiles.html toolbar <select> overflow at 390px --------------


def test_profiles_toolbar_select_has_min_width_zero_at_every_breakpoint():
    """A flex item's default min-width is `auto` (its content's intrinsic
    width), so a <select> with a long option string grew past its container
    regardless of `max-width:none` -- reproduced: 618px wide inside a 390px
    viewport, causing sideways page scroll on profiles.html. `min-width:0`
    is the actual fix; `max-width` alone was never enough. Checked at both
    the base `.toolbar select` rule and its `@media (max-width:480px)`
    override, since the override fully replaces the flex properties."""
    text = (REPO / "profiles.html").read_text(encoding="utf-8")
    base = re.search(r"\.toolbar select\{([^}]*)\}", text)
    assert base, "no base .toolbar select rule found"
    assert "min-width:0" in base.group(1)
    mobile = re.search(r"@media \(max-width:480px\)\{.*?\.toolbar select\{([^}]*)\}", text, re.S)
    assert mobile, "no .toolbar select override inside the 480px media query"
    assert "min-width:0" in mobile.group(1)


# --- fix 3: visible focus ring on search/text inputs -----------------------


def test_layout_css_gives_searchbox_and_text_inputs_a_focus_ring():
    """assets/belpulse/layout.css had :focus-visible rings for menu items and
    the theme/language switches only (~lines 178-179); every page's own
    `.searchbox` sets its <input> to outline:0 with no border change on
    focus either, so a keyboard user tabbing to #communeSearch (home2.html,
    commune.html) or #pfSearch (profiles.html) saw no focus indicator at
    all. :focus-within on the wrapper is what actually shows a ring around
    the box; a bare text/search input with no .searchbox wrapper still gets
    its own :focus-visible outline restored directly."""
    layout_css = (REPO / "assets" / "belpulse" / "layout.css").read_text(encoding="utf-8")
    assert re.search(
        r"\.searchbox:focus-within\{[^}]*outline:", layout_css
    ), ".searchbox has no :focus-within outline rule"
    assert re.search(
        r'input\[type="text"\]:focus-visible,\s*input\[type="search"\]:focus-visible\{[^}]*outline:',
        layout_css,
    ), "bare text/search inputs have no :focus-visible outline rule"


def test_every_page_search_input_lives_inside_a_searchbox_wrapper():
    """The :focus-within fix only reaches an input whose wrapper actually
    carries the .searchbox class -- guards against a page's search input
    silently falling outside the fixed selector.

    Issue #312 batch 2: home2.html's commune search was rebuilt as a real
    ARIA 1.2 combobox (a visible label, a submit button, a listbox of
    suggestions) -- the old bare `<label class="searchbox"><input></label>`
    is gone, replaced by `<div class="herosearch-field">`, which carries
    the SAME focus-ring contract this test exists to guard
    (`.herosearch-field:focus-within` in home2.html's own <style>, the same
    border-colour + box-shadow treatment `.searchbox:focus-within` gives
    the other two pages). commune.html and profiles.html are unchanged."""
    for page, input_id, wrapper_open in (
        ("home2.html", "communeSearch", '<div class="herosearch-field">'),
        ("commune.html", "communeSearch", '<label class="searchbox">'),
        ("profiles.html", "pfSearch", '<label class="searchbox">'),
    ):
        text = (REPO / page).read_text(encoding="utf-8")
        m = re.search(re.escape(wrapper_open) + r'.*?id="' + input_id + r'"', text, re.S)
        assert m, f"{page}'s #{input_id} is not inside {wrapper_open!r}"

    # And home2.html's new wrapper really does carry its own focus-within
    # ring -- the property this test is actually guarding, not just the
    # wrapper's name.
    home2 = (REPO / "home2.html").read_text(encoding="utf-8")
    rule = re.search(r"\.herosearch-field:focus-within\{([^}]+)\}", home2)
    assert rule, "no .herosearch-field:focus-within rule in home2.html"


# --- fix 4: --bp-text-faint reserved for decoration, never a fact ---------

#: Selectors the handoff names as carrying a FACT (a status, a period, a
#: unit, a source/freshness line, the "last data update" label, a KPI note)
#: and therefore requiring --bp-text-muted's contrast (>=4.5:1), never
#: --bp-text-faint (~2.7:1, documented on the token itself as decoration
#: only). Checked in both commune.html's own <style> block and
#: assets/belpulse/macro_portrait.css (macro.html + micro.html); a selector
#: not styled in a given file is skipped there rather than failing, since
#: not every class exists on every page.
FACT_SELECTORS = (
    ".mtile-chip",
    ".mtile-period",
    ".value-unit",
    ".leadmap-caption .src",
    ".side-update .lbl",
    ".kpi-note",
)


@pytest.mark.parametrize(
    "path",
    ["commune.html", "assets/belpulse/macro_portrait.css"],
)
def test_fact_carrying_selectors_never_use_text_faint(path):
    text = (REPO / path).read_text(encoding="utf-8")
    offenders = []
    for selector in FACT_SELECTORS:
        # Selectors may be prefixed per-page (".macro .kpi-note", ".micro
        # .kpi-note") in macro_portrait.css, or bare in commune.html -- match
        # either, as long as the declaration block itself uses text-faint.
        pattern = re.compile(
            r"(?:^|[,{}])\s*(?:\.\w[\w-]*\s+)?" + re.escape(selector) + r"\s*\{([^}]*)\}",
            re.M,
        )
        for m in pattern.finditer(text):
            if "--bp-text-faint" in m.group(1):
                offenders.append(f"{selector}: {m.group(0)[:120]}")
    assert (
        not offenders
    ), f"{path} still uses --bp-text-faint on a fact-carrying selector:\n" + "\n".join(offenders)


def test_hero_credit_and_map_hint_keep_text_faint():
    """The two decorative exceptions the handoff explicitly keeps -- a
    regression here would mean the fix over-corrected and stripped faint
    from things that were never facts in the first place."""
    for path in ("commune.html", "assets/belpulse/macro_portrait.css"):
        text = (REPO / path).read_text(encoding="utf-8")
        assert re.search(
            r"\.hero-credit\{[^}]*--bp-text-faint", text
        ), f"{path}: .hero-credit lost --bp-text-faint"
        assert re.search(
            r"\.leadmap-caption \.hint\{[^}]*--bp-text-faint", text
        ), f"{path}: .leadmap-caption .hint lost --bp-text-faint"


# --- fix 5: GDP_ANNUAL_CY's unit ------------------------------------------


def test_gdp_annual_cy_unit_is_percent_yy_not_a_contribution():
    """GDP_ANNUAL_CY's own name ("Annual GDP growth") and definition
    ("Belgium's annual economic growth rate, year on year") describe the
    total growth rate itself, and its fetch query pulls B1GM -- GDP at
    market prices, the aggregate -- not an expenditure component. Its seven
    siblings (CHG_STOCKS_CY, GFCF_DWELLINGS_CY, GFCF_ENTERPRISES_CY,
    GFCF_PUBLIC_CY, GOV_CONSUMPTION_CY, NET_EXPORTS_CY,
    PRIV_CONSUMPTION_CY) correctly keep pp_contribution: each queries its own
    named expenditure component (P52, P51_DWE, P51_ENT, P51_PAD, P3_S13,
    B11, P31_S14_S15) and each one's own definition says, explicitly, "as a
    contribution to Belgium's GDP growth" -- wording GDP_ANNUAL_CY's
    definition does not carry. percent_yy is the unit GDP_QUARTERLY_YY (the
    same aggregate, at quarterly frequency) already uses."""
    config = yaml.safe_load(
        (REPO / "config" / "indicators" / "GDP_ANNUAL_CY.yaml").read_text(encoding="utf-8")
    )
    assert config["unit"] == "percent_yy"
    assert (
        "B1GM" in config["fetch"]["query"]
    ), "GDP_ANNUAL_CY no longer queries the GDP aggregate (B1GM)"


def test_gdp_annual_cy_siblings_still_correctly_carry_a_contribution_unit():
    """Regression guard in the opposite direction: this fix must not have
    been applied too broadly. Every *_CY sibling whose own definition says
    "contribution to Belgium's GDP growth" keeps pp_contribution."""
    for path in (REPO / "config" / "indicators").glob("*_CY.yaml"):
        if path.stem == "GDP_ANNUAL_CY":
            continue
        config = yaml.safe_load(path.read_text(encoding="utf-8"))
        definition_en = config.get("definition", {}).get("en", "")
        if "contribution to Belgium's GDP growth" in definition_en:
            assert config["unit"] == "pp_contribution", f"{path.stem} lost its pp_contribution unit"


# --- fix 7: page titles/descriptions localize, canonical tags exist -------

#: (page, i18n title key, i18n description key) for the six pages that
#: switch language client-side rather than by URL (docs/features/i18n.md).
TITLED_PAGES = (
    ("home2.html", "pageTitle_home2", "pageDesc_home2"),
    ("macro.html", "pageTitle_macro", "pageDesc_macro"),
    ("micro.html", "pageTitle_micro", "pageDesc_micro"),
    ("sources.html", "pageTitle_sources", "pageDesc_sources"),
    ("profiles.html", "pageTitle_profiles", "pageDesc_profiles"),
    ("map.html", "pageTitle_map", "pageDesc_map"),
)


@pytest.mark.parametrize("page,title_key,desc_key", TITLED_PAGES)
def test_page_title_and_description_are_wired_to_i18n(page, title_key, desc_key):
    """<title> and <meta name="description"> used to be plain English text
    baked into the HTML, so a reader who switched to French or Dutch (every
    OTHER string on the page re-translates via data-t) still saw an English
    browser-tab title and an English description in any shared link preview,
    in every language, forever -- nothing ever re-applied these two.
    I18N.applyStrings() now also handles title[data-t-doctitle] and
    [data-t-content], so wiring the attribute is what actually fixes it."""
    text = (REPO / page).read_text(encoding="utf-8")
    assert (
        f'<title data-t-doctitle="{title_key}">' in text
    ), f"{page}'s <title> is not wired to {title_key}"
    assert (
        f'data-t-content="{desc_key}"' in text
    ), f"{page}'s <meta description> is not wired to {desc_key}"
    # The static fallback text must still be present too (progressive
    # enhancement / no-JS / pre-paint), not replaced outright.
    assert '<meta name="description"' in text


@pytest.mark.parametrize("page,title_key,desc_key", TITLED_PAGES)
def test_page_title_and_description_keys_exist_in_all_three_languages(page, title_key, desc_key):
    import json
    import subprocess

    result = subprocess.run(
        [
            "node",
            "-e",
            "const I=require('./assets/i18n.js');process.stdout.write(JSON.stringify(I.STRINGS))",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=REPO,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    strings = json.loads(result.stdout)
    for lang in ("en", "fr", "nl"):
        assert strings[lang].get(title_key), f"{lang} has no {title_key}"
        assert strings[lang].get(desc_key), f"{lang} has no {desc_key}"


@pytest.mark.parametrize("page", [p for p, _, _ in TITLED_PAGES])
def test_page_has_a_self_referencing_canonical_link(page):
    """These six pages switch language client-side (one URL, re-translated in
    place) rather than by separate fr/.../nl/... URLs the way about.html and
    /local/{nis}/ do -- so a self-referencing <link rel="canonical"> is
    correct here, and an hreflang set (which about.html carries) would be
    WRONG: it would claim language-specific URLs that do not exist."""
    text = (REPO / page).read_text(encoding="utf-8")
    expected = (
        f'<link rel="canonical" href="https://mdruszcz.github.io/belgian-macro-pipeline/{page}">'
    )
    assert expected in text, f"{page} is missing its self-referencing canonical link"
    assert (
        "hreflang" not in text.split("</head>")[0]
    ), f"{page} claims per-language URLs via hreflang, but only has one URL"


# --- fix 8: ecoles-ise.html's NIS sentence ---------------------------------


def test_ecoles_ise_no_longer_claims_the_file_has_no_nis_code():
    """The old sentence ('Le fichier ne contient pas de code NIS communal.'
    and its fr/en/nl equivalents) was read as 'this data cannot be tied to a
    commune at all', when in fact scripts/export_schools_ise.py DOES resolve
    every site to a commune -- by the site register's own commune NAME,
    normalised and matched against config/geography/geographies.csv (with a
    merger-lineage fallback via municipality_crosswalk.csv) -- and only a
    small share (measured 2026-09-26: 48/4,005 = 1.2%, refused above 2%)
    could not be matched, shown under "commune unknown" rather than
    silently dropped or mis-assigned (docs/features/schools_ise.md, "Commune
    name resolution")."""
    text = (REPO / "ecoles-ise.html").read_text(encoding="utf-8")
    assert "ne contient pas de code NIS" not in text
    assert "has no municipality NIS code" not in text
    assert "bevat geen gemeentelijke NIS-code" not in text
    # The replacement sentence must appear in the static markup AND in all
    # three entries of the page's own `tr` translation table (4 occurrences
    # of the French sentence: once in <p id="comparison">, once as
    # tr.fr.comparison -- plus the true statement in en and nl).
    assert text.count("rattachées à leur commune par le nom de commune") == 2
    assert "matched to their commune by the commune name" in text
    assert "gekoppeld via de gemeentenaam" in text


def test_ecoles_ise_nav_matches_the_other_public_pages():
    """ecoles-ise.html had only Accueil/Profils communaux/Sources in its own
    hand-rolled nav (it predates the shared bp-shell header and was judged
    too large a rewrite to convert wholesale in this batch -- see the PR
    body); Macro and Cartes were missing even though every other public page
    links to all five."""
    text = (REPO / "ecoles-ise.html").read_text(encoding="utf-8")
    nav = re.search(r"<nav>(.*?)</nav>", text, re.S)
    assert nav, "no <nav> found in ecoles-ise.html's header"
    nav_html = nav.group(1)
    for href in (
        "home2.html",
        "profiles.html",
        "macro.html",
        "micro.html",
        "map.html",
        "sources.html",
    ):
        assert f'href="{href}"' in nav_html, f"ecoles-ise.html's nav is missing a link to {href}"


# --- fix 9: home2 images ----------------------------------------------------


def test_namur_card_is_lazy_and_async():
    text = (REPO / "home2.html").read_text(encoding="utf-8")
    m = re.search(r"<img[^>]*home2-namur-card\.png[^>]*>", text)
    assert m, "home2-namur-card.png <img> tag not found"
    tag = m.group(0)
    assert 'loading="lazy"' in tag
    assert 'decoding="async"' in tag


def test_home2_no_longer_carries_a_film_poster_at_all():
    """Issue #312 batch 2: the film -- and its poster -- left home2.html
    entirely (it now plays only on request, on about.html). The
    <picture>/<source webp>/<img id=filmPoster jpg> construct this file
    used to guard (and the #filmPoster id with it) has no reason to exist
    here any more; this test now pins its ABSENCE, the same reversal
    tests/test_home_film_hero.py's own rewrite documents in full."""
    text = (REPO / "home2.html").read_text(encoding="utf-8")
    assert 'id="filmPoster"' not in text
    assert "<picture>" not in text


def test_about_film_block_reuses_poster_webp_with_no_new_binary():
    """The poster.webp this file's own history is about (238KB, alongside
    poster.jpg, 243KB) is still reused -- just from its new home, the
    about.html film block's <video poster=...> (a video's own poster
    attribute cannot take a <picture>, so there is no webp/jpg fallback
    pair to guard here any more; render.py's _render_film() simply writes
    whatever the block's own `poster` prop says, and the approved mockup's
    own markup uses poster.webp directly). No new binary added (rule 12)."""
    for page, prefix in (("about.html", ""), ("fr/about.html", "../"), ("nl/about.html", "../")):
        text = (REPO / page).read_text(encoding="utf-8")
        assert f'poster="{prefix}assets/belpulse/home-film/poster.webp"' in text
    assert (REPO / "assets" / "belpulse" / "home-film" / "poster.webp").is_file()
    assert (REPO / "assets" / "belpulse" / "home-film" / "poster.jpg").is_file()
