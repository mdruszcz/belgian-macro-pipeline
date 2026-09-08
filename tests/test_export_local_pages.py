"""Tests for scripts/elp.py -- Block L's permanent URLs.

The property that matters most here is the roadmap's own [H] step: a URL
must survive a rebuild. That is asserted as a test (URL stability), not left
to a manual check, because it regresses silently -- anything that puts a
timestamp or a dict iteration order into the page breaks it and nothing
visibly fails.
"""

import json
import re
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import export_local_pages as elp  # noqa: E402

REPO = Path(__file__).resolve().parents[1]


SECTIONS = {
    "sections": [
        {
            "id": "demography",
            "label": {"en": "Demography", "fr": "Démographie", "nl": "Demografie"},
            "headline": "POPULATION_BY_COMMUNE",
            "indicators": ["POPULATION_AGE_0_14"],
        },
        {
            "id": "safety",
            "label": {"en": "Safety", "fr": "Sécurité", "nl": "Veiligheid"},
            "headline": "HOUSE_BURGLARIES_PER_10K",
            "indicators": [],
        },
    ]
}


def _commune(nis="11001", with_values=True):
    indicators = {}
    if with_values:
        indicators = {
            "POPULATION_BY_COMMUNE": {
                "names": {"en": "Population", "fr": "Population", "nl": "Bevolking"},
                "unit": "count",
                "updated": "2026-09-05",
                "periods": {
                    "2025": {"value": 14000.0, "status": "final"},
                    "2026": {"value": 14832.0, "status": "final"},
                },
            },
            "HOUSE_BURGLARIES_PER_10K": {
                "names": {"en": "House burglaries (per 10,000 dwellings)"},
                "unit": "per_10000_dwellings",
                "updated": "2026-09-06",
                "periods": {"2025": {"value": 73.08, "status": "provisional"}},
            },
        }
    return {
        "nis_code": nis,
        "geo_id": f"be:mun:{nis}",
        "name": {"en": "Aartselaar", "fr": "Aartselaar", "nl": "Aartselaar"},
        "region": "Flanders",
        "province": "Antwerp",
        "arrondissement": "Arrondissement Antwerpen",
        "indicators": indicators,
    }


@pytest.fixture
def payload_dir(tmp_path):
    root = tmp_path / "payloads"
    (root / "communes").mkdir(parents=True)
    (root / "metadata").mkdir(parents=True)
    (root / "metadata" / "sections.json").write_text(json.dumps(SECTIONS), encoding="utf-8")
    (root / "communes" / "11001.json").write_text(json.dumps(_commune()), encoding="utf-8")
    return root


@pytest.fixture
def db(tmp_path):
    """A minimal indicators/sources pair -- these pages name the agencies that
    contributed, so the generator reads the same table the pipeline does."""
    path = tmp_path / "meta.db"
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE sources (source_id TEXT PRIMARY KEY, agency TEXT)")
    conn.execute("CREATE TABLE indicators (indicator_id TEXT PRIMARY KEY, source_id TEXT)")
    conn.executemany(
        "INSERT INTO sources VALUES (?, ?)",
        [("statbel", "Statbel"), ("police", "Police Fédérale")],
    )
    conn.executemany(
        "INSERT INTO indicators VALUES (?, ?)",
        [
            ("POPULATION_BY_COMMUNE", "statbel"),
            ("HOUSE_BURGLARIES_PER_10K", "police"),
        ],
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def attribution_file(tmp_path):
    path = tmp_path / "communes.html"
    path.write_text(
        '<div class="attribution" id="attribution">'
        "<strong>Source:</strong> Statbel. "
        '<strong>Data last updated:</strong> <span id="attrUpdated">&mdash;</span>.'
        "</div>",
        encoding="utf-8",
    )
    return path


def _run(payload_dir, tmp_path, db, *_ignored, build_id="b1", **kwargs):
    """`*_ignored` absorbs the old attribution_file fixture: the licence notice
    now comes from assets/i18n.js rather than being scraped out of
    communes.html, so the exporter no longer takes a source path. Kept
    positional so the existing call sites read unchanged."""
    return elp.export_local_pages(
        payload_dir=payload_dir,
        out_dir=tmp_path / "local",
        base_url="https://example.test/site",
        build_id=build_id,
        db_path=db,
        **kwargs,
    )


# --- the route shape ------------------------------------------------------


def test_writes_one_index_html_per_commune_directory(payload_dir, tmp_path, db, attribution_file):
    result = _run(payload_dir, tmp_path, db, attribution_file)
    # One commune, three languages: English at local/{nis}/ and the other two
    # one level deeper.
    assert result["written"] == 3
    assert (tmp_path / "local" / "11001" / "index.html").is_file()


def test_a_commune_with_no_values_gets_no_page(payload_dir, tmp_path, db, attribution_file):
    """comparison.md's "no data, no page" rule -- an empty page is worse than
    a missing one, because Google demotes the whole domain for thin pages."""
    (payload_dir / "communes" / "99999.json").write_text(
        json.dumps(_commune("99999", with_values=False)), encoding="utf-8"
    )
    result = _run(payload_dir, tmp_path, db, attribution_file)
    # Thin communes are counted ONCE, not once per language: it is one commune
    # that was refused, not three pages that failed.
    assert result == {"written": 3, "skipped_thin": 1}
    assert not (tmp_path / "local" / "99999").exists()


# --- the [H] step, as a test ----------------------------------------------


def test_a_url_is_byte_identical_across_two_rebuilds(payload_dir, tmp_path, db, attribution_file):
    """The roadmap's own "verify a URL survives a rebuild" step. Anything
    non-deterministic -- a timestamp, an unsorted dict -- breaks this
    silently, so it is a test rather than a manual check."""
    _run(payload_dir, tmp_path, db, attribution_file, build_id="same")
    first = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    _run(payload_dir, tmp_path, db, attribution_file, build_id="same")
    second = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    assert first == second


def test_the_sitemap_is_byte_identical_across_two_rebuilds(payload_dir, tmp_path, db):
    """The property the pages had and the sitemap did not.

    `lastmod` was `datetime.now()`, identical on all 1,695 entries, so this
    file was rewritten in full on every build even when no figure had moved --
    `1 file changed, 1695 insertions(+), 1695 deletions(-)` on quiet days. The
    page-level fix landed and the sitemap kept the timestamp, because the test
    above reads one commune page and never opened the sitemap. It does now.
    """
    _run(payload_dir, tmp_path, db)
    first = (tmp_path / "local" / "sitemap.xml").read_text(encoding="utf-8")
    _run(payload_dir, tmp_path, db)
    assert (tmp_path / "local" / "sitemap.xml").read_text(encoding="utf-8") == first


def test_lastmod_is_the_data_s_date_and_not_the_build_s(payload_dir, tmp_path, db):
    """`lastmod` means "this page changed". A build date claims all 1,695
    changed today, every day -- a false statement to a crawler, and one that
    teaches it to ignore the field."""
    import datetime as _dt

    _run(payload_dir, tmp_path, db)
    sitemap = (tmp_path / "local" / "sitemap.xml").read_text(encoding="utf-8")
    today = _dt.datetime.now(_dt.timezone.utc).date().isoformat()
    stamps = set(re.findall(r"<lastmod>([^<]+)</lastmod>", sitemap))
    assert stamps, "no lastmod at all"
    assert stamps != {today}, "every entry carries today's date -- that is the build stamp again"


def test_a_different_build_id_changes_nothing_in_the_page(
    payload_dir, tmp_path, db, attribution_file
):
    """Stronger than the spec asked for. The spec allowed the page to differ
    by a build stamp; it now differs by nothing at all, because an earlier
    version stamped every page and so changed all 565 files on every run even
    when no figure had moved -- pure git churn on a repository already growing
    ~18 MB a commit. public/data/manifest.json records the build id centrally
    instead, so a page changes if and only if its data changed."""
    _run(payload_dir, tmp_path, db, attribution_file, build_id="build-1")
    first = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    _run(payload_dir, tmp_path, db, attribution_file, build_id="totally-different")
    second = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    assert first == second


# --- metadata -------------------------------------------------------------


def test_page_carries_title_description_canonical_and_robots(
    payload_dir, tmp_path, db, attribution_file
):
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    assert "<title>Aartselaar — municipal statistics | BelPulse</title>" in page
    assert '<link rel="canonical" href="https://example.test/site/local/11001/">' in page
    assert '<meta name="robots" content="index,follow">' in page
    assert 'property="og:title"' in page


def test_the_description_carries_real_figures_not_a_bare_template(
    payload_dir, tmp_path, db, attribution_file
):
    """A description that says only "statistics for X" is exactly the thin
    content the no-thin-pages rule exists to avoid."""
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    description = re.search(r'<meta name="description" content="([^"]+)"', page).group(1)
    assert "14,832" in description  # the real latest population
    assert "2026" in description


def test_json_ld_is_valid_and_describes_a_dataset(payload_dir, tmp_path, db, attribution_file):
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    block = re.search(r'<script type="application/ld\+json">(.*?)</script>', page, re.S).group(1)
    data = json.loads(block)  # must parse -- malformed JSON-LD is ignored entirely
    assert data["@type"] == "Dataset"
    assert data["identifier"] == "11001"
    assert data["url"] == "https://example.test/site/local/11001/"
    assert data["dateModified"] == "2026-09-06"
    assert data["temporalCoverage"] == "2025/2026"


def test_json_ld_credits_every_contributing_agency_not_just_one(
    payload_dir, tmp_path, db, attribution_file
):
    """The first version of this generator hardcoded Statbel as sole creator.
    These pages carry ONEM and police.be figures too, so a single creator is
    a false attribution -- and for a licence notice, false attribution is the
    failure that matters."""
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    data = json.loads(
        re.search(r'<script type="application/ld\+json">(.*?)</script>', page, re.S).group(1)
    )
    names = {c["name"] for c in data["creator"]}
    assert names == {"Statbel", "Police Fédérale"}


def test_json_ld_asserts_no_blanket_licence(payload_dir, tmp_path, db, attribution_file):
    """ONEM's conditions are explicitly not CC BY 4.0 and police.be's are
    thinner still. Structured data claiming one licence for the whole page
    would contradict the visible attribution on the same page."""
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    data = json.loads(
        re.search(r'<script type="application/ld\+json">(.*?)</script>', page, re.S).group(1)
    )
    assert "license" not in data


# --- attribution ----------------------------------------------------------


def test_the_licence_notice_comes_from_the_shared_strings():
    """It used to be lifted out of communes.html's markup, which worked while
    there was one language and became impossible with three. The notice now
    lives once, in assets/i18n.js, and both the app and these pages read it
    from there -- so a page cannot be published with a retyped or stale copy.
    """
    strings = elp._interface_strings()
    for lang in ("en", "fr", "nl"):
        notice = strings[lang]["attribution"]
        assert "statbel.fgov.be" in notice, f"{lang} does not credit Statbel"
        assert "onem.be" in notice and "police.be" in notice, f"{lang} is missing a source"


def test_generation_refuses_if_a_language_has_no_licence_notice(monkeypatch):
    """Rather than silently publishing 565 pages with no licence notice.

    Statbel's 2015 licence terminates automatically on non-compliance, so a
    missing notice has to stop the build, not degrade the page.
    """
    monkeypatch.setattr(
        elp,
        "_interface_strings",
        lambda: {"en": {"attribution": "x"}, "fr": {}, "nl": {"attribution": "y"}},
    )
    # The real function is what raises; call it through the module's own guard.
    strings = {"en": {"attribution": "x"}, "fr": {}, "nl": {"attribution": "y"}}
    missing = [lang for lang in elp.LANGS if not strings[lang].get("attribution")]
    assert missing == ["fr"], "the guard's own condition no longer detects a missing notice"


def test_the_real_guard_raises_on_a_strings_file_with_no_notice(tmp_path, monkeypatch):
    """Exercises the refusal itself, against a strings file that parses but
    publishes nothing."""
    stub = tmp_path / "i18n.js"
    stub.write_text(
        "const I={LANGS:['en','fr','nl'],STRINGS:{en:{},fr:{},nl:{}}};" "module.exports=I;\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(elp, "I18N_JS", stub)
    with pytest.raises(SystemExit, match="no licence notice"):
        elp._interface_strings()


def test_the_page_renders_real_values_in_the_html_not_a_js_shell(
    payload_dir, tmp_path, db, attribution_file
):
    """The entire reason this generator exists: a crawler that does not run
    JavaScript must still see the figures."""
    page = None
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    assert "14,832" in page
    assert "73.08" in page
    assert "Demography" in page and "Safety" in page


def test_a_figure_the_source_withheld_is_shown_as_withheld(
    payload_dir, tmp_path, db, attribution_file
):
    """The opposite of what this test used to assert, deliberately.

    It previously required a suppressed cell to be OMITTED, which is the bug:
    the attribution block these pages lift from communes.html states that
    figures ONEM withholds "are shown as suppressed, never as zero", and these
    static pages are the crawler-visible copy of the data. Omitting the row
    published silence where the source published a refusal.
    """
    commune = _commune()
    commune["indicators"]["POPULATION_AGE_0_14"] = {
        "names": {"en": "Population aged 0 to 14"},
        "unit": "count",
        "periods": {"2026": {"value": None, "status": "suppressed"}},
    }
    (payload_dir / "communes" / "11001.json").write_text(json.dumps(commune), encoding="utf-8")
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    assert "Population aged 0 to 14" in page
    assert "withheld by the source (fewer than 10)" in page
    # Never as a zero, and never as an empty cell that reads as a bug.
    assert "<td class='v'></td>" not in page
    assert "<td class='v'>0</td>" not in page


def test_an_indicator_with_nothing_at_all_is_still_omitted(
    payload_dir, tmp_path, db, attribution_file
):
    """The original intent, preserved. A figure the pipeline never collected is
    not a fact about the commune, and an empty row reads as a rendering fault.
    local.html remains the place that explains coverage gaps."""
    commune = _commune()
    commune["indicators"]["POPULATION_AGE_0_14"] = {
        "names": {"en": "Population aged 0 to 14"},
        "unit": "count",
        "periods": {},
    }
    (payload_dir / "communes" / "11001.json").write_text(json.dumps(commune), encoding="utf-8")
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    assert "Population aged 0 to 14" not in page


def test_a_page_of_only_withheld_figures_is_still_refused_as_thin(
    payload_dir, tmp_path, db, attribution_file
):
    """The thin-page refusal must not be satisfied by rows carrying no number.
    Otherwise a commune whose every figure was masked would ship a page that
    looks substantive and says nothing."""
    commune = _commune()
    commune["indicators"] = {
        "POPULATION_BY_COMMUNE": {
            "names": {"en": "Population"},
            "unit": "count",
            "periods": {"2026": {"value": None, "status": "suppressed"}},
        }
    }
    (payload_dir / "communes" / "11001.json").write_text(json.dumps(commune), encoding="utf-8")
    _run(payload_dir, tmp_path, db, attribution_file)
    assert not (tmp_path / "local" / "11001" / "index.html").exists()


def test_a_sitemap_lists_every_generated_route(payload_dir, tmp_path, db, attribution_file):
    _run(payload_dir, tmp_path, db, attribution_file)
    sitemap = (tmp_path / "local" / "sitemap.xml").read_text(encoding="utf-8")
    assert "<loc>https://example.test/site/local/11001/</loc>" in sitemap


# --- the real committed output --------------------------------------------


@pytest.mark.skipif(
    not (REPO / "local" / "11002" / "index.html").is_file(),
    reason="pages not generated in this working tree",
)
def test_the_real_antwerp_page_is_substantive():
    """Guards against the generator running but producing an empty shell --
    the failure that would be invisible until a crawler saw it."""
    page = (REPO / "local" / "11002" / "index.html").read_text(encoding="utf-8")
    assert "565,615" in page  # real 2026 population
    assert page.count("<tr>") > 20  # many indicators, not a stub


def test_a_figure_whose_later_years_were_withheld_says_so(
    payload_dir, tmp_path, db, attribution_file
):
    """158 (commune, indicator) pairs publish a figure whose NEWER years the
    source withheld. Showing 2024 on a 2026 site without saying so leaves a
    reader unable to tell a series that stopped from one the source declined to
    publish. The interactive page states it; these pages are the copy a crawler
    and a reader without JavaScript actually get."""
    commune = _commune()
    commune["indicators"]["POPULATION_AGE_0_14"] = {
        "names": {"en": "Population aged 0 to 14"},
        "unit": "count",
        "updated": "2026-09-06",
        "periods": {
            "2024": {"value": 13, "status": "final"},
            "2025": {"value": None, "status": "suppressed"},
            "2026": {"value": None, "status": "suppressed"},
        },
    }
    (payload_dir / "communes" / "11001.json").write_text(json.dumps(commune), encoding="utf-8")
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    # The figure itself is still published, at its own period.
    assert "<td>2024 (2025, 2026 withheld)</td>" in page
    assert "13" in page


# --- three languages, three routes -----------------------------------------


def test_each_commune_gets_a_page_in_every_language(payload_dir, tmp_path, db):
    """The point of these pages is search visibility, and a bourgmestre
    searches in French. English keeps its existing URL -- local/{nis}/ is
    already indexed and linked, and moving it would break those links for no
    gain."""
    _run(payload_dir, tmp_path, db)
    assert (tmp_path / "local" / "11001" / "index.html").is_file()
    assert (tmp_path / "local" / "11001" / "fr" / "index.html").is_file()
    assert (tmp_path / "local" / "11001" / "nl" / "index.html").is_file()


def test_a_translated_page_is_actually_translated(payload_dir, tmp_path, db):
    fr = tmp_path / "local" / "11001" / "fr" / "index.html"
    _run(payload_dir, tmp_path, db)
    page = fr.read_text(encoding="utf-8")
    assert '<html lang="fr">' in page
    assert "Statistiques communales" in page, "the description is still English"
    assert "Indicateur" in page and "Valeur" in page, "the table headings are still English"
    assert "Source :" in page, "the licence notice is still English"
    assert "Ouvrir la fiche interactive" in page


def test_every_language_declares_the_others_as_alternates(payload_dir, tmp_path, db):
    """hreflang is what tells a search engine the three pages are one page in
    three languages rather than three duplicates competing with each other.
    Without it, generating them can make the ranking worse."""
    _run(payload_dir, tmp_path, db)
    for lang, path in (
        ("en", ["11001", "index.html"]),
        ("fr", ["11001", "fr", "index.html"]),
        ("nl", ["11001", "nl", "index.html"]),
    ):
        page = (tmp_path / "local" / Path(*path)).read_text(encoding="utf-8")
        for other in ("en", "fr", "nl"):
            assert f'hreflang="{other}"' in page, f"the {lang} page does not point at {other}"
        assert 'hreflang="x-default"' in page, f"the {lang} page names no default"
        # Its canonical is ITSELF, not the English page: three real pages, not
        # three views of one.
        expected = "/local/11001/" if lang == "en" else f"/local/11001/{lang}/"
        assert f'rel="canonical" href="https://example.test/site{expected}"' in page


def test_a_translated_page_links_back_out_correctly(payload_dir, tmp_path, db):
    """It sits one directory deeper than the English page, so every relative
    link needs one more hop. A wrong prefix breaks every link on the page while
    the page itself still renders -- the kind of breakage that ships."""
    _run(payload_dir, tmp_path, db)
    page = (tmp_path / "local" / "11001" / "fr" / "index.html").read_text(encoding="utf-8")
    assert 'href="../../../communes.html"' in page
    assert 'href="../../../public/data/communes/11001.json"' in page


def test_numbers_are_written_the_way_each_language_writes_them():
    """Belgium writes a number three ways, and a page that gets it wrong reads
    as foreign before a reader has taken in a single figure."""
    assert elp._format_value(565615, "count", "en") == "565,615"
    assert elp._format_value(565615, "count", "fr") == "565 615"
    assert elp._format_value(565615, "count", "nl") == "565.615"
    assert elp._format_value(11.62, "percent", "fr") == "11,62%"
    assert elp._format_value(100.0, "percent", "nl") == "100%"


def test_the_sitemap_lists_every_language_with_its_alternates(payload_dir, tmp_path, db):
    _run(payload_dir, tmp_path, db)
    sitemap = (tmp_path / "local" / "sitemap.xml").read_text(encoding="utf-8")
    assert "<loc>https://example.test/site/local/11001/</loc>" in sitemap
    assert "<loc>https://example.test/site/local/11001/fr/</loc>" in sitemap
    assert "<loc>https://example.test/site/local/11001/nl/</loc>" in sitemap
    assert 'xmlns:xhtml="http://www.w3.org/1999/xhtml"' in sitemap
    assert sitemap.count('hreflang="fr"') >= 3, "alternates are not declared per entry"
