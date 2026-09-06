"""Tests for scripts/export_local_pages.py -- Block L's permanent URLs.

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


def _run(payload_dir, tmp_path, db, attribution_file, build_id="b1", **kwargs):
    return elp.export_local_pages(
        payload_dir=payload_dir,
        out_dir=tmp_path / "local",
        base_url="https://example.test/site",
        build_id=build_id,
        attribution_source=attribution_file,
        db_path=db,
        **kwargs,
    )


# --- the route shape ------------------------------------------------------


def test_writes_one_index_html_per_commune_directory(payload_dir, tmp_path, db, attribution_file):
    result = _run(payload_dir, tmp_path, db, attribution_file)
    assert result["written"] == 1
    assert (tmp_path / "local" / "11001" / "index.html").is_file()


def test_a_commune_with_no_values_gets_no_page(payload_dir, tmp_path, db, attribution_file):
    """comparison.md's "no data, no page" rule -- an empty page is worse than
    a missing one, because Google demotes the whole domain for thin pages."""
    (payload_dir / "communes" / "99999.json").write_text(
        json.dumps(_commune("99999", with_values=False)), encoding="utf-8"
    )
    result = _run(payload_dir, tmp_path, db, attribution_file)
    assert result == {"written": 1, "skipped_thin": 1}
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


def test_attribution_is_lifted_from_communes_html_not_retyped(
    payload_dir, tmp_path, db, attribution_file
):
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    assert '<div class="attribution">' in page
    assert "<strong>Source:</strong> Statbel." in page


def test_the_licence_required_update_date_is_filled_in_not_left_as_a_placeholder(
    payload_dir, tmp_path, db, attribution_file
):
    """Statbel's 2015 licence requires the date of last update. A static page
    has no JavaScript to fill the placeholder communes.html uses, so the
    generator must substitute the real date -- an em-dash would be a licence
    breach on 565 pages."""
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    assert 'id="attrUpdated"' not in page
    assert "<strong>Data last updated:</strong> 2026-09-06." in page


def test_generation_refuses_if_the_attribution_block_is_missing(payload_dir, tmp_path, db):
    """Rather than silently publishing 565 pages with no licence notice."""
    empty = tmp_path / "no_attribution.html"
    empty.write_text("<html><body>nothing here</body></html>", encoding="utf-8")
    with pytest.raises(ValueError, match="no .attribution block"):
        _run(payload_dir, tmp_path, db, empty)


# --- content --------------------------------------------------------------


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


def test_an_indicator_with_no_value_is_omitted_rather_than_shown_empty(
    payload_dir, tmp_path, db, attribution_file
):
    commune = _commune()
    commune["indicators"]["POPULATION_AGE_0_14"] = {
        "names": {"en": "Population aged 0 to 14"},
        "unit": "count",
        "periods": {"2026": {"value": None, "status": "suppressed"}},
    }
    (payload_dir / "communes" / "11001.json").write_text(json.dumps(commune), encoding="utf-8")
    _run(payload_dir, tmp_path, db, attribution_file)
    page = (tmp_path / "local" / "11001" / "index.html").read_text(encoding="utf-8")
    assert "Population aged 0 to 14" not in page


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
