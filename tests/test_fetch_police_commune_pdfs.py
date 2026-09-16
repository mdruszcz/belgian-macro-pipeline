"""Tests for scripts/fetch_police_commune_pdfs.py.

The script constructs no URL: it downloads only what the quarterly index page
publishes, because police.be's own filenames cannot be derived from a commune
list (see the script's docstring). What is tested here is exactly that
contract -- link extraction picks the requested quarter, language and
per-commune section and nothing else, and preserves the site's spelling
whatever shape it takes -- plus the refusal to keep a non-PDF body, which is
what police.be's 403 maintenance page would otherwise leave on disk looking
like a downloaded report.
"""

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from fetch_police_commune_pdfs import commune_links, download  # noqa: E402

BASE = (
    "/statistiques/sites/statspol/files/statistics_files_upload/"
    "Criminalit%C3%A9%20-%20Criminaliteit"
)
# Real shapes from the 2025_T04 French index: an accent-folded name, a name
# that keeps its hyphens, one that keeps a literal space, and a French exonym
# for a Flemish commune. A slug rule built from a commune list gets these wrong.
INDEX_HTML = f"""
<a href="{BASE}/2025/2025_T04/crimi_fr/01_Rapports/01_F%C3%A9d%C3%A9ral/rapport_fr.pdf">nat</a>
<a href="{BASE}/2025/2025_T04/crimi_fr/05_Par%20zone%20de%20police/01_Antwerpen/5345_ANTWERPEN_fr.pdf">pz</a>
<a href="{BASE}/2025/2025_T04/crimi_fr/06_Par%20commune/05_Li%C3%A8ge/Liege_Amay_fr.pdf">a</a>
<a href="{BASE}/2025/2025_T04/crimi_fr/06_Par%20commune/05_Li%C3%A8ge/Liege_La%20Calamine_fr.pdf">b</a>
<a href="{BASE}/2025/2025_T04/crimi_fr/06_Par%20commune/06_Limburg/Limburg_Bilzen-Hoeselt_fr.pdf">c</a>
<a href="{BASE}/2025/2025_T04/crimi_fr/06_Par%20commune/09_Oost-Vlaanderen/Oost_Vlaanderen_Renaix_fr.pdf">d</a>
<a href="{BASE}/2025/2025_T03/crimi_fr/06_Par%20commune/05_Li%C3%A8ge/Liege_Amay_fr.pdf">older</a>
<a href="{BASE}/2025/2025_T04/crimi_nl/06_Per%20gemeente/05_Li%C3%A8ge/Liege_Amay_nl.pdf">dutch</a>
"""


def test_commune_links_takes_only_the_requested_quarter_language_and_section():
    links = commune_links(INDEX_HTML, "2025_T04", "fr")
    tails = [link.rsplit("/", 2)[-2] + "/" + link.rsplit("/", 1)[-1] for link in links]
    assert tails == [
        "05_Li%C3%A8ge/Liege_Amay_fr.pdf",
        "05_Li%C3%A8ge/Liege_La%20Calamine_fr.pdf",
        "06_Limburg/Limburg_Bilzen-Hoeselt_fr.pdf",
        "09_Oost-Vlaanderen/Oost_Vlaanderen_Renaix_fr.pdf",
    ]
    assert all(link.startswith("https://www.police.be/") for link in links)


def test_commune_links_reads_the_dutch_tree_under_its_own_section_name():
    links = commune_links(INDEX_HTML, "2025_T04", "nl")
    assert [link.rsplit("/", 1)[-1] for link in links] == ["Liege_Amay_nl.pdf"]


def test_download_refuses_to_keep_a_body_that_is_not_a_pdf(tmp_path):
    """police.be answers its wall with an 8 KB HTML page. Keeping one would
    put a file on disk that every later step reads as a downloaded report."""
    dest = tmp_path / "Liege_Amay_fr.pdf"

    def fake_curl(url, path):
        path.write_bytes(b"<!DOCTYPE html><title>Maintenance</title>")
        return 403, b""

    with mock.patch("fetch_police_commune_pdfs.curl", side_effect=fake_curl), mock.patch(
        "fetch_police_commune_pdfs.time.sleep"
    ):
        status, size, digest = download("https://www.police.be/x.pdf", dest)

    assert status == "http_403"
    assert (size, digest) == (0, "")
    assert not dest.exists()
    assert not dest.with_suffix(".part").exists()


def test_download_keeps_a_real_pdf_and_reports_its_digest(tmp_path):
    dest = tmp_path / "Liege_Amay_fr.pdf"
    body = b"%PDF-1.7\nbody"

    def fake_curl(url, path):
        path.write_bytes(body)
        return 200, b""

    with mock.patch("fetch_police_commune_pdfs.curl", side_effect=fake_curl):
        status, size, digest = download("https://www.police.be/x.pdf", dest)

    assert status == "ok"
    assert size == len(body)
    assert dest.read_bytes() == body
    assert len(digest) == 64


def test_filter_narrows_to_published_links_only():
    """The French tree's gaps are filled from the Dutch one by selecting among
    published links -- the filter must never widen the set."""
    links = commune_links(INDEX_HTML, "2025_T04", "fr", "06_Limburg|Renaix")
    assert [link.rsplit("/", 1)[-1] for link in links] == [
        "Limburg_Bilzen-Hoeselt_fr.pdf",
        "Oost_Vlaanderen_Renaix_fr.pdf",
    ]
    assert commune_links(INDEX_HTML, "2025_T04", "fr", "Vlaams_Brabant_Leuven") == []
