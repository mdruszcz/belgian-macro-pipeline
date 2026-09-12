"""Batch B: one document, 565 communes.

Batch A proved the block types on ONE hand-authored page pinned to Namur.
Multiplying that page is where the cheap-looking part hides the expensive ones,
and each test here stands for a defect that would otherwise reach 1,695 live
pages at once:

* a shared `PayloadReader` accumulating all 565 payloads in memory;
* `asset_prefix_for()` counting `/local/11001/` as one level deep when the file
  it writes lives two down, which 404s every stylesheet and every block link
  while the page still renders perfectly;
* one title and one description across 1,695 URLs -- the duplicate-content
  state search engines demote, and a worse position than the hand-built pages
  hold today;
* the subject's own name left as Namur on all of them, because four block props
  name the commune and only the SEO strings were templated;
* the route inventory publishing `/local/{nis}/` verbatim: three URLs that no
  file answers.

The templated document is built HERE rather than committed, because
`scripts/export_local_pages.py` still owns `/local/{nis}/`, and a committed
templated document would have `make pages` overwrite all 1,695 live pages the
next time it ran. The cutover is its own decision; the machinery is this one.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scripts.export_page_documents import build_one, communes_with_data  # noqa: E402
from src.pages import load_registry  # noqa: E402
from src.pages.document import validate_document  # noqa: E402
from src.pages.metadata import load_metadata  # noqa: E402
from src.pages.resolve import PayloadReader  # noqa: E402
from src.pages.shell import read_attribution  # noqa: E402
from src.site.routes import block_page_routes, path_for  # noqa: E402

#: Two communes, one Flemish and one Walloon. Their figures are never asserted
#: against typed-in numbers -- every expected value below is read from the same
#: payload the page reads (rule 36).
A, B = "11001", "92094"


# The sweep over generated output. One case per route or page, so this file
# belongs to the `generated_site` tier (pyproject.toml), not the everyday loop.
pytestmark = pytest.mark.generated_site


@pytest.fixture(scope="module")
def catalogue():
    return load_metadata(), load_registry()


@pytest.fixture(scope="module")
def templated():
    """The published Namur profile, turned into the templated commune page.

    This is exactly the edit the cutover will make to that file: the route and
    `context.nis` take the placeholder, and every string naming Namur takes
    `{commune}`. Built by substitution over the real document so the fixture
    cannot drift from the page that ships.
    """
    source = REPO_ROOT / "config" / "pages" / "commune-profile" / "published.json"
    raw = json.loads(source.read_text(encoding="utf-8"))
    # The subject's name in each language, taken from the document rather than
    # typed here, so renaming the page does not silently empty this fixture.
    names = {raw["seo"]["title"][lang].split()[0] for lang in ("en", "fr", "nl")}
    text = json.dumps(raw, ensure_ascii=False)
    for name in names:
        text = text.replace(name, "{commune}")
    doc = json.loads(text)
    doc["route"] = "/local/{nis}/"
    doc["context"] = {"nis": "{nis}"}
    # The figures come from blocks ALREADY ON THE PAGE, so a description can
    # never state a number the reader cannot see.
    leads = {
        "en": "Population, income, employment, housing and safety figures for {commune}.",
        "fr": "Population, revenus, emploi, logement et securite pour {commune}.",
        "nl": "Bevolking, inkomen, werk, wonen en veiligheid voor {commune}.",
    }
    for lang, lead in leads.items():
        doc["seo"]["description"][lang] = lead + " {figures:cp-stat-0,cp-stat-1}"
    return doc


def build(doc, catalogue, nis, lang="en"):
    """One commune, one language. Writes nothing: build_one returns the bytes."""
    metadata, registry = catalogue
    return build_one(
        REPO_ROOT / "config" / "pages" / "commune-profile",
        lang=lang,
        metadata=metadata,
        registry=registry,
        attribution=read_attribution(lang),
        doc=doc,
        nis=nis,
        reader=PayloadReader(),
    )


def payload_of(nis):
    path = REPO_ROOT / "public" / "data" / "communes" / f"{nis}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_the_templated_document_is_valid_as_published(templated, catalogue):
    """The placeholder route and `context.nis` are allowlisted shapes.

    If this goes red the cutover cannot happen at all, and that is better
    learnt here than from a validator failing 565 times.
    """
    metadata, registry = catalogue
    errors = validate_document(templated, metadata=metadata, registry=registry)
    assert not errors, [(e.path, e.message) for e in errors]


def test_the_two_exporters_agree_on_which_communes_get_a_page():
    """ "No data, no page", stated in two exporters, still one rule.

    `export_local_pages.py` skips a commune whose every indicator is empty, and
    the block loop applies the same test. If the two ever disagree the cutover
    silently adds or drops URLs, and a URL that used to resolve and stops is
    rule 31.
    """
    published = {path.parent.name for path in (REPO_ROOT / "local").glob("*/index.html")}
    assert published, "no commune pages are built; run `make pages` first"
    assert set(communes_with_data()) == published


def test_each_commune_page_lands_at_its_own_route(templated, catalogue):
    first, _ = build(templated, catalogue, A)
    second, _ = build(templated, catalogue, B)
    assert first == REPO_ROOT / "local" / A / "index.html"
    assert second == REPO_ROOT / "local" / B / "index.html"


def test_a_commune_page_carries_its_own_title_and_its_own_figures(templated, catalogue):
    """Not one title and one description over 1,695 URLs."""
    _, one = build(templated, catalogue, A)
    _, other = build(templated, catalogue, B)
    titles = [re.search(r"<title>(.*?)</title>", html).group(1) for html in (one, other)]
    assert titles[0] != titles[1]
    descriptions = [
        re.search(r'name="description" content="(.*?)"', html).group(1) for html in (one, other)
    ]
    assert descriptions[0] != descriptions[1]
    # Real figures, from that commune's own payload -- the same shape
    # `export_local_pages._describe` gives the pages live today.
    for description, nis in zip(descriptions, (A, B), strict=True):
        periods = payload_of(nis)["indicators"]["POPULATION_BY_COMMUNE"]["periods"]
        latest = max(p for p, cell in periods.items() if cell.get("value") is not None)
        assert f"({latest})" in description


def test_the_page_names_its_own_commune_in_the_body(templated, catalogue):
    """The four props that name the subject, not only the SEO strings.

    The hero overlay, the locator map's accessible name and both comparison
    headings all carry the commune. Templating `<title>` alone would have left
    every page in the country greeting its reader as Namur.
    """
    _, html = build(templated, catalogue, A)
    assert payload_of(A)["name"]["en"] in html
    assert "{commune}" not in html
    assert "{nis}" not in html
    assert "{figures" not in html


def test_the_licence_notice_survives_the_loop(templated, catalogue):
    """A page of Statbel figures without the notice is a licence breach, and
    the loop is where a page leaves the single-page path that was tested."""
    for lang in ("en", "fr", "nl"):
        _, html = build(templated, catalogue, A, lang=lang)
        assert read_attribution(lang)[:40] in html


def test_every_asset_a_commune_page_links_to_resolves(templated, catalogue):
    """The off-by-one that renders perfectly and 404s everything.

    `/local/11001/` counts one slash, and the file it writes lives at
    `local/11001/index.html` -- two directories down. Every stylesheet, script
    and in-page link on all 1,695 pages would have been one level short.
    """
    target, html = build(templated, catalogue, A)
    base = target.parent
    checked = 0
    for value in re.findall(r'(?:href|src)="([^"]+)"', html):
        if value.startswith(("http", "//", "#", "data:", "mailto:")):
            continue
        assert not value.startswith("/"), f"{value} is root-absolute; this site is on a subpath"
        candidate = (base / value.split("#")[0].split("?")[0]).resolve()
        if candidate.is_dir():
            candidate = candidate / "index.html"
        assert candidate.exists(), f"{value} does not resolve from {base}"
        checked += 1
    assert checked > 5, "no relative link was checked, so this assertion proved nothing"


def test_a_commune_page_switches_language_to_a_page_that_exists(templated, catalogue):
    for lang in ("fr", "nl"):
        target, _ = build(templated, catalogue, A, lang=lang)
        assert target == REPO_ROOT / "local" / A / lang / "index.html"


def test_localising_leaves_the_source_document_untouched(templated, catalogue):
    """565 communes are rendered from ONE parsed document. A localise() that
    mutated it would leave every later commune wearing the first one's name --
    and the first page would look right."""
    before = json.dumps(templated, sort_keys=True)
    build(templated, catalogue, A)
    build(templated, catalogue, B)
    assert json.dumps(templated, sort_keys=True) == before


def test_two_builds_of_one_commune_are_byte_identical(templated, catalogue):
    """Rule 35, over the path that now runs 1,695 times."""
    _, first = build(templated, catalogue, B)
    _, second = build(templated, catalogue, B)
    assert first == second


def test_the_loop_does_not_hold_every_commune_in_memory(monkeypatch, tmp_path, catalogue):
    """One reader per commune, dropped before the next one starts.

    A `PayloadReader` caches every file it opens for its own lifetime. One
    shared across the loop ends up holding all 565 commune payloads -- about
    37 MB -- for no gain, since each page reads its own exactly once.
    """
    from scripts import export_page_documents as exporter

    made = []

    class Counted(PayloadReader):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            made.append(self)

    page_dir = tmp_path / "commune-profile"
    page_dir.mkdir()
    source = REPO_ROOT / "config" / "pages" / "commune-profile" / "published.json"
    doc = json.loads(source.read_text(encoding="utf-8"))
    doc["route"] = "/local/{nis}/"
    doc["context"] = {"nis": "{nis}"}
    page_dir.joinpath("published.json").write_text(
        json.dumps(doc, ensure_ascii=False), encoding="utf-8"
    )

    monkeypatch.setattr(exporter, "PAGES_ROOT", tmp_path)
    monkeypatch.setattr(exporter, "PayloadReader", Counted)
    # --check writes nothing. It reports a difference, because what is live at
    # these URLs today is the hand-built page -- the state this batch
    # deliberately leaves alone.
    exporter.main(["--check", "--lang", "en", "--nis", A, "--nis", B])
    assert len(made) == 2, "a reader is being shared across communes, or made per language"


def test_the_inventory_expands_a_templated_route(tmp_path):
    """`/local/{nis}/` is a document, not a URL.

    Published verbatim it puts three URLs in the inventory that no file
    answers, and `test_every_block_page_route_resolves` would then demand a
    literal `{nis}` directory on disk.
    """
    pages = tmp_path / "config" / "pages" / "commune-profile"
    pages.mkdir(parents=True)
    pages.joinpath("published.json").write_text(
        json.dumps({"route": "/local/{nis}/"}), encoding="utf-8"
    )
    for nis in (A, B):
        for lang in ("", "fr", "nl"):
            page = tmp_path / "local" / nis / lang / "index.html"
            page.parent.mkdir(parents=True, exist_ok=True)
            page.write_text("<!doctype html>", encoding="utf-8")

    routes = block_page_routes(tmp_path)
    assert "/local/{nis}/" not in routes
    assert set(routes) == {f"/local/{nis}/{lang}" for nis in (A, B) for lang in ("", "fr/", "nl/")}
    for route in routes:
        assert path_for(route, tmp_path).is_file()
