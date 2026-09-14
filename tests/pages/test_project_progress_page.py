"""Contracts for the human-readable view of the project roadmap."""

from html.parser import HTMLParser
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PAGE = REPO / "docs" / "step.html"
ROADMAP = REPO / "docs" / "steps"


class _DocumentParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.ids: set[str] = set()
        self.landmarks: list[str] = []
        self.lang = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if values.get("id"):
            self.ids.add(values["id"] or "")
        if tag in {"header", "main", "nav", "footer"}:
            self.landmarks.append(tag)
        if tag == "html":
            self.lang = values.get("lang") or ""


def test_progress_page_is_adjacent_to_the_plan_of_record():
    assert PAGE.parent == ROADMAP.parent
    assert PAGE.exists()
    assert ROADMAP.exists()


def test_progress_page_reads_the_plan_instead_of_copying_its_status_counts():
    page = PAGE.read_text(encoding="utf-8")

    assert 'fetch(new URL("./steps", window.location.href)' in page
    assert "parseRoadmap(await response.text())" in page
    assert "150 étapes" not in page
    assert "298 étapes" not in page
    assert "[NEXT] Dagster step 3" not in page


def test_progress_page_exposes_the_professional_summary_and_filters():
    page = PAGE.read_text(encoding="utf-8")
    parser = _DocumentParser()
    parser.feed(page)

    assert parser.lang == "fr"
    assert {"header", "main", "nav", "footer"} <= set(parser.landmarks)
    assert {
        "page-title",
        "progress-ring",
        "phase-grid",
        "next-title",
        "attention-list",
        "roadmap-search",
        "load-status",
        "roadmap",
    } <= parser.ids
    for status_filter in ("all", "done", "active", "blocked", "todo", "deferred"):
        assert f'data-filter="{status_filter}"' in page


def test_explicitly_partial_steps_are_not_presented_as_delivered():
    page = PAGE.read_text(encoding="utf-8")

    assert 'partial: "Partiel"' in page
    assert 'currentItem.status = "partial"' in page
    assert '["next", "pending", "partial"].includes(item.status)' in page


def test_progress_page_has_no_remote_runtime_dependency():
    page = PAGE.read_text(encoding="utf-8")

    assert "fonts.googleapis.com" not in page
    assert "cdn." not in page
    assert "<script src=" not in page
    assert '<link rel="stylesheet"' not in page


def test_latest_achievements_are_the_most_recently_dated_steps():
    """The tail of docs/steps is the Phase III list, not the latest work, so
    "Dernières réalisations" must be chosen by the date in each step's note."""
    page = PAGE.read_text(encoding="utf-8")

    assert "function recentByDate(items, count)" in page
    assert "const recent = recentByDate(state.items, 3);" in page
    assert '.filter(item => item.status === "done").slice(-3)' not in page
