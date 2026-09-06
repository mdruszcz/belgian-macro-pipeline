from pathlib import Path

import pytest
import requests

from src.fetchers.statbel import StatbelSource, UnresolvedCommuneError, _parse_quarter

FIXTURE = Path(__file__).parent / "fixtures" / "statbel_local_units_sample.json"


class _FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        pass


def test_parses_fixture_and_resolves_every_commune(tmp_path, monkeypatch):
    """The fixture deliberately includes two ambiguous-name pairs, both
    resolved via NAME_OVERRIDES rather than the plain name+arrondissement
    lookup: Bestat's Sint-Niklaas (colliding with a genuinely different
    Saint-Nicolas in Liège, resolved via plain lookup) and Bestat's "Zwalin"
    for Zwalm -- Statbel's own typo, live in the API, independent of and
    surviving the correction made to geographies.csv's raw files (see
    config/geography/name_fr_corrections.csv and NAME_OVERRIDES' own
    comment). The second pair is the regression for a real production
    failure: daily_fetch.yml's sync_statbel step raised
    UnresolvedCommuneError on 2026-09-06 once geographies.csv stopped
    spelling it "Zwalin", because Bestat kept sending that spelling."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(FIXTURE.read_bytes()),
    )

    rows = StatbelSource().fetch("https://example.test/bestat", cache_key="LOCAL_UNITS")

    by_geo_id = {r["geo_id"]: r for r in rows}
    assert set(by_geo_id) == {
        "be:mun:11001",
        "be:mun:11002",
        "be:mun:46021",
        "be:mun:62093",
        "be:mun:45065",
    }
    assert by_geo_id["be:mun:46021"]["value"] == 8596.0  # Sint-Niklaas, via the override
    assert by_geo_id["be:mun:62093"]["value"] == 1068.0  # Saint-Nicolas (Liège), plain lookup
    assert by_geo_id["be:mun:45065"]["value"] == 1133.0  # "Zwalin" (really Zwalm), via the override
    assert all(r["period"] == "2023-Q4" for r in rows)
    assert all(r["status"] == "final" for r in rows)


def test_unattributed_row_is_excluded_not_guessed(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(FIXTURE.read_bytes()),
    )
    source = StatbelSource()
    rows = source.fetch("https://example.test/bestat", cache_key="LOCAL_UNITS")

    assert len(rows) == 5  # 6 fixture rows minus the 1 unattributed
    assert source._skipped_unattributed == 1


def test_rows_read_hint_includes_the_excluded_row(tmp_path, monkeypatch):
    """fetch_runs.rows_read must show the gap, not hide it."""
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(FIXTURE.read_bytes()),
    )
    source = StatbelSource()
    rows = source.fetch("https://example.test/bestat", cache_key="LOCAL_UNITS")
    assert source._rows_read_hint(rows) == 6


def test_unknown_commune_raises_rather_than_guessing(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    bad = FIXTURE.read_text(encoding="utf-8").replace("Aartselaar", "Not A Real Commune")
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: _FakeResponse(bad.encode("utf-8")),
    )
    with pytest.raises(UnresolvedCommuneError, match="Not A Real Commune"):
        StatbelSource().fetch("https://example.test/bestat", cache_key="LOCAL_UNITS")


def test_source_propagates_http_errors(tmp_path, monkeypatch):
    monkeypatch.setattr("src.fetchers.base.RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        "src.fetchers.base.requests.get",
        lambda *a, **k: (_ for _ in ()).throw(requests.exceptions.ConnectionError("down")),
    )
    monkeypatch.setattr("src.fetchers.base.time.sleep", lambda *_: None)
    with pytest.raises(requests.exceptions.ConnectionError):
        StatbelSource().fetch("https://example.test/bestat", cache_key="LOCAL_UNITS")


@pytest.mark.parametrize(
    "text,expected",
    [
        ("4ème trimestre 2023", "2023-Q4"),
        ("1er trimestre 2024", "2024-Q1"),
        ("2e trimestre 2024", "2024-Q2"),
        ("3ème trimestre 2024", "2024-Q3"),
    ],
)
def test_parse_quarter(text, expected):
    assert _parse_quarter(text) == expected


def test_parse_quarter_rejects_unknown_format():
    with pytest.raises(ValueError, match="Unrecognized quarter format"):
        _parse_quarter("Q4 2023")
