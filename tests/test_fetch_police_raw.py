"""Tests for scripts/fetch_police_raw.py.

This script is a reachability probe, not a parser -- no indicator has been
defined from police.be because no response body has ever been read here
(CLAUDE.md rule 13). What is tested is the part that does not depend on the
network working: the front page is visited before the API so a session
cookie can be picked up the way any ordinary client does, a 403 is reported
rather than worked around, and the script never fails the workflow it runs
in.
"""

import sys
from pathlib import Path
from unittest import mock

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from fetch_police_raw import API_URL, HOME_URL, STATS_URL, probe  # noqa: E402


class _FakeSession:
    """Records the request order. A real session is what picks up police.be's
    cookie, so the order of calls is the thing under test."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requested: list[str] = []
        self.headers: dict = {}
        self.cookies = requests.cookies.RequestsCookieJar()

    def get(self, url, **kwargs):
        self.requested.append(url)
        nxt = self._responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


def _ok(content=b"{}"):
    return mock.Mock(status_code=200, content=content)


def _forbidden():
    return mock.Mock(status_code=403, content=b"<title>Politie - Police - Polizei: Maintenance")


def test_front_page_is_visited_before_the_api(tmp_path):
    """The API needs the session cookie the front page hands out; requesting
    it cold is what gets a bare 403."""
    session = _FakeSession([_ok(), _ok(), _ok(b'{"rows": []}')])
    with mock.patch("fetch_police_raw.time.sleep"):
        probe(session=session, out_dir=tmp_path)

    assert session.requested == [HOME_URL, STATS_URL, API_URL]


def test_a_successful_json_response_is_cached_under_todays_date(tmp_path):
    body = b'{"rows": [{"pretend": "real police json"}]}'
    session = _FakeSession([_ok(), _ok(), _ok(body)])
    with mock.patch("fetch_police_raw.time.sleep"):
        result = probe(session=session, out_dir=tmp_path)

    assert result["api"] == 200
    assert result["json_bytes"] == len(body)
    cached = list(tmp_path.glob("*/*.json"))
    assert len(cached) == 1
    assert cached[0].read_bytes() == body


def test_the_403_maintenance_page_is_reported_not_cached(tmp_path):
    """The actual response this pipeline's own network gets. It must be
    reported as-is -- nothing about a 403 should be retried differently or
    routed around."""
    session = _FakeSession([_forbidden(), _forbidden(), _forbidden()])
    with mock.patch("fetch_police_raw.time.sleep"):
        result = probe(session=session, out_dir=tmp_path)

    assert result["api"] == 403
    assert result["json_bytes"] == 0
    assert result["cookies"] == []  # no session is ever established
    assert list(tmp_path.glob("*/*")) == []


def test_a_connection_failure_is_reported_not_raised(tmp_path):
    session = _FakeSession(
        [
            requests.exceptions.ConnectTimeout("timed out"),
            requests.exceptions.ConnectTimeout("timed out"),
            requests.exceptions.ConnectTimeout("timed out"),
        ]
    )
    with mock.patch("fetch_police_raw.time.sleep"):
        result = probe(session=session, out_dir=tmp_path)

    assert result["home"] == 0
    assert result["api"] == 0


def test_the_api_url_is_the_maintainer_verified_request_unchanged():
    """The `_4` suffix on the NIS code is undecoded (see module docstring),
    so the verified request is used verbatim rather than reconstructed."""
    assert "nis=21012_4" in API_URL
    assert "criminality_table/content" in API_URL


def test_main_never_exits_nonzero(monkeypatch, tmp_path, capsys):
    import fetch_police_raw

    monkeypatch.setattr(fetch_police_raw, "RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(sys, "argv", ["fetch_police_raw.py"])
    with (
        mock.patch(
            "fetch_police_raw.requests.Session.get",
            side_effect=OSError("Network is unreachable"),
        ),
        mock.patch("fetch_police_raw.time.sleep"),
    ):
        try:
            fetch_police_raw.main()
        except SystemExit as exc:
            assert exc.code == 0
    assert "no usable response" in capsys.readouterr().out
