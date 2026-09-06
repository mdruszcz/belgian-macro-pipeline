"""Tests for scripts/fetch_onem_raw.py.

This script is a reachability test, not a parser -- no indicator has been
defined from these files because no one has opened one (CLAUDE.md rule 13).
What is tested here is the part that does not depend on the network actually
working: the URLs are built correctly, a failure is reported per file rather
than silently, and the script never fails the workflow it runs in (its
whole purpose is to report on reachability, not to block the pipeline while
that is still unknown).
"""

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from fetch_onem_raw import BASE_URL, FILES, _fetch_one, fetch_all  # noqa: E402


def test_six_files_named_exactly_as_the_maintainer_supplied():
    assert FILES == [
        "CCI_Commune_Statut_UP_FR.xls",
        "CCI_Commune_Statut_M_FR.xls",
        "CT_Commune_Statut_UP_FR.xls",
        "CT_Commune_Statut_M_FR.xls",
        "TTP_Commune_Statut_UP_FR.xls",
        "EMPL_Commune_Statut_M_FR.xls",
    ]


def test_base_url_matches_the_maintainer_supplied_path():
    assert BASE_URL == "https://www.onem.be/sites/default/files/assets/statistiques/113"


def test_a_successful_fetch_is_cached_under_todays_date(tmp_path):
    fake_response = mock.Mock(status_code=200, content=b"pretend .xls bytes")
    with mock.patch("fetch_onem_raw.requests.get", return_value=fake_response) as get:
        results = fetch_all(out_dir=tmp_path)

    assert get.call_count == len(FILES)
    assert all(results.values()), "every file should report success"
    cached = list(tmp_path.glob("*/*"))
    assert len(cached) == len(FILES)
    assert cached[0].read_bytes() == b"pretend .xls bytes"


def test_a_404_is_reported_as_a_failure_not_retried_forever():
    fake_response = mock.Mock(status_code=404, content=b"")
    with mock.patch("fetch_onem_raw.requests.get", return_value=fake_response) as get:
        ok, detail = _fetch_one("does_not_exist.xls")

    assert ok is False
    assert "404" in detail
    # A client error is terminal -- one call, not three retries burning the
    # workflow's time budget on something that will never succeed.
    assert get.call_count == 1


def test_a_500_is_retried_then_still_reported_as_a_failure():
    fake_response = mock.Mock(status_code=503, content=b"")
    with (
        mock.patch("fetch_onem_raw.requests.get", return_value=fake_response) as get,
        mock.patch("fetch_onem_raw.time.sleep"),
    ):
        ok, detail = _fetch_one("temporarily_down.xls")

    assert ok is False
    assert get.call_count == 3  # MAX_ATTEMPTS


def test_a_connection_failure_is_retried_then_reported_not_raised():
    """The actual failure mode this pipeline's own network hits against
    onem.be -- a connection that never completes, not an HTTP error code.
    The script must report it as a per-file failure and keep going, not
    crash the whole run."""
    import requests

    with (
        mock.patch(
            "fetch_onem_raw.requests.get",
            side_effect=requests.exceptions.ConnectTimeout("timed out"),
        ) as get,
        mock.patch("fetch_onem_raw.time.sleep"),
    ):
        ok, detail = _fetch_one("unreachable.xls")

    assert ok is False
    assert "timed out" in detail
    assert get.call_count == 3


def test_fetch_all_continues_past_an_individual_failure(tmp_path):
    """One file failing must not stop the other five from being attempted --
    each is independent."""
    responses = [
        mock.Mock(status_code=200, content=b"ok"),
        mock.Mock(status_code=404, content=b""),
        mock.Mock(status_code=200, content=b"ok"),
        mock.Mock(status_code=200, content=b"ok"),
        mock.Mock(status_code=200, content=b"ok"),
        mock.Mock(status_code=200, content=b"ok"),
    ]
    with mock.patch("fetch_onem_raw.requests.get", side_effect=responses) as get:
        results = fetch_all(out_dir=tmp_path)

    assert get.call_count == len(FILES)
    assert sum(results.values()) == 5
    assert results["CCI_Commune_Statut_M_FR.xls"] is False


def test_main_never_exits_nonzero(monkeypatch, tmp_path, capsys):
    """The workflow step is continue-on-error, but the script itself must
    also never fail the job on its own -- its purpose is to REPORT on
    reachability while that is still unknown, not to gate the pipeline on
    an answer nobody has yet."""
    import fetch_onem_raw

    monkeypatch.setattr(fetch_onem_raw, "RAW_CACHE_DIR", tmp_path)
    monkeypatch.setattr(sys, "argv", ["fetch_onem_raw.py"])
    with (
        mock.patch(
            "fetch_onem_raw.requests.get",
            side_effect=OSError("Network is unreachable"),
        ),
        mock.patch("fetch_onem_raw.time.sleep"),
    ):
        try:
            fetch_onem_raw.main()
        except SystemExit as exc:
            assert exc.code == 0
    out = capsys.readouterr().out
    assert "0/6 succeeded" in out
