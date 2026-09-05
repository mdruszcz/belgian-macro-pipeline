import json

import fetch_stocks


def test_fetch_data_returns_false_and_keeps_file_when_everything_fails(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    existing = tmp_path / "data" / "stocks.json"
    existing.write_text('{"BEL20": {"price": 1.0}}')

    def boom(*args, **kwargs):
        raise OSError("network down")

    monkeypatch.setattr(fetch_stocks.urllib.request, "urlopen", boom)
    monkeypatch.setattr(fetch_stocks.requests, "get", boom)

    assert fetch_stocks.fetch_data() is False
    # last known-good file must be untouched, not overwritten with {}
    assert json.loads(existing.read_text()) == {"BEL20": {"price": 1.0}}


def test_fetch_data_returns_false_on_partial_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()

    monkeypatch.setattr(
        fetch_stocks.urllib.request,
        "urlopen",
        lambda *a, **k: (_ for _ in ()).throw(OSError("down")),
    )

    class FakeResp:
        status_code = 200

        def json(self):
            return {"series": {"docs": [{"value": [1.0, 2.0]}]}}

    monkeypatch.setattr(fetch_stocks.requests, "get", lambda *a, **k: FakeResp())

    assert fetch_stocks.fetch_data() is False
    written = json.loads((tmp_path / "data" / "stocks.json").read_text())
    assert "BE_10Y" in written
