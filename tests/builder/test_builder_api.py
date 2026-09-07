"""Batch 11 builder-service HTTP API: routing, auth, every required response
state, and preview byte-identity (batch-11 spec API surface table, "Required
states", and Tests items 2, 3, 8, 11).

A real server bound to 127.0.0.1:0 is used throughout, per the batch
instructions, because this file is about HTTP behaviour: headers, status
codes, routing, and the exact bytes a client receives -- not about what ends
up on disk (that is test_builder_transaction.py's job).

`src.builder.paths.PAGES_ROOT` and `src.builder.store.PAGES_ROOT` are BOTH
monkeypatched to a tmp directory before any server starts: `store.py` imports
`PAGES_ROOT` by value (`from src.builder.paths import PAGES_ROOT`), so it
holds its own separate module-level binding that patching only `paths` would
not reach, and no test here may write into the real `config/pages/`.

No indicator id, NIS code or commune figure is hand-typed: page documents come
from tests/fixtures/pages/builders.py, which itself reads the real registry
and metadata.
"""

from __future__ import annotations

import http.client
import json
import secrets
import socket
import threading

import pytest

from src.builder import paths as builder_paths
from src.builder import store as builder_store
from src.pages import load_registry, render_document
from tests.fixtures.pages import builders

try:
    from src.builder.service import BuilderConfig, make_server, preview_data
except ImportError:  # pragma: no cover - reported, not swallowed
    BuilderConfig = None
    make_server = None
    preview_data = None


pytestmark = pytest.mark.skipif(
    make_server is None, reason="src.builder.service not implemented yet"
)


def _free_loopback_port() -> int:
    """An ephemeral port free right now.

    `BuilderConfig` itself rejects port 0 (it validates `1 <= port <= 65535`
    strictly, since printing "the one URL that works" needs a real port
    number, not the kernel's "pick one" sentinel) -- so tests must pick a real
    port up front, same as the CLI's `--port` would receive one."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class ServerHarness:
    """Runs a real BuilderConfig/make_server instance on a free 127.0.0.1
    port for the duration of a test, and gives back a small HTTP client
    helper."""

    def __init__(self, token=None):
        self.token = token or secrets.token_urlsafe(32)
        self.config = BuilderConfig(host="127.0.0.1", port=_free_loopback_port(), token=self.token)
        self.server = make_server(self.config)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    def origin(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def request(
        self,
        method,
        path,
        *,
        body=None,
        token="__default__",
        origin="__default__",
        extra_headers=None,
    ):
        headers = {}
        if token == "__default__":
            headers["X-BelPulse-Token"] = self.token
        elif token is not None:
            headers["X-BelPulse-Token"] = token
        if origin == "__default__":
            if method == "POST":
                headers["Origin"] = self.origin()
        elif origin is not None:
            headers["Origin"] = origin
        data = None
        if body is not None:
            data = (
                body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode("utf-8")
            )
            headers.setdefault("Content-Type", "application/json")
        if extra_headers:
            headers.update(extra_headers)
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            try:
                conn.request(method, path, body=data, headers=headers)
            except BrokenPipeError:
                # The server is allowed to reject an oversize body and close
                # the connection before the client finishes writing it (it
                # must not buffer the whole thing first) -- the response is
                # still readable off the half-closed socket.
                pass
            resp = conn.getresponse()
            raw = resp.read()
        finally:
            conn.close()
        return resp, raw

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


@pytest.fixture()
def pages_root(tmp_path, monkeypatch):
    root = tmp_path / "pages"
    root.mkdir()
    monkeypatch.setattr(builder_paths, "PAGES_ROOT", root)
    monkeypatch.setattr(builder_store, "PAGES_ROOT", root)
    return root


@pytest.fixture()
def server(pages_root):
    harness = ServerHarness()
    yield harness
    harness.close()


def _json(raw):
    return json.loads(raw.decode("utf-8"))


# --- routing: every route in the API surface table resolves -----------------


def test_get_root_returns_bootstrap_html(server):
    resp, raw = server.request("GET", f"/?token={server.token}", token=None, origin=None)
    assert resp.status == 200
    assert resp.getheader("Content-Type", "").startswith("text/html")
    assert b"<" in raw


def test_get_api_pages_lists_no_pages_on_an_empty_root(server):
    resp, raw = server.request("GET", "/api/pages")
    assert resp.status == 200
    body = _json(raw)
    assert body["pages"] == []


def test_unknown_route_is_not_found(server):
    resp, _raw = server.request("GET", "/api/does-not-exist")
    assert resp.status == 404


def test_method_not_allowed_on_a_get_only_route(server):
    resp, _raw = server.request("DELETE", "/api/pages")
    assert resp.status == 405


def test_method_not_allowed_posting_to_a_get_only_route(server):
    resp, _raw = server.request("POST", "/api/pages", body={})
    assert resp.status == 405


def test_method_not_allowed_getting_a_post_only_route(server):
    resp, _raw = server.request("GET", "/api/validate")
    assert resp.status == 405


# --- token check --------------------------------------------------------


def test_missing_token_is_unauthorized(server):
    resp, _raw = server.request("GET", "/api/pages", token=None)
    assert resp.status == 401


def test_wrong_token_is_unauthorized(server):
    resp, _raw = server.request("GET", "/api/pages", token="not-the-real-token")
    assert resp.status == 401


def test_correct_prefix_truncated_token_is_unauthorized(server):
    truncated = server.token[: len(server.token) - 1]
    resp, _raw = server.request("GET", "/api/pages", token=truncated)
    assert resp.status == 401


def test_token_differing_by_one_character_is_unauthorized(server):
    mutated = ("a" if server.token[-1] != "a" else "b") + server.token[1:]
    assert mutated != server.token
    resp, _raw = server.request("GET", "/api/pages", token=mutated)
    assert resp.status == 401


def test_query_string_token_is_rejected_on_an_api_route(server):
    """Only GET / accepts ?token=; every other route must still require the
    header, even carrying a VALID token on the query string."""
    resp, _raw = server.request("GET", f"/api/pages?token={server.token}", token=None)
    assert resp.status == 401


def test_correct_token_in_header_passes(server):
    resp, _raw = server.request("GET", "/api/pages")
    assert resp.status == 200


# --- origin check on POST ------------------------------------------------


def test_post_with_no_origin_is_forbidden(server):
    doc = builders.minimal_valid_document()
    resp, _raw = server.request(
        "POST", "/api/validate", body={"page_id": "home", "document": doc}, origin=None
    )
    assert resp.status == 403


def test_post_with_wrong_origin_is_forbidden(server):
    doc = builders.minimal_valid_document()
    resp, _raw = server.request(
        "POST",
        "/api/validate",
        body={"page_id": "home", "document": doc},
        origin="http://evil.example",
    )
    assert resp.status == 403


def test_post_with_localhost_origin_passes_auth(server):
    doc = builders.minimal_valid_document()
    resp, _raw = server.request(
        "POST",
        "/api/validate",
        body={"page_id": "home", "document": doc},
        origin=f"http://localhost:{server.port}",
    )
    assert resp.status != 403


# --- required states, each reached at least once ----------------------------


def test_state_ok(server):
    doc = builders.minimal_valid_document()
    resp, raw = server.request("POST", "/api/validate", body={"page_id": "home", "document": doc})
    assert resp.status == 200
    assert _json(raw)["ok"] is True


def test_state_validation_failed(server):
    doc = builders.minimal_valid_document()
    doc["sections"][0]["blocks"][0]["type"] = "not_a_real_block_type"
    resp, raw = server.request("POST", "/api/validate", body={"page_id": "home", "document": doc})
    assert resp.status == 422
    body = _json(raw)
    assert body["ok"] is False


def test_state_unauthorized(server):
    resp, _raw = server.request("GET", "/api/pages", token=None)
    assert resp.status == 401


def test_state_forbidden_origin(server):
    doc = builders.minimal_valid_document()
    resp, _raw = server.request(
        "POST",
        "/api/validate",
        body={"page_id": "home", "document": doc},
        origin="http://evil.example",
    )
    assert resp.status == 403


def test_state_bad_request_malformed_json(server):
    resp, raw = server.request("POST", "/api/validate", body=b"{not json", origin=server.origin())
    assert resp.status == 400


def test_state_bad_request_bad_page_id(server):
    doc = builders.minimal_valid_document()
    resp, raw = server.request("POST", "/api/validate", body={"page_id": "../etc", "document": doc})
    assert resp.status == 400


def test_state_bad_request_bad_which(server):
    resp, raw = server.request("GET", "/api/document?page_id=home&which=sideways")
    assert resp.status == 400


def test_state_not_found_unknown_page(server):
    resp, raw = server.request("GET", "/api/document?page_id=nonexistent-page&which=draft")
    assert resp.status == 404


def test_state_not_found_unknown_version(server):
    doc = builders.minimal_valid_document()
    server.request("POST", "/api/save", body={"page_id": "home", "document": doc})
    resp, raw = server.request("POST", "/api/restore", body={"page_id": "home", "version": "9999"})
    assert resp.status == 404


def test_state_payload_too_large(server):
    huge = b"{" + b'"x":"' + (b"a" * (3 * 1024 * 1024)) + b'"}'
    resp, raw = server.request("POST", "/api/validate", body=huge, origin=server.origin())
    assert resp.status == 413


def test_state_method_not_allowed(server):
    resp, _raw = server.request("PATCH", "/api/pages")
    assert resp.status == 405


def test_state_guard_rejected_echoes_no_document_detail(server):
    """A guard rejection (document too big/deep) must not echo any detail of
    the offending document back to the client -- rev 1 spec, "Guard findings
    are answered without echoing detail"."""
    doc = builders.minimal_valid_document()
    secret_marker = "SUPER-SECRET-MARKER-STRING-NEVER-ECHOED"
    # A single string over MAX_STRING_LENGTH (8192) trips the string_too_long
    # guard specifically (rather than the raw body cap), so this exercises the
    # guard-rejection path, not merely the body-size 413.
    doc["seo"]["title"]["en"] = secret_marker + ("z" * 9000)
    resp, raw = server.request("POST", "/api/validate", body={"page_id": "home", "document": doc})
    assert resp.status == 413
    body = _json(raw)
    assert body["error"]["errors"] == []
    assert secret_marker.encode("utf-8") not in raw
    for _header, value in resp.getheaders():
        assert secret_marker not in value


# --- no secret / filesystem detail ever leaks ------------------------------


def test_no_response_ever_contains_an_absolute_filesystem_path(server, pages_root):
    doc = builders.minimal_valid_document()
    resp, raw = server.request("POST", "/api/validate", body={"page_id": "home", "document": doc})
    assert str(pages_root) not in raw.decode("utf-8", errors="replace")
    import os as _os

    repo_root = _os.getcwd()
    assert repo_root.encode("utf-8") not in raw


def test_no_response_ever_contains_the_session_token(server):
    doc = builders.minimal_valid_document()
    resp, raw = server.request("POST", "/api/validate", body={"page_id": "home", "document": doc})
    assert server.token.encode("utf-8") not in raw


# --- restore writes draft.json only -----------------------------------------


def test_restore_endpoint_writes_draft_and_never_published(server, pages_root):
    doc = builders.minimal_valid_document()
    server.request("POST", "/api/save", body={"page_id": "home", "document": doc})
    resp, raw = server.request("POST", "/api/publish", body={"page_id": "home"})
    assert resp.status == 200
    first_publish = _json(raw)
    assert first_publish["version"] is None, "the first publish of a page has no previous content"

    doc2 = builders.deep_clone(doc)
    doc2["seo"]["title"]["en"] = "Edited after first publish"
    server.request("POST", "/api/save", body={"page_id": "home", "document": doc2})
    resp, raw = server.request("POST", "/api/publish", body={"page_id": "home"})
    assert resp.status == 200
    second_publish = _json(raw)
    version = second_publish["version"]
    assert version is not None, "the second publish must report the snapshot of the FIRST content"

    doc3 = builders.deep_clone(doc2)
    doc3["seo"]["title"]["en"] = "Edited after second publish, not yet published"
    server.request("POST", "/api/save", body={"page_id": "home", "document": doc3})

    published_before = builder_paths.published_path("home").read_bytes()
    resp, raw = server.request("POST", "/api/restore", body={"page_id": "home", "version": version})
    assert resp.status == 200
    published_after = builder_paths.published_path("home").read_bytes()
    assert published_after == published_before, "restore must never touch published.json"

    draft_after = builder_paths.draft_path("home").read_text(encoding="utf-8")
    import src.pages as pages_mod

    restored_doc = pages_mod.loads(draft_after)
    assert (
        restored_doc["seo"]["title"]["en"] == doc["seo"]["title"]["en"]
    ), "restore must bring the SNAPSHOTTED (first) content into draft.json"


# --- /api/document migrated flag (rev 2 #12) --------------------------------


def test_get_document_reports_migrated_flag(server):
    doc = builders.minimal_valid_document()
    server.request("POST", "/api/save", body={"page_id": "home", "document": doc})
    resp, raw = server.request("GET", "/api/document?page_id=home&which=draft")
    assert resp.status == 200
    body = _json(raw)
    assert "migrated" in body
    assert isinstance(body["migrated"], bool)


# --- preview byte-identity (Tests item 8, rev 2 #13/#14) --------------------


@pytest.mark.parametrize("lang", ["en", "fr", "nl"])
def test_preview_is_byte_identical_to_render_document_with_the_same_preview_data(server, lang):
    doc = builders.realistic_multi_section_document()
    save_resp, _ = server.request("POST", "/api/save", body={"page_id": "home", "document": doc})
    assert save_resp.status == 200

    resp, raw = server.request("GET", f"/preview?page_id=home&which=draft&lang={lang}")
    assert resp.status == 200

    registry = load_registry()
    expected = render_document(doc, registry=registry, lang=lang, data=preview_data(doc))
    assert raw == expected.encode("utf-8"), (
        "preview must be byte-identical to render_document called directly "
        "with the SAME preview_data map, or the comparison fails for the wrong reason"
    )


def test_preview_response_has_hardening_headers(server):
    doc = builders.minimal_valid_document()
    server.request("POST", "/api/save", body={"page_id": "home", "document": doc})
    resp, _raw = server.request("GET", "/preview?page_id=home&which=draft&lang=en")
    csp = resp.getheader("Content-Security-Policy", "")
    assert "default-src 'none'" in csp
    assert resp.getheader("X-Content-Type-Options") == "nosniff"
    assert resp.getheader("Referrer-Policy") == "no-referrer"


def test_preview_data_marks_bound_blocks_unavailable_not_loading():
    """rev 2 #13: with data={}, _state_for would render 'loading' forever for
    any block with a binding -- preview_data must instead mark those blocks
    'unavailable' so the preview never lies about something that will load."""
    doc = builders.realistic_multi_section_document()
    result = preview_data(doc)
    bound_block_ids = {
        block["id"]
        for section in doc["sections"]
        for block in section["blocks"]
        if block.get("binding") is not None
    }
    assert bound_block_ids, "fixture must contain at least one bound block"
    for block_id in bound_block_ids:
        assert result[block_id]["state"] == "unavailable"


def test_bootstrap_html_does_not_embed_a_scriptable_same_origin_iframe():
    """rev 2 #3: the preview iframe must be sandboxed without
    allow-same-origin and without allow-scripts."""
    from src.builder.service import BuilderConfig as _Config
    from src.builder.service import bootstrap_html

    config = _Config(host="127.0.0.1", port=8787, token="x" * 43)
    html = bootstrap_html(config)
    assert "iframe" in html
    assert "allow-same-origin" not in html
    assert "allow-scripts" not in html
    assert "sandbox" in html


def test_bootstrap_html_strips_token_from_url_via_history_replacestate():
    from src.builder.service import BuilderConfig as _Config
    from src.builder.service import bootstrap_html

    config = _Config(host="127.0.0.1", port=8787, token="x" * 43)
    html = bootstrap_html(config)
    assert "history.replaceState" in html
