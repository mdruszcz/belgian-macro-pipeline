"""Batch 12a -- the service seam the builder shell is loaded through.

Four things are under test here, and each one exists because the obvious
implementation of it is wrong in a way that is invisible until it ships:

* ``GET /api/registry``. ``BuilderConfig.registry`` is a frozen dataclass whose
  mappings are ``MappingProxyType``, so both ``json.dumps`` and
  ``dataclasses.asdict`` raise -- the endpoint has to build a plain dict by
  hand, and the negative test below pins that. It is asserted against
  ``src/pages/registry.py``, never against ``registry.json``, because
  ``load_registry()`` deliberately drops four top-level keys the file carries.

* ``bootstrap_html()``. Batch 11's demo controls are gone; the token appears
  exactly once, inside a closure; ``history.replaceState`` still strips the
  query token; and the preview iframe keeps a BARE sandbox. That last one is
  the P0: ``/preview`` renders unvalidated on-disk content on the same origin
  as the write API.

* The ``</script`` escape. The shell's JavaScript is inlined between
  ``<script>`` and ``</script>``, so one such sequence in a shell file would
  close the element early. Both locks are tested: the escape on the way out,
  and the assertion on the way in.

* The Content-Security-Policy on ``/``, gated in a REAL BROWSER rather than
  reasoned about, because a ``srcdoc`` iframe inherits its embedder's policy
  and a policy that blocks the preview's inlined stylesheet fails silently --
  no error, just an unstyled canvas nobody notices until a screenshot.

No indicator id, NIS code or commune figure is hand-typed anywhere in this
file; the one rendered document comes from tests/fixtures/pages/builders.py,
which reads the real registry and metadata.
"""

from __future__ import annotations

import dataclasses
import http.client
import json
import re
import secrets
import socket
import threading

import pytest

from src.builder import paths as builder_paths
from src.builder import store as builder_store
from src.pages import load_registry, render_document
from src.pages.registry import REGISTRY_PATH
from tests.fixtures.pages import builders

from src.builder.service import (  # isort: skip
    BUILDER_APP_DIR,
    DESIGN_SYSTEM_DIR,
    GET_ROUTES,
    POST_ROUTES,
    PREVIEW_CSS_FILES,
    SHELL_CSS_FILES,
    SHELL_JS_FILES,
    BuilderConfig,
    _escape_script_end,
    bootstrap_html,
    content_security_policy,
    make_server,
    registry_payload,
)

SCRIPT_END = "</" + "script"

#: The routes Batch 11 shipped. Batch 12a adds exactly one.
BATCH_11_GET_ROUTES = frozenset({"/", "/api/pages", "/api/document", "/api/versions", "/preview"})
BATCH_11_POST_ROUTES = frozenset({"/api/validate", "/api/save", "/api/publish", "/api/restore"})


# --------------------------------------------------------------------------
# harness
# --------------------------------------------------------------------------


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class ServerHarness:
    def __init__(self):
        self.token = secrets.token_urlsafe(32)
        self.config = BuilderConfig(host="127.0.0.1", port=_free_loopback_port(), token=self.token)
        self.server = make_server(self.config)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def request(self, method, path, *, token="__default__"):
        headers = {}
        if token == "__default__":
            headers["X-BelPulse-Token"] = self.token
        elif token is not None:
            headers["X-BelPulse-Token"] = token
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            conn.request(method, path, headers=headers)
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


@pytest.fixture()
def config() -> BuilderConfig:
    return BuilderConfig(host="127.0.0.1", port=8787, token="x" * 43)


def _json(raw):
    return json.loads(raw.decode("utf-8"))


# --------------------------------------------------------------------------
# GET /api/registry
# --------------------------------------------------------------------------


def test_api_registry_returns_exactly_what_the_module_loads(server):
    """Asserted against ``src/pages/registry.py``, so the block library and the
    validator cannot drift: they are literally the same object graph."""
    resp, raw = server.request("GET", "/api/registry")
    assert resp.status == 200
    body = _json(raw)
    assert body["ok"] is True

    loaded = load_registry()
    assert body["registry"] == {
        "registry_version": loaded.registry_version,
        "block_types": dict(loaded.block_types),
        "$defs": dict(loaded.defs),
    }


def test_api_registry_declares_every_block_type_the_module_knows(server):
    resp, raw = server.request("GET", "/api/registry")
    served = _json(raw)["registry"]["block_types"]
    assert tuple(sorted(served)) == load_registry().types
    for block_type in served:
        entry = served[block_type]
        # The fields the shell's library and inspector are built from.
        assert set(entry) >= {
            "accepts_binding",
            "current_version",
            "interactive",
            "requires_binding",
            "supported_versions",
            "versions",
        }


def test_api_registry_does_not_claim_to_be_the_registry_file(server):
    """``load_registry()`` DROPS four top-level keys ``registry.json`` carries.

    A test written against the file would either fail here or, worse, push an
    implementer into re-reading the file in the handler -- at which point the
    endpoint and the validator would be reading two things that are allowed to
    differ. The endpoint returns what the loader kept, on purpose.
    """
    dropped = {
        "accessible_name_note",
        "deliberately_absent",
        "forward_compatibility",
        "props_schema_note",
    }
    on_disk = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    assert dropped <= set(on_disk), (
        "this test is pinning the loader's behaviour against these exact keys; "
        "if registry.json no longer carries them, update both together"
    )

    resp, raw = server.request("GET", "/api/registry")
    served = _json(raw)["registry"]
    assert dropped.isdisjoint(set(served))
    assert set(served) == {"registry_version", "block_types", "$defs"}


def test_the_registry_object_itself_cannot_be_serialised(config):
    """The negative case that catches the obvious implementation.

    Both of the two things anyone reaches for first raise, so a handler that
    did either would be a 500 on every single request -- and the endpoint the
    whole block library depends on would be dead.
    """
    with pytest.raises(TypeError):
        json.dumps(config.registry)
    with pytest.raises(TypeError):
        json.dumps(config.registry.block_types)
    with pytest.raises(TypeError):
        dataclasses.asdict(config.registry)

    # And the payload builder does not.
    text = json.dumps(registry_payload(config.registry), ensure_ascii=False, allow_nan=False)
    assert json.loads(text)["registry_version"] == config.registry.registry_version


def test_api_registry_needs_a_token_like_every_other_route(server):
    resp, _raw = server.request("GET", "/api/registry", token=None)
    assert resp.status == 401
    resp, _raw = server.request("GET", "/api/registry", token="not-the-token")
    assert resp.status == 401


def test_api_registry_does_not_accept_a_query_token(server):
    """Only ``/`` accepts a query token, and that must not have widened."""
    resp, _raw = server.request("GET", f"/api/registry?token={server.token}", token=None)
    assert resp.status == 401


def test_api_registry_is_get_only(server):
    conn = http.client.HTTPConnection("127.0.0.1", server.port, timeout=5)
    try:
        conn.request(
            "POST",
            "/api/registry",
            body=b"{}",
            headers={
                "X-BelPulse-Token": server.token,
                "Origin": server.base_url,
                "Content-Type": "application/json",
                "Content-Length": "2",
            },
        )
        resp = conn.getresponse()
        resp.read()
    finally:
        conn.close()
    assert resp.status == 405


def test_the_router_is_still_exact_match_only(server):
    """The new route is one exact path. It grows no sub-tree, which is the
    whole reason there is no static-file route in this design."""
    for path in (
        "/api/registry/",
        "/api/registry/hero",
        "/api/registry/../pages",
        "/api/Registry",
        "/api/registry.json",
    ):
        resp, _raw = server.request("GET", path)
        assert resp.status == 404, path


def test_a_percent_encoded_spelling_is_the_same_route_not_a_second_one(server):
    """Pinned because the module comment used to claim the opposite.

    The path is decoded ONCE and then compared exactly, so `%72` is just an
    `r`: the encoded spelling resolves to the SAME route, and the traversal
    defence is the exact match afterwards, not a refusal to decode. Treating
    the two spellings differently is how a filter and a handler end up
    disagreeing about which route ran.
    """
    resp, raw = server.request("GET", "/api/%72egistry")
    assert resp.status == 200
    assert _json(raw)["registry"]["registry_version"] == load_registry().registry_version


# --------------------------------------------------------------------------
# the route table
# --------------------------------------------------------------------------


def test_get_routes_gained_api_registry_and_nothing_else():
    assert GET_ROUTES - BATCH_11_GET_ROUTES == {"/api/registry"}
    assert BATCH_11_GET_ROUTES - GET_ROUTES == set()
    assert POST_ROUTES == BATCH_11_POST_ROUTES


def test_no_route_bypasses_the_transport_and_token_checks():
    """``_dispatch`` runs ``_check_transport`` then ``_check_token`` BEFORE it
    looks a path up, so any route added to the frozensets is covered by
    construction. This pins the ordering rather than re-testing each route."""
    import inspect

    from src.builder.service import BuilderHandler

    source = inspect.getsource(BuilderHandler._dispatch)
    transport = source.index("_check_transport()")
    token = source.index("_check_token(")
    lookup = source.index("if path not in routes")
    assert transport < token < lookup, (
        "the transport and token checks must run before route lookup, or a new "
        "route is only as safe as whoever remembered to guard it"
    )


def test_the_route_sets_are_frozensets_of_exact_paths():
    assert isinstance(GET_ROUTES, frozenset)
    assert isinstance(POST_ROUTES, frozenset)
    for path in GET_ROUTES | POST_ROUTES:
        assert path == "/" or re.fullmatch(r"/[a-z]+(?:/[a-z_]+)*", path), path


# --------------------------------------------------------------------------
# bootstrap_html: the shell skeleton
# --------------------------------------------------------------------------


def test_the_batch_11_demo_controls_are_gone(config):
    html = bootstrap_html(config, "nonce-value")
    for gone in (
        'id="page"',
        'id="which"',
        'id="lang"',
        'id="list"',
        'id="load"',
        'id="show"',
        'id="out"',
        "List pages",
        "Load document",
        "Batch 11: the API only",
    ):
        assert gone not in html, f"Batch 11 demo control {gone!r} is still in the shell"


def test_the_token_appears_exactly_once_and_only_in_the_script(config):
    html = bootstrap_html(config, "nonce-value")
    assert html.count(config.token) == 1
    before, after = html.split(config.token)
    assert "<script" in before and "</script" in after
    # Never anywhere it could be read back out of the page or persisted.
    for forbidden in (
        "localStorage",
        "sessionStorage",
        "document.cookie",
        "data-token",
        "?token=",
        "console.log",
    ):
        assert forbidden not in html, forbidden


def test_history_replacestate_still_strips_the_query_token(config):
    html = bootstrap_html(config, "nonce-value")
    assert "history.replaceState" in html
    assert "location.pathname" in html


def test_the_preview_iframe_keeps_a_bare_sandbox(config):
    """P0. ``/preview`` renders UNVALIDATED on-disk content on the same origin
    as the write API: either flag turns a renderer slip into token theft plus
    arbitrary writes under ``config/pages/``."""
    html = bootstrap_html(config, "nonce-value")
    assert "<iframe" in html
    assert "sandbox" in html
    assert "allow-same" + "-origin" not in html
    assert "allow-" + "scripts" not in html
    match = re.search(r"<iframe\b[^>]*>", html)
    assert match is not None
    tag = match.group(0)
    assert re.search(
        r"\bsandbox\b(?!\s*=\s*[\"'][^\"']+[\"'])", tag
    ), f"the sandbox attribute must carry no value at all, got: {tag}"


def test_the_shell_modules_get_an_api_closure_and_never_the_token(config):
    """The module boundary is a capability, not a credential."""
    html = bootstrap_html(config, "nonce-value")
    assert "function api(path, options)" in html
    assert "shell.start({" in html
    entry = html.split("shell.start({", 1)[1].split("});", 1)[0]
    assert "api: api" in entry
    assert "TOKEN" not in entry, "the entry point must receive the closure, never the token"


def test_the_api_closure_refuses_a_path_that_is_not_same_origin(config):
    """A mistyped absolute URL in a later batch must not send the token
    somewhere else. The check is in the closure, not in the caller."""
    html = bootstrap_html(config, "nonce-value")
    assert "path.charAt(0) !== '/'" in html
    assert "path.charAt(1) === '/'" in html


# The paths an audit found could carry the token off-site. A backslash is a
# path separator to the WHATWG URL parser for http(s), so "/\host" resolves
# exactly like "//host" -- checking only for a second "/" is not enough. The
# CSP's connect-src 'self' also blocks these, but a guard that needs the CSP
# to be right is not a guard.
_CROSS_ORIGIN_PATHS = ["//evil.example", "/\\evil.example", "/\\/evil.example"]
# Not cross-origin -- a backslash INSIDE a path is normalised to "/", so this
# stays on the origin. It is refused anyway because no legitimate builder path
# contains one, and a narrow rule about position is easier to get wrong than a
# blanket one. Kept separate so this test never claims it escapes.
_REFUSED_BUT_SAME_ORIGIN = ["/a\\b"]
_SAME_ORIGIN_PATHS = ["/api/pages", "/api/document?page_id=x", "/preview"]


def test_the_api_guard_actually_rejects_every_cross_origin_path(browser_page, config):
    """Behavioural, not textual: the shipped condition is lifted out of the
    generated page and run in a real browser. The earlier version of this test
    asserted the source CONTAINED two substrings, which is how a backslash
    hole survived it -- the strings were both present and the guard was still
    wrong."""
    page, _violations = browser_page
    html = bootstrap_html(config, "nonce-value")

    marker = "  if (typeof path !== 'string'"
    start = html.index(marker)
    condition = html[start + len("  if (") : html.index(") {", start)]

    # First: prove these really are cross-origin, using the browser's own URL
    # parser rather than our belief about it. A test that only asserted the
    # guard rejects them would still pass if the strings were harmless.
    for path in _CROSS_ORIGIN_PATHS:
        resolved = page.evaluate("(p) => new URL(p, 'http://127.0.0.1:9/x').origin", path)
        assert resolved != "http://127.0.0.1:9", f"{path!r} was expected to leave the origin"

    guard = f"(path) => !!({condition})"
    for path in _CROSS_ORIGIN_PATHS + _REFUSED_BUT_SAME_ORIGIN:
        assert page.evaluate(guard, path) is True, f"the guard must refuse {path!r}"
    for path in _SAME_ORIGIN_PATHS:
        assert page.evaluate(guard, path) is False, f"the guard must allow {path!r}"


def test_the_script_carries_the_nonce_it_was_given(config):
    html = bootstrap_html(config, "abc123")
    assert '<script nonce="abc123">' in html
    # And no nonce attribute at all when none was minted, so a caller cannot
    # accidentally ship an empty nonce="" that matches nothing.
    assert 'nonce=""' not in bootstrap_html(config)


def test_the_shell_is_wrapped_in_one_closure(config):
    html = bootstrap_html(config, "nonce-value")
    script = html.split("<script", 1)[1].split(">", 1)[1].split(SCRIPT_END, 1)[0]
    assert script.lstrip().startswith("(function () {")
    assert script.rstrip().endswith("})();")


def test_the_shell_makes_no_external_request(config):
    """No framework, no CDN, no external fetch (claude.md rules 17 and 30).

    Scoped to the skeleton and the shell's own script. The inlined design-system
    CSS is deliberately excluded: `tokens.css` names a Google Fonts URL in a
    COMMENT, which issues no request, and the policy on `/` blocks font and
    style loads off this machine anyway. The consequence -- the preview falls
    back to IBM Plex Sans / system-ui rather than Inter -- is recorded in the
    batch report, not papered over here.
    """
    html = bootstrap_html(config, "nonce-value")
    skeleton = html.split("const PREVIEW_CSS =", 1)[0] + html.split(";\nconst LANGUAGES", 1)[1]
    for scheme in ("http://", "https://", "//cdn", "integrity=", "import(", "src="):
        assert scheme not in skeleton, f"the shell must make no external request, found {scheme!r}"


def test_the_narrow_window_notice_is_an_honest_refusal(config):
    """A desktop tool squeezed into a phone-width layout misrepresents what a
    visitor sees. The shell refuses rather than fake-responds."""
    html = bootstrap_html(config, "nonce-value")
    assert 'id="shell-too-narrow"' in html
    assert "max-width:1023px" in html


# --------------------------------------------------------------------------
# inlining: the </script escape, both locks
# --------------------------------------------------------------------------


def test_no_inlined_file_contains_the_script_end_sequence():
    """Lock one: on the way in."""
    checked = []
    for name in SHELL_JS_FILES + SHELL_CSS_FILES:
        checked.append(BUILDER_APP_DIR / name)
    for name in PREVIEW_CSS_FILES:
        checked.append(DESIGN_SYSTEM_DIR / name)
    assert checked
    for path in checked:
        assert path.is_file(), f"{path.name} is named in a module constant but is not on disk"
        assert SCRIPT_END not in path.read_text(
            encoding="utf-8"
        ), f"{path.name} would close the <script> element early"


def test_the_inlining_escapes_the_sequence_if_one_ever_appears():
    """Lock two: on the way out. ``<\\/script`` is valid inside a JavaScript
    string and inside a regular expression literal, and the raw sequence
    outside either is already a syntax error -- so the rewrite can never break
    working code."""
    assert _escape_script_end(SCRIPT_END + ">") == "<\\/script>"
    assert _escape_script_end("var a = 1;") == "var a = 1;"
    assert SCRIPT_END not in _escape_script_end(f"const s = '{SCRIPT_END}>';")


def test_a_shell_file_carrying_the_sequence_cannot_break_out(config, monkeypatch, tmp_path):
    """The end-to-end version: a shell file that really contains it produces a
    document whose script element still ends exactly once."""
    import src.builder.service as service

    hostile = tmp_path / "hostile.js"
    hostile.write_text(f"shell.note = '{SCRIPT_END}><h1>escaped</h1>';\n", encoding="utf-8")
    monkeypatch.setattr(service, "BUILDER_APP_DIR", tmp_path)
    monkeypatch.setattr(service, "SHELL_JS_FILES", ("hostile.js",))
    # The real stylesheet lives in the real app directory, not this fixture's.
    # This test is about the JS escaping only.
    monkeypatch.setattr(service, "SHELL_CSS_FILES", ())

    html = service.bootstrap_html(config, "nonce-value")
    # Exactly one `</script` in the whole document: the real closing tag. The
    # hostile file's own markup survives as TEXT inside the script element,
    # which is inert -- what matters is that it never reaches the parser as
    # markup.
    assert html.count(SCRIPT_END) == 1
    body_after_script = html.split(SCRIPT_END, 1)[1]
    assert "<h1>escaped</h1>" not in body_after_script
    assert "<\\/script><h1>escaped</h1>" in html


def test_every_inlined_filename_is_a_plain_basename():
    """There is no user-controlled path component anywhere -- but a one-line
    edit to the tuples in a later batch must not be able to introduce one."""
    for name in SHELL_JS_FILES + SHELL_CSS_FILES + PREVIEW_CSS_FILES:
        assert "/" not in name and "\\" not in name and ".." not in name, name
        assert re.fullmatch(r"[a-z0-9]+(?:[._-][a-z0-9]+)*\.(?:js|css)", name), name


def test_a_filename_with_a_path_component_is_refused(tmp_path):
    from src.builder.service import _read_shell_file

    for name in ("../secrets.js", "sub/dir.js", "/etc/passwd", "history.JS"):
        with pytest.raises(ValueError):
            _read_shell_file(tmp_path, name)


def test_a_missing_shell_file_is_refused_rather_than_skipped(tmp_path):
    """A silent skip would ship a shell with a module quietly absent."""
    from src.builder.service import _read_shell_file

    with pytest.raises(FileNotFoundError):
        _read_shell_file(tmp_path, "history.js")


def test_history_js_is_inlined_and_the_design_system_is_read_not_copied(config):
    html = bootstrap_html(config, "nonce-value")
    assert "/* builder/app/history.js */" in html
    assert "shell.createHistory" in html

    # The design system's own text reaches the shell as a string constant...
    assert "const PREVIEW_CSS =" in html
    token_declaration = "--bp-accent"
    assert token_declaration in html
    # ...and is NOT copied into builder/app/, which would be exactly the drift
    # the shared renderer exists to prevent.
    for path in BUILDER_APP_DIR.glob("*.css"):
        assert token_declaration not in path.read_text(
            encoding="utf-8"
        ), f"{path.name} is a second copy of the public design system"


# --------------------------------------------------------------------------
# builder/app/ is publicly fetchable
# --------------------------------------------------------------------------


def test_no_shell_file_carries_a_secret_or_a_local_path():
    """GitHub Pages serves this repository's root, so every file under
    ``builder/app/`` is downloadable at the public URL the moment it is
    committed."""
    files = sorted(BUILDER_APP_DIR.glob("*.js")) + sorted(BUILDER_APP_DIR.glob("*.css"))
    assert files, "expected at least builder/app/history.js"
    for path in files:
        text = path.read_text(encoding="utf-8")
        assert "X-BelPulse-Token" not in text, path.name
        assert "localStorage" not in text and "sessionStorage" not in text, path.name
        assert "document.cookie" not in text, path.name
        assert not re.search(r"(?m)^\s*\S*/(home|workspaces|Users)/", text), path.name
        assert not re.search(r"https?://(?!$)", text), f"{path.name} names an external URL"


def test_history_js_contains_no_figure_that_could_read_as_data():
    """claude.md rule 36 / invariant 3, mechanised. The only numbers in the
    history primitive are stack and memory bounds."""
    text = (BUILDER_APP_DIR / "history.js").read_text(encoding="utf-8")
    code = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith(("*", "/*", "//"))
    )
    numbers = set(re.findall(r"(?<![\w.])\d+(?:\.\d+)?", code))
    allowed = {"0", "1", "2", "8", "100", "500", "1024"}
    assert numbers <= allowed, f"unexpected numeric literals in history.js: {numbers - allowed}"


# --------------------------------------------------------------------------
# Content-Security-Policy
# --------------------------------------------------------------------------


def test_root_sends_a_csp_with_a_fresh_nonce_each_response(server):
    resp, raw = server.request("GET", f"/?token={server.token}", token=None)
    assert resp.status == 200
    policy = resp.getheader("Content-Security-Policy")
    assert policy, "/ must send a Content-Security-Policy"
    match = re.search(r"script-src 'nonce-([^']+)'", policy)
    assert match, policy
    nonce = match.group(1)
    assert f'<script nonce="{nonce}">' in raw.decode("utf-8")

    resp2, _raw2 = server.request("GET", f"/?token={server.token}", token=None)
    other = re.search(r"script-src 'nonce-([^']+)'", resp2.getheader("Content-Security-Policy"))
    assert other.group(1) != nonce, "the nonce must be minted per response"


def test_the_policy_allows_inline_style_because_a_nonce_cannot_reach_a_srcdoc():
    """Nonces are NOT inherited by a ``srcdoc`` document, so a style nonce here
    would block the preview's inlined design-system CSS -- silently. The
    preview frame's bare sandbox already denies it script execution, so inline
    style there is style and nothing else."""
    policy = content_security_policy("abc")
    assert "style-src 'unsafe-inline'" in policy
    assert "style-src" in policy and "nonce" not in policy.split("style-src", 1)[1].split(";", 1)[0]
    assert "default-src 'none'" in policy
    assert "connect-src 'self'" in policy
    assert "base-uri 'none'" in policy
    assert "form-action 'none'" in policy
    assert "frame-ancestors 'none'" in policy
    assert "'unsafe-eval'" not in policy
    assert "script-src 'unsafe-inline'" not in policy


def test_the_preview_route_keeps_its_own_stricter_policy(server, pages_root):
    """Unchanged from Batch 11, and it must stay unchanged: the fragment is
    unvalidated on-disk content."""
    doc = builders.minimal_valid_document()
    page_dir = pages_root / doc["page_id"]
    page_dir.mkdir(parents=True, exist_ok=True)
    (page_dir / "draft.json").write_text(
        json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    resp, _raw = server.request("GET", f"/preview?page_id={doc['page_id']}&which=draft&lang=en")
    assert resp.status == 200
    assert resp.getheader("Content-Security-Policy") == (
        "default-src 'none'; style-src 'unsafe-inline'; sandbox"
    )


# --------------------------------------------------------------------------
# the real-browser gate
# --------------------------------------------------------------------------


def _chromium():
    """A launched Chromium, or a clean skip.

    Playwright IS declared (pyproject.toml dev extras) and CI installs the
    Chromium binary, so these tests run there rather than skipping. The guard
    stays for a developer who has not run `playwright install` locally.
    """
    try:
        from playwright import sync_api
    except ImportError as exc:
        # Not `importorskip`: that only skips on ModuleNotFoundError, and a
        # half-installed playwright raises a plain ImportError from its own
        # module body. CI must skip in BOTH shapes, or this batch turns the
        # suite red on a dependency nobody declared.
        pytest.skip(f"playwright is not a declared dependency ({exc})")
    try:
        manager = sync_api.sync_playwright().start()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"playwright could not start: {exc}")
    try:
        return manager, manager.chromium.launch()
    except Exception as exc:  # pragma: no cover - environment dependent
        manager.stop()
        pytest.skip(f"no chromium available: {exc}")


@pytest.fixture()
def browser_page(server):
    manager, browser = _chromium()
    context = browser.new_context(viewport={"width": 1440, "height": 900})
    page = context.new_page()
    violations = []
    page.on("console", lambda msg: violations.append(msg.text) if msg.type == "error" else None)
    page.goto(f"{server.base_url}/?token={server.token}", wait_until="load")
    try:
        yield page, violations
    finally:
        context.close()
        browser.close()
        manager.stop()


def test_the_shell_script_actually_runs_under_the_policy(browser_page):
    """If the nonce and the header disagree the script is blocked, and the page
    still looks fine -- so this asserts a side effect only the script produces:
    the query token is gone from the address bar."""
    page, violations = browser_page
    assert page.evaluate("location.search") == ""
    assert page.evaluate("typeof structuredClone") == "function"
    blocked = [v for v in violations if "Content Security Policy" in v]
    assert not blocked, blocked


def test_the_preview_is_actually_styled_inside_the_sandboxed_frame(browser_page, pages_root):
    """THE GATE. A ``srcdoc`` iframe inherits the embedder's policy, so this is
    the assertion that stops a future CSP from silently unstyling the canvas.

    The document rendered here comes from the fixture builders, which read the
    real registry -- no hand-typed figure reaches the preview.
    """
    page, _violations = browser_page

    css = "\n".join(
        (DESIGN_SYSTEM_DIR / name).read_text(encoding="utf-8") for name in PREVIEW_CSS_FILES
    )
    doc = builders.realistic_multi_section_document()
    fragment = render_document(doc, registry=load_registry(), lang="en", data={})
    srcdoc = f"<!doctype html><html><head><style>{css}</style></head><body>{fragment}</body></html>"

    page.evaluate(
        """(srcdoc) => new Promise((resolve) => {
            const frame = document.getElementById('shell-preview');
            frame.addEventListener('load', () => resolve(true), { once: true });
            frame.srcdoc = srcdoc;
        })""",
        srcdoc,
    )
    frame = page.frame_locator("#shell-preview")
    accent = frame.locator("body").evaluate(
        "el => getComputedStyle(el).getPropertyValue('--bp-accent').trim()"
    )
    declared = re.search(r"--bp-accent:\s*([^;]+);", (DESIGN_SYSTEM_DIR / "tokens.css").read_text())
    assert accent == declared.group(1).strip(), (
        "the inlined design-system CSS did not apply inside the srcdoc preview -- "
        "the Content-Security-Policy on / has unstyled the canvas"
    )

    # A token resolving proves the <style> parsed. This proves a real RULE
    # from a second file (blocks.css) reached a real rendered element: the
    # block grid the whole layout engine depends on.
    grid = frame.locator(".bp-grid").first
    assert grid.evaluate("el => getComputedStyle(el).display") == "grid"
    columns = grid.evaluate("el => getComputedStyle(el).gridTemplateColumns").split()
    # And the media queries resolved against the FRAME's width, not the
    # parent's -- which is what makes Batch 12b's viewport switcher honest.
    width = page.evaluate("document.getElementById('shell-preview').clientWidth")
    expected = 12 if width > 1024 else (8 if width > 640 else 4)
    assert len(columns) == expected, f"grid at {width}px gave {len(columns)} columns: {columns}"


def test_the_sandboxed_preview_really_cannot_script_or_reach_the_parent(browser_page):
    """The other half of the gate: the policy is permissive enough for style
    and no more. A script inside the srcdoc must not run, whatever the CSP
    says, because the sandbox denies it."""
    page, _violations = browser_page
    page.evaluate("""() => new Promise((resolve) => {
            const frame = document.getElementById('shell-preview');
            frame.addEventListener('load', () => resolve(true), { once: true });
            frame.srcdoc = '<!doctype html><html><body><p id="m">no</p>'
              + '<scr' + 'ipt>document.getElementById("m").textContent="yes";'
              + 'window.parent.__escaped=true;</scr' + 'ipt></body></html>';
        })""")
    frame = page.frame_locator("#shell-preview")
    assert frame.locator("#m").inner_text() == "no"
    assert page.evaluate("window.__escaped === undefined") is True


# --------------------------------------------------------------------------
# builder/app/history.js, exercised in the browser that will run it
# --------------------------------------------------------------------------


def _history_harness(page, options="{}"):
    """Load history.js on its own and return a handle to a stack.

    The file is concatenated into bootstrap_html's closure where ``shell`` is
    in scope, so a standalone load supplies that scope and nothing else.
    """
    source = (BUILDER_APP_DIR / "history.js").read_text(encoding="utf-8")
    page.evaluate(
        "(src) => { const shell = {}; (new Function('shell', src))(shell); "
        "window.__shell = shell; }",
        source,
    )
    page.evaluate(
        f"() => {{ window.__clock = 0; window.__h = window.__shell.createHistory("
        f"{{v: 0}}, Object.assign({{now: () => window.__clock}}, {options})); }}"
    )


@pytest.fixture()
def history_page(browser_page):
    page, _violations = browser_page
    _history_harness(page)
    return page


def test_history_push_undo_redo_round_trips(history_page):
    page = history_page
    page.evaluate("() => { window.__clock = 1000; window.__h.push({v: 1}); }")
    page.evaluate("() => { window.__clock = 2000; window.__h.push({v: 2}); }")
    assert page.evaluate("window.__h.current().v") == 2
    assert page.evaluate("window.__h.canUndo()") is True
    assert page.evaluate("window.__h.undo().v") == 1
    assert page.evaluate("window.__h.undo().v") == 0
    assert page.evaluate("window.__h.canUndo()") is False
    assert page.evaluate("window.__h.undo()") is None
    assert page.evaluate("window.__h.redo().v") == 1
    assert page.evaluate("window.__h.redo().v") == 2
    assert page.evaluate("window.__h.canRedo()") is False


def test_history_stores_a_clone_so_the_past_cannot_be_mutated(history_page):
    """A stack holding a reference into the live model is a stack that silently
    rewrites its own history when the model changes."""
    page = history_page
    assert page.evaluate("""() => {
                const live = {v: 1, deep: {n: 1}};
                window.__clock = 1000;
                window.__h.push(live);
                live.deep.n = 999;
                const got = window.__h.current();
                got.deep.n = 12345;
                return window.__h.current().deep.n;
            }""") == 1


def test_history_coalesces_rapid_same_field_edits(history_page):
    """Typing a title is one undo step, not one per keystroke."""
    page = history_page
    depth = page.evaluate("""() => {
            for (let i = 1; i <= 5; i += 1) {
                window.__clock += 50;
                window.__h.push({v: i}, {coalesceKey: 'seo.title.en'});
            }
            return window.__h.stats().entries;
        }""")
    assert depth == 2  # the initial state plus one coalesced edit
    assert page.evaluate("window.__h.current().v") == 5
    assert page.evaluate("window.__h.undo().v") == 0


def test_history_does_not_coalesce_across_the_window_or_across_fields(history_page):
    page = history_page
    entries = page.evaluate("""() => {
            window.__clock += 50;
            window.__h.push({v: 1}, {coalesceKey: 'a'});
            window.__clock += 5000;                     // past the window
            window.__h.push({v: 2}, {coalesceKey: 'a'});
            window.__clock += 10;
            window.__h.push({v: 3}, {coalesceKey: 'b'}); // different field
            window.__clock += 10;
            window.__h.push({v: 4});                     // no key at all
            return window.__h.stats().entries;
        }""")
    assert entries == 5


def test_holding_a_key_down_cannot_collapse_a_whole_session_into_one_step(history_page):
    """The coalesced entry keeps the ORIGINAL timestamp, so a continuous stream
    of edits still breaks into steps instead of rolling the window forward for
    ever."""
    page = history_page
    entries = page.evaluate("""() => {
            for (let i = 0; i < 40; i += 1) {
                window.__clock += 100;
                window.__h.push({v: i}, {coalesceKey: 'same'});
            }
            return window.__h.stats().entries;
        }""")
    assert entries > 2


def test_history_keeps_at_least_fifty_states_and_drops_from_the_tail(browser_page):
    """Undo depth >= 50, bounded memory, and the head -- the state on screen --
    is never the one thrown away."""
    page, _violations = browser_page
    _history_harness(page, options="{maxEntries: 50}")
    result = page.evaluate("""() => {
            for (let i = 1; i <= 200; i += 1) {
                window.__clock += 1000;
                window.__h.push({v: i});
            }
            const stats = window.__h.stats();
            let steps = 0;
            let last = window.__h.current().v;
            while (window.__h.canUndo()) { last = window.__h.undo().v; steps += 1; }
            return {entries: stats.entries, dropped: stats.droppedFromTail,
                    steps: steps, oldest: last};
        }""")
    assert result["entries"] == 50
    assert result["steps"] >= 49
    assert result["dropped"] > 0
    # The oldest surviving state is a LATE one: the tail went, the head stayed.
    assert result["oldest"] > 100


def test_history_memory_ceiling_also_drops_from_the_tail_and_keeps_undo_alive(browser_page):
    page, _violations = browser_page
    _history_harness(page, options="{maxEntries: 500, maxChars: 4000}")
    result = page.evaluate("""() => {
            for (let i = 1; i <= 50; i += 1) {
                window.__clock += 1000;
                window.__h.push({v: i, pad: 'x'.repeat(400)});
            }
            const stats = window.__h.stats();
            return {entries: stats.entries, chars: stats.chars,
                    canUndo: window.__h.canUndo(), head: window.__h.current().v};
        }""")
    assert result["entries"] >= 2
    assert result["chars"] <= 4000 or result["entries"] == 2
    assert result["canUndo"] is True, "the memory ceiling must never disable undo entirely"
    assert result["head"] == 50


def test_a_push_after_an_undo_abandons_the_redo_branch(history_page):
    page = history_page
    assert page.evaluate("""() => {
                window.__clock += 1000; window.__h.push({v: 1});
                window.__clock += 1000; window.__h.push({v: 2});
                window.__h.undo();
                window.__clock += 1000; window.__h.push({v: 9});
                return window.__h.canRedo();
            }""") is False


def test_the_transaction_surface_batch_13_will_use(history_page):
    """A drag produces a state per pointermove. One undo step, or none if the
    drag is cancelled -- and nothing may interleave while it is open."""
    page = history_page
    result = page.evaluate("""() => {
            const before = window.__h.stats().entries;
            const t = window.__h.begin('move block');
            const pushedMidDrag = window.__h.push({v: 99});
            const undoDuring = window.__h.canUndo();
            window.__clock += 1000;
            const committed = window.__h.commit(t, {v: 7});
            const t2 = window.__h.begin('resize block');
            window.__h.abort(t2);
            const stale = window.__h.commit(t2, {v: 8});
            return {before: before, pushedMidDrag: pushedMidDrag, undoDuring: undoDuring,
                    committed: committed, stale: stale,
                    after: window.__h.stats().entries, head: window.__h.current().v};
        }""")
    assert result["pushedMidDrag"] is False
    assert result["undoDuring"] is False
    assert result["committed"] is True
    assert result["stale"] is False, "a commit on a finished transaction must be a no-op"
    assert result["after"] == result["before"] + 1
    assert result["head"] == 7


def test_history_notifies_subscribers_so_the_undo_buttons_can_follow(history_page):
    page = history_page
    assert page.evaluate("""() => {
                let calls = 0;
                const off = window.__h.subscribe(() => { calls += 1; });
                window.__clock += 1000; window.__h.push({v: 1});
                window.__h.undo();
                off();
                window.__clock += 1000; window.__h.push({v: 2});
                return calls;
            }""") == 2


def test_history_reset_forgets_everything_and_starts_a_new_baseline(history_page):
    page = history_page
    result = page.evaluate("""() => {
            window.__clock += 1000; window.__h.push({v: 1});
            window.__h.reset({v: 100});
            return {canUndo: window.__h.canUndo(), canRedo: window.__h.canRedo(),
                    head: window.__h.current().v, entries: window.__h.stats().entries};
        }""")
    assert result == {"canUndo": False, "canRedo": False, "head": 100, "entries": 1}
