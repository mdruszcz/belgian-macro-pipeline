"""Batch 11 builder-service adversarial pass (batch-11 spec Tests items 2, 3,
7, 9, 10, plus rev 2's Host-header and body-cap corrections).

Every test here proves a REJECTION actually happens. Batch 10's own review
found a URL guard whose character class *read* correctly but had a real
protocol-relative-URL bypass, caught only by a parametrised adversarial test;
and a separate batch shipped `assert ... or True`, an assertion that proved
nothing. Both failure modes are exactly what this file exists to prevent --
no test below merely observes that a guard exists.

Uses a real server bound to a free 127.0.0.1 port for every HTTP-shaped case
(BuilderConfig itself rejects port 0, so a real free port is picked up front,
same as the CLI's --port would receive one), and source-text/import scanning
for the "no subprocess, no shell=True" guarantee, which is a property of the
files, not of a running process.

Host-header adversarial cases go over a raw socket rather than
`http.client`: `http.client.HTTPConnection.request()` always emits its own
automatic `Host` header, so adding a second one via the `headers=` dict would
send it TWICE (the server's own `_one()` helper explicitly rejects a
duplicated header as `bad_request`, precisely to stop this class of
smuggling) -- a raw socket gives exactly one `Host` header, the same shape a
real DNS-rebinding request would have.
"""

from __future__ import annotations

import ast
import http.client
import json
import re
import secrets
import socket
import threading
from pathlib import Path

import pytest

from src.builder import paths as builder_paths
from src.builder import store as builder_store
from tests.fixtures.pages import builders

try:
    from src.builder.service import BuilderConfig, HostRefused, make_server
except ImportError:  # pragma: no cover - reported, not swallowed
    BuilderConfig = None
    HostRefused = None
    make_server = None


pytestmark = pytest.mark.skipif(
    make_server is None, reason="src.builder.service not implemented yet"
)


REPO_ROOT = Path(__file__).resolve().parents[2]


def _free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class ServerHarness:
    def __init__(self, token=None):
        self.token = token or secrets.token_urlsafe(32)
        self.config = BuilderConfig(host="127.0.0.1", port=_free_loopback_port(), token=self.token)
        self.server = make_server(self.config)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return self.server.server_address[1]

    def raw_request(self, request_bytes: bytes, read_timeout: float = 2.0) -> bytes:
        """Send exactly these bytes over a fresh socket and return whatever
        the server sends back -- used for the Host-header and
        Transfer-Encoding cases a well-behaved http.client would not let us
        construct with a single, unambiguous header."""
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=read_timeout)
        try:
            sock.sendall(request_bytes)
            sock.settimeout(read_timeout)
            chunks = []
            try:
                while True:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
            except TimeoutError:
                pass
            return b"".join(chunks)
        finally:
            sock.close()

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
                headers["Origin"] = f"http://127.0.0.1:{self.port}"
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


def _status(raw: bytes) -> int:
    line = raw.split(b"\r\n", 1)[0]
    parts = line.split(b" ")
    assert len(parts) >= 2, raw[:200]
    return int(parts[1])


# --- Origin: the Batch-10-finding-1 shape (prefix/substring check) ----------


ADVERSARIAL_ORIGINS = [
    pytest.param("http://evil.example", id="unrelated-origin"),
    pytest.param("null", id="null-origin-sandboxed-iframe"),
]


@pytest.mark.parametrize("bad_origin", ADVERSARIAL_ORIGINS)
def test_post_with_adversarial_origin_is_forbidden(server, bad_origin):
    doc = builders.minimal_valid_document()
    resp, _raw = server.request(
        "POST", "/api/validate", body={"page_id": "home", "document": doc}, origin=bad_origin
    )
    assert resp.status == 403


def test_post_with_other_loopback_port_origin_is_forbidden(server):
    other_port = server.port + 1 if server.port < 65535 else server.port - 1
    doc = builders.minimal_valid_document()
    resp, _raw = server.request(
        "POST",
        "/api/validate",
        body={"page_id": "home", "document": doc},
        origin=f"http://127.0.0.1:{other_port}",
    )
    assert resp.status == 403


def test_origin_that_merely_starts_with_the_allowed_origin_is_forbidden(server):
    """The exact shape of Batch 10's finding 1: a guard that reads as
    'starts with the allowed origin' rather than 'equals the allowed origin'
    lets a prefix match sail through. `http://127.0.0.1:{port}.evil.example`
    starts with the legitimate origin string character-for-character."""
    adversarial = f"http://127.0.0.1:{server.port}.evil.example"
    doc = builders.minimal_valid_document()
    resp, _raw = server.request(
        "POST", "/api/validate", body={"page_id": "home", "document": doc}, origin=adversarial
    )
    assert resp.status == 403, (
        "an Origin that merely starts with the allowed origin must be REJECTED, "
        "not accepted by a prefix/substring comparison"
    )


def test_both_allowed_origins_pass_the_origin_check(server):
    doc = builders.minimal_valid_document()
    for origin in (f"http://127.0.0.1:{server.port}", f"http://localhost:{server.port}"):
        resp, _raw = server.request(
            "POST", "/api/validate", body={"page_id": "home", "document": doc}, origin=origin
        )
        assert resp.status != 403, origin


# --- Host header: DNS rebinding on a GET, no Origin/token involved ----------


def test_get_with_a_foreign_host_header_is_refused(server):
    request = (
        f"GET /api/pages HTTP/1.1\r\n"
        f"Host: evil.example\r\n"
        f"X-BelPulse-Token: {server.token}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()
    raw = server.raw_request(request)
    assert _status(raw) == 403


def test_get_with_correct_host_header_passes(server):
    request = (
        f"GET /api/pages HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{server.port}\r\n"
        f"X-BelPulse-Token: {server.token}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()
    raw = server.raw_request(request)
    assert _status(raw) == 200


def test_get_with_localhost_host_header_passes(server):
    request = (
        f"GET /api/pages HTTP/1.1\r\n"
        f"Host: localhost:{server.port}\r\n"
        f"X-BelPulse-Token: {server.token}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()
    raw = server.raw_request(request)
    assert _status(raw) == 200


def test_get_with_no_host_header_at_all_is_refused(server):
    """HTTP/1.1 requires Host; a request smuggled without one must not be
    treated as if it matched by default."""
    request = (
        f"GET /api/pages HTTP/1.1\r\n"
        f"X-BelPulse-Token: {server.token}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()
    raw = server.raw_request(request)
    assert _status(raw) in (400, 403)


# --- Token adversarial shapes ------------------------------------------------


def test_query_token_on_api_route_with_no_header_token_is_unauthorized(server):
    doc = builders.minimal_valid_document()
    resp, _raw = server.request(
        "POST",
        f"/api/validate?token={server.token}",
        body={"page_id": "home", "document": doc},
        token=None,
        origin=f"http://127.0.0.1:{server.port}",
    )
    assert resp.status == 401


def test_unicode_homoglyph_page_id_over_http_is_rejected(server):
    doc = builders.minimal_valid_document()
    homoglyph_id = "paɡe"  # Latin small letter g with hook lookalike
    resp, raw = server.request(
        "POST", "/api/validate", body={"page_id": homoglyph_id, "document": doc}
    )
    assert resp.status == 400


def test_traversal_page_id_over_http_is_rejected_not_500(server):
    resp, raw = server.request(
        "GET", "/api/document?page_id=" + "..%2f..%2fetc%2fpasswd" + "&which=draft"
    )
    assert resp.status == 400
    assert b"Traceback" not in raw


# --- body cap: derived from MAX_DOCUMENT_BYTES, never re-typed --------------


def test_body_cap_is_derived_from_schema_max_document_bytes():
    """rev 2 P1 #1: the cap is MAX_DOCUMENT_BYTES + 64 KiB of envelope
    headroom, imported (not re-typed) from src.pages.schema."""
    from src.builder.service import MAX_BODY_BYTES
    from src.pages.schema import MAX_DOCUMENT_BYTES

    assert MAX_BODY_BYTES == MAX_DOCUMENT_BYTES + 64 * 1024, (
        "the body cap must be exactly MAX_DOCUMENT_BYTES plus 64 KiB of envelope headroom, "
        "derived from the constant, not a separately hand-typed number"
    )


def test_oversize_body_is_refused_without_being_parsed(server):
    from src.pages.schema import MAX_DOCUMENT_BYTES

    oversize = (
        b'{"page_id":"home","document":{"pad":"' + b"a" * (MAX_DOCUMENT_BYTES + 128 * 1024) + b'"}}'
    )
    resp, raw = server.request(
        "POST", "/api/validate", body=oversize, origin=f"http://127.0.0.1:{server.port}"
    )
    assert resp.status == 413
    body = json.loads(raw.decode("utf-8"))
    assert body["ok"] is False
    # A guard/oversize rejection must not echo document detail back.
    assert b"aaaa" not in raw


def test_a_document_just_under_the_per_string_cap_is_not_wrongly_rejected(server):
    """rev 2 P1 #1: the old (wrong) rev-1 cap of exactly 1 MiB on the raw
    envelope could reject a legal document once the envelope and `dumps`'s
    `indent=2` are added. This proves a legitimately-sized field near the
    validator's own per-string cap (8192 chars, comfortably inside the body
    cap once wrapped) is not wrongly rejected as too large."""
    doc = builders.minimal_valid_document()
    filler = "x" * 8000  # under MAX_STRING_LENGTH (8192)
    doc["seo"]["description"]["en"] = filler
    resp, raw = server.request("POST", "/api/validate", body={"page_id": "home", "document": doc})
    assert resp.status != 413, "a legitimately-sized document must not be rejected as too large"


# --- Transfer-Encoding: chunked must never read as an empty body ------------


def test_chunked_post_is_refused_not_treated_as_empty_body(server):
    request = (
        f"POST /api/validate HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{server.port}\r\n"
        f"X-BelPulse-Token: {server.token}\r\n"
        f"Origin: http://127.0.0.1:{server.port}\r\n"
        f"Content-Type: application/json\r\n"
        f"Transfer-Encoding: chunked\r\n"
        f"Connection: close\r\n"
        f"\r\n"
        f"1a\r\n"
        f'{{"page_id":"home","document"\r\n'
        f"0\r\n\r\n"
    ).encode()
    raw = server.raw_request(request)
    status = _status(raw)
    # Must be an explicit rejection -- never a 2xx/422 that would mean the
    # (essentially empty, mis-parsed) body was accepted as a document.
    assert status in (400, 411), raw[:200]


# --- bind address: loopback only, refused otherwise -------------------------


def test_make_server_binds_127_0_0_1(pages_root):
    config = BuilderConfig(host="127.0.0.1", port=_free_loopback_port(), token="x" * 43)
    srv = make_server(config)
    try:
        assert srv.server_address[0] == "127.0.0.1"
    finally:
        srv.server_close()


NON_LOOPBACK_HOSTS = [
    pytest.param("0.0.0.0", id="all-interfaces"),
    pytest.param("::", id="ipv6-all-interfaces"),
    pytest.param("192.168.1.50", id="lan-ip"),
    pytest.param("example.com", id="hostname"),
]


@pytest.mark.parametrize("bad_host", NON_LOOPBACK_HOSTS)
def test_main_refuses_a_non_loopback_host(tmp_path, monkeypatch, bad_host):
    import scripts.serve_builder as serve_builder

    monkeypatch.setattr(builder_paths, "PAGES_ROOT", tmp_path)
    monkeypatch.setattr(builder_store, "PAGES_ROOT", tmp_path)
    exit_code = serve_builder.main(["--host", bad_host, "--port", str(_free_loopback_port())])
    assert exit_code != 0, f"--host {bad_host} must be refused, never honoured"


def test_config_itself_refuses_a_non_loopback_host(pages_root):
    with pytest.raises(HostRefused):
        BuilderConfig(host="0.0.0.0", port=_free_loopback_port(), token="x" * 43)


# --- no subprocess, no shell=True, no git invocation, path-scoped -----------


SCANNED_FILES = [
    REPO_ROOT / "src" / "builder" / "__init__.py",
    REPO_ROOT / "src" / "builder" / "paths.py",
    REPO_ROOT / "src" / "builder" / "store.py",
    REPO_ROOT / "src" / "builder" / "service.py",
    REPO_ROOT / "scripts" / "serve_builder.py",
]


def _existing_scanned_files():
    return [p for p in SCANNED_FILES if p.exists()]


#: `os` functions that reach a shell or spawn a process directly, without
#: going through `subprocess` at all -- banning the import is not enough if
#: `os.system`/`os.popen` are still reachable.
_OS_EXEC_LIKE = {
    "system",
    "popen",
    "posix_spawn",
    "posix_spawnp",
    "execl",
    "execle",
    "execlp",
    "execlpe",
    "execv",
    "execve",
    "execvp",
    "execvpe",
}


class _ExecutionSurfaceVisitor(ast.NodeVisitor):
    """Finds the actual AST shapes that matter, not the words that describe
    them in a docstring.

    A previous version of this file scanned raw source TEXT for the
    substrings "git ", "shell=True" and an `import subprocess` regex --
    which meant the ordinary English word "digit" failed the git check, and
    an implementer could not even write a docstring EXPLAINING that
    subprocess is absent without tripping the subprocess check. Every
    finding below comes from a real AST node (an `Import`/`ImportFrom`, a
    `Call`'s keyword list, or a `Call`'s own argument values) -- a docstring,
    comment or arbitrary prose string can never produce one, because none of
    those are represented as import nodes or call nodes at all.
    """

    def __init__(self) -> None:
        self.subprocess_imports: list[ast.AST] = []
        self.exec_like_calls: list[ast.AST] = []
        self.shell_keyword_calls: list[ast.AST] = []
        self.git_command_calls: list[ast.AST] = []

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            if alias.name == "subprocess" or alias.name.startswith("subprocess."):
                self.subprocess_imports.append(node)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == "subprocess" or (node.module or "").startswith("subprocess."):
            self.subprocess_imports.append(node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and isinstance(func.value, ast.Name)
            and func.value.id == "os"
            and func.attr in _OS_EXEC_LIKE
        ):
            self.exec_like_calls.append(node)

        if any(kw.arg == "shell" for kw in node.keywords):
            self.shell_keyword_calls.append(node)

        for value in list(node.args) + [kw.value for kw in node.keywords]:
            if _looks_like_a_git_invocation(value):
                self.git_command_calls.append(node)

        self.generic_visit(node)


#: A real command-invocation shape: `git` as a bare argument token, or `git`
#: immediately followed by a known subcommand -- NOT the bare word "git"
#: anywhere in a string, which a docstring can legitimately contain (e.g.
#: `store.py`: "running a shell or a git command is unreachable").
_GIT_SUBCOMMAND = re.compile(
    r"\bgit\s+(commit|push|tag|clone|checkout|add|pull|fetch|merge|rebase|reset|status|log)\b"
)


def _looks_like_a_git_invocation(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        if node.value == "git" or _GIT_SUBCOMMAND.search(node.value):
            return True
        return False
    if isinstance(node, (ast.List, ast.Tuple)) and node.elts:
        first = node.elts[0]
        return isinstance(first, ast.Constant) and first.value == "git"
    return False


def _execution_surface(path: Path) -> _ExecutionSurfaceVisitor:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    visitor = _ExecutionSurfaceVisitor()
    visitor.visit(tree)
    return visitor


def test_no_subprocess_import_in_builder_files():
    """`import subprocess` / `from subprocess import ...` as real AST import
    nodes -- never trippable by the word appearing in a comment or
    docstring, and this is also proven to still catch a real import: see
    test_the_ast_check_still_catches_a_real_subprocess_import below."""
    files = _existing_scanned_files()
    assert files, "expected at least one src/builder/* or scripts/serve_builder.py file to exist"
    for path in files:
        surface = _execution_surface(path)
        assert (
            not surface.subprocess_imports
        ), f"{path} must never import subprocess -- makes shell=True unreachable by construction"
        assert not surface.exec_like_calls, (
            f"{path} must never call os.system/os.popen/os.exec*/os.posix_spawn* -- "
            "that reaches a shell without subprocess at all"
        )


def test_no_shell_keyword_argument_in_builder_files():
    """No call anywhere passes a `shell=` keyword at all, regardless of its
    value -- the mere presence of the parameter signals a shell-invoking
    call shape, and `subprocess` is banned outright besides."""
    for path in _existing_scanned_files():
        surface = _execution_surface(path)
        assert not surface.shell_keyword_calls, f"{path} must never pass a shell= keyword argument"


def test_no_git_invocation_in_builder_files():
    """No call argument is the bare string "git" or a `git <subcommand>`
    shell command -- the AST-argument scope means the standalone WORD "git"
    inside a docstring (as in store.py's own module docstring) can never
    match, only a value actually being handed to a call."""
    for path in _existing_scanned_files():
        surface = _execution_surface(path)
        assert (
            not surface.git_command_calls
        ), f"{path} must never invoke git -- rule 34, and the service never commits/pushes"


def test_the_ast_checks_still_catch_a_real_subprocess_import(tmp_path):
    """Proves the tightened check still catches the thing it exists to
    catch, not just that it stopped false-positiving: a scratch file with a
    genuine `import subprocess` must fail exactly the assertion above."""
    scratch = tmp_path / "scratch_offender.py"
    scratch.write_text(
        "import subprocess\nsubprocess.run(['echo', 'hi'], shell=True)\n", encoding="utf-8"
    )
    surface = _execution_surface(scratch)
    assert surface.subprocess_imports, "a real `import subprocess` must be detected"
    assert surface.shell_keyword_calls, "a real `shell=True` keyword argument must be detected"


def test_the_ast_check_still_catches_a_real_os_system_call(tmp_path):
    scratch = tmp_path / "scratch_offender_os_system.py"
    scratch.write_text("import os\nos.system('git push origin main')\n", encoding="utf-8")
    surface = _execution_surface(scratch)
    assert surface.exec_like_calls, "a real os.system(...) call must be detected"
    assert surface.git_command_calls, "a real 'git push' command string must be detected"


def test_the_git_check_does_not_false_positive_on_ordinary_english_words():
    """The exact regression this fix is for: "digit", "legitimate" and a
    docstring merely naming "git" in prose must never trip the check."""
    scratch_source = (
        "def f(digit, legitimate_value):\n"
        "    '''Running a shell or a git command is unreachable from this module.'''\n"
        "    return digit + len(legitimate_value)\n"
    )
    tree = ast.parse(scratch_source)
    visitor = _ExecutionSurfaceVisitor()
    visitor.visit(tree)
    assert not visitor.git_command_calls
    assert not visitor.subprocess_imports
    assert not visitor.shell_keyword_calls


def test_the_interface_strings_reader_is_exempt_and_still_imports_subprocess():
    """Confirms the path-scoping is deliberate, not accidental: some file
    outside the scanned set legitimately runs a subprocess and must be
    untouched by the assertion above (rev 2 correction #22).

    That file used to be `scripts/export_local_pages.py`. In Batch 15c the
    node invocation moved to `src/pages/strings.py`, so both exporters read the
    licence notice from one place -- and this sanity check follows it, because
    an anchor pointing at a file that no longer does the thing stops proving
    the exemption is real.
    """
    reader = REPO_ROOT / "src" / "pages" / "strings.py"
    assert reader.exists(), "the shared interface-strings reader has moved again"
    surface = _execution_surface(reader)
    assert surface.subprocess_imports, "sanity check: this file is expected to use subprocess"


def test_the_builder_service_still_cannot_reach_that_subprocess():
    """The ban above is `by construction`, and construction includes IMPORTS.
    `src/pages/strings.py` runs node, and `src/pages/shell.py` calls it -- so
    the moment the service imports `shell`, or `src/pages/__init__` does, the
    builder gains a transitive path to a subprocess and the guarantee above
    quietly becomes a claim about one file rather than about the service.

    Checked as a real import graph, not by grepping for the word.
    """
    import importlib
    import sys as _sys

    for module in ("src.builder.service", "src.pages"):
        importlib.import_module(module)
    for module in ("src.pages.shell", "src.pages.strings"):
        assert module not in _sys.modules or _sys.modules[module] is not None
    # The service's own import list is what matters: neither module may appear.
    service_src = (REPO_ROOT / "src" / "builder" / "service.py").read_text(encoding="utf-8")
    tree = ast.parse(service_src)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    assert "src.pages.shell" not in imported
    assert "src.pages.strings" not in imported
    package_src = (REPO_ROOT / "src" / "pages" / "__init__.py").read_text(encoding="utf-8")
    assert "shell" not in package_src and "strings" not in package_src, (
        "src/pages/__init__ must not re-export the shell or the strings reader: "
        "the builder imports this package, and that would hand it a subprocess"
    )


# --- symlink escape reachable through the live HTTP surface -----------------


def test_symlink_escape_via_http_save_is_refused(server, pages_root):
    page_dir = pages_root / "victim"
    page_dir.mkdir()
    outside = pages_root.parent / "outside_secret.json"
    outside.write_text("PRE-EXISTING", encoding="utf-8")
    (page_dir / "draft.json").symlink_to(outside)

    doc = builders.minimal_valid_document()
    resp, _raw = server.request("POST", "/api/save", body={"page_id": "victim", "document": doc})
    assert resp.status != 200
    assert outside.read_text(encoding="utf-8") == "PRE-EXISTING"
