"""The local builder API: one loopback listener, one per-process token.

Batch 11 of docs/features/page_builder.md. This is the first component in the
programme with filesystem write access and a network listener, so the posture
is deliberately paranoid and each guard below is written against the attack it
is supposed to stop, not against the sentence in the spec that asks for it.

What holds this together:

* **Loopback only.** `--host` accepts nothing but `127.0.0.1`/`localhost`;
  `0.0.0.0`, `::`, a LAN address and a hostname are REFUSED, not honoured.
  The public site stays static (rule 30) -- this server is a local editor, and
  nothing it produces needs it to be running.
* **A capability, not a credential.** The session token comes from
  `secrets.token_urlsafe(32)` once per process, is never written to disk and is
  never read from configuration or the environment. It authorises the one
  browser tab the operator opened; it is not a repository secret, which is the
  reading of "no credentials to the browser" this batch adopts.
* **Three independent checks on every request**: `Host` allowlist (Origin and a
  token do not stop a DNS-rebinding GET), the token, and -- on writes --
  an exact `Origin` match. A MISSING `Origin` on a POST is a rejection: a form
  post from another origin can simply omit it.
* **No CORS header is ever emitted.** Not `*`, not a reflected origin, not
  `Access-Control-Allow-*` at all. There is no browser we want to grant
  cross-origin access to.
* **Nothing about this machine reaches the browser.** No environment value, no
  absolute path, no repository path, no stack trace. A 500 is opaque to the
  client and detailed only in the local log, and `log_message` strips the query
  string so the token never lands in the terminal transcript either.
* **Single-threaded `HTTPServer`, explicitly not `ThreadingHTTPServer`.** The
  deep-nesting guard in `src/pages/serialize.py` depends on CPython raising
  `RecursionError`; on a worker thread with a smaller C stack that can become a
  hard crash instead, and a guard that crashes the process has not guarded
  anything. It also removes the version-number race in `store.publish`.
* **Nothing here can spawn a process.** No process-spawning module is
  imported anywhere in this package, so running a shell is unreachable rather
  than merely avoided, and publishing cannot commit or push (rule 34). The
  hardening suite asserts this by scanning these files as text, which is why
  the forbidden spellings do not appear even inside a docstring.
"""

import hmac
import json
import re
import secrets
import traceback
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from src.builder import store
from src.builder.paths import PathError, validate_page_id, validate_version, validate_which
from src.pages import (
    dumps,
    is_guard_error,
    load_metadata,
    load_registry,
    loads,
    migrate,
    render_document,
    validate_document,
)
from src.pages.metadata import PageMetadata
from src.pages.registry import Registry
from src.pages.schema import (
    MAX_DOCUMENT_BYTES,  # deep import on purpose: not in src.pages.__all__
    PageValidationError,
    check_raw_size,
)

#: The only address this service will ever bind, and the only two spellings of
#: it a request may use.
BIND_ADDRESS = "127.0.0.1"
LOOPBACK_NAMES = frozenset({"127.0.0.1", "localhost"})

DEFAULT_PORT = 8787

TOKEN_HEADER = "X-BelPulse-Token"
TOKEN_BYTES = 32

#: The document cap the validator itself enforces, plus room for the
#: `{"page_id", "document"}` envelope and its JSON punctuation. Imported, never
#: re-typed: a copy of the number here would drift from `src/pages/schema.py`
#: and either 413 a legal document or accept one `load_document` then refuses.
ENVELOPE_HEADROOM_BYTES = 65_536
MAX_BODY_BYTES = MAX_DOCUMENT_BYTES + ENVELOPE_HEADROOM_BYTES

LANGUAGES = ("en", "fr", "nl")

JSON_CONTENT_TYPE = "application/json"

#: Route allowlists. A request path is decoded ONCE and must then equal one of
#: these exactly, so there is nothing left to normalise afterwards and
#: `/api/pages/../pages` is a 404 rather than a second spelling of
#: `/api/pages`.
#:
#: Note what this does NOT mean, corrected in Batch 12a because the sentence
#: that used to stand here said the opposite and would have misled the next
#: reader: decoding happens BEFORE the comparison, so `/api/%73ave` IS matched
#: as `/api/save` (it answers 405 to a GET, not 404). That is the intended and
#: correct behaviour -- percent-encoding is a transport spelling of the same
#: path, and treating the two differently is how a filter and a handler end up
#: disagreeing about which route ran. The traversal defence is the exact match
#: after a single decode, not a refusal to decode.
GET_ROUTES = frozenset(
    {"/", "/api/pages", "/api/document", "/api/registry", "/api/versions", "/preview"}
)
POST_ROUTES = frozenset({"/api/validate", "/api/save", "/api/publish", "/api/restore"})

#: The repository root, derived from this file's own location and from nothing
#: a request can influence.
_REPO_ROOT = Path(__file__).resolve().parents[2]

#: Where the shell's real, separately-owned source files live (ADR 0004).
BUILDER_APP_DIR = _REPO_ROOT / "builder" / "app"

#: The PUBLIC design system. READ from here, never copied into `builder/app/`:
#: a second copy is exactly the drift the shared renderer exists to prevent.
DESIGN_SYSTEM_DIR = _REPO_ROOT / "assets" / "belpulse"

#: THERE IS NO STATIC-FILE ROUTE, DELIBERATELY. `bootstrap_html` inlines the
#: shell from these fixed tuples, so no filename is ever derived from a
#: request: there is no path to traverse, no allowlist to get wrong and no
#: directory to list. A route that cannot be addressed cannot be attacked.
#:
#: ORDER IS LOAD ORDER. Every file is concatenated into ONE closure, in which a
#: `shell` namespace object and the `api(path, options)` token closure are
#: already in scope. `history.js` is builder-core's and must stay first, since
#: the interface layer builds on it.
#:
#: Batch 12b adds its own filenames here -- that ONE-LINE edit is the only
#: change to this module its scope allows.
SHELL_JS_FILES: tuple[str, ...] = (
    "history.js",
    "dom.js",
    "model.js",
    "api.js",
    "canvas.js",
    "inspector.js",
    "sidebar.js",
    "topbar.js",
    "app.js",
)
SHELL_CSS_FILES: tuple[str, ...] = ("shell.css",)

#: Read and handed to the shell as a string constant for `srcdoc` injection.
#: The preview iframe has a BARE sandbox, so it has an opaque origin and a
#: `<link rel=stylesheet>` inside it cannot load. Inlining the text is the only
#: way to style the preview that does not require `allow-same-origin`, which is
#: forbidden.
PREVIEW_CSS_FILES: tuple[str, ...] = (
    "tokens.css",
    "components.css",
    "layout.css",
    "blocks.css",
)

#: A shell filename must be a plain lowercase basename with a .js/.css suffix.
#: Nothing user-controlled reaches these tuples, so this is not the security
#: boundary -- it is the guard that keeps a future one-line edit to the tuples
#: from quietly introducing a path component.
_SHELL_FILENAME = re.compile(r"\A[a-z0-9]+(?:[._-][a-z0-9]+)*\.(?:js|css)\Z")

#: Bytes of entropy for the per-response script nonce in the CSP on `/`.
CSP_NONCE_BYTES = 16

STATUS_FOR_CODE = {
    "bad_request": 400,
    "unauthorized": 401,
    "forbidden_origin": 403,
    "forbidden_host": 403,
    "not_found": 404,
    "method_not_allowed": 405,
    "validation_failed": 422,
    "guard_rejected": 413,
    "payload_too_large": 413,
    "version_space_exhausted": 409,
    "internal_error": 500,
}


class HostRefused(ValueError):
    """`--host` named something that is not loopback. Refused, never honoured."""


def mint_token() -> str:
    """A fresh per-process session token. Never persisted, never configured."""
    return secrets.token_urlsafe(TOKEN_BYTES)


def resolve_bind_host(host: str) -> str:
    """`127.0.0.1` for either loopback spelling; raise for anything else.

    Refusing rather than silently rewriting is the point: an operator who
    types `--host 0.0.0.0` has asked for the whole LAN to reach a service that
    writes files, and a server that quietly bound loopback anyway would leave
    them believing the opposite of whichever thing is true.
    """
    if not isinstance(host, str) or host.strip().lower() not in LOOPBACK_NAMES:
        raise HostRefused(
            f"refusing to bind {host!r}: the builder serves {BIND_ADDRESS} only "
            "(it writes files and holds a session token)"
        )
    return BIND_ADDRESS


def _valid_port(port: object) -> int:
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError(f"port must be an integer between 1 and 65535, got {port!r}")
    return port


@dataclass
class BuilderConfig:
    """One server's settings, with the token minted and the read-only inputs
    loaded exactly once.

    `load_metadata()` reads four files (~6.4 ms) and `load_registry()` one;
    `src/pages/metadata.py` already says "once per validator instance", so
    doing it per request would be both slower and a way for the answer to
    change under a running editor. A missing or malformed input is fatal at
    startup, naming the path, rather than a repeating opaque 500.
    """

    host: str = BIND_ADDRESS
    port: int = DEFAULT_PORT
    token: str = field(default_factory=mint_token)
    metadata: PageMetadata | None = None
    registry: Registry | None = None

    def __post_init__(self) -> None:
        self.host = resolve_bind_host(self.host)
        self.port = _valid_port(self.port)
        if not isinstance(self.token, str) or not self.token:
            raise ValueError("token must be a non-empty string")
        if self.metadata is None:
            self.metadata = load_metadata()
        if self.registry is None:
            self.registry = load_registry()

    @property
    def allowed_origins(self) -> frozenset[str]:
        return frozenset(f"http://{name}:{self.port}" for name in sorted(LOOPBACK_NAMES))

    @property
    def allowed_hosts(self) -> frozenset[str]:
        return frozenset(f"{name}:{self.port}" for name in sorted(LOOPBACK_NAMES))

    @property
    def url(self) -> str:
        return f"http://{BIND_ADDRESS}:{self.port}/"

    @property
    def token_url(self) -> str:
        """The one URL that works. Printed to the operator's terminal only."""
        return f"{self.url}?token={self.token}"


class ClientError(Exception):
    """A rejection to render as the uniform failure shape."""

    def __init__(self, code: str, message: str, errors: list[dict] | None = None):
        super().__init__(message)
        self.code = code
        self.client_message = message
        self.errors = errors or []


def _project(errors) -> list[dict]:
    """`validate_document` findings as `{code, path, message}`."""
    return [{"code": e.code, "path": e.path, "message": str(e.message)} for e in errors]


def _reject_json_constant(name: str):
    """Refuse `NaN`/`Infinity` in a request envelope.

    `json.loads` accepts them by default and `src.pages.dumps` uses
    `allow_nan=False`, so without this a body containing `NaN` would pass
    validation and then raise `ValueError` deep inside the write path -- a 500
    for what is really a malformed request.
    """
    raise ValueError(f"{name} is not valid JSON")


def preview_data(doc) -> dict:
    """`{block_id: {"state": "unavailable"}}` for every block with a binding.

    Batch 11 resolves no bindings, and rev 1's "honest empty states" claim was
    wrong: with `data={}`, `src/pages/render.py`'s `_state_for` returns
    `"loading"` for any block carrying a binding, so a preview would say
    "Loading..." forever for something that will never load -- the state
    confusion rule 26 exists to prevent. `unavailable` is the truth in the
    builder: there is no resolver yet.

    This resolves nothing and reads no payload. It looks at the document's own
    structure only.
    """
    data: dict = {}
    if not isinstance(doc, dict):
        return data
    for section in doc.get("sections") or []:
        if not isinstance(section, dict):
            continue
        for block in section.get("blocks") or []:
            if not isinstance(block, dict):
                continue
            block_id = block.get("id")
            if isinstance(block_id, str) and block_id and block.get("binding"):
                data[block_id] = {"state": "unavailable"}
    return data


def _canonical_text(doc) -> str:
    """The exact text about to be written, size-checked before writing.

    `validate_document` never measures raw size (`src/pages/document.py`
    starts at the tree caps), and `dumps` adds `indent=2` -- so a compact body
    comfortably under the cap can exceed it once canonicalised. Checking here
    means the service never writes a `draft.json` that `load_document` would
    afterwards refuse to read.
    """
    text = dumps(doc)
    oversize = check_raw_size(text)
    if oversize:
        raise ClientError(
            "payload_too_large",
            "the document is larger than this service will store",
        )
    return text


def _parse_document_text(text: str) -> dict:
    """`loads` + `migrate`, with findings turned into client rejections.

    The ONLY caller of `loads()` in this module. A second, unguarded call
    used to live in `_get_document` (to compare stored-vs-migrated shape) --
    a security-red-team pass found it 500'd on exactly the malformed-document
    shapes this function exists to turn into a proper 400/413/422, because
    `PageValidationError` is not `ClientError` and nothing else in the
    dispatch loop catches it. Callers that need the pre-migration document
    too use `parse_document_text_with_original` instead of reaching for
    `loads` a second time.
    """
    try:
        doc = loads(text)
        return migrate(doc)
    except PageValidationError as exc:
        raise _from_finding(exc) from exc


def _parse_document_text_with_original(text: str) -> tuple[dict, dict]:
    """(pre-migration, migrated). Same guarantees as `_parse_document_text`,
    for the one caller that needs to report whether migration changed
    anything."""
    try:
        stored = loads(text)
        return stored, migrate(stored)
    except PageValidationError as exc:
        raise _from_finding(exc) from exc


def _from_finding(exc: PageValidationError) -> ClientError:
    if is_guard_error(exc):
        # No detail echoed: the finding text quotes sizes and paths from a
        # document we have already decided is hostile.
        return ClientError("guard_rejected", "the document exceeded a resource limit")
    return ClientError("validation_failed", "the document is not valid", _project([exc]))


def registry_payload(registry: Registry) -> dict:
    """`config.registry` as something `json.dumps` will actually accept.

    Two traps, both of which a reasonable implementation walks straight into:

    * `Registry` is a FROZEN dataclass whose `block_types` and `defs` are
      `MappingProxyType`. `json.dumps` raises `TypeError` on a mappingproxy,
      and `dataclasses.asdict` raises too (it deep-copies, and a mappingproxy
      cannot be pickled). Both would be a 500 on every request, so the plain
      dict below is built by hand and a test pins the serialisation.
    * `load_registry()` DROPS four top-level keys the file carries
      (`accessible_name_note`, `deliberately_absent`, `forward_compatibility`,
      `props_schema_note`). So this endpoint does not return "the registry
      file"; it returns what the loader kept, which is what the validator and
      the renderer actually use. The block library must be driven by THIS, so
      that what the editor offers and what the public site renders cannot
      drift.

    `$defs` keeps its JSON-Schema spelling, because a shell that hands a props
    schema to a JSON-Schema-shaped control needs the `#/$defs/...` refs to
    resolve against the same key name the schema uses.
    """
    return {
        "registry_version": registry.registry_version,
        "block_types": dict(registry.block_types),
        "$defs": dict(registry.defs),
    }


def _read_shell_file(directory: Path, name: str) -> str:
    """One file's text, by a name that came from a module constant.

    Raises rather than skipping a missing file. A silent skip would ship a
    shell with a module quietly absent -- the interface would half-work, and
    the cause (a typo in the tuple above) would be invisible in the browser.
    """
    if not _SHELL_FILENAME.match(name):
        raise ValueError(f"{name!r} is not a plain shell filename")
    target = directory / name
    if not target.is_file():
        # The path is NOT quoted into the message: this string can reach the
        # operator's log, and nothing about this machine goes to the browser.
        raise FileNotFoundError(f"the builder shell file {name!r} is missing")
    return target.read_text(encoding="utf-8")


def _escape_script_end(text: str) -> str:
    """Neutralise `</script` in text that is about to sit inside `<script>`.

    The HTML parser ends a script element at the first `</script`, whatever the
    surrounding JavaScript thinks -- so one such sequence in a shell file would
    close the element early and spray the rest of the shell into the document
    as markup. `<\\/script` is a valid escape both inside a JavaScript string
    and inside a regular expression literal, and `</script` outside either is
    already a syntax error, so this rewrite can never break working code. A
    test also asserts no shell file contains the sequence in the first place;
    this is the second of the two locks, not the only one.
    """
    return text.replace("</script", "<\\/script")


def _js_string(text: str) -> str:
    """`text` as a JavaScript string literal, safe inside `<script>`."""
    return _escape_script_end(json.dumps(text, ensure_ascii=False))


def content_security_policy(nonce: str) -> str:
    """The policy `/` sends, decided here rather than left to a later batch.

    This was gated on a real browser, not reasoned about, because the failure
    mode is silent: a `srcdoc` iframe INHERITS its embedder's policy, so the
    preview's inlined stylesheet lives or dies by this string. `/` shipping no
    policy at all is the only reason the preview was styled before this batch,
    and the first person to add the obvious hardening would have unstyled the
    canvas without any error appearing anywhere.

    Why each directive is what it is:

    * `default-src 'none'` -- nothing loads unless named below.
    * `script-src 'nonce-...'` -- a fresh nonce per response. The shell is the
      only script in the document, and an injected `<script>` has no nonce.
    * `style-src 'unsafe-inline'` -- and it cannot be a nonce. Nonces are NOT
      inherited by a `srcdoc` document, so a nonce here would block the
      preview's inlined design-system CSS. Note the preview frame's bare
      sandbox already denies it script execution entirely, so inline style
      there is style and nothing else.
    * `connect-src 'self'` -- the API, on loopback, and nowhere else. The shell
      makes zero external requests and this makes that enforceable rather than
      merely true today.
    * `img-src 'self' data:` / `font-src 'self' data:` -- a block may carry an
      inline image; nothing may be fetched off this machine.
    * `frame-src 'self'` -- the preview frame only.
    * `base-uri 'none'`, `form-action 'none'`, `frame-ancestors 'none'` -- no
      base-tag hijack, no form posting anywhere, and this page may not be
      embedded by anything.
    """
    return "; ".join(
        (
            "default-src 'none'",
            f"script-src 'nonce-{nonce}'",
            "style-src 'unsafe-inline'",
            "connect-src 'self'",
            "img-src 'self' data:",
            "font-src 'self' data:",
            "frame-src 'self'",
            "base-uri 'none'",
            "form-action 'none'",
            "frame-ancestors 'none'",
        )
    )


#: The shell's own chrome. Deliberately small: it exists so the skeleton is a
#: usable page rather than unstyled text, and so the mount points below have a
#: shape. Batch 12b's stylesheet is concatenated AFTER this one and wins.
_SHELL_BASE_CSS = (
    ":root{color-scheme:light}"
    "*{box-sizing:border-box}"
    "body{margin:0;font:14px/1.5 system-ui,sans-serif;background:#fafafa;color:#18181b}"
    "#shell-root{display:flex;flex-direction:column;height:100vh}"
    "#shell-topbar{display:flex;gap:.5rem;align-items:center;padding:.5rem .75rem;"
    "border-bottom:1px solid #d4d4d8;background:#fff}"
    "#shell-body{display:flex;flex:1;min-height:0}"
    "#shell-sidebar,#shell-inspector{width:18rem;overflow:auto;background:#fff;padding:.75rem}"
    "#shell-sidebar{border-right:1px solid #d4d4d8}"
    "#shell-inspector{border-left:1px solid #d4d4d8}"
    "#shell-canvas{flex:1;min-width:0;display:flex;flex-direction:column;padding:.75rem;gap:.5rem}"
    "#shell-preview{flex:1;width:100%;border:1px solid #d4d4d8;background:#fff}"
    "#shell-status{margin:0;padding:.25rem .75rem;border-top:1px solid #d4d4d8;background:#fff}"
    ":focus-visible{outline:2px solid #1d4ed8;outline-offset:2px}"
    "#shell-too-narrow{display:none;padding:2rem}"
    "@media (max-width:1023px){#shell-root{display:none}#shell-too-narrow{display:block}}"
)


def bootstrap_html(config: BuilderConfig, nonce: str = "") -> str:
    """The builder shell: skeleton, inlined stylesheet, inlined script, token.

    Batch 12a. Batch 11's demo controls are gone; this is the page the real
    interface is built into.

    THERE IS NO STATIC-FILE ROUTE. The shell's files are read from
    `BUILDER_APP_DIR` at request time, by names taken from module constants,
    and emitted inline in this one document. That is not a shortcut -- it is
    what makes the shell loadable at all:

    * A subresource cannot send a header, and `_check_token` accepts a query
      token on `/` and nowhere else. `<script src=...>` would have 401'd on
      every load.
    * The token literal is therefore already inside the same closure as the
      code that needs it. It goes into no global, no storage, no cookie, no
      DOM attribute and no URL -- and the shell modules never receive the token
      at all, only an `api(path, options)` closure that adds the header for
      them. That closure also refuses any path that is not same-origin, so a
      mistyped absolute URL in a later batch cannot send the token elsewhere.
    * A stale cached script against a restarted server (and so a dead token) is
      impossible; there is nothing to cache.

    `history.replaceState` still drops `?token=` from the address bar, so the
    token does not sit in the browser's history for the next person here.

    The preview stays in a `<iframe sandbox>` with NO extra permissions --
    which is why the design-system CSS is read from `assets/belpulse/` and
    handed to the shell as a string for `srcdoc` injection. `/preview` renders
    UNVALIDATED on-disk content on the same origin as the write API; relaxing
    that sandbox so a stylesheet link would load turns any renderer slip into
    token theft plus arbitrary writes under `config/pages/`. The frame element
    stays in this Python skeleton on purpose, so the assertion about it is made
    against something a later batch cannot move.
    """
    token_literal = json.dumps(config.token)
    shell_css = "".join(_read_shell_file(BUILDER_APP_DIR, name) for name in SHELL_CSS_FILES)
    shell_js = "\n".join(
        f"/* builder/app/{name} */\n{_read_shell_file(BUILDER_APP_DIR, name)}"
        for name in SHELL_JS_FILES
    )
    preview_css = "\n".join(_read_shell_file(DESIGN_SYSTEM_DIR, name) for name in PREVIEW_CSS_FILES)
    nonce_attr = f' nonce="{nonce}"' if nonce else ""
    return (
        "<!doctype html>\n"
        '<html lang="en"><head><meta charset="utf-8">'
        '<meta name="referrer" content="no-referrer">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        "<title>BelPulse builder</title>"
        f"<style>{_escape_script_end(_SHELL_BASE_CSS)}"
        f"{_escape_script_end(shell_css)}</style>"
        "</head><body>"
        '<div id="shell-too-narrow"><h1>The builder needs a wider window</h1>'
        "<p>This is a desktop tool. Widen the window to at least 1024 pixels "
        "rather than working in a layout that would misrepresent what a "
        "visitor sees.</p></div>"
        '<div id="shell-root">'
        '<header id="shell-topbar" role="banner"></header>'
        '<div id="shell-body">'
        '<nav id="shell-sidebar" aria-label="Pages and blocks"></nav>'
        '<main id="shell-canvas">'
        '<iframe id="shell-preview" sandbox referrerpolicy="no-referrer" '
        'title="page preview"></iframe>'
        "</main>"
        '<aside id="shell-inspector" aria-label="Inspector"></aside>'
        "</div>"
        '<p id="shell-status" role="status" aria-live="polite">The builder '
        "interface is not built yet. Nothing here changes what a visitor sees "
        "until you publish explicitly.</p>"
        "</div>"
        f"<script{nonce_attr}>\n"
        "(function () {\n"
        '"use strict";\n'
        f"const TOKEN = {token_literal};\n"
        "history.replaceState(null, '', location.pathname);\n"
        "// The ONLY thing that ever sees TOKEN. Shell modules get this\n"
        "// closure, never the value, and a path that is not same-origin is\n"
        "// refused rather than sent.\n"
        "function api(path, options) {\n"
        "  if (typeof path !== 'string' || path.charAt(0) !== '/'\n"
        "      || path.charAt(1) === '/') {\n"
        "    return Promise.reject(new Error('builder paths must start with a "
        "single /'));\n"
        "  }\n"
        "  const opts = Object.assign({}, options || {});\n"
        f"  opts.headers = Object.assign({{}}, opts.headers, {{'{TOKEN_HEADER}': TOKEN}});\n"
        "  opts.credentials = 'omit';\n"
        "  opts.referrerPolicy = 'no-referrer';\n"
        "  return fetch(path, opts);\n"
        "}\n"
        f"const PREVIEW_CSS = {_js_string(preview_css)};\n"
        f"const LANGUAGES = {json.dumps(list(LANGUAGES))};\n"
        "const shell = {};\n"
        f"{_escape_script_end(shell_js)}\n"
        "// Entry point. Batch 12b's module assigns shell.start; until it\n"
        "// does, the skeleton stands on its own and says so.\n"
        "if (typeof shell.start === 'function') {\n"
        "  shell.start({\n"
        "    api: api,\n"
        "    previewCss: PREVIEW_CSS,\n"
        "    languages: LANGUAGES,\n"
        "    root: document.getElementById('shell-root'),\n"
        "    preview: document.getElementById('shell-preview'),\n"
        "    status: document.getElementById('shell-status'),\n"
        "  });\n"
        "}\n"
        "})();\n"
        "</script></body></html>\n"
    )


class BuilderHandler(BaseHTTPRequestHandler):
    """One request. Every check runs before any routing decision is made."""

    protocol_version = "HTTP/1.1"

    #: A single-threaded server must not be held open by a client that opened
    #: a socket and then said nothing. Measured: a port scanner doing exactly
    #: that blocked the whole server for the length of this timeout, so it is
    #: short. Single-threaded is still the right call (see the module
    #: docstring); the timeout is what makes it safe.
    timeout = 10

    # ---- logging -------------------------------------------------------

    def handle_one_request(self) -> None:
        """As the stdlib does it, but a disconnecting client is not an error.

        Without this, a browser that closes a tab mid-response (or any port
        scanner that hangs up after the request line) prints a full traceback
        to the operator's terminal, which trains them to ignore tracebacks.
        """
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def log_message(self, fmt, *args) -> None:
        """Log without the query string.

        `BaseHTTPRequestHandler` prints the whole request line, so
        `GET /?token=... HTTP/1.1` would be in every terminal transcript and
        in every screen recording of one. Stripping it here is why the
        bootstrap page can be reached with a query token at all.
        """
        scrubbed = tuple(_scrub(arg) for arg in args)
        super().log_message(fmt, *scrubbed)

    # ---- responses -----------------------------------------------------

    def _send(self, status: int, body: bytes, content_type: str, extra=()) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        # Exact length on every response, errors included: HTTP/1.1 keep-alive
        # with a wrong or missing length is how a client ends up reading the
        # next response as part of this one.
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        # No Access-Control-Allow-* header, deliberately, on any response.
        for name, value in extra:
            self.send_header(name, value)
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_json(self, status: int, payload: dict) -> None:
        # json.dumps, not src.pages.dumps: this is an API envelope, not a page
        # document. Nothing here is ever written to disk.
        body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def _send_error_payload(self, code: str, message: str, errors=None) -> None:
        status = STATUS_FOR_CODE.get(code, 400)
        # CLOSE THE CONNECTION ON EVERY ERROR. Measured, not theoretical: a
        # POST rejected by the Origin check has not had its body read, so with
        # keep-alive the unread body stays in the socket and the stdlib parses
        # the next line of the page document as a request line -- answering
        # 400 to a request the client never made, and printing the document
        # into the operator's terminal on the way. Tearing the connection down
        # discards the unread body instead.
        self.close_connection = True
        self._send_json(
            status,
            {"ok": False, "error": {"code": code, "message": message, "errors": errors or []}},
        )

    def _send_html(self, status: int, html: str, extra=()) -> None:
        self._send(status, html.encode("utf-8"), "text/html; charset=utf-8", extra)

    # ---- request checks ------------------------------------------------

    @property
    def config(self) -> BuilderConfig:
        return self.server.builder_config  # type: ignore[attr-defined]

    def _one(self, name: str) -> str | None:
        """A header's single value, or raise if it was sent more than once.

        Duplicated headers are how a request smuggles a second `Origin` past a
        check that only reads the first one.
        """
        values = self.headers.get_all(name) or []
        if len(values) > 1:
            raise ClientError("bad_request", f"{name} was sent more than once")
        return values[0].strip() if values else None

    def _check_transport(self) -> None:
        if self._one("Transfer-Encoding"):
            # The stdlib does not de-chunk, so a chunked POST would read as an
            # empty body -- and an empty body validates as an empty document.
            self.close_connection = True
            raise ClientError("bad_request", "chunked requests are not accepted")
        host = self._one("Host")
        if host is None or host.lower() not in self.config.allowed_hosts:
            # Origin and the token do not stop a DNS-rebinding GET: the
            # attacker's page resolves its own name to 127.0.0.1, so the
            # request is same-origin as far as the browser is concerned.
            raise ClientError("forbidden_host", "this service answers on loopback only")

    def _check_token(self, path: str, query: dict) -> None:
        supplied = self._one(TOKEN_HEADER)
        if supplied is None and path == "/":
            # Exactly one route accepts a query token, and this is how the
            # token reaches the browser at all. Every other route requires the
            # header, so the token never appears in a URL the browser keeps.
            supplied = _single(query, "token")
        if not supplied or not _token_matches(supplied, self.config.token):
            raise ClientError("unauthorized", "a valid session token is required")

    def _check_write_headers(self) -> None:
        origin = self._one("Origin")
        if origin is None:
            # A missing Origin is a rejection, not a pass: a cross-site form
            # post can simply omit it, and "absent" must not read as "same
            # site".
            raise ClientError("forbidden_origin", "this request needs a same-origin Origin header")
        if origin not in self.config.allowed_origins:
            raise ClientError("forbidden_origin", "this origin may not write to the builder")
        content_type = (self._one("Content-Type") or "").split(";")[0].strip().lower()
        if content_type != JSON_CONTENT_TYPE:
            raise ClientError("bad_request", "Content-Type must be application/json")

    def _read_body(self) -> bytes:
        """The body, capped, without trusting `Content-Length`.

        `Content-Length` is the client's claim. It is used to refuse early,
        and the read is capped independently so a lying length cannot make us
        buffer more than the cap either way.
        """
        raw_length = self._one("Content-Length")
        if raw_length is None:
            raise ClientError("bad_request", "Content-Length is required")
        try:
            declared = int(raw_length)
        except ValueError:
            raise ClientError("bad_request", "Content-Length is not a number") from None
        if declared < 0:
            raise ClientError("bad_request", "Content-Length is negative")
        if declared > MAX_BODY_BYTES:
            # Refuse before reading a single byte of it, and close: leaving the
            # unread body in the socket would desynchronise keep-alive.
            self.close_connection = True
            raise ClientError("payload_too_large", "the request body is too large")
        body = self.rfile.read(min(declared, MAX_BODY_BYTES))
        if len(body) < declared:
            raise ClientError("bad_request", "the request body ended early")
        return body

    def _envelope(self) -> dict:
        body = self._read_body()
        try:
            payload = json.loads(body.decode("utf-8"), parse_constant=_reject_json_constant)
        except (UnicodeDecodeError, ValueError):
            raise ClientError("bad_request", "the request body is not valid JSON") from None
        if not isinstance(payload, dict):
            raise ClientError("bad_request", "the request body must be a JSON object")
        return payload

    # ---- dispatch ------------------------------------------------------

    def _dispatch(self, method: str) -> None:
        try:
            split = urlsplit(self.path)
            # Decoded EXACTLY ONCE, and validated after decoding, so `..%2f`
            # and `%2e%2e` are seen as `../` and `..`. Decoding twice
            # anywhere would undo this.
            path = unquote(split.path)
            query = parse_qs(split.query, keep_blank_values=True)

            self._check_transport()
            self._check_token(path, query)

            if "\x00" in path:
                raise ClientError("bad_request", "the request path contains a NUL byte")

            routes = GET_ROUTES if method == "GET" else POST_ROUTES
            other = POST_ROUTES if method == "GET" else GET_ROUTES
            if path not in routes:
                if path in other:
                    raise ClientError("method_not_allowed", f"{path} does not accept {method}")
                raise ClientError("not_found", "no such route")

            if method == "POST":
                self._check_write_headers()
                self._route_post(path)
            else:
                self._route_get(path, query)
        except ClientError as exc:
            self._send_error_payload(exc.code, exc.client_message, exc.errors)
        except PathError as exc:
            self._send_error_payload(exc.code, str(exc))
        except store.StoreError as exc:
            if exc.code == "internal_error":
                self._log_internal(exc)
                self._send_error_payload("internal_error", "the builder could not complete that")
            else:
                self._send_error_payload(exc.code, str(exc))
        except Exception as exc:  # noqa: BLE001
            # Opaque to the client, detailed only in the local log. A stack
            # trace in an HTTP response is a map of this machine.
            self._log_internal(exc)
            self._send_error_payload("internal_error", "the builder could not complete that")

    def _log_internal(self, exc: BaseException) -> None:
        self.log_error("unhandled error: %s", "".join(traceback.format_exception(exc)).strip())

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802 - stdlib naming
        self._dispatch("POST")

    def _refuse_method(self) -> None:
        self.close_connection = True
        self._send_error_payload("method_not_allowed", "this method is not allowed")

    do_HEAD = _refuse_method  # noqa: N815 - stdlib naming
    do_PUT = _refuse_method  # noqa: N815
    do_DELETE = _refuse_method  # noqa: N815
    do_PATCH = _refuse_method  # noqa: N815
    do_OPTIONS = _refuse_method  # noqa: N815 - and so no CORS preflight ever succeeds
    do_TRACE = _refuse_method  # noqa: N815

    # ---- GET routes ----------------------------------------------------

    def _route_get(self, path: str, query: dict) -> None:
        if path == "/":
            # A fresh nonce per response, minted next to the header that
            # names it so the two cannot drift apart.
            nonce = secrets.token_urlsafe(CSP_NONCE_BYTES)
            self._send_html(
                200,
                bootstrap_html(self.config, nonce),
                extra=(("Content-Security-Policy", content_security_policy(nonce)),),
            )
        elif path == "/api/pages":
            self._send_json(200, {"ok": True, "pages": store.list_pages()})
        elif path == "/api/registry":
            # The block library and the inspector are driven by THIS, never by
            # a second copy of the block list inside the shell -- two lists
            # drift the first time a block type gains a prop.
            self._send_json(200, {"ok": True, "registry": registry_payload(self.config.registry)})
        elif path == "/api/versions":
            page_id = validate_page_id(_single(query, "page_id"))
            self._send_json(
                200,
                {
                    "ok": True,
                    "page_id": page_id,
                    "versions": store.list_versions(page_id),
                    "draft_versions": store.list_draft_versions(page_id),
                },
            )
        elif path == "/api/document":
            self._get_document(query)
        elif path == "/preview":
            self._get_preview(query)

    def _get_document(self, query: dict) -> None:
        page_id = validate_page_id(_single(query, "page_id"))
        which = validate_which(_single(query, "which") or "draft")
        text = store.read_document_text(page_id, which)
        if text is None:
            raise ClientError("not_found", "no such document")
        stored, migrated = _parse_document_text_with_original(text)
        self._send_json(
            200,
            {
                "ok": True,
                "page_id": page_id,
                "which": which,
                # The MIGRATED document, plus a flag, so the UI can tell the
                # user their schema_version moved rather than changing it
                # silently on the next save.
                "document": migrated,
                "migrated": migrated != stored,
            },
        )

    def _get_preview(self, query: dict) -> None:
        page_id = validate_page_id(_single(query, "page_id"))
        which = validate_which(_single(query, "which") or "draft")
        lang = _single(query, "lang") or "en"
        if lang not in LANGUAGES:
            raise ClientError("bad_request", "lang must be en, fr or nl")
        text = store.read_document_text(page_id, which)
        if text is None:
            raise ClientError("not_found", "no such document")
        doc = _parse_document_text(text)
        html = render_document(
            doc,
            registry=self.config.registry,
            lang=lang,
            # See preview_data: with data={} every bound block would say
            # "Loading..." forever.
            data=preview_data(doc),
        )
        # Served AS-IS -- a fragment, no page shell, no stylesheet link. The
        # acceptance property is that this is byte-identical to calling
        # render_document directly, and wrapping it would break exactly that.
        # Styling the preview is Batch 12's job.
        self._send_html(
            200,
            html,
            extra=(
                # This is unvalidated on-disk content rendered on the same
                # origin as the write API. Nothing in it may load, script or
                # phone anywhere.
                (
                    "Content-Security-Policy",
                    "default-src 'none'; style-src 'unsafe-inline'; sandbox",
                ),
            ),
        )

    # ---- POST routes ---------------------------------------------------

    def _route_post(self, path: str) -> None:
        payload = self._envelope()
        page_id = validate_page_id(payload.get("page_id"))
        if path == "/api/validate":
            self._post_validate(page_id, payload)
        elif path == "/api/save":
            self._post_save(page_id, payload)
        elif path == "/api/publish":
            self._post_publish(page_id)
        elif path == "/api/restore":
            self._post_restore(page_id, payload)

    def _findings(self, doc) -> list:
        return validate_document(doc, metadata=self.config.metadata, registry=self.config.registry)

    def _reject_findings(self, findings) -> None:
        if any(is_guard_error(finding) for finding in findings):
            raise ClientError("guard_rejected", "the document exceeded a resource limit")
        raise ClientError("validation_failed", "the document is not valid", _project(findings))

    def _post_validate(self, page_id: str, payload: dict) -> None:
        """Answers, and WRITES NOTHING. Not a temp file, not a lock file."""
        findings = self._findings(payload.get("document"))
        if findings:
            self._reject_findings(findings)
        self._send_json(200, {"ok": True, "page_id": page_id, "errors": []})

    def _post_save(self, page_id: str, payload: dict) -> None:
        doc = payload.get("document")
        findings = self._findings(doc)
        if findings:
            # Rejected BEFORE any filesystem call, so draft.json is byte
            # unchanged -- and if it did not exist, it still does not.
            self._reject_findings(findings)
        text = _canonical_text(doc)
        result = store.save_draft(page_id, text)
        self._send_json(
            200,
            {
                "ok": True,
                "page_id": page_id,
                "bytes": result.bytes,
                "sha256": result.sha256,
            },
        )

    def _post_publish(self, page_id: str) -> None:
        """Validate the draft on disk, then promote it atomically.

        Batch 11 does validate -> temp -> atomic replace. Gating a publish on
        the test suite passing is Batch 16 and is deliberately not folded in
        here.
        """
        text = store.read_document_text(page_id, "draft")
        if text is None:
            raise ClientError("not_found", "this page has no draft to publish")
        doc = _parse_document_text(text)
        findings = self._findings(doc)
        if findings:
            self._reject_findings(findings)
        canonical = _canonical_text(doc)
        result, previous = store.publish(page_id, canonical)
        self._send_json(
            200,
            {
                "ok": True,
                "page_id": page_id,
                # The snapshot holding the PREVIOUS published content. The
                # first publish of a page has no previous content and returns
                # null.
                "version": previous,
                "bytes": result.bytes,
                "sha256": result.sha256,
            },
        )

    def _post_restore(self, page_id: str, payload: dict) -> None:
        """Restore a published snapshot into the DRAFT (rule 32).

        Findings are returned as non-blocking `warnings`, not as a rejection:
        an old `published.json` can be invalid against today's registry, and
        refusing to restore it would make restore useless exactly when it is
        needed. Making it public again still requires an explicit publish,
        which does re-validate and does block.
        """
        version = validate_version(payload.get("version"))
        text = store.read_version_text(page_id, version)
        if text is None:
            raise ClientError("not_found", "no such version for this page")
        warnings = []
        try:
            doc = _parse_document_text(text)
        except ClientError as exc:
            # Unparseable: there is nothing to hand the editor, so this one
            # does block.
            raise ClientError(exc.code, "that snapshot could not be read", exc.errors) from exc
        warnings = _project(self._findings(doc))
        result, restored = store.restore(page_id, version)
        self._send_json(
            200,
            {
                "ok": True,
                "page_id": page_id,
                "version": restored,
                "bytes": result.bytes,
                "sha256": result.sha256,
                "warnings": warnings,
            },
        )


def _token_matches(supplied: str, expected: str) -> bool:
    """Constant-time comparison, tolerant of a non-ASCII supplied value.

    `hmac.compare_digest` raises `TypeError` on a non-ASCII str, and an
    attacker choosing the exception is an attacker choosing a 500 instead of a
    401.
    """
    try:
        return hmac.compare_digest(supplied, expected)
    except TypeError:
        return False


def _single(query: dict, name: str) -> str | None:
    """One query value, or raise if the parameter was repeated.

    `?page_id=home&page_id=../../etc` must not become a question about which
    one the parser happened to keep.
    """
    values = query.get(name)
    if not values:
        return None
    if len(values) > 1:
        raise ClientError("bad_request", f"{name} was given more than once")
    return values[0]


def _scrub(value):
    """A logged value with any query string removed."""
    if isinstance(value, str) and "?" in value:
        return value.split("?", 1)[0] + "?<redacted>"
    return value


class BuilderServer(HTTPServer):
    """`HTTPServer`, single-threaded, explicitly not `ThreadingHTTPServer`.

    See the module docstring: the recursion guard in `src/pages/serialize.py`
    needs CPython's `RecursionError`, and a worker thread's smaller C stack
    can turn that into a crash instead of a rejection.
    """

    #: A dead process must not leave a port that prints "address in use" for
    #: two minutes; this does NOT let a second builder steal a live port.
    allow_reuse_address = True

    def __init__(self, config: BuilderConfig):
        self.builder_config = config
        super().__init__((resolve_bind_host(config.host), config.port), BuilderHandler)


def make_server(config: BuilderConfig) -> HTTPServer:
    """Bind the loopback listener for `config`.

    Raises `OSError` if the port is taken. There is deliberately NO fallback to
    another port: a fallback prints a working URL while a stale tab still
    holds a token for a dead process.
    """
    return BuilderServer(config)
