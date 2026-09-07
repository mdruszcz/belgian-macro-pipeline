# Batch 11 — builder service: SPECIFICATION (rev 1)

Written before implementation, per the programme's own workflow. Routed to
`repository-architect` for a dependency/regression pre-check before any code is written.

Base / rollback point: `57ebf9b5` (Batch 10, #108) on `origin/develop`. Branch
`feat/builder-service`.

## Objective

A local-only HTTP service that lets the builder UI (Batch 12) validate, save, publish and
restore page documents, with a preview rendered by the *same* function the public build
calls. The service is the first component in this programme with filesystem write access
and a network listener, so its acceptance gate is a real adversarial pass, not a code read.

## Included work

1. `scripts/serve_builder.py` — CLI entry point, behind `make builder`. Binds loopback only,
   mints a per-process session token, prints the one URL that works, serves until Ctrl-C.
2. `src/builder/` — the service logic as importable modules so tests can exercise the
   request-handling code path without a live socket:
   - `paths.py` — page-id validation and the path allowlist (the single place any filesystem
     path is derived).
   - `store.py` — atomic writes, version snapshots, draft/published/restore file operations.
   - `service.py` — the `BaseHTTPRequestHandler` subclass: token check, Origin check,
     body limits, routing, JSON responses.
3. The API surface below: `validate`, `save`, `publish`, `restore` (plus the read-only
   helpers the UI needs to be usable at all: list pages, load a document, list versions,
   preview).
4. `config/pages/` layout decided and documented (see "Path-allowlist decision").
5. `make builder` target.
6. Tests (see "Tests").

## Excluded work

- **Any builder UI.** No panels, no inspector, no canvas, no drag. Batch 12/13. The service
  serves a deliberately minimal bootstrap page whose only job is to hold the token and prove
  the API is reachable.
- **Binding resolution.** `render_document` is handed `data={}`; turning a binding into a
  number is Batch 14. A preview therefore shows blocks in their honest empty states, which is
  correct, not a defect.
- **The full publication transaction with test-gating.** Batch 16 adds "runs required tests
  before promoting". Batch 11 does the validate → temp → atomic-replace part only, and says
  so in the report.
- **Wiring any published route to `config/pages/`.** Nothing in the static export reads these
  files yet (Batch 15). This batch cannot change a single published byte, by construction.
- **Any change to `src/pages/`.** The validator and renderer are consumed exactly as merged.
  If the red-team pass finds a hole *in* `src/pages/`, it is recorded as a finding and, if
  P0/P1, fixed in its own follow-up with its own spec — not quietly patched inside this batch.
- **Any new dependency.** stdlib `http.server` only. `pyproject.toml` and `requirements.txt`
  are not in the allowed-files list.

## Allowed files

Implementation (`builder-core`):

```
src/builder/__init__.py          (new)
src/builder/paths.py             (new)
src/builder/store.py             (new)
src/builder/service.py           (new)
scripts/serve_builder.py         (new)
Makefile                         (ADD a `builder` target and its `## builder:` help line; change nothing else)
config/pages/README.md           (new -- the layout decision, committed next to the data)
```

Tests (`test-engineer`):

```
tests/builder/__init__.py                        (new, if needed for imports)
tests/builder/test_builder_paths.py              (new)
tests/builder/test_builder_api.py                (new)
tests/builder/test_builder_transaction.py        (new)
tests/security/test_builder_service_hardening.py (new)
tests/fixtures/                                  (ADD a page-document fixture only; change no existing fixture)
```

Lead only (`belpulse-lead`): `docs/implementation/batches/batch-11-*.md`, `docs/steps`,
`docs/features/page_builder.md`, `docs/implementation/known-risks.md`.

Explicitly out of scope for every agent on this batch: `src/pages/**`, `src/fetchers/**`,
`src/analytics/**`, `resolve_geo()`, `migrations/**`, `scripts/export_*.py`, the database
schema, `assets/**`, `pyproject.toml`, `requirements*.txt`, any existing test file. If you
need one of these, stop and say so.

## Path-allowlist decision

`config/pages/` **already exists and is empty** (no `.gitkeep`, no README). It is the
directory `docs/features/page_builder.md` and claude.md rule 22 already name, so no new
location is invented. This batch fixes its layout:

```
config/pages/
  README.md                       # the layout contract, committed
  {page_id}/
    draft.json                    # the builder writes ONLY this (rule 32)
    published.json                # what the static export will read (Batch 15)
    versions/
      published-0001.json         # snapshot of published.json taken BEFORE each successful replace
      published-0002.json         # monotonic, zero-padded, never reused, never deleted by the service
```

Allowlist rules, enforced in `paths.py` and nowhere else:

- `page_id` must match `^[a-z0-9][a-z0-9-]{0,63}$`. No dot, no slash, no backslash, no NUL,
  no percent, no unicode. Traversal is impossible *by construction* before any path is built.
- `version` must match `^[0-9]{4,}$`.
- The only filenames the service may ever open for writing are `draft.json`,
  `published.json`, `versions/published-NNNN.json`, and a `.tmp-*` sibling of one of those.
- Every resolved path is then checked with `os.path.realpath` for containment inside the
  realpath of `config/pages/` — defence in depth, so a symlink planted inside
  `config/pages/{page_id}/` cannot redirect a write outside the tree.
- The root is derived from the repository root, never from a request field. No request may
  set, override or hint at the root.

## API surface

Transport: HTTP/1.1 on `127.0.0.1:{port}` (default 8787, `--port` overridable). No TLS —
loopback only. No CORS headers are ever emitted.

Every request, without exception, must carry the session token as
`X-BelPulse-Token: <token>`; the bootstrap page may instead pass `?token=` on `GET /`, which
is how the token reaches the browser at all. Comparison uses `hmac.compare_digest`.

Every `POST` must additionally carry `Origin` exactly equal to `http://127.0.0.1:{port}` or
`http://localhost:{port}`, and `Content-Type: application/json`. A missing `Origin` on a POST
is a rejection, not a pass. Request bodies are capped at 1 MiB (matching `src/pages/schema.py`'s
own raw-size guard) and read to that cap, never trusting `Content-Length`.

| Method | Route | Body | Success | Writes |
|---|---|---|---|---|
| GET | `/` | — | `200 text/html`, bootstrap page holding the token | none |
| GET | `/api/pages` | — | `{"pages":[{"page_id","has_draft","has_published","versions":n}]}` | none |
| GET | `/api/document?page_id=&which=draft\|published` | — | `{"page_id","which","document":{...}}` | none |
| GET | `/api/versions?page_id=` | — | `{"page_id","versions":["0001",...]}` | none |
| GET | `/preview?page_id=&which=draft\|published&lang=en\|fr\|nl` | — | `200 text/html` from `render_document` | none |
| POST | `/api/validate` | `{"page_id","document"}` | `{"ok":true,"errors":[]}` | **none, ever** |
| POST | `/api/save` | `{"page_id","document"}` | `{"ok":true,"page_id","bytes","sha256"}` | `draft.json` |
| POST | `/api/publish` | `{"page_id"}` | `{"ok":true,"page_id","version","bytes","sha256"}` | `versions/published-NNNN.json` then `published.json` |
| POST | `/api/restore` | `{"page_id","version"}` | `{"ok":true,"page_id","version","bytes","sha256"}` | `draft.json` only |

Failure shape, uniform: `{"ok":false,"error":{"code","message","errors":[...]}}` where `errors`
is the `validate_document` finding list projected to `{code,path,message}`.

Semantics that are non-negotiable:

- **`validate` never writes.** Not a temp file, not a lock file, nothing.
- **`save` validates first.** A document with any finding is rejected `422` and `draft.json`
  is left exactly as it was — including the case where no `draft.json` existed yet, which must
  still not exist afterwards.
- **`publish` reads the draft from disk**, runs the real `src/pages.validate_document()`, and
  on any finding returns `422` having touched nothing. On success it snapshots the current
  `published.json` into `versions/`, writes the new content to a temp file in the same
  directory, `fsync`s it, and `os.replace`s it over `published.json`. A crash at any point
  leaves either the old file or the new file, never a partial one.
- **`restore` restores into the DRAFT, not into published.** Bringing back an old version is
  an edit; making it public again requires an explicit `publish`. This is rule 32 read
  literally.
- **The service never invokes git.** No commit, no push, no tag (rule 34). No `subprocess`
  import at all in `src/builder/`, which makes `shell=True` unreachable rather than merely
  absent, and is testable as an import-level assertion.
- **No secrets to the browser.** Nothing from the environment, no absolute filesystem paths,
  no repository paths, no `Server:` banner detail beyond the stdlib default, no stack traces.
  A 500 returns an opaque message and logs locally. The session token is a per-process
  capability minted by `secrets.token_urlsafe(32)`, never written to disk and never read from
  configuration — it is the browser's authorisation, not a repository credential, and that
  distinction is the reading of "no credentials sent to the browser" this batch adopts.
- **Guard findings are answered without echoing detail** using `src/pages.is_guard_error` —
  the hook Batch 9 left for exactly this caller.

## Required states (response states, all of which must be reachable and tested)

`ok` · `validation_failed` (422) · `guard_rejected` (413, no detail) · `unauthorized` (401,
absent or wrong token) · `forbidden_origin` (403) · `bad_request` (400, malformed JSON / bad
page_id / bad `which`) · `not_found` (404, unknown page or version, and unknown route) ·
`payload_too_large` (413) · `method_not_allowed` (405).

## Tests (written before or alongside implementation, by `test-engineer`)

Every item below must be a test that *proves the rejection*, not a test that observes the
guard exists. Batch 10's protocol-relative-URL hole passed a guard that read correctly.

1. Path allowlist rejects traversal: `../`, `..%2f`, `%2e%2e/`, absolute `/etc/passwd`,
   `a/b`, `a\b`, a NUL byte, a leading `.`, a 64+ char id, an empty id — and a *symlink*
   planted inside `config/pages/{page_id}/` pointing outside the tree.
2. Token check: absent token → 401; wrong token → 401; correct-prefix-but-truncated token →
   401; token on the query string for an API route → 401 (only `GET /` accepts it there).
3. Origin check: absent `Origin` on a POST → 403; `http://evil.example` → 403;
   `http://127.0.0.1:{other_port}` → 403; both allowed origins → pass.
4. A failed `validate` blocks `save`: an invalid document is 422 and `draft.json` is
   unchanged, byte for byte; and if it did not exist, it still does not exist.
5. A failed `publish` leaves `published.json` **byte-identical** to before the attempt
   (sha256 compared), and creates no stray version snapshot and no leftover temp file.
6. Atomic write under a simulated crash mid-write: monkeypatch the write so it raises after
   the temp file is created but before `os.replace`; assert the destination is untouched and
   no partial file is visible; assert `os.replace` is what performs the swap.
7. No `shell=True` anywhere, and no `subprocess` import in `src/builder/` or
   `scripts/serve_builder.py`; no `git` invocation.
8. The preview route produces **byte-identical** HTML to `src/pages.render_document(doc,
   registry=..., lang=...)` called directly for the same document — the property Batch 10
   exists to guarantee.
9. Bind address: the server binds `127.0.0.1`; `--host 0.0.0.0` (and any non-loopback value)
   is refused rather than honoured.
10. Body cap: a 2 MiB body is refused without being fully buffered into a parsed document.

## Performance limits

- `validate` on a 30-block document: < 150 ms server-side (Batch 9 measured the validator
  itself at 12.6 ms against a 50 ms budget; the remainder is HTTP and JSON overhead).
- `save`: < 200 ms including fsync.
- `preview`: < 300 ms.
- Server startup to first served request: < 1 s.
- Zero new dependencies; `pyproject.toml` byte-identical.

## Audits required before acceptance

- `security-red-team`, attacking the running service **and** `src/pages/` — the latter is the
  deferred Batch 9 review that `docs/implementation/known-risks.md` (row 2026-09-07) makes a
  hard gate on Batch 11 acceptance. Findings are recorded in the batch report even if there
  are none; "no findings recorded" is not "clean".
- Full offline suite (`python3 -m pytest -q`), baseline 999 passing.
- `pre-commit run --files` on every changed file.

No visual or data audit applies: this batch renders no figure and reads no payload.

## Rollback procedure

`git revert` the batch commit, or delete `src/builder/`, `scripts/serve_builder.py`,
`tests/builder/`, `tests/security/test_builder_service_hardening.py`, `config/pages/README.md`
and the `builder` Makefile target. Nothing else in the repository imports any of them, no
published byte changes, and `make all` does not invoke the target. Rollback point `57ebf9b5`.

## Next batch

12 (builder shell) consumes this API. 16 (publication transaction) extends `publish` with
test-gating and is deliberately not folded in here.

---

# Rev 2 — changes required by the pre-implementation architecture check

`repository-architect` checked rev 1 against the real merged code and returned 30 findings,
5 of them P1. Rev 1 is kept above deliberately: every item below is something that would
have shipped wrong. **Where rev 2 contradicts rev 1, rev 2 wins.**

## P1 corrections (rev 1 was wrong)

1. **The 1 MiB body cap did not match the validator's own cap, and `save` could have written
   a draft the loader would later refuse.** `MAX_DOCUMENT_BYTES = 1_048_576`
   (`src/pages/schema.py:46`) caps the *document text*, and is enforced only inside `loads()`
   (`src/pages/serialize.py:70`) — `validate_document` never checks raw size
   (`src/pages/document.py:54`). So a legal ~1 MiB document inside a `{"page_id","document"}`
   envelope would have been wrongly 413'd, and a compact body under 1 MiB can exceed 1 MiB
   after `dumps()` adds `indent=2`, writing a `draft.json` that `load_document` then rejects.
   **Required:** the body cap is `MAX_DOCUMENT_BYTES` imported from `src.pages.schema`
   (a deliberate deep import — the constant is not in `__all__`; note that in a comment, never
   re-type the number) plus 64 KiB of envelope headroom; and `save` and `publish` must run
   `check_raw_size(dumps(doc))` on the *canonical text they are about to write* and refuse
   before writing. `document_too_large` can only ever come from `loads()`/`check_raw_size`,
   never from `validate_document`, so the 413 path must be wired to both call sites.
2. **`/preview` was unreachable under rev 1's own auth rule.** An `<iframe src="/preview?…">`
   cannot set a header, and rev 1 forbids `?token=` on anything but `GET /`. **Required:** the
   bootstrap page `fetch()`es `/preview` with `X-BelPulse-Token` and injects the result into a
   sandboxed iframe via `srcdoc`. Exactly one route accepts a query token, still `GET /`.
3. **A same-origin preview would turn any renderer escaping slip into token theft plus
   arbitrary writes under `config/pages/`.** The bootstrap page, the write API and the
   rendered *unvalidated on-disk draft* would share `http://127.0.0.1:8787`. **Required:**
   `/preview` responds with `Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'`,
   `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, and the iframe is
   `sandbox` without `allow-same-origin` and without `allow-scripts`. The token exists only in
   the parent document.
4. **The token would have leaked into the terminal log and the browser's history.**
   `BaseHTTPRequestHandler.log_message` prints the whole request line, so
   `GET /?token=… HTTP/1.1` would be in every transcript. **Required:** override
   `log_message` to strip the query string before logging, and the bootstrap page calls
   `history.replaceState` to drop the token from the URL on load.
5. **`publish`'s snapshot ordering contradicted its own test 5.** Rev 1 said snapshot then
   replace, while test 5 asserts a failed publish leaves no stray snapshot — impossible if the
   replace fails after the snapshot. **Required ordering, exactly:** validate the draft →
   read the current `published.json` bytes → write the *new* content to `.tmp-publish-*` in the
   same directory and `fsync` → write the *old* bytes to `.tmp-snapshot-*` and `fsync` →
   `os.replace` the new content over `published.json` → `os.link`/`os.replace` the snapshot into
   `versions/published-NNNN.json` claimed with `O_EXCL` → unlink both temps. Any failure before
   the first `os.replace` unlinks both temps and leaves everything untouched. Version numbers
   are exactly four digits (`^[0-9]{4}$`, so lexical and numeric order agree — rev 1's `{4,}`
   would have sorted `10000` before `9999`); exhausting 9999 is a loud refusal, not a wrap.
   The `version` field in the response is the number of the snapshot holding the **previous**
   published content; the **first** publish of a page has no previous content, writes no
   snapshot, and returns `"version": null`.

## P2 decisions (rev 1 was silent; these are now settled)

6. `load_metadata()` (~6.4 ms, four files) and `load_registry()` (~0.3 ms) are loaded **once at
   server startup**, never per request — `src/pages/metadata.py:60-61` already says "once per
   validator instance". A `MetadataError`/`RegistryError` at startup is a fatal exit naming the
   missing path, not a repeating opaque 500.
7. `make clean` deletes `public/data/national.json`, which the startup metadata load needs.
   The `## builder:` help line and `config/pages/README.md` both say: run `make exports` first
   if you have cleaned. A fresh clone needs nothing — all four inputs are git-tracked.
8. **Committed drafts are publicly fetchable.** GitHub Pages serves the repository root, so a
   committed `config/pages/{id}/draft.json` is downloadable at the public URL. Invariant 10
   still holds literally (nothing renders it), but unpublished copy being world-readable is a
   surprise. Recorded in `config/pages/README.md` and as a new row in
   `docs/implementation/known-risks.md`.
9. **`HTTPServer`, single-threaded, explicitly not `ThreadingHTTPServer`.** The deep-nesting
   guard depends on CPython raising `RecursionError` (`src/pages/serialize.py:76-84`), which on
   a worker thread with a smaller C stack can become a hard crash instead — the guard would stop
   guarding. Single-threaded also removes the version-number race. `protocol_version =
   "HTTP/1.1"` with an exact `Content-Length` on every response including errors; a request
   carrying `Transfer-Encoding` is refused (the stdlib does not de-chunk, so a chunked POST
   reads as an empty body and would validate an empty document); an over-cap body closes the
   connection rather than being read past `Content-Length`.
10. A `Host` header allowlist (`127.0.0.1:{port}` / `localhost:{port}`) is added to every
    request, GET included — Origin and token do not stop a DNS-rebinding GET. New required
    state: `forbidden_host` (403).
11. `restore` **snapshots the current draft first** to `versions/draft-NNNN.json` before
    overwriting it, so an accidental restore is recoverable; rev 1 would have destroyed
    in-progress work irreversibly. It re-validates the restored content and returns any findings
    in a non-blocking `warnings` array — an old `published.json` can be invalid against today's
    registry, and refusing to restore it would make restore useless exactly when it is needed.
12. `GET /api/document` returns the **migrated** document (what `load_document` yields) plus a
    `"migrated": true|false` flag, so the UI can tell the user their `schema_version` moved
    rather than changing it silently on the next save.
13. **Rev 1's "honest empty states" claim was wrong.** With `data={}`, `_state_for` returns
    `"loading"` for any block carrying a binding (`src/pages/render.py:361-362`) — a preview
    that says "Loading…" forever for something that will never load, which is precisely the
    state confusion rule 26 exists to prevent. **Required:** `src/builder` exposes
    `preview_data(doc) -> dict` returning `{block_id: {"state": "unavailable"}}` for every block
    with a binding, and `/preview` passes it. This is not binding resolution — it resolves
    nothing and reads no payload. Test 8 must call `render_document` with the same
    `preview_data(doc)` map, or byte-identity fails for the wrong reason.
14. `/preview` serves `render_document`'s output **as-is**, a fragment with no `<html>`,
    `<head>` or stylesheet link. Wrapping it in a page shell is incompatible with test 8's
    byte-identity requirement, and styling the preview is Batch 12's job. Recorded as a known
    limitation: the Batch 11 preview is structurally correct and visually unstyled.
15. Port already occupied → fail loudly with the port in the message. **Never fall back to
    another port**: a fallback prints a working URL while a stale tab still holds a token for a
    dead process.
16. `bytes` and `sha256` in every response mean "of the file bytes actually written" — without
    that definition tests 4-6 assert nothing.

## Allowed-files corrections

17. **No new page-document fixture.** `tests/fixtures/pages/builders.py` already provides
    `minimal_valid_document`, `realistic_multi_section_document`, `make_block`, `make_section`,
    `municipal_binding`, `national_binding`, `deep_clone`, and `tests/fixtures/pages/real_data.py`
    reads every indicator id and NIS code from the real payloads. A hand-written fixture is the
    most likely way this batch breaks rule 36 / invariant 3. `tests/fixtures/**` is
    **read-only for every agent on this batch**; import from it, add nothing.
18. **No `tests/builder/__init__.py`.** Neither `tests/pages/` nor `tests/security/` has one and
    there is no `conftest.py`; collection works on unique basenames, and the four filenames are
    unique. Adding one breaks the convention for no gain.
19. `Makefile` needs the new target added to `.PHONY` (line 25) as well as a `## builder:` help
    line. `Makefile` has **one owner this batch: `builder-core`.**
20. The bootstrap HTML is **generated from Python inside `src/builder/`**. Not `builder/app/`
    (that path is ADR 0004's location for the Batch 12 *UI*), not `assets/`, and above all not
    the repository root — anything at the root is published by GitHub Pages.
21. `scripts/serve_builder.py` copies the existing script idiom:
    `sys.path.insert(0, str(Path(__file__).resolve().parents[1]))` before importing `src.*`, with
    `# noqa: E402` (as `scripts/validate_config.py:9-16` does). `make` runs scripts as
    `$(PYTHON) scripts/x.py` and cwd is not on `sys.path`.
22. The "no `subprocess`" assertion must be **path-scoped** to `src/builder/` and
    `scripts/serve_builder.py`. `scripts/export_local_pages.py:61` imports `subprocess`
    legitimately, so a repo-wide assertion would fail on existing code.

## Confirmed clear by the pre-check (no action needed)

- **`config/pages/` has no existing reader.** `scripts/validate_config.py` and
  `src/validation/config_schema.py` glob `*.yaml` under `config/indicators`, `config/sources`,
  `config/indicators/derived` only. No test globs `config/**/*.json`. No exporter, Makefile
  target, `.gitignore` rule or CI step touches it. JSON files appearing there break nothing.
- **`src/builder/` is packaging-invisible.** `py-modules` never installs `src/`; imports work via
  pytest's `pythonpath = ["."]` plus `src/__init__.py`. `pyproject.toml` stays byte-identical.
- **No new ADR required.** ADR 0004 already authorises this verbatim: "A `builder/` app served
  locally (via `make builder`…) edits `config/pages/{page_id}/draft.json` through a small local
  API (`scripts/serve_builder.py`). Publishing validates a draft and, only on success, atomically
  replaces `config/pages/{page_id}/published.json`"
  (`docs/decisions/0004-repository-native-website-builder.md:17-22`), plus "No automatic publish,
  commit or push from the browser" at :47-48. Rule 19 is not engaged — no schema, adapter,
  geography resolution or analytical formula is touched.
- **Invariant 1 holds.** Its `git grep` scope is `assets/`, `local/` and the page-root HTML files;
  `src/builder/` and `scripts/` are outside all three. The only existing loopback code is
  `scripts/capture_baseline_screenshots.py` on port 8912 — no clash with 8787.
- **Baseline regression risk is low.** No test greps the whole repo; the one source-scanning test
  (`tests/pages/test_page_document_render.py:295-304`) reads `src/pages/render.py` by name.
  `pre-commit` runs only trailing-whitespace, large-files, ruff (`E,W,F,I,B,UP,C4` — no bandit)
  and black, so `http.server` draws no lint objection.

## Lead-owned documentation debt this batch also closes

ADR 0004 (`:21`) and `docs/features/page_builder.md` (`:46-49`) both still say the shared
renderer is `assets/belpulse/renderer.js`. That file does not exist — Batch 10 shipped Python
`src/pages/render.py` and recorded the reversal in its own module docstring. Both get a
"superseded in part by Batch 10" correction in this batch, since a reader of the architecture
paragraph is currently pointed at a file that was never written.
