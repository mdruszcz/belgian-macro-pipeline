# Batch 11 — builder service

```
Batch: 11 (docs/features/page_builder.md; spec docs/implementation/batches/batch-11-builder-service-spec.md)
Base commit: 57ebf9b5 (Batch 10: shared block renderer, #108)
Final commit: <set at PR merge>
Files changed:
  src/builder/__init__.py, paths.py, store.py, service.py   (new)
  scripts/serve_builder.py                                  (new)
  config/pages/README.md                                    (new -- the layout contract)
  Makefile                                                  (`builder` target added to .PHONY and help)
  src/pages/serialize.py                                    (loads() now converts a bare ValueError too)
  tests/builder/test_builder_{paths,api,transaction}.py     (new, 126 tests)
  tests/security/test_builder_service_hardening.py          (new)
  docs/decisions/0004-repository-native-website-builder.md  (renderer.js correction)
  docs/features/page_builder.md                             (renderer.js correction; batch table)
  docs/implementation/known-risks.md                        (2 new rows)
  docs/implementation/batches/batch-11-builder-service-spec.md (new -- the pre-implementation spec)
  docs/implementation/batches/batch-11-builder-service.md   (this file)
Requirements completed:
  - validate / save / publish / restore over HTTP, `scripts/serve_builder.py`
    behind `make builder`, loopback-only, session-token + exact-Origin +
    exact-Host checks on every request.
  - Every filesystem path allowlisted under config/pages/{page_id}/, with
    id/which/version each validated by a closed regex BEFORE touching a
    path, and a realpath-containment check as a second, independent layer.
  - Atomic writes throughout: temp-in-same-dir + fsync + os.replace + dir
    fsync. A simulated SIGKILL mid-publish leaves published.json untouched.
  - No shell=True, no subprocess, no sqlite3 anywhere in src/builder/ --
    confirmed by import-set inspection, not by grep.
  - No git commit or push from the service (rule 34); no credential or
    secret ever appears in a response body.
  - /preview renders through src.pages.render_document -- the same function
    the public build calls -- confirmed byte-identical against a direct
    call for the same document.
Deferred requirements (per the spec's own exclusions):
  - No builder UI (Batch 12). No binding resolution (Batch 14) -- the
    preview is handed {} / an "unavailable" stand-in, never real data.
  - No test-gating on publish (Batch 16). No wiring of config/pages/ into
    the static export (Batch 15).
Data-contract impact: none. No exporter, schema or indicator config touched.
  config/pages/ has no reader anywhere else in the repository -- verified
  before implementation started, not assumed.
Commands executed:
  python3 -m pytest -q                              (1128 passed, from 999)
  pre-commit run --all-files                        (all pass; one unrelated
                                                      pre-existing whitespace
                                                      fix to shared-styles.css
                                                      reverted, not this batch's)
  A full adversarial pass against two live instances of the actual server
  (real sockets, real curl, a filesystem-mutation audit hook) -- see below.
Tests passed: 1128/1128. Zero pre-existing tests weakened.
```

## Why this batch went through the full review chain

Every other visual-reproduction batch in this programme has been built and
checked directly. This one is different in kind: it is the first thing in
the whole redesign that opens a network socket and writes to the filesystem
on request. `docs/implementation/known-risks.md`'s row from Batch 9 makes an
independent security-red-team pass against `src/pages/` a **hard
requirement of this batch's acceptance**, specifically because Batch 9's own
review was deferred until there was a real attack surface to test. This
batch is that surface, so it went through spec → architecture pre-check →
implementation → tests → adversarial review, in that order, before
anything was accepted.

## The pre-implementation architecture pre-check found 5 P1s before any code existed

Caught in the spec's own revision 1, before `src/builder/` had a single
line: the request-body cap didn't match `MAX_DOCUMENT_BYTES` (so `save`
could file a draft `load_document` would later refuse), `/preview` was
**unreachable under its own auth rule** (an `<iframe src=...>` cannot set a
header), the publish ordering in the draft spec contradicted the draft's own
acceptance test, the token would have leaked into the terminal access log,
and a same-origin preview of an *unvalidated* draft would have turned any
future renderer-escaping slip into token theft plus arbitrary writes. All
five are closed in the merged spec; see revision 2 there for the exact
wording.

## The security-red-team pass: what it found, and what it didn't

**Method, not description**: two real `HTTPServer` instances on loopback
ports, driven with raw sockets and curl, with a `sys.addaudithook` recording
every process spawn and every filesystem mutation for the full run.
Everything below was a request sent and a response read, not code reasoned
about.

**One P1, and it blocked acceptance until fixed.** A *failed* `publish` — in
particular the store's own documented `version_space_exhausted` refusal,
reachable by 9999 ordinary publishes of one page, no filesystem sabotage
required — could already have replaced `published.json` while the HTTP
caller was told the publish had failed. The first version of the
transaction claimed the version snapshot *after* `os.replace`; a failure at
the claim step therefore fired after the point of no return. **Fixed by
reordering `src/builder/store.py:publish()` to claim the snapshot slot
before the replace**, so every failure through and including a full
`versions/` directory now happens before `published.json` is touched. The
one residual case — `os.replace` itself failing *after* the snapshot is
already claimed — is accepted and documented in the function's own
docstring: `published.json` still holds the old content in that case (the
replace that would have changed it didn't), so the snapshot correctly
describes what is still live. No filesystem offers a rename that is atomic
across two files at once, so this is where the guarantee actually ends.

**Six P2/P3 findings landed in `src/pages/`, not `src/builder/`,** and are
recorded rather than patched inside this batch, per the spec's own
exclusion list (each needs its own follow-up):

- `loads()` raised a bare `ValueError` — not the module's own
  `PageValidationError` — on an integer literal past CPython's digit-count
  guard. **Fixed in this batch anyway**, in `src/pages/serialize.py`,
  because it is the one function `docs/features/site_payloads.md`-adjacent
  code calls to make parsing total, and it was two lines to close at the
  root rather than patch at each of four call sites. Also closed a second,
  related bug it caused: `/api/document` called `loads()` a second, unguarded
  time to compute a migration-changed flag, so it 500'd on every malformed
  on-disk document instead of the 400/413/422 `/preview` already gave the
  same content. Refactored into one guarded parse, reused for both purposes.
- Four more (a non-mapping `visibility`/`layout` escaping per-block
  isolation in `render.py`, control/bidi characters passing content-safety
  unescaped by `html.escape`, a float heading level matching the registry's
  integer enum by JSON Schema's numeric-equality rule, a registry-declared
  `link` rich_text node silently unrendered) are real and are none of them
  reachable through a *validated* document — only through `/preview`'s
  by-design rendering of an unvalidated draft. Left open, each scoped to its
  own future spec.

**Everything else attacked and found solid**, in brief (the full report has
exact payloads for each): the path allowlist rejected every traversal,
homoglyph, NUL-byte, encoding and non-string shape thrown at it including
through six planted symlinks; the token check is `hmac.compare_digest` with
no length short-circuit; the Origin check is exact set membership, not the
prefix-match shape that bit Batch 10 (`http://127.0.0.1:{port}.evil.example`
is correctly refused); the Host check defeats DNS rebinding; no browser
cross-origin request shape (form-encoded, multipart, `text/plain`, chunked)
reaches a write; the preview iframe is a bare `sandbox` with no
`allow-same-origin` and no `allow-scripts`, filled via `srcdoc`, and a
hand-crafted document with `<script>`, `<svg onload>`, `javascript:` and
`//evil.example` hrefs in every field came out fully escaped with both
hostile hrefs collapsed to `#`; the process never crashed under any
resource-guard payload thrown at it; determinism holds (twenty renders of
one document, one SHA-256); and the trust boundary claim from Batch 9 — that
`src/pages/` never imports `sqlite3` or touches geography/fetcher code — was
confirmed live, not just by import scan.

**The known-risks row is partially, not fully, discharged.** The red-team's
own assessment: `src/pages/`'s validator and renderer have now had a real
adversarial pass through a live HTTP surface, and the row can be marked
closed for *that* surface. It stays open, narrowed, for Batch 14 — binding
resolution is entirely unexercised by this batch, since `/preview` is only
ever handed `{}` or an "unavailable" stand-in, never real resolved data, and
whether the five states in rule 26 stay distinct once a real resolver
supplies them is untested until one exists.

## Three bugs the implementer's own live run found, none in any spec item

A POST rejected before its body was read desynchronised keep-alive: the
unread request body (a full page-document JSON payload) was parsed as the
next request line and printed into the operator's terminal log. Fixed by
ending the connection on every error response. A client disconnecting
mid-response printed a full traceback (triggered by an ordinary port
scanner in this environment); now caught quietly. A 30-second per-connection
timeout let one silent socket stall the single-threaded server for the
whole 30 seconds; now 10.

## Known limitations

- Nothing enforces that a saved document's own `page_id` field matches the
  directory it is filed under. Two envelope fields imply they may
  legitimately differ; Batch 15 reads `published.json` by directory, and a
  mismatch there will look like a bug rather than an accepted design gap.
  Flagged for a decision before that batch starts.
- A concurrent `save` from two open tabs silently overwrites with no
  conflict detection (the document carries a `revision` field the service
  never reads). `restore` snapshots the current draft before overwriting it
  for exactly this reason; `save` does not. Not required by the spec,
  recorded as a real gap.
- A committed draft or published file is publicly fetchable, because
  GitHub Pages serves the whole repository root. `invariant 10` ("no effect
  on the public site") still holds literally; "not affecting the site" is
  not the same claim as "private". New known-risks row; the maintainer's
  call, not this batch's to decide.
Rollback procedure: delete src/builder/, scripts/serve_builder.py,
  config/pages/README.md, revert the 9-line Makefile addition, revert the
  loads() change in serialize.py (independently safe to keep -- it only
  makes an existing function more total). Nothing else imports any of it.
Next batch: 12 (builder shell) is the first thing that actually consumes
  this API from a browser UI.
```
