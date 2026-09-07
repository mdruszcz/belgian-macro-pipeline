# `config/pages/` — the page documents the builder reads and writes

This directory holds the page-document files described in
`docs/features/block_contract.md` and `docs/features/page_builder.md`. Layout fixed by
Batch 11 and enforced in `src/builder/paths.py`, which is the only place in the codebase
that turns a page id into a filesystem path:

```
config/pages/
  README.md                       this file
  {page_id}/
    draft.json                    the builder writes ONLY this while you edit
    published.json                what the static export will read (Batch 15)
    versions/
      published-0001.json         the PREVIOUS published.json, snapshotted at publish time
      published-0002.json         monotonic, zero-padded, never reused, never deleted here
      draft-0001.json             the previous draft.json, snapshotted before a restore
```

## The rules these filenames encode

- `page_id` must match `^[a-z0-9][a-z0-9-]{0,63}$`. No dot, no slash, no backslash, no
  percent, no NUL, no uppercase, no unicode. A page id that cannot express a traversal
  cannot be made to perform one.
- A version is exactly four digits, `^[0-9]{4}$`, so sorting the filenames as text gives
  the same order as sorting them as numbers. Running out at `9999` is a loud refusal, not
  a wrap onto `0001` — a wrap would overwrite a snapshot.
- Saving edits `draft.json` and nothing else. Publishing is a separate, explicit step
  (claude.md rule 32); a draft has no effect on what a visitor sees.
- Publishing snapshots the outgoing `published.json` into `versions/` and replaces
  `published.json` atomically. A publish that fails leaves the previous published file
  byte-identical and leaves no snapshot behind (rule 33).
- Restoring a version writes `draft.json`. It never writes `published.json`: bringing an
  old version back is an edit, and making it public again needs another publish.
- The builder writes files. It never commits, pushes or tags (rule 34) — `git status` after
  a publish is yours to review and commit yourself.

## Two things worth knowing before you commit a draft

1. **A committed draft is publicly downloadable.** GitHub Pages serves this repository's
   root, so once `config/pages/{page_id}/draft.json` is committed and pushed, anyone can
   fetch it at the public URL. Nothing *renders* it — the static site reads only
   `published.json` — but "a draft does not affect the public site" is not the same as "a
   draft is private". Tracked in `docs/implementation/known-risks.md`.
2. **The service needs the published payloads at startup.** It loads
   `public/data/metadata/indicators.json`, `public/data/national.json`,
   `public/data/metadata/geographies.json` and `config/geography/municipality_crosswalk.csv`
   once, to check data bindings against real indicator ids. `make clean` deletes
   `public/data/national.json`, so run `make exports` first if you have cleaned. A fresh
   clone needs nothing: all four inputs are tracked in git.

## Running the builder

```
make builder          # http://127.0.0.1:8787/?token=... printed on startup
```

Loopback only, one token per process, and the token is never written to disk. Batch 11
serves the API plus a deliberately minimal bootstrap page; the builder interface itself is
Batch 12.
