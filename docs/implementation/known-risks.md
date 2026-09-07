# Known risks — redesign and page-builder work

Severity levels (from the maintainer's plan, unchanged):

- **P0** — data corruption, a security issue, a broken deployment, or an unusable route. Blocks
  immediately.
- **P1** — a wrong value, a missing required behaviour, or a major visual/accessibility failure.
  Blocks merge.
- **P2** — a minor defect or maintainability issue. Fix or explicitly document.
- **P3** — an optional improvement. Backlog.

## Open risks, tracked as they're found

| Date | Risk | Severity | Status |
|---|---|---|---|
| 2026-09-07 | No reference design files exist in the repo yet; Batches 1, 3, 4, 6, 7 cannot start without them | P1 (blocks those batches) | open — needs the maintainer |
| 2026-09-07 | The builder's serving technology (plain http.server vs a small framework) is not yet decided | P2 | open — recommendation in docs/features/page_builder.md |
| 2026-09-07 | "More data" (the maintainer's second ask alongside the redesign) has no specific sources named yet, so no docs/data_catalog.md row can be drafted (rule 8) | P2 | open — needs the maintainer |
| 2026-09-07 | Peer-group data bindings are specified ahead of Peer Model v1 (Block M) existing; the binding validator must reject them cleanly until then, not silently no-op | P2 | tracked in docs/features/data_binding.md |

Add a row here the moment an audit finds something, even if it's fixed in the same session —
this table is the project's memory of what has already gone wrong once.
