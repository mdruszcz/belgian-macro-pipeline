# Invariants for the redesign and page-builder work

These are the properties every batch must hold, restated from claude.md rules 17-36 and
docs/features/page_builder.md so an agent working on one batch can check itself without
re-reading three documents. If a batch's own audit finds one of these broken, that is a P0 or P1
finding (docs/implementation/known-risks.md defines the severity levels), not a note for later.

1. The public site is still 100% static after every batch. `git grep` for a server-only
   dependency in anything under `assets/`, `local/`, or the page-root HTML files should find
   nothing.
2. Every route that resolves today still resolves after the batch — canonical and legacy.
3. No block, template or design asset contains a hand-typed indicator value, commune name tied
   to a specific figure, or fabricated number. Every number on screen traces to a real payload
   path.
4. The database schema, `src/fetchers/`, `src/analytics/`, and `resolve_geo()` are untouched
   unless the batch's own spec is an ADR-approved exception.
5. A page document never contains `<script>`, inline event handlers, a remote URL, or SQL.
6. Missing / unavailable / suppressed / not-applicable / explicit-zero remain five distinct,
   correctly-labelled states in every new block, exactly as they already are in
   `assets/commune_map.js` and `local.html`.
7. A ratio is never averaged across geographies by a block. It is recomputed from summed
   components using the existing aggregation functions, or refused.
8. Every block showing a figure also shows its source, unit, period and freshness, read from
   metadata, never typed by whoever built the page.
9. Two builds from the same published page documents and the same data produce byte-identical
   output.
10. A draft can be edited and saved freely with zero effect on the public site. Only an explicit,
    validated publish step changes what a visitor sees, and a failed publish changes nothing.
