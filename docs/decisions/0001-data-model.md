# 0001. Canonical data model (five tables)

Status: Accepted
Date: 2026-09-05
Issue: #20

## Context

`docs/features/data_model.md` specifies a five-table canonical model — `sources`,
`geographies`, `indicators`, `observations`, `fetch_runs` — to replace the flat
`(indicator_code, period)`-keyed schema in `belgian_macro_db.py`. It was written, reviewed
against municipal mergers, sector-level data, multi-source conflicts and revisions, and
approved on issue #20.

Without a freeze, every future session is free to re-propose the table structure, the
`observations` primary key, or the enums, which is a standing cost on review attention rather
than a one-time design decision.

## Decision

The five-table model in `docs/features/data_model.md`, as written at the time of issue #20's
approval, is frozen. In particular, frozen:

- The table set: `sources`, `geographies`, `indicators`, `observations`, `fetch_runs`. No sixth
  table (forecasts explicitly deferred, not added).
- `observations` primary key: exactly `(indicator_id, geo_id, period, vintage)`.
- The `status` enum: `final, provisional, estimate, revised, suppressed, na`.
- The `preferred_direction` enum: `lower_is_better, higher_is_better, neutral, contextual`.
- The period format rules and the `period_start`/`period_end` sorting mechanism.
- The resolution to multi-source conflicts (one source per indicator, not a widened key) and to
  municipal mergers (`valid_from`/`valid_to`/`successor_geo_id`, additive-only back-aggregation).

## Consequences

Any change to the above — not an addition alongside it, but a change to it — requires a new ADR
under `docs/decisions/` that explicitly supersedes this one, stating what changes and why. It is
not made by editing `docs/features/data_model.md` in place, and not made by an agent proposing
"improvements" to the schema in the course of unrelated work.

Explicitly not frozen by this ADR, and open for follow-up decisions without superseding it:

- Where FPB forecasts live (spec's own open question Q1) — deferred, not resolved.
- The geo_id convention for non-Belgium indicators (Germany, France, Netherlands, Spain, EA
  aggregate, EU27 series currently in `SOURCES`) — not yet assigned.
- Sector/NACE-level breakdowns (spec Review §2) — explicitly out of scope until genuinely
  needed, per the spec's own recommendation to encode into `indicator_id` for now and open a new
  ADR if a wider key is ever required.
- Adding new indicators, geographies, or sources within the frozen structure — routine, not a
  schema change.
