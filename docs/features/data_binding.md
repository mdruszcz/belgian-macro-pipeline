# Feature: data-binding engine (block → payload)

Status: draft — spec only, Batch 14 not yet implemented
Issue: none (part of docs/features/page_builder.md, Batch 14)
Branch: feat/data-binding-engine (when started)

## Problem

A block on a page needs a value. The builder must let someone pick "which indicator, which
geography, which period, which operation" without ever letting them type a SQL query, a raw file
path, or an arbitrary URL into a page document (claude.md rules 20, 21, 23).

## Goal

A binding is a small, closed vocabulary — provider, indicator, geography, period, operation,
comparison scope — resolved entirely against payloads this pipeline already publishes. Nothing a
binding can express can produce a value the existing exporters wouldn't also produce.

## Non-goals

- A generic query builder. There is no "custom SQL" or "custom fetch" escape hatch, ever.
- New aggregation rules. A binding can only ask for what `src/analytics/aggregate.py` and
  `src/analytics/ranking.py` already compute; it cannot invent a new one in the browser.

## Proposed approach

### Allowed providers

national · municipal · indicator-snapshot (`indicators/{id}.json`) · geography metadata ·
page context (e.g. the current municipality from `context.nis`) · comparisons ·
rankings/percentiles · peer groups (once Peer Model v1 exists, Block M)

### Allowed operations

latest · history · comparison · ranking · percentile · table · map values

No generic "query" operation exists in the schema at all — not disabled, absent.

### Binding resolution rules (all inherited, not invented here)

- Unit, decimals, direction and trilingual name come from `metadata/indicators.json` — never
  typed into the binding.
- Whether a geography level is valid for an indicator comes from the indicator's own
  `geo_levels`.
- A non-additive indicator cannot be bound to a "sum" operation; the binding validator rejects it
  using the same `is_additive`/`aggregation_method` check the exporters already apply.
- A single-period indicator (e.g. a census snapshot) cannot be bound to a "history" operation
  that would silently render a one-point line chart as if it were a trend.
- A suppressed value resolves to the suppressed state (claude.md rule 26), never to a number,
  never to a blank cell that looks like "not collected."
- A derived indicator's binding carries no fetched date of its own, per the existing rule in
  `docs/features/provenance.md` — a block must not invent one.
- Query size (period range, geography count) is bounded, so a block cannot request "every period
  of every commune" and blow up the page payload.

### Binding → block wiring

```jsonc
"binding": {
  "provider": "municipal",
  "indicator": "AVG_NET_TAXABLE_INCOME",
  "geography": {"mode": "context"},      // or {"mode": "fixed", "nis": "52018"}
  "period": {"mode": "latest"},
  "operation": "latest",
  "comparison_scope": ["province", "region", "country"]
}
```

## Tests

- Belgium / region / province / municipality geography modes
- Fixed NIS vs context NIS (the latter only valid on a page type that declares a context)
- Valid and invalid aggregation per indicator
- Missing vs suppressed vs a real explicit zero, rendered distinctly
- A derived indicator's binding never carries a fabricated retrieval date
- Excessive period range or geography count is rejected
- A malicious or malformed indicator id / NIS code in a binding is rejected, not coerced
- Payload → preview → public render produce the identical value (parity test)

## Assumptions and open questions

- Peer-group bindings are listed as "allowed provider" ahead of Peer Model v1 existing (Block M).
  Until that model is built, any binding naming `peer` as a provider must be rejected by the
  validator with a clear "not available yet" message, not silently ignored.

## Rollout / risks

The binding vocabulary is the single most security-sensitive surface in the builder (it is the
one place a page document reaches back into the data pipeline). `security-red-team` review is
required before this ships, per the audit plan in docs/features/page_builder.md.
