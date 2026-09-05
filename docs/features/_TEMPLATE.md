# Feature: <name>

Status: draft | approved | in-progress | done
Issue: #<issue-number>
Branch: feat/<issue-number>-<slug>

## Problem

What is broken or missing, for whom, and why it matters. No solution here.

## Goal

What must be true when this is done. Concrete and checkable, not aspirational.

## Non-goals

What this explicitly does not cover, to prevent scope drift.

## Proposed approach

How this will be implemented. Reference `docs/architecture.md` / `docs/data_model.md` where
relevant. Flag any deviation from those documents explicitly.

## Data / schema changes

Any changes to `observations`, `indicators`, `geo`, `forecasts`, `fetch_log`, or
`config/indicators/*.yaml`. Link to a `docs/decisions/` ADR if the change is architecturally
significant.

## New data sources

If this introduces a new data source, link the approved row in `docs/data_catalog.md`.
Per `CLAUDE.md` rule 8, this is required before implementation, not after.

## Tests

What automated tests will be added (unit tests for derived values, adapter contract tests,
`tests/golden/` entries for generated text) and how they will be run.

## Assumptions and open questions

Anything unresolved or uncertain. Do not guess at Belgian administrative or accounting
semantics — escalate instead.

## Rollout / risks

What could go wrong, and what happens if it does.
