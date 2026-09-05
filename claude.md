# CLAUDE.md — Operating rules for AI agents on BelPulse

## What this project is
BelPulse is a Belgian economic-intelligence platform. It ingests official
statistics, normalizes them into a canonical model, computes deterministic
analytics, and publishes municipality-level profiles and reports.

Correctness beats speed. A wrong number published to a municipality is worse
than a missing feature.

## Non-negotiable rules

1. NEVER modify production data manually. Data changes only through adapters
   and migrations.
2. NEVER put indicator-specific metadata in dashboard HTML or JS. All metadata
   lives in config/indicators/*.yaml.
3. ALL observations use canonical NIS-based geo IDs resolved via
   resolve_geo(nis, period). Never trust a raw NIS from a source file.
4. NEVER let an LLM calculate a statistic. Python computes; the LLM only
   phrases verified facts from the fact object.
5. ALL derived statistics require unit tests with hand-computed expected values.
6. NEVER write a derived value into observations as if it were source data.
7. Preserve multilingual labels (en/fr/nl) on every user-facing string.
8. NEVER add a new data source without a row in docs/data_catalog.md approved
   by the maintainer.
9. Run the full test suite before claiming completion. Report the actual output.
10. Do not modify files unrelated to the issue. If you believe an unrelated
    change is needed, stop and say so.
11. Never commit directly to main or develop.
12. Never commit files over 25 MB or new binary artefacts without asking.
13. If a source schema changed, fail loudly. Never silently coerce or drop rows.
14. Every new adapter must implement the DataSource interface and pass the
    shared contract test.
15. Statistical claims in generated text must pass tests/golden/.

## Definitions you must respect
- period: YYYY, YYYY-Qn, YYYY-MM, or YYYY-MM-DD, matching the indicator's
  declared frequency.
- observations PK: (indicator_id, geo_id, period, vintage).
- status: final | provisional | estimate | revised | suppressed | na.
- preferred_direction: lower_is_better | higher_is_better | neutral |
  contextual.
- National aggregates are population-weighted unless the indicator config says
  otherwise. Document any exception.

## Workflow you must follow
1. Read the linked spec in docs/features/. If no spec exists, stop and ask.
2. Work on a branch named feat/<issue-number>-<slug>.
3. Implement. Add tests. Run ruff, black, pytest.
4. Open a PR stating: files changed, tests added, tests run and their output,
   assumptions made, unresolved issues.
5. Do not merge. The maintainer merges.

## Prohibited until the 50% milestone
- AI chatbot interfaces
- User accounts and authentication
- Mobile apps
- Vector databases, agent frameworks, custom LLMs
- Rewriting the frontend in React or any framework
- Any new data source outside the approved catalogue

## When you are unsure
Say so explicitly under "Assumptions and open questions". Do not guess at
Belgian administrative or accounting semantics. Escalate instead.

## Reporting format
End every task with: files changed / tests added and result / assumptions /
what you did NOT do / risks you noticed.