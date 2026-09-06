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
16. ALWAYS read docs/steps before starting work — it is the current plan of
    record (checked-off steps are marked ✅). ALWAYS update docs/steps to
    check off a step (✅) the moment it is genuinely done; never batch this
    for later, and never check off a step you have not personally verified.

## Definitions you must respect
- period: YYYY, YYYY-Qn, YYYY-MM, or YYYY-MM-DD, matching the indicator's
  declared frequency.
- observations PK: (indicator_id, geo_id, period, vintage).
- status: final | provisional | estimate | revised | suppressed | na.
- preferred_direction: lower_is_better | higher_is_better | neutral |
  contextual.
- Aggregates (province, region, Belgium) are built from the ground up, never
  by averaging communes:
  - an additive indicator (a count or a total) is SUMMED;
  - a ratio or average is RECOMPUTED from those sums, using the same formula
    as at commune level -- e.g. average income = total income / total tax
    returns, never the mean of 565 commune averages;
  - anything that is neither (an index, a share) has no defensible aggregate.
    Refuse, do not invent one.
  Population-weighting is NOT used and must not be added. It was measured
  against the correct figure and is wrong for both ratios in the pipeline
  today, because population is not their denominator -- income is per tax
  return, the dependency ratio is per working-age person. See
  docs/features/comparison.md, "Correction 1", for the numbers.
- Aggregate over the geographies that existed IN THAT PERIOD, not today's.
  Summing today's 565 communes understates a 2023 province total by up to
  24%, because communes created in the 2025 mergers have no earlier rows.
- Every aggregate carries its coverage (how many geographies contributed out
  of how many existed) and is suppressed below 90%. A total that is 24%
  short is not a number with a footnote.

## Workflow you must follow
1. Read the linked spec in docs/features/. If no spec exists, stop and ask.
2. Work on a branch named feat/<issue-number>-<slug>.
3. Implement. Add tests. Run ruff, black, pytest.
4. Open a PR stating: files changed, tests added, tests run and their output,
   assumptions made, unresolved issues.
5. Do not merge. The maintainer merges.
6. After finishing a big step (a Block, a CONTROL/GATE, or any multi-PR unit
   of work in docs/steps), update docs/steps' checkmarks first, then run
   /clear before starting the next big step. Keeps context focused on the
   step at hand instead of dragging the whole history forward.

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

Put that detail in the PR body, not in the chat reply. The PR is the record;
the chat reply is a conversation with a person.

## How to talk to the maintainer (read this every time)

The maintainer is a vibe coder: sharp about the product and the Belgian
domain, NOT a career software engineer, and not interested in becoming one.
He is the customer for your explanations. If he doesn't understand your
summary, your summary failed — no matter how good the code was.

**Write like you're telling a smart colleague what happened. Not like a
changelog, a compliance report, or a thesis.**

Rules:

1. **Lead with the answer.** First sentence = what happened and whether he
   needs to do anything. Not what you explored, not how hard it was.
2. **Max ~150 words unless he asks for more.** Long replies are not
   thorough, they're unread. Detail goes in the PR.
3. **No jargon without a plain gloss.** If you write "vintage",
   "idempotent", "topological sort", "peer set", "coverage-gated", add the
   plain meaning in the same sentence or don't use the word. Write "the old
   version of a number we keep on file" instead of "vintage" where you can.
4. **Say why it matters in his terms** — money, a client seeing a wrong
   number, legal risk, wasted time. Not "this violates the contract in
   source_adapter.md".
5. **No tables, no nested bullets, no bold-every-third-word** in a chat
   reply. Plain sentences and at most a short list.
6. **Decisions he must make go at the top, as a plain question with your
   recommendation.** Never bury a question that blocks work under three
   paragraphs of what you built. One question at a time if possible.
7. **Numbers need a "so what".** "Limburg was 24% short" means nothing on
   its own. "We'd have shown Limburg's tax base as 16 billion when it's
   21 billion — a client there would spot it immediately" means something.
8. **Don't perform diligence.** He does not need to hear that you measured
   before writing, verified rather than assumed, or considered three
   options. Just say what's true now. Show the work only where it changes
   his decision.
9. **Bad news first and plainly.** "I broke X" / "we were breaking a licence
   rule" up front, in one sentence, no cushioning.
10. **Don't re-explain what he already knows.** He knows what his own
    project does.

Test before sending: would this make sense read aloud to someone who has
never opened the repo? If not, rewrite it.