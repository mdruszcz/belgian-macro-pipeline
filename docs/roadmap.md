# Roadmap

Three milestones. Each is defined by what must be true, not by a date or a task list. See
[architecture.md](architecture.md) for as-is/target architecture and
[data_model.md](data_model.md) for the target schema referenced below.

## 50% — the national local-data platform works

Definition: the pipeline reliably produces canonical, municipality-resolvable Belgian
indicator data, end to end, with no manual intervention and no unresolved data-quality gaps.

Must be true:

- `observations` is keyed by `(indicator_id, geo_id, period, vintage)` per
  [data_model.md](data_model.md) — not the current `(indicator_code, period)` — for at least
  one real indicator with genuine municipality-level granularity, not just a national/EU
  aggregate tagged with a placeholder `geo_id`.
- `resolve_geo(nis, period)` exists and is used by every adapter; no adapter stores a raw NIS
  code directly.
- Indicator metadata lives in `config/indicators/*.yaml`, each entry with a corresponding
  approved row in `docs/data_catalog.md` — not the `SOURCES` dict in `belgian_macro_db.py`.
- Every adapter implements the shared `DataSource` interface and passes the shared contract
  test (`NBBFetcher`, `DBnomicsFetcher`, `FPBFetcher`, and the `fetch_stocks.py` sources are
  either migrated to this interface or explicitly dropped).
- A source schema change fails loudly (`fetch_log.status = 'SCHEMA_CHANGED'` or equivalent) —
  not silently coerced, not merged into the generic `'ERROR'` bucket as it is today.
- Derived statistics (e.g. the BE–DE spread currently computed inline in `fetch_stocks.py`) are
  computed in Python only, covered by unit tests with hand-computed expected values, and never
  written into `observations` as if they were source data.
- `en/fr/nl` labels exist for every indicator and every geo entity actually in use.
- Revisions are preserved: re-fetching a period that later gets restated adds a new `vintage`
  row rather than overwriting the value, and `is_latest` is maintained correctly.
- The full test suite (unit tests on derived values, the adapter contract test) runs and
  passes in CI on every change, not just ad hoc local runs.
- Nothing in the "Prohibited until the 50% milestone" list in `CLAUDE.md` has been built
  (no chatbot UI, no user accounts, no vector DB/agent framework/custom LLM, no frontend
  framework rewrite, no data source outside the approved catalogue) — the platform can reach
  50% without any of that, and building it early is itself a sign of scope drift, not progress.

Explicitly NOT required at 50%: any output framed as advice, a report, or a deliverable a
commune would recognize as something to act on. At 50% the platform produces trustworthy data;
it does not yet produce judgment.

## 80% — it produces analysis a commune would pay for

Definition: municipality-level output exists that a specific named commune, shown it without
being told it's a demo, would recognize as answering a question they actually have — not a
dashboard of numbers, an analysis with a conclusion.

Must be true:

- At least one concrete analysis product exists end to end for real municipalities: a defined
  question (e.g. "how is commune X's economic position trending relative to comparable
  communes"), computed deterministically from `observations`/`geo`/`indicators`, phrased into
  text by an LLM that only phrases verified facts from a fact object it did not compute
  (`CLAUDE.md` rule 4) — the LLM never calculates the statistic itself.
- The analysis has been validated against at least one real commune's numbers by a human who
  knows that commune, and the numbers were correct — not merely "the pipeline ran without
  error."
- `preferred_direction` and the population-weighted aggregation rule (with documented
  exceptions) are actually used to produce comparisons/rankings across municipalities, not just
  defined in the schema.
- `tests/golden/` exists and every statistical claim the analysis generates passes against it —
  this is the gate that lets generated text be trusted, not just the underlying data.
- The output is something a commune official could hand to a councillor or put in a report
  without embarrassment: correct, dated, sourced, and in their preferred language (en/fr/nl).
- At least one round of feedback has been sought from an actual commune (or a credible proxy —
  someone who currently does this analysis manually for a commune) on whether the output
  answers a real question, and that feedback has been acted on. "Would a commune pay for this"
  is a claim about a specific person's reaction, not an assumption.
- Multiple municipalities are covered well enough that the analysis isn't a one-off hand-tuned
  case — the pipeline, not a person, produces the next commune's version.

Explicitly NOT required at 80%: a priced product, a contract, a subscription, or a sales
process. 80% is "this is worth paying for," independently of whether anyone has yet been asked
to pay.

## 100% — somebody actually pays for it

Definition: a real commune (or an institution acting for one — a federation, a province, a
consultancy that resells to communes) has paid actual money for actual continued access to
this, under terms where non-payment stops the deliverable. Not a grant with no renewal
condition, not a compliment, not a verbal "we'd definitely use this."

Must be true:

- There is a paying customer with a name, an invoice or contract, and a renewal or churn point —
  i.e., a moment where they could stop paying and the deliverable would stop, and they chose to
  keep paying (or this is the first payment and a second is still owed to prove it wasn't a
  one-off favor).
- The thing they are paying for is the 80% analysis product (or something built directly on
  it), not a one-off consulting engagement that happens to use this codebase's data once and
  otherwise has nothing to do with the pipeline.
- The maintainer, not an agent, made every pricing, contracting, and sales decision — none of
  that is in scope for an AI agent working on this repo under `CLAUDE.md`.
- Whatever commercial terms exist are compatible with the "Prohibited until the 50% milestone"
  list having been respected on the way here — i.e., the thing being sold is the platform
  described in this roadmap, not a shortcut (chatbot wrapper, no-real-geo-resolution demo, etc.)
  that happened to look sellable faster.

Explicitly NOT sufficient for 100%: a pilot with no payment, a municipality "interested," a
non-binding letter of intent, internal enthusiasm, or a payment for something unrelated to this
platform's actual output.

## Sequencing note

These are gates, not phases to schedule against a calendar. Work items should be evaluated
against "does this move a specific unmet condition above from false to true," not against
whether it is generically good engineering. Anything that helps 50% pass without helping
answer "would a named commune pay for this" is not automatically worth doing next.
