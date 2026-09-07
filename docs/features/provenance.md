# Feature: provenance — where every figure came from, and how it was made

Status: done (2026-09-07)
Issue: none (roadmap-driven — `docs/steps`, Block X)
Branch: feat/withheld-figures-visible (the defect), then feat/provenance-badge (the badge)

## Problem

Two roadmap items in Block X, and one live defect that is the real reason to do them now.

**The defect.** `data/communes_history.csv` holds **1,044 rows with status `S`** — ONEM withholds
any count below 10 for privacy, and those are stored with a NULL value and status `suppressed`.
The schema models this deliberately: `migrations/001_core_schema.sql` line 86,
`CHECK (value IS NOT NULL OR status IN ('suppressed','na'))`.

Both payload readers in `scripts/export_site_payloads.py` — `_read_communes_history()` and
`_read_communes_latest()` — test `if row["value"] == "": continue` *before* recording anything, so
those cells never reach `public/data/` at all. Measured against the real store:

| | |
|---|---|
| suppressed cells in the history export | 1,044 |
| `(commune, indicator)` pairs affected | 203 |
| communes affected | **188 of 565** |
| pairs that vanish from the payloads entirely | 36 |
| pairs whose **latest** period is suppressed | 158 |

The consequence is not cosmetic. On a `/local/{nis}` page a figure the source *has and refuses to
publish* is indistinguishable from one that was never collected — and for those 36 pairs the
indicator does not appear at all.

Worse, `local.html`'s attribution block states, in all three languages:

> Figures ONEM withholds for privacy (fewer than 10 people) are shown as suppressed, never as zero.

The string `suppressed` appears **exactly once** in `local.html` — inside that sentence. There is
no rendering path for it. That sentence is part of a licence notice, and it describes behaviour the
page does not have.

Meanwhile `communes.html`'s *table* renders a Suppressed pill correctly, because it reads
`data/communes_table.json`, which preserves status. So the site currently contradicts itself: the
same withheld figure is labelled on one page and invisible on another.

**A second, smaller defect.** `STATUS_LETTER_TO_WORD` maps only `{"A": "final", "P":
"provisional"}` and `_status_word()` passes anything else through unchanged.
`scripts/export_communes_csv.py`'s `STATUS_TO_LETTER` can emit `A P R E S N`. Today only
A/P/S/derived occur so nothing is visibly wrong, but a revised or estimate value would reach the
pages as a bare `"R"`. `communes.html` shipped this exact bug once already — see the comment above
its `statusPill()`.

**The missing feature.** No payload carries a source name. A reader cannot tell whether a figure
came from Statbel, ONEM or the federal police, nor whether it is the agency's own published number
or something this pipeline computed.

## Goal

Checkable when done:

1. A suppressed cell is published as `{"value": null, "status": "suppressed"}` and rendered as
   withheld — on the interactive commune page, on the statically generated one, and in both map
   tooltips.
2. A reader can tell three states apart: **withheld** (source has it, won't say), **not collected**
   (no row at all), and **a real measured zero**.
3. No comparison, percentile, chart point or headline figure is ever drawn from a period whose
   value is null. (158 pairs would otherwise regress.)
4. `_status_word()` raises on an unmapped status letter rather than passing it through.
5. Every municipal indicator carries a grade in {A, B, C, D} and, where applicable, a source id
   resolvable in a published source registry.
6. A derived figure exposes **its inputs'** retrieval dates, clearly attributed to the inputs and
   never presented as the figure's own update date.
7. Every new user-facing string exists in all three languages (rule 7).

## Non-goals

- **Translating the map component or `communes.html`/`map.html` chrome.** Those pages are English;
  the trilingual interface is Block X's own separate `[BUILD]` step. A half-translated map now is
  the failure `_indicator_names()`'s docstring already argues against. Every *new payload-carried*
  string is trilingual from day one, which is what rule 7 actually binds.
- **Canonicalising forecasts.** Grade D is defined and ships empty (see below).
- **Changing any published value.** The only value-level change is that withheld cells become
  visible as withheld.
- **Touching the bulk CSVs.** They already carry `S` correctly.
- Stale-data *alerting* (a separate Block X item; the staleness rule itself already exists in
  `src/validation/rules.py`).

## Proposed approach

### Three orthogonal axes, deliberately not collapsed

| Axis | Question it answers | Where it lives |
|---|---|---|
| **grade** | How was this number made? | per indicator (lineage) |
| **status** | How confident is the source in it? | per observation |
| **freshness** | When did we retrieve it, and what period does it describe? | per indicator + per observation |

Collapsing any two of these is what would make the badge lie. A provisional official figure and a
final derived one are different in kind, and a reader needs both facts.

### The grade taxonomy

| Grade | Meaning | Mechanical rule | Today |
|---|---|---|---|
| **A — Official** | The agency's own published figure for this geography and period, carried through re-keying and merger resolution only | Row in the `indicators` table, and its config declares no `transform` | 39 municipal indicators |
| **B — Restated** | Still one source series, but this pipeline changed the number itself (rebasing, unit conversion) | Config declares a `transform:` block | 2, both national |
| **C — Derived** | Computed by us from two or more series, or across time or peers | A `config/indicators/derived/{id}.yaml` exists | 13 indicators |
| **D — Forecast** | A value for a period the source has not yet measured | `status = 'estimate'`, or the config declares `display.forecast_of` | **empty by design** |

**Grade D ships empty, on purpose.** `SELECT source_id, count(*) FROM indicators GROUP BY 1` gives
`statbel 29, nbb 14, onem 7, police 4, eurostat 2, ameco_ec 1` — `fpb` has **zero** rows. FPB
forecasts stay on the legacy table, outside the canonical model, per
`docs/decisions/0001-data-model.md`; `data/belgian_forecasts.csv` is read only by
`dashboard.html`. Rather than build a badge with no data behind it, D is defined and a test asserts
the D set is empty *with the reason in the failure message* — so the day forecasts are
canonicalised, that test says the badge now has a live case.

**Grade B must be declared, never inferred.** Eleven configs declare `unit: index_2010`, but only
two are loaded: `EUROSTAT_GDP_Q_MEUR` and `LABOUR_COST_BE`, rescaled by
`EurostatFetcher._rebase_to_2010` (`src/fetchers/eurostat.py:56`). Grading off the unit string
would be wrong: `INDUSTRIAL_PROD` is `index_2021` *as NBB publishes it*, which is grade A. So B
comes from an explicit `transform:` block in the config — inferring it from a unit string is the
silent coercion rule 13 forbids.

### Suppressed is not a grade

**A suppressed cell carries no grade key at all.**

Grade describes how a value was produced. A suppressed cell has no value to describe. Giving it a
letter — even a demoted one — puts *the absence of a number* on the same ladder as the numbers, and
the first interface that sorts or colour-ramps by grade would imply "a poorer-quality figure exists
here" when the truth is "the source holds this figure and deliberately refuses to publish it".

It rides the axis that already exists and is already correct: `status: "suppressed"`, per period,
exactly as the schema models it. Grade-absent also matches this format's own absent-not-null
convention.

The distinction a reader must be able to make is **three-way**:

- **withheld** — `status: "suppressed"`, `value: null`
- **not collected** — no key at all
- **a real zero** — `value: 0`

### Provenance lives on the index, not in every commune payload

Source and grade are properties of the indicator's lineage — not of the commune, and not of the
period. `public/data/metadata/indicators.json` already exists as exactly one row per municipal
indicator (52 rows, 13 KB) and is already the anti-hardcoding index the 50% gate depends on.

Putting provenance there costs **zero bytes** across the 565-file `communes/` directory. Putting it
in each indicator entry would repeat the same source name roughly 29,000 times.

### `metadata/sources.json` is built from config, not from the database

One entry per source: `source_id`, trilingual `label`, `agency`, `name`, `url`, trilingual
`licence_note`, `cadence`, `catalog_ref`.

Built from `config/sources/*.yaml`, **not** the `sources` table. The configs are the declared
truth; the DB rows for `nbb`, `eurostat` and `ameco_ec` were written by
`scripts/port_existing_indicators.py:149` with `agency` copied into `name` and
`catalog_ref = "docs/data_catalog.md (pending)"`. Reading the DB would publish the weaker copy.

The three licences must not blur. `licence_note` is kept verbatim per source, and no combined
"Sources: Statbel, ONEM, Police" string is ever composed —
`test_page_does_not_claim_a_reuse_grant_police_never_made` exists because that blur was caught once
already. Statbel grants commercial reuse and requires a date of last update; ONEM grants commercial
reuse and requires the date of the information used; the federal police grant attribution only,
with no stated permission beyond it.

## Data / schema changes

**No changes to `observations`, `indicators`, `geographies` or any migration.** Grade and source are
computed at export from lineage that already exists, following the precedent of `additive`,
`updated`, `comparison` and `percentile` — all computed at export rather than stored (rule 6).

Config changes:

- `transform: {method: rebase, base: 2010}` added to `config/indicators/EUROSTAT_GDP_Q_MEUR.yaml`
  and `LABOUR_COST_BE.yaml`, plus the corresponding optional property in
  `docs/features/indicator_config.schema.json`.
- Optional trilingual `label` and `licence_note` on `config/sources/*.yaml`, plus the properties in
  `docs/features/source_config.schema.json` (which is `additionalProperties: false`, so this edit is
  required). Optional, falling back to `agency`, so the three national sources need no immediate
  translation work.

## Payload contract changes

### `communes/{nis}.json` — `periods` gains withheld cells

```jsonc
"PART_TIME_BENEFIT_RECIPIENTS": {
  "periods": {"2024": {"value": 11,   "status": "final"},
              "2025": {"value": null, "status": "suppressed"}},   // NEW
  "updated": "2026-09-06", ...
}
```

This is the **one documented exception** to the absent-means-no-data rule that
`docs/features/site_payloads.md` asserts. Absence means "we have no reading"; a withheld cell means
"the source has a reading and will not publish it". Collapsing the two destroys a distinction the
source deliberately created.

`updated` stays gated on a real value, so it keeps meaning "when the number you are looking at was
retrieved".

### `metadata/indicators.json` — carries the badge

```jsonc
// stored
{"indicator_code":"UNEMPLOYED_JOBSEEKERS", ..., "coverage":564,
 "grade":"A","source":"onem","updated":"2026-09-06","suppressed":1}

// derived
{"indicator_code":"AVG_NET_TAXABLE_INCOME", ..., "coverage":552,
 "grade":"C","source":null,
 "derived_from":["FISCAL_TOT_NET_TAXABLE_INC","FISCAL_NBR_NON_ZERO_INC"],
 "input_sources":["statbel"],"inputs_updated":"2026-09-05","suppressed":0}
```

`coverage` counts **numeric values only** (it previously counted keys, which now over-counts), with
a sibling `suppressed` count. No indicator may carry both `updated` and `inputs_updated`.

## The derived-date ruling

`docs/data_catalog.md` recorded this as open: *whether "derived" is a sufficient disclosure for a
derived figure under point 2 of Statbel's 2015 licence, or whether such a figure must also expose
the retrieval dates of its inputs.*

**Ruling: it must expose its inputs' retrieval dates, attributed to the inputs, never presented as
the figure's own update date.**

- Point 2 requires the date of last update *of the information reused*. The information reused in
  `AVG_NET_TAXABLE_INCOME` **is** two Statbel series — they are published inside that number.
  "derived" discloses the method and withholds the date the clause actually names.
- Point 5 forbids misleading a reader about the update date. Printing the inputs' date in the
  figure's own `updated` slot would do exactly that, which is the correct reason `_note_updated()`
  leaves the key absent today. That reasoning stays intact.
- The two conflict only if the date has one slot. It gets two: `updated` (this figure's own
  retrieval date — still absent for a derived figure) and `inputs_updated` + `input_sources`,
  worded differently on the page: *"computed from Statbel figures last updated 2026-09-05"*.
- It is mechanically free: every derived config names `derived.inputs`, and the graph is already
  proven acyclic by `resolve_order()` in `src/analytics/engine.py`.

This also closes a live gap. `map.html` currently prints *"derived — computed from the figures
above, not separately fetched"* with **no date at all**, for 13 of 52 indicators, on a page
publishing Statbel-derived municipal figures.

## Tests

Exporter:

1. A suppressed row reaches the commune payload as `{"value": null, "status": "suppressed"}`.
2. An all-suppressed `(commune, indicator)` pair produces an entry *with* periods — asserted
   against the real store: 36 such pairs.
3. `_status_word()` **raises** on an unmapped letter (fed `"X"` directly).
4. `_attach_comparisons()` matches the latest **valued** period, not the latest period — fixture
   with `2024: 11, 2025: suppressed` and aggregates for both years.
5. Every municipal indicator has a grade in {A,B,C,D}; grade A or B implies a `source` present in
   `metadata/sources.json`. Run against the **real published payloads** (skipped if unbuilt), like
   `test_no_page_names_an_indicator`, so it grows by itself as indicators are added.
6. Every derived indicator's `derived_from` names existing ids, and `inputs_updated` equals the max
   `updated` of those ids — hand-computed for `AVG_NET_TAXABLE_INCOME` (rule 5).
7. No indicator carries both `updated` and `inputs_updated`.
8. The grade-D set is empty, with the reason in the assertion message.
9. `metadata/sources.json` covers every referenced source; every entry has `en`, `fr` and `nl`.
10. `coverage` never counts a null; `coverage + suppressed <= 565`.

Frontend, via the existing Node harness in `tests/test_local_ui_logic.py`:

11. `latestOf` skips a suppressed newest period and returns the previous valued one.
12. `comparisonRows` compares at the latest valued period.
13. `suppressedTail` returns the withheld periods after the latest valued one.
14. Chart point arrays contain no nulls.
15. `provenance()` returns the derived-inputs sentence for a derived entry and the retrieval date
    for a fetched one, in each of EN/FR/NL.
16. Every new string key exists in all three `LocalUI.STRINGS` blocks. A key present in `en` and
    missing in `nl` silently falls back to English (`LocalUI.t`), which is the exact failure rule 7
    exists to prevent.

Compliance:

17. In `tests/test_statbel_attribution.py`: every municipal page states that a derived figure's date
    is its **inputs'** date and not its own.

## A finding worth recording: the config and the database disagree on two source ids

The lineage joins `indicators.source_id` to the source configs, and two of them do not match:

| indicator | config declares | `indicators` table says |
|---|---|---|
| `EUROSTAT_GDP_Q_MEUR` | `dbnomics_eurostat` | `eurostat` |
| `LABOUR_COST_BE` | `dbnomics_ameco` | `ameco_ec` |

Same sources, two identifiers each. The join would have silently found nothing for them, and two
national figures would have published with no attributable source.

Aliased explicitly in `DB_TO_CONFIG_SOURCE_ID` rather than renamed on either side: renaming a
`source_id` touches the observations that reference it and is a migration, not an export change.
`indicator_lineage()` **refuses** on an id it cannot resolve, so a third mismatch cannot hide the
way these two did.

## Assumptions and open questions

- The three-way withheld/not-collected/zero distinction is asserted to be what a reader needs. It
  follows the source's own behaviour (ONEM publishes a mask, not a zero) and the schema's existing
  model, so it is not a judgement call about Belgian semantics.
- `status = 'estimate'` is taken to mean forecast for grading purposes. No such rows exist today, so
  this is untested against real data and is recorded as the rule the empty-D test guards.
- Still open, and **not** decided here: which Statbel licence document governs (the 2015 PDF or the
  CC BY 4.0 in the *Conditions générales d'utilisation*). We continue to satisfy the union of both.
  See `docs/data_catalog.md`, "Open question for the maintainer".

## Rollout / risks

- **The 158 latest-period-suppressed pairs are the main danger.** Five call sites assume the newest
  period has a value: `_attach_comparisons()` in the exporter, and `latestOf`, `comparisonRows` and
  both chart-point builders in `local.html`. Miss one and a commune page shows a dash against a real
  province figure, or a chart with a hole. All must route through one helper, and the verification
  step counts comparisons drawn from null periods (must be zero; was 158).
- **Grade B is one declaration away from being wrong.** Nine `index_2010` configs are unloaded
  today; loading one without a `transform:` block would silently grade it A. The export refuses when
  a unit starts with `index_` and no `transform` is declared.
- **Two status-letter maps** exist — `STATUS_LETTER_TO_WORD` (Python) and `MAP_STATUS_WORDS`
  (`communes.html`). One fact stored twice, the drift class already guarded elsewhere by
  `test_swatch_width_agrees_between_the_component_and_its_stylesheet`.
- **Static-page rebuilds must stay byte-identical.** Everything added derives from committed data
  and no build stamp is introduced, so `test_url_stability` continues to hold.
- Payload growth is under 60 KB in total on a 37 MB directory.
