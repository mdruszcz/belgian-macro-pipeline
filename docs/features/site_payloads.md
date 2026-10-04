# Feature: site payload exports (Block J)

Status: built
Issue: (Block J — Payload exports, docs/steps)
Branch: spec/block-j-site-payloads

## Implementation (2026-09-06)

`scripts/export_site_payloads.py` implements the layout above by reshaping
the already-written bulk exports (`communes_history.csv`, `communes_export.csv`,
`belgian_macro_export.csv`) plus a `geographies` query for
`metadata/geographies.json` — no new computation, no new database read of
`observations`. Wired into `daily_fetch.yml` (after `validate_data.py`, so
`manifest.json`'s `validation_status` is always a real "pass") and
`manual_sources.yml` (which has no full-DB validation step, so it honestly
records `"unknown"` there instead of a check that did not happen).

Measured against the real committed data: 565 commune payloads, 5 indicator
payloads (raw indicators only — `communes_export.csv` is latest-only and does
not carry derived indicators), 17 national indicators, 622 geographies. The
largest commune payload (Antwerp, full history) was 9.5 KB compact / 2.3 KB gzip
when this was measured at 14 indicators; it is 52 indicators today and larger,
still far inside the ~150 KB budget, and
close to the pre-build projection above.

Tests in `tests/test_export_site_payloads.py` cover the round-trip (every
value in a commune payload traces back to a row in the history CSV and vice
versa), the current-communes-only contract for `indicators/{id}.json`, the
ancestor-walk resolving to a real region-level entry in
`metadata/geographies.json`, and manifest row counts matching generated file
counts.

## What this is for

Block K's `/local` interface (not yet built) needs one commune's worth of data fast, on a phone, in
a council meeting. The bulk exports this pipeline already publishes — `communes_export.csv`,
`communes_history.csv`, `belgian_macro_export.csv` — are the right shape for a researcher downloading
everything and the wrong shape for a browser that wants Aartselaar and nothing else: today's
`communes_history.csv` alone is 18 MB for 565 communes neither Block K nor a mobile visitor should
ever have to fetch to render one page.

This block splits the bulk exports into per-entity payloads small enough to fetch on demand, without
changing or removing the bulk exports themselves — they stay the credibility feature the roadmap
already names them as, for researchers and journalists who want everything at once.

## Measured today, not guessed

Built the actual payload shape (see below) from the real committed `communes_history.csv` and
measured it directly, rather than estimating a budget before there was anything to check it against:

| | compact JSON | gzip |
|---|---|---|
| All 565 communes today (14 indicators, 2005–2026) | 4.60 MB total, 8.1 KB average | 1.06 MB total |
| Largest commune today (Aartselaar, 169 rows) | 8.3 KB | 1.9 KB |
| Projected at 200 indicators, same avg. history depth (12.07 periods/indicator) | ~119 KB | ~27 KB |

The 200-indicator projection scales today's density (169 rows / 14 indicators ≈ 12.07 periods per
indicator on average) linearly to 200 indicators — 2,414 rows for the richest commune. That is a
real projection from measured density, not an assumption that every one of 200 indicators will carry
full history; some (like `LOCAL_UNITS_BY_COMMUNE` today) will always be single-period.

**Conclusion: the roadmap's own "~150 KB per commune" budget is not a stretch target here, it is
comfortably inside what real density projects to** — even the pessimistic compact-JSON figure (not
gzip) lands at ~119 KB for the richest commune at full 200-indicator scale. GitHub Pages (served via
Fastly) compresses JSON responses in transit automatically, so a browser fetch is closer to the gzip
column than the compact one; the compact figure is what matters for a client that stores the payload
(e.g. a service-worker cache) rather than only fetching it.

## File layout

```
public/data/
  national.json                 -- be:country, every indicator, full history
  aggregates.json               -- Batch 7: province/region/country cross-section, per indicator
  communes/{nis_code}.json      -- one commune, every indicator that has a value for it, full history
  indicators/{indicator_id}.json -- one indicator, every current commune, LATEST value only
  metadata/indicators.json      -- already exists (Block B): categories + indicator display metadata
  metadata/geographies.json     -- NEW: commune list (id, NIS, trilingual name, region/province/
                                    arrondissement) for Block K's search/autocomplete and Block L's
                                    comparison picker
  metadata/national_sections.json -- Batch 6: the macro.html layout
  metadata/micro_sections.json  -- Batch 7: the micro.html layout
  manifest.json                 -- build_id, git_commit, build_date, row counts per dataset
```

Filenames use `nis_code` (`11001.json`) rather than `geo_id` (`be:mun:11001.json`): shorter URLs, and
the NIS code is already the public-facing identifier Statbel itself uses — `/local/11001` reads as a
real address, `/local/be:mun:11001` does not. `indicator_id` is already filename-safe
(`^[A-Z][A-Z0-9_]*$`, enforced by `indicator_config.schema.json`).

### `communes/{nis}.json` — full history, one commune

The shape measured above: every indicator that commune has *any* value for, keyed by period, exactly
mirroring what `communes_history.csv` already contains for that commune — this file is that CSV,
sliced by commune and reshaped to JSON, not a new computation. Includes derived indicators exactly as
`communes_history.csv` does today; there is no second, payload-specific derivation path (CONTROL G
stays satisfied: nothing here writes to `observations`, it only reads what
`export_communes_history_csv.py` already assembled).

A commune with no fiscal or population history yet (i.e. it does not exist as a distinct entity in
those files — the 13 communes formed in the 2025 merger wave, see `fiscal_income.md`) simply has
fewer keys in `indicators`; there is no placeholder, no `n/a` sentinel. An indicator's absence *is*
the "no data" signal a Block K missing-data component reads, matching how the CSV exports already
treat a missing cell as an absent row rather than a null one.

#### The one exception to absent-not-null: a WITHHELD cell (added 2026-09-07)

A cell the source holds and refuses to publish **is** published here, as
`{"value": null, "status": "suppressed"}`. It is the only null value this format emits, and it is
deliberate.

ONEM masks any count below 10 for privacy. The canonical schema models that explicitly —
`migrations/001_core_schema.sql`: `CHECK (value IS NOT NULL OR status IN ('suppressed','na'))` —
and `communes_history.csv` carries 1,044 such rows today.

Absence and a withheld cell are **different facts**:

| | Meaning |
|---|---|
| key absent | we have no reading |
| `{"value": null, "status": "suppressed"}` | the source has a reading and will not publish it |
| `{"value": 0, ...}` | a real, measured zero |

Collapsing the first two destroys a distinction the source deliberately created. Until 2026-09-07
this file did collapse them: both readers in `export_site_payloads.py` skipped an empty value before
recording anything, so those 1,044 cells never appeared — 203 (commune, indicator) pairs across
**188 of the 565 communes**, 36 of which vanished entirely, taking the whole indicator off those
commune pages. `local.html`'s attribution block meanwhile stated in all three languages that
withheld figures "are shown as suppressed, never as zero", which the page had no code to do. That
sentence is part of a licence notice. See `docs/features/provenance.md`.

**Consequences a consumer must handle.** A null can be the newest cell — for 158 pairs it is — so
nothing may take `sorted(periods)[-1]` and assume a number. Read the latest *valued* period instead
(`_latest_valued_period()` in the exporter, `LocalUI.latestPeriodWithValue()` on the page). A chart
drops withheld cells, leaving a visible gap; a comparison is drawn at the latest valued period; and
`coverage` in `metadata/indicators.json` counts numbers, never keys, with a sibling `suppressed`
count so "the source masked 152 communes" and "13 were never measured" stay separable.

### `indicators/{id}.json` — one indicator, every commune, latest only

The cross-sectional companion to the per-commune file: every current commune's *latest* value for
one indicator, for a national choropleth (Block X) or a "how does my commune rank" view (Block L)
that needs one indicator across all 565 communes, not all indicators for one commune. Reuses
`communes_export.csv`'s existing snapshot query (`export_communes_csv.py`) rather than inventing a
second "latest value" computation — filtered to one indicator and reshaped, the same relationship
`communes/{nis}.json` has to `communes_history.csv`.

### `national.json`

`be:country`'s full history across every indicator currently in `belgian_macro_export.csv`, reshaped
the same way as a commune payload. One file rather than one-per-indicator because there is only one
national geography — splitting it further would multiply file count for no fetch-size benefit.

### `live_counters.json` (added for the public-finance live counters)

The segment parameters for the simulated public-finance "live counters" (population,
debt, deficit, revenue and spending with named parts) -- written by
`scripts/export_live_counters.py`, computed by `src/analytics/live_counters.py`.
Reads `national.json`, `aggregates.json` and `metadata/indicators.json` (all three
already written by the time this runs), never SQLite. See
`docs/features/public_finance_live.md` and
`docs/decisions/0016-simulated-live-counters.md` for the method; this entry only
records the file's shape.

```json
{
  "schema_version": 1,
  "simulated": true,
  "method": {"trend_window_years": 3, "horizon_years": 2, "remainder_tolerance_meur": 0.5},
  "valid_from_ms": 1767222000000,
  "valid_until_ms": 1830294000000,
  "updated": "2026-10-03",
  "counters": [
    {
      "id": "revenue", "kind": "flow", "unit": "meur",
      "label": {"en": "Government revenue", "fr": "...", "nl": "..."},
      "basis": [{"indicator": "GOV_REVENUE_BE", "period": "2025", "value": 314736.4,
                 "status": "provisional", "source": "eurostat", "updated": "2026-10-03"}],
      "state": "available", "growth_rate": 0.0461895754,
      "annual_meur": {"2026": 329273.9, "2027": 344483.0},
      "segments": [{"start_ms": 1767222000000, "end_ms": 1798758000000,
                     "v0": 0.0, "v1": 329273940678.92, "rate_per_ms": 10.441208164602994}],
      "breakdown": {"state": "available", "year": "2025", "within": "GOV_REVENUE_BE",
                     "parts": [{"id": "pit", "names": {"en": "...", "...": "..."},
                                "share": 0.240808, "basis": [...], "segments": [...]}]}
    }
  ],
  "placements": {"macro_strip": ["population", "revenue", "spending", "deficit", "debt"],
                  "macro_breakdowns": ["revenue", "spending"], "home_strip": ["debt", "deficit"]}
}
```

A counter (or a breakdown alone, leaving its parent counter's own totals untouched)
publishes `"state": "unavailable"` plus a `"reason"` string instead of its numeric
fields when a DATA condition — not a config one — makes it impossible to compute for
this run (ADR 0016's own failure policy). `placements` is config-driven
(`config/live_counters.yaml`), never hand-typed per page.

**The top-level `valid_from_ms`/`valid_until_ms` are an ENVELOPE, not every counter's
own window.** They are `min`/`max` over every available counter's own first/last
segment boundary (`build_payload`, `scripts/export_live_counters.py`) — useful only to
know the payload's outer bounds. Each counter's OWN window is its own
`segments[0].start_ms` / `segments[-1].end_ms`, and these genuinely differ: `debt`
anchors on whichever of its two configured series has the later period end (e.g.
2026-Q1, starting mid-quarter), while `revenue`/`spending`/`deficit`/`population` all
start at the next full calendar year after the latest annual figure. A fixed real
example: envelope `valid_from_ms` 1767222000000 (1 Jan 2026) while `debt`'s own first
segment starts at 1774998000000 (2026-Q1's own start, later). PR 2's browser code must
read each counter's own `segments[0].start_ms`/`segments[-1].end_ms` to know when ITS
line is valid, never assume every counter shares the envelope.

#### `metadata/national_sections.json` gains a `finances_publiques` key (issue #309 PR 2)

The one-line mention in the file layout above (Batch 6) covers every OTHER chapter's own
key in this file; this is the first new key added since. Shape, inside the existing
top-level object:

Real excerpt (abbreviated) from the committed file, `finances_publiques` is a sibling of
`official_figures`'s own `charts` list, not nested inside it:

```json
{
  "...": "... every existing key (kpis, panels, unavailable, ...) unchanged ...",
  "finances_publiques": {
    "official_figures": {
      "label": {"en": "Official figures", "fr": "...", "nl": "..."},
      "note": {"en": "...", "fr": "...", "nl": "..."},
      "cards": [
        {"id": "debt", "label": {"en": "Government debt", "fr": "...", "nl": "..."},
         "anchor_indicator": "GOV_DEBT_Q_MEUR_BE",
         "items": [{"indicator": "GOV_DEBT_MEUR_BE"}, {"indicator": "GOV_DEBT_PCT_GDP_BE"}]}
      ]
    },
    "charts": ["... five chart configs, same {id, title, ...} shape every other chapter's panel_charts uses ..."],
    "simulated": {
      "strip_placement": "macro_strip", "home_strip_placement": "home_strip",
      "chapter_badge": "finChapterBadge", "chapter_pause": "finChapterPause",
      "strip_card": "finance-live-strip", "strip_list": "finStripCounters",
      "strip_badge": "finStripBadge", "strip_headline": "finStripHeadline", "strip_why": "finStripWhy",
      "breakdowns": [
        {"counter": "revenue", "card": "finance-breakdown-revenue", "title_key": "finRevenueByType",
         "year_key": "finRevenueShareYear", "year_note": "finRevenueYearNote",
         "bar": "finRevenueShareBar", "list": "finRevenueShareList",
         "badge": "finRevenueBadge", "headline": "finRevenueHeadline", "why": "finRevenueWhy"}
      ]
    }
  }
}
```

Produced by the same `_national_sections()` / `_check_national_sections()` pair in
`scripts/export_site_payloads.py` every other chapter's own cards/panels already go
through, from a new `finances_publiques` block in `config/national_sections.yaml` --
not a second config file or a second exporter step. `_check_national_sections()` was
extended (not replaced) to also refuse: an `indicator`/`anchor_indicator` absent from
`public/data/national.json`'s indicator set, a `counter`/`strip_placement`/
`home_strip_placement` absent from `config/live_counters.yaml`'s own declared ids, and a
trilingual `label` missing `en`/`fr`/`nl` -- exercised by
`tests/test_export_site_payloads.py::test_finances_publiques_refuses_*`.
`anchor_indicator` on the `debt` card (the quarterly debt series, `GOV_DEBT_Q_MEUR_BE`)
is what lets macro.html's script find that counter's own official annual/quarterly
figure without ever naming an indicator id in the page's `<script>` (rule 24) -- it looks
up the card whose `anchor_indicator` matches the simulated debt counter's own anchor
basis, not the other way around. Every `strip_*`/`chapter_*`/breakdown field above is an
element id or an i18n key, read generically by macro.html/home2.html's own wiring and by
`live_counters.js`'s `mount()` -- never a literal in either script (rule 2/24).

### `aggregates.json` (added Batch 7)

The province/region/country cross-section micro.html's territorial comparison and any future
comparison view read, reshaped from `data/aggregates.csv` (`export_aggregates_csv.py`, Block L) --
the same file `_read_aggregates()` already folded into commune payloads' `comparison` field, now
also published on its own so a page can compare GEOGRAPHIES to each other, not only a commune to its
ancestors.

```json
{
  "levels": ["country", "region", "province"],
  "indicators": {
    "AVG_NET_TAXABLE_INCOME": {
      "be:country": {
        "level": "country", "nis_code": null, "name": {"en": "Belgium", "fr": "Belgique", "nl": "België"},
        "periods": {"2023": {"value": 27453.1, "coverage": {"n": 565, "of": 565, "pct": 100.0}}}
      },
      "be:prov:10000": {"level": "province", "nis_code": "10000", "name": {"...": "..."}, "periods": {"...": "..."}}
    }
  }
}
```

**Arrondissement is computed in the CSV but deliberately excluded here**, matching
`COMPARISON_LEVELS` in this module and `docs/features/comparison.md`'s "The comparison set" -- the
same reason a commune's own comparison field stops at province/region/country. Written whenever an
aggregates CSV is supplied, independent of whether a micro-style layout exists, since the aggregate
values are useful on their own.

Built with `sorted()` at both the indicator and geography level rather than `sort_keys=True` on the
final `json.dumps` -- the same determinism approach every other payload here uses (rule 35: identical
inputs, byte-identical output).

### `explorer/index.json` and `explorer/{scope}/{CODE}.json` (added Batch 8a)

The data explorer's own payloads, one small file per indicator, written by
`scripts/export_explorer_payloads.py`. 87 files in all: 68 municipal, 19 national, 5.4 MB together,
median 46 KB, largest 394 KB (`MEDIAN_HOUSE_PRICE`, 14,748 observations).

**They are sharded out of the two files this site offers for download, not out of the database.**
`data/communes_history.csv` for the municipal scope and `data/belgian_macro_export.csv` for the
national one. That is the whole point of the batch: the roadmap line asks that the explorer "never
disagree with a download", and the only way to guarantee it is to make the page's figures a reshape
of the download itself rather than a second query against the same source. Note the municipal input
is `communes_history.csv`, the trimmed last-ten-years file that is actually committed and offered --
NOT `communes_history_full.csv`, which is gitignored and which no reader can obtain.
`tests/test_export_explorer_payloads.py` asserts the correspondence row for row in both directions:
no payload cell absent from the CSV, no CSV row missing from a payload.

Sharding is what makes the full dataset browsable at all. The municipal history is 178,128 rows and
35 MB; a browser fetches one indicator, so the median request is 46 KB.

```json
{
  "indicator_code": "MEDIAN_HOUSE_PRICE", "scope": "municipal",
  "names": {"en": "...", "fr": "...", "nl": "..."},
  "unit": "EUR", "decimals": 0, "direction": "contextual",
  "grade": "A", "source": "statbel", "updated": "2026-09-06",
  "periods": ["2010-Q1", "..."],
  "geographies": ["11001", "..."],
  "series": {"11001": {"2026-Q1": [445000.0, "P"]}}
}
```

A cell is `[value, status]`. The status letters are the canonical six -- `A` final, `P` provisional,
`R` revised, `E` estimate, `S` suppressed, `N` not applicable -- plus `derived` for a computed
figure, the same vocabulary `communes.html`'s `statusPill()` uses. A suppressed cell carries its
status and a null value, never a zero (rule 26).

`index.json` is the catalogue the page loads first: one entry per indicator carrying its names, unit,
decimals, direction, grade, source, scope, period list and geography list, so every filter on the
page can be populated without fetching a single payload.

**Batch 8a also changed a published download.** `scripts/export_canonical_csv.py` used to map only
`final` and `provisional` to letters and send every other status to an empty cell. Nine rows in
`belgian_macro_export.csv` -- all 2009 annual figures the database records as `revised` -- therefore
reached readers with no status at all. The exporter's own docstring said the shortcut existed only
because "the frontend has no dedicated visual for estimate/revised/suppressed/na yet"; this batch
builds that visual, so the mapping now covers all six.

### `metadata/micro_sections.json` (added Batch 7)

The micro.html layout, same premise as `metadata/national_sections.json`: a KPI row, a key-indicator
list, a province comparison, a household tile grid, a choropleth map, one history chart and the cards
the design draws that no series can fill. Built from `config/micro_sections.yaml` and checked against
the union of `national.json`'s codes and `aggregates.json`'s `be:country` codes -- the two files
micro.html can actually read from -- so a card can never point at a series neither provides.

### `metadata/sources.json`

One entry per data source, plus the grade vocabulary. ~5.5 KB, fetched once by a page and
referenced by id from `metadata/indicators.json`, which is why no source name is repeated in the
565 commune payloads.

Built from `config/sources/*.yaml`, **not** the `sources` table: three rows in that table were
written by `scripts/port_existing_indicators.py` with the agency copied into the name and
`catalog_ref = "docs/data_catalog.md (pending)"`, so reading the database would publish the weaker
copy of a licence notice.

Each entry carries `source_id`, a trilingual `label`, `agency`, `name`, `homepage`, a trilingual
`licence_note`, `cadence` and `catalog_ref`. **The notes are per source and are never composed into
one string** — Statbel and ONEM state commercial reuse, the federal police state only attribution,
and a combined "Sources: Statbel, ONEM, Police" line would claim a permission nobody granted.

`metadata/indicators.json` gains, per indicator: `grade` (A/B/C/D), `source` (null for a derived
indicator, which has none), `transform`, `derived_from`, `input_sources`, `inputs_updated` and
`updated`. See `docs/features/provenance.md` for what the grades mean and why a suppressed cell
carries no grade at all.

### `metadata/geographies.json`

Every currently-valid geography (622 rows measured today: country, regions, provinces,
arrondissements, 565 communes) with its trilingual name and its region/province/arrondissement
ancestry — the data Block K's autocomplete search and Block L's comparison picker both need before
either can be built, and neither should have to re-derive the ancestor walk
`export_communes_csv.py`'s `_ancestor_names()` already does correctly.

### `manifest.json`

```json
{
  "build_id": "<github.run_id or a local timestamp>",
  "git_commit": "<full SHA>",
  "build_date": "<UTC ISO timestamp>",
  "datasets": {
    "national": {"indicators": 35, "periods_max": 216},
    "communes": {"count": 565, "indicators": 14},
    "fiscal_income": {"rows": 44156, "years": "2005-2023"},
    "population": {"rows": 25532, "years": "2016-2026"}
  },
  "validation_status": "pass"
}
```

Row counts per dataset, not a single total: "which build produced this number" (the roadmap's own
framing, Block AD) needs to know which *dataset* changed, and a single combined total hides that.
`validation_status` is `pass`/`fail` from the same `scripts/validate_data.py` run already wired into
`daily_fetch.yml` — the manifest does not re-validate, it records what validation already decided.

## What is deliberately NOT built here

- **The `/local` pages that consume these payloads.** That is Block K, which does not exist yet. This
  block produces the data; Block K is a separate, later `[SPEC]`.
- **Per-language payload variants.** Names are already trilingual within one JSON file
  (`name: {en, fr, nl}`); there is no reason to fork the *data* payload by language when only the
  *interface* around it needs to be localized (Block X's job, not this one's).
- **A CDN or edge-cache layer.** GitHub Pages already compresses and caches; adding a second caching
  layer before there is a `/local` page to serve is solving a problem that does not exist yet.

## Where it runs

Wired into `daily_fetch.yml` and `manual_sources.yml`, after the existing bulk exports (so a payload
export failure never blocks the CSVs that already work) and after `validate_data.py` (so
`manifest.json`'s `validation_status` reflects a real check, not an assumption). Bulk CSV/JSON
exports are left completely untouched, per the roadmap's own instruction for this step.

## Tests

- A round-trip test: every value in a generated `communes/{nis}.json` must appear in
  `communes_history.csv` for that commune, and vice versa — the payload must not drop or invent a
  cell relative to the export it is sliced from.
- `indicators/{id}.json` must contain exactly the current communes (565), never a historical
  predecessor geo_id — the same current-vs-historical distinction `export_communes_csv.py` already
  enforces for its own snapshot.
- `metadata/geographies.json` round-trips the ancestor walk: every commune's stated region resolves
  to a real `region`-level entry in the same file.
- `manifest.json`'s row counts match `SELECT COUNT(*)` / CSV line counts at generation time — a
  manifest that lies about row counts is worse than no manifest.

## Open questions for the maintainer

- **Should `communes/{nis}.json` include the derived indicators that currently have no live
  `[BUILD]` consumer** (`regional_share`, blocked on Block L's aggregates per
  `derived_indicators.md`'s own gap note)? Proposed default: include only what
  `export_communes_history_csv.py` actually computes today, so this block never gets ahead of what
  Block G can honestly produce.
- **`indicators/{id}.json` for a derived indicator**: does "latest" mean the latest period the
  *underlying* raw series has, or the latest period the derived value is *computable* for (e.g.
  `POPULATION_CAGR_10Y` is null for any commune with under ten years of history)? Proposed: the
  latter — a listed-but-null latest value is indistinguishable from a computation bug, so a commune
  without a computable latest value should be absent from that file, consistent with the
  absent-not-null rule the rest of this spec follows.
- **File count at 200 indicators × 565 communes**: this design produces 565 + 200 + 1 + 2 = 768
  files today, growing only with commune count and indicator count, not their product — worth
  confirming GitHub Pages / Actions has no meaningful per-build file-count concern at that scale
  before Block K starts depending on it.
