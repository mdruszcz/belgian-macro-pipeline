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
  communes/{nis_code}.json      -- one commune, every indicator that has a value for it, full history
  indicators/{indicator_id}.json -- one indicator, every current commune, LATEST value only
  metadata/indicators.json      -- already exists (Block B): categories + indicator display metadata
  metadata/geographies.json     -- NEW: commune list (id, NIS, trilingual name, region/province/
                                    arrondissement) for Block K's search/autocomplete and Block L's
                                    comparison picker
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
