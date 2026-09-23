# Feature: Communal additional IPP rate (SPF Finances, annual, automatic)

Status: implemented
Branch: feat/ns6-ipp-rate
Stacked on: #233 (feat/ns3-population-movement)

## Source

SPF Finances' communal additional personal-income-tax rate (taxe communale
additionnelle à l'impôt des personnes physiques / aanvullende gemeentebelasting
op de personenbelasting). One XLSX file per tax year (exercice d'imposition),
at a fixed URL pattern:
`https://fin.belgium.be/sites/default/files/media/documents/taux-taxe-communale-{year}.xlsx`.
New source, `config/sources/spf_finances.yaml`, `source_id: spf_finances`.
Reachable live from this pipeline's network context, so the sync is DAILY and
AUTOMATIC, wired into the Dagster asset graph and `make fetch` like the
bankruptcies and population-movement sources.

## Year discovery, not link discovery

Unlike Statbel's bankruptcies/population-movement landing pages, SPF Finances
has no page that links these files by href (checked 2026-09-23). So
`scripts/sync_ipp_rate.py` ITERATES tax years from 2024 up to (current year +
1) at the known URL pattern: a 404 on a year beyond what has been published
yet stops the iteration (expected — not yet released); a 404 on a year
already loaded in a previous successful run, or a response whose sheet/header
has drifted from the documented shape, fails the WHOLE run (CLAUDE.md rule
13) — never a partial load, never a cached or hard-coded fallback.

Measured 2026-09-23: 2024, 2025 and 2026 are live; 2018–2023 return 404 as
XLSX (PDF-only for those years — out of scope for this batch).

## The file

One sheet, `Liste communes`, header `('VILLE OU COMMUNE', 'Taux (%)')`,
checked by name, never position. Measured row counts: 581 for 2024 (the
pre-2025-merger commune count), 565 for 2025 and 2026. Rates are numeric,
0–9. **No NIS code anywhere in the file** — the only key is a commune name.

## Name resolution, not NIS resolution

`src/fetchers/spf_finances.py` resolves nothing: `geo_id` in its output rows
is the raw commune name exactly as published, and `period` is the tax year
exactly as published — the same adapter/sync split
`src/fetchers/bankruptcies.py` and `src/fetchers/population_movement.py` use,
for the same underlying reason (name resolution needs a live db connection
this layer does not have), but one step further: those two resolve a raw
*NIS code*; this one resolves a raw *name*, since the source has no code at
all.

`scripts/sync_ipp_rate.py` does the resolution in two steps: (1) name → NIS,
matched against `geographies.name_nl` / `geographies.name_fr` valid at
`f"{tax_year}-01-01"`, after accent/case/punctuation normalisation
(`_normalize_name`); (2) NIS → geo_id via the ordinary `resolve_geo(conn,
nis, tax_year)` (CLAUDE.md rule 3).

Measured 2026-09-23: normalised-name matching resolves every row in all
three years to exactly one commune, with **one exception** — `Saint-Nicolas`,
which is both the French name of Sint-Niklaas (NIS 46021) and the name of a
distinct commune in Liège province (NIS 62093). The file lists Sint-Niklaas
separately under its own Dutch name, so `Saint-Nicolas` in this file is
always the Liège commune. One explicit, verified override,
`sync_ipp_rate.NAME_OVERRIDES`, the same shape as
`src/fetchers/statbel.py:NAME_OVERRIDES` — no fuzzy fallback, ever. Any
*other* unmatched or ambiguous name — one the override does not cover —
fails the entire run; every row is checked before the run is either fully
committed or fully refused (`scripts/sync_bankruptcies.py`'s `unresolved`
pattern).

## Status and the zero rule

Every row is written `status: final` — the file publishes the year's settled
rate, no provisional marker anywhere in it. Knokke-Heist's 2026 rate is
`0.0`: a real published rate, not missing and not `na` (CLAUDE.md rule 26).
The adapter does not special-case it; the value passes straight through.

## The indicator

`MUN_IPP_ADDITIONAL_RATE` (percent, annual, `geo_levels: [municipal]`,
`preferred_direction: contextual`, `max_age_days: 730`, `display: null`).
**No aggregate is computed or computable**: the rate is neither an additive
total nor a ratio recomputable from an underlying sum this pipeline holds,
so province/region/Belgium figures are refused, never averaged. There is no
`is_additive` field in the indicator config schema (`docs/features/
indicator_config.schema.json`'s properties are fixed); `is_additive = 0`,
`aggregation_method = 'not_applicable'` are written directly into the
`indicators` reference row by `scripts/sync_ipp_rate.py`'s
`_ensure_reference_rows`, the same `scripts/sync_walstat.py` pattern.

**This rate is never combined with fiscal income or tax-amount figures** —
no yield-of-an-IPP-point calculation, no revenue estimate, no derived
indicator. The maintainer has not confirmed how a tax year maps to the
income year it taxes, so `period` is stored as the tax year exactly as
published and nothing further is asserted about it.

## Store

`config/stores.yaml`'s `ipp_rate` entry: `mode: in_db`, `source_id:
spf_finances`, one indicator, `reference_rows` pointing at
`scripts/sync_ipp_rate.py --reference-rows-only`. Single CSV (not
`one_csv_per_indicator` — one indicator, no split needed),
`data/ipp_rate_observations.csv`, 1,711 rows (581 + 565 + 565 communes across
the three tax years), ~240 KB, well under the 25 MB commit limit. The initial
committed data was produced by running the real sync against the live SPF
Finances files once and offloading — not left empty for the first daily run
to fill, the same precedent as bankruptcies/population-movement/ONEM/WalStat.

## Config schema change

`docs/features/source_config.schema.json`'s `adapter` enum gained
`"spf_finances"` — the only edit to that file, needed because the enum is
closed (`additionalProperties: false`) and every source's `adapter` value
must appear in it.

## Out of scope (this batch)

PDF-only years (2018–2023). Any combination with fiscal income or tax-amount
indicators. Commune-page surfacing (`config/local_sections.yaml`). Any
change to `src/analytics/`, `resolve_geo()`, the schema, or an existing
source/adapter.
