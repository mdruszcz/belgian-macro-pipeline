# Feature: Where movers and buyers come from — commune-to-commune flows

Status: **design only, nothing implemented**. Block NS [SPEC], maintainer's request 2026-09-24.
Catalogue: the SPF datasets are already approved (docs/data_catalog.md, "Waves 4–5 — SPF
Finances, patrimony open data (AGDP)", which names .16/.19/.20/.21/.22/.23). No new row needed.

## What we measured first (2026-09-24, live)

The `9bb31275-…` feed is the **service** catalogue, not a dataset feed; each dataset's own ATOM
feed is its entry's `alternate` link. Buyers' origin, natural persons (52.01.21) is
`b90b50be-9dfc-11f0-99e9-00be432db085`, ten years 2016–2025; owners' origin, natural persons
(52.01.16) is `d22abd5e-a0c2-11ee-ba9e-0050569393d1`, sixteen years 2011–2026. The existing
`parse_atom_feed()` reads both unchanged, but with **different period modes**: buyers' origin is
`AY` (a 31-December calendar-year flow), owners' origin is `A` (a 1-January stock snapshot).
They are a flow and a stock and must never be added together.

The matrix is one row per destination commune **× parcel nature**, one column per origin
(`BuyerFrom<NIS>` / `OwnerFrom<NIS>`). 52.01.21 for 2023: 835 columns, 156,870 rows, 581
destinations × 270 parcel natures, `ParcelNature=TOTAL` present for every destination.
52.01.16 for 2026: 819 columns, 565 destinations. The Municipality member is ~266 MB raw but
~1.8 MB compressed, and the existing ranged-zip reader pulls it in ~5 seconds without touching
the 9.2 GB StatisticalUnit member.

**The cell is not a count.** Values are fractional — Antwerpen's 2023 TOTAL row has 89.30 from
one origin, 4.63 from another. A parcel bought by two co-buyers living in different communes
contributes a share to each. So a cell is *parcels weighted by each buyer's share*, and the
honest label is "parcels bought, counted by share of the buyers' home communes", not "number of
buyers" and not "number of sales". Origin cells sum to slightly less than the row's own
`ParcelsNumber` (2023 national TOTAL: 269,926.01 against 279,171, 96.7%); the gap is the part
bought by legal persons and unattributable shares, and it is why we publish coverage.

Suppression: **no blank cells at all** (481,649 cells checked, 0 blank). 94% are an explicit
`0`. The one destination whose row is entirely zero is Herstappe, with `ParcelsNumber=0` — a
genuine "nothing was sold here", not a missing value. There is no 1–4 masking tier here.

Distribution, 52.01.21 2023 at `ParcelNature=TOTAL`: a median commune has 39 origins with a
non-zero value (min 0, max 330); the top 5 origins capture a median 71.7% of the total (worst
case 20%); buyers from the commune itself are a median 43.3% (range 10.9%–74.8%). So "top 5 plus
buckets" is faithful for a typical commune and visibly incomplete for Brussels-area ones — the
payload therefore always carries the residual, never just the top 5.

A `OriginTable.csv` (830 rows) ships inside the buyers' zip and is the origin lookup: 581 NIS
codes plus **248 non-NIS codes** for countries (`101`…`694`, with ISO codes), plus `000`
"Indéterminé" and `999` "Inconnu". It is **latin-1, not UTF-8** — the existing reader's
`utf-8-sig` assumption crashes on it. The owners' zip has no `OriginTable.csv`.

**Statbel true migration flows: not available as open data.** Statbel publishes internal
migration only as per-commune totals, which this pipeline already ingests
(`INTERNAL_MIGRATION_IN` / `_OUT` / `_NET`, landed 2026-09-24). No origin × destination
municipality matrix appears on the open-data portal or the migration theme page, whose only
download is international migration 1948–2025. Statbel's open data is CC BY 4.0, so if a flow
matrix exists on request the licence is fine; it would still need its own catalogue row.
Consequence: **we cannot say where people move from. We can only say where property buyers
live.** The page copy must not blur those.

## 1. The product

**First release, one block, commune pages only: "Where buyers of property here live."** For the
selected commune and latest year: the share bought by people living in the commune itself, then
the top 5 named origin communes with their values, then buckets — rest of the same
arrondissement, rest of the same region, other regions, abroad — and a residual line "bought by
companies or share not attributable". Every figure carries the "parcels by buyer share" wording
above, the reference year, and coverage.

Later, in order: the same block for owners (a stock, separate block, separate wording); a
selected-commune choropleth on the existing map shading every commune by how much it sends to
the selected one; the reverse direction ("where people who buy here also buy"); arrondissement
pages. **No flow arrows** in any release — 39 median origins per commune makes an arrow map
unreadable, and it would need a second map implementation (rule 29 forbids one).

## 2. Data rules

Published cells only. Named origins and buckets are **sums of published cells**, which rule 6
allows. Every **share** (own-commune %, % from outside, bucket %) is a derived statistic, and
the bucket definitions encode geography judgement, so this needs **one ADR before any code**:
`docs/decisions/0013-commune-flow-shares-and-buckets.md`, outline — the denominator is the row's
summed origin cells, not `ParcelsNumber` (the two differ by 3.3% nationally); the five buckets
and their exact membership rule; that coverage = summed cells ÷ `ParcelsNumber` and is published
beside every share; suppression below 90% coverage per the aggregation rule; and that no share
is ever averaged across communes. A second ADR is needed only if we later publish an aggregate.

Five states (rule 26): an explicit `0` is a measured zero and renders "0", never "–"; a
destination absent from a year's file is **not applicable** for that year (it did not exist);
`000`/`999` origins are **unknown**, shown in the residual, never folded into "abroad"; coverage
below 90% is **suppressed**.

Period semantics: buyers' origin is a calendar-year flow labelled `YYYY`; owners' origin is a
1-January stock. Never compared like-for-like.

Geography: **both axes** resolve through `resolve_geo(nis, period)` at that period (rules 3/25).
This is the sharpest risk, and it is measured: 2023 buyers has 581 destinations, 2026 owners has
565. Pre-2025-merger communes appear as both origin and destination in older years. A merged
commune's history must sum its predecessors on **both** axes, and an origin that merged must map
forward or the top-5 list silently loses it. Unresolvable NIS fails loudly (rule 13).

Aggregation: a province's **inflow total** is a defensible coverage-gated sum of its communes'
cells. Any share at province level is recomputed from those sums, never averaged over communes.
"Top origin commune" for a province is refused — neither additive nor a recomputable ratio.

## 3. Storage

Not the observations table: its PK `(indicator_id, geo_id, period, vintage)` has no origin
dimension, and rule 18 forbids reshaping the canonical schema for this. So: a **separate
committed flow store**, one JSON file per dataset per year (PR 1, as built:
`data/flows/buyer_origin_<year>.json`, `src/flows/store.py`) — a document per destination
carrying its denominator, coverage, the top 8 named origins and the SIX bucket values
(ADR 0013 decision 4 added `origin_unknown` as its own bucket, so five buckets in this
paragraph's original draft is superseded by the ADR's six), not a flat long-format CSV as
first sketched here. JSON was chosen over CSV because the ADR's own per-destination shape
(a denominator, a coverage figure, 8 named rows and 6 bucket rows, several of them carrying
both an unrounded value and a rounded share) nests naturally and needs no synthetic
`origin_key` enum to distinguish a named origin from a bucket row. Deterministic key
ordering (`sort_keys=True`) and destinations sorted by `dest_geo_id` keep two runs over the
same real input byte-identical (rule 35) except the provenance `fetched_at` timestamp, which
legitimately differs per run. Measured on the real 2025 file, all 565 destinations: **0.96 MB**
(the ADR's example bucket count grew by one since this section's 0.12 MB CSV estimate, and JSON
carries more punctuation than long-format CSV) — still far below rule 12's 25 MB ceiling. The
full ~259 MB Municipality-wide CSV (and the 266 MB StatisticalUnit member this dataset never
reads) is **never committed**: read via the ranged zip reader, trimmed, discarded.

Public payload: `public/data/flows/<dataset>/<nis>.json` — one small static file per commune,
fetched on demand by the block, GitHub-Pages compatible (rule 30). Source, unit, period and
freshness come from `public/data/metadata/**` (rule 28), never typed into the file.

Daily refresh: a new `scripts/sync_commune_flows.py` reusing the AGDP incremental ATOM pattern —
compare each version's `length` against what we hold, re-read only what changed. Both feeds are
on `opendata.fin.belgium.be`, which GitHub runners can reach, so unlike the Statbel workbook
this **can** stay in the daily run.

## 4. Where it plugs in

A new typed block in the shared registry. Dataset ids, bucket ids and indicator ids live in
`config/` and in the block's binding, never in the generic renderer (rule 24). The later map
release reuses `assets/commune_map.js` with a supplied choropleth series; no second map engine
(rule 29). No framework on public pages (rule 17). No hand-typed figure anywhere — the numbers
in this spec came from the real files, and the block's fixtures must too (rule 36).

## 5. Batch plan

**PR 1 — ADR 0013 plus the buyers' reader.** The ADR merges first. Then a
`FlowDatasetConfig` beside `AgdpDatasetConfig`, the latin-1 `OriginTable.csv` parser, the
trim-to-top-8-plus-buckets step, the store, and `sync_commune_flows.py` for 52.01.21 only.
Acceptance: the store rebuilds byte-identically twice (rule 35); the 2023 national TOTAL row
reproduces 269,926.01 against 279,171 parcels; a bad NIS on either axis raises. Tests: an ATOM
fixture for both `A` and `AY`; a hand-computed bucket and coverage on a small fixture; a
latin-1 round-trip on a country name; a merged-commune case on both axes; a rule-26 case for
Herstappe's real zero.

**PR 2 — the payload and the block.** Exporter writing `public/data/flows/**`, plus the commune
block. Acceptance: byte-identical rebuild; every share on screen traceable to published cells;
Herstappe renders "0" not "–"; coverage below 90% suppresses. Tests: exporter determinism, a
golden payload for one commune, a renderer test for each of the five states.

**PR 3 — owners (52.01.16) through the same path**, with its own stock wording. Acceptance: no
code path can add a stock to a flow.

PR 1 and 2 are the first valuable release; PR 3 is optional. Given the ADR, the fractional
cells and the two-axis geography, PR 1 and PR 2 each warrant an independent audit on data
integrity and geography.

## Decisions only the maintainer can take

1. **Is "parcels bought, counted by share of the buyers' home communes" a figure you want to
   publish at all?** It is not a headcount of buyers and a reader may assume it is.
   *Recommendation: yes, but label it exactly that way and never call it "buyers".* If you'd
   rather not carry that explanatory burden, we drop the whole block and lose nothing else.
2. **Ask Statbel for real migration flows?** A request to `demos@economie.fgov.be` for internal
   migration by origin and destination commune. *Recommendation: yes, send it now — it is the
   figure visitors actually want, and if it arrives it would need its own catalogue row and
   approval.*
3. **Ten years of buyers, or just the latest?** *Recommendation: latest year only for the first
   release. Backfill is cheap to store but every extra year is another merger boundary to get
   right.*
