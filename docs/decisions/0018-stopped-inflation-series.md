# ADR 0018 — the Belgian inflation series stopped, and nothing noticed

Date: 2026-10-04
Status: **ACCEPTED — approved by the maintainer 2026-10-04**, option (b) plus the Europe repoint,
with the proposed indicator id confirmed, in his words: "Approve with that name (Recommended)".
Superseded the PROPOSED status of the same day.
Required by: CLAUDE.md rules 6, 8, 13, 19, 26, 28, 35. Sibling record:
`docs/decisions/0017-ameco-forecast-periods.md` (also ACCEPTED 2026-10-04). Page-side half of the
fix: `docs/features/site_clarity.md`, batch 1. Catalogue rows: `docs/data_catalog.md`, "Belgian
inflation from Eurostat — APPROVED by the maintainer 2026-10-04".

**Implementation happens in its own pull request, not in this one.** The branch that carries this
record changes documents only: no indicator config, no `fetch` string, no adapter and no store is
touched by it, and nothing has fetched `prc_hicp_minr` yet.

## Context

**Nothing failed.** The NBB froze the dataflow we query at December 2025 and continues the series in
a new one; Eurostat did the same with its dataset code. The site therefore shows Belgian inflation
as 2.2 % (December 2025) while both sources publish **4.2 % for August 2026** and a **4.6 % flash
estimate for September 2026**. The Europe panel's inflation is frozen at `2025-12` for all 36
geographies for the same reason.

**What we ask for.** `config/indicators/HICP.yaml:19` holds
`fetch.query: ',DF_HICP,1.0/M.BE.000000.2015.HCP.GROWTH_RATE?startPeriod=2010-01&dimensionAtObservation=AllDimensions'`
(verified by reading the file), appended to `config/sources/nbb.yaml`'s base URL. The base year 2015
is hard-coded inside the SDMX key. `unit: percent_yy`, `frequency: M`, no `max_age_days`, no
`transform`; `sdmx_code: HICP_INDEX` at line 34 is a misnomer — the series is a rate, not an index.

**What the fetch log says.** 229 HICP rows since 2026-03-01. 227 are `OK` with 192 rows and an empty
message, including every run from 2026-09-14 to 2026-10-03; the two `ERROR` rows are transient
(`RemoteDisconnected`, 19 and 26 April). `fetch_runs` id 3712 (2026-10-03T10:12:00Z) is `ok`,
HTTP 200, 192 read, 192 written; the canonical sync (id 3773) read 192 and wrote 0. Those 192 rows
cover 2010-01 to 2025-12 and have not changed for seven months.

**The dataflow really has stopped.** Against `https://nsidisseminate-stat.nbb.be/rest`:
`data/BE2,DF_HICP,1.0/M.BE.000000.2015.HCP.GROWTH_RATE?startPeriod=2010-01` returns 200 with 192
rows, last `…,2015,HCP,GROWTH_RATE,2025-12,2.17715,A`. The same flow with
`M.BE.000000..HCP.GROWTH_RATE` offers only `INDEX_BASE=2015`; with `…2025.HCP.GROWTH_RATE` it is
404 `NoRecordsFound`; `…/all?startPeriod=2026-01` is 404 `NoRecordsFound`. The whole flow ends at
2025-12.

**And NBB continues it elsewhere.** `dataflow/all/all/latest` lists 223 flows including
`BE2:DF_HICP_2025(1.0)` — "…(HICP): base period=2025", own structure `DS_HICP_2025`, annotated
`NonFinalDataflow`. `data/BE2,DF_HICP_2025,1.0/M.BE.000000.2025.HCP.GROWTH_RATE?startPeriod=2010-01`
returns 200 with **9 rows**: 2026-01 1.35424, 02 1.38157, 03 2.15655, 04 4.2119, 05 3.96418,
06 3.34435, 07 3.64536, 08 4.18822 (`A`), 09 4.63074 (`E`). Both the dataflow id and the
`INDEX_BASE` code changed. The new rate series starts at 2026-01 because its index starts at
2025-01. Old and new index differ by a constant factor (1.3549) across 2025 — a pure
re-referencing, not a revision. `NBBSource._parse` reads the new response unchanged, and `E` maps to
`estimate` via `src/fetchers/sdmx_status.py:27`.

**Eurostat did the same thing.** `prc_hicp_manr`'s label now ends "(1997-2025)", it was last updated
2026-02-06, its last period is 2025-12 = 2.2, and `sinceTimePeriod=2026-01` returns empty. The
catalogue puts it under "ECOICOP ver.1". Its successor is `prc_hicp_minr` ("ECOICOP ver.2", indices
and rates), updated 2026-10-02, covering 1996-01 to 2026-09. Four things changed: the dataset code;
the dimension `coicop` is now `coicop18`; the all-items code `CP00` is now `TOTAL`; and there is a
new `unit` dimension (`I25`, `I15`, `RCH_M`, `RCH_A`, `RCH_MV12MAVR`).
`…/prc_hicp_minr?unit=RCH_A&coicop18=TOTAL&geo=BE` gives 2025-12 = 2.2, 2026-08 = 4.2,
2026-09 = 4.6 (flag `e`). Against the old dataset, 11 of 348 months differ by 0.1 point, 6 of them
since 2008. `config/indicators/HICP_ANNUAL_RATE_EUROPE.yaml:18-24` still asks for `prc_hicp_manr`
with `coicop: CP00` (verified); `data/international/HICP_ANNUAL_RATE_EUROPE.csv` has Belgium at 216
rows ending 2025-12 = 2.2 `final`, vintage 2026-09-13, and all 36 geographies end at 2025-12.

**Where the stale figure shows, and how its date reads.** `public/data/national.json`'s `HICP` has
192 periods, last `2025-12` = 2.17715 `final`, `updated: 2026-09-05`, grade A, and no frequency or
freshness field (verified by reading the payload). It appears in `macro.html`'s KPI row
(`config/national_sections.yaml:21`, date printed at `macro.html:469`), in the Prices chapter as its
only series (`:180-182`, date at `:563`), in the Europe panel
(`public/data/europe/countries/HICP_ANNUAL_RATE_EUROPE.json`, `latest_period: 2025-12`, credited
"Eurostat (prc_hicp_manr), retrieved 2026-09-13"), in `home2.html` hero card 2 as a raw `2025-12`
with no source date (`:830-831`) and national card 3 (`:885-886`), and in `explorer.html` plus its
index row (`period_max: 2025-12`). `macro.html`'s sidebar prints the build date as "Last data
update" and "Validation passed" (`:49-52`, `:797-803`). "Retrieved 2026-09-05" is the day the row
entered the canonical table (`scripts/export_site_payloads.py:119-140`) — neither NBB's publication
date nor the last check on 3 October.

**Why no reader and no check saw it.** `staleness` is a `WARN` by design
(`src/validation/rules.py:525`) and its own comment at `:51-52` names HICP as "stuck on 2025-12".
Warnings reach the Actions summary (`.github/workflows/daily_fetch.yml:106-107`) and the daily PR
body, which auto-merges — PR #308 merged in 12 minutes. `orchestration/checks.py:123-124` ignores
warnings, so the page prints "Validation passed". Nothing tracked it: no issue, no
`known-risks.md` row. And the adapter could not have noticed, because the dead flow still answers
200 with 192 rows.

## Options

**(a) Repoint NBB to `DF_HICP_2025`.** A one-line edit of `config/indicators/HICP.yaml:19` returns
9 rows. The 192 old rows would survive only because the upsert never deletes
(`belgian_macro_db.py:187-201`) — so the joined series would exist at no source and no query would
reproduce it, which is rule 6 in a new costume. A rebuild would return 9 rows and trip
`row_collapse` (`src/validation/rules.py:452`). Doing it properly needs two queries per indicator;
the schema allows one (`docs/features/indicator_config.schema.json`, `fetch.query`), so it is a
schema change plus a fetch-loop change — ADR and approval (rule 19). Further against it: the new
flow is annotated `NonFinalDataflow`, and the NBB licence is still `licence: null  # TODO`
(`config/sources/nbb.yaml:6`, `docs/data_catalog.md:1139`).

**(b) A Belgium-only Eurostat indicator on `prc_hicp_minr`**, filters `unit: RCH_A`,
`coicop18: TOTAL`, `geo: BE`. Fits the existing schema and the existing adapter with no change to
either: running `EurostatSource._parse` in memory over the live response produced 225 rows, 2008-01
to 2026-09, last 4.6 `estimate`. One dataset carrying its own back-cast history, so there is no
splice and no invented joint series. Touches a new indicator YAML, `config/stores.yaml` (the
international list, near line 265), `config/national_sections.yaml:21,158,182,204`, a catalogue row
(rule 8) and a spec. Against it: two Belgian inflation series live in the store unless the NBB one
leaves the pages, and four months since 2010 read 0.1 point differently from the NBB series.

**(c) Let the existing multi-country row onto Belgian pages.** Fixes nothing as it stands — that
series is equally frozen — and `tests/test_international_stays_off_belgian_pages.py:28,76-111`
forbids it. That guard followed a real leak of 755 duplicate rows. Needs its own ADR. Not
recommended.

**Required in every case:** repoint `HICP_ANNUAL_RATE_EUROPE` to `prc_hicp_minr`. That is a query
change and so rule 19 applies. A dry run returned 9,513 rows across 46 geographies, none outside the
allowlist and exclusion lists. Swapping only the dataset code fails loudly, as it should: HTTP 400
on the old `coicop` filter, and a `FetchError` on the unpinned `unit` dimension.

## Recommendation

**(b) plus the Europe repoint, under this one record.**

1. **A new Belgian indicator from Eurostat `prc_hicp_minr`** — a new id, never a re-sourcing of
   `HICP`. Re-sourcing would lay Eurostat vintages over NBB's inside the same
   `(indicator_id, geo_id, period, vintage)` key, which is a data-integrity problem, not a config
   change. Proposed id `HICP_EUROSTAT_BE`; the id is part of the `observations` primary key and
   appears in published downloads forever, so **the maintainer confirms the name before anything is
   built**.
2. **`HICP_ANNUAL_RATE_EUROPE` repointed** to `prc_hicp_minr` with `coicop18: TOTAL` and
   `unit: RCH_A` pinned explicitly, so an unpinned dimension fails loudly rather than returning a
   different measure.
3. **The NBB `HICP` series is kept, not deleted.** December 2025 is a real, final, correct figure —
   not a missing one (rule 26). It keeps its rows, its chart, its `explorer.html` row and its
   download, labelled with its last publication date and the fact that the source stopped it. It
   leaves the "current inflation" slots.
4. **`DF_HICP_2025` is not queried**, so no joined series is constructed and the `NonFinalDataflow`
   annotation and the unresolved NBB licence stay out of the decision.
5. **The NBB licence TODO should be closed separately.** `config/sources/nbb.yaml:6` has said
   `licence: null  # TODO` through production use. It does not block this record, but it is a real
   exposure.

## What exactly was approved, 2026-10-04

The maintainer chose **option (b) plus the Europe repoint**, and confirmed the proposed id, verbatim:
"Approve with that name (Recommended)". Concretely, he approved:

1. **A new Belgian indicator, `HICP_EUROSTAT_BE`**, from Eurostat `prc_hicp_minr` with all three
   dimensions pinned (`unit=RCH_A`, `coicop18=TOTAL`, `geo=BE`). **The name is settled**: he
   confirmed it rather than renaming it, which matters because an indicator id is part of the
   `observations` primary key and stays in published downloads permanently.
2. **A new id, never a re-sourcing of `HICP`** — re-sourcing would lay Eurostat vintages over NBB's
   inside the same `(indicator_id, geo_id, period, vintage)` key.
3. **`HICP_ANNUAL_RATE_EUROPE` repointed** to `prc_hicp_minr` with `coicop18: TOTAL` and
   `unit: RCH_A` pinned explicitly, so an unpinned dimension fails loudly instead of returning a
   different measure.
4. **The NBB `HICP` series is kept, not deleted, and marked as stopped.** December 2025 is a real,
   final, correct figure, not a missing one (rule 26). It keeps its rows, its chart, its
   `explorer.html` row and its download, labelled with its last publication date and the fact that
   the source stopped it, and it leaves the "current inflation" slots.
5. **`DF_HICP_2025` is not queried**, so no joined series is constructed at a base year no source
   publishes, and the `NonFinalDataflow` annotation stays out of the pipeline.
6. **Both catalogue rows are approved** and now sit under "Belgian inflation from Eurostat —
   APPROVED by the maintainer 2026-10-04" in `docs/data_catalog.md` (rule 8). No new data source and
   no new licence decision: the `eurostat` source was approved on 2026-09-13.

The NBB licence `TODO` (`config/sources/nbb.yaml:6`) was **not** part of this decision and is still
open.

## What the approval does not change yet

Approval is not implementation. **This record's own pull request changes documents only.** Until the
separate implementation PR merges, all of the following are still true:

- `config/indicators/HICP.yaml` — `fetch.query` untouched; it keeps asking the frozen dataflow and
  keeps returning the same 192 rows.
- `config/indicators/HICP_ANNUAL_RATE_EUROPE.yaml` — still `prc_hicp_manr`, `coicop: CP00`, so the
  Europe panel is still frozen at 2025-12 for all 36 geographies.
- `src/fetchers/nbb.py`, `src/fetchers/eurostat.py`, `config/stores.yaml` — unchanged.
- No `HICP_EUROSTAT_BE` config exists, and **no row has been fetched from `prc_hicp_minr`**.
- The site still shows 2.2 % as Belgian inflation until batch 1 lands.

**What happens meanwhile.** `docs/features/site_clarity.md` batch 1 takes the December 2025 figure
out of the current-inflation slots and labels it with its own date and the fact that the source
stopped. It does that by exporting two deterministic keys from config — `stale_after` (the period
end plus the indicator's staleness allowance: **2026-04-03** for HICP) and `superseded_by` — so the
page only compares dates and the output stays byte-identical on an unchanged input (rules 28, 35).
A reader then sees an honest stale figure instead of a wrong current one. They still do not see
4.2 %.

## Consequences

- Belgian inflation on the site becomes current again, from Eurostat rather than NBB.
- Two Belgian inflation series exist in the store. The NBB one is published-but-stopped; the Eurostat
  one is current. Every page slot must name which it shows, and the two must never be concatenated.
- Four months since 2010 will read 0.1 point differently from the figure the site showed before.
  Small, and the kind of difference a reader can spot and ask about, so it belongs in the indicator's
  own `definition`.
- The Europe panel gains 2026 for all its geographies and gains Eurostat's own flag handling, so a
  flash estimate arrives as `estimate`, not as `final`.
- Eurostat becomes the source of a headline Belgian figure that NBB used to provide. That is a
  product change, not only a technical one.
- The same class of failure will recur: a source retires a dataset code and the old one keeps
  answering 200. The durable protection is not this record — it is making a named, known-stale
  series a build failure instead of a warning, which belongs in the validation layer and is not part
  of issue #312.

## Tests that would guard it

- An adapter test against a recorded `prc_hicp_minr` response: 2026-09 arrives as `estimate` from
  the `e` flag, 2026-08 as `final`, and the all-items dimension is `coicop18: TOTAL`.
- A loud-failure test: the old `coicop: CP00` filter raises, and an unpinned `unit` raises — neither
  silently returns a different measure (rule 13).
- A store test: no NBB `HICP` row exists for a period after 2025-12, and no Eurostat
  `HICP_EUROSTAT_BE` row is written under the `HICP` id.
- A payload test: `stale_after` and `superseded_by` are derived from config and identical across two
  export runs (rule 35).
- A page test: no slot labelled "current" shows a period whose end is past that series'
  `stale_after`; the stopped series still has a tile, a chart row and a download link (rule 26).
- `tests/test_international_stays_off_belgian_pages.py` stays green unchanged — the new Belgian
  series is a Belgian indicator, not the multi-country row.
- The `make all` byte-identical rebuild tests stay green.

## Also noticed, not part of this decision

- **`src/fetchers/nbb.py:46-51` silently skips an unparseable row.** Against rule 13. Recorded in
  `docs/implementation/known-risks.md`; not fixed here.
- **`config/indicators/HICP.yaml:34` has `sdmx_code: HICP_INDEX` on a growth-rate series.** A
  comment-level misnomer; corrected as display text in `docs/features/site_clarity.md` batch 1.
- **Two gitignored SQLite sidecar files** (`data/belgian_macro.db-shm`, `-wal`) were left in the
  read-only audit copy by the research queries. No tracked file changed.
