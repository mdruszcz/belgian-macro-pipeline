# Feature: Europe panel — NUTS 2 choropleth (GDP per capita PPS, unemployment, population)

Status: **implemented, batch B2** (2026-09-14) — GDP per capita PPS and population loaded and
published; unemployment rate configured but blocked, pending a maintainer decision, see
"Measured at load, 2026-09-14" below. PR: #174.
Issue: none yet (this document is what the maintainer approved before one is opened)
Branch: feat/europe-nuts2-spec (B1, spec) → feat/europe-nuts2-pipeline (B2, implementation)

## Measured at load, 2026-09-14 (batch B2)

Real sync against live Eurostat data, the committed 2024 Nuts2json geometry, and the real
published payloads — see the batch's PR body for the full run log; the headline numbers:

| | GDP per capita, PPS | Unemployment rate | Population |
|---|---|---|---|
| Dataset | `nama_10r_2gdp` | `lfst_r_lfu3rt` | `demo_r_pjanaggr3` |
| Fetch time / response size | 0.25 s / 164,149 B | 0.24 s / 191,129 B | 0.40 s / 960,975 B |
| Loaded? | **Yes** | **No — blocked** | **Yes** |
| Rows written | 6,896 (285 regions × up to 25 years, 2000–2024) | 0 | 8,326 (314 regions × up to 36 years, 1990–2025) |
| Regions with a value, latest year | 276 in 2024 (146 provisional / 96 final / 34 estimate — matches the B1 coverage report's own flag census exactly) | — | 295 in 2025 (260 final / 27 provisional / 8 estimate) |
| Committed CSV | `data/nuts2/GDP_PC_PPS_NUTS2.csv`, 947,284 B | none | `data/nuts2/POPULATION_NUTS2.csv`, 1,156,234 B |

**Why unemployment is still blocked, and how population got unblocked.** The compound-`OBS_FLAG`
adapter fix (#168, merged) and the `u` → `estimate` mapping (confirmed by the maintainer
2026-09-14, `docs/decisions/0010-eurostat-compound-observation-flags.md`) resolved the gap the B1
coverage report found. Running the real sync for this batch found a **different, previously
unseen** gap: a genuine minority of cells carry an `OBS_FLAG` with **no value published at
all** — `lfst_r_lfu3rt` has 149 such cells (`u`: 116, `bu`: 33; e.g. `geo=DE22, period=2020,
flag=bu`), `demo_r_pjanaggr3` has 1 (`geo=PL912, period=2010, flag=b`). `EurostatSource._parse`
refuses these outright: the resolved canonical status (`estimate`/`final`) is not one of the two
statuses (`suppressed`/`na`) the `observations` table's own CHECK constraint allows to pair with
`value = NULL`.

`PL912` turned out to be a 5-character NUTS 3 code — not a NUTS 2 region this loader ever wanted
in the first place. `EurostatSource._parse` (`src/fetchers/eurostat.py`) gained an optional
`geo_filter` parameter: a geo code it rejects is skipped entirely, for every period, before its
flag/value are ever read — so a bad cell for a geography nobody asked for cannot block a fetch
nobody asked it not to. `scripts/sync_nuts2.py` passes `geo_filter=is_nuts2_code`, which filters
`PL912` out and unblocks `POPULATION_NUTS2` cleanly, without touching the `u`/flag-status mapping
at all. `geo_filter=None` (every pre-existing caller — the five country-level pilot indicators,
the eight single-country ones) preserves the exact previous behaviour, proved by the existing
fixture-replay tests passing unchanged plus new unit tests in `tests/test_eurostat_source.py`.
`lfst_r_lfu3rt`'s 149 bad cells include genuine 4-character NUTS 2 codes (`DE22` among them), so
the same filter does not help unemployment — those cells ARE the geographies this fetch wants,
and are still validated, and still refuse. How to label them is the maintainer's decision,
explicitly deferred, not attempted here. `UNEMPLOYMENT_RATE_NUTS2` is fully configured
(`config/indicators/UNEMPLOYMENT_RATE_NUTS2.yaml`, approved by the maintainer) and its
`public/data/europe/nuts2/UNEMPLOYMENT_RATE_NUTS2.json` payload exists with `"status": "blocked"`
and a `blocked_reason`, never silently omitted. See ADR 0009's 2026-09-14 amendment, item 8, for
the full mechanism.

**Geography.** `config/geography/nuts2.csv`: 320 regions, built from the union of all three
datasets' own `geo` dimension listings (real Eurostat codes + labels, not hand-typed), licence-
filtered by 2-letter country prefix against `international.csv`/`international_excluded.csv` —
45 codes dropped (all `UK*`), 0 unresolved after excluding the 4 known non-region aggregate codes
(`EA20`, `EA21`, `EFTA`, `EU28`) and 20 pseudo-region codes (`*ZZ`/`*XX`). **Brussels' `BE10` needed
no alias**: it is a real code with real observations in all three live responses — see ADR 0009's
2026-09-14 amendment, which supersedes the original alias proposal once this was found. All 11
Belgian NUTS 2 codes (`BE10`, `BE21`–`BE25`, `BE31`–`BE35`) cross-reference their real
`geographies.csv` row via `belgian_geo_id` (read from that table's `nuts` column for the 10
provinces, and by `geo_id` for Brussels, never hand-typed) but resolve to their own, separate
`:nuts2` geo_id for every observation.

**`FRY1`–`FRY5`/`PT20`/`PT30` (decision 8, resolved):** Nuts2json's own README ("Overseas
territories – map insets" section, confirmed 2026-09-14) publishes these as separate per-territory
files at `.../<YEAR>/<GEO>/<PROJECTION>/<SCALE>/<LEVEL>.json` (`GEO` = `GP`, `MQ`, `GF`, `RE`,
`YT`, `PT20`, `PT30`) — not in the level-2 continental file this batch committed. Not fetched
(out of scope, B3 builds the map); their values are published under `no_outline` with this reason
(`KNOWN_NO_OUTLINE` in `scripts/export_europe_nuts2.py`). The exporter fails loudly on any *other*
data-vs-geometry mismatch — verified against GDP: none occur once the check is based on the real
committed CSV rather than the full `nuts2.csv` catalogue (some catalogued codes, e.g. Greece's
pre-2016 `EL11`–`EL25`, appear in `nama_10r_2gdp`'s `geo` dimension *labels* but carry zero actual
GDP observations, so they never reach this check at all).

Unblocking population surfaced a SECOND, genuinely new no-outline category
(`SUPERSEDED_NUTS_VINTAGE_NO_OUTLINE`, 19 codes): `demo_r_pjanaggr3`'s `since: "1990"` fetch
reaches back far enough that some codes' committed rows stop at a real transition year while a
successor code's rows start around it — checked against the working database, not assumed (e.g.
`NL31`/`NL33` run through 2023, `NL35`/`NL36` — the Dutch reclassification this document's own B1
coverage report already found — run from 2014 onward; `PT16`–`PT18` run through 2023, `PT19`/
`PT1A`–`PT1D` — the Portuguese reclassification B1 also already found — run from 2014). Unlike
FRY/PT's insets, the 2024 geometry has no outline for these codes under ANY URL: the region no
longer exists under that exact code. See ADR 0009's 2026-09-14 amendment, item 7, for the full
evidence and code list.

**Geometry.** `public/data/geo/nuts2/2024/2.json`, downloaded unchanged, 574,331 bytes (matches
the B1 report exactly), sha256 in `public/data/geo/nuts2/ATTRIBUTION.md`. 292 regions; 291 pass
the licence filter, 1 (`XK00`, Kosovo) is licence-excluded and listed in every payload's
`excluded_by_licence`.

**Belgian pages:** every published Belgian page/payload byte (`public/data/national.json`,
`aggregates.json`, `communes/*`, `indicators/*`, and dashboard.html's rendered rows) is
unchanged — `tests/test_nuts2_stays_off_belgian_pages.py` proves it behaviourally (a real
exporter run, hashed before/after, plus `is_canonical_eligible()` returning `False` for all three
NUTS 2 indicators). **One shared, cross-cutting file DID change on purpose:**
`data/metadata/indicators.json` (regenerated via `python -m src.exporters.metadata`, 87 → 90
indicator entries) — it is the ONE lookup table every configured indicator gets a row in,
Belgian and non-Belgian alike, and always has been (the five country-level pilot indicators and
the eight single-country Eurostat ones are already in it). `dashboard.html` only renders a row
for a code that also appears in its own `DATA` object, populated from the Belgian canonical CSV
export (`belgian_macro_export.csv`) — never from this metadata file directly — so the three new
entries (`display: null`, like the five pilot indicators before them) sit in the lookup table
unused and unrendered, exactly like those five already do.

This batch produced no code that touches `config/indicators/*.yaml`, `config/geography/*`,
`config/stores.yaml`, adapter code, or any file under `data/` / `public/data/`. It is the
coverage report, this spec, a draft ADR and (now approved) catalogue rows — all measured today
(2026-09-14, ~15:45–15:53 UTC) with `scripts/report_nuts2_coverage.py`, a read-only probe.
Nothing downstream starts until the maintainer approves the decisions listed at the end.

## Problem

The Europe panel needs a NUTS 2 (EU statistical-region) choropleth for three indicators — GDP
per capita in purchasing power standards, unemployment rate, population — with a year selector
(latest year by default, same year for every region at once), a visible missing-data state, and
source/unit/vintage shown on the map. Nothing today fetches, stores or publishes anything at
NUTS 2 granularity; the international pilot (PR #150) only reaches national level.

## Goal

A spec the maintainer can approve, backed by measured facts, not guesses: exact dataset
dimensions and codes, exact region counts after the same licence filter the pilot already
applies, one recommended geometry vintage, a verified `eurostat-map` build that can run with
zero runtime requests to Eurostat or GitHub, and catalogue rows in PROPOSED state.

## Non-goals

No UI, no fetch code, no config changes, no vendored library, no new adapter capability. No
decision here is self-executing — every one of them is listed under "Decisions needed from the
maintainer" and nothing in a later batch starts on an unapproved one.

## Coverage — measured 2026-09-14, `scripts/report_nuts2_coverage.py`

All three fetches went to `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/`
with `format=JSON&lang=EN`, JSON-stat 2.0, no pagination (same transport ADR 0008 already
verified). Dimension codes below are read from each response's own `dimension` block, not
assumed. "NUTS 2 real" excludes two kinds of pseudo-region Eurostat mixes into the same `geo`
list: `ZZ`-suffixed "extra-regio" codes (activity not attributable to any region) and, only in
the population dataset, `XX`-suffixed "Not regionalised/Unknown NUTS 2" codes (label read
verbatim from the response) — neither is a real, mappable area.

| | GDP per capita, PPS | Unemployment rate | Population |
|---|---|---|---|
| Dataset | `nama_10r_2gdp` | `lfst_r_lfu3rt` | `demo_r_pjanaggr3` |
| Filters used | `unit=PPS_EU27_2020_HAB` | `isced11=TOTAL&sex=T&age=Y15-74&unit=PC` | `sex=T&age=TOTAL&unit=NR` |
| Unit, confirmed | Purchasing power standard (PPS, EU27 from 2020), **per inhabitant** — NOT `PPS_HAB_EU27_2020` ("in % of the EU27 average"), a different series entirely | Percentage | Number |
| Frequency | Annual | Annual | Annual |
| Years at source | 2000–2024 (25) | 1999–2025 (27) | 1990–2025 (36) |
| `geo` total (all NUTS levels) | 429 | 512 | 2195 |
| NUTS 2-shaped codes | 285 | 352 | 361 |
| … of which pseudo-region | 0 | 0 | 4 (`ALXX`, `FRXX`, `HUXX`, `MKXX`) |
| **NUTS 2 real regions** | **285** | **352** | **357** |
| Latest year | 2024 | 2025 | 2025 |
| Regions with a value, latest year | 276 | 290 | 296 |
| Response size / fetch time | 164,149 B / 0.25 s | 191,129 B / 0.25 s | 960,975 B / 0.39 s |
| Production adapter (`EurostatSource._parse`) | **succeeds** | **refuses** — see below | **refuses** — see below |

**GDP status-flag mix, last 3 years (canonical, via the real adapter):** 2022 final 200 /
provisional 78 / estimate 1; 2023 final 174 / provisional 101 / estimate 1; 2024 provisional 146
/ final 96 / estimate 34 — the latest year is mostly provisional, which the panel's "latest year
by default" needs to show as `provisional`, not silently as `final`.

### The adapter gap: compound OBS_FLAG values (new finding, not in ADR 0008's scope)

`EurostatSource._parse` (`src/fetchers/eurostat.py`) refuses any status flag not in its
`FLAG_STATUS` table, by design (CLAUDE.md rule 13). Both regional datasets carry flags that
table has never seen, because it was built from national-accounts series that don't compound
flags:

- **Unemployment (`lfst_r_lfu3rt`):** raw flag census over the whole series —
  `b`: 2008, `u`: 361, `d`: 226, `bu`: 117, `bd`: 106, `du`: 6, `bdu`: 4. Plain `u` ("unreliable",
  a single letter) is *also* not in `FLAG_STATUS` today — this is not only a compound-flag gap.
- **Population (`demo_r_pjanaggr3`):** `be`: 13, `e`: 1196, `b`: 1370, `bep`: 3, `ep`: 199,
  `p`: 631. Here the single-letter flags (`b`, `e`, `p`) already map fine; only the compounds
  (`be`, `bep`, `ep`) fail.

The probe script never calls the adapter's `.fetch()`, so this refusal only shows up in a
report, not a crash — but the real daily pipeline would crash exactly like this the first day
it tried either dataset. **This is a required adapter change before either dataset can be
loaded**, out of scope for this batch (CLAUDE.md rule 19 excludes adapter changes here) and not
attempted. The fix is straightforward in shape — split a compound flag into its letters and
resolve to the "worst" recognized status, plus add `u` → some status (the maintainer should say
which; Eurostat's codelist calls it "unreliable", which is closest to `estimate` but is a
judgment call, not a mechanical one) — but it is real adapter work, and a new unit test per
compound combination actually observed, not assumed.

### Licence filter (ADR 0008's rule, at NUTS 2 granularity by country prefix)

Applying `config/geography/international.csv` (40 allowlisted country prefixes: EU27 + 4 EFTA +
9 candidates) and `international_excluded.csv` (UK, XK, US, JP, + superseded aggregates) by the
NUTS 2 code's 2-letter prefix:

| | GDP per capita, PPS | Unemployment rate | Population |
|---|---|---|---|
| Kept (allowlisted) | 285 | 306 | 314 |
| Dropped by licence | 0 | 44 — all `UK*` | 41 — 40 `UK*` + `EU28` |
| **Unresolved (neither list)** | 0 | 1 — `EA21` | 1 — `EFTA` |

`EA21` and `EFTA` are Eurostat aggregate codes that happen to be 4 characters and pass the
structural NUTS-2 check (`EA`/`EF`-style prefixes are not real countries). Per the pilot's own
rule and CLAUDE.md rule 13, the probe reports these as **unresolved, not silently dropped** —
exactly the behaviour a real loader must also have. **A NUTS 2 loader needs an explicit
non-region exclusion list** (aggregate codes: `EA`, `EA20`, `EA21`, `EU`, `EU27_2020`, `EU28`,
`EFTA`, …) alongside the country allowlist — `is_nuts2_code()`'s 4-character heuristic cannot
tell an aggregate from a real region on its own, and neither can a plain prefix check.

The 2024 Nuts2json vintage (see below) drops all UK codes and adds Kosovo's `XK00` for the
first time — `XK00` still needs the same explicit exclusion, since geometry existing is not a
licence decision.

## NUTS vintage and geometry match

Downloaded `Nuts2json` (github.com/eurostat/Nuts2json) TopoJSON, level 2, projection 3035,
resolution 20M: `pub/v2/{year}/3035/20M/2.json`.

| | 2021 vintage | 2024 vintage |
|---|---|---|
| Regions (`nutsrg` layer) | 327 | 292 |
| Size / fetch time | 766,275 B / 0.64 s | 574,331 B / 0.60 s |

**2021 → 2024 diff:** 46 codes dropped, **all 46 are UK regions** (`UKC1`… `UKN0`) — the 2024
vintage retired the UK from NUTS entirely, which happens to match the licence exclusion exactly.
11 codes added: `BA01`–`BA03` (Bosnia, first NUTS coding), `NL35`/`NL36` (a Dutch NUTS
reclassification), `PT19`/`PT1A`–`PT1D` (a Portuguese reclassification), `XK00` (Kosovo, first
NUTS coding — must still be licence-excluded).

**Match against each dataset's own latest year** (the number that actually decides the vintage —
a broader "every year ever published" diff mixes in codes a dataset renamed a decade ago):

| | vs 2021 vintage | vs 2024 vintage |
|---|---|---|
| GDP (2024, 276 regions): in geometry, no data | 65 (incl. all CH, IS, LI) | 23 (incl. all CH, IS, LI, BA) |
| GDP: in data, no geometry | 14 | 7 — `FRY1`–`FRY5`, `PT20`, `PT30` |
| Unemployment (2025, 290 regions): in geometry, no data | 52 (incl. UK) | 10 |
| Unemployment: in data, no geometry | 15 (incl. `EA21`) | 8 — `EA21`, `FRY1`–`FRY5`, `PT20`, `PT30` |
| Population (2025, 296 regions): in geometry, no data | 46 (all UK) | 4 — `BA01`–`BA03`, `XK00` |
| Population: in data, no geometry | 15 (incl. `EFTA`) | 8 — `EFTA`, `FRY1`–`FRY5`, `PT20`, `PT30` |

**Recommendation: the 2024 vintage.** It matches every dataset's latest year far better (10–23
mismatches vs. 46–65 for 2021) and its UK retirement lines up with the licence exclusion exactly,
so choosing it removes a whole class of "region we must not publish but the old geometry still
draws" bugs for free.

**Unresolved, not guessed:** `FRY1`–`FRY5` (French overseas departments) and `PT20`/`PT30`
(Azores, Madeira) appear in all three datasets but under neither geometry vintage's level-2 file
— Nuts2json may publish these single-region overseas territories under a different code or only
at another level; this needs a direct check against Nuts2json's own docs or issue tracker before
implementation, not a guess here. They would render with no outline under either vintage today.

`AL01`–`AL03`, `CH01`–`CH07`, `IS00`, `LI00`, most of `NO0*`, `ME00`, `BA01`–`BA03` have a 2024
outline but **no GDP-per-capita data at all** (the whole EFTA group, plus every candidate country
except Turkey) — genuine partial coverage the panel must show as `missing`, never blank.

## eurostat-map — verified, downloaded, measured 2026-09-14

- **Version:** 4.11.3 (npm `dist-tags.latest`, published 2026-08-18). **Licence: EUPL-1.2**,
  confirmed from the package's own `LICENSE` file (downloaded to the scratch cache), matching
  the brief's expectation.
- **Build:** `build/eurostatmap.min.js` (npm `main`/`unpkg` field), a single webpack UMD bundle
  — `!function webpackUniversalModuleDefinition(...)`. **Downloaded and measured: 1,219,896
  bytes (1.16 MiB) minified**, self-contained: `d3-*`, `proj4`, `topojson-client`, `idb-keyval`,
  `jsonstat-toolkit` and `simple-statistics` (all listed as `dependencies` in `package.json`) are
  inlined by webpack, not loaded separately — one `<script src>` is enough, no companion d3
  `<script>` tags.
- **Runtime fetches by default (grepped from the downloaded bundle, exact strings):**
  - `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/` — live statistics,
    the library's own default data source.
  - `https://ec.europa.eu/eurostat/cache/GISCO/pub/nuts2json/v2/` (when the page's own hostname
    contains `ec.europa.eu`) or `https://raw.githubusercontent.com/eurostat/Nuts2json/master/pub/v2`
    (otherwise, i.e. BelPulse's case) — live geometry.
  - `https://ec.europa.eu/assets/estat/E/E4/gisco/eurostat-map/world-topo-2024-60M-4326.json` and
    `.../IMAGE/WORLD_4326.json` — the library's own basemap layer.
  - `https://ec.europa.eu/eurostat/cache/GISCO/pub/euronym/v3/...` and its
    `raw.githubusercontent.com/eurostat/euronym` mirror — place-name labels.
  - `https://github.com/babel/babel/...` and `https://fontawesome.com/...` are comment-only
    licence links, not runtime requests.
  - `http://www.w3.org/...` strings are SVG/XML namespace URIs (harmless, not fetched).
  - **None of these point at anything BelPulse controls by default** — every one must be
    overridden for the "no request leaves the site" requirement (rule 30/21-adjacent: the public
    site stays static, no server round-trip the builder didn't already generate).
- **Self-hosting confirmed present in the bundle:** a `.nuts2jsonBaseURL(url)` accessor
  (`nuts2jsonBaseURL_` internally) controls the geometry source — its default is exactly the
  conditional above, so setting it to a same-origin path (e.g. `/data/geo/nuts2/`) removes the
  GitHub/ec.europa.eu geometry fetch entirely.
- **In-memory data injection confirmed present:** a `statData()` accessor with `.setData(...)`
  exists in the bundle, matching the documented pattern of handing the library pre-fetched
  values instead of letting it call Eurostat's API itself.
- **Not verified in this batch:** an actual page wiring both options together and confirming
  zero network calls in a browser devtools trace. That is implementation work, not a document.
- **Geometry/attribution (Nuts2json's own README, `Copyright` section, quoted verbatim):**
  "The Eurostat NUTS dataset is copyrighted. There are specific provisions for the usage of this
  dataset which must be respected. The usage of these data is subject to their acceptance." — see
  <https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts>.
  The library's own default map footnote, found verbatim in the bundle, is the credit line to
  reuse: **"Administrative boundaries: ©EuroGeographics ©OpenStreetMap"**. Nuts2json itself is
  EUPL-1.2 (its own `LICENSE`, confirmed).

## Proposed approach

**Do not touch `config/geography/geographies.csv`.** It is Belgium's NIS-keyed table (rule 25);
NUTS 2 codes for the other ~30 countries must never be rows in it. A **new, separate,
allowlisted CSV**, `config/geography/nuts2.csv` (not created in this batch — proposed shape
only), parallel to `international.csv`'s pattern: `nuts_code,geo_id,country_prefix,name_en,
name_fr,name_nl,nuts_version,valid_from`, `geo_id` styled `fr10:nuts2` (lowercase, `:nuts2`
suffix, matching `international.csv`'s `xx:country` convention so the two allowlists read the
same way). `level: nuts2` would be a new `geographies.level` value, never `nis_code`-bearing.

**Belgium is a genuine wrinkle, found while checking this, not assumed:** `geographies.csv`
already carries NUTS codes for **10 of Belgium's 11 real NUTS 2 regions** — the 10 provinces,
tagged `BE21`–`BE25`/`BE31`–`BE35` in its existing `nuts` column. **Brussels-Capital's NUTS 2
code, `BE10`, is not tagged anywhere**: the Brussels region row (`be:reg:04000`) carries `BE1`
(NUTS 1), and its one arrondissement (`be:arr:21000`) carries `BE100` (NUTS 3) — Brussels has no
province level, so nothing in the table sits at exactly the NUTS 2 depth for it. **This needs a
maintainer decision, not a guess**: either add `BE10` as an explicit alias resolving to
`be:reg:04000` for NUTS 2 map purposes only (no schema change, a lookup-table entry), or accept
Brussels renders as `missing` on the Europe panel until that's resolved. Either way, Belgium's
10 provinces should resolve through the **existing** Belgian geography (their real NIS ids,
never duplicated into `nuts2.csv`) — `nuts2.csv` should carry every OTHER country's regions only,
to avoid two geo_ids meaning the same Belgian province.

**Indicator configs** (not created in this batch) would use the existing `MultiGeoTimeSeriesSource`
`fetch` shape ADR 0008 already established (`{dataset, filters, geographies: allowlist, since}`),
e.g.:

```yaml
id: GDP_PC_PPS_NUTS2
source_id: eurostat
geo_levels: [nuts2]
fetch:
  dataset: nama_10r_2gdp
  filters:
    unit: PPS_EU27_2020_HAB
  geographies: allowlist
  since: "2000"
```

**What the adapter lacks**, precisely, for this to actually run (none fixed in this batch):

1. The compound/unrecognized-`OBS_FLAG` gap above — blocking for both `lfst_r_lfu3rt` and
   `demo_r_pjanaggr3` as they stand.
2. `EurostatSource._parse` walks the whole `geo` dimension and expects `geographies: allowlist`
   to resolve every code through `config/geography/international.csv`
   (`src/geography/international.py`) — it has no notion of a *second* allowlist file or of a
   NUTS-shaped code at all. Wiring a NUTS 2 fetch through the existing loader means either
   teaching it to pick the right allowlist file by `geo_levels`, or a parallel loader script
   (`scripts/sync_nuts2.py`, mirroring `sync_international.py`) that calls the same adapter with
   its own geography module. This batch takes no position on which; it is an implementation
   decision, not a spec one, but it is real work, not "reuse as-is."
3. No dimension filter narrows the `geo` axis server-side today — all three fetches above pulled
   every NUTS level (0–3) and filtered client-side to NUTS 2 shape. That is fine at these sizes
   (worst case 961 KB, 0.4 s) but is worth knowing before assuming it scales the same way at
   ~100-indicator volume.

**A directory store**, `nuts2` (`layout: one_csv_per_indicator`, same registry mechanism as
`international`), `data/nuts2/{ID}.csv`.

**Published payload**, `public/data/europe/nuts2/{indicator}.json` (not written in this batch):

```jsonc
{
  "indicator_id": "GDP_PC_PPS_NUTS2",
  "unit": "PPS_EU27_2020_HAB",
  "source": "eurostat",
  "retrieved_at": "2026-09-14",
  "nuts_version": "2024",
  "years": ["2020", "2021", "2022", "2023", "2024"],
  "latest_year": "2024",
  "class_breaks": {"2024": [12500, 18000, 24000, 31000, 45000]},
  "values": {
    "2024": {
      "BE21": {"value": 41230.0, "state": "final"},
      "BE10": {"value": null, "state": "missing"},
      "CH01": {"value": null, "state": "unavailable"},
      "XK00": {"value": null, "state": "suppressed", "note": "excluded by licence"}
    }
  },
  "dropped_by_licence": ["UK*", "XK00"]
}
```

Five distinct states per rule 26: `missing` (no row this year, e.g. Brussels until the alias
above is decided), `unavailable` (no row from Eurostat at all, e.g. Swiss/Icelandic regions in
the GDP dataset), `suppressed` (present at source but excluded here, e.g. by licence), `na`
(Eurostat's own not-applicable flag), and a real `0.0` — never collapsed into each other. Class
breaks computed once in Python per year so every theme/legend reads the same classification —
not recomputed per render in the browser.

**Self-hosted geometry** under `public/data/geo/nuts2/{vintage}/{level}.json` (2024 vintage,
level 2 only — no need to ship 0/1/3 for this panel), with the EuroGeographics/OpenStreetMap
credit line quoted above shown wherever the map renders. **Vendored library** under
`assets/vendor/eurostat-map/eurostatmap.min.js` (1.16 MiB, EUPL-1.2 notice kept alongside it),
lazy-loaded only when the Europe panel opens — not on every page load.

## Data / schema changes

None in this batch. Proposed for a later, approved batch: `config/geography/nuts2.csv` (new
file, new `level: nuts2`, never touching `geographies.csv`), `config/indicators/*_NUTS2.yaml`
(three new indicator configs), a `nuts2` entry in `config/stores.yaml`. An ADR is required
before any of that lands — see the draft ADR alongside this spec — because it is a geography
change (CLAUDE.md rule 19).

## New data sources

None approved yet. Three catalogue rows added in **PROPOSED** state this batch (`docs/data_catalog.md`,
"Eurostat regional (NUTS 2)" section): the two datasets with adapter gaps, the GDP dataset
without one, the Nuts2json geometry, and the `eurostat-map` library. None are in the "Approved
sources" table — rule 8 requires the maintainer's approval first, which this document is
requesting, not assuming.

## Tests

Written this batch: `tests/test_report_nuts2_coverage.py` (23 tests, pure functions only — NUTS 2
code recognition including both pseudo-region suffixes, the licence split's three branches
including the "unresolved, fail loudly" case and Greece's `EL` prefix, the geometry-vs-data
diff, and the raw-flag fallback used when the production `_parse` refuses a compound flag). No
network in tests.

For the implementation batch (not written here): the two adapter gaps above each need a unit
test per real compound flag combination measured in this report, not a synthetic one; a test
that Belgian NUTS 2 codes never appear in `nuts2.csv` (mirroring
`tests/test_international_stays_off_belgian_pages.py`'s shape); a test that every value in a
published `{indicator}.json` file resolves to exactly one of the five states; a test that the
2024-vintage geometry file's ids and the payload's value keys are cross-checked at export time,
failing the export rather than shipping an unstyled region silently.

## Assumptions and open questions

1. **Brussels' `BE10` alias** — described above under Proposed approach. Needs a maintainer
   decision before implementation, not a default.
2. **The `FRY1`–`FRY5`/`PT20`/`PT30` geometry gap** — not resolved; needs a direct check against
   Nuts2json's issue tracker or a different resolution/level file before assuming they are simply
   missing from NUTS 2 entirely.
3. **The compound-`OBS_FLAG` adapter gap** blocks two of the three datasets outright. Whether the
   maintainer wants that fixed as its own small PR before the panel, or as part of the panel's
   implementation batch, is open.
4. **Which class-break scheme** (quantile, Jenks, fixed) is not decided here — `eurostat-map`
   supports several; the spec only fixes that breaks are computed once in Python, not per-render.

## Rollout / risks

- **Rollback:** nothing to roll back — no config, data, or public file was written this batch.
- **Size, if approved and built:** committed CSVs unknown until fetched (GDP dataset's regional
  slice would be roughly 285 regions × 25 years × ~60 bytes/row ≈ 430 KB uncompressed, per
  indicator, well under any per-file limit); geometry 574 KB (2024, level 2 only); vendored
  library 1.16 MiB; a per-indicator payload well under 100 KB at these region counts.
- **Risk:** shipping the 2021 geometry vintage by mistake would put UK outlines back on the map
  with no licence to publish UK data under them — the 46-region UK diff above is the concrete
  number this risk is measured against, not a general worry.
- **Risk:** an aggregate code (`EA21`, `EFTA`, future ones) slipping past a naive prefix filter
  and rendering as if it were a real region — this is exactly what the "unresolved" bucket above
  exists to catch; a NUTS 2 loader must fail loudly on it exactly like the pilot's country
  allowlist already does, not silently drop or silently keep it.

## Decisions needed from the maintainer

1. **Approve the three datasets** — `nama_10r_2gdp` (GDP per capita PPS), `lfst_r_lfu3rt`
   (unemployment rate), `demo_r_pjanaggr3` (population) — as PROPOSED catalogue rows. My
   recommendation: yes, all three; the unit codes and coverage are confirmed and the licence
   filter behaves identically to the already-approved pilot's.
   **Answer (2026-09-14): approved as recommended.**
2. **Approve the Nuts2json geometry source and the 2024 vintage.** My recommendation: yes to
   both — self-hosting is confirmed possible, licence is EUPL-1.2 with a one-line attribution
   requirement, and the 2024 vintage measurably fits all three datasets better while removing UK
   for free.
   **Answer (2026-09-14): approved as recommended.**
3. **Approve vendoring `eurostat-map` 4.11.3 (1.16 MiB, EUPL-1.2).** My recommendation: yes, with
   the two self-hosting options (`nuts2jsonBaseURL`, `statData().setData`) both wired before this
   ships, verified in a browser trace as part of the implementation batch, not assumed from the
   bundle grep alone.
   **Answer (2026-09-14): approved as recommended.**
4. **Approve NUTS 2 outside the international pilot's stage-1 approval.** The pilot
   (`docs/features/international.md`) approved five *national* indicators pending a 14-day
   measurement; NUTS 2 is sub-national and was never in that approval. My recommendation: treat
   this as its own approval, not an extension of stage 1 — the failure modes (compound flags, a
   second allowlist file, an aggregate code slipping through) are different enough to want a
   separate sign-off.
   **Answer (2026-09-14): approved as recommended.**
5. **The NUTS vintage choice** — see the geometry section: recommend 2024.
   **Answer (2026-09-14): approved as recommended (2024).**
6. **How to handle Brussels' missing `BE10` tag** — add the alias, or ship with Brussels
   rendering `missing` until it's resolved. My recommendation: add the alias (a lookup-table
   entry, not a `geographies.csv` change) — Brussels being blank on a Belgian company's own
   Europe map is the kind of gap a client would notice immediately.
   **Answer (2026-09-14): approved as recommended — the BE10 alias is a lookup-table entry, not
   a `geographies.csv` change.**
7. **Whether the compound-`OBS_FLAG` adapter fix is its own PR before this panel, or part of the
   panel's implementation batch.** My recommendation: its own small PR first — it is a general
   adapter correctness fix (today's national-accounts indicators could hit the same wall the
   moment Eurostat starts compounding a flag on one of them), not NUTS-2-specific.
   **Answer (2026-09-14): approved as recommended — its own PR first.** The mapping of the
   single flag `u` ("unreliable") to a canonical status was proposed as a stated assumption in
   that PR (`u` → `estimate`) and **confirmed by the maintainer the same day** (docs/decisions/
   0010-eurostat-compound-observation-flags.md).
