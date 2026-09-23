# Feature: SPF Finances AGDP patrimony datasets (leases + transactions, Wave 4; owner occupants + property dynamics, Wave 5 lot A)

## Source

SPF Finances / FOD Financiën, Administration Generale de la Documentation
Patrimoniale (AGDP) -- the same `spf_finances` source_id Wave 6 already
registered for the communal additional IPP rate
(`config/sources/spf_finances.yaml`), reused here rather than duplicated.
Licence: SPF Finances' open-data licence, based on CC BY. Attribution
required: "FPS Finance - General Administration of Patrimonial Documentation
(GAPD)".

Two datasets loaded this batch, each published as a quarterly zip via an
ATOM feed:

- **Real-estate leases (52.01.01)**, ATOM feed
  `https://opendata.fin.belgium.be/download/ATOM/84d5f470-51ca-11eb-8a67-3448ed25ad7c-en.xml`
  -- 41 versions, 2016-Q1 through 2026-Q1 at cutover.
- **Real-estate transactions (52.01.02)**, ATOM feed
  `https://opendata.fin.belgium.be/download/ATOM/89209670-51ca-11eb-beeb-3448ed25ad7c-en.xml`
  -- 40 versions, 2016-Q1 through 2025-Q4 at cutover.

Wave 5 (not built here) adds 13 more, mostly annual, AGDP datasets through
the same generic reader (`src/fetchers/spf_agdp.py`'s `AgdpDatasetConfig`) --
only the per-dataset configuration is new work at that point, not a new
adapter class.

## ATOM feed shape

Each `<link rel="section">` element is one published version (one quarter):

```xml
<link rel="section" href="https://.../some-file.zip"
      time="2026-03-31T00:00:00Z" length="214978234" .../>
```

`href` is the zip download URL. `time` is the quarter-end timestamp -- the
ONLY period signal anywhere in this source; no CSV column carries a period.
`20260331` maps to `2026-Q1`. `length` is the zip's byte size, which is this
source's only republish signal (see Incremental fetch below).

## The zip

Each version's zip holds seven CSVs (national, regional, provincial,
arrondissement, municipality, division, statistical-unit) plus French and
Dutch TechSpec PDFs. Only the `Municipality*.csv` member is ever read.
Division and StatisticalUnit members are never opened -- the transactions
dataset's StatisticalUnit CSV alone is 4.3 GB uncompressed, and no indicator
in this batch needs anything finer than commune level.

CSV shape, measured directly: UTF-8 with a BOM (`utf-8-sig`), `;`-delimited,
CRLF line endings, decimal point, no thousands separator.

### Leases columns

`NISCode;NameFre;NameDut;NameGer;RegistrationType;LessorType;TakerType;
RentsNumber;RentP25;RentP50;RentP75;ChargesP25;ChargesP50;ChargesP75;
TotalRentP25;TotalRentP50;TotalRentP75`

80 rows per commune: 5 `RegistrationType` values (GeneralRegistration,
HousingRegistration, HuntingFishingRegistration, PublicServiceRegistration,
UnknownRegistration -- no TOTAL row for this dimension) x 4 `LessorType` x 4
`TakerType` (LegalPerson, Mixed, NaturalPerson, TOTAL).

### Transactions columns

`NISCode;NameFre;NameDut;NameGer;TransactionType;ParcelNature;
ParcelsNumber;PriceP25;PriceP50;PriceP75;ParcelsAreaP25;ParcelsAreaP50;
ParcelsAreaP75`

`TransactionType` in {CESSION, COMMUNAUTE, DONATIMMEUB, OTHER, PARTAGE,
SUCCNALAT, TOTAL, VENTEIMMEUB, VENTEPUBIMMEUB}. `ParcelNature` has ~290
distinct values including TOTAL, TYPE_HOUSE, TYPE_APART.

No total-market-value column exists in either dataset. None is invented
(CLAUDE.md rule 36).

`TYPE_HOUSE`/`TYPE_APART` are absent from the ParcelNature dimension in
2025-Q1 through 2025-Q3 (measured); no headline indicator in this batch
depends on either value, so this gap does not affect what is loaded, but it
rules out building a house-vs-apartment price split from this batch's scope.

## Ranged zip reads

The transactions StatisticalUnit CSV alone is 4.3 GB uncompressed; each
zip is roughly 205 MB; a naive one-time backfill of every version of both
datasets would be roughly 8.1 GB of downloads to read two 6 MB CSVs per
version.

The server returns HTTP 206 and `Accept-Ranges: bytes` for a byte-range GET.
`src/fetchers/spf_agdp.py`'s `_RangedHttpFile` is a seekable
`io.RawIOBase` that answers `zipfile.ZipFile`'s reads with range requests,
wrapped in `io.BufferedReader(..., buffer_size=1<<22)`. Opening only the
`Municipality*.csv` member this way reads roughly 6 MB in roughly 5 range
requests instead of the whole ~205 MB zip -- verified directly against the
real server. Each range request retries transient failures up to 3 times.

If the server answers 200 instead of 206 for a range request (a proxy that
silently drops range support), `open_municipality_csv()` falls back to a
whole-body GET for the rest of that read and prints a loud
`::warning::` line naming the dataset and quarter, so the fallback is
visible in the daily run log rather than silently eating the bandwidth and
time budget.

## Incremental fetch

The daily job does not re-read all ~40 versions of both datasets every day.
`scripts/sync_spf_agdp.py` keeps a small committed JSON file,
`data/spf_agdp_state.json`, recording per (dataset, quarter) the ATOM
`length` and the timestamp of the run that loaded it -- the least invasive
option available (no new database table, no schema change). A run re-reads
a version's zip only if:

- that quarter is not yet in the state file for that dataset (a genuinely
  new version), or
- it IS in the state file but the feed's current `length` no longer matches
  the recorded one (SPF Finances republished that quarter's file under the
  same href -- the only republish signal ATOM exposes here).

Every other already-loaded version is skipped without any network call for
its zip (the ATOM feed itself is still fetched every run -- it is small,
and is the only way to learn whether anything changed).

The one-time backfill is not a separate code path: on a missing or empty
state file, every version in both feeds is "new", so the ordinary
incremental-fetch logic reads all of them.

## Geography: period-correct, not pinned

Every (indicator, NIS, quarter) row resolves via
`resolve_geo(conn, nis, quarter)` against that row's OWN quarter -- the
ordinary rule (CLAUDE.md rule 3), the same convention
`scripts/sync_population_movement.py` uses and the opposite of
`scripts/sync_bankruptcies.py` / `scripts/sync_ipp_rate.py`'s pinned-period
resolution. Measured commune counts: 589 communes 2016-Q1 through 2018-Q4,
581 2019-Q1 through 2024-Q4, 565 from 2025-Q1. 44001 is present before 2019
and absent after; 44083 first appears 2019-Q1; 23106 first appears 2025-Q1
-- all three match the geographies table's own validity windows exactly, so
no transition-sheet exclusion list is needed here (unlike
population_movement.py's forward-mapped transition sheets).

Because every live commune's file row already exists every quarter (unlike
bankruptcies.py's file, which genuinely omits a commune-month with zero
bankruptcies), there is no zero-fill grid to build: a NIS code absent from a
live quarter's Municipality CSV is a schema surprise, and
`src/fetchers/spf_agdp.py`'s "zero matched rows" guard (and, for a still
partially-loaded quarter, the ordinary per-row parse) catches it rather than
this script silently synthesizing a missing row.

An NIS code that fails to resolve for its own quarter is collected across
the whole run and refuses the run at the end (never a partial load), the
same `unresolved` pattern `sync_population_movement.py` and
`sync_bankruptcies.py` already use.

## The five states

Per commune-quarter row, for each indicator:

1. **Count column present and > 0** -> value, status `final`.
2. **Count column blank on a row that exists** -> the SPF published none of
   that kind that quarter: the count indicator writes `0.0`, status
   `final`. This is never confused with a missing/unmeasured state, because
   every live commune already has its row that quarter (see above) -- a
   present row with a blank count IS the measurement.
3. **Percentile column blank, count 1-4** -> value `None`, status
   `suppressed`. SPF Finances withholds the percentile below five
   observations to avoid identifying an individual lease or transaction;
   never written as zero.
4. **Percentile column blank, count 0 or blank** -> value `None`, status
   `na` -- there is nothing to suppress because there is nothing to measure.
5. **A NIS code that does not resolve for its own quarter** -> collected and
   the whole run is refused (see Geography above); this is a run-level
   failure, not a per-row state.

Measured directly: the minimum row count with a published percentile is
exactly 5; counts 1-4 always carry a blank percentile; the blank string
(`''`) is the only suppression marker in either dataset -- never a sentinel
number.

## The indicators (4)

All frequency `Q`, period `YYYY-Qn`.

**Leases**, row selected by `RegistrationType=HousingRegistration,
LessorType=TOTAL, TakerType=TOTAL`:

- `MUN_LEASES_NEW_HOUSING` -- `RentsNumber`. Unit `count`, `is_additive=1`,
  `aggregation_method='sum'`, `preferred_direction=contextual`.
- `MUN_LEASE_RENT_MEDIAN_HOUSING` -- `RentP50`. Unit `eur_per_month`
  (no existing indicator uses a EUR-per-month unit; `eur_per_inhabitant` and
  `eur` are the closest existing spellings and neither fits a monthly rent
  figure, so a new, self-describing unit string was used rather than
  overloading an existing one that means something else).
  `is_additive=0`, `aggregation_method='not_applicable'`,
  `preferred_direction=contextual`.
- `MUN_LEASE_CHARGES_MEDIAN_HOUSING` -- `ChargesP50`. Same unit and
  aggregation shape as the rent median.

**Transactions**, row selected by `TransactionType=VENTEIMMEUB,
ParcelNature=TOTAL`:

- `MUN_PROPERTY_SALES` -- `ParcelsNumber`. Unit `count` (matches the
  existing spelling used by `HOUSE_SALES_TRANSACTIONS` and others, and the
  `counts_non_negative` validation rule in `src/validation/rules.py`
  applies to `unit='count'` directly -- both count indicators in this batch
  are always >= 0 by construction, since a blank count writes `0.0`, never
  a negative). `is_additive=1`, `aggregation_method='sum'`,
  `preferred_direction=contextual`.

A median sale price is deliberately left out: `VENTEIMMEUB`/`TOTAL` mixes
houses, flats, land and garages, whose prices are not comparable, so no
price indicator is derived from this transaction type at this granularity
(CLAUDE.md rule 36 -- refuse rather than invent a figure that mixes
incomparable things).

`is_additive` / `aggregation_method` are not indicator-YAML fields (no such
field in `docs/features/indicator_config.schema.json`); as with
`sync_walstat.py` and `sync_ipp_rate.py`, they are written directly into the
`indicators` reference row by `scripts/sync_spf_agdp.py`'s
`_ensure_reference_rows`.

## Store

`spf_agdp`, `config/stores.yaml`, `source_id: spf_finances`, `mode: in_db`
(all four ATOM feeds are reachable live from CI, the same reason
bankruptcies/population_movement/ipp_rate are `in_db`). Reference-rows
script `scripts/sync_spf_agdp.py --reference-rows-only`. Path
`data/spf_agdp_observations.csv`. Wave 4 alone measured ~94,300 rows,
~12.8 MB; with Wave 5 lot A's ~32,000 rows added, the real committed file is
~126,800 rows / ~18.9 MB -- still a single CSV, not
`layout: one_csv_per_indicator`, under the 25 MB commit limit (re-measure
before adding further AGDP datasets to this store; the handoff's own
threshold to switch layouts was 20 MB).

## Wave 5 lot A: owner occupants + real-estate property dynamics (annual)

Two more AGDP datasets, reusing every piece of this module unchanged except
the ATOM period parser (below): SPF Finances publishes these two as annual
1-January snapshots, not calendar quarters, so `AgdpDatasetConfig` gained a
`frequency` field (`"Q"` default, unchanged behaviour; `"A"` for these two)
and `parse_atom_feed()` gained a `frequency` parameter that switches between
the existing quarter-end parser and a new one requiring the ATOM `time`
attribute to be exactly `YYYY-01-01T...` -- any other month or day is a
schema surprise, refused loudly (CLAUDE.md rule 13), never guessed.

**Owner occupants (52.01.14, "Titulaires occupants")**, uuid
`a54ced71-dcc5-4b51-99f5-40b391631727`, 7 versions (2020-2026), member
`MunicipalityWideOwnerOccupant_<YYYYMMDD>.csv`. Columns:
`NISCode;Fictious;NameFre;NameDut;NameGer;PersonType;HousingRightType;PersonNumber`.
Row selected: `PersonType=Total, HousingRightType=OCCUPANTPUPES`. The
dataset publishes one fictitious placeholder row set per version
(`Fictious=1`, `NISCode="N/A"`) which is skipped by `AgdpDatasetConfig.
row_filter` before `select` is ever checked, along with any row whose
NISCode is not all-digit -- a new mechanism (`row_filter`, a
`Callable[[dict], bool]` run once per raw row) added instead of
special-casing this one dataset inside `_parse`.

- `MUN_OWNER_OCCUPIERS` -- `PersonNumber`. Unit `count`, `is_additive=1`,
  `aggregation_method='sum'`, `preferred_direction=contextual`. A real zero
  is written as-is, `final` -- same count semantics as Wave 4's
  `MUN_PROPERTY_SALES`.

**Real-estate property dynamics (52.01.24, "Dynamique de la propriété
immobilière")**, uuid `219cd997-631a-11f0-bb32-00be432db085`, 16 versions
(2011-2026), member `MunicipalityWidePropertyDynamics_<YYYYMMDD>.csv`.
Columns include `ParcelNature;ParcelsNumber;PropertyDurationP25/P50/P75;
PropertyDurationMean;PropertyRotationMean`. Row selected:
`ParcelNature=TOTAL`.

- `MUN_PARCELS_OWNED` -- `ParcelsNumber`. Unit `count`, `is_additive=1`,
  `aggregation_method='sum'`, `preferred_direction=contextual`.
- `MUN_OWNERSHIP_DURATION_MEDIAN` -- `PropertyDurationP50`. Unit `years`,
  `is_additive=0`, `aggregation_method='not_applicable'`,
  `preferred_direction=contextual`.
- `MUN_OWNERSHIP_ROTATION_MEAN` -- `PropertyRotationMean`. Same shape as the
  duration median.

**Guard against a fabricated zero**: at other parcel natures than `TOTAL`
the source writes a literal `0` for every duration/rotation column when
`ParcelsNumber` is `0` for that row -- not a measured duration. At
`ParcelNature=TOTAL` every commune measured has a non-zero `ParcelsNumber`
and a real percentile/mean in every one of the 16 versions, but the guard
exists in `_annual_row_to_observations` regardless: `ParcelsNumber` blank or
`0` maps every duration/rotation column to `value=None, status='na'`, never
a fabricated zero; `ParcelsNumber >= 1` requires a non-blank cell or the row
is refused (CLAUDE.md rule 13) -- no 1-4 suppression tier exists for either
Wave 5 dataset (none was measured, unlike Leases/Transactions' `RentsNumber`
/`ParcelsNumber` suppression at 1-4).

**Geography**: an ordinary, unremarked `resolve_geo(conn, nis, "YYYY")` call
-- the annual period string resolves at `YYYY-01-01`
(`src/geography/resolve.py`'s `period_to_date`), exactly matching the ATOM
feed's own 1-January snapshot semantics. Verified: 23106 (Pajottegem)
resolves for 2025 and raises for 2024; 82039 resolves for 2025 not 2024;
23023 resolves for 2024 not 2025; 44011 resolves for 2018, 44083 for 2020.

**Excluded from this batch (originally scoped for Wave 5 lot A, dropped by
the lead)**:

- 52.01.15 (buyer profile) -- no `TOTAL` rows; a commune headline would
  require summing partly-blanked cells, and a blank is indistinguishable
  from suppressed at that granularity. Follow-up.
- 52.01.21 (buyers' origin) -- an origin-destination matrix, ~259 MB per
  year; its only scalar duplicates `MUN_PROPERTY_SALES` already loaded by
  Wave 4. Follow-up.

## Out of scope (this batch)

- 52.01.15 and 52.01.21 (see above).
- Wave 5's remaining AGDP datasets beyond this lot.
- Any page surfacing of these eight indicators.
- `src/analytics/aggregate.py`, `resolve_geo()`, any other adapter, or the
  database schema.
- `docs/data_catalog.md` and `docs/steps` (not touched by this batch).
- The Division and StatisticalUnit members of any zip.
- Province/region rows from any dataset (Municipality-level only).
- A median sale price (see above, Wave 4).
