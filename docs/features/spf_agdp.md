# Feature: SPF Finances AGDP patrimony datasets (leases + transactions, Wave 4; owner occupants + property dynamics, Wave 5 lot A; land use + building condition + tax exemptions, Wave 5 lot B)

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

## Out of scope (Wave 5 lot A)

- 52.01.15 and 52.01.21 (see above).
- Wave 5's remaining AGDP datasets beyond lot A.
- Any page surfacing of these eight indicators.
- `src/analytics/aggregate.py`, `resolve_geo()`, any other adapter, or the
  database schema.
- `docs/data_catalog.md` and `docs/steps` (not touched by this batch).
- The Division and StatisticalUnit members of any zip.
- Province/region rows from any dataset (Municipality-level only).
- A median sale price (see above, Wave 4).

## Wave 5 lot B: land use + building condition + tax exemptions (annual)

Three more annual AGDP datasets, reusing every piece of this module
unchanged: same `frequency="A"` annual mapping, same ATOM discovery, same
ranged zip read. Six new indicators, a NEW committed store
(`spf_agdp_patrimony`, config/stores.yaml), kept separate from `spf_agdp`
above purely for committed-CSV size (see that store's own comment).

**Dropped from this lot (originally scoped, dropped by the lead before
work started)**: 52.01.10, distribution of real-estate wealth for natural
persons. Its `Range` dimension has no all-bands `Total` row -- 34 distinct
`Range*` values and nothing summing them -- so a commune headline would
require summing 34 partly-blanked bands, a derived function needing its own
ADR (CLAUDE.md rule 19). Follow-up, not built.

**Land use (52.01.04, "Utilisation du sol")**, uuid
`86999d70-51ca-11eb-9238-3448ed25ad7c`, 16 versions (2011-2026), Municipality
member 14.6 MB, UTF-8 BOM, `;`-delimited, 21 columns, 155,375 rows in 2026.
Row selected: `ParcelNature=TOTAL`. Columns loaded: `ParcelsNumber`,
`TotalCadastralIncome`, `TaxableCadastralIncome`.

- `MUN_CADASTRAL_PARCELS_TOTAL` -- `ParcelsNumber`. Unit `count`,
  `is_additive=1`, `aggregation_method='sum'`, `preferred_direction=contextual`.
- `MUN_CADASTRAL_INCOME_TOTAL` -- `TotalCadastralIncome`. Unit `eur` (already
  used elsewhere in the schema, no new unit), `is_additive=1`,
  `aggregation_method='sum'`, `preferred_direction=contextual`.
- `MUN_CADASTRAL_INCOME_TAXABLE` -- `TaxableCadastralIncome`. Same shape as
  the total.

`TaxExemptCadastralIncome` (the third euro column on this row) is NOT loaded
as its own indicator: it is byte-identical, for all 565 communes measured in
2026, to 52.01.03's `TotalCadastralIncome` at `ExemptionType=TOTAL` --
verified: 11001 1,522,760 EUR, 44083 3,641,626 EUR, 23106 925,425 EUR, 82039
2,272,702 EUR, in both datasets independently. Loading it twice under two
indicator IDs would publish the same euro figure twice; only 52.01.03's
parcel COUNT is loaded from that dataset instead (see below).

**Building condition (52.01.05, "Etat du bati")**, uuid
`857b351e-51ca-11eb-a86d-3448ed25ad7c`, 16 versions (2011-2026), 82 columns,
105,090 rows in 2026, zero blank cells measured. Row selected:
`ParcelNature=TOTAL`. Columns loaded: `ParcelsNumber`, `CentralHeating`.

- `MUN_BUILDINGS_TOTAL` -- `ParcelsNumber`. Unit `count`, `is_additive=1`,
  `aggregation_method='sum'`, `preferred_direction=contextual`.
- `MUN_BUILDINGS_CENTRAL_HEATING` -- `CentralHeating`. Same shape. No
  share/percentage of central-heating coverage is published here -- that
  would be a derived ratio, out of scope for this batch and needing its own
  ADR (CLAUDE.md rule 19). Follow-up.

**Property-tax exemptions (52.01.03, "Exonerations du precompte
immobilier")**, uuid `8607969e-51ca-11eb-8d7d-3448ed25ad7c`, 16 versions
(2011-2026), 14 columns, 2,825 rows in 2026 (565 communes x ExemptionType
{1,2,3,4,TOTAL}). Row selected: `ExemptionType=TOTAL`. Column loaded:
`ParcelsNumber` only -- `TotalCadastralIncome` on this row duplicates land
use's own euro total (see above) and is deliberately not loaded a second
time.

- `MUN_PARCELS_TAX_EXEMPT` -- `ParcelsNumber`. Unit `count`, `is_additive=1`,
  `aggregation_method='sum'`, `preferred_direction=contextual` -- explicitly
  NOT higher_is_better or lower_is_better: a higher exempt count can mean
  more public/charitable land use in the commune, or a weaker taxable base
  relative to its size. Neither reading is uniformly "better".

All six: `frequency=A`, `max_age_days=450`, `source_id=spf_finances`. No new
unit (`count` and `eur` both already existed) and no formatter/i18n change.

### The blank-count fix (a real hazard, fixed here)

`_annual_row_to_observations`'s count branch previously wrote
`0.0 if count is None else float(count)` -- a blank count cell silently
became a fabricated measured zero (CLAUDE.md rule 26 forbids exactly this:
missing and measured-zero are different states). Fixed to raise
`AgdpSchemaError`, naming the dataset, year and NIS, on a blank count --
matching every other unexpected-blank guard in this module. No live cell in
any dataset routed through this function (lot A or lot B) has ever actually
been blank at a count column, so this changes behaviour only if that
measured invariant is ever violated. One existing lot A test asserted the
old (buggy) behaviour directly --
`test_property_dynamics_blank_parcels_is_na_never_a_fabricated_zero_duration`
in tests/test_spf_agdp_source.py, which fed a blank `ParcelsNumber` and
asserted `MUN_PARCELS_OWNED` came back `0.0, final`. That assertion was
itself the bug; it is now
`test_property_dynamics_blank_parcels_count_refuses_never_a_fabricated_zero`
and asserts the raise instead. No other existing test relied on the old
behaviour.

### A second, independent hazard the new columns exposed: `always_final_columns`

The original count/non-count split in `_annual_row_to_observations` assumed
at most one CSV column per config is the actual count driver (`is_count`
doubled as "read the count value, not your own cell" AND "this is the
row-count column"). Lot A never had a second additive column, so this never
surfaced. Land use (`TotalCadastralIncome`, `TaxableCadastralIncome`) and
building condition (`CentralHeating`) are each their own independently
measured additive figure -- not derived by dividing by the parcel count the
way a percentile/median/mean is -- so marking them `is_count=True` would
have made them silently copy `ParcelsNumber`'s own value instead of their
own cell (caught by this batch's own tests before it ever reached a
fixture). `AgdpDatasetConfig` gained `always_final_columns`, a tuple of CSV
column names that are always read from their own cell and written `final`,
never collapsed to `na` at a zero/blank count the way a lot A percentile is.
`is_count` stays reserved for exactly the one column that IS
`config.count_column` itself.

### The real 1-4 suppression tier lot A never measured

Lot A's docstring said no 1-4 suppression tier existed in the annual shape;
lot B's raw files prove that only true of the specific columns lot A
happened to load. Land use and tax exemptions DO blank their
`TotalCadastralIncome`/`TaxableCadastralIncome` columns at low
`ParcelsNumber` -- measured 2026: land use blanks 30,171 rows at
`ParcelsNumber` 1-4; tax exemptions blanks 156 rows at `ParcelsNumber` 1-4.
The relation is exact: `ParcelsNumber=0` -> not blank; `1-4` -> blank; `>=5`
-> not blank -- the same shape as the quarterly Leases/Transactions
suppression tier, just previously unmeasured for the annual datasets. It
never touches any of lot B's six CHOSEN columns (verified non-blank at
TOTAL/ExemptionType=TOTAL across 2026, 2025 and 2017), so no suppression
mapping was added for them -- but the module docstring and
`_annual_row_to_observations`'s own docstring now name it, so the next
annual dataset added here does not assume it does not exist.

**Herstappe (NIS 73028)**: the one place this tier bites at TOTAL rather
than a sub-band -- `ParcelsNumber=4` at `ExemptionType=TOTAL` in 2025 and
2017, with `TotalCadastralIncome` blank: a genuine suppressed state. That
column is excluded from this batch entirely (see "duplication" above); the
one column this batch DOES read from that dataset, `ParcelsNumber` itself,
is never blank for Herstappe or any other commune measured.

### The five states (lot B)

For all three datasets: the chosen cell is always published (non-blank) for
a live commune; a literal `0` is a real measured zero, final (e.g. NIS 44001
in 2017 has `ExemptionType=TOTAL ParcelsNumber=0` -- loaded as a real zero,
not `na`); a blank cell at TOTAL/ExemptionType=TOTAL is a schema surprise
and the load refuses (CLAUDE.md rule 13) rather than guess; a commune absent
from a given year's file is not zero-filled or written at all. `suppressed`
does not arise for any of the six chosen columns -- it is a real state in
these files (see above), just never on a column this batch reads.

### Geography

Ordinary `resolve_geo(conn, nis, "YYYY")` per row's own year, same as lot A
and the module's general rule (CLAUDE.md rule 3). Verified period-correct
against the real land-use file: 2026 has 565 communes, 2017 has 589; 44083
present 2026 absent 2017; 23106 present 2026 absent 2017; 82039 present 2026
absent 2017; 44001 absent 2026 present 2017. No `Fictious` column and no
`N/A` NIS placeholder in any of the three datasets (unlike lot A's Owner
Occupants) -- asserted directly in
`test_no_fictitious_column_or_non_numeric_nis_in_lot_b_row_filter`
(tests/test_sync_spf_agdp.py); no `row_filter` configured for any of the
three.

### Hand-computed expected values (real files, TOTAL rows)

Land use 2026 -- 11001: parcels 11,540, cadastral income total 15,431,671,
taxable 13,908,911. 44083: 46,948 / 30,809,990 / 27,168,364. 23106: 38,624 /
14,644,408 / 13,718,983. 82039: 55,633 / 14,059,868 / 11,787,166.
Land use 2017 -- 11001: 9,925 / 14,338,902. 44001: 23,424 / 16,144,345.

Building condition 2026 -- 11001: parcels 9,553, central heating 6,419.
44083: 29,756 / 18,711. 23106: 13,798 / 9,216. 82039: 13,716 / 8,505.
Building condition 2017 -- 11001: 8,011 / 5,636. 44001: 11,937 / 7,211.

Tax exemptions 2026 -- 11001: 216. 44083: 1,210. 23106: 775. 82039: 2,638.
Tax exemptions 2017 -- 11001: 206. 44001: 0 (real measured zero, final, not
`na`).

Cross-check, land use `TaxExemptCadastralIncome` vs. exemptions dataset's
own `TotalCadastralIncome`, both 2026: 11001 1,522,760; 44083 3,641,626;
23106 925,425; 82039 2,272,702 -- confirmed equal in both datasets
independently, and (indirectly) as
`MUN_CADASTRAL_INCOME_TOTAL - MUN_CADASTRAL_INCOME_TAXABLE` for each commune
in the real committed store.

### Store

`spf_agdp_patrimony`, config/stores.yaml, `source_id: spf_finances`,
`mode: in_db`, same sync script (`scripts/sync_spf_agdp.py`) as `spf_agdp`
and `ipp_rate` -- one script now writes rows for two stores in the same run;
`scripts/offload_stores.py` splits them by declared `indicator_id`, refusing
a run where two stores claim the same indicator (its own `declared` overlap
check). Real backfill: 55,968 rows across all 16 versions of all three
datasets, 620 distinct communes (covering every commune that has existed
since 2011). Committed CSV `data/spf_agdp_patrimony_observations.csv`
measures 8.30 MB; its history shard
`data/communes_history/spf_agdp_patrimony.csv` measures 6.91 MB -- both well
under the 20 MB threshold the handoff set for staying a single CSV (not
`layout: one_csv_per_indicator`). `spf_agdp`'s own committed CSV and shard
are unchanged by this batch (18.86 MB / 21.69 MB).

### Out of scope (Wave 5 lot B)

- 52.01.10 (see above -- dropped, not built).
- Any page surfacing of these six indicators.
- A central-heating share/percentage (needs its own ADR).
- `src/analytics/aggregate.py`, `resolve_geo()`, any other adapter, or the
  database schema.
- `docs/data_catalog.md` and `docs/steps` (not touched by this batch).
- The quarterly Leases/Transactions path (unchanged).
- Province/region rows (Municipality-level only).
