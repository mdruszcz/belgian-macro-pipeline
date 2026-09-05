# Data Catalog

Per `CLAUDE.md` rule 8: no new data source may be added without a row here, approved by the
maintainer.

These four sources are already in production use (predating this catalog); rows added here
during Block B (`config/sources/*.yaml`) formalize existing fetches, not new approvals. Licence
terms are marked TODO pending separate verification — flagged, not blocking, since nothing new
is being introduced.

| source_id | Agency | Adapter | Base URL | Licence | Cadence |
|---|---|---|---|---|---|
| `nbb` | National Bank of Belgium | `nbb` (SDMX) | `nsidisseminate-stat.nbb.be/rest/data/BE2` | TODO | daily |
| `dbnomics_eurostat` | Eurostat (via DBnomics) | `dbnomics` | `api.db.nomics.world/v22/series/Eurostat` | TODO | daily |
| `dbnomics_ameco` | AMECO/EC (via DBnomics) | `dbnomics` | `api.db.nomics.world/v22/series/AMECO` | TODO | daily |
| `fpb` | Federal Planning Bureau | `fpb` (XLSX) | `plan.be` | TODO | quarterly |
| `statbel_geography` | Statbel (Directorate-general Statistics) | manual download → `scripts/derive_geography_csv.py` | statbel.fgov.be open-data portal | **CC BY 4.0** — commercial reuse permitted, attribution required | ad hoc (on Belgian administrative reorganizations) |

## `statbel_geography` — reference geography

Added in Block C. This is **reference data, not an observation source**: it populates
`geographies` (the country, 3 regions, 10 provinces, 43 arrondissements, 565 communes, and 55
historical predecessor communes) and carries no indicator values.

Files, downloaded by the maintainer on 2026-09-05 and held under `data/raw/statbel/`
(gitignored — see `docs/features/geography.md`):

| File | Contents |
|---|---|
| `Nis9_Nis6_refnis_names_01012026.xlsx` | Statistical sectors with the full explicit hierarchy and NUTS codes, valid 2026-01-01 |
| `REFNIS_DEFINITIEF.csv` | Administrative entities, pre-2019 (589 communes) |
| `REFNIS_2019.csv` | Administrative entities, post-2019 wave (581 communes) |
| `REFNIS_2025.csv` | Administrative entities, post-2025 wave (565 communes) |
| `Conversion Postal code_Refnis code_va01012025.xlsx` | Postal code → commune; retained, not yet used |

### Licence — CC BY 4.0

Confirmed by the maintainer from Statbel's *Conditions générales d'utilisation* on 2026-09-05.
Statbel publishes data it owns under **Creative Commons Attribution 4.0**.

**Commercial reuse is explicitly permitted.** Both rights are granted "pour toute utilisation,
y compris commerciale":

- *Partager* — copy, distribute and communicate the material in any medium or format.
- *Adapter* — remix, transform and build upon it.

This settles the question the roadmap raised for Block E ("a dataset you cannot resell is worse
than useless once it is embedded in a paid report"). Selling analysis built on this geography is
within the licence.

#### Obligations this places on us

These are conditions, not suggestions — clause 6 terminates the licence automatically if they
are breached.

1. **Attribution** (5.1). Credit as `Source : Statbel` or
   `Source Statbel : (Direction générale Statistique - Statistics Belgium)`.
2. **Link to the licence** (5.1) — https://creativecommons.org/licenses/by/4.0/
3. **State that changes were made** (5.1). We do modify: the raw files are reshaped into
   `config/geography/*.csv`, codes are re-keyed to internal `geo_id`s, validity windows are
   derived by diffing vintages, and a merger crosswalk is computed. The attribution must say so,
   not merely name the source.
4. **No implied endorsement** (5.2). Nothing may suggest Statbel backs this product or approves
   how its data is used.
5. **Unofficial translations must be labelled** (3). Translations made on one's own initiative
   must be marked as such and cite the original source. **This applies to `name_en`**: no Statbel
   file supplies English, so every English name here is either an unofficial exonym from
   `config/geography/name_en_exonyms.csv` or the entity's own official-language name. Any surface
   showing English geography names must not present them as official Statbel labels.
6. **Third-party data is out of scope** (2). The licence covers Statbel's own data only. The
   NUTS codes carried in `geographies.csv` originate with Eurostat and are governed separately —
   they are currently captured but not published.

Governed by Belgian law (clause 7); granted for an indefinite term (clause 6).

**Not yet done:** points 1–4 are obligations on *published output*. The dashboard and any
exported payload that carries municipal geography needs the attribution string, the licence link
and the "modified" notice. Nothing municipal is published yet, so this is not currently in
breach — but it must land before the first commune-level page goes live. Tracked as a
Block K/J item.
