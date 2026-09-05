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
