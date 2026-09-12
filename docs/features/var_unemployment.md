# Feature: current municipal unemployment from the Vlaamse Arbeidsrekening

Status: implemented
Approved by the maintainer: 2026-09-12

## Goal

Replace the Census 2021 unemployment rate in the commune-page headline and employment section with
the latest nationwide municipal administrative rate. Keep the Census observations in the research
store, but do not present the old snapshot as the current unemployment headline.

## Source and definition

Steunpunt Werk's Vlaamse Arbeidsrekening publishes annual administrative labour-market indicators
for Belgium, its regions, provinces, reference regions and municipalities. The selected measure is
`Werkloosheidsgraad (%)`, total men and women, ages 15-64. It is unemployed residents divided by the
administrative labour force. It is not the survey-based ILO/Eurostat rate.

Required source credit: **Steunpunt Werk - Vlaamse Arbeidsrekening o.b.v. DWH AM&SB - KSZ, BISA**.
The source page provides export and explicitly requires attribution for use or distribution, but
does not name a standard open-data licence. BelPulse records that limitation and does not apply
Statbel's CC BY 4.0 grant to these figures.

## Refresh procedure

1. Open the `BNW - T1 - Tabel` sheet from the Vlaamse Arbeidsrekening dashboard.
2. Select geographic level `Gemeente`, sex `Totaal`, age `15-64`, the required year(s), and measure
   `Werkloosheidsgraad (%)`.
3. Choose **Downloaden -> Kruistabel -> CSV** and save the result as
   `data/raw/var/var_unemployment_15_64.csv`.
4. Load a disposable database with `python scripts/sync_var.py --db data/local/var.db`.
5. Export the committed observation store with `python scripts/export_observations_csv.py --db
   data/local/var.db --out data/var_unemployment_observations.csv --indicators
   ADMIN_UNEMPLOYMENT_RATE_COM`.
6. Run the manual-source workflow/export commands with the VAR store included.

The apparent CSV is UTF-16 tab-separated. The loader refuses changed headers, any non-total sex or
non-15-64 row, duplicate commune-years, a missing percent sign, and anything other than exactly 565
unique current communes per period.

## Geography and suppression

The export contains Dutch municipality names, not NIS codes. Matching is therefore exact against
the unique current `name_nl`, after only Unicode normalization, apostrophe normalization, and
removal of Tableau's final disambiguation suffix (`Aalst (Aalst)`, for example). There is no fuzzy
or accent-insensitive fallback.

The dashboard expresses historical values on its current 565-municipality grid, including the two
municipalities formed in 2025. Those rows are source-published restatements and stay pinned to that
grid, as already documented for ONEM. Blank rate cells caused by the source's fewer-than-four rule
are stored as `suppressed`, never zero.

## UI

`ADMIN_UNEMPLOYMENT_RATE_COM` replaces `UNEMPLOYMENT_RATE_COM` in the commune hero, page-builder
document and employment section. The Census 2021 activity counts remain in the data store but are
not shown in that section.
