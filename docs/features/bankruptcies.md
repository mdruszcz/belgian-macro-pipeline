# Feature: Bankruptcies (Statbel, monthly, automatic)

Status: implemented
Catalogue row: docs/data_catalog.md (approved, maintainer, 2026-09-23, PR #229)
Branch: feat/ns1-bankruptcies

## Source

Statbel's monthly bankruptcies-by-NACE open-data file. Landing page:
`https://statbel.fgov.be/fr/open-data/evolution-mensuelle-des-faillites-par-nace`.
`source_id: statbel` (existing `config/sources/statbel.yaml`, untouched by this
batch). Unlike police.be and the real-estate workbook, this source used to be
reachable live from this pipeline's network context in CI, wired into the
Dagster asset graph and `make fetch` like ONEM/WalStat. **Not daily any more
as of 2026-09-23 -- see below.**

## Not daily any more (2026-09-23)

Diagnosed on the scheduled run 2026-09-23T18:09: statbel.fgov.be answers
every request from a GitHub Actions runner with a CAPTCHA challenge page
(HTTP 200, text/html, ~46 KB, "This question is for testing whether you are
a human visitor... What code is in the image?", carrying a support ID)
instead of the landing page or the zip. A probe run confirmed this is
runner-specific -- the same URLs still return the real page/file from a
maintainer's own machine. This pipeline does not solve or evade CAPTCHAs.

`orchestration/commands.py`'s `bankruptcies_observations` Command now
carries no `workflow_step`, so it is absent from `TRACKED` and from the
daily `fetch_sources` job. The committed store (`config/stores.yaml`
`bankruptcies`, `mode: in_db`) is unchanged and keeps flowing into every
export; only the automatic refresh stopped.

**How to refresh**, from a machine that still passes the CAPTCHA:
```
python scripts/sync_bankruptcies.py --db data/belgian_macro.db
```
or, if even that machine gets challenged, open the landing page in a
browser (which passes the CAPTCHA interactively), follow its
`TF_BANKRUPTCIES(<year>).zip` link, save it, then:
```
python scripts/sync_bankruptcies.py --db data/belgian_macro.db \
    --from-file TF_BANKRUPTCIES_2026.zip
```
`--from-file` runs the exact same parse/pinned-resolution/zero-fill path as
the live fetch. The indicators' `max_age_days` and the `staleness`
validation rule are what flag when a refresh of THIS data is actually due.
`statbel.yaml`'s `fetch_window_days`/`fetch_silence` check is keyed on the
shared `statbel` source_id, not per-indicator -- it still passes because
`statbel_local_units` (Bestat) stays in the daily gate and keeps fetching
that source_id every day; it says nothing about bankruptcies specifically
having gone quiet.

## Link discovery

The download link's filename carries a year that changes as Statbel republishes
(`TF_BANKRUPTCIES(2025).zip` today). `src/fetchers/bankruptcies.discover_zip_url()`
reads the href off the landing page's own HTML with a plain regex — no HTML-parsing
library is a dependency of this pipeline, and one link on one page does not justify
adding one. It matches `TF_BANKRUPTCIES(<year>).zip` (URL-encoded or literal
parens) and explicitly excludes the two sibling variants the page also carries,
`TF_BANKRUPTCIES_accdb(<year>).zip` and `TF_BANKRUPTCIES_sqlite(<year>).zip`, which
share the same prefix. Zero matches, or more than one, raises
`BankruptcyLinkNotFoundError` — never a cached or hard-coded URL fallback
(CLAUDE.md rule 13).

## The file

One member in the zip, `TF_BANKRUPTCIES.txt`: pipe-separated, 41 columns, header
row present. Encoding is UTF-8 with a BOM — read as `utf-8-sig`, never cp1252
(measured: "Liège" is the UTF-8 bytes `4c 69 c3 a8 67 65`; cp1252 would decode
those bytes without error and silently produce "LiÃ¨ge"). Five columns are read,
resolved by name from the header, never by position: `MS_COUNTOF_BANKRUPTCIES`,
`MS_COUNTOF_WORKERS`, `CD_YEAR`, `CD_MONTH`, `CD_MUNTY_REFNIS`. Any of the five
missing is a schema change and fails loudly. `CD_MONTH` is unpadded in the source
(`8`, not `08`); the period is built as `f"{year:04d}-{month:02d}"`.

One row in the file is one (commune × month × employment class × legal form ×
NACE class × company duration) cell. The adapter sums `MS_COUNTOF_BANKRUPTCIES`
and `MS_COUNTOF_WORKERS` down to one value per (commune, month), and emits two
rows per surviving cell — one per indicator.

## The zero rule and its two boundaries

A commune with no row in the file for a given month had zero bankruptcies that
month, and is written as an explicit `0.0`/`final`, never left absent. The
maintainer's words: "une commune sans faillite un mois donné compte pour 0".
`scripts/sync_bankruptcies.py` builds the full grid — every commune LIVE on the
pinned map × every month the file's own data covers — after summing, and fills
every cell the summed totals do not cover. The fill iterates the *geography* map,
not the file's own distinct NIS codes, which is why Herstappe (73028), absent from
the file across its whole 21-year history, still gets a complete series of zeros.

Two boundaries, both load-bearing:
- Never a month beyond the file's own last observed month (derived from the
  max of the periods actually present in the parsed rows, never today's date).
- Never a month before the file's own first observed month, for the same reason.

## Geography: pinned resolution

The file is published on today's commune map: 31 post-merger codes (e.g. 44083,
23106, 37021) carry real rows in years before those communes existed. Every row
resolves via `resolve_geo(conn, nis, PINNED_PERIOD)` with `PINNED_PERIOD = "2026"`
— the same pattern `scripts/sync_police.py` (pinned "2024") and
`scripts/sync_realestate.py` (pinned "2025") already use for their own sources,
and for the same reason: the source backcasts its current grid onto its whole
history, so resolving each row at its own period would raise for a
merger-created commune in a pre-merger year, and would misattribute the value
even where it didn't raise. Any code that fails to resolve at the pin is
collected; the whole run refuses (`SystemExit`) rather than loading a partial
series, naming the first few failures.

## The two indicators

`BANKRUPTCIES` (count, monthly, `is_additive=1`, `aggregation_method=sum`):
company bankruptcies pronounced in the commune that month.
`BANKRUPTCY_JOBS_LOST` (count, monthly, same aggregation): workers employed at
companies declared bankrupt in the commune that month, from `MS_COUNTOF_WORKERS`.
Both `preferred_direction: lower_is_better`, `max_age_days: 75`, `display: null`.
Both additive and aggregatable the normal way (sum communes, no ratio to
recompute) — unlike police.be's rates or WalStat's per-capita euros, which have
no underlying total.

## Store

`config/stores.yaml`'s `bankruptcies` entry: `mode: in_db`, `source_id: statbel`,
both indicators declared alphabetically, `reference_rows` pointing at
`scripts/sync_bankruptcies.py --reference-rows-only`. `layout:
one_csv_per_indicator` (the same mechanism `international`/`nuts2` already use,
`data/bankruptcies/BANKRUPTCIES.csv` and `data/bankruptcies/BANKRUPTCY_JOBS_LOST.csv`)
rather than one combined CSV: the monthly, 21-year, 565-commune,
explicit-zero-filled series is 40.5 MB as a single file, over the project's
25 MB commit limit. Split, the two files measure 19.7 MB and 20.9 MB
(maintainer decision, 2026-09-23). The initial committed data was produced by
running the real sync against the live Statbel file once and offloading, the
same way ONEM and WalStat's own first committed CSVs were produced when they
became `in_db` stores (PR #147) — not left empty for the first daily run to
fill, and not the 6.5 MB source file itself, which is never committed.

## Out of scope (this batch)

Annual totals. Per-capita / per-local-unit rates. Commune-page surfacing
(`config/local_sections.yaml`). Any change to
`scripts/export_aggregates_csv.py`, `resolve_geo()`, the schema, or
`config/sources/statbel.yaml`.
