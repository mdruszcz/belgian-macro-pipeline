# Feature: FWB school-site ISE classes, per commune

Status: **implemented** (this batch). Catalogue: docs/data_catalog.md, "FWB school-site ISE
classes" and "FWB AGE site register (FASE)" — the second row added by this batch, requested by
the maintainer 2026-09-26.

## Sources

1. **ISE classes** (`fwb-age_classes-dise-pour-les-implantations-scolaires`, year 2025, 4,005
   rows). Per school site × formula: the differentiated-support (ED) class ("1".."20", "3a",
   "3b" or null) or specialised (HED) class. FO (fondamental ordinaire) and SO (secondaire
   ordinaire) are different calculation formulas; their class numbers are not comparable to
   each other.
2. **Site register** ("FASE 2026", `fwb-age-fichier-signaletique-des-etablissements-...`,
   8,378 rows, one per site × level). Carries each site's commune name (French), network
   (`reseau`), level (`niveau`) and coordinates, but no NIS code and no ISE class.

## Join

`scripts/export_schools_ise.py` fetches both live (same `urllib` + User-Agent pattern as the
original ISE-only export), joins on the integer FASE site ID, and writes
`public/data/schools_ise_2025.json`. A site with no register row keeps `nis: null` and is
counted in the payload's `unassigned_sites`; if the unmatched share of ISE records exceeds 2%
the run refuses outright (measured 2026-09-26: 48 / 4,005 = 1.2%, so today's run always
succeeds, but a future schema drift that produced a much larger gap would be refused, not
silently published).

## Commune name resolution

The register's commune name is resolved to a NIS code the same way `scripts/sync_ipp_rate.py`
resolves SPF Finances' commune names: NFKD accent stripping, case folding, non-alphanumeric
collapse, matched against `name_nl`/`name_fr` in `config/geography/geographies.csv` valid on
1 January 2026. Two additions specific to this source:

- **Typographic-apostrophe folding** (’ → ') happens before normalising, because the register
  spells "Braine-l’Alleud", "Fontaine-l’Evêque" and "Mont-de-l’Enclus" with the typographic
  apostrophe while `geographies.csv` uses the straight one.
- **Merger-lineage fallback.** `geographies.csv`'s own `successor_geo_id` column is empty in
  every row of the committed file (a pre-existing gap in that CSV's own derivation, not touched
  by this batch — the live database does carry it correctly). A name that matches nothing on
  the current map is looked up in `config/geography/municipality_crosswalk.csv` (the file that
  already exists to record merger lineage) by its old name, and followed to `new_nis`. This is
  a single hop only. "Bertogne" (11 sites, merged into Bastogne 2024-12-02) is the one name that
  needs this today.

One explicit override, matching `sync_ipp_rate.py`'s own: "Saint-Nicolas" resolves to the
Liège-province commune (NIS 62093), never Sint-Niklaas, which the register lists under its own
Dutch name. Any other name matching zero or more than one commune, on both the current map and
the crosswalk, refuses the whole run and lists every such name. No fuzzy matching.

## Per-commune summary

`scripts/export_schools_by_commune.py` reads `schools_ise_2025.json` only (never the ODWB
source directly) and writes `public/data/schools/by_commune.json`, keyed by NIS, one entry per
commune with at least one site:

- `sites`, `site_ids` — every distinct site assigned to the commune.
- `FO` / `SO`, each: `sites` (sites under that formula), `sites_with_class` (of those, how many
  carry a non-null ED class), `mean_class` (arithmetic mean over `sites_with_class`, "3a"/"3b"
  counted as 3, rounded to 1 decimal, `null` — never 0 — when `sites_with_class` is 0),
  `min_class`, `max_class`.

FO and SO are never averaged together. No Belgium, region or province figure is computed: an
ISE class is neither an additive count nor a ratio with a defensible aggregation formula, so
nothing above commune level is published for it.

## States

Three, kept distinct end to end:

- **Has sites** — the commune has an entry in `by_commune.json`.
- **In the FWB area, zero sites** — a Wallonia or Brussels-Capital Region commune (per its own
  `public/data/communes/<nis>.json` `region` field) absent from `by_commune.json`. Not the same
  as suppressed or unavailable; it means no FWB school site was assigned to this commune.
- **Not applicable** — a Flanders commune. Decided from the commune payload's own `region`
  field, never from the mere absence of a row (CLAUDE.md rule 26).

## Pages

- `ecoles-ise.html`: unchanged site-level search/filter, plus a commune `<select>` (built from
  `by_commune.json`'s own commune names, current language), a per-commune summary line (sites,
  mean FO class, mean SO class) and the FO/SO/rank caveat in fr/en/nl. Sites with no NIS appear
  under "commune unknown".
- `commune.html`: a new Schools panel (`assets/i18n.js` keys `cpSchools*`), following the
  `pyramidPanel`/`renderAgeSex` `bp-state` pattern: `data-state` is `unavailable` when
  `by_commune.json` has no row for the commune's NIS and the commune's region is Wallonia or
  Brussels-Capital, `not-applicable` when the region is Flanders, and `ready` otherwise. Fetches
  `public/data/schools/by_commune.json` once, alongside the other side payloads.

## Not done in this batch

No map (a later step may use the register's coordinates with the shared map engine). No
Belgium/region/province aggregate — refused by design, not a gap. No new indicator in the
observations store. No SQLite involved anywhere in the join or the page.
