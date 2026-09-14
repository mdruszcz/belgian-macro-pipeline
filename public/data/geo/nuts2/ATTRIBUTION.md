# NUTS 2 geometry -- attribution and provenance

## What this is

`2024/2.json` is the Nuts2json TopoJSON file for NUTS level 2 regions, 2024
classification vintage, EPSG:3035 (European LAEA) projection, 1:20M
resolution -- downloaded unchanged from
<https://github.com/eurostat/Nuts2json>, `pub/v2/2024/3035/20M/2.json>`, on
2026-09-14 (Europe NUTS 2 batch B2, `docs/features/europe_nuts2.md`).

It is kept exactly as downloaded (CLAUDE.md rule 12 -- committed as text,
574,331 bytes, well under the 25 MB limit). It is not reshaped, reprojected,
simplified further, or re-encoded by this pipeline.

sha256: `b0ea4ecc8b2eb5f218ddf4f70632446df76acdfcfc93542347425c79ea74416d`

## Credit line (use wherever this geometry is rendered)

> Administrative boundaries: &copy;EuroGeographics &copy;OpenStreetMap

Quoted verbatim from the `eurostat-map` library's own default map footnote
(`docs/features/europe_nuts2.md`, batch B1: found by grepping the downloaded
`eurostatmap.min.js` bundle, 2026-09-14) -- the credit line Eurostat's own
mapping tooling attaches to this geometry.

## Licence

Nuts2json itself: **EUPL-1.2** (its own `LICENSE`, confirmed by fetching it
from the Nuts2json repository, 2026-09-14).

The underlying NUTS boundary data is Eurostat/GISCO's own and carries its own
usage terms, quoted verbatim from the Nuts2json README's "Copyright"
section:

> The Eurostat NUTS dataset is copyrighted. There are specific provisions for
> the usage of this dataset which must be respected. The usage of these data
> is subject to their acceptance.

See
<https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts>
for the full provisions.

## Codes with no outline in this file

`FRY1`-`FRY5` (Guadeloupe, Martinique, Guyane, La R&eacute;union, Mayotte --
France's overseas NUTS 2 regions) and `PT20`/`PT30` (Azores, Madeira) do not
appear in this level-2 continental file. Nuts2json publishes them separately,
as per-territory "map inset" files at a different URL pattern
(`.../<YEAR>/<GEO>/<PROJECTION>/<SCALE>/<LEVEL>.json`, with `GEO` one of
`GP`, `MQ`, `GF`, `RE`, `YT`, `PT20`, `PT30` -- confirmed from the Nuts2json
README's own "Overseas territories - map insets" section, 2026-09-14), not
fetched or vendored in this batch (out of scope -- B3 builds the map). Their
indicator VALUES are still published in this batch's per-indicator payloads
(`public/data/europe/nuts2/*.json`), listed under `no_outline` with this same
reason, per the maintainer's standing decision until told otherwise.
