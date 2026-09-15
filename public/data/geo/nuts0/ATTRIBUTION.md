# NUTS 0 (country) geometry -- attribution and provenance

## What this is

`2024/0.json` is the Nuts2json TopoJSON file for NUTS level 0 (whole-country)
outlines, 2024 classification vintage, EPSG:3035 (European LAEA) projection,
1:20M resolution -- the same release as `public/data/geo/nuts2/2024/2.json`
(Europe NUTS 2 batch B2), fetched for the Europe countries batch
(`docs/features/europe_countries.md`) so the map's country mode needs no new
remote host: same publisher, same vintage, one level up.

Downloaded unchanged from Eurostat's own GISCO cache mirror of Nuts2json,
`https://ec.europa.eu/eurostat/cache/GISCO/pub/nuts2json/v2/2024/3035/20M/0.json`,
on 2026-09-15 (`raw.githubusercontent.com/eurostat/Nuts2json` -- the mirror
B2 also documents -- did not respond on this network; the `ec.europa.eu`
cache mirror did, and is the same file Nuts2json itself publishes).

It is kept exactly as downloaded (CLAUDE.md rule 12 -- committed as text,
294,569 bytes, well under the 25 MB limit). It is not reshaped, reprojected,
simplified further, or re-encoded by this pipeline.

sha256: `43ac926df21bcad84ef42d9d9d56dc6ab7b4c99372700c61790708480f4b611d`

## Coverage

39 country outlines: every EU/EFTA/candidate country the licence allowlist
(`config/geography/international.csv`) recognises EXCEPT Georgia (`GE`) and
Moldova (`MD`), which this geometry vintage does not carry at all -- both
render in country mode as `no_outline` (same status word and reasoning as
`public/data/europe/nuts2/*.json`'s `no_outline`, `scripts/export_europe_nuts2.py`'s
own module docstring), never silently dropped. Kosovo (`XK`) has an outline
here (`XK`) but stays licence-excluded, exactly as `international_excluded.csv`
already decides for every other payload. The United Kingdom has NO outline
in this 2024 vintage at all (dropped, same finding `docs/features/europe_nuts2.md`
already recorded for the NUTS 2 file) -- moot, since `UK` is licence-excluded
regardless.

## Credit line (use wherever this geometry is rendered)

> Administrative boundaries: &copy;EuroGeographics &copy;OpenStreetMap

Same credit line as `public/data/geo/nuts2/2024/2.json` -- see that
directory's own `ATTRIBUTION.md` for where it was found (Eurostat's own
`eurostat-map` library bundle).

## Licence

Identical terms to `public/data/geo/nuts2/2024/2.json` -- Nuts2json itself
EUPL-1.2, the underlying NUTS boundary data Eurostat/GISCO's own, subject to
its usage provisions. See that file's `ATTRIBUTION.md` for the full text.
