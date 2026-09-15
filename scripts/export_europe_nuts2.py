"""
Publish the Europe NUTS 2 batch's regional payloads -- Europe NUTS 2, batch
B2 (docs/features/europe_nuts2.md). Reads ONLY the committed store
(data/nuts2/{ID}.csv), the geography allowlist (config/geography/nuts2.csv,
nuts2_excluded.csv), the licence allowlist (config/geography/international.csv,
international_excluded.csv), the indicator configs, and the committed
geometry (public/data/geo/nuts2/2024/2.json) -- never SQLite, never a raw
source file (CLAUDE.md rule 20).

Writes:
  public/data/europe/nuts2/index.json               -- the three indicators,
      each's status, the geometry path and the attribution text.
  public/data/europe/nuts2/{indicator_id}.json       -- one per indicator,
      whether or not it has data (a "blocked" indicator still gets a file,
      with empty years/values and a `blocked_reason` -- CLAUDE.md rule 13:
      say why nothing loaded rather than omitting the file silently).

FIVE STATES, NEVER COLLAPSED (CLAUDE.md rule 26): a real number keeps its
own canonical status (final/provisional/estimate/revised) in `s`; an
Eurostat-flagged suppressed/na cell keeps that status (`v` stays null, `s`
carries the real reason); a region the allowlisted geometry carries but this
year's data does not is `"s": "missing"` (v null) -- NEVER a bare 0, NEVER
omitted from the `values` object. A region excluded by licence (a NUTS code
whose 2-letter prefix is not on config/geography/international.csv's
allowlist, e.g. every UK* region) gets NO entry in `values` at all -- it is
listed once, by code, in `excluded_by_licence`, which is NOT the same word
as 'suppressed' (that means Eurostat withheld the figure; this means
Eurostat published it and this pipeline chooses not to redistribute it).

GEOMETRY CROSS-CHECK (CLAUDE.md rule 13, the spec's own requirement): every
NUTS code this indicator has an observation row for must be either (a) in
the 2024 geometry, or (b) one of two documented no-outline lists: KNOWN_NO_OUTLINE
(FRY1-FRY5, PT20, PT30 -- see public/data/geo/nuts2/ATTRIBUTION.md for where
Nuts2json actually publishes their outlines, found while building this batch)
or SUPERSEDED_NUTS_VINTAGE_NO_OUTLINE (codes real in a long-history dataset
but retired from the 2024 NUTS classification entirely -- found while
unblocking POPULATION_NUTS2; see that constant's own comment for the
measured evidence). ANY OTHER data code with no geometry match FAILS THE
EXPORT outright -- the known cases are documented, the unknown case is
refused, never silently dropped or silently kept.

CLASS BREAKS are a display classification, computed once here (5-quantile,
deterministic), not a statistic and not recomputed per render -- see
`_quantile_breaks`'s own docstring for the exact method, an explicit
assumption this batch is making (docs/features/europe_nuts2.md,
"Assumptions").

Usage:  python scripts/export_europe_nuts2.py
        python scripts/export_europe_nuts2.py --out-dir SOME/OTHER/DIR
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.analytics.derived import growth_rate  # noqa: E402
from src.geography.international import load_excluded as load_international_excluded  # noqa: E402
from src.geography.international import load_international_rows  # noqa: E402
from src.geography.nuts2 import is_nuts2_code, load_nuts2_rows  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

DEFAULT_OUT_DIR = REPO_ROOT / "public" / "data" / "europe" / "nuts2"
GEOMETRY_PATH = REPO_ROOT / "public" / "data" / "geo" / "nuts2" / "2024" / "2.json"
#: Repo-relative path recorded IN the payload -- relative to public/data/,
#: matching how the rest of public/data/** cross-references itself.
GEOMETRY_PAYLOAD_PATH = "geo/nuts2/2024/2.json"
NUTS2_STORE_DIR = REPO_ROOT / "data" / "nuts2"
INDICATORS_DIR = REPO_ROOT / "config" / "indicators"
SOURCES_DIR = REPO_ROOT / "config" / "sources"

#: Verbatim from the eurostat-map bundle's own default map footnote
#: (docs/features/europe_nuts2.md, batch B1, grepped 2026-09-14) -- the
#: credit line Eurostat's own mapping tooling attaches to this geometry.
ATTRIBUTION_TEXT = "Administrative boundaries: ©EuroGeographics ©OpenStreetMap"

NUTS_VERSION = "2024"

#: NUTS 2 codes real in the data but absent from the 2024 level-2 geometry
#: file, verified against Nuts2json's own README (2026-09-14): each is
#: published as a separate per-territory "map inset" file instead (a
#: different URL pattern, not fetched by this batch -- out of scope, B3
#: builds the map). See public/data/geo/nuts2/ATTRIBUTION.md.
KNOWN_NO_OUTLINE = {
    "FRY1": "Guadeloupe -- published by Nuts2json as a separate map inset (GEO=GP), not in the level-2 continental file",
    "FRY2": "Martinique -- published by Nuts2json as a separate map inset (GEO=MQ), not in the level-2 continental file",
    "FRY3": "Guyane -- published by Nuts2json as a separate map inset (GEO=GF), not in the level-2 continental file",
    "FRY4": "La Réunion -- published by Nuts2json as a separate map inset (GEO=RE), not in the level-2 continental file",
    "FRY5": "Mayotte -- published by Nuts2json as a separate map inset (GEO=YT), not in the level-2 continental file",
    "PT20": "Azores -- published by Nuts2json as a separate map inset (GEO=PT20), not in the level-2 continental file",
    "PT30": "Madeira -- published by Nuts2json as a separate map inset (GEO=PT30), not in the level-2 continental file",
}

#: A SECOND, separately-evidenced no-outline category, found while unblocking
#: POPULATION_NUTS2 (2026-09-14): demo_r_pjanaggr3's `since: "1990"` fetch
#: reaches back far enough that Eurostat's own retroactive backfill runs out
#: for these codes -- each one's committed rows stop at a real transition
#: year (checked against the working database, not assumed): EL11-EL25 (a
#: single 2011 row each -- a one-off historical entry under the pre-2016
#: Greek NUTS 2 classification), HR04 (rows through 2016; Croatia's own
#: successor codes HR02/HR03/HR05/HR06 carry data from 2001-2013 onward,
#: i.e. Eurostat re-backfilled the split under the NEW codes), NL31/NL33
#: (rows through 2023; NL35/NL36 -- the Dutch reclassification the B1
#: coverage report already found, docs/features/europe_nuts2.md -- carry
#: data from 2014 onward), NO01/NO03-05 (rows through 2016; NO08-NO0B carry
#: data from 2010 onward), PT16-PT18 (rows through 2023; PT19/PT1A-PT1D --
#: the Portuguese reclassification the B1 report also already found --
#: carry data from 2014 onward). Unlike KNOWN_NO_OUTLINE, this is not
#: because Nuts2json publishes the outline somewhere else; it is because
#: the region, under this exact code, no longer exists in the 2024
#: classification at all -- the 2024 geometry legitimately has no outline
#: for it under any URL. A later batch that wants these years reachable
#: under their CURRENT codes needs a NUTS-vintage crosswalk, out of scope
#: here.
#:
#: Extended again unblocking UNEMPLOYMENT_RATE_NUTS2 (2026-09-14, same
#: evidence method -- rows stop at a transition year, a successor's rows
#: start there): HU10 (rows through 2012; HU11/HU12 from 2013), IE01/IE02
#: (rows through 2011; IE04/IE05/IE06 from 2012), LT00 (rows through 2012;
#: LT01/LT02 from 2013), SI01/SI02 (rows through 2009; SI03/SI04 from 2010).
_SUPERSEDED_NUTS_VINTAGE_CODES = (
    "EL11", "EL12", "EL13", "EL14", "EL21", "EL22", "EL23", "EL24", "EL25",
    "HR04", "NL31", "NL33", "NO01", "NO03", "NO04", "NO05", "PT16", "PT17", "PT18",
    "HU10", "IE01", "IE02", "LT00", "SI01", "SI02",
)  # fmt: skip
_SUPERSEDED_NUTS_VINTAGE_REASON = (
    "Pre-2024 NUTS 2 classification code with real historical observations; the 2024 "
    "geometry has no outline for it under any code (see export_europe_nuts2.py's own "
    "comment on SUPERSEDED_NUTS_VINTAGE_NO_OUTLINE for the measured evidence)."
)
SUPERSEDED_NUTS_VINTAGE_NO_OUTLINE = dict.fromkeys(
    _SUPERSEDED_NUTS_VINTAGE_CODES, _SUPERSEDED_NUTS_VINTAGE_REASON
)

#: Why a configured indicator has no data yet -- see
#: scripts/sync_nuts2.py's own module docstring for the full story. Keyed by
#: indicator_id; an indicator not in this dict is assumed loaded (its CSV
#: file existing or not is the actual, authoritative signal -- this text is
#: shown to a human, not used as a decision). Empty as of 2026-09-14: all
#: three configured indicators are now loaded -- UNEMPLOYMENT_RATE_NUTS2's
#: former entry here is gone, not merely stale, now that the maintainer's
#: empty-u-is-suppressed decision unblocked it (docs/decisions/0010-...md,
#: amendment). Kept as a dict, not removed outright, so a FUTURE indicator
#: that genuinely can't load yet has an obvious place to explain why.
BLOCKED_REASONS: dict[str, str] = {}

#: Same "croissance annuelle" toggle as export_europe_countries.py's own
#: YOY_INDICATOR_IDS (docs/features/europe_countries.md, 2026-09-15
#: amendment) -- level series only. UNEMPLOYMENT_RATE_NUTS2 is already a
#: rate and is deliberately excluded, matching the maintainer's country-
#: level decision.
YOY_INDICATOR_IDS = {"GDP_PC_PPS_NUTS2", "POPULATION_NUTS2"}


class ExportError(Exception):
    """A data-vs-geometry mismatch (or other invariant violation) this
    exporter refuses to paper over. CLAUDE.md rule 13: fail the export
    rather than ship an unstyled region, or a region silently dropped."""


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )


def _geometry_ids() -> set[str]:
    if not GEOMETRY_PATH.is_file():
        raise ExportError(f"No committed geometry at {GEOMETRY_PATH}")
    topo = json.loads(GEOMETRY_PATH.read_text(encoding="utf-8"))
    return {g["properties"]["id"] for g in topo["objects"]["nutsrg"]["geometries"]}


def _licence_split(codes: set[str]) -> tuple[set[str], set[str], set[str]]:
    """(kept, excluded_by_licence, unresolved) by 2-letter prefix, exactly
    the pilot's own rule (config/geography/international.csv /
    international_excluded.csv) -- see src/geography/nuts2.py's module
    docstring for why this is prefix-based, not nuts2.csv-row-based: a
    geometry-only code (e.g. Bosnia's BA01-BA03, which has an outline but no
    indicator has ever returned a value for it) is still a real, licence-
    allowed region and must render as 'missing', not be silently excluded
    for lack of a data row.
    """
    allowed_prefixes = {
        code
        for code, row in load_international_rows().items()
        if row["scope"] == "pilot" and len(code) == 2
    }
    excluded_prefixes = {c for c in load_international_excluded() if len(c) == 2}
    kept, excluded, unresolved = set(), set(), set()
    for code in codes:
        prefix = code[:2]
        if prefix in allowed_prefixes:
            kept.add(code)
        elif prefix in excluded_prefixes:
            excluded.add(code)
        else:
            unresolved.add(code)
    return kept, excluded, unresolved


def _quantile_breaks(values: list[float]) -> list[float]:
    """5-quantile class breaks (the boundaries BETWEEN the 5 classes, so 4
    numbers) over one year's real values, computed deterministically:
    sort, then linear interpolation at the 20/40/60/80th percentiles
    (the "R-7"/Excel method -- the same one Python's statistics.quantiles
    default(n=5) uses), rounded to 3 significant figures so the map's
    legend reads sensibly rather than showing Eurostat's full float
    precision. This is a DISPLAY CLASSIFICATION, not a statistic (CLAUDE.md
    rule 4/5 do not apply the way they would to a computed indicator) --
    stated as an assumption (docs/features/europe_nuts2.md, "Assumptions"):
    a later batch may prefer Jenks natural breaks instead.

    Fewer than 2 distinct values: returns [] -- there is nothing to break
    into 5 classes.
    """
    distinct = sorted(set(values))
    if len(distinct) < 2:
        return []
    ordered = sorted(values)
    n = len(ordered)
    breaks = []
    for q in (0.2, 0.4, 0.6, 0.8):
        pos = q * (n - 1)
        lo = int(pos)
        hi = min(lo + 1, n - 1)
        frac = pos - lo
        interpolated = ordered[lo] + (ordered[hi] - ordered[lo]) * frac
        breaks.append(_round_sensibly(interpolated))
    return breaks


def _round_sensibly(x: float) -> float:
    """3 significant figures -- enough to distinguish class boundaries
    across this batch's three units (PPS in the tens of thousands, percent
    in the tens, population in the millions) without pretending to more
    precision than a display legend needs. `digits` is deliberately allowed
    to go negative (round(76234.5, -2) == 76200.0) -- clamping it to 0 would
    round a 5-digit PPS figure to the nearest whole unit instead of the
    nearest hundred, which is not 3 significant figures at all."""
    if x == 0:
        return 0.0
    from math import floor, log10

    digits = 2 - int(floor(log10(abs(x))))
    return round(x, digits)


def _load_store_rows(indicator_id: str) -> list[dict] | None:
    csv_path = NUTS2_STORE_DIR / f"{indicator_id}.csv"
    if not csv_path.is_file():
        return None
    with csv_path.open(encoding="utf-8", newline="") as fh:
        return [row for row in csv.DictReader(fh) if row["is_latest"] == "1"]


def _retrieved_date(rows: list[dict]) -> str | None:
    """The latest fetch date, read from the store's own `vintage` column
    (an ISO timestamp) -- never wall-clock 'today', so a rebuild from the
    same committed CSV reproduces the same date (CLAUDE.md rule 35)."""
    if not rows:
        return None
    return max(row["vintage"] for row in rows)[:10]


def _attach_yoy(values_out: dict[str, dict[str, dict]], years: list[str]) -> None:
    """Same mechanism as export_europe_countries.py's own `_attach_yoy` (a
    separate parallel copy, not imported -- each Europe batch keeps its own,
    see this module's own docstring on why geography logic is duplicated
    rather than shared): mutates `values_out` (year -> code -> {"v", "s"})
    in place, adding a `yoy` percent-change key via
    src.analytics.derived.growth_rate, rounded to 1 decimal at export."""
    codes: set[str] = set()
    for year_values in values_out.values():
        codes.update(year_values)
    for code in codes:
        series = {y: values_out[y][code]["v"] for y in years if code in values_out[y]}
        for y in years:
            cell = values_out[y].get(code)
            if cell is None:
                continue
            yoy = growth_rate(series, y)
            cell["yoy"] = None if yoy is None else round(yoy, 1)


def _build_indicator_payload(
    indicator_id: str,
    indicator_cfg: dict,
    source_cfg: dict,
    nuts2_rows: dict[str, dict],
    geometry_ids_allowed: set[str],
    excluded_by_licence: set[str],
) -> dict:
    geo_id_to_code = {row["geo"]["geo_id"]: code for code, row in nuts2_rows.items()}
    names = {
        "en": indicator_cfg["name"]["en"],
        "fr": indicator_cfg["name"]["fr"],
        "nl": indicator_cfg["name"]["nl"],
    }
    fetch = indicator_cfg["fetch"]
    source_block = {
        "source_id": indicator_cfg["source_id"],
        "dataset": fetch["dataset"],
        "filters": dict(sorted((fetch.get("filters") or {}).items())),
    }

    has_yoy = indicator_id in YOY_INDICATOR_IDS

    rows = _load_store_rows(indicator_id)
    if rows is None:
        return {
            "indicator_id": indicator_id,
            "names": names,
            "unit": indicator_cfg["unit"],
            "source": {**source_block, "retrieved": None},
            "nuts_version": NUTS_VERSION,
            "status": "blocked",
            "blocked_reason": BLOCKED_REASONS.get(
                indicator_id, "Not loaded (no committed data/nuts2/*.csv yet)."
            ),
            "years": [],
            "latest_year": None,
            "values": {},
            "class_breaks": {},
            "excluded_by_licence": sorted(excluded_by_licence),
            "no_outline": {},  # same type as a loaded payload's (code -> reason), just empty
            "has_yoy": has_yoy,
        }

    by_year: dict[str, dict[str, dict]] = {}
    data_codes: set[str] = set()
    for row in rows:
        code = geo_id_to_code.get(row["geo_id"])
        if code is None:
            raise ExportError(
                f"{indicator_id}: {row['geo_id']!r} has a committed observation but no row in "
                "config/geography/nuts2.csv -- the store and the allowlist have drifted apart."
            )
        data_codes.add(code)
        value = None if row["value"] == "" else float(row["value"])
        by_year.setdefault(row["period"], {})[code] = {"v": value, "s": row["status"]}

    no_outline_reasons = {**KNOWN_NO_OUTLINE, **SUPERSEDED_NUTS_VINTAGE_NO_OUTLINE}
    no_outline_here = data_codes - geometry_ids_allowed - excluded_by_licence
    unknown_no_outline = no_outline_here - set(no_outline_reasons)
    if unknown_no_outline:
        raise ExportError(
            f"{indicator_id}: {sorted(unknown_no_outline)} have committed observations but no "
            "geometry match and are not in either documented no-outline list "
            f"(KNOWN_NO_OUTLINE: {sorted(KNOWN_NO_OUTLINE)}; "
            f"SUPERSEDED_NUTS_VINTAGE_NO_OUTLINE: {sorted(SUPERSEDED_NUTS_VINTAGE_NO_OUTLINE)}). "
            "This is a genuine surprise (CLAUDE.md rule 13) -- verify against Nuts2json and the "
            "code's own period history (SELECT MIN/MAX(period) ... WHERE geo_id=...) before "
            "adding it to either list."
        )

    # The universe every year's `values` covers: every licence-allowed
    # geometry region (whether or not this indicator has ever had a row for
    # it -- e.g. Bosnia's BA01-BA03 currently have an outline and no data at
    # all, and must still render as 'missing', not be silently omitted),
    # plus this indicator's own documented no-outline codes.
    value_universe = geometry_ids_allowed | no_outline_here

    years = sorted(by_year)
    values_out: dict[str, dict[str, dict]] = {}
    class_breaks: dict[str, list[float]] = {}
    for year in years:
        year_values: dict[str, dict] = {}
        real_numbers: list[float] = []
        for code in sorted(value_universe):
            cell = by_year[year].get(code)
            if cell is None:
                year_values[code] = {"v": None, "s": "missing"}
            else:
                year_values[code] = cell
                if cell["v"] is not None:
                    real_numbers.append(cell["v"])
        values_out[year] = year_values
        class_breaks[year] = _quantile_breaks(real_numbers)
    if has_yoy:
        _attach_yoy(values_out, years)

    latest_year = None
    for year in sorted(years, reverse=True):
        if any(cell["v"] is not None for cell in values_out[year].values()):
            latest_year = year
            break

    return {
        "indicator_id": indicator_id,
        "names": names,
        "unit": indicator_cfg["unit"],
        "source": {**source_block, "retrieved": _retrieved_date(rows)},
        "nuts_version": NUTS_VERSION,
        "status": "loaded",
        "years": years,
        "latest_year": latest_year,
        "values": values_out,
        "class_breaks": class_breaks,
        "excluded_by_licence": sorted(excluded_by_licence),
        "no_outline": {code: no_outline_reasons[code] for code in sorted(no_outline_here)},
        "has_yoy": has_yoy,
    }


def export(out_dir: Path = DEFAULT_OUT_DIR) -> list[str]:
    """Writes index.json and one {indicator_id}.json per configured NUTS 2
    indicator. Returns the list of indicator_ids processed, in id order."""
    indicator_configs, source_configs = load_and_validate_all(INDICATORS_DIR, SOURCES_DIR)
    nuts2_indicator_ids = sorted(
        code
        for code, cfg in indicator_configs.items()
        if cfg.get("source_id") == "eurostat" and "nuts2" in (cfg.get("geo_levels") or [])
    )
    if not nuts2_indicator_ids:
        raise ExportError("No config/indicators/*.yaml declares geo_levels: [nuts2]")

    geometry_ids = _geometry_ids()
    nuts2_rows = load_nuts2_rows()
    for code in nuts2_rows:
        if not is_nuts2_code(code):  # pragma: no cover -- load_nuts2_rows already enforces this
            raise ExportError(f"config/geography/nuts2.csv: {code!r} is not NUTS-2-shaped")
    geometry_ids_allowed, excluded_by_licence, unresolved = _licence_split(geometry_ids)
    if unresolved:
        raise ExportError(
            f"Geometry code(s) with a 2-letter prefix in neither international.csv nor "
            f"international_excluded.csv: {sorted(unresolved)} -- CLAUDE.md rule 13."
        )

    index_indicators = []
    for indicator_id in nuts2_indicator_ids:
        payload = _build_indicator_payload(
            indicator_id,
            indicator_configs[indicator_id],
            source_configs[indicator_configs[indicator_id]["source_id"]],
            nuts2_rows,
            geometry_ids_allowed,
            excluded_by_licence,
        )
        _write_json(out_dir / f"{indicator_id}.json", payload)
        index_indicators.append(
            {
                "id": indicator_id,
                "status": payload["status"],
                "payload": f"{indicator_id}.json",
            }
        )

    index_payload = {
        "nuts_version": NUTS_VERSION,
        "geometry": GEOMETRY_PAYLOAD_PATH,
        "attribution": ATTRIBUTION_TEXT,
        "indicators": index_indicators,
    }
    _write_json(out_dir / "index.json", index_payload)
    return nuts2_indicator_ids


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish the Europe NUTS 2 regional payloads")
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()
    try:
        indicator_ids = export(args.out_dir)
    except ExportError as exc:
        print(f"\nEXPORT FAILED: {exc}\n", file=sys.stderr)
        raise SystemExit(1) from exc
    print(f"Published {len(indicator_ids)} indicator(s) to {args.out_dir}: {indicator_ids}")


if __name__ == "__main__":
    main()
