"""
scripts/report_nuts2_coverage.py

READ-ONLY coverage probe for the three candidate Eurostat NUTS 2 datasets
(GDP per capita in PPS, unemployment rate, population) proposed for the
Europe panel (batch B1, docs/features/europe_nuts2.md). Produces the
measured numbers that spec cites -- exact dimension codes, years, region
counts, status-flag counts, geometry-vintage match, and the licence
filter split -- so nothing in the spec is typed from memory.

WRITES NOTHING under config/, data/, or public/data/. Raw responses are
cached under a scratch directory (default: C:\\Users\\mdruszcz\\AppData\\
Local\\Temp\\bp-europe-cache\\, override with --cache), never under
data/raw/**.

REUSE, AND WHY ONLY THESE TWO PIECES OF THE ADAPTER ARE REUSED:
  - EurostatSource.build_url() (src/fetchers/eurostat.py) is a
    @staticmethod, pure string formatting. Reused as-is.
  - EurostatSource()._parse(raw, dataset=...) is pure: bytes in, list[dict]
    out, no I/O. Reused as-is -- this is the same JSON-stat cube walk and
    the same OBS_FLAG -> canonical-status mapping the daily pipeline uses,
    so this report's flag counts and row counts describe the exact data the
    real adapter would load, not a re-implementation that could drift from
    it. `_parse` requires every non-geo/time dimension to already be
    pinned to one code by the request's filters (else it raises
    FetchError) -- this script always fetches with the dataset's full
    filter set for that reason, matching the shape an indicator config's
    `fetch.filters` would use.
  - DataSource._get_with_retry() (src/fetchers/base.py) is ALSO reused: it
    is a plain GET-with-retry over `requests`, writes nothing to disk,
    logs only through the stdlib `logging` module, and never touches
    `fetch_runs`. The two methods that do have side effects,
    `DataSource.fetch()` (calls `_cache_raw` + `_log_run`) and `_cache_raw`
    itself (writes under data/raw/**), are deliberately NEVER called here.
    This script does its own caching, into --cache, using plain
    `Path.write_bytes`.
  - Everything else in this file (NUTS 2 code recognition, the licence
    split, the geometry-vs-data code diff) is new, pure, and unit-tested in
    tests/test_report_nuts2_coverage.py with inline fixtures -- none of it
    exists in the adapter today.

Usage:
  .venv/Scripts/python.exe scripts/report_nuts2_coverage.py
  .venv/Scripts/python.exe scripts/report_nuts2_coverage.py --use-cache
  .venv/Scripts/python.exe scripts/report_nuts2_coverage.py --cache DIR
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.fetchers.base import FetchError  # noqa: E402
from src.fetchers.eurostat import EurostatSource, _strides  # noqa: E402
from src.geography.international import (  # noqa: E402
    INTERNATIONAL_CSV,
    INTERNATIONAL_EXCLUDED_CSV,
    load_excluded,
    load_international_rows,
)

DEFAULT_CACHE = Path(r"C:\Users\mdruszcz\AppData\Local\Temp\bp-europe-cache")
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
NUTS2JSON_RAW_BASE = "https://raw.githubusercontent.com/eurostat/Nuts2json/master/pub/v2"

#: The three candidates, verified dimension-by-dimension against a live
#: fetch on 2026-09-14 (see docs/features/europe_nuts2.md, "Coverage" --
#: every code below was read off the response's own `dimension` block, not
#: assumed from Eurostat's documentation).
DATASETS: dict[str, dict] = {
    "GDP_PC_PPS": {
        "dataset": "nama_10r_2gdp",
        "filters": {"unit": "PPS_EU27_2020_HAB"},
        "label": "GDP per capita, PPS (EU27=2020 basis), per inhabitant",
    },
    "UNEMPLOYMENT_RATE": {
        "dataset": "lfst_r_lfu3rt",
        "filters": {"isced11": "TOTAL", "sex": "T", "age": "Y15-74", "unit": "PC"},
        "label": "Unemployment rate, 15-74, total, percent",
    },
    "POPULATION": {
        "dataset": "demo_r_pjanaggr3",
        "filters": {"sex": "T", "age": "TOTAL", "unit": "NR"},
        "label": "Population on 1 January, total, number",
    },
}

GEOMETRY_YEARS = ["2021", "2024"]
GEOMETRY_LEVEL = "2"
GEOMETRY_PROJECTION = "3035"
GEOMETRY_RESOLUTION = "20M"


# --------------------------------------------------------------------------
# Pure functions -- unit tested in tests/test_report_nuts2_coverage.py
# --------------------------------------------------------------------------


def is_nuts2_code(code: str) -> bool:
    """A NUTS 2 code is exactly 4 characters: a 2-letter country prefix
    (uppercase ASCII letters) followed by 2 more code characters (Eurostat
    mixes digits and letters here, e.g. 'BE10', 'LU00', 'TRA1' -- Turkey's
    codes are letter-led at NUTS1 and mixed at NUTS2/3). Deliberately
    length-only plus a letter-prefix check: this dataset's `geo` dimension
    mixes NUTS 0/1/2/3 codes and this pipeline has no independent NUTS
    level table to check against, so 4 characters is the only structural
    signal available (verified against nama_10r_2gdp's own geo list,
    2026-09-14: 3-char entries are NUTS1/country, 5-char entries are NUTS3).
    """
    return len(code) == 4 and code[:2].isalpha() and code[:2].isupper()


#: The regional-position suffix Eurostat uses for a pseudo-region that maps
#: to no real NUTS 2 outline. Two different conventions, found by comparing
#: the three candidate datasets' own JSON-stat labels (2026-09-14):
#:   'ZZ' -- national-accounts 'extra-regio' (activity not attributable to
#:           any real region: offshore, embassies, etc.), e.g. 'DEZZ'.
#:   'XX' -- demography's 'Not regionalised/Unknown NUTS 2', e.g. 'FRXX'
#:           (label read verbatim from demo_r_pjanaggr3's geo dimension:
#:           "Not regionalised/Unknown level 2"). NOT the same concept as
#:           extra-regio, but the same practical requirement: no outline,
#:           must never be counted as a 566th region or silently kept.
#: '00' is deliberately NOT here -- it is a real single-region country's
#: own NUTS 2 code (LU00, IS00, LI00), not a pseudo-region marker.
_PSEUDO_REGION_SUFFIXES = frozenset({"ZZ", "XX"})


def is_extra_regio(code: str) -> bool:
    """True for a structurally NUTS2-shaped code that is actually one of
    Eurostat's pseudo-regions (see _PSEUDO_REGION_SUFFIXES) rather than a
    real, mappable NUTS 2 area."""
    return is_nuts2_code(code) and code[2:] in _PSEUDO_REGION_SUFFIXES


def nuts2_country_prefix(code: str) -> str:
    """The 2-letter country prefix of a NUTS 2 code. Not validated here --
    callers apply is_nuts2_code() first."""
    return code[:2]


def split_by_licence(
    codes: set[str], allowed_prefixes: set[str], excluded_codes: set[str]
) -> tuple[set[str], dict[str, set[str]], set[str]]:
    """Apply the international pilot's licence rule (docs/features/
    international.md, 'Geography'; ADR 0008 decision 3) at NUTS 2
    granularity, by country prefix rather than by whole-country code:
    kept   -- prefix is in the allowlist (international.csv).
    dropped -- {country_code: {nuts2 codes}} for prefixes explicitly in
               international_excluded.csv (UK, XK, US, JP, ...).
    unresolved -- prefixes in NEITHER file. Per the pilot's own rule
               (international.py's module docstring, CLAUDE.md rule 13),
               this must never be silently dropped OR silently kept --
               the caller reports it as a decision still to be made.
    `excluded_codes` here is the set of 2-letter codes international_
    excluded.csv lists directly (it is written at country granularity,
    e.g. 'UK', not 'UK21').
    """
    kept: set[str] = set()
    dropped: dict[str, set[str]] = defaultdict(set)
    unresolved: set[str] = set()
    for code in codes:
        prefix = nuts2_country_prefix(code)
        if prefix in allowed_prefixes:
            kept.add(code)
        elif prefix in excluded_codes:
            dropped[prefix].add(code)
        else:
            unresolved.add(code)
    return kept, dict(dropped), unresolved


def geometry_ids(topojson: dict, object_name: str = "nutsrg") -> set[str]:
    """The set of region ids (NUTS codes) a Nuts2json TopoJSON file's region
    layer carries. `nutsrg` is the filled-region layer (as opposed to
    `nutsbn`, boundary arcs keyed by internal ids, not NUTS codes) --
    verified against both downloaded files, 2026-09-14."""
    geoms = topojson["objects"][object_name]["geometries"]
    return {g["properties"]["id"] for g in geoms}


def raw_flag_census(raw: bytes) -> Counter:
    """A dependency-free census of the RAW OBS_FLAG strings a JSON-stat cube
    carries, bypassing EurostatSource._parse()'s canonical-status mapping
    entirely. Needed because _parse() refuses (FetchError) the moment it
    meets a flag its FLAG_STATUS table does not recognize -- correct
    behaviour for the production adapter (CLAUDE.md rule 13: fail loudly
    rather than guess), but it means _parse() cannot be used to find out
    WHAT flags a candidate dataset actually carries before that table is
    extended. This walks the same cube, with the same linear-offset
    arithmetic EurostatSource._parse uses (_strides, reused verbatim), but
    only ever records the flag string -- it never maps it to a status and
    never raises. Used only for the coverage report; the production adapter
    still goes through _parse and still refuses unmapped flags.
    """
    data = json.loads(raw)
    dims: list[str] = data["id"]
    sizes: list[int] = data["size"]
    status: dict = data.get("status") or {}
    strides = _strides(dims, sizes)
    geo_stride = strides["geo"]
    time_stride = strides["time"]
    geo_index = data["dimension"]["geo"]["category"]["index"]
    time_index = data["dimension"]["time"]["category"]["index"]
    census: Counter = Counter()
    for _geo_code, geo_pos in geo_index.items():
        for _period, time_pos in time_index.items():
            offset = str(geo_pos * geo_stride + time_pos * time_stride)
            flag = status.get(offset)
            if flag is not None:
                census[flag] += 1
    return census


def raw_rows(raw: bytes) -> list[dict]:
    """Fallback row walk used only when EurostatSource._parse() refuses a
    response over an unrecognized OBS_FLAG (see raw_flag_census). Same cube
    walk, same linear-offset arithmetic, but `obs_status` is the RAW flag
    string verbatim ('' when unflagged) rather than a canonical status --
    this is explicitly not the production contract and callers must label
    it as such."""
    data = json.loads(raw)
    dims: list[str] = data["id"]
    sizes: list[int] = data["size"]
    values: dict = data.get("value") or {}
    status: dict = data.get("status") or {}
    strides = _strides(dims, sizes)
    geo_stride = strides["geo"]
    time_stride = strides["time"]
    geo_index = data["dimension"]["geo"]["category"]["index"]
    time_index = data["dimension"]["time"]["category"]["index"]
    rows: list[dict] = []
    for geo_code, geo_pos in geo_index.items():
        for period, time_pos in time_index.items():
            offset = str(geo_pos * geo_stride + time_pos * time_stride)
            raw_value = values.get(offset)
            flag = status.get(offset, "")
            if raw_value is None and flag == "":
                continue
            rows.append(
                {
                    "geo": geo_code,
                    "period": str(period),
                    "value": None if raw_value is None else float(raw_value),
                    "obs_status": flag,
                }
            )
    return rows


def diff_codes(data_codes: set[str], geometry_codes: set[str]) -> dict[str, set[str]]:
    """codes in data but not geometry, and vice versa -- the two mismatch
    directions a choropleth must never paper over: a data code with no
    outline cannot be drawn, and an outline with no data must render as
    the explicit 'missing' state, not silently unstyled."""
    return {
        "in_data_not_geometry": data_codes - geometry_codes,
        "in_geometry_not_data": geometry_codes - data_codes,
    }


# --------------------------------------------------------------------------
# Fetch / cache (side-effecting, but read-only: no DB, no data/raw/**,
# no fetch_runs -- see module docstring)
# --------------------------------------------------------------------------


def _cache_path(cache_dir: Path, name: str) -> Path:
    return cache_dir / name


def fetch_cached(url: str, cache_file: Path, *, use_cache: bool) -> tuple[bytes, float, bool]:
    """Returns (raw_bytes, elapsed_seconds, was_cached). elapsed_seconds is
    0.0 for a cache hit (nothing was measured)."""
    if use_cache and cache_file.is_file():
        return cache_file.read_bytes(), 0.0, True
    source = EurostatSource()
    started = time.monotonic()
    raw, _http_status = source._get_with_retry(
        url
    )  # noqa: SLF001 -- deliberate reuse, see module docstring
    elapsed = time.monotonic() - started
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_bytes(raw)
    return raw, elapsed, False


def fetch_geometry_cached(
    url: str, cache_file: Path, *, use_cache: bool
) -> tuple[bytes, float, bool]:
    """Same contract as fetch_cached, but geometry is a plain static file on
    raw.githubusercontent.com, not an Eurostat dataset query, so
    EurostatSource is not involved.

    Shells out to curl rather than using `requests.get` directly: on this
    machine, `requests`' bundled certifi trust store does not include the
    corporate TLS-inspecting proxy's root certificate, so a direct
    `requests.get` to raw.githubusercontent.com fails with
    CERTIFICATE_VERIFY_FAILED (verified 2026-09-14) even though the chain
    is genuinely valid -- Windows' own certificate store (which curl's
    schannel backend reads) already trusts that root. `--ssl-no-revoke`
    only skips the OCSP/CRL revocation check curl otherwise cannot complete
    through the same proxy; it does not skip certificate validation.
    ec.europa.eu (the Eurostat dataset fetches above) is not behind this
    proxy and needs none of this -- confirmed by the same run succeeding
    for all three datasets via plain `requests` before this function is
    ever called.
    """
    if use_cache and cache_file.is_file():
        return cache_file.read_bytes(), 0.0, True
    import subprocess

    started = time.monotonic()
    result = subprocess.run(
        ["curl", "--ssl-no-revoke", "-sS", "-m", "30", "-f", url],
        capture_output=True,
        check=True,
    )
    elapsed = time.monotonic() - started
    content = result.stdout
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_bytes(content)
    return content, elapsed, False


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def analyze_dataset(key: str, spec: dict, cache_dir: Path, use_cache: bool) -> dict:
    source = EurostatSource()
    url = EurostatSource.build_url(BASE_URL, spec["dataset"], spec["filters"], since="1975")
    cache_file = _cache_path(cache_dir, f"{spec['dataset']}_probe.json")
    raw, elapsed, cached = fetch_cached(url, cache_file, use_cache=use_cache)
    size_bytes = len(raw)

    data = json.loads(raw)
    dims = data["id"]
    dim_codes = {
        d: sorted(data["dimension"][d]["category"]["index"].keys())
        for d in dims
        if d not in ("geo", "time")
    }

    parse_error: str | None = None
    flag_census: dict[str, int] | None = None
    try:
        rows = source._parse(raw, dataset=spec["dataset"])  # noqa: SLF001 -- see module docstring
        raw_status_fallback = False
    except FetchError as exc:
        # The production adapter refuses this response outright (an OBS_FLAG
        # its FLAG_STATUS table does not recognize -- CLAUDE.md rule 13).
        # That refusal is itself the headline finding for this dataset; the
        # coverage numbers below still need to be produced, from the raw
        # cube walk, so the report can say precisely what is being refused.
        parse_error = str(exc)
        flag_census = dict(raw_flag_census(raw))
        rows = raw_rows(raw)
        raw_status_fallback = True
    all_geo_codes = {r["geo"] for r in rows}
    nuts2_all = {c for c in all_geo_codes if is_nuts2_code(c)}
    extra_regio = {c for c in nuts2_all if is_extra_regio(c)}
    nuts2_real = nuts2_all - extra_regio

    years = sorted({r["period"] for r in rows})
    per_year_regions: dict[str, int] = {}
    per_year_status: dict[str, Counter] = {}
    per_year_codes: dict[str, set[str]] = {}
    for year in years:
        year_rows = [r for r in rows if r["period"] == year and r["geo"] in nuts2_real]
        year_codes = {r["geo"] for r in year_rows}
        per_year_regions[year] = len(year_codes)
        per_year_status[year] = Counter(r["obs_status"] for r in year_rows)
        per_year_codes[year] = year_codes
    latest_year = years[-1] if years else None

    return {
        "key": key,
        "dataset": spec["dataset"],
        "label": spec["label"],
        "url": url,
        "size_bytes": size_bytes,
        "elapsed_seconds": round(elapsed, 3),
        "cached": cached,
        "dims": dims,
        "dim_codes": dim_codes,
        "frequency": dim_codes.get("freq"),
        "first_year": years[0] if years else None,
        "last_year": years[-1] if years else None,
        "n_years": len(years),
        "n_geo_total": len(all_geo_codes),
        "n_nuts2_total": len(nuts2_all),
        "n_extra_regio": len(extra_regio),
        "extra_regio_codes": sorted(extra_regio),
        "n_nuts2_real": len(nuts2_real),
        "nuts2_codes": nuts2_real,
        "latest_year": latest_year,
        "latest_year_codes": per_year_codes.get(latest_year, set()),
        "per_year_regions": per_year_regions,
        "per_year_status": {y: dict(c) for y, c in per_year_status.items()},
        "parse_error": parse_error,
        "raw_status_fallback": raw_status_fallback,
        "raw_flag_census": flag_census,
    }


def analyze_geometry(cache_dir: Path, use_cache: bool) -> dict[str, dict]:
    out = {}
    for year in GEOMETRY_YEARS:
        url = f"{NUTS2JSON_RAW_BASE}/{year}/{GEOMETRY_PROJECTION}/{GEOMETRY_RESOLUTION}/{GEOMETRY_LEVEL}.json"
        cache_file = _cache_path(cache_dir, f"nuts2json_{year}_lvl2.json")
        raw, elapsed, cached = fetch_geometry_cached(url, cache_file, use_cache=use_cache)
        topo = json.loads(raw)
        ids = geometry_ids(topo)
        out[year] = {
            "url": url,
            "size_bytes": len(raw),
            "elapsed_seconds": round(elapsed, 3),
            "cached": cached,
            "n_regions": len(ids),
            "ids": ids,
        }
    return out


def licence_report(nuts2_codes: set[str], international_csv: Path, excluded_csv: Path) -> dict:
    international_rows = load_international_rows(international_csv)
    allowed_prefixes = {
        code
        for code, row in international_rows.items()
        if row["scope"] == "pilot" and len(code) == 2
    }
    excluded_codes = set(load_excluded(excluded_csv))
    kept, dropped, unresolved = split_by_licence(nuts2_codes, allowed_prefixes, excluded_codes)
    return {
        "kept": kept,
        "dropped": dropped,
        "unresolved": unresolved,
        "allowed_prefixes": sorted(allowed_prefixes),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument(
        "--use-cache", action="store_true", help="reuse a previously cached response, no network"
    )
    args = parser.parse_args()
    args.cache.mkdir(parents=True, exist_ok=True)

    report: dict = {"datasets": {}, "geometry": {}, "licence": {}}

    for key, spec in DATASETS.items():
        print(f"--- {key} ({spec['dataset']}) ---", file=sys.stderr)
        result = analyze_dataset(key, spec, args.cache, args.use_cache)
        report["datasets"][key] = result
        lic = licence_report(result["nuts2_codes"], INTERNATIONAL_CSV, INTERNATIONAL_EXCLUDED_CSV)
        report["licence"][key] = lic

    print("--- geometry (Nuts2json level 2, 2021 & 2024) ---", file=sys.stderr)
    geometry = analyze_geometry(args.cache, args.use_cache)
    report["geometry"] = geometry

    for year_a, year_b in [("2021", "2024")]:
        report[f"geometry_diff_{year_a}_vs_{year_b}"] = {
            "in_2021_not_2024": sorted(geometry[year_a]["ids"] - geometry[year_b]["ids"]),
            "in_2024_not_2021": sorted(geometry[year_b]["ids"] - geometry[year_a]["ids"]),
        }

    for key, result in report["datasets"].items():
        for year in GEOMETRY_YEARS:
            # All-years-ever-seen diff (broad signal of code churn across
            # the dataset's whole history).
            diff = diff_codes(result["nuts2_codes"], geometry[year]["ids"])
            report["datasets"][key][f"geometry_diff_{year}"] = {
                k: sorted(v) for k, v in diff.items()
            }
            # The diff that actually decides the geometry vintage: this
            # dataset's own latest published year against each candidate
            # geometry vintage. "All years ever seen" mixes in codes a
            # dataset used a decade ago and has since renamed -- the panel
            # only ever shows one year at a time, latest by default, so
            # this is the number the recommendation is based on.
            latest_diff = diff_codes(result["latest_year_codes"], geometry[year]["ids"])
            report["datasets"][key][f"latest_year_geometry_diff_{year}"] = {
                k: sorted(v) for k, v in latest_diff.items()
            }

    def _default(o):
        if isinstance(o, set):
            return sorted(o)
        raise TypeError

    print(json.dumps(report, indent=2, default=_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
