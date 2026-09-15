"""
Publish the Europe countries batch's payloads -- the macro.html Europe
panel's country mode (docs/features/europe_countries.md): a NUTS 0
choropleth plus the "Comparaison internationale" comparison charts. Reads
ONLY the committed `international` store (data/international/{ID}.csv), the
licence allowlist (config/geography/international.csv,
international_excluded.csv), the indicator configs, and the committed NUTS 0
geometry (public/data/geo/nuts0/2024/0.json) -- never SQLite, never a raw
source file (CLAUDE.md rule 20).

Two kinds of indicator, seven total:

  MAP indicators (6) -- GDP_PC_PPS_COUNTRY, POPULATION_COUNTRY,
  UNEMPLOYMENT_RATE_EUROPE, HICP_ANNUAL_RATE_EUROPE, GOV_DEBT_EUROPE,
  CONSUMER_CONFIDENCE_EUROPE -- painted on the choropleth AND drawn in the
  comparison charts. Each country's own published figure; the two EU/EFTA
  aggregates (EU27_2020, EA21 -- whichever the indicator's own dataset
  actually carries) are never painted on the map (no polygon exists for
  them) but ARE published as `reference_lines`, for the comparison chart's
  reference-line toggle.

  CHART-ONLY indicator (1) -- GDP_VOLUME_EUROPE, a level in million EUR, not
  comparable across countries on a map (a small country and a large one
  cannot share one choropleth scale for a raw level). Published only as a
  REBASED INDEX (average of the four real 2015 quarters = 100), computed
  here in Python (CLAUDE.md rules 4/6: never in the browser, never written
  to observations) -- see `_rebase_to_2015_index`'s own docstring for the
  exact method and tests/test_export_europe_countries.py for the
  hand-computed expected values (rule 5). A country missing any of its four
  2015 quarters gets `"s": "na"` for its ENTIRE series (the maintainer's
  decision, docs/features/europe_countries.md) -- never rebased on another
  year. Carries an `adapted` notice (grade B, src/exporters/provenance.py's
  own GRADES["B"] text -- international.md point 4: adapted data must say so
  prominently).

FIVE STATES, NEVER COLLAPSED (CLAUDE.md rule 26), same as
scripts/export_europe_nuts2.py: a real number keeps its own canonical status
in `s`; an Eurostat-flagged suppressed/na cell keeps that status (`v` stays
null); an allowlisted country this indicator's data does not cover is
`"s": "missing"` (v null); a country excluded by licence (XK: Kosovo has an
outline in this geometry but is not on the licence allowlist) gets NO entry
in `values` at all, listed once in `excluded_by_licence`.

GEOMETRY CROSS-CHECK: every allowlisted country code must be either in the
committed NUTS 0 geometry or in the documented NO_OUTLINE list (GE, MD --
absent from this Nuts2json 2024 vintage entirely, found live 2026-09-15,
see public/data/geo/nuts0/ATTRIBUTION.md). Any other geometry mismatch fails
the export outright (rule 13).

CLASS BREAKS reuse export_europe_nuts2.py's own `_quantile_breaks`/
`_round_sensibly` (imported, not re-derived) -- a general-purpose display
classification with no NUTS2-specific behaviour, unlike the geography
resolution logic, which each Europe batch deliberately keeps as its own
parallel copy (see scripts/sync_nuts2.py's own module docstring for why).

Usage:  python scripts/export_europe_countries.py
        python scripts/export_europe_countries.py --out-dir SOME/OTHER/DIR
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from export_europe_nuts2 import _quantile_breaks  # noqa: E402

from src.analytics.derived import growth_rate  # noqa: E402
from src.exporters.provenance import GRADES  # noqa: E402
from src.geography.international import load_excluded, load_international_rows  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

DEFAULT_OUT_DIR = REPO_ROOT / "public" / "data" / "europe" / "countries"
GEOMETRY_PATH = REPO_ROOT / "public" / "data" / "geo" / "nuts0" / "2024" / "0.json"
GEOMETRY_PAYLOAD_PATH = "geo/nuts0/2024/0.json"
INTERNATIONAL_STORE_DIR = REPO_ROOT / "data" / "international"
INDICATORS_DIR = REPO_ROOT / "config" / "indicators"
SOURCES_DIR = REPO_ROOT / "config" / "sources"

#: Same credit line as public/data/europe/nuts2/*.json -- same publisher,
#: same vintage, one NUTS level up. See public/data/geo/nuts0/ATTRIBUTION.md.
ATTRIBUTION_TEXT = "Administrative boundaries: ©EuroGeographics ©OpenStreetMap"

GEO_VINTAGE = "2024"

#: The indicators painted on the choropleth AND drawn in the comparison
#: charts, in the order the map's indicator select and the comparison
#: card's small multiples both use: the original six first (what a reader
#: already knows), then the Eurostat additional domains batch
#: (docs/data_catalog.md, 2026-09-15) grouped by domain -- public finance,
#: employment, social inequalities, energy/transition, demography,
#: innovation. Wired into the panel on the maintainer's 2026-09-15 ask
#: ("brancher sur le panneau Europe en mode Pays").
#:
#: NOT here on purpose: POPULATION_EUROPE (demo_pjan) duplicates
#: POPULATION_COUNTRY (the NUTS 0 rows of demo_r_pjanaggr3, already on the
#: map) -- two "Population" entries with slightly different vintages would
#: read as a mistake, not a choice; it stays in the store, unpublished here.
MAP_INDICATOR_IDS = (
    "GDP_PC_PPS_COUNTRY",
    "POPULATION_COUNTRY",
    "UNEMPLOYMENT_RATE_EUROPE",
    "HICP_ANNUAL_RATE_EUROPE",
    "GOV_DEBT_EUROPE",
    "CONSUMER_CONFIDENCE_EUROPE",
    "GOV_BALANCE_EUROPE",
    "TAX_RECEIPTS_EUROPE",
    "GOV_EXPENDITURE_HEALTH_EUROPE",
    "GOV_DEBT_QUARTERLY_EUROPE",
    "EMPLOYMENT_LFS_EUROPE",
    "GINI_COEFFICIENT_EUROPE",
    "POVERTY_RATE_EUROPE",
    "POVERTY_SOCIAL_EXCLUSION_EUROPE",
    "RENEWABLE_ENERGY_SHARE_EUROPE",
    "GHG_EMISSIONS_EUROPE",
    "LIFE_EXPECTANCY_EUROPE",
    "POPULATION_GROWTH_RATE_EUROPE",
    "RD_EXPENDITURE_EUROPE",
    "RD_PERSONNEL_EUROPE",
)
#: Chart-only indicators -- never painted. A level in million euro is not
#: comparable across countries on a map (Germany vs Malta is a map of
#: country size, not of anything economic -- the reasoning the maintainer
#: approved for GDP_VOLUME_EUROPE in the countries batch); the same holds
#: for total value added and for exports/imports of goods and services.
#: Only GDP_VOLUME_EUROPE is rebased to an index (see module docstring);
#: the other three are drawn as the levels Eurostat publishes, with the
#: growth toggle as the cross-country reading.
REBASED_INDICATOR_IDS = ("GDP_VOLUME_EUROPE",)
LEVEL_CHART_ONLY_INDICATOR_IDS = (
    "VALUE_ADDED_TOTAL_EUROPE",
    "EXPORTS_GOODS_SERVICES_EUROPE",
    "IMPORTS_GOODS_SERVICES_EUROPE",
)
CHART_ONLY_INDICATOR_IDS = REBASED_INDICATOR_IDS + LEVEL_CHART_ONLY_INDICATOR_IDS
ALL_INDICATOR_IDS = MAP_INDICATOR_IDS + CHART_ONLY_INDICATOR_IDS

#: What the "Comparaison internationale" card shows before the reader
#: touches its indicator filter: the seven charts it showed before the
#: additional domains were wired in. Twenty-four small multiples at once is
#: a wall, not a comparison -- the rest are one checkbox away (the filter,
#: maintainer's 2026-09-15 ask: "25 indicateurs possiblement visibles en
#: dessous mais avec un filtre pour choisir"). Published as `compare_default`
#: on each index entry: a data binding the generic renderer reads, never an
#: id it knows (CLAUDE.md rules 2/24).
COMPARE_DEFAULT_IDS = frozenset(
    {
        "GDP_PC_PPS_COUNTRY",
        "POPULATION_COUNTRY",
        "UNEMPLOYMENT_RATE_EUROPE",
        "HICP_ANNUAL_RATE_EUROPE",
        "GOV_DEBT_EUROPE",
        "CONSUMER_CONFIDENCE_EUROPE",
        "GDP_VOLUME_EUROPE",
    }
)

#: Allowlisted countries with no outline in this Nuts2json 2024 NUTS0
#: vintage -- verified against a live fetch of
#: https://ec.europa.eu/eurostat/cache/GISCO/pub/nuts2json/v2/2024/3035/20M/0.json
#: (2026-09-15): 39 country ids, neither GE nor MD among them. See
#: public/data/geo/nuts0/ATTRIBUTION.md, "Coverage".
NO_OUTLINE = {
    "GE": "Georgia -- not in the 2024 Nuts2json NUTS 0 geometry vintage (found live, 2026-09-15).",
    "MD": "Moldova -- not in the 2024 Nuts2json NUTS 0 geometry vintage (found live, 2026-09-15).",
}

#: The four 2015 quarters GDP_VOLUME_EUROPE's index is rebased against.
BASE_QUARTERS_2015 = ("2015-Q1", "2015-Q2", "2015-Q3", "2015-Q4")

#: The "croissance annuelle" toggle (macro.html Europe panel, 2026-09-15
#: maintainer-requested follow-up, docs/features/europe_countries.md
#: amendment) is offered only on a LEVEL series, never on something that is
#: already a rate or a balance -- UNEMPLOYMENT_RATE_EUROPE,
#: HICP_ANNUAL_RATE_EUROPE, GOV_DEBT_EUROPE, CONSUMER_CONFIDENCE_EUROPE are
#: deliberately excluded (maintainer's explicit correction, after first
#: saying "all charts"). GDP_VOLUME_EUROPE is quarterly; every other member
#: here is annual -- both frequencies are handled by the SAME call below via
#: src.analytics.derived.growth_rate/shift_period_years, which shifts only
#: the YEAR component of a period string and so lands on the same quarter
#: one year back for free.
YOY_INDICATOR_IDS = {
    "GDP_PC_PPS_COUNTRY",
    "POPULATION_COUNTRY",
    "GDP_VOLUME_EUROPE",
    # Additional domains batch: the level series among them, by the same
    # rule. Not the rates/shares/balances (poverty, Gini, % of GDP, growth
    # rate, renewable share), not life expectancy (a level in years, but
    # "growth of life expectancy" is not a reading anyone uses).
    "EMPLOYMENT_LFS_EUROPE",
    "GHG_EMISSIONS_EUROPE",
    "RD_PERSONNEL_EUROPE",
    "VALUE_ADDED_TOTAL_EUROPE",
    "EXPORTS_GOODS_SERVICES_EUROPE",
    "IMPORTS_GOODS_SERVICES_EUROPE",
}

#: Trilingual "this number was changed by us" statement -- reused verbatim
#: from the grade vocabulary every other page's provenance badge already
#: uses (international.md point 4: the adapted-data notice must exist and
#: must be the one this pipeline already shows, not new copy invented here).
ADAPTED_NOTICE = GRADES["B"]["definition"]


class ExportError(Exception):
    """A data-vs-geometry mismatch (or other invariant violation) this
    exporter refuses to paper over (CLAUDE.md rule 13)."""


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


def _load_store_rows(indicator_id: str) -> list[dict] | None:
    csv_path = INTERNATIONAL_STORE_DIR / f"{indicator_id}.csv"
    if not csv_path.is_file():
        return None
    with csv_path.open(encoding="utf-8", newline="") as fh:
        return [row for row in csv.DictReader(fh) if row["is_latest"] == "1"]


def _retrieved_date(rows: list[dict]) -> str | None:
    """The latest fetch date, from the store's own `vintage` column -- never
    wall-clock 'today', so a rebuild from the same committed CSV reproduces
    the same date (CLAUDE.md rule 35)."""
    if not rows:
        return None
    return max(row["vintage"] for row in rows)[:10]


def _rebase_to_2015_index(period_cells: dict[str, dict]) -> dict[str, dict]:
    """One country/aggregate's {period: {"v", "s"}} cells (a level, e.g.
    million EUR) -> the SAME periods rebased so the average of the four
    real 2015 quarters equals exactly 100.0.

    Method: base = mean(v for the 4 BASE_QUARTERS_2015), then every period's
    index = round(100 * v / base, 1). Any missing one of the four base
    quarters (absent from `period_cells`, or present with v=None) makes the
    base undefined -- every period in the returned series is then
    {"v": None, "s": "na"}, the maintainer's explicit decision
    (docs/features/europe_countries.md): never rebase on a different year.
    A real value's own status (final/provisional/estimate) is preserved on
    its rebased cell; a None value keeps its own real status (suppressed/na)
    rather than being overwritten.

    Hand-computed example (tests/test_export_europe_countries.py): 2015
    quarters 90, 100, 105, 105 -> base = 100.0 exactly; a 2016 quarter of
    110 -> index 110.0.
    """
    base_values = [period_cells.get(q, {}).get("v") for q in BASE_QUARTERS_2015]
    if any(v is None for v in base_values):
        return {period: {"v": None, "s": "na"} for period in period_cells}
    base = sum(base_values) / 4.0
    if base == 0:
        return {period: {"v": None, "s": "na"} for period in period_cells}
    out: dict[str, dict] = {}
    for period, cell in period_cells.items():
        if cell["v"] is None:
            out[period] = {"v": None, "s": cell["s"]}
        else:
            out[period] = {"v": round(100.0 * cell["v"] / base, 1), "s": cell["s"]}
    return out


def _attach_yoy(values_out: dict[str, dict[str, dict]], periods: list[str]) -> None:
    """Mutates `values_out` (period -> code -> {"v", "s"}) in place, adding a
    `yoy` percent-change key to every cell -- reuses
    src.analytics.derived.growth_rate (years=1) rather than a second
    reimplementation (CLAUDE.md rule 4: never computed in the browser; rule
    5's hand-computed tests live in tests/test_derived.py for growth_rate
    itself, and tests/test_export_europe_countries.py only proves this
    function wires the field onto the right cell). growth_rate already
    returns None when the prior period is missing or zero (same period
    format -- YYYY or YYYY-Qn -- growth_rate already parses). Rounded to 1
    decimal here, matching every other value this exporter rounds AT
    EXPORT, never inside the pure derivation function (derived.py's own
    docstring, point 3)."""
    codes: set[str] = set()
    for period_values in values_out.values():
        codes.update(period_values)
    for code in codes:
        series = {p: values_out[p][code]["v"] for p in periods if code in values_out[p]}
        for p in periods:
            cell = values_out[p].get(code)
            if cell is None:
                continue
            yoy = growth_rate(series, p)
            cell["yoy"] = None if yoy is None else round(yoy, 1)


def _build_indicator_payload(
    indicator_id: str,
    indicator_cfg: dict,
    source_cfg: dict,
    international_rows: dict[str, dict],
    country_codes_allowed: set[str],
    aggregate_codes: set[str],
    excluded_codes: set[str],
    geometry_ids: set[str],
) -> dict:
    is_map = indicator_id in MAP_INDICATOR_IDS
    geo_id_to_code = {row["geo"]["geo_id"]: code for code, row in international_rows.items()}
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
    unit = indicator_cfg["unit"]
    adapted = None
    if indicator_id in REBASED_INDICATOR_IDS:
        unit = "index_2015_100"
        adapted = {
            "base_period": "2015",
            "base_value": 100.0,
            "method": "average_of_4_quarters",
            "notice": ADAPTED_NOTICE,
        }

    has_yoy = indicator_id in YOY_INDICATOR_IDS

    rows = _load_store_rows(indicator_id)
    if rows is None:
        return {
            "indicator_id": indicator_id,
            "names": names,
            "unit": unit,
            "frequency": indicator_cfg["frequency"],
            "source": {**source_block, "retrieved": None},
            "geo_vintage": GEO_VINTAGE,
            "map": is_map,
            "status": "blocked",
            "blocked_reason": "Not loaded (no committed data/international/*.csv yet).",
            "periods": [],
            "latest_period": None,
            "values": {},
            "reference_lines": {},
            "class_breaks": {},
            "excluded_by_licence": sorted(excluded_codes & geometry_ids),
            "no_outline": {},
            "adapted": adapted,
            "has_yoy": has_yoy,
        }

    by_period: dict[str, dict[str, dict]] = {}
    by_code_all_periods: dict[str, dict[str, dict]] = {}
    data_codes: set[str] = set()
    for row in rows:
        code = geo_id_to_code.get(row["geo_id"])
        if code is None:
            raise ExportError(
                f"{indicator_id}: {row['geo_id']!r} has a committed observation but no row in "
                "config/geography/international.csv -- the store and the allowlist have drifted "
                "apart."
            )
        data_codes.add(code)
        value = None if row["value"] == "" else float(row["value"])
        cell = {"v": value, "s": row["status"]}
        by_period.setdefault(row["period"], {})[code] = cell
        by_code_all_periods.setdefault(code, {})[row["period"]] = cell

    unknown = data_codes - country_codes_allowed - aggregate_codes
    if unknown:
        raise ExportError(
            f"{indicator_id}: {sorted(unknown)} have committed observations but are neither an "
            "allowlisted country nor a pilot aggregate in config/geography/international.csv -- "
            "the store and the allowlist have drifted apart (CLAUDE.md rule 13)."
        )
    no_outline_here = (data_codes & country_codes_allowed) - geometry_ids
    unknown_no_outline = no_outline_here - set(NO_OUTLINE)
    if unknown_no_outline:
        raise ExportError(
            f"{indicator_id}: {sorted(unknown_no_outline)} have committed observations, are "
            "licence-allowed, but have no NUTS 0 geometry outline and are not in the documented "
            f"NO_OUTLINE list ({sorted(NO_OUTLINE)}). This is a genuine surprise (CLAUDE.md rule "
            "13) -- verify against the committed public/data/geo/nuts0/2024/0.json before adding "
            "it there."
        )

    value_universe = country_codes_allowed  # every allowlisted country, data or not
    periods = sorted(by_period)

    if indicator_id in REBASED_INDICATOR_IDS:
        rebased_by_code = {
            code: _rebase_to_2015_index(cells) for code, cells in by_code_all_periods.items()
        }
        values_out: dict[str, dict[str, dict]] = {p: {} for p in periods}
        reference_lines: dict[str, dict[str, dict]] = {p: {} for p in periods}
        for code, series in rebased_by_code.items():
            target = values_out if code in value_universe else reference_lines
            for period, cell in series.items():
                target[period][code] = cell
        for period in periods:
            # sorted(): value_universe is a set, and a set of strings
            # iterates in a per-process order (hash randomisation) -- the
            # committed GDP_VOLUME_EUROPE.json had "UA" in a different slot
            # from a rebuild on another run, a byte-level difference with no
            # data behind it (rule 35). The map branch below already sorts.
            for code in sorted(value_universe):
                values_out[period].setdefault(code, {"v": None, "s": "missing"})
        class_breaks: dict[str, list[float]] = {}  # never painted -- no map class breaks
        if has_yoy:
            _attach_yoy(values_out, periods)
    else:
        values_out = {}
        reference_lines = {}
        class_breaks = {}
        for period in periods:
            year_values: dict[str, dict] = {}
            ref_values: dict[str, dict] = {}
            real_numbers: list[float] = []
            for code in sorted(value_universe):
                cell = by_period[period].get(code)
                if cell is None:
                    year_values[code] = {"v": None, "s": "missing"}
                else:
                    year_values[code] = cell
                    if cell["v"] is not None:
                        real_numbers.append(cell["v"])
            for code in sorted(aggregate_codes):
                cell = by_period[period].get(code)
                if cell is not None:
                    ref_values[code] = cell
            values_out[period] = year_values
            reference_lines[period] = ref_values
            # A chart-only level (LEVEL_CHART_ONLY_INDICATOR_IDS) is never
            # painted, so it gets no class breaks either -- same as the
            # rebased index above, and honest about what the payload is for.
            if is_map:
                class_breaks[period] = _quantile_breaks(real_numbers)
        if has_yoy:
            _attach_yoy(values_out, periods)

    latest_period = None
    for period in sorted(periods, reverse=True):
        if any(cell["v"] is not None for cell in values_out.get(period, {}).values()):
            latest_period = period
            break

    return {
        "indicator_id": indicator_id,
        "names": names,
        "unit": unit,
        "frequency": indicator_cfg["frequency"],
        "source": {**source_block, "retrieved": _retrieved_date(rows)},
        "geo_vintage": GEO_VINTAGE,
        "map": is_map,
        "status": "loaded",
        "periods": periods,
        "latest_period": latest_period,
        "values": values_out,
        "reference_lines": reference_lines,
        "class_breaks": class_breaks,
        "excluded_by_licence": sorted(excluded_codes & geometry_ids),
        "no_outline": {code: NO_OUTLINE[code] for code in sorted(no_outline_here)},
        "adapted": adapted,
        "has_yoy": has_yoy,
    }


def export(out_dir: Path = DEFAULT_OUT_DIR) -> list[str]:
    """Writes index.json and one {indicator_id}.json per configured Europe
    countries indicator. Returns the list of indicator_ids processed, in the
    map-then-chart-only order MAP_INDICATOR_IDS + CHART_ONLY_INDICATOR_IDS."""
    indicator_configs, source_configs = load_and_validate_all(INDICATORS_DIR, SOURCES_DIR)
    missing_cfgs = [i for i in ALL_INDICATOR_IDS if i not in indicator_configs]
    if missing_cfgs:
        raise ExportError(f"config/indicators/*.yaml is missing: {missing_cfgs}")

    geometry_ids = _geometry_ids()
    international_rows = load_international_rows()
    excluded_codes = set(load_excluded())
    country_codes_allowed = {
        code
        for code, row in international_rows.items()
        if row["scope"] == "pilot" and row["basis"] != "aggregate"
    }
    aggregate_codes = {
        code
        for code, row in international_rows.items()
        if row["scope"] == "pilot" and row["basis"] == "aggregate"
    }

    unresolved_geometry = geometry_ids - country_codes_allowed - excluded_codes
    if unresolved_geometry:
        raise ExportError(
            f"NUTS 0 geometry code(s) with no licence decision at all: "
            f"{sorted(unresolved_geometry)} -- CLAUDE.md rule 13."
        )

    index_indicators = []
    for indicator_id in ALL_INDICATOR_IDS:
        payload = _build_indicator_payload(
            indicator_id,
            indicator_configs[indicator_id],
            source_configs[indicator_configs[indicator_id]["source_id"]],
            international_rows,
            country_codes_allowed,
            aggregate_codes,
            excluded_codes,
            geometry_ids,
        )
        _write_json(out_dir / f"{indicator_id}.json", payload)
        index_indicators.append(
            {
                "id": indicator_id,
                "status": payload["status"],
                "map": payload["map"],
                "compare_default": indicator_id in COMPARE_DEFAULT_IDS,
                "payload": f"{indicator_id}.json",
            }
        )

    countries = []
    for code in sorted(country_codes_allowed):
        row = international_rows[code]
        countries.append(
            {
                "code": code,
                "geo_id": row["geo"]["geo_id"],
                "names": {
                    "en": row["geo"]["name_en"],
                    "fr": row["geo"]["name_fr"],
                    "nl": row["geo"]["name_nl"],
                },
                "basis": row["basis"],
                "has_outline": code in geometry_ids,
            }
        )
    aggregates = []
    for code in sorted(aggregate_codes):
        row = international_rows[code]
        aggregates.append(
            {
                "code": code,
                "geo_id": row["geo"]["geo_id"],
                "names": {
                    "en": row["geo"]["name_en"],
                    "fr": row["geo"]["name_fr"],
                    "nl": row["geo"]["name_nl"],
                },
            }
        )

    index_payload = {
        "geo_vintage": GEO_VINTAGE,
        "geometry": GEOMETRY_PAYLOAD_PATH,
        "attribution": ATTRIBUTION_TEXT,
        "countries": countries,
        "aggregates": aggregates,
        "excluded_by_licence": sorted(excluded_codes & geometry_ids),
        "indicators": index_indicators,
    }
    _write_json(out_dir / "index.json", index_payload)
    return list(ALL_INDICATOR_IDS)


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish the Europe countries payloads")
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
