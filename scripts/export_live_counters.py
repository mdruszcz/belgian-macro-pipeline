"""Publish public/data/live_counters.json: the parameters a later PR's
browser code evaluates to show labelled "simulated live" counters --
population, debt, deficit, revenue and spending (each with named parts) --
ticking once a second. ALL arithmetic happens HERE, in Python (CLAUDE.md
rules 4/6); the browser only ever evaluates v0 + rate_per_ms*(t - start_ms)
on these precomputed segments. See docs/features/public_finance_live.md
and docs/decisions/0016-simulated-live-counters.md.

Reads ONLY already-published, already-validated payloads -- never SQLite,
never a raw source file (rule 20): public/data/national.json (the 25
GOV_*_BE series), public/data/aggregates.json (POPULATION_BY_COMMUNE,
be:country), public/data/metadata/indicators.json (population's own
source/updated -- it is a municipal-scope indicator, absent from
national.json, confirmed 2026-10-03), and config/live_counters.yaml.

FAILURE POLICY (rule 13): a config/schema problem -- src/validation/
live_counters_config.py's own checks, or a LiveCounterError from
src/analytics/live_counters.py (an unknown unit, a malformed period,
mismatched segment boundaries) -- raises and fails the build loudly. A
DATA condition -- the trend's base year is missing, a breakdown's
remainder is too negative to clamp, population coverage is short of 100%,
a debt anchor sits beyond the horizon -- never raises: that one counter or
breakdown is published with "state": "unavailable" and a reason, and this
script prints a WARN line for it. A simulated widget must never block the
daily site update.

Deterministic (rule 35): `updated` in the payload is the MAX of the
`updated`/freshness fields actually read from the input payloads -- never
the wall clock. Identical inputs produce byte-identical output.

Usage:  python scripts/export_live_counters.py
        python scripts/export_live_counters.py --out SOME/OTHER/PATH
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.analytics.live_counters import (  # noqa: E402
    CET,
    LiveCounterError,
    Unavailable,
    annual_value,
    apply_shares_rounded,
    compute_breakdown,
    difference_segments,
    flow_segments,
    money_round,
    period_end_ms,
    population_segments,
    scale_for,
    shares_from_breakdown,
    stock_segments,
    trend_growth,
    year_bounds,
)
from src.validation.live_counters_config import load_and_validate_live_counters  # noqa: E402

DEFAULT_NATIONAL = REPO_ROOT / "public" / "data" / "national.json"
DEFAULT_AGGREGATES = REPO_ROOT / "public" / "data" / "aggregates.json"
DEFAULT_METADATA = REPO_ROOT / "public" / "data" / "metadata" / "indicators.json"
DEFAULT_CONFIG = REPO_ROOT / "config" / "live_counters.yaml"
DEFAULT_OUT = REPO_ROOT / "public" / "data" / "live_counters.json"

#: The source series' own precision (meur, one decimal) -- never a
#: hand-typed rounding, just matching what gov_10a_main etc. publish.
ANNUAL_DP = Decimal("0.1")


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8"
    )


def _annual_series(national: dict, indicator_id: str) -> dict[int, float]:
    """{year: value} for an ANNUAL national indicator. A quarterly period
    (e.g. GOV_DEBT_Q_MEUR_BE's own) never reaches this -- only 4-digit,
    all-numeric period strings qualify."""
    entry = national["indicators"].get(indicator_id)
    if entry is None:
        raise LiveCounterError(f"{indicator_id}: not published in national.json")
    out: dict[int, float] = {}
    for period, cell in entry["periods"].items():
        if len(period) == 4 and period.isdigit() and cell.get("value") is not None:
            out[int(period)] = cell["value"]
    return out


def _basis_entry(
    national: dict, indicator_id: str, period: str, *, role: str | None = None
) -> dict:
    entry = national["indicators"][indicator_id]
    cell = entry["periods"][period]
    out = {
        "indicator": indicator_id,
        "names": entry.get("names"),
        "period": period,
        "value": cell.get("value"),
        "unit": entry.get("unit"),
        "status": cell.get("status"),
        "source": entry.get("source"),
        "updated": entry.get("updated"),
    }
    # `role` distinguishes WHY a basis row is cited -- e.g. debt's own
    # anchor value vs. the series that set its post-anchor pace -- so a
    # reader (or PR 2's UI) never has to guess which basis row is which.
    # Omitted (not even "null") when the caller has nothing to disambiguate,
    # same shape every existing basis row already had before this fix.
    if role is not None:
        out["role"] = role
    return out


def _period_sort_key(period: str) -> int:
    """Sorts 'YYYY' and 'YYYY-Qn' by the instant they END -- so the latest
    period of a mixed-frequency series is always the chronologically
    latest one, never a lexical accident."""
    return period_end_ms(period)


def _collect_updated(*values: str | None) -> str:
    present = [v for v in values if v]
    if not present:
        raise LiveCounterError("no `updated` date found in any input payload")
    return max(present)


# ── flow counters (revenue, spending) ───────────────────────────────────────


def _flow_entry(
    counter: dict, national: dict, window: int, tolerance: Decimal, latest_year: int, horizon: int
) -> tuple[dict, list | None, dict[int, Decimal]]:
    indicator_id = counter["indicator"]
    series = _annual_series(national, indicator_id)
    scale = scale_for(counter["unit"])
    growth = trend_growth(series, latest_year, window=window)

    base_year = latest_year - window
    basis = [
        _basis_entry(national, indicator_id, str(y))
        for y in (latest_year, base_year)
        if y in series
    ]
    entry: dict = {
        "id": counter["id"],
        "kind": "flow",
        "unit": counter["unit"],
        "label": counter["label"],
        "basis": basis,
    }

    annual_native: dict[int, Decimal | Unavailable] = {
        y: annual_value(series, y, latest_year, growth) for y in range(latest_year + 1, horizon + 1)
    }
    bad = next((v for v in annual_native.values() if isinstance(v, Unavailable)), None)
    if isinstance(growth, Unavailable) or bad is not None:
        reason = growth.reason if isinstance(growth, Unavailable) else bad.reason
        entry["state"] = "unavailable"
        entry["reason"] = reason
        return entry, None, {}

    annual_eur = {y: v * scale for y, v in annual_native.items()}
    segments = flow_segments(annual_eur, latest_year + 1, horizon)
    if isinstance(segments, Unavailable):
        entry["state"] = "unavailable"
        entry["reason"] = segments.reason
        return entry, None, {}

    entry["state"] = "available"
    entry["growth_rate"] = float(growth)
    entry["annual_meur"] = {
        str(y): float(v.quantize(ANNUAL_DP)) for y, v in sorted(annual_native.items())
    }
    entry["segments"] = [s.as_dict() for s in segments]

    breakdown_cfg = counter.get("breakdown")
    if breakdown_cfg:
        entry["breakdown"] = _breakdown_entry(
            breakdown_cfg, national, tolerance, annual_eur, latest_year, horizon
        )
    return entry, segments, annual_eur


def _breakdown_entry(
    breakdown_cfg: dict,
    national: dict,
    tolerance: Decimal,
    annual_eur_by_year: dict[int, Decimal],
    latest_year: int,
    horizon: int,
) -> dict:
    within_id = breakdown_cfg["within"]
    parts_cfg = breakdown_cfg["parts"]
    named_parts = [p for p in parts_cfg if "indicator" in p]
    nested = [p for p in parts_cfg if "remainder_of" in p]
    top_remainder = next(p for p in parts_cfg if p.get("remainder") is True)

    required_ids = (
        [within_id]
        + [p["indicator"] for p in named_parts]
        + [p["remainder_of"] for p in nested]
        + [extra for p in nested for extra in p.get("plus", [])]
    )
    series_by_id = {i: _annual_series(national, i) for i in required_ids}
    common_years = set.intersection(*(set(s) for s in series_by_id.values()))
    eligible = {y for y in common_years if y <= latest_year}
    if not eligible:
        return {"state": "unavailable", "reason": "no_common_breakdown_year", "within": within_id}
    breakdown_year = max(eligible)

    resolved: dict[str, Decimal] = {
        p["id"]: Decimal(str(series_by_id[p["indicator"]][breakdown_year])) for p in named_parts
    }
    for part in nested:
        # D.995 fix: `whole` is `remainder_of`'s own value PLUS every series
        # named in `plus` (e.g. GOV_TAX_UNCOLLECTED_BE), never just
        # `remainder_of` alone -- GOV_TAX_SSC_TOTAL_BE nets D.995 out, TR
        # does not, so the gross total other_taxes computes against must add
        # it back (docs/features/public_finance_live.md, docs/data_catalog.md).
        whole = Decimal(str(series_by_id[part["remainder_of"]][breakdown_year])) + sum(
            (Decimal(str(series_by_id[extra][breakdown_year])) for extra in part.get("plus", [])),
            start=Decimal(0),
        )
        covered = {pid: resolved[pid] for pid in part["covers"]}
        result = compute_breakdown(whole, covered, part["id"], tolerance)
        if isinstance(result, Unavailable):
            return {
                "state": "unavailable",
                "reason": result.reason,
                "year": str(breakdown_year),
                "within": within_id,
            }
        resolved[part["id"]] = result[part["id"]]

    within_whole = Decimal(str(series_by_id[within_id][breakdown_year]))
    full = compute_breakdown(within_whole, resolved, top_remainder["id"], tolerance)
    if isinstance(full, Unavailable):
        return {
            "state": "unavailable",
            "reason": full.reason,
            "year": str(breakdown_year),
            "within": within_id,
        }
    shares = shares_from_breakdown(full, within_whole)

    # Against the counter's OWN rounded total (money_round), via
    # apply_shares_rounded -- not the unrounded apply_shares -- so the
    # published parts always sum to exactly that total, never a cent off
    # (each part would otherwise round independently and could land the
    # sum a cent either side of the total's own rounding).
    parts_by_year = {
        y: apply_shares_rounded(shares, money_round(annual_eur_by_year[y]), top_remainder["id"])
        for y in annual_eur_by_year
    }

    parts_out = []
    for part_cfg in parts_cfg:
        pid = part_cfg["id"]
        part_eur_by_year = {y: parts_by_year[y][pid] for y in parts_by_year}
        part_segments = flow_segments(part_eur_by_year, latest_year + 1, horizon)
        if "indicator" in part_cfg:
            names = national["indicators"][part_cfg["indicator"]].get("names")
            basis = [_basis_entry(national, part_cfg["indicator"], str(breakdown_year))]
        else:
            names = part_cfg["label"]
            source_id = part_cfg.get("remainder_of")
            basis = [_basis_entry(national, source_id, str(breakdown_year))] if source_id else []
            # D.995 fix: a nested remainder's `whole` is remainder_of PLUS
            # every `plus` series -- its basis names all of them, not just
            # remainder_of, so a reader can see exactly what was added back.
            basis += [
                _basis_entry(national, extra, str(breakdown_year))
                for extra in part_cfg.get("plus", [])
            ]
        parts_out.append(
            {
                "id": pid,
                "names": names,
                "share": float(shares[pid]),
                "basis": basis,
                "segments": (
                    None
                    if isinstance(part_segments, Unavailable)
                    else [s.as_dict() for s in part_segments]
                ),
            }
        )
    return {
        "state": "available",
        "year": str(breakdown_year),
        "within": within_id,
        "parts": parts_out,
    }


# ── difference counter (deficit) ────────────────────────────────────────────


def _difference_entry(
    counter: dict,
    segments_by_id: dict[str, list | None],
    national: dict,
    minuend_indicator: str,
    subtrahend_indicator: str,
    latest_year: int,
) -> tuple[dict, list | None]:
    minuend = segments_by_id.get(counter["minuend"])
    subtrahend = segments_by_id.get(counter["subtrahend"])
    # A `difference` counter has no series lookup of its own -- its basis
    # names the two flow counters' own latest-year figures it is computed
    # from (TE and TR for `deficit`), so a reader always sees what a
    # "-33,220.7" deficit figure traces back to, same as every other
    # counter already does for its own basis.
    basis = [
        _basis_entry(national, minuend_indicator, str(latest_year), role="minuend"),
        _basis_entry(national, subtrahend_indicator, str(latest_year), role="subtrahend"),
    ]
    entry = {
        "id": counter["id"],
        "kind": "difference",
        "unit": counter["unit"],
        "label": counter["label"],
        "basis": basis,
    }
    if minuend is None or subtrahend is None:
        entry["state"] = "unavailable"
        entry["reason"] = "component_unavailable"
        return entry, None
    segments = difference_segments(subtrahend, minuend)
    entry["state"] = "available"
    entry["segments"] = [s.as_dict() for s in segments]
    return entry, segments


# ── stock counters (population, debt) ───────────────────────────────────────


def _population_entry(counter: dict, aggregates: dict, metadata: dict, horizon_end_ms: int) -> dict:
    indicator_id = counter["aggregate_indicator"]
    geo_id = counter["geo_id"]
    meta = next((m for m in metadata["indicators"] if m["indicator_code"] == indicator_id), None)
    entry: dict = {
        "id": counter["id"],
        "kind": "stock",
        "unit": counter["unit"],
        "label": counter["label"],
        "basis": [],
    }
    try:
        periods = aggregates["indicators"][indicator_id][geo_id]["periods"]
    except KeyError:
        entry["state"] = "unavailable"
        entry["reason"] = "missing_series"
        return entry

    years = sorted(int(y) for y in periods if y.isdigit())
    if len(years) < 2:
        entry["state"] = "unavailable"
        entry["reason"] = "insufficient_history"
        return entry
    latest_year, previous_year = years[-1], years[-2]
    latest = periods[str(latest_year)]
    previous = periods[str(previous_year)]

    def _basis_row(year: int, cell: dict) -> dict:
        row = {
            "indicator": indicator_id,
            "period": str(year),
            "value": cell["value"],
            "status": "final",
        }
        if meta:
            row.update(
                names=meta.get("names"),
                unit=meta.get("unit"),
                source=meta.get("source"),
                updated=meta.get("updated"),
            )
        return row

    entry["basis"] = [_basis_row(latest_year, latest), _basis_row(previous_year, previous)]

    segments = population_segments(
        latest["value"],
        previous["value"],
        latest_year,
        horizon_end_ms,
        coverage_latest_pct=latest["coverage"]["pct"],
        coverage_previous_pct=previous["coverage"]["pct"],
    )
    if isinstance(segments, Unavailable):
        entry["state"] = "unavailable"
        entry["reason"] = segments.reason
        return entry
    entry["state"] = "available"
    entry["segments"] = [s.as_dict() for s in segments]
    return entry


def _debt_entry(
    counter: dict,
    national: dict,
    deficit_pace_by_year: dict[int, Decimal] | None,
    revenue_indicator: str,
    spending_indicator: str,
    latest_year: int,
    horizon_end_ms: int,
) -> dict:
    scale = scale_for(counter["unit"])
    entry: dict = {
        "id": counter["id"],
        "kind": "stock",
        "unit": counter["unit"],
        "label": counter["label"],
        "basis": [],
    }
    candidates = []
    for indicator_id in counter["anchors"]:
        series_entry = national["indicators"].get(indicator_id)
        if not series_entry:
            continue
        valued_periods = [
            p for p, cell in series_entry["periods"].items() if cell.get("value") is not None
        ]
        if not valued_periods:
            continue
        latest_period = max(valued_periods, key=_period_sort_key)
        candidates.append((period_end_ms(latest_period), indicator_id, latest_period))
    if not candidates:
        entry["state"] = "unavailable"
        entry["reason"] = "missing_series"
        return entry

    # Latest period-end wins; a tie keeps the FIRST anchor in config order
    # (the annual EDP figure is listed first) -- max() with a stable sort
    # over the original candidate order achieves exactly that.
    best_ms = max(c[0] for c in candidates)
    anchor_ms, anchor_indicator, anchor_period = next(c for c in candidates if c[0] == best_ms)
    anchor_value_native = national["indicators"][anchor_indicator]["periods"][anchor_period][
        "value"
    ]
    anchor_value_eur = Decimal(str(anchor_value_native)) * scale
    # Basis names BOTH roles a reader needs to trust this figure: the
    # Basis names BOTH roles a reader needs to trust this figure: the
    # anchor itself (where the line starts) and the series that sets its
    # pace from the anchor onward (fix round, 2026-10-03 -- debt's basis
    # previously named only the anchor, docs/features/public_finance_live.md).
    entry["basis"] = [_basis_entry(national, anchor_indicator, anchor_period, role="anchor")]

    if anchor_ms >= horizon_end_ms:
        entry["state"] = "unavailable"
        entry["reason"] = "anchor_beyond_horizon"
        return entry

    anchor_year = datetime.fromtimestamp(anchor_ms / 1000, tz=CET).year
    # If the anchor year is already official (<= latest_year), the TR/TE
    # pair whose difference sets debt's pace for that year IS itself
    # latest_year's own official figure -- cite it directly. Every pace
    # year after that (including every year today's anchor is itself
    # beyond latest_year, e.g. a 2026-Q1 anchor against a 2025 latest_year)
    # uses the already-computed, PROJECTED `deficit` counter's own segment
    # instead -- citing TR/TE there directly would overstate precision
    # (that pace is a trend extrapolation, not an official figure), so the
    # sibling counter itself is named instead: a reader follows `deficit`'s
    # own basis for ITS source citation one level down.
    if anchor_year <= latest_year:
        entry["basis"].append(
            _basis_entry(national, spending_indicator, str(latest_year), role="pace_minuend")
        )
        entry["basis"].append(
            _basis_entry(national, revenue_indicator, str(latest_year), role="pace_subtrahend")
        )
    # Every pace year beyond latest_year (always true when the anchor
    # itself is already beyond latest_year, e.g. a 2026-Q1 anchor against a
    # 2025 latest_year; otherwise true for the later part of the horizon)
    # uses the already-computed, PROJECTED `deficit` counter's own segment
    # -- cited structurally by counter id, since a reader follows
    # `deficit`'s own basis for the TR/TE pair behind THAT trend
    # extrapolation one level down, rather than this entry re-citing TR/TE
    # directly and overstating the pace's precision.
    if horizon_end_ms > year_bounds(max(anchor_year, latest_year))[1]:
        entry["basis"].append({"counter": counter["paced_by"], "role": "pace_counter"})

    revenue_series = _annual_series(national, revenue_indicator)
    spending_series = _annual_series(national, spending_indicator)
    year_pace: dict[int, Decimal] = {}

    # Build the pace map year by year: official TE-TR for years <=
    # latest_year, the already-computed projected deficit pace after.
    year = anchor_year
    while True:
        y_start, y_end = year_bounds(year)
        if year <= latest_year:
            if year in revenue_series and year in spending_series:
                year_pace[year] = (
                    Decimal(str(spending_series[year])) - Decimal(str(revenue_series[year]))
                ) * scale
        elif deficit_pace_by_year is not None and year in deficit_pace_by_year:
            year_pace[year] = deficit_pace_by_year[year]
        if y_end >= horizon_end_ms:
            break
        year += 1

    segments = stock_segments(anchor_ms, anchor_value_eur, year_pace, horizon_end_ms)
    if isinstance(segments, Unavailable):
        entry["state"] = "unavailable"
        entry["reason"] = segments.reason
        return entry
    entry["state"] = "available"
    entry["segments"] = [s.as_dict() for s in segments]
    return entry


# ── top-level orchestration ─────────────────────────────────────────────────


def build_payload(national: dict, aggregates: dict, metadata: dict, config: dict) -> dict:
    method = config["method"]
    window = method["trend_window_years"]
    tolerance = Decimal(str(method["remainder_tolerance_meur"]))

    by_id = {c["id"]: c for c in config["counters"]}
    diff_counter = next(c for c in config["counters"] if c["kind"] == "difference")
    minuend_cfg = by_id[diff_counter["minuend"]]
    subtrahend_cfg = by_id[diff_counter["subtrahend"]]

    minuend_series = _annual_series(national, minuend_cfg["indicator"])
    subtrahend_series = _annual_series(national, subtrahend_cfg["indicator"])
    common = set(minuend_series) & set(subtrahend_series)
    if not common:
        raise LiveCounterError(
            f"{minuend_cfg['indicator']} and {subtrahend_cfg['indicator']} share no common year"
        )
    latest_year = max(common)
    horizon = latest_year + method["horizon_years"]
    horizon_end_ms = year_bounds(horizon)[1]

    counters_out: list[dict] = []
    flow_segments_by_id: dict[str, list | None] = {}
    updated_values: list[str] = []

    def _note_updated(entry: dict) -> None:
        for row in entry.get("basis", []):
            if row.get("updated"):
                updated_values.append(row["updated"])
        for row in entry.get("breakdown", {}).get("parts", []) if entry.get("breakdown") else []:
            for b in row.get("basis", []):
                if b.get("updated"):
                    updated_values.append(b["updated"])

    deficit_pace_by_year: dict[int, Decimal] = {}

    for counter in config["counters"]:
        kind = counter["kind"]
        if kind == "flow":
            entry, segments, _eur = _flow_entry(
                counter, national, window, tolerance, latest_year, horizon
            )
            flow_segments_by_id[counter["id"]] = segments
            counters_out.append(entry)
            _note_updated(entry)
        elif kind == "difference":
            entry, segments = _difference_entry(
                counter,
                flow_segments_by_id,
                national,
                minuend_indicator=by_id[counter["minuend"]]["indicator"],
                subtrahend_indicator=by_id[counter["subtrahend"]]["indicator"],
                latest_year=latest_year,
            )
            counters_out.append(entry)
            _note_updated(entry)
            if entry["state"] == "available":
                for offset, seg in enumerate(segments):
                    deficit_pace_by_year[latest_year + 1 + offset] = seg.v1
        elif kind == "stock" and "aggregate_indicator" in counter:
            entry = _population_entry(counter, aggregates, metadata, horizon_end_ms)
            counters_out.append(entry)
            _note_updated(entry)
        elif kind == "stock":
            paced_by_cfg = by_id[counter["paced_by"]]
            entry = _debt_entry(
                counter,
                national,
                deficit_pace_by_year,
                revenue_indicator=by_id[paced_by_cfg["subtrahend"]]["indicator"],
                spending_indicator=by_id[paced_by_cfg["minuend"]]["indicator"],
                latest_year=latest_year,
                horizon_end_ms=horizon_end_ms,
            )
            counters_out.append(entry)
            _note_updated(entry)
        else:
            raise LiveCounterError(f"unknown counter kind {kind!r}")

    starts = [entry["segments"][0]["start_ms"] for entry in counters_out if entry.get("segments")]
    valid_from_ms = min(starts) if starts else year_bounds(latest_year + 1)[0]

    return {
        "schema_version": 1,
        "simulated": True,
        "method": method,
        "valid_from_ms": valid_from_ms,
        "valid_until_ms": horizon_end_ms,
        "updated": _collect_updated(*updated_values),
        "counters": counters_out,
        "placements": config["placements"],
    }


def export_live_counters(
    national_path: Path = DEFAULT_NATIONAL,
    aggregates_path: Path = DEFAULT_AGGREGATES,
    metadata_path: Path = DEFAULT_METADATA,
    config_path: Path = DEFAULT_CONFIG,
    out_path: Path = DEFAULT_OUT,
) -> dict:
    national = json.loads(Path(national_path).read_text(encoding="utf-8"))
    aggregates = json.loads(Path(aggregates_path).read_text(encoding="utf-8"))
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
    config = load_and_validate_live_counters(Path(config_path))

    payload = build_payload(national, aggregates, metadata, config)
    _write_json(Path(out_path), payload)

    for counter in payload["counters"]:
        if counter["state"] != "available":
            print(f"WARN  {counter['id']}: unavailable ({counter.get('reason')})")
        breakdown = counter.get("breakdown")
        if breakdown and breakdown["state"] != "available":
            print(f"WARN  {counter['id']}.breakdown: unavailable ({breakdown.get('reason')})")
    return payload


def main() -> None:
    ap = argparse.ArgumentParser(description="Publish the simulated public-finance live counters")
    ap.add_argument("--national", type=Path, default=DEFAULT_NATIONAL)
    ap.add_argument("--aggregates", type=Path, default=DEFAULT_AGGREGATES)
    ap.add_argument("--metadata", type=Path, default=DEFAULT_METADATA)
    ap.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    try:
        payload = export_live_counters(
            args.national, args.aggregates, args.metadata, args.config, args.out
        )
    except LiveCounterError as exc:
        print(f"\nEXPORT FAILED: {exc}\n", file=sys.stderr)
        raise SystemExit(1) from exc
    available = sum(1 for c in payload["counters"] if c["state"] == "available")
    print(f"Published {len(payload['counters'])} counter(s) ({available} available) to {args.out}")


if __name__ == "__main__":
    main()
