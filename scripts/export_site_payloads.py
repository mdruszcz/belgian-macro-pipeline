"""
Split the bulk CSV exports into per-entity JSON payloads small enough for a
browser to fetch on demand -- Block K's future `/local` interface needs one
commune's worth of data, not all 565. See docs/features/site_payloads.md.

Every payload here is a RESHAPE of an existing bulk export, never a new
computation: `communes/{nis}.json` slices `communes_history.csv`,
`indicators/{id}.json` slices `communes_export.csv`, `national.json` slices
`belgian_macro_export.csv`. Nothing here writes to `observations`, and
nothing here recomputes a derived indicator -- CONTROL G stays satisfied.

Bulk CSV/JSON exports are left completely untouched; this only reads them.
"""

import argparse
import csv
import json
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

STATUS_LETTER_TO_WORD = {
    "A": "final",
    "P": "provisional",
}


def _status_word(status: str) -> str:
    return STATUS_LETTER_TO_WORD.get(status, status or None)


def _note_updated(indicator: dict, fetched_at: str) -> None:
    """Record the latest retrieval date seen for an indicator, as a date.

    This is a LICENCE CONDITION, not a nicety: Statbel's 2015 open-data
    licence requires the attribution to carry "de datum van de laatste
    bijwerking" (the date of last update) and terminates automatically if it
    does not -- see docs/data_catalog.md, "What this obliges us to build".
    The pages cannot show a date the payload does not carry.

    A DERIVED indicator has no retrieval date of its own (its `fetched_at`
    is empty in the source CSV, correctly -- it was computed, not fetched),
    so the key is simply absent for it rather than carrying an invented or
    inherited date. Absence is already this format's "no data" signal
    (docs/features/site_payloads.md); a wrong date here would be a licence
    breach of its own, since the 2015 licence also forbids misleading a
    reader about the update date.
    """
    if not fetched_at:
        return
    day = fetched_at[:10]
    if day > indicator.get("updated", ""):
        indicator["updated"] = day


def _read_communes_history(csv_path: Path) -> dict[str, dict]:
    """One entry per commune, keyed by nis_code, holding every indicator that
    commune has any value for across every period -- the shape
    `communes/{nis}.json` publishes directly."""
    communes: dict[str, dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            nis = row["nis_code"]
            commune = communes.setdefault(
                nis,
                {
                    "nis_code": nis,
                    "geo_id": row["geo_id"],
                    "name": {"en": row["name_en"], "fr": row["name_fr"], "nl": row["name_nl"]},
                    "region": row["region"] or None,
                    "province": row["province"] or None,
                    "arrondissement": row["arrondissement"] or None,
                    "indicators": {},
                },
            )
            indicator = commune["indicators"].setdefault(
                row["indicator_code"],
                {"name": row["indicator_name"], "unit": row["unit"], "periods": {}},
            )
            if row["value"] == "":
                continue
            indicator["periods"][row["period"]] = {
                "value": float(row["value"]),
                "status": _status_word(row["status"]),
            }
            _note_updated(indicator, row["fetched_at"])
    return communes


def _read_communes_latest(csv_path: Path) -> dict[str, dict]:
    """One entry per indicator, holding every current commune's latest value
    -- the cross-sectional shape `indicators/{id}.json` publishes directly.
    `communes_export.csv` already restricts to current communes and the most
    recent period per (commune, indicator), so no filtering happens here."""
    indicators: dict[str, dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            indicator = indicators.setdefault(
                row["indicator_code"],
                {"name": row["indicator_name"], "unit": row["unit"], "communes": {}},
            )
            if row["value"] == "":
                continue
            indicator["communes"][row["nis_code"]] = {
                "value": float(row["value"]),
                "period": row["period"],
                "status": _status_word(row["status"]),
            }
            _note_updated(indicator, row["fetched_at"])
    return indicators


def _read_national(csv_path: Path) -> dict[str, dict]:
    """be:country's full history across every indicator in
    `belgian_macro_export.csv`, reshaped the same way as a commune payload."""
    indicators: dict[str, dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            indicator = indicators.setdefault(
                row["indicator_code"],
                {
                    "name": row["name"],
                    "unit": row["unit"],
                    "source_agency": row["source_agency"],
                    "periods": {},
                },
            )
            if row["value"] == "":
                continue
            _note_updated(indicator, row["fetched_at"])
            indicator["periods"][row["period"]] = {
                "value": float(row["value"]),
                "status": _status_word(row["obs_status"]),
            }
    return indicators


def _build_geographies(db_path: Path) -> list[dict]:
    """Every currently-valid geography with its trilingual name and its
    region/province/arrondissement ancestry, for Block K's search and Block
    L's comparison picker."""
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute("""
        SELECT geo_id, nis_code, level, name_en, name_fr, name_nl, parent_geo_id
        FROM geographies WHERE valid_to IS NULL ORDER BY level, nis_code
        """).fetchall()
    conn.close()

    out = []
    for geo_id, nis_code, level, name_en, name_fr, name_nl, parent_geo_id in rows:
        out.append(
            {
                "geo_id": geo_id,
                "nis_code": nis_code,
                "level": level,
                "name": {"en": name_en, "fr": name_fr, "nl": name_nl},
                "parent_geo_id": parent_geo_id,
            }
        )
    return out


def _ancestry(db_path: Path) -> dict[str, list[str]]:
    """geo_id -> its ancestor geo_ids, nearest first.

    Walked from parent_geo_id over ALL geographies (including historical
    ones), tolerating a missing level and stopping on a cycle -- the same
    defence the exporters' own _ancestor_names() applies.
    """
    conn = sqlite3.connect(str(db_path))
    # Canonicalised, matching export_aggregates_csv.py: the geographies table
    # holds versioned ancestors (be:prov:10000@1977-01-01) whose figures the
    # aggregate export now folds into the current geography. If the two walks
    # disagreed, a comparison would silently fail to match and simply not
    # appear. No CURRENT commune has a versioned parent today -- 34 historical
    # ones do -- so this is agreement insurance, not a live fix.
    parents = {
        geo_id.split("@", 1)[0]: (parent.split("@", 1)[0] if parent else None)
        for geo_id, parent in conn.execute("SELECT geo_id, parent_geo_id FROM geographies")
    }
    conn.close()

    chains: dict[str, list[str]] = {}
    for geo_id in parents:
        chain: list[str] = []
        seen = {geo_id}
        current = parents.get(geo_id)
        while current and current not in seen:
            seen.add(current)
            chain.append(current)
            current = parents.get(current)
        chains[geo_id] = chain
    return chains


def _read_percentiles(csv_path: Path) -> dict[tuple[str, str], dict]:
    """Peer positions keyed by (nis_code, indicator).

    From scripts/export_percentiles_csv.py, latest period per indicator. An
    empty `percentile` column means the peer set was below the floor and the
    percentile was deliberately withheld -- the rank is still there, and the
    page says "4th of 19" instead. That distinction has to survive into the
    payload, so a withheld percentile becomes None rather than 0.
    """
    out: dict[tuple[str, str], dict] = {}
    if not csv_path.is_file():
        return out
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            entry = out.setdefault((row["nis_code"], row["indicator_code"]), {})
            entry[row["scope"]] = {
                "scope_name": row["scope_name"],
                "pct": float(row["percentile"]) if row["percentile"] != "" else None,
                "rank": int(row["rank"]),
                "peers": int(row["peers"]),
                "period": row["period"],
            }
    return out


def _attach_percentiles(communes: dict[str, dict], percentiles: dict) -> int:
    attached = 0
    for nis, commune in communes.items():
        for indicator_id, entry in commune["indicators"].items():
            pos = percentiles.get((nis, indicator_id))
            if pos:
                entry["percentile"] = pos
                attached += 1
    return attached


def _read_aggregates(csv_path: Path) -> dict[tuple[str, str, str], dict]:
    """Aggregate values keyed by (geo_id, indicator, period).

    Produced by scripts/export_aggregates_csv.py -- Block L. Read rather than
    recomputed here, for the same reason every other payload is a reshape of
    an existing export: one computation, one set of numbers, and no chance of
    the page disagreeing with the CSV a researcher downloaded.
    """
    out: dict[tuple[str, str, str], dict] = {}
    if not csv_path.is_file():
        return out
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            out[(row["geo_id"], row["indicator_code"], row["period"])] = {
                "geo_id": row["geo_id"],
                "level": row["level"],
                "name": {"en": row["name_en"], "fr": row["name_fr"], "nl": row["name_nl"]},
                "value": float(row["value"]),
                "period": row["period"],
                "coverage": {
                    "n": int(row["coverage_n"]),
                    "of": int(row["coverage_of"]),
                    "pct": float(row["coverage_pct"]),
                },
            }
    return out


# Levels shown in the comparison column, nearest first. Arrondissement is
# computed but deliberately NOT shown -- see docs/features/comparison.md,
# "The comparison set". A Brussels commune has no province and simply gets
# two entries instead of three; nothing here assumes a fixed depth.
COMPARISON_LEVELS = ("province", "region", "country")


def _additive_indicators(db_path: Path) -> set[str]:
    """Indicators that are counts or totals rather than ratios.

    Needed by the page, not just the maths: a commune is PART of its province,
    so "Antwerp population is -70.7% of Antwerp province" is arithmetically
    true and useless, while "Antwerp holds 29.3% of its province" is the
    figure a reader wants. A ratio is the opposite -- it belongs on the same
    scale as its reference, so the difference is what means something.

    Read from the indicators table rather than inferred from the unit: a
    total and an average can share the unit `eur`, so the unit cannot carry
    this. Derived indicators are absent from that table and correctly default
    to non-additive.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        return {
            row[0]
            for row in conn.execute("SELECT indicator_id FROM indicators WHERE is_additive = 1")
        }
    finally:
        conn.close()


def _attach_comparisons(
    communes: dict[str, dict],
    aggregates: dict[tuple[str, str, str], dict],
    ancestry: dict[str, list[str]],
) -> int:
    """Attach province/region/Belgium values to each commune's indicators.

    Matched at the commune's OWN latest period for that indicator, never at
    the aggregate's latest: comparing a commune's 2023 income against
    Belgium's 2026 would be a different kind of wrong number, and the
    mismatch would be invisible on screen. If the aggregate does not exist
    for that exact period, the entry is absent -- the payload format's
    absent-not-null rule.
    """
    attached = 0
    for commune in communes.values():
        for indicator_id, entry in commune["indicators"].items():
            periods = sorted(entry["periods"])
            if not periods:
                continue
            period = periods[-1]
            comparison = {}
            for ancestor in ancestry.get(commune["geo_id"], []):
                agg = aggregates.get((ancestor, indicator_id, period))
                if agg and agg["level"] in COMPARISON_LEVELS:
                    comparison[agg["level"]] = agg
            if comparison:
                entry["comparison"] = comparison
                attached += 1
    return attached


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def _git_commit(repo_root: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo_root, text=True
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def export_site_payloads(
    db_path: Path,
    communes_history_csv: Path,
    communes_latest_csv: Path,
    national_csv: Path,
    out_dir: Path,
    build_id: str,
    validation_status: str,
    aggregates_csv: Path | None = None,
    percentiles_csv: Path | None = None,
) -> dict[str, int]:
    communes = _read_communes_history(communes_history_csv)
    indicators = _read_communes_latest(communes_latest_csv)
    national = _read_national(national_csv)
    geographies = _build_geographies(db_path)

    additive = _additive_indicators(db_path)
    for commune in communes.values():
        for indicator_id, entry in commune["indicators"].items():
            entry["additive"] = indicator_id in additive

    compared = 0
    if aggregates_csv is not None:
        compared = _attach_comparisons(
            communes, _read_aggregates(aggregates_csv), _ancestry(db_path)
        )

    ranked = 0
    if percentiles_csv is not None:
        ranked = _attach_percentiles(communes, _read_percentiles(percentiles_csv))

    _write_json(out_dir / "national.json", {"geo_id": "be:country", "indicators": national})

    for nis, payload in communes.items():
        _write_json(out_dir / "communes" / f"{nis}.json", payload)

    for indicator_id, payload in indicators.items():
        _write_json(
            out_dir / "indicators" / f"{indicator_id}.json",
            {"indicator_code": indicator_id, **payload},
        )

    _write_json(out_dir / "metadata" / "geographies.json", {"geographies": geographies})

    manifest = {
        "build_id": build_id,
        "git_commit": _git_commit(db_path.resolve().parents[0]),
        "build_date": datetime.now(timezone.utc).isoformat(),
        "datasets": {
            "national": {"indicators": len(national)},
            "comparisons": {"indicator_cells": compared},
            "percentiles": {"indicator_cells": ranked},
            "communes": {"count": len(communes), "indicators": len(indicators)},
            "geographies": {"count": len(geographies)},
        },
        "validation_status": validation_status,
    }
    _write_json(out_dir / "manifest.json", manifest)

    return {
        "comparisons": compared,
        "percentiles": ranked,
        "national_indicators": len(national),
        "communes": len(communes),
        "indicator_files": len(indicators),
        "geographies": len(geographies),
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export per-entity site payloads for the /local interface"
    )
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file (for geographies)")
    ap.add_argument("--communes-history", default="data/communes_history.csv")
    ap.add_argument("--communes-latest", default="data/communes_export.csv")
    ap.add_argument("--national", default="data/belgian_macro_export.csv")
    ap.add_argument(
        "--aggregates",
        default="data/aggregates.csv",
        help="Aggregate CSV from export_aggregates_csv.py; comparisons are omitted if absent",
    )
    ap.add_argument(
        "--percentiles",
        default="data/percentiles.csv",
        help="Peer positions from export_percentiles_csv.py; omitted if absent",
    )
    ap.add_argument("--out-dir", default="public/data")
    ap.add_argument("--build-id", default="local")
    ap.add_argument("--validation-status", default="unknown")
    args = ap.parse_args()

    counts = export_site_payloads(
        Path(args.db),
        Path(args.communes_history),
        Path(args.communes_latest),
        Path(args.national),
        Path(args.out_dir),
        args.build_id,
        args.validation_status,
        Path(args.aggregates) if args.aggregates else None,
        Path(args.percentiles) if args.percentiles else None,
    )
    print(
        f"Exported {counts['communes']} commune payloads, {counts['indicator_files']} "
        f"indicator payloads, {counts['national_indicators']} national indicators, "
        f"{counts['comparisons']} comparison cells, {counts['percentiles']} ranked cells, "
        f"{counts['geographies']} geographies to {args.out_dir}"
    )


if __name__ == "__main__":
    main()
