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
) -> dict[str, int]:
    communes = _read_communes_history(communes_history_csv)
    indicators = _read_communes_latest(communes_latest_csv)
    national = _read_national(national_csv)
    geographies = _build_geographies(db_path)

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
            "communes": {"count": len(communes), "indicators": len(indicators)},
            "geographies": {"count": len(geographies)},
        },
        "validation_status": validation_status,
    }
    _write_json(out_dir / "manifest.json", manifest)

    return {
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
    )
    print(
        f"Exported {counts['communes']} commune payloads, {counts['indicator_files']} "
        f"indicator payloads, {counts['national_indicators']} national indicators, "
        f"{counts['geographies']} geographies to {args.out_dir}"
    )


if __name__ == "__main__":
    main()
