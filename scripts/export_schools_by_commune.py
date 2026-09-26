"""Summarize the commune-assigned school-site ISE snapshot (export_schools_ise.py's
output, public/data/schools_ise_2025.json) into one small per-commune payload,
public/data/schools/by_commune.json.

READS public/data/schools_ise_2025.json only -- never the ODWB source directly, and
never SQLite (rule 20/21). Every number here is copied or averaged from that file's
own `ed`/`hed`/`nis`/`formula` fields; nothing is hand-typed (rule 36).

ONE OBJECT PER NIS WITH >= 1 SITE. No Belgium, region or province figure: an ISE
class is neither an additive count nor a ratio with a defensible aggregation
formula (CLAUDE.md's Definitions section) -- refused, not invented, at every level
above the commune.

MEAN CLASS, PER FORMULA. FO and SO are never averaged together (the publisher
states the two formulas are only comparable within themselves, and the source
export/tests already treat mixing them as an error). For each formula:
  sites             -- every site in the commune classified under that formula,
                        whether or not it carries an ED class.
  sites_with_class  -- of those, how many have a non-null `ed`.
  mean_class        -- arithmetic mean of `ed` over sites_with_class, "3a" and
                       "3b" counted as 3, rounded to 1 decimal; null if
                       sites_with_class is 0 (a formula this commune has zero
                       classified sites for is `mean_class: null`, not 0 -- rule 26,
                       an average of nothing is not a measured zero).
  min_class / max_class -- over the same sites_with_class, same "3a"/"3b" -> 3 rule.

A site with `nis: null` (unmatched against the site register, see
export_schools_ise.py) is excluded from every commune's totals and is not
itself a commune -- it is counted once, globally, in the top-level
`unassigned_sites`, copied straight from the source file.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
INPUT = REPO_ROOT / "public/data/schools_ise_2025.json"
OUTPUT = REPO_ROOT / "public/data/schools/by_commune.json"

FORMULAS = ("FO", "SO")


class ByCommuneExportError(ValueError):
    """The source snapshot is not the shape this summary requires."""


def _class_value(ed: str | None) -> int | None:
    """ED class as an integer rank: "1".."20" as-is, "3a"/"3b" as 3, None
    (no ED class -- an HED-only specialised site, or genuinely unclassified)
    stays None and is excluded from the mean (module docstring)."""
    if ed is None:
        return None
    if ed in ("3a", "3b"):
        return 3
    return int(ed)


def summarize(payload: dict) -> dict:
    records = payload.get("records")
    if records is None:
        raise ByCommuneExportError("input snapshot has no `records`")
    if "nis" not in (records[0] if records else {}):
        raise ByCommuneExportError(
            "input snapshot has no commune assignment (`nis`) -- run export_schools_ise.py "
            "with the site register, not the ISE file alone"
        )

    # {nis: {"commune": {...}, formula: [class_value_or_None, ...], "site_ids": set()}}
    by_nis: dict[str, dict] = {}
    for record in records:
        nis = record.get("nis")
        if nis is None:
            continue
        formula = record["formula"]
        if formula not in FORMULAS:
            raise ByCommuneExportError(f"unknown formula in source snapshot: {formula!r}")
        bucket = by_nis.setdefault(
            nis,
            {"commune": record["commune"], "site_ids": set(), **{f: [] for f in FORMULAS}},
        )
        bucket["site_ids"].add(record["site"])
        bucket[formula].append(_class_value(record["ed"]))

    communes: dict[str, dict] = {}
    for nis, bucket in by_nis.items():
        entry: dict = {
            "commune": bucket["commune"],
            "sites": len(bucket["site_ids"]),
            "site_ids": sorted(bucket["site_ids"]),
        }
        for formula in FORMULAS:
            classes = bucket[formula]
            classed = [c for c in classes if c is not None]
            entry[formula] = {
                "sites": len(classes),
                "sites_with_class": len(classed),
                "mean_class": round(sum(classed) / len(classed), 1) if classed else None,
                "min_class": min(classed) if classed else None,
                "max_class": max(classed) if classed else None,
            }
        communes[nis] = entry

    return {
        "source": [payload["source"], payload.get("register_source")],
        "publisher": payload["publisher"],
        "license": payload["license"],
        "year": payload["year"],
        "register_year": payload.get("register_year"),
        "unassigned_sites": payload.get("unassigned_sites", 0),
        "communes": communes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=INPUT)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    result = summarize(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"Summarized {len(result['communes'])} communes to {args.output}")


if __name__ == "__main__":
    main()
