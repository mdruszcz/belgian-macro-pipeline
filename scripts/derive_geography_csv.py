"""
Derive the committed geography reference CSVs from Statbel's raw downloads.

This is a *manual refresh* step, not part of the daily workflow. It reads the
raw Statbel files under data/raw/statbel/ -- which are deliberately gitignored,
being multi-megabyte binary spreadsheets -- and writes small, diffable,
human-reviewable CSVs under config/geography/, which are committed and are what
the pipeline actually loads.

That split is the point. A fresh clone can run the whole pipeline from the
committed CSVs without the .xlsx files, and a reviewer can see in a PR diff
exactly which of Belgium's 565 communes changed, rather than "a 2.7 MB binary
changed". Re-running this after a Statbel republication produces a diff that is
readable by a human, which is the only way an error in it gets caught.

Inputs (see docs/features/geography.md for the download procedure):
  Nis9_Nis6_refnis_names_01012026.xlsx  -- primary; explicit full hierarchy
  REFNIS_DEFINITIEF.csv                 -- pre-2019 vintage (589 communes)
  REFNIS_2019.csv                       -- post-2019 wave  (581 communes)
  REFNIS_2025.csv                       -- post-2025 wave  (565 communes)

Outputs:
  config/geography/geographies.csv            -- the administrative hierarchy
  config/geography/municipality_crosswalk.csv -- merger lineage, for [H] review

Usage:
    python scripts/derive_geography_csv.py
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.geography.crosswalk import (  # noqa: E402
    VINTAGE_DATES,
    derive_crosswalk,
    unresolved,
)
from src.geography.refnis import build_hierarchy, parse_nis9_xlsx, parse_refnis_csv  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "raw" / "statbel"
CONFIG_DIR = REPO_ROOT / "config" / "geography"

NIS9_FILE = "Nis9_Nis6_refnis_names_01012026.xlsx"
# Oldest first -- derive_crosswalk() diffs consecutive pairs.
REFNIS_VINTAGES = ("REFNIS_DEFINITIEF.csv", "REFNIS_2019.csv", "REFNIS_2025.csv")

GEOGRAPHIES_COLUMNS = [
    "geo_id",
    "nis_code",
    "level",
    "name_nl",
    "name_fr",
    "name_en",
    "parent_geo_id",
    "valid_from",
    "valid_to",
    "successor_geo_id",
    "nuts",
]
CROSSWALK_COLUMNS = [
    "old_nis",
    "old_name_nl",
    "old_name_fr",
    "new_nis",
    "relationship",
    "has_partial_transfer",
    "valid_from",
    "valid_to",
    "evidence",
    "verified",
    "note",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("derive_geography_csv")


def load_exonyms(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8") as handle:
        return {row["nis_code"]: row["name_en"] for row in csv.DictReader(handle)}


def _require(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing raw input {path}. See docs/features/geography.md for the "
            "Statbel download procedure; these files are gitignored by design."
        )
    return path


def _first_appearance(vintages: list[tuple[str, dict[str, str]]]) -> dict[str, str]:
    """nis_code -> valid_from, evidenced by the first vintage containing it."""
    valid_from: dict[str, str] = {}
    for filename, communes in vintages:
        wave_date = VINTAGE_DATES[filename]
        for code in communes:
            valid_from.setdefault(code, wave_date)
    return valid_from


def derive(raw_dir: Path, config_dir: Path) -> tuple[int, int, int]:
    """Returns (geography rows, crosswalk rows, crosswalk rows needing review)."""
    nis9_rows = parse_nis9_xlsx(_require(raw_dir / NIS9_FILE))
    log.info("Parsed %d statistical-sector rows from %s", len(nis9_rows), NIS9_FILE)

    vintages = []
    for filename in REFNIS_VINTAGES:
        rows = parse_refnis_csv(_require(raw_dir / filename))
        communes = {
            r["nis_code"]: {"name_nl": r["name_nl"], "name_fr": r["name_fr"]}
            for r in rows
            if r["level_hint"] == "commune"
        }
        vintages.append((filename, communes))
        log.info("Parsed %s: %d communes", filename, len(communes))

    latest_refnis = parse_refnis_csv(raw_dir / REFNIS_VINTAGES[-1])
    exonyms = load_exonyms(config_dir / "name_en_exonyms.csv")
    valid_from_by_code = _first_appearance(vintages)

    hierarchy = build_hierarchy(
        nis9_rows,
        latest_refnis,
        exonyms,
        valid_from_by_code=valid_from_by_code,
    )
    names_by_code = {e["nis_code"]: e["name_nl"] for e in hierarchy if e["nis_code"]}
    crosswalk = derive_crosswalk(vintages, nis9_rows, names_by_code, valid_from_by_code)

    config_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(config_dir / "geographies.csv", GEOGRAPHIES_COLUMNS, hierarchy)
    _write_csv(config_dir / "municipality_crosswalk.csv", CROSSWALK_COLUMNS, crosswalk)

    needs_review = unresolved(crosswalk)
    return len(hierarchy), len(crosswalk), len(needs_review)


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: ("" if row.get(c) is None else row.get(c)) for c in columns})
    log.info("Wrote %s (%d rows)", path.relative_to(REPO_ROOT), len(rows))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Statbel raw download directory")
    parser.add_argument("--config-dir", default=str(CONFIG_DIR), help="Output directory")
    args = parser.parse_args()

    geographies, crosswalk, needs_review = derive(Path(args.raw_dir), Path(args.config_dir))
    print(f"Derived {geographies} geography rows and {crosswalk} crosswalk rows.")
    if needs_review:
        print(
            f"{needs_review} crosswalk row(s) need maintainer verification "
            "(see the `note` column). CONTROL C is not satisfied until they are resolved."
        )


if __name__ == "__main__":
    main()
