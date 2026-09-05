"""
Load the Belgian geography hierarchy and merger lineage into `geographies`.

Reads only the committed CSVs under config/geography/ -- never the raw Statbel
spreadsheets, which are gitignored -- so this runs on a fresh clone with no
downloads. Regenerating those CSVs from raw is a separate manual step
(scripts/derive_geography_csv.py).

Two kinds of row are written:

1. **Current entities** (622): the country, 3 regions, 10 provinces, 43
   arrondissements and 565 communes valid today, with `parent_geo_id` chains
   terminating at `be:country` and `valid_to` NULL.

2. **Historical predecessors** (55): communes that the 2019 and 2025 merger
   waves ended. They carry `valid_to` and `successor_geo_id`, which is what
   lets resolve_geo() return the entity that actually reported a 2015 figure
   rather than the commune that exists today. Their `parent_geo_id` is left
   NULL -- the arrondissement they belonged to at the time is not recoverable
   from these files, and inventing one would attach historical numbers to a
   possibly-wrong province (the Hasselt/Kortessem merger crossed an
   arrondissement boundary). See docs/features/geography.md.

`be:country` is upserted, never deleted: every observation in the database
references it by foreign key.

Usage:
    python scripts/load_geography.py --db data/belgian_macro.db
"""

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.geography.refnis import geo_id_for  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO_ROOT / "config" / "geography"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("load_geography")

UPSERT_SQL = """
INSERT INTO geographies
    (geo_id, nis_code, level, name_nl, name_fr, name_en, parent_geo_id,
     valid_from, valid_to, successor_geo_id, population, area_km2)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
ON CONFLICT(geo_id) DO UPDATE SET
    nis_code         = excluded.nis_code,
    level            = excluded.level,
    name_nl          = excluded.name_nl,
    name_fr          = excluded.name_fr,
    name_en          = excluded.name_en,
    parent_geo_id    = excluded.parent_geo_id,
    valid_from       = excluded.valid_from,
    valid_to         = excluded.valid_to,
    successor_geo_id = excluded.successor_geo_id
"""


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Run scripts/derive_geography_csv.py to regenerate it "
            "from the raw Statbel files."
        )
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _none_if_blank(value: str | None) -> str | None:
    return value if value else None


def load(db_path: Path, config_dir: Path = CONFIG_DIR) -> tuple[int, int]:
    """Returns (current entities written, historical predecessors written)."""
    hierarchy = _read_csv(config_dir / "geographies.csv")
    crosswalk = _read_csv(config_dir / "municipality_crosswalk.csv")

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON")

    # Parents must exist before children, or the foreign key rejects the row.
    # The hierarchy is shallow and fixed, so an explicit order beats a
    # topological sort nobody would ever need to re-read.
    level_order = {
        "country": 0,
        "region": 1,
        "province": 2,
        "arrondissement": 3,
        "municipality": 4,
    }
    ordered = sorted(hierarchy, key=lambda r: level_order[r["level"]])

    for row in ordered:
        conn.execute(
            UPSERT_SQL,
            (
                row["geo_id"],
                _none_if_blank(row["nis_code"]),
                row["level"],
                row["name_nl"],
                row["name_fr"],
                row["name_en"],
                _none_if_blank(row["parent_geo_id"]),
                row["valid_from"],
                _none_if_blank(row["valid_to"]),
                _none_if_blank(row["successor_geo_id"]),
            ),
        )

    historical = 0
    for row in crosswalk:
        successors = [s for s in row["new_nis"].split(";") if s]
        # A predecessor split across several successors has no single lineage
        # target; it is flagged for review in the CSV and left without a
        # successor here rather than being pointed at an arbitrary one.
        successor_geo_id = (
            geo_id_for("municipality", successors[0]) if len(successors) == 1 else None
        )
        conn.execute(
            UPSERT_SQL,
            (
                geo_id_for("municipality", row["old_nis"]),
                row["old_nis"],
                "municipality",
                row["old_name_nl"],
                row["old_name_fr"],
                # No exonym list applies to communes that no longer exist, and
                # the vintage files carry no English. The Dutch name is the
                # honest placeholder rather than a fabricated translation.
                row["old_name_nl"],
                None,
                row["valid_from"],
                row["valid_to"],
                successor_geo_id,
            ),
        )
        historical += 1

    conn.commit()
    conn.close()
    return len(ordered), historical


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Belgian geography into the canonical schema")
    parser.add_argument("--db", required=True, help="Path to the SQLite DB file")
    parser.add_argument("--config-dir", default=str(CONFIG_DIR), help="Geography CSV directory")
    args = parser.parse_args()

    current, historical = load(Path(args.db), Path(args.config_dir))
    print(f"Loaded {current} current geography rows and {historical} historical predecessors.")


if __name__ == "__main__":
    main()
