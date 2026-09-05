"""
Load the Belgian geography hierarchy and merger lineage into `geographies`.

Reads only the committed CSVs under config/geography/ -- never the raw Statbel
spreadsheets, which are gitignored -- so this runs on a fresh clone with no
downloads. Regenerating those CSVs from raw is a separate manual step
(scripts/derive_geography_csv.py).

Every row is one *validity window* of one entity: 622 windows open today (the
country, 3 regions, 10 provinces, 43 arrondissements, 565 communes) and 66
closed ones -- the 55 communes ended by the 2019 and 2025 merger waves, plus
the aggregates whose territory changed while keeping their code.

Historical rows carry the parent they actually had at the time, taken from the
REFNIS vintage that recorded them. That matters: Kortessem sat in
arrondissement 73000 and was merged into Hasselt, which is in 71000, so
borrowing its successor's parent would file its history under the wrong
arrondissement. Loading them without a parent is equally wrong -- it silently
drops them from every historical province and region aggregate.

`be:country` is upserted, never deleted: every observation in the database
references it by foreign key.

The loader refuses to run while any crosswalk row still awaits the maintainer's
sign-off (`verified` in municipality_crosswalk.csv). The derivation flags rows
whose successor was guessed by name matching, or that involve a partial
boundary transfer; writing them regardless would make the flagging decorative.

Usage:
    python scripts/load_geography.py --db data/belgian_macro.db
"""

import argparse
import csv
import logging
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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


class UnverifiedCrosswalkError(Exception):
    """The crosswalk still contains rows the maintainer has not signed off."""


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _require_date(value: str, field: str, context: str) -> str:
    """`valid_from` is NOT NULL in the schema, but an empty string satisfies
    that and sorts before every real date -- which would make the entity valid
    for all of recorded history and resolve for any period asked."""
    if not _DATE_RE.match(value or ""):
        raise ValueError(
            f"{context}: {field}={value!r} is not a YYYY-MM-DD date. "
            "Refusing to load a validity window that would match every period."
        )
    return value


def _check_crosswalk_reviewed(crosswalk: list[dict], allow_unverified: bool) -> None:
    """Enforce the [H] review gate that the derivation's flags exist for.

    derive_crosswalk() carefully marks rows whose successor was guessed by name
    matching, or which involve a partial boundary transfer that is not a 1:1
    lineage at all. Loading those without a human sign-off would make the
    flagging decorative -- the loader would write exactly the confident
    successor links the flags exist to question.
    """
    if allow_unverified:
        return
    pending = [
        row
        for row in crosswalk
        if (row.get("note") or row.get("has_partial_transfer") == "true")
        and row.get("verified") != "true"
    ]
    if pending:
        listed = ", ".join(f"{r['old_nis']} ({r['old_name_nl']})" for r in pending)
        raise UnverifiedCrosswalkError(
            f"{len(pending)} crosswalk row(s) need maintainer verification before they may be "
            f"loaded: {listed}. Check each against official merger lists, then set "
            "verified=true in config/geography/municipality_crosswalk.csv. "
            "Pass --allow-unverified to load anyway (development only)."
        )


def _check_no_geo_id_collisions(hierarchy: list[dict]) -> None:
    """`geo_id` is the primary key and an upsert silently overwrites on
    conflict, so a duplicate would replace a live commune with another entity
    rather than failing."""
    seen: dict[str, str] = {}
    for row in hierarchy:
        previous = seen.get(row["geo_id"])
        if previous is not None:
            raise ValueError(
                f"Duplicate geo_id {row['geo_id']!r} in geographies.csv "
                f"({previous} and {row['nis_code']}). An upsert would silently overwrite."
            )
        seen[row["geo_id"]] = row["nis_code"]


def load(
    db_path: Path, config_dir: Path = CONFIG_DIR, allow_unverified: bool = False
) -> tuple[int, int]:
    """Returns (geography rows written, lineage links written).

    Raises UnverifiedCrosswalkError unless every crosswalk row the derivation
    flagged has been signed off, or `allow_unverified` is set explicitly.
    """
    hierarchy = _read_csv(config_dir / "geographies.csv")
    crosswalk = _read_csv(config_dir / "municipality_crosswalk.csv")
    _check_crosswalk_reviewed(crosswalk, allow_unverified)
    _check_no_geo_id_collisions(hierarchy)

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
                _require_date(row["valid_from"], "valid_from", row["geo_id"]),
                _none_if_blank(row["valid_to"]),
                _none_if_blank(row["successor_geo_id"]),
            ),
        )

    # Historical communes are already in geographies.csv, with the real parent
    # they had in the vintage that recorded them. The crosswalk's only job here
    # is lineage: pointing each predecessor at its successor.
    by_nis = {row["nis_code"]: row["geo_id"] for row in hierarchy if not row["valid_to"]}
    links = 0
    for row in crosswalk:
        successors = [s for s in row["new_nis"].split(";") if s]
        # A predecessor split across several successors, or one whose successor
        # could not be derived, has no single lineage target and is left without
        # one rather than pointed at an arbitrary commune.
        if len(successors) != 1:
            continue
        successor_geo_id = by_nis.get(successors[0])
        if successor_geo_id is None:
            raise ValueError(
                f"Crosswalk row {row['old_nis']} -> {successors[0]} names a successor that is "
                "not a currently-valid commune in geographies.csv. Refusing to write a dangling "
                "lineage link (CLAUDE.md rule 13)."
            )
        predecessor_geo_id = by_nis.get(row["old_nis"])
        if predecessor_geo_id is not None:
            raise ValueError(
                f"Crosswalk row {row['old_nis']} is also a currently-valid commune "
                f"({predecessor_geo_id}). Loading it would mark a live commune as ended."
            )
        updated = conn.execute(
            "UPDATE geographies SET successor_geo_id = ? WHERE nis_code = ? AND valid_to = ?",
            (successor_geo_id, row["old_nis"], row["valid_to"]),
        )
        if updated.rowcount != 1:
            raise ValueError(
                f"Crosswalk row {row['old_nis']} (ended {row['valid_to']}) matched "
                f"{updated.rowcount} geography rows, expected exactly 1. The crosswalk and "
                "the hierarchy disagree; refusing to guess."
            )
        links += 1

    conn.commit()
    conn.close()
    return len(ordered), links


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Belgian geography into the canonical schema")
    parser.add_argument("--db", required=True, help="Path to the SQLite DB file")
    parser.add_argument("--config-dir", default=str(CONFIG_DIR), help="Geography CSV directory")
    parser.add_argument(
        "--allow-unverified",
        action="store_true",
        help="Load crosswalk rows the maintainer has not signed off (development only)",
    )
    args = parser.parse_args()

    rows, links = load(Path(args.db), Path(args.config_dir), args.allow_unverified)
    print(f"Loaded {rows} geography rows and {links} lineage links.")


if __name__ == "__main__":
    main()
