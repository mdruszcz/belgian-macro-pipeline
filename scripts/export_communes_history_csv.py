"""
Export EVERY period of municipal-level observations, one row per
(commune, indicator, period) -- the "time-series view" export_communes_csv.py
names as "a different, future feature". Feeds communes.html's year selector.

export_communes_csv.py deliberately keeps only the most recent period per
(commune, indicator), because that CSV's contract (one snapshot row per
commune x indicator) is depended on elsewhere: the export_parses validation
rule, its own tests. This is a SEPARATE file with a separate contract, so
neither has to compromise for the other.

Also computes and includes every configured DERIVED indicator
(config/indicators/derived/*.yaml) via the Block G engine, across every
period the raw data covers -- AVG_NET_TAXABLE_INCOME, DEPENDENCY_RATIO,
POPULATION_CHANGE_5Y, POPULATION_CAGR_10Y, POPULATION_PERCENTILE all appear
as ordinary indicator columns. communes.html needs no derived-specific code;
it just sees more indicator_codes, same as it already does for a second raw
one (CONTROL G: computed on the way out, never written to a database).

THE PEER SET TRAP. A percentile must rank a commune against the communes
that existed in ITS year -- 589 in 2016-2018, 581 in 2019-2024, 565 from
2025 (docs/features/derived_indicators.md). export_communes_csv.py's own
query restricts to CURRENT communes only (g.valid_to IS NULL), which is
correct for a snapshot of today's communes but WRONG as an input to the
engine: it would rank every year against 565, understating a 2016 commune's
percentile by counting a denominator 24 communes too small. So the engine
here is fed the UNRESTRICTED observation set (every historical geo_id the
population CSV carries), and only the final OUTPUT rows are filtered down to
current communes -- the same two-stage shape as sync_population.py resolving
historical NIS codes and export_communes_csv.py display only today's set.
"""

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from export_communes_csv import STATUS_TO_LETTER, _ancestor_names  # noqa: E402

from src.analytics.engine import ObservationSet, compute  # noqa: E402
from src.validation.config_schema import load_and_validate_derived  # noqa: E402

DEFAULT_DERIVED_DIR = Path(__file__).resolve().parents[1] / "config" / "indicators" / "derived"


def _all_municipal_rows_from_db(conn: sqlite3.Connection) -> list[tuple]:
    """Every is_latest=1 municipal observation, EVERY geo_id -- including a
    commune merged away since, if one ever carries a municipal observation.
    (Measured: none does today; LOCAL_UNITS_BY_COMMUNE and the fiscal
    indicators all resolve to current geo_ids only. Kept unrestricted anyway
    so this does not silently start dropping rows the day that changes.)
    """
    return conn.execute("""
        SELECT o.geo_id, o.indicator_id, i.name_en, i.unit, o.period, o.value,
               o.status, o.created_at
        FROM observations o
        JOIN indicators i ON o.indicator_id = i.indicator_id
        JOIN geographies g ON g.geo_id = o.geo_id AND g.level = 'municipality'
        WHERE o.is_latest = 1
        """).fetchall()


def _all_rows_from_csv(csv_path: Path, indicator_meta: dict[str, tuple[str, str]]) -> list[tuple]:
    """Every is_latest=1 row in a committed observations CSV -- every period,
    every geo_id, unlike export_communes_csv.py's `_latest_rows_from_csv`.
    """
    if not csv_path.is_file():
        raise FileNotFoundError(
            f"--extra-observations {csv_path} does not exist. Refusing to silently "
            "export a history file missing that source's indicators."
        )
    out = []
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["is_latest"] != "1":
                continue
            indicator_id = row["indicator_id"]
            if indicator_id not in indicator_meta:
                raise ValueError(
                    f"{csv_path.name} references indicator {indicator_id!r} absent from "
                    "the `indicators` table. Refusing to guess its name and unit."
                )
            name_en, unit = indicator_meta[indicator_id]
            out.append(
                (
                    row["geo_id"],
                    indicator_id,
                    name_en,
                    unit,
                    row["period"],
                    float(row["value"]) if row["value"] != "" else None,
                    row["status"],
                    row["created_at"],
                )
            )
    return out


def export_communes_history_csv(
    db_path: Path,
    out_path: Path,
    extra_observations: tuple[Path, ...] = (),
    derived_dir: Path = DEFAULT_DERIVED_DIR,
) -> int:
    conn = sqlite3.connect(str(db_path))
    communes = conn.execute(
        "SELECT geo_id, nis_code, name_en, name_fr, name_nl FROM geographies "
        "WHERE level = 'municipality' AND valid_to IS NULL ORDER BY nis_code"
    ).fetchall()
    commune_by_id = {geo_id: (nis, en, fr, nl) for geo_id, nis, en, fr, nl in communes}
    ancestors = {geo_id: _ancestor_names(conn, geo_id) for geo_id in commune_by_id}

    indicator_meta = {
        row[0]: (row[1], row[2])
        for row in conn.execute("SELECT indicator_id, name_en, unit FROM indicators")
    }

    raw = _all_municipal_rows_from_db(conn)
    for csv_path in extra_observations:
        raw = raw + _all_rows_from_csv(csv_path, indicator_meta)

    # The engine sees every geo_id (current AND historical) so a percentile's
    # peer set has the right size for its year -- see the module docstring.
    obs_set = ObservationSet(
        (indicator_id, geo_id, period, value)
        for geo_id, indicator_id, _name, _unit, period, value, _status, _created in raw
    )
    known_ids = {row[1] for row in raw}
    derived_cfgs = load_and_validate_derived(derived_dir, known_ids) if derived_dir.is_dir() else {}
    result = compute(obs_set, derived_cfgs, known_ids)

    # Raw rows: filtered down to CURRENT communes only for display.
    obs = [row for row in raw if row[0] in commune_by_id]

    # Derived rows: same current-communes filter, and a cell with no value
    # (an indicator not yet computable for that year -- e.g. a 10-year CAGR
    # before ten years of history exist) is DROPPED, not written as a blank
    # row. That mirrors how a missing raw observation already renders as
    # "n/a" in communes.html: absent, not an explicit null.
    for ind_id, cfg in derived_cfgs.items():
        name_en = cfg["name"]["en"]
        unit = cfg["unit"]
        for geo_id, period in sorted(result.cells(ind_id)):
            if geo_id not in commune_by_id:
                continue
            value = result.value(ind_id, geo_id, period)
            if value is None:
                continue
            obs.append((geo_id, ind_id, name_en, unit, period, value, "derived", ""))

    obs.sort(key=lambda r: (r[0], r[1], r[4]))
    conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(
            [
                "geo_id",
                "nis_code",
                "name_en",
                "name_fr",
                "name_nl",
                "region",
                "province",
                "arrondissement",
                "indicator_code",
                "indicator_name",
                "unit",
                "period",
                "value",
                "status",
                "fetched_at",
            ]
        )
        for geo_id, indicator_id, ind_name, unit, period, value, status, created_at in obs:
            nis, name_en, name_fr, name_nl = commune_by_id[geo_id]
            a = ancestors.get(geo_id, {})
            writer.writerow(
                [
                    geo_id,
                    nis,
                    name_en,
                    name_fr,
                    name_nl,
                    a.get("region", ""),
                    a.get("province", ""),
                    a.get("arrondissement", ""),
                    indicator_id,
                    ind_name,
                    unit,
                    period,
                    value,
                    STATUS_TO_LETTER.get(status, status or ""),
                    created_at,
                ]
            )
    return len(obs)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export the full time series of municipal observations, including derived indicators"
    )
    ap.add_argument("--db", required=True, help="Path to the SQLite DB file")
    ap.add_argument("--out", default="data/communes_history.csv", help="Output CSV path")
    ap.add_argument(
        "--extra-observations",
        action="append",
        default=[],
        metavar="CSV",
        help="Manual-only committed observations CSV to merge in (repeatable).",
    )
    ap.add_argument("--derived-dir", type=Path, default=DEFAULT_DERIVED_DIR)
    args = ap.parse_args()
    n = export_communes_history_csv(
        Path(args.db),
        Path(args.out),
        tuple(Path(p) for p in args.extra_observations),
        args.derived_dir,
    )
    print(f"Exported {n} rows to {args.out}")


if __name__ == "__main__":
    main()
