"""Load the Vlaamse Arbeidsrekening municipal unemployment-rate export.

The source is Steunpunt Werk's Tableau dashboard. It explicitly offers a
crosstab export, but the measure selector cannot be represented reliably in
the public CSV URL. Refresh is therefore manual: select municipalities,
total sex, ages 15-64, ``Werkloosheidsgraad (%)`` and the required years,
then download the ``BNW - T1 - Tabel`` crosstab as CSV. The resulting file is
UTF-16 tab-separated despite its .csv suffix.

The file exposes Dutch municipality labels but no NIS code. Resolution is
strict and deliberately narrow: labels are matched to the unique current
``name_nl`` after removing only Tableau's final disambiguation suffix, such
as ``Aalst (Aalst)``. No accent stripping or fuzzy matching is permitted.
All 565 current municipalities must occur exactly once per year.

Steunpunt Werk publishes its historical years on the current municipal grid,
including Beveren-Kruibeke-Zwijndrecht and Bilzen-Hoeselt in 2024. That is a
source-defined restatement, the same fixed-geography pattern already handled
for ONEM and police.be. We preserve it rather than mixing predecessor and
successor boundaries.
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
CONFIG_DIR = REPO / "config"
DEFAULT_SOURCE_FILE = REPO / "data" / "raw" / "var" / "var_unemployment_15_64.csv"

SOURCE_ID = "steunpunt_werk"
INDICATOR_ID = "ADMIN_UNEMPLOYMENT_RATE_COM"
MEASURE = "Werkloosheidsgraad (%)"
EXPECTED_COLUMNS = (
    "Geo",
    "Geslacht",
    "Leeftijdsklasse",
    "Jaar",
    "Geslacht_titel",
    MEASURE,
)

_PAREN_SUFFIX = re.compile(r"\s*\([^()]+\)\s*$")
_YEAR = re.compile(r"^20\d{2}$")


def normalize_label(label: str) -> str:
    """Normalise only presentation details, never spelling or accents."""
    label = unicodedata.normalize("NFKC", str(label)).replace("’", "'").strip()
    label = _PAREN_SUFFIX.sub("", label)
    return " ".join(label.casefold().split())


class CommuneIndex:
    def __init__(self, conn: sqlite3.Connection):
        rows = conn.execute("""
            SELECT geo_id, name_nl
              FROM geographies
             WHERE level = 'municipality'
               AND (valid_to IS NULL OR valid_to = '')
            """).fetchall()
        self.count = len(rows)
        by_name: dict[str, list[str]] = {}
        for geo_id, name_nl in rows:
            by_name.setdefault(normalize_label(name_nl), []).append(geo_id)
        ambiguous = {name: ids for name, ids in by_name.items() if len(ids) != 1}
        if ambiguous:
            raise ValueError(
                f"Current Dutch municipality names are not unique after strict normalization: "
                f"{ambiguous}. Refusing name-based resolution."
            )
        self._by_name = {name: ids[0] for name, ids in by_name.items()}

    def resolve(self, label: str) -> str | None:
        return self._by_name.get(normalize_label(label))


def _parse_percent(raw: str) -> float | None:
    value = str(raw).strip()
    if not value:
        return None
    if not value.endswith("%"):
        raise ValueError(f"VAR rate {raw!r} has no percent sign; refusing a possible unit change.")
    return float(value[:-1].replace(".", "").replace(",", "."))


def read_export(path: Path, expected_communes: int = 565) -> list[dict]:
    if not path.is_file():
        raise FileNotFoundError(
            f"{path} not found. Export the Tableau crosstab as documented in "
            "docs/features/var_unemployment.md."
        )
    with path.open(encoding="utf-16", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        if tuple(reader.fieldnames or ()) != EXPECTED_COLUMNS:
            raise ValueError(
                f"{path} columns are {reader.fieldnames}, expected exactly {list(EXPECTED_COLUMNS)}. "
                "Refusing to guess after a Tableau layout or filter change."
            )
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path} contains no data rows.")

    periods = Counter(str(row["Jaar"]).strip() for row in rows)
    bad_periods = sorted(period for period in periods if not _YEAR.fullmatch(period))
    if bad_periods:
        raise ValueError(f"Unexpected VAR period labels: {bad_periods}")
    wrong_counts = {
        period: count for period, count in periods.items() if count != expected_communes
    }
    if wrong_counts:
        raise ValueError(
            f"VAR requires exactly {expected_communes} municipality rows per year; got {wrong_counts}."
        )

    seen: set[tuple[str, str]] = set()
    parsed = []
    for row in rows:
        if row["Geslacht"] != "Totaal" or row["Geslacht_titel"] != "Mannen en vrouwen":
            raise ValueError(f"VAR export is not filtered to total sex: {row}")
        if row["Leeftijdsklasse"] != "15-64":
            raise ValueError(f"VAR export is not filtered to ages 15-64: {row}")
        period = row["Jaar"].strip()
        label = row["Geo"].strip()
        key = (period, normalize_label(label))
        if key in seen:
            raise ValueError(f"Duplicate VAR municipality/year row: {label!r}, {period}")
        seen.add(key)
        parsed.append({"label": label, "period": period, "value": _parse_percent(row[MEASURE])})
    return parsed


def _ensure_reference_rows(
    conn: sqlite3.Connection, indicator_configs: dict, source_configs: dict
) -> None:
    source = source_configs[SOURCE_ID]
    indicator = indicator_configs[INDICATOR_ID]
    conn.execute(
        """
        INSERT INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(source_id) DO UPDATE SET
            name = excluded.name,
            agency = excluded.agency,
            adapter = excluded.adapter,
            base_url = excluded.base_url,
            licence = excluded.licence,
            catalog_ref = excluded.catalog_ref,
            cadence = excluded.cadence,
            is_active = 1
        """,
        (
            SOURCE_ID,
            source["name"],
            source["agency"],
            source["adapter"],
            source.get("base_url"),
            source.get("licence"),
            source.get("catalog_ref"),
            source.get("cadence"),
        ),
    )
    conn.execute(
        """
        INSERT INTO indicators
            (indicator_id, source_id, name_nl, name_fr, name_en,
             description_nl, description_fr, description_en,
             frequency, unit, preferred_direction, aggregation_method,
             is_additive, decimals, config_path, is_active)
        VALUES (?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'not_applicable', 0, ?, ?, 1)
        ON CONFLICT(indicator_id) DO UPDATE SET
            source_id = excluded.source_id,
            name_nl = excluded.name_nl,
            name_fr = excluded.name_fr,
            name_en = excluded.name_en,
            description_en = excluded.description_en,
            frequency = excluded.frequency,
            unit = excluded.unit,
            preferred_direction = excluded.preferred_direction,
            aggregation_method = 'not_applicable',
            is_additive = 0,
            decimals = excluded.decimals,
            is_active = 1
        """,
        (
            INDICATOR_ID,
            SOURCE_ID,
            indicator["name"]["nl"],
            indicator["name"]["fr"],
            indicator["name"]["en"],
            indicator.get("description", {}).get("en", ""),
            indicator["frequency"],
            indicator["unit"],
            indicator["preferred_direction"],
            indicator.get("decimals", 1),
            f"config/indicators/{INDICATOR_ID}.yaml",
        ),
    )
    conn.commit()


def sync(
    db_path: Path,
    source_file: Path = DEFAULT_SOURCE_FILE,
    reference_rows_only: bool = False,
) -> tuple[int, int]:
    indicator_configs, source_configs = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, indicator_configs, source_configs)
    if reference_rows_only:
        conn.close()
        return 0, 0

    index = CommuneIndex(conn)
    rows = read_export(source_file, expected_communes=index.count)
    unmatched = sorted({row["label"] for row in rows if index.resolve(row["label"]) is None})
    if unmatched:
        conn.close()
        raise SystemExit(
            f"::error::{len(unmatched)} VAR municipality label(s) do not match a unique current "
            f"Dutch name, e.g. {unmatched[:10]}. Refusing to load a partial series."
        )

    periods = sorted({row["period"] for row in rows})
    for period in periods:
        covered = {index.resolve(row["label"]) for row in rows if row["period"] == period}
        if len(covered) != index.count:
            conn.close()
            raise SystemExit(
                f"::error::VAR {period} resolves to {len(covered)} of {index.count} current "
                "municipalities. Refusing incomplete coverage."
            )

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, 'ok')",
        (SOURCE_ID, "var", now),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    written = 0
    suppressed = 0
    for row in sorted(rows, key=lambda item: (item["period"], item["label"])):
        period_start, period_end = derive_period_bounds(row["period"], "A")
        value = row["value"]
        status = "suppressed" if value is None else "final"
        suppressed += int(value is None)
        written += upsert_observation(
            conn,
            indicator_id=INDICATOR_ID,
            geo_id=index.resolve(row["label"]),
            period=row["period"],
            period_start=period_start,
            period_end=period_end,
            value=value,
            status=status,
            vintage=now,
            fetch_run_id=fetch_run_id,
        )
    conn.execute(
        "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ?, message = ? "
        "WHERE fetch_run_id = ?",
        (
            datetime.now(timezone.utc).isoformat(),
            len(rows),
            written,
            f"manual Tableau crosstab; {suppressed} publisher-suppressed cell(s)",
            fetch_run_id,
        ),
    )
    conn.commit()
    conn.close()
    print(
        f"Read {len(rows)} VAR rates for {len(periods)} year(s), covering {index.count} current "
        f"municipalities; {suppressed} cell(s) suppressed by the publisher."
    )
    return len(rows), written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--db", required=True)
    parser.add_argument("--source-file", type=Path, default=DEFAULT_SOURCE_FILE)
    parser.add_argument("--reference-rows-only", action="store_true")
    args = parser.parse_args()
    read, written = sync(Path(args.db), args.source_file, args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for {INDICATOR_ID}.")
    else:
        print(f"Wrote {written} new vintage(s) from {read} source row(s).")


if __name__ == "__main__":
    main()
