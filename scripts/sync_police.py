"""Load the federal police's house-burglary rate file -- docs/data_catalog.md's
police.be row, the pipeline's 14th dataset and its first from stats-pol.

MANUAL ONLY, and for a different reason than Statbel/ONEM. onem.be and
statbel.fgov.be fail at the network layer (TCP handshake timeout); police.be
responds, but with an HTTP 403 "Maintenance" page and no session cookie, to
every request this pipeline's own network context and GitHub Actions' runner
both send (see scripts/fetch_police_raw.py). The maintainer fetched this one
file from their own browser instead, where the same request returns 200.

THE FILE: a single JSON object, `{"error": false, "data": [...]}`, one row
per commune, `{"geo_code": nis, "z": rate}` -- house burglaries per 10,000
inhabitants. Committed at
`data/raw/police/police 2025 cambriolages dans les maisons par 10000hab.txt`.
Three OTHER files the maintainer supplied alongside it
(`police2025.txt`/`a`/`b`) are GeoJSON commune/province boundary shapes for
drawing a map, not statistics -- two are byte-identical duplicates -- and are
not read here.

SIX OF THE FILE'S 587 `geo_code`S ARE NOT REAL GEOGRAPHY, under either
period: the three negative placeholders -1/-3/-4 (a residual/unknown bucket
in the source's own export), plus 21020, 23095 and 31999, which resemble
NIS codes but match no geography row at all, current or historical. All six
are always paired with `z: 0`. Skipped, counted, and -- unlike every other
"unresolved code" case in this pipeline -- NOT fatal, because their value is
always exactly 0: rule 13 forbids guessing a mapping for a code that
carries a real number, not refusing to store a well-formed zero that names
no commune. This is a GENERAL rule (any code unresolvable under both
periods, with value 0, is skipped) rather than a hardcoded list of the six
codes seen today, so a future file's own garbage rows are handled the same
way without needing this script edited first. A nonzero value on an
unresolvable code is a different situation entirely and raises.

THE GEOGRAPHY IS THE MAP AS IT STOOD THROUGH 2024-12-31, NOT THE CURRENT
ONE, despite the file's own "2025" label -- confirmed by 32 of its 587
`geo_code`s naming communes that merged away on 2025-01-01 (e.g. Borsbeek,
Zwijndrecht, Gooik+Herne+Gammerages who became Pajottegem), while the 13
communes CREATED by that merger wave are entirely absent from the file.
Opposite direction from ONEM, which backcasts the CURRENT map onto its past
years -- here the source has simply not caught up to the merger yet.
Resolved by trying `resolve_geo(nis, "2025")` first (works for every code
whose commune survived the mergers) and falling back to `resolve_geo(nis,
"2024")` for a code only valid before them, rather than guessing which
successor commune a predecessor's rate should attach to -- there usually
is none that is a clean 1:1 match, and a burglary RATE cannot be
apportioned across a merger the way a count could be summed.

STATUS IS "provisional", NOT "final" -- the one substantive thing this
loader is honestly unsure of. Nothing in the file or its filename states
whether "2025" is a completed calendar year, a rolling 12-month window, or
a year-to-date snapshot. ONEM's part-year problem (a euro TOTAL for two
months of a year, which reads like an 83% collapse) is the reason this
distinction matters at all; here there is no prior year to compare against
to even notice if it is wrong, so "provisional" records the uncertainty
rather than asserting a finality nobody has confirmed.

NOT AGGREGATABLE, ON PURPOSE. HOUSE_BURGLARIES_PER_10K is a rate with no
underlying count in the source to derive it from, so it cannot be summed
(it is not additive) and there is no recompute path (CLAUDE.md rule 6 is
about deriving a ratio from STORED additive components; there are none
here). `is_additive=0`, `aggregation_method='not_applicable'` --
export_aggregates_csv.py's methods_from_metadata() then refuses it, so it
shows at commune level only, with no province/region/Belgium comparison row
manufactured by averaging a rate across communes (docs/decisions/0003).
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.geography.resolve import UnknownGeographyError, resolve_geo  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
DEFAULT_SOURCE_FILE = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "raw"
    / "police"
    / "police 2025 cambriolages dans les maisons par 10000hab.txt"
)

INDICATOR_ID = "HOUSE_BURGLARIES_PER_10K"
PERIOD = "2025"
FALLBACK_PERIOD = "2024"  # see module docstring: the file's own pre-merger map


def _read_rates(path: Path) -> dict[str, float]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("error"):
        raise ValueError(f"{path.name} itself reports error=true: {payload}")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError(
            f"{path.name} has no usable 'data' list. Its top-level keys are "
            f"{list(payload.keys())}. Refusing to guess a replacement (CLAUDE.md rule 13)."
        )

    rates: dict[str, float] = {}
    for row in rows:
        code = str(row["geo_code"])
        rates[code] = float(row["z"])
    return rates


def _resolve(conn: sqlite3.Connection, nis: str) -> str:
    try:
        return resolve_geo(conn, nis, PERIOD)
    except UnknownGeographyError:
        return resolve_geo(conn, nis, FALLBACK_PERIOD)


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('police', 'Police Fédérale / Federale Politie — stats-pol',
                "Police Fédérale — Direction de l'information policière et des moyens ICT",
                'police', 'https://www.police.be/statistiques/', ?,
                'docs/data_catalog.md -- police.be row', 'annual (manual, ad hoc)', 1)
        """,
        (
            "Attribution required, maintainer-supplied 2026-09-06 -- see "
            "docs/data_catalog.md's police.be row for the exact text and its caveats.",
        ),
    )
    ind = indicator_configs[INDICATOR_ID]
    conn.execute(
        """
        INSERT OR IGNORE INTO indicators
            (indicator_id, source_id, name_nl, name_fr, name_en,
             description_nl, description_fr, description_en,
             frequency, unit, preferred_direction, aggregation_method,
             is_additive, decimals, config_path, is_active)
        VALUES (?, 'police', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'not_applicable', 0, 1, ?, 1)
        """,
        (
            INDICATOR_ID,
            ind["name"]["nl"],
            ind["name"]["fr"],
            ind["name"]["en"],
            ind.get("description", {}).get("en", ""),
            ind["frequency"],
            ind["unit"],
            ind["preferred_direction"],
            f"config/indicators/{INDICATOR_ID}.yaml",
        ),
    )
    conn.commit()


def sync(
    db_path: Path, source_file: Path = DEFAULT_SOURCE_FILE, reference_rows_only: bool = False
) -> tuple[int, int]:
    indicator_configs, _sources = load_and_validate_all(
        CONFIG_DIR / "indicators", CONFIG_DIR / "sources"
    )
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    _ensure_reference_rows(conn, indicator_configs)
    if reference_rows_only:
        conn.close()
        return 0, 0

    if not source_file.is_file():
        raise FileNotFoundError(
            f"{source_file} not found. Hand-fetched from police.be -- see "
            "docs/features/manual_sources.md."
        )
    rates = _read_rates(source_file)

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("police", "police", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    period_start, period_end = derive_period_bounds(PERIOD, "A")
    rows_read = rows_written = 0
    unresolved: list[str] = []
    skipped_zero_placeholder = 0

    for nis, value in sorted(rates.items()):
        rows_read += 1
        try:
            geo_id = _resolve(conn, nis)
        except UnknownGeographyError:
            if value == 0:
                # See module docstring: a code that names no commune under
                # either period AND carries a value of exactly 0 is treated
                # as one of the source's own placeholder rows, not a real
                # geography this pipeline failed to load. A nonzero value on
                # an unresolvable code is a different situation and is not
                # forgiven the same way.
                skipped_zero_placeholder += 1
                continue
            unresolved.append(nis)
            continue
        rows_written += upsert_observation(
            conn,
            indicator_id=INDICATOR_ID,
            geo_id=geo_id,
            period=PERIOD,
            period_start=period_start,
            period_end=period_end,
            value=value,
            status="provisional",
            vintage=now,
            fetch_run_id=fetch_run_id,
        )

    conn.execute(
        "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
        "WHERE fetch_run_id = ?",
        (datetime.now(timezone.utc).isoformat(), rows_read, rows_written, fetch_run_id),
    )
    conn.commit()
    conn.close()

    if unresolved:
        raise SystemExit(
            f"::error::{len(unresolved)} NIS code(s) did not resolve under {PERIOD} or "
            f"{FALLBACK_PERIOD}, e.g. {unresolved[:5]}. Refusing to load a partial series."
        )
    print(
        f"Read {rows_read} commune rates ({skipped_zero_placeholder} zero-valued "
        "non-geography code(s) skipped)."
    )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load the federal police's house-burglary rate file (manual, not daily)"
    )
    ap.add_argument("--db", required=True)
    ap.add_argument("--source-file", type=Path, default=DEFAULT_SOURCE_FILE)
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no source file",
    )
    args = ap.parse_args()
    read, written = sync(Path(args.db), args.source_file, args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for: {INDICATOR_ID}")
    else:
        print(f"Wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
