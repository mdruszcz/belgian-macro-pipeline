"""Load ONEM/RVA's own commune-level unemployment RATE -- the second ONEM
feed, alongside scripts/sync_onem.py's six Excel tables.

WHY THIS EXISTS. Until now no series in this pipeline was a current
unemployment rate with a real labour-force denominator. UNEMPLOYMENT_RATE_COM
is Census 2021, frozen. ADMIN_UNEMPLOYMENT_RATE_COM is 2024 and
administrative. UNEMPLOYMENT_CLAIMANT_RATE_WORKING_AGE divides claimants by
the whole 15-64 population, which is not a labour force and reads about a
third of a real rate. UNEMPLOYMENT_RATE_BIT is the ILO figure but only for
Wallonia and only to 2023.

ONEM publishes the missing one and computes it themselves:

    "Le taux de chomage resulte de la division du nombre de CCI demandeurs
     d'emploi par le nombre d'assures contre le chomage.
     Source: calculs ONEM sur base des donnees de l'ONEM, de l'ONSS et de
     l'INAMI."

That sentence is printed on every page of their interactive statistics and
on the PDF export. The numerator is the same CCI-DE this repository already
loads as UNEMPLOYED_JOBSEEKERS; the denominator -- persons insured against
unemployment, assembled from three institutions -- is the part we could not
build ourselves. So the rate is taken AS PUBLISHED. Nothing here computes a
statistic (CLAUDE.md rule 4); the division is ONEM's.

HOW THE DATA IS REACHED, and why this is an ordinary fetch and not scraping.
The interactive map at interactivestats.services.rvaonem.fgov.be is a JSF
application with a session, a ViewState and AJAX POSTs. None of that is
needed: the page loads papaparse, and its own controller
(js/UnemploymentRatesController.js) names the file it parses. The file
answers a plain GET with no cookie, no session and no referer:

    https://interactivestats.services.rvaonem.fgov.be
        /interactivestats/csvResource/interact_taux_V1.csv

2.0 MB, semicolon-separated, 77,376 rows, the whole published history in one
request. Verified 2026-09-12: byte-identical whether fetched with or without
the browser's session cookies.

WHAT THE FILE CONTAINS, read from it rather than assumed:

  * header exactly `jaar;maand;level;zonegeog;graad;diff1an`, quoted in the
    header row only;
  * `level` 1 = Belgium (zone 99), 2 = region, 3 = province, 4 =
    arrondissement, 5 = commune. 565 commune zones, and `zonegeog` at that
    level IS the NIS code;
  * `maand` 1-12 are the months; 13 is that year's average;
  * 124 periods per zone, 2017-01 to 2026-06 plus each year's 13;
  * `graad` always present, always two decimals, never masked. 70,060
    commune values, zero blanks.

ONLY LEVELS 5 AND 1 ARE LOADED, and the reason is not laziness. The rate's
denominator is not published, so an aggregate cannot be RECOMPUTED from
commune figures the way ADR 0003 requires -- and ONEM's own aggregates use
ONEM's own zone codes, which are not this repository's. Level 2 is decisive:
it splits `Region wallonne a l'excl. de la Com. germ.` (55) from
`Com. germanophone` (56), whereas be:reg:03000 here is Wallonia INCLUDING
the German-speaking communes. Loading 55 as Wallonia would publish a
different territory under our name. Level 3 uses codes `0` and `29` that are
not province NIS codes. Belgium (level 1, zone 99) is unambiguous and is
loaded. Everything between is left out; see
docs/decisions/0005-onem-published-rate.md.

ONEM BACKCASTS TODAY'S COMMUNE MAP here exactly as it does in the Excel
tables: all 124 periods carry the same 565 codes, 31 of which did not exist
in January 2017. Resolution is therefore against CURRENT municipalities
rather than period by period, the same pinned-vintage treatment
scripts/sync_onem.py already documents.

THE `diff1an` COLUMN IS NOT LOADED -- it is the 12-month change, which our
own derived engine can compute, and writing a publisher's derived value in as
source data is CLAUDE.md rule 6. It is used instead as a free integrity check
on this parser: measured over 69,264 pairs it equals the 12-month difference
of `graad` to within 0.01 pp of rounding, every time. If a column shifts or a
key is built wrong, that check breaks before anything is written.

A MARCH 2026 DEFINITIONAL BREAK IS CARRIED, NOT SMOOTHED. Belgium reads
6.28 % in February 2026, 5.47 in March, 4.41 in April; the fall appears in
546 of 565 communes. Time-limiting unemployment benefit removes people from
the CCI-DE numerator whether or not they find work. The loader does not
touch it -- suppressing or flattening a real published movement would be
worse -- but both indicator descriptions state it, and the commune page must
never render that fall as a labour-market improvement.
"""

import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
RAW_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "onem_rates"

CSV_URL = (
    "https://interactivestats.services.rvaonem.fgov.be"
    "/interactivestats/csvResource/interact_taux_V1.csv"
)
CSV_NAME = "interact_taux_V1.csv"
REQUEST_TIMEOUT = 120

# The header this parser was written against, in order. Compared exactly: a
# renamed or reordered column is a schema change and must crash (CLAUDE.md
# rule 13), never be guessed at.
EXPECTED_HEADER = ["jaar", "maand", "level", "zonegeog", "graad", "diff1an"]

LEVEL_COUNTRY = "1"
LEVEL_MUNICIPAL = "5"
COUNTRY_ZONE = "99"
COUNTRY_GEO_ID = "be:country"

ANNUAL_MONTH = 13  # ONEM's own code for "average over the year"

INDICATOR_ANNUAL = "UNEMPLOYMENT_RATE_INSURED"
INDICATOR_MONTHLY = "UNEMPLOYMENT_RATE_INSURED_MONTHLY"
INDICATOR_IDS = [INDICATOR_ANNUAL, INDICATOR_MONTHLY]

# HOW MUCH MONTHLY DETAIL IS KEPT, and why it is not "all of it".
#
# THE BINDING CONSTRAINT IS daily_fetch.yml's OWN COMMIT GUARD, not a
# preference: `find data/ public/ local/ -type f -size +40000k`, which is
# 39.06 MB. data/belgian_macro.db is committed by that workflow, so a load
# that takes it past the guard does not produce a big repository -- it FAILS
# THE DAILY RUN and stops every other source's data from landing that day.
# The guard has already been raised twice and docs/steps records the decision
# to fix the next overrun at the source instead of raising it a third time.
#
# Measured, each from the committed baseline and vacuumed:
#
#   baseline, no monthly rows                                32.32 MB
#   last  18 months   13,736 rows   db 37.88 MB   PASS, 1.18 MB spare  <- chosen
#   last  24 months   17,696 rows   db 39.45 MB   FAILS the guard by 0.38 MB
#   last  36 months   26,036 rows   db 42.60 MB   FAILS
#   last  60 months   39,620 rows   db 48.90 MB   FAILS
#   all  114 months   70,184 rows   db 63.00 MB   FAILS; 46 % of every
#                                                 observation in the store,
#                                                 for one indicator
#
# The ANNUAL series is NOT trimmed -- it carries the full history from 2017 at
# a twelfth of the volume, and costs 2.4 MB of the above. The monthly rows
# exist for the recent shape, above all the March 2026 break the maintainer
# asked to be visible. Eighteen months reaches back to 2025-01, so the break
# is read against fourteen months of the 6.26-6.85 % plateau that ran to
# February 2026, and a full year-on-year comparison is still possible.
#
# WIDENING THIS NEEDS THE FILE-SIZE PROBLEM SOLVED FIRST, not just a bigger
# number here: at 24 months the daily run breaks. The fix is NOT to stop
# committing the database -- daily_fetch.yml appends to it rather than
# rebuilding it, and it is the only store that holds the automated sources and
# their superseded vintages. It is that 62 % of the file is INDEX: 23.2 MB of
# 37.9 MB, measured with dbstat. The five secondary indexes hold no
# information and rebuild from the data in 0.68 s, so not committing them
# gives 21.96 MB and 17.1 MB of headroom. See
# docs/features/onem_unemployment_rate.md.
#
# Counted from the LATEST MONTH IN THE FILE, never from today's date, so the
# same input always produces the same output -- the determinism the
# byte-identical rebuild tests depend on (CLAUDE.md rule 35).
MONTHLY_HISTORY_MONTHS = 18

# Rounding slack when checking `diff1an` against our own 12-month difference.
# Both `graad` values are published to two decimals while ONEM differences the
# unrounded figures, so 0.01 is reachable honestly; 0.02 is the refusal line.
# Measured worst case over the whole 2026-09-12 file: exactly 0.0100.
DIFF_TOLERANCE = 0.02


class OnemRateError(RuntimeError):
    """A refusal: the file is not shaped the way this parser requires."""


def fetch_csv(out_dir: Path = RAW_CACHE_DIR) -> Path:
    """Download the rate file. One plain GET, no session."""
    import requests

    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / CSV_NAME
    resp = requests.get(CSV_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    target.write_bytes(resp.content)
    return target


def read_rows(path: Path) -> list[dict]:
    """Parse the file, refusing anything whose shape this parser did not see."""
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        try:
            header = next(reader)
        except StopIteration as exc:
            raise OnemRateError(f"{path} is empty.") from exc
        header = [h.strip().strip('"') for h in header]
        if header != EXPECTED_HEADER:
            raise OnemRateError(
                f"{path} header is {header}, expected {EXPECTED_HEADER}. ONEM changed the "
                "rate file's shape; refusing to guess which column is now the rate "
                "(CLAUDE.md rule 13). Update EXPECTED_HEADER only after reading the new file."
            )
        rows = []
        for lineno, raw in enumerate(reader, start=2):
            if not any(cell.strip() for cell in raw):
                continue
            if len(raw) != len(EXPECTED_HEADER):
                raise OnemRateError(
                    f"{path}:{lineno} has {len(raw)} fields, expected "
                    f"{len(EXPECTED_HEADER)}: {raw!r}"
                )
            rows.append(dict(zip(EXPECTED_HEADER, (cell.strip() for cell in raw), strict=True)))
    if not rows:
        raise OnemRateError(f"{path} has a valid header but no data rows.")
    return rows


def _parse_cell(row: dict, where: str) -> tuple[int, int, float]:
    try:
        year = int(row["jaar"])
        month = int(row["maand"])
    except ValueError as exc:
        raise OnemRateError(
            f"{where}: jaar/maand not integers ({row['jaar']!r}, {row['maand']!r})."
        ) from exc
    if not 1 <= month <= ANNUAL_MONTH:
        raise OnemRateError(
            f"{where}: maand is {month}. ONEM uses 1-12 for months and "
            f"{ANNUAL_MONTH} for the annual average; a further code would be a new "
            "meaning this parser must not silently drop."
        )
    if row["graad"] == "":
        raise OnemRateError(
            f"{where}: `graad` is empty. Measured on the 2026-09-12 file every one of "
            "70,060 commune values was present, so an empty cell is a change in how ONEM "
            "handles small or missing figures and must be understood before it is loaded -- "
            "a blank must not become a zero (CLAUDE.md rule 26)."
        )
    try:
        value = float(row["graad"])
    except ValueError as exc:
        raise OnemRateError(
            f"{where}: `graad` is {row['graad']!r}, not a number. If ONEM has begun masking "
            "cells, the mask must be loaded as status 'suppressed', never as 0."
        ) from exc
    return year, month, value


def check_year_on_year(rows: list[dict]) -> int:
    """Verify our parse against the file's own redundant `diff1an` column.

    `diff1an` is the change over twelve months. We do not store it (it is a
    derived value, CLAUDE.md rule 6) but it lets the file audit this parser:
    if a column were shifted or a key built wrong, the identity would fail.
    Returns the number of identities actually checked.
    """
    graad: dict[tuple[str, str, int, int], float] = {}
    for row in rows:
        year, month, value = _parse_cell(row, "diff1an pre-pass")
        graad[(row["level"], row["zonegeog"], year, month)] = value

    checked = 0
    for row in rows:
        year, month, value = _parse_cell(row, "diff1an check")
        previous = graad.get((row["level"], row["zonegeog"], year - 1, month))
        if previous is None or row["diff1an"] == "":
            continue
        try:
            published = float(row["diff1an"])
        except ValueError as exc:
            raise OnemRateError(
                f"level {row['level']} zone {row['zonegeog']} {year}-{month}: diff1an is "
                f"{row['diff1an']!r}, not a number."
            ) from exc
        deviation = abs((value - previous) - published)
        if deviation > DIFF_TOLERANCE:
            raise OnemRateError(
                f"level {row['level']} zone {row['zonegeog']} {year}-{month}: the file says "
                f"the 12-month change is {published:+.2f} pp but its own rates give "
                f"{value - previous:+.2f} pp ({value:.2f} against {previous:.2f}), a "
                f"deviation of {deviation:.2f} pp beyond the {DIFF_TOLERANCE} pp rounding "
                "allowance. Either the file is internally inconsistent or this parser is "
                "reading the wrong columns. Refusing to load either way."
            )
        checked += 1
    return checked


def current_municipalities(conn: sqlite3.Connection) -> dict[str, str]:
    """NIS code -> geo_id for the municipalities in force today.

    Current rather than period-accurate on purpose: ONEM restates all 124
    periods on today's 565-commune map, so 31 codes carry 2017 figures for
    entities that did not exist in 2017. Resolving period by period would
    reject those rows; resolving on today's map is what sync_onem.py already
    does for the same publisher's Excel tables.
    """
    return {
        nis: geo_id
        for geo_id, nis in conn.execute(
            "SELECT geo_id, nis_code FROM geographies "
            "WHERE level = 'municipality' AND valid_to IS NULL"
        )
    }


# AGGREGATION IS `not_applicable`, AND THAT IS THE WHOLE POINT. ADR 0003 says
# a ratio's aggregate must be RECOMPUTED from the summed numerator and summed
# denominator, never averaged over communes. ONEM publishes only the quotient,
# so neither term is available here and no province or region figure can be
# built. The remaining option in this schema, `population_weighted`, is
# forbidden outright by CLAUDE.md -- the whole population is not this rate's
# denominator, insured persons are. Refusing an aggregate is the correct
# answer, not a gap.
AGGREGATION_METHOD = "not_applicable"

# One decimal, matching the national UNEMPLOYMENT_RATE row. ONEM publishes two,
# but a second decimal on a commune of a few thousand insured people is
# precision the figure does not carry.
DECIMALS = 1


def check_commune_coverage(rows: list[dict], municipalities: dict[str, str]) -> None:
    """Refuse unless the file's commune set is EXACTLY today's municipalities.

    Both directions matter and for different reasons. An unknown code means
    ONEM has a commune we do not, so we would be dropping a figure. A missing
    commune means its own page shows a blank while every neighbour shows a
    rate, which reads as "no unemployment here" rather than "not published".
    Neither is a warning.
    """
    seen = {r["zonegeog"] for r in rows if r["level"] == LEVEL_MUNICIPAL}
    unknown = sorted(seen - set(municipalities))
    missing = sorted(set(municipalities) - seen)
    if unknown or missing:
        raise OnemRateError(
            f"ONEM's commune set does not match the {len(municipalities)} municipalities in "
            f"force today: {len(unknown)} unknown code(s) {unknown[:5]}, {len(missing)} "
            f"municipality(ies) absent from the file {missing[:5]}. Refusing to load a "
            "partial series -- a silently missing commune leaves a blank on its own page "
            "and understates anything built from the set."
        )


def monthly_cutoff(rows: list[dict], months: int = MONTHLY_HISTORY_MONTHS) -> str:
    """The earliest `YYYY-MM` to load, counted back from the file's own latest
    month rather than from today.

    Anchoring on the data keeps the load reproducible: re-running this tomorrow
    on the same file writes the same rows, which is what the byte-identical
    rebuild checks require. Anchoring on `date.today()` would quietly drop a
    month at every month boundary and make two runs of the same input differ.
    """
    latest = max(
        (year, month)
        for row in rows
        for year, month, _ in [_parse_cell(row, "monthly cutoff")]
        if month != ANNUAL_MONTH
    )
    index = latest[0] * 12 + (latest[1] - 1) - (months - 1)
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


# The `onem` source row, written identically by scripts/sync_onem.py. Both
# scripts use INSERT OR IGNORE, so whichever runs first wins and a divergence
# between them would be silent -- tests/test_sync_onem_rates.py runs both in
# both orders and asserts the row comes out the same either way.
SOURCE_LICENCE = (
    "Free of rights for commercial reuse, quoted in full in docs/data_catalog.md. "
    "Obligations: credit ONEM/RVA as the source and state the date of the "
    "information used."
)


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    """Insert the `onem` source row and the two new indicator rows.

    The source row is written here as well as in sync_onem.py rather than
    assumed: on a fresh database the two scripts can run in either order, and
    an indicator whose source is absent fails the foreign key -- which is the
    schema telling us not to depend on run order.
    """
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence,
             is_active)
        VALUES ('onem', 'ONEM/RVA commune unemployment statistics',
                'ONEM / RVA', 'onem',
                'https://www.onem.be/sites/default/files/assets/statistiques/113', ?,
                'docs/data_catalog.md -- ONEM/RVA row', 'monthly (refreshed in place)', 1)
        """,
        (SOURCE_LICENCE,),
    )
    for indicator_id in INDICATOR_IDS:
        ind = indicator_configs[indicator_id]
        description = ind.get("description", {})
        conn.execute(
            """
            INSERT OR IGNORE INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'onem', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 1)
            """,
            (
                indicator_id,
                ind["name"]["nl"],
                ind["name"]["fr"],
                ind["name"]["en"],
                description.get("nl", ""),
                description.get("fr", ""),
                description.get("en", ""),
                ind["frequency"],
                ind["unit"],
                ind["preferred_direction"],
                AGGREGATION_METHOD,
                DECIMALS,
                f"config/indicators/{indicator_id}.yaml",
            ),
        )
    conn.commit()


def annual_status(rows: list[dict]) -> dict[int, str]:
    """`final` for a year whose twelve months are all in the file, else
    `provisional`.

    Judged from the data rather than the calendar. ONEM's own annual row for
    the running year is a mean over the months published so far -- six of them
    for 2026, which reads 8.08 % for Namur against 6.74 % in June. The same
    quantity over a shorter window is still loadable (sync_onem.py argues this
    for its `Moyenne annuelle` columns), but a reader must not be told a
    six-month mean is settled.
    """
    months: dict[int, set[int]] = defaultdict(set)
    for row in rows:
        if row["level"] != LEVEL_MUNICIPAL:
            continue
        year, month, _ = _parse_cell(row, "annual status")
        if month != ANNUAL_MONTH:
            months[year].add(month)
    return {
        year: ("final" if set(range(1, 13)) <= present else "provisional")
        for year, present in months.items()
    }


def sync(
    db_path: Path,
    csv_path: Path | None = None,
    reference_rows_only: bool = False,
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

    path = csv_path or fetch_csv()
    rows = read_rows(path)
    pairs = check_year_on_year(rows)

    municipalities = current_municipalities(conn)
    check_commune_coverage(rows, municipalities)

    statuses = annual_status(rows)
    cutoff = monthly_cutoff(rows)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("onem", "onem_rates", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    rows_read = rows_written = skipped_old = 0
    for row in rows:
        level = row["level"]
        if level == LEVEL_MUNICIPAL:
            geo_id = municipalities[row["zonegeog"]]
        elif level == LEVEL_COUNTRY and row["zonegeog"] == COUNTRY_ZONE:
            geo_id = COUNTRY_GEO_ID
        else:
            # Levels 2-4, and any future level-1 zone other than 99: skipped by
            # design, see this module's docstring and decision 0005.
            continue

        year, month, value = _parse_cell(
            row, f"level {level} zone {row['zonegeog']} {row['jaar']}-{row['maand']}"
        )
        if month == ANNUAL_MONTH:
            indicator_id = INDICATOR_ANNUAL
            period = f"{year:04d}"
            frequency = "A"
            status = statuses.get(year, "provisional")
        else:
            indicator_id = INDICATOR_MONTHLY
            period = f"{year:04d}-{month:02d}"
            frequency = "M"
            status = "final"
            if period < cutoff:
                skipped_old += 1
                continue
        period_start, period_end = derive_period_bounds(period, frequency)
        rows_read += 1
        rows_written += upsert_observation(
            conn,
            indicator_id=indicator_id,
            geo_id=geo_id,
            period=period,
            period_start=period_start,
            period_end=period_end,
            value=value,
            status=status,
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
    provisional = sorted(y for y, s in statuses.items() if s == "provisional")
    print(
        f"  {CSV_NAME}: {len(rows)} rows read, {pairs} year-on-year identities verified, "
        f"{len(municipalities)} communes + Belgium loaded, "
        f"provisional year(s): {provisional or 'none'}, "
        f"monthly from {cutoff} ({skipped_old} earlier monthly rows not stored; "
        "the annual series keeps the full history)"
    )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(description="Load ONEM/RVA's published commune unemployment rate")
    ap.add_argument("--db", required=True)
    ap.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Read an already-fetched interact_taux_V1.csv instead of fetching (tests)",
    )
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the indicators rows; needs no file and no network",
    )
    args = ap.parse_args()
    read, written = sync(Path(args.db), args.csv, args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for: {', '.join(INDICATOR_IDS)}")
    else:
        print(f"Read {read} values, wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
