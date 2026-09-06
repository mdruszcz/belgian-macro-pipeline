"""Load ONEM/RVA's commune-level unemployment tables -- docs/data_catalog.md's
ONEM/RVA row, the pipeline's FIRST AUTOMATED municipal source.

Every other municipal source here is a hand download, because
statbel.fgov.be cannot be reached from GitHub's runners. ONEM can: run
34056982618 fetched all six files from a real runner, 0.5-1.4 MB each,
Excel BIFF saved 2026-08-05 by a named ONEM employee. So these
observations go into the daily database rather than a committed CSV
(ADR 0002 splits the stores precisely on whether automation can refresh
them), and this script runs in daily_fetch.yml.

WHAT THE FILES ACTUALLY ARE, read from run 34056982618's artifact rather
than inferred from the filenames:

  * One sheet per YEAR, 2017 through 2026, in every file.
  * Row 0 the agency, row 1 the dataset, ROW 3 THE UNIT, row 5 the column
    headers, rows 6+ the data.
  * 618 data rows: 565 communes, 42 `arr.*` subtotals, 10 `prov.*`
    subtotals and one `Région Bruxelles-Capitale` row. Only the commune
    rows are loaded; the subtotals are this pipeline's own job
    (src/analytics/aggregate.py) and storing them too would create a
    second source of truth for one fact.
  * Communes are named in FRENCH TEXT with no NIS code, so they are
    matched by name like the Bestat business-units feed, not resolved by
    code like the census files.

THE FILENAME SUFFIXES ARE NOW DECODED, AND NOT BY GUESSING. An earlier
note in this repo recorded UP/M as "likely two report granularities".
Row 3 of the actual files says otherwise: `M` files are `Montants -
Total` (euros paid) and `UP` files are `Unités physiques - Moyenne
annuelle` (people, averaged over the year's months). Guessing at an
abbreviation is what produced the Census 2021 `CAS` bug, so every
dataset below declares the unit string it expects and this script REFUSES
to load a file whose row 3 disagrees -- if ONEM ever swaps the meaning of
a suffix, that is a crash, not a silent unit change from people to euros.

ONLY TOTAL COLUMNS ARE LOADED, and the reason is measured. ONEM masks
small counts as the literal string `<10` for privacy. In the `UP`
files that masking is pervasive in the fine breakdowns -- 513 of 618 rows
for CCI-NDE voluntary part-timers, 519 for unpaid teaching periods -- but
almost absent from the total columns (1 row for CCI-DE, 3 for the CT
total). A column that is masked for four communes in five carries no
usable commune-level signal, so it is not stored. The `M` files are not
masked at all: euros are not disclosive.

A MASKED CELL IS NOT A ZERO. Where a total column is masked, the
observation is written with `status = 'suppressed'` and a NULL value --
the one thing migrations/001_core_schema.sql's CHECK constraint allows a
NULL value for. Writing 0 instead would understate every aggregate built
on it, and skipping the row entirely would make the commune
indistinguishable from one ONEM does not cover. This is the first source
in the pipeline to use that status, which is why the values-vs-coverage
distinction matters here more than elsewhere: TTP's total is masked for
132 of 618 rows, one commune in five.

THE CURRENT YEAR IS PROVISIONAL. Row 3 says "Moyenne annuelle" on every
sheet including the current one, but a mean over the months published so
far is not a mean over twelve. The 2026 sheet in the file fetched
2026-09-06 was last saved 2026-08-05. So any sheet whose year is the
current calendar year is written `status = 'provisional'`, and the
completed years `final`.

ONEM BACKCASTS TODAY'S COMMUNE MAP. All ten sheets carry the identical
566 labels, so 2017 is expressed on the 2025 geography rather than the
589 communes that existed then. That makes it a pinned-vintage source in
exactly the sense scripts/export_aggregates_csv.py already detects for
fiscal income, and it is why names resolve against CURRENT municipalities
rather than period by period.
"""

import argparse
import sqlite3
import sys
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fetch_onem_raw import RAW_CACHE_DIR, fetch_all  # noqa: E402
from port_existing_indicators import derive_period_bounds  # noqa: E402

from src.db.vintages import upsert_observation  # noqa: E402
from src.validation.config_schema import load_and_validate_all  # noqa: E402

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

UNIT_PERSONS = "Unités physiques - Moyenne annuelle"
UNIT_EUROS = "Montants - Total"

MASKED = "<10"

# Structural rows in the first column, skipped rather than loaded: this
# pipeline computes its own arrondissement/province/region figures from the
# commune rows (src/analytics/aggregate.py, ADR 0003).
SUBTOTAL_PREFIXES = ("arr.", "prov.")
REGION_LABELS = {"Région Bruxelles-Capitale"}

# Which column of which file becomes which indicator. Keyed on the column's
# header TEXT rather than its position, so an inserted column shifts nothing;
# `unit` is the row-3 string the file must actually declare.
DATASETS = [
    {
        "file": "CCI_Commune_Statut_UP_FR.xls",
        "unit": UNIT_PERSONS,
        "columns": {
            "CCI-DE": "UNEMPLOYED_JOBSEEKERS",
            "Total": "UNEMPLOYMENT_BENEFIT_RECIPIENTS",
        },
    },
    {
        "file": "CCI_Commune_Statut_M_FR.xls",
        "unit": UNIT_EUROS,
        "columns": {"Total": "UNEMPLOYMENT_BENEFIT_PAID"},
    },
    {
        "file": "CT_Commune_Statut_UP_FR.xls",
        "unit": UNIT_PERSONS,
        "columns": {"Total": "TEMP_UNEMPLOYED"},
    },
    {
        "file": "CT_Commune_Statut_M_FR.xls",
        "unit": UNIT_EUROS,
        "columns": {"Total": "TEMP_UNEMPLOYMENT_BENEFIT_PAID"},
    },
    {
        "file": "TTP_Commune_Statut_UP_FR.xls",
        "unit": UNIT_PERSONS,
        "columns": {"Total": "PART_TIME_BENEFIT_RECIPIENTS"},
    },
    {
        "file": "EMPL_Commune_Statut_M_FR.xls",
        "unit": UNIT_EUROS,
        "columns": {"Total": "ACTIVATION_MEASURES_PAID"},
    },
]

INDICATOR_IDS = sorted({ind for d in DATASETS for ind in d["columns"].values()})


def normalize_name(name: str) -> str:
    """Fold a commune name to a form both ONEM and Statbel agree on.

    Accents are stripped because ONEM writes `Chatelet` and `Vise` where
    Statbel writes `Châtelet` and `Visé`. The APOSTROPHE matters as much:
    Statbel's geographies.csv uses U+2019 (`Braine-l’Alleud`) and ONEM uses
    the ASCII quote, which left exactly three communes unmatched until it was
    handled -- Braine-l'Alleud, Fontaine-l'Evêque and Mont-de-l'Enclus.
    """
    # Separators are replaced BEFORE the ASCII fold, not after: U+2019 is not
    # ASCII, so folding first DELETES it outright and turns `Braine-l’Alleud`
    # into `braine lalleud`, which no longer matches ONEM's `braine l alleud`.
    # That ordering left exactly three communes unmatched on the first run.
    separated = str(name).replace("’", " ").replace("'", " ").replace("-", " ")
    folded = unicodedata.normalize("NFKD", separated).encode("ascii", "ignore").decode()
    return " ".join(folded.lower().split())


ARRONDISSEMENT_PREFIXES = ("arrondissement de ", "arrondissement d ", "arrondissement ")


def _strip_arrondissement(name: str) -> str:
    """`Arrondissement de Saint-Nicolas` -> `saint nicolas`, so ONEM's own
    `arr.Sint-Niklaas` can be compared against it."""
    folded = normalize_name(name)
    for prefix in ARRONDISSEMENT_PREFIXES:
        if folded.startswith(prefix):
            return folded[len(prefix) :]
    return folded


class CommuneIndex:
    """Resolves ONEM's French commune labels to geo_ids, over CURRENT
    municipalities only.

    Current-only, deliberately: ONEM expresses all ten years on today's
    commune map (see module docstring), so a historical predecessor name
    appearing here would mean the file changed shape, not that a 2017 row
    needs a 2017 geography.

    AMBIGUITY IS DETECTED, NOT RESOLVED BY WHICHEVER ROW CAME FIRST. Two
    current communes carry the French name Saint-Nicolas -- 46021 in East
    Flanders and 62093 in Liège -- and ONEM lists both, using the Dutch
    `Sint-Niklaas` for the Flemish one. A dict built with `setdefault` maps
    BOTH labels onto 46021 and never reaches 62093; that is not a subtle
    failure, it raised a vintage collision on the first real run, but had the
    two been in different files it would have silently doubled one commune's
    figure and dropped another's. So a name with more than one candidate is
    resolved by the ARRONDISSEMENT the file itself states -- the same compound
    (name, arrondissement) match src/fetchers/statbel.py needs for the same
    pair, for the same reason.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._by_name: dict[str, set[str]] = {}
        self._by_name_and_arr: dict[tuple[str, str], str] = {}
        rows = conn.execute("""
            SELECT m.geo_id, m.name_fr, m.name_nl, m.name_en,
                   a.name_fr, a.name_nl, a.name_en
              FROM geographies m
              LEFT JOIN geographies a ON a.geo_id = m.parent_geo_id
             WHERE m.level = 'municipality' AND (m.valid_to IS NULL OR m.valid_to = '')
            """).fetchall()
        for geo_id, *names in rows:
            commune_names, arr_names = names[:3], names[3:]
            for name in commune_names:
                if not name:
                    continue
                key = normalize_name(name)
                self._by_name.setdefault(key, set()).add(geo_id)
                for arr in arr_names:
                    if arr:
                        self._by_name_and_arr[(key, _strip_arrondissement(arr))] = geo_id

    def resolve(self, label: str, arrondissement: str | None) -> str | None:
        """geo_id, or None if the label matches no current commune."""
        key = normalize_name(label)
        candidates = self._by_name.get(key)
        if not candidates:
            return None
        if len(candidates) == 1:
            return next(iter(candidates))
        if arrondissement is None:
            raise ValueError(
                f"ONEM label {label!r} matches {len(candidates)} current communes "
                f"({sorted(candidates)}) and the sheet gave no arrondissement to "
                "disambiguate it. Refusing to pick one."
            )
        resolved = self._by_name_and_arr.get((key, normalize_name(arrondissement)))
        if resolved is None:
            raise ValueError(
                f"ONEM label {label!r} is ambiguous between {sorted(candidates)} and its "
                f"stated arrondissement {arrondissement!r} matches neither. Refusing to "
                "pick one (CLAUDE.md rule 13)."
            )
        return resolved


def read_sheet(sheet, columns: dict[str, str], expected_unit: str, filename: str) -> dict:
    """Parse one year-sheet. Returns indicator_id -> {commune label: value},
    where a value of None means ONEM masked it as `<10`."""
    declared_unit = str(sheet.cell_value(3, 0)).strip()
    if declared_unit != expected_unit:
        raise ValueError(
            f"{filename}!{sheet.name} declares unit {declared_unit!r}, expected "
            f"{expected_unit!r}. Refusing to load (CLAUDE.md rule 13): the UP/M "
            "filename suffixes distinguish people from euros, so a changed unit "
            "row would silently store euros in a person-count indicator."
        )

    header = [str(sheet.cell_value(5, c)).strip() for c in range(sheet.ncols)]
    missing = [label for label in columns if label not in header]
    if missing:
        raise ValueError(
            f"{filename}!{sheet.name} is missing expected column(s) {missing}. Its "
            f"columns are {header}. Refusing to guess a replacement (CLAUDE.md "
            "rule 13) -- if ONEM restructured this table, DATASETS must be "
            "re-derived from the new layout."
        )
    col_idx = {label: header.index(label) for label in columns}

    labels = [str(sheet.cell_value(r, 0)).strip() for r in range(6, sheet.nrows)]

    # Which arrondissement each commune row belongs to, read from the file's
    # own `arr.*` subtotal row -- which TERMINATES its block rather than
    # heading it, so the context is the next such row BELOW, not above. Only
    # needed to disambiguate the two Saint-Nicolas communes (see CommuneIndex),
    # but derived for every row rather than special-casing that one pair.
    arr_of_row: list[str | None] = [None] * len(labels)
    pending: list[int] = []
    for i, label in enumerate(labels):
        if label.startswith("arr."):
            for j in pending:
                arr_of_row[j] = label[len("arr.") :].strip()
            pending = []
        elif label and not label.startswith(SUBTOTAL_PREFIXES) and label not in REGION_LABELS:
            pending.append(i)

    out: dict[str, dict[str, tuple[str | None, float | None]]] = {
        ind: {} for ind in columns.values()
    }
    for i, label in enumerate(labels):
        if not label or label.startswith(SUBTOTAL_PREFIXES) or label in REGION_LABELS:
            continue
        r = i + 6
        for column, indicator_id in columns.items():
            raw = sheet.cell_value(r, col_idx[column])
            if isinstance(raw, str) and raw.strip() == MASKED:
                out[indicator_id][label] = (arr_of_row[i], None)
            elif isinstance(raw, (int, float)):
                out[indicator_id][label] = (arr_of_row[i], float(raw))
            # Anything else -- a blank, a footnote marker -- is left absent
            # rather than coerced: absent and suppressed are different facts.
    return out


def _status_for(period: str, today: date) -> str:
    """`provisional` for a year still being filled, `final` for a closed one.

    Row 3 of every sheet reads "Moyenne annuelle" -- including the current
    year's, where a mean over the months published so far is not a mean over
    twelve. The file fetched 2026-09-06 was last saved 2026-08-05 and its 2026
    sheet still says the same thing, so the year has to be judged by the
    calendar rather than taken from the label.
    """
    return "provisional" if int(period) >= today.year else "final"


def _should_load(period: str, unit: str, today: date) -> bool:
    """Whether a sheet's year should be loaded at all.

    A PART-YEAR TOTAL IS NOT AN ANNUAL TOTAL -- it is a different quantity
    wearing the same label -- so the current year is SKIPPED for the euro
    files. Measured on the files fetched 2026-09-06: Antwerp's full
    unemployment benefit reads EUR 261.0m for 2025 and EUR 44.3m for 2026,
    about two months' worth. Published as the latest figure that is an 83%
    collapse, and no status letter fixes a number a reader has already
    misread. `Montants - Total` means exactly what it says.

    A part-year AVERAGE is a different matter and IS loaded: `Unités
    physiques - Moyenne annuelle` over the months so far is the same quantity
    measured over a shorter window, and Antwerp's claimant count moves from
    17,328 to 18,207 across that boundary rather than falling off a cliff. It
    is still marked provisional, because an average over two months is not an
    average over twelve.
    """
    if unit == UNIT_EUROS and int(period) >= today.year:
        return False
    return True


def _ensure_reference_rows(conn: sqlite3.Connection, indicator_configs: dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO sources
            (source_id, name, agency, adapter, base_url, licence, catalog_ref, cadence, is_active)
        VALUES ('onem', 'ONEM/RVA commune unemployment statistics',
                'ONEM / RVA', 'onem',
                'https://www.onem.be/sites/default/files/assets/statistiques/113', ?,
                'docs/data_catalog.md -- ONEM/RVA row', 'monthly (refreshed in place)', 1)
        """,
        (
            "Free of rights for commercial reuse, quoted in full in docs/data_catalog.md. "
            "Obligations: credit ONEM/RVA as the source and state the date of the "
            "information used.",
        ),
    )
    for indicator_id in INDICATOR_IDS:
        ind = indicator_configs[indicator_id]
        conn.execute(
            """
            INSERT OR IGNORE INTO indicators
                (indicator_id, source_id, name_nl, name_fr, name_en,
                 description_nl, description_fr, description_en,
                 frequency, unit, preferred_direction, aggregation_method,
                 is_additive, decimals, config_path, is_active)
            VALUES (?, 'onem', ?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'sum', 1, ?, ?, 1)
            """,
            (
                indicator_id,
                ind["name"]["nl"],
                ind["name"]["fr"],
                ind["name"]["en"],
                ind.get("description", {}).get("en", ""),
                ind["frequency"],
                ind["unit"],
                ind["preferred_direction"],
                ind.get("decimals", 0),
                f"config/indicators/{indicator_id}.yaml",
            ),
        )
    conn.commit()


def sync(
    db_path: Path,
    onem_dir: Path | None = None,
    reference_rows_only: bool = False,
    today: date | None = None,
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

    import xlrd

    today = today or date.today()
    if onem_dir is None:
        fetch_all()
        onem_dir = RAW_CACHE_DIR / today.isoformat()

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO fetch_runs (source_id, adapter, started_at, status) VALUES (?, ?, ?, ?)",
        ("onem", "onem", now, "ok"),
    )
    fetch_run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    index = CommuneIndex(conn)
    rows_read = rows_written = 0
    suppressed = 0
    unmatched: set[str] = set()
    skipped_partial: list[tuple[str, str]] = []

    for dataset in DATASETS:
        path = onem_dir / dataset["file"]
        if not path.is_file():
            raise FileNotFoundError(
                f"{path} not found. ONEM files are fetched by scripts/fetch_onem_raw.py "
                "-- see docs/data_catalog.md's ONEM/RVA row."
            )
        workbook = xlrd.open_workbook(str(path))
        for sheet_name in workbook.sheet_names():
            if not sheet_name.isdigit():
                continue
            period = sheet_name
            if not _should_load(period, dataset["unit"], today):
                skipped_partial.append((dataset["file"], period))
                continue
            status = _status_for(period, today)
            period_start, period_end = derive_period_bounds(period, "A")
            parsed = read_sheet(
                workbook.sheet_by_name(sheet_name),
                dataset["columns"],
                dataset["unit"],
                dataset["file"],
            )
            for indicator_id, by_label in parsed.items():
                for label, (arrondissement, value) in sorted(by_label.items()):
                    geo_id = index.resolve(label, arrondissement)
                    if geo_id is None:
                        unmatched.add(label)
                        continue
                    rows_read += 1
                    if value is None:
                        suppressed += 1
                    rows_written += upsert_observation(
                        conn,
                        indicator_id=indicator_id,
                        geo_id=geo_id,
                        period=period,
                        period_start=period_start,
                        period_end=period_end,
                        value=value,
                        status="suppressed" if value is None else status,
                        vintage=now,
                        fetch_run_id=fetch_run_id,
                    )
        print(f"  {dataset['file']:34} {', '.join(sorted(dataset['columns'].values()))}")

    conn.execute(
        "UPDATE fetch_runs SET finished_at = ?, rows_read = ?, rows_written = ? "
        "WHERE fetch_run_id = ?",
        (datetime.now(timezone.utc).isoformat(), rows_read, rows_written, fetch_run_id),
    )
    conn.commit()
    conn.close()

    if unmatched:
        raise SystemExit(
            f"::error::{len(unmatched)} ONEM commune name(s) did not match any current "
            f"municipality, e.g. {sorted(unmatched)[:5]}. Refusing to load a partial "
            "series: a commune silently missing would understate every aggregate built "
            "on it. Add the spelling to the geography name corrections rather than "
            "loosening the match."
        )
    print(f"  {suppressed} cell(s) masked by ONEM as '<10', stored as status='suppressed'.")
    if skipped_partial:
        years = sorted({period for _f, period in skipped_partial})
        print(
            f"  Skipped {len(skipped_partial)} euro-file sheet(s) for {', '.join(years)}: "
            "a part-year TOTAL is not an annual total (see _should_load)."
        )
    return rows_read, rows_written


def main() -> None:
    ap = argparse.ArgumentParser(description="Load ONEM/RVA commune unemployment tables")
    ap.add_argument("--db", required=True)
    ap.add_argument(
        "--onem-dir",
        type=Path,
        default=None,
        help="Read already-fetched files from here instead of fetching (used by tests)",
    )
    ap.add_argument(
        "--reference-rows-only",
        action="store_true",
        help="Insert only the sources/indicators rows; needs no workbook and no network",
    )
    args = ap.parse_args()
    read, written = sync(Path(args.db), args.onem_dir, args.reference_rows_only)
    if args.reference_rows_only:
        print(f"Reference rows ensured for: {', '.join(INDICATOR_IDS)}")
    else:
        print(f"Read {read} commune values, wrote {written} new vintage(s).")


if __name__ == "__main__":
    main()
