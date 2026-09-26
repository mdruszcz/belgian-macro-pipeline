"""Export the age-by-sex pyramid's full available history, one file per
CURRENT commune, so the builder profile page can play the pyramid year by
year instead of showing a single frozen year.

Source is the same annual Statbel ``TF_SOC_POP_STRUCT_<year>`` bulk file
``export_commune_age_sex.py`` already reads (five-year age bands, male/female
counts). This sibling script reads every year found in a directory of those
files, sums each year's counts onto TODAY's 565 communes using the maintainer
-approved "growth on current territory" rule -- counts are additive, so a
predecessor commune's cells are summed onto its successor for exactly the
years the predecessor still had its own rows, with no averaging and no
estimation of a missing cell (CLAUDE.md rules 3, 6, 25-27).

The lineage used is `config/geography/municipality_crosswalk.csv` (old_nis ->
new_nis), walked to the current commune exactly as
`src/analytics/backaggregate.py`'s `resolve_successor` walks successor_geo_id
-- recursively, with a cycle guard, and deliberately ignoring
`has_partial_transfer` for the same reason that module does (it annotates the
pre-existing 1977 structure, not the 2019/2025 merger rows this crosswalk
records; see that module's docstring).

Coverage per year is computed from the crosswalk's own `valid_to` date, not
merely from which communes happened to appear in the source file: a
commune C's "expected contributors" for year Y are C itself (if Y's 1
January is on/after C's own creation) plus every predecessor P whose
valid_to is strictly after 1 January of Y (P still existed then). A
contributor is only "found" if that NIS actually appears with real rows in
year Y's parsed source file. A year where fewer contributors were found than
expected is still published -- CLAUDE.md forbids inventing a fill for a
missing cell, not publishing an incomplete year -- but its `coverage` field
records the shortfall so a reader (and the page) can tell. A year is never
silently skipped and never averaged.

This does NOT touch the single-year `public/data/demography/<nis>.json`
payload `export_commune_age_sex.py` already writes -- that stays
byte-identical, since public pages already read it. This script writes a
NEW, additional payload at `public/data/demography_history/<nis>.json`.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from export_commune_age_sex import (  # noqa: E402
    AgeSexExportError,
    _current_nis_codes,
    read_age_sex,
)

REPO = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIR = REPO / "data" / "raw" / "statbel" / "population"
DEFAULT_GEOGRAPHIES = REPO / "public" / "data" / "metadata" / "geographies.json"
DEFAULT_CROSSWALK = REPO / "config" / "geography" / "municipality_crosswalk.csv"
DEFAULT_OUTPUT = REPO / "public" / "data" / "demography_history"

BAND_STARTS = list(range(0, 101, 5))


class CycleError(AgeSexExportError):
    """A crosswalk new_nis chain loops back on itself."""


@dataclass(frozen=True)
class CrosswalkRow:
    old_nis: str
    new_nis: str
    valid_to: str  # ISO date the predecessor's own rows stop, e.g. "2019-01-01"


def read_crosswalk(path: Path) -> list[CrosswalkRow]:
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = []
        for row in reader:
            old_nis = (row.get("old_nis") or "").strip()
            new_nis = (row.get("new_nis") or "").strip()
            valid_to = (row.get("valid_to") or "").strip()
            if not old_nis or not new_nis or not valid_to:
                raise AgeSexExportError(f"{path.name}: row missing old_nis/new_nis/valid_to: {row}")
            rows.append(CrosswalkRow(old_nis, new_nis, valid_to))
        return rows


def resolve_successor(nis: str, by_old: dict[str, CrosswalkRow]) -> str | None:
    """Walk old_nis -> new_nis to the code that is current today, or None if
    `nis` was never superseded. Mirrors
    `src/analytics/backaggregate.py::resolve_successor`, reimplemented here so
    this script stays a plain NIS-keyed CSV/JSON reader with no database or
    `src` import."""
    seen = {nis}
    current = nis
    while True:
        row = by_old.get(current)
        if row is None:
            return None if current == nis else current
        if row.new_nis in seen:
            raise CycleError(f"crosswalk chain starting at {nis!r} cycles back to {row.new_nis!r}")
        seen.add(row.new_nis)
        current = row.new_nis


def lineage_from_crosswalk(
    rows: list[CrosswalkRow], current_codes: set[str]
) -> dict[str, list[tuple[str, str]]]:
    """current NIS -> [(predecessor_nis, valid_to), ...], grouped by the FINAL
    successor after walking every hop (a two-hop lineage lists the original
    predecessor under the ultimate successor, not the intermediate one) --
    same grouping rule as `backaggregate.lineage_from_geographies`. Every
    successor a row resolves to must be one of today's 565 communes, or the
    crosswalk and the geography snapshot disagree and this refuses rather
    than silently dropping the row (CLAUDE.md rule 13)."""
    by_old = {row.old_nis: row for row in rows}
    out: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        final = resolve_successor(row.old_nis, by_old)
        if final is None:
            continue
        if final not in current_codes:
            raise AgeSexExportError(
                f"crosswalk row {row.old_nis} resolves to {final!r}, which is not a "
                f"current commune in geographies.json -- crosswalk/geography disagreement"
            )
        out.setdefault(final, []).append((row.old_nis, row.valid_to))
    return out


def _year_start(period: str) -> str:
    return f"{period}-01-01"


def _successor_expected(
    nis: str,
    period: str,
    predecessors: list[tuple[str, str]],
    ever_seen_by: dict[str, str],
) -> bool:
    """True if the successor's OWN code should be expected to carry rows for
    `period`, without guessing at a creation date the crosswalk does not
    record.

    Two ways a successor is legitimately expected:
    1. Its merger has already happened by this year (any predecessor's
       valid_to <= this year's 1 January) -- the code now covers the whole
       current territory.
    2. It has been SEEN in the source data in some year <= this one --
       covers a pre-existing entity like Antwerp (11002), which reports on
       its own long before it ever absorbs anything, so it must not be
       marked "not yet expected" merely because no merger has fired yet.

    `ever_seen_by[nis]` is the earliest period `nis` appears in any source
    file at all (precomputed once over the whole `by_year` set) -- reading
    the data rather than assuming a code's history, per CLAUDE.md rule 13
    (refuse to guess; here, guess nothing and consult the file instead)."""
    if any(valid_to <= _year_start(period) for _, valid_to in predecessors):
        return True
    first_seen = ever_seen_by.get(nis)
    return first_seen is not None and first_seen <= period


def build_history(
    source_dir: Path,
    geographies_path: Path,
    crosswalk_path: Path,
) -> dict[str, dict]:
    current_codes = _current_nis_codes(geographies_path)
    crosswalk_rows = read_crosswalk(crosswalk_path)
    lineage = lineage_from_crosswalk(crosswalk_rows, current_codes)

    source_files = sorted(
        p for p in source_dir.iterdir() if p.suffix.lower() in {".zip", ".txt", ".csv"}
    )
    if not source_files:
        raise AgeSexExportError(f"no source files found under {source_dir}")

    by_year: dict[str, dict[str, dict[int, dict[str, int]]]] = {}
    for path in source_files:
        year, values = read_age_sex(path)
        period = str(year)
        if period in by_year:
            raise AgeSexExportError(f"duplicate year {period} in {source_dir}: {path.name}")
        by_year[period] = values

    periods = sorted(by_year)

    # Earliest period each source NIS code appears in, over the whole
    # dataset -- read from the files, never assumed, so `_successor_expected`
    # can tell a pre-existing entity (Antwerp, reporting long before it ever
    # absorbs anything) from a genuinely brand-new code with no rows yet.
    ever_seen_by: dict[str, str] = {}
    for period in periods:
        for code in by_year[period]:
            if code not in ever_seen_by:
                ever_seen_by[code] = period

    # nis -> period -> {band_start: {male, female}}, plus per-nis per-period
    # "did this code actually have rows in this year's file" (found), and
    # "should it have" (expected), both computed per current commune.
    history: dict[str, dict] = {}
    for nis in sorted(current_codes):
        predecessors = lineage.get(nis, [])
        contributors = [nis] + [p for p, _ in predecessors]
        years_out = []
        for period in periods:
            values = by_year[period]
            found_codes = [c for c in contributors if c in values]

            expected_codes = []
            for code in contributors:
                if code == nis:
                    if _successor_expected(nis, period, predecessors, ever_seen_by):
                        expected_codes.append(code)
                else:
                    valid_to = next(vt for p, vt in predecessors if p == code)
                    if valid_to > _year_start(period):
                        expected_codes.append(code)

            if not found_codes:
                # Neither the commune nor any predecessor has rows this year
                # (e.g. a future merger not yet in effect for an unrelated
                # commune's file set is irrelevant here) -- nothing to
                # publish for this period, not even a coverage-0 row.
                continue

            bands: dict[int, dict[str, int]] = {
                start: {"male": 0, "female": 0} for start in BAND_STARTS
            }
            for code in found_codes:
                for start, cell in values[code].items():
                    bands[start]["male"] += cell["male"]
                    bands[start]["female"] += cell["female"]

            expected_n = len(expected_codes) or len(found_codes)
            years_out.append(
                {
                    "period": period,
                    "reference_date": f"{period}-01-01",
                    "male": [bands[s]["male"] for s in BAND_STARTS],
                    "female": [bands[s]["female"] for s in BAND_STARTS],
                    "coverage": {"found": len(found_codes), "expected": expected_n},
                }
            )
        history[nis] = years_out
    return history


def export(
    source_dir: Path,
    output_dir: Path,
    geographies_path: Path,
    crosswalk_path: Path,
) -> tuple[int, int]:
    history = build_history(source_dir, geographies_path, crosswalk_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    band_defs = [
        {"from": start, "to": None if start == 100 else start + 4} for start in BAND_STARTS
    ]

    communes = 0
    years_written = 0
    for nis in sorted(history):
        years = history[nis]
        if not years:
            continue
        payload = {
            "nis_code": nis,
            "source_id": "statbel",
            "age_top_code": 100,
            "bands": band_defs,
            "years": years,
        }
        (output_dir / f"{nis}.json").write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        communes += 1
        years_written += len(years)
    return communes, years_written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--geographies", type=Path, default=DEFAULT_GEOGRAPHIES)
    parser.add_argument("--crosswalk", type=Path, default=DEFAULT_CROSSWALK)
    args = parser.parse_args()
    communes, years_written = export(
        args.source_dir, args.output_dir, args.geographies, args.crosswalk
    )
    print(f"Exported {communes} commune histories ({years_written} commune-years total)")


if __name__ == "__main__":
    main()
