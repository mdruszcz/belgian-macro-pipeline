"""
CONTROL C verification aid: do merged communes stitch across the merger?

    "Plot population 2010-2025 for each and look for the discontinuity.
     A visible step change in a population series is the signature of a
     crosswalk failure, and it is instantly obvious on a chart."
        -- docs/steps, CONTROL C

THROWAWAY. This is a one-off check, not pipeline code. It is deliberately not
wired into .github/workflows/daily_fetch.yml, it writes nothing to the
database, and it may be deleted once CONTROL C is signed off. It reads the
committed crosswalk plus raw population files and prints a verdict.

Why it exists at all: the control asks a human to eyeball a chart, but "looks
continuous to me" is not a record anyone can audit six months later. So the
verdict here is a NUMBER -- the step change at the merger boundary, as a
percentage of the commune's population -- and the chart is optional. A picture
nobody can re-derive is not evidence.

WHAT IT NEEDS (not committed -- data/raw is gitignored):
    data/raw/statbel/population/*.zip|*.csv|*.txt, one file per year -- direct
    download URL, maintainer-supplied 2026-09-06 (year is the only variable):

      https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_<YEAR>.zip

    Each zip is reported at ~99 MB (a per-sector breakdown by nationality,
    civil status, age and sex -- this script sums it down to one total per
    commune per year). Only TWO merger-boundary years exist across the six
    groups this control checks (2019 and 2025 -- see select_groups/ALWAYS_
    INCLUDE below), so only SIX files are actually needed, not the full
    2010-2025 span: 2018, 2019, 2020, 2024, 2025, 2026.

    This must be downloaded by hand. statbel.fgov.be is unreachable from this
    pipeline's own network context -- confirmed three separate ways on
    2026-09-06 (curl IPv4, curl IPv6, and an independent WebFetch path all
    fail on this exact URL; DNS resolves fine, so it is a connection-level
    block, not a DNS or 403 issue). bestat.statbel.fgov.be, a different host,
    is reachable, but its Census 2011 view is the only commune-level
    population data it carries -- a single year, not a series. See
    docs/features/statbel_adapter.md, Non-goals.

HOW TO READ THE OUTPUT:
    For each merger group, two series are compared over the merger boundary:
      raw       -- the successor commune alone. SHOULD show a step change;
                   before the merger it did not exist or covered less area.
      stitched  -- predecessors summed before the merger, successor after,
                   per config/geography/municipality_crosswalk.csv.
                   MUST be continuous. A step here is a crosswalk bug.

    A step change is judged against year-on-year movement in the same series
    away from the boundary, so a genuinely fast-growing commune is not flagged
    for growing. Belgian communes move by well under 5%/year; the threshold is
    set at 5x the series' own median absolute YoY change, floored at 2%.
"""

import argparse
import csv
import io
import re
import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CROSSWALK = REPO_ROOT / "config" / "geography" / "municipality_crosswalk.csv"
DEFAULT_POP_DIR = REPO_ROOT / "data" / "raw" / "statbel" / "population"
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "exports"

# A step is only meaningful relative to how much this commune normally moves.
STEP_THRESHOLD_MULTIPLE = 5.0
STEP_THRESHOLD_FLOOR_PCT = 2.0

# Tongeren-Borgloon (73111) is not optional. It is the one merger the NIS6
# prefix rule missed entirely -- its sub-municipal codes were renumbered under
# the new commune instead of preserving the old NIS5 as a prefix, so only the
# vintage diff caught it (see docs/features/geography.md and the crosswalk's
# own `note` column, which flags it as name-matched and unverified). It is
# therefore the group most likely to be silently wrong, and the whole point of
# this control.
ALWAYS_INCLUDE = {"73111"}

# Column-name candidates, lowercased. Statbel varies these between vintages
# and between the FR/NL editions of the same file, so match rather than assume.
NIS_COLUMNS = ("cd_refnis", "cd_munty_refnis", "nis", "nis_code", "refnis")
POP_COLUMNS = ("ms_population", "population", "ms_pop", "aantal", "nombre", "ms_num_pop")


class MissingPopulationData(Exception):
    """The raw files this script needs are absent. Raised loudly, with the
    download instructions -- never silently skipped (CLAUDE.md rule 13)."""


def _year_from_filename(path: Path) -> int | None:
    """Statbel names these files inconsistently across vintages, so take the
    year from any 19xx/20xx in the name rather than assuming one pattern."""
    years = re.findall(r"(19|20)\d{2}", path.stem)
    if not years:
        return None
    return int(re.findall(r"((?:19|20)\d{2})", path.stem)[-1])


def _pick_column(fieldnames: list[str], candidates: tuple[str, ...], path: Path, kind: str) -> str:
    lowered = {name.lower().strip(): name for name in fieldnames}
    for candidate in candidates:
        if candidate in lowered:
            return lowered[candidate]
    raise MissingPopulationData(
        f"{path.name}: could not find a {kind} column.\n"
        f"  Looked for any of: {', '.join(candidates)}\n"
        f"  File has: {', '.join(fieldnames)}\n"
        f"  Add the real column name to the {kind.upper()}_COLUMNS tuple in this script "
        f"rather than renaming the source file, so the next download works too."
    )


def _open_text_member(path: Path):
    """Return (display_name, text_stream) for a .csv/.txt file, or for the
    single data member inside a .zip -- Statbel ships TF_SOC_POP_STRUCT as one
    ~99 MB .txt per year, zipped. Refuses to guess if a zip holds more than
    one plausible member rather than silently picking one (CLAUDE.md rule 13)."""
    if path.suffix.lower() != ".zip":
        return path.name, path.open(encoding="utf-8-sig", newline="")

    zf = zipfile.ZipFile(path)
    candidates = [n for n in zf.namelist() if n.lower().endswith((".txt", ".csv"))]
    if len(candidates) != 1:
        raise MissingPopulationData(
            f"{path.name}: expected exactly one .txt/.csv inside the zip, found "
            f"{len(candidates)}: {candidates or zf.namelist()}"
        )
    raw = zf.read(candidates[0])
    return f"{path.name}:{candidates[0]}", io.TextIOWrapper(
        io.BytesIO(raw), encoding="utf-8-sig", newline=""
    )


def _sum_communes(fh, display_name: str) -> dict[str, int]:
    """Sniff the delimiter (Statbel varies pipe/semicolon/comma across
    vintages, always with a BOM -- the same trap the REFNIS parsers hit in
    Block C) and sum population to one total per commune."""
    sample = fh.read(8192)
    fh.seek(0)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters="|;,\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(fh, dialect=dialect)
    if not reader.fieldnames:
        raise MissingPopulationData(f"{display_name} is empty or has no header row.")
    nis_col = _pick_column(list(reader.fieldnames), NIS_COLUMNS, Path(display_name), "nis")
    pop_col = _pick_column(list(reader.fieldnames), POP_COLUMNS, Path(display_name), "pop")

    totals: dict[str, int] = {}
    for row in reader:
        nis = (row.get(nis_col) or "").strip()
        raw = (row.get(pop_col) or "").strip().replace(" ", "").replace(",", "")
        if not nis or not raw:
            continue
        try:
            totals[nis] = totals.get(nis, 0) + int(float(raw))
        except ValueError:
            continue

    if not totals:
        raise MissingPopulationData(
            f"{display_name}: parsed 0 communes. Columns picked were "
            f"{nis_col!r}/{pop_col!r} -- one of them is probably not what it looks like."
        )
    return totals


def read_population_by_year(pop_dir: Path) -> dict[int, dict[str, int]]:
    """-> {year: {nis5: total_population}}.

    Statbel publishes these broken down by nationality, sex, age and marital
    status, so a commune appears on many rows; they are summed to one total.
    """
    if not pop_dir.is_dir():
        raise MissingPopulationData(
            f"No population data at {pop_dir}.\n\n"
            f"{__doc__.split('WHAT IT NEEDS')[1].split('HOW TO READ')[0].strip()}"
        )

    files = sorted(p for p in pop_dir.iterdir() if p.suffix.lower() in {".csv", ".txt", ".zip"})
    if not files:
        raise MissingPopulationData(
            f"{pop_dir} exists but holds no .csv/.txt/.zip files.\n"
            f"Found instead: {[p.name for p in pop_dir.iterdir()] or 'nothing'}\n"
            f"If the download was .xlsx, export each sheet to CSV first -- this script "
            f"deliberately does not parse Excel, to keep a throwaway check dependency-free."
        )

    by_year: dict[int, dict[str, int]] = {}
    for path in files:
        year = _year_from_filename(path)
        if year is None:
            print(f"  skipping {path.name}: no year in the filename", file=sys.stderr)
            continue

        display_name, fh = _open_text_member(path)
        try:
            by_year[year] = _sum_communes(fh, display_name)
        finally:
            fh.close()
        print(f"  {path.name}: year {year}, {len(by_year[year])} communes")

    return by_year


def read_merger_groups(crosswalk: Path) -> dict[str, dict]:
    """-> {new_nis: {"predecessors": [...], "merger_year": int, "names": [...]}}

    Only 'merged' rows with a real successor and no partial transfer: a partial
    boundary transfer cannot be stitched by summing whole communes, so including
    one would manufacture a discontinuity this control would then blame on the
    crosswalk.
    """
    groups: dict[str, dict] = {}
    with crosswalk.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            new_nis = (row.get("new_nis") or "").strip()
            old_nis = (row.get("old_nis") or "").strip()
            if not new_nis or not old_nis:
                continue
            if (row.get("has_partial_transfer") or "").strip().lower() == "true":
                continue
            valid_to = (row.get("valid_to") or "").strip()
            if not valid_to:
                continue
            group = groups.setdefault(
                new_nis,
                {"predecessors": [], "names": [], "merger_year": int(valid_to[:4])},
            )
            group["predecessors"].append(old_nis)
            group["names"].append((row.get("old_name_nl") or old_nis).strip())
    return groups


def select_groups(groups: dict[str, dict], how_many: int) -> list[str]:
    """The control says five. Take the groups with the most predecessors --
    more predecessors means more ways to get the sum wrong -- then force in
    the known-fragile Tongeren-Borgloon case regardless of its rank."""
    ranked = sorted(groups, key=lambda n: (-len(groups[n]["predecessors"]), n))
    chosen = ranked[:how_many]
    for nis in ALWAYS_INCLUDE:
        if nis in groups and nis not in chosen:
            chosen.append(nis)
    return chosen


def build_series(by_year: dict[int, dict[str, int]], nis_codes: list[str]) -> dict[int, int | None]:
    """Sum the given communes per year. None where no code is present that
    year -- an absent commune is not a commune with zero people."""
    series: dict[int, int | None] = {}
    for year, totals in sorted(by_year.items()):
        present = [totals[n] for n in nis_codes if n in totals]
        series[year] = sum(present) if present else None
    return series


def step_change_pct(series: dict[int, int | None], boundary_year: int) -> float | None:
    """Percentage jump across the merger boundary: last year before it vs.
    first year at or after it."""
    before = [y for y, v in series.items() if y < boundary_year and v]
    after = [y for y, v in series.items() if y >= boundary_year and v]
    if not before or not after:
        return None
    prev, nxt = series[max(before)], series[min(after)]
    if not prev:
        return None
    return (nxt - prev) / prev * 100.0


def typical_yoy_pct(series: dict[int, int | None], boundary_year: int) -> float:
    """Median absolute year-on-year change, ignoring the boundary step itself
    -- this is what 'normal' looks like for this particular commune."""
    years = sorted(y for y, v in series.items() if v)
    moves = []
    # strict=False is deliberate: this pairs each year with the next, so the
    # two sides differ in length by one by construction.
    for prev, nxt in zip(years, years[1:], strict=False):
        if prev < boundary_year <= nxt:
            continue
        a, b = series[prev], series[nxt]
        if a:
            moves.append(abs((b - a) / a * 100.0))
    if not moves:
        return 0.0
    moves.sort()
    mid = len(moves) // 2
    return moves[mid] if len(moves) % 2 else (moves[mid - 1] + moves[mid]) / 2


def sparkline(series: dict[int, int | None]) -> str:
    blocks = "▁▂▃▄▅▆▇█"
    vals = [v for v in series.values() if v]
    if not vals:
        return ""
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1
    return "".join(
        " " if v is None else blocks[min(int((v - lo) / span * (len(blocks) - 1)), len(blocks) - 1)]
        for _, v in sorted(series.items())
    )


def try_plot(results: list[dict], out_dir: Path) -> Path | None:
    """Chart only if matplotlib happens to be installed. It is deliberately
    NOT in requirements.txt: a throwaway verification aid must not add a
    dependency the daily workflow then has to install forever. The numbers
    below are the verdict; the chart is a convenience."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    n = len(results)
    fig, axes = plt.subplots(n, 1, figsize=(9, 3 * n), squeeze=False)
    # strict=True: one axis per result by construction, so a mismatch means
    # a chart silently missing a commune -- exactly what must not pass quietly.
    for ax, res in zip((a for row in axes for a in row), results, strict=True):
        for label, series, style in (
            ("raw successor", res["raw"], "--"),
            ("crosswalk-stitched", res["stitched"], "-"),
        ):
            pts = [(y, v) for y, v in sorted(series.items()) if v]
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], style, label=label)
        ax.axvline(res["merger_year"] - 0.5, color="grey", lw=1, alpha=0.6)
        ax.set_title(f"{res['new_nis']} — {res['verdict']}")
        ax.legend(fontsize=8)
    fig.tight_layout()
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "population_continuity.png"
    fig.savefig(path, dpi=110)
    return path


def run(pop_dir: Path, out_dir: Path, how_many: int) -> int:
    print(f"Reading population files from {pop_dir}")
    by_year = read_population_by_year(pop_dir)
    if not by_year:
        raise MissingPopulationData("No usable population files found.")
    print(f"Loaded {len(by_year)} years: {min(by_year)}-{max(by_year)}\n")

    groups = read_merger_groups(CROSSWALK)
    chosen = select_groups(groups, how_many)
    print(f"Checking {len(chosen)} merger groups from {CROSSWALK.name}\n")

    results, failures = [], 0
    for new_nis in chosen:
        group = groups[new_nis]
        preds, year = group["predecessors"], group["merger_year"]
        raw = build_series(by_year, [new_nis])
        stitched = build_series(by_year, preds + [new_nis])

        step = step_change_pct(stitched, year)
        typical = typical_yoy_pct(stitched, year)
        threshold = max(typical * STEP_THRESHOLD_MULTIPLE, STEP_THRESHOLD_FLOOR_PCT)

        if step is None:
            verdict, ok = "NO DATA across the boundary", None
        elif abs(step) > threshold:
            verdict, ok = f"FAIL step {step:+.1f}% (threshold {threshold:.1f}%)", False
            failures += 1
        else:
            verdict, ok = f"PASS step {step:+.1f}% (threshold {threshold:.1f}%)", True

        results.append(
            {
                "new_nis": new_nis,
                "raw": raw,
                "stitched": stitched,
                "merger_year": year,
                "verdict": verdict,
                "ok": ok,
            }
        )

        flag = (
            " <-- known-fragile, prefix rule missed this one" if new_nis in ALWAYS_INCLUDE else ""
        )
        print(f"{new_nis}  merged {year}  <- {', '.join(group['names'])}{flag}")
        print(f"   raw      {sparkline(raw)}")
        print(f"   stitched {sparkline(stitched)}")
        print(f"   {verdict}\n")

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "population_continuity.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["new_nis", "merger_year", "year", "raw", "stitched", "verdict"])
        for res in results:
            for year in sorted(res["stitched"]):
                writer.writerow(
                    [
                        res["new_nis"],
                        res["merger_year"],
                        year,
                        res["raw"].get(year),
                        res["stitched"].get(year),
                        res["verdict"],
                    ]
                )
    print(f"Wrote {csv_path}")
    png = try_plot(results, out_dir)
    print(f"Wrote {png}" if png else "matplotlib not installed -- numbers above are the verdict")

    print(
        f"\n{'FAILED' if failures else 'PASSED'}: {failures} of {len(results)} show a step change"
    )
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--population-dir", type=Path, default=DEFAULT_POP_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--groups", type=int, default=5, help="how many merger groups (default 5)")
    args = parser.parse_args()
    try:
        return run(args.population_dir, args.out_dir, args.groups)
    except MissingPopulationData as exc:
        print(f"\nCANNOT RUN -- data missing.\n\n{exc}\n", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
