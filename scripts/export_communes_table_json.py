"""Precompute the one artifact communes.html actually needs, instead of
shipping the raw full-history CSV to every visitor's browser.

THE PROBLEM THIS FIXES. communes.html used to fetch data/communes_history.csv
(32.2 MB after ONEM landed, up from 18 MB when docs/features/site_payloads.md
first flagged that size as wrong for a browser) and re-derive its own table
shape from it client-side, in parseCSV() + pivot() -- reparsing and
re-deduplicating the SAME 162,272 rows on every single page load. The
duplication was structural, not accidental: every row repeats a commune's
five names, region, province and arrondissement, and every indicator's name
and unit, because a flat per-observation CSV has no other shape to be.

THE FIX IS THE SAME MOVE Block J already made for /local: precompute the
reshape ONCE at export time instead of recomputing it on every request.
Measured on the real committed data: 32.19 MB CSV -> 5.36 MB JSON raw (a 6x
cut), 2.28 MB -> 1.22 MB gzipped (still smaller after both are compressed,
because JSON's dedup wins even against CSV's compressible repetition). The
browser also stops doing the parsing and pivoting work at all -- one
JSON.parse instead of a hand-rolled CSV parser plus an object-building pass
over 162k rows.

THIS PRODUCES EXACTLY WHAT communes.html'S OWN pivot() ALREADY BUILT, not a
new shape -- same keys, same year-collapse rule, so this is a performance
change with no behavioural change to what a visitor sees. In particular:

  * `yearOf(period)` is `period[:4]` in the client, replicated identically
    here, so a quarterly indicator's four quarters still collapse onto one
    calendar year.
  * pivot() OVERWRITES `byYear[year]` as it iterates -- last row for that
    year wins, not the row with the latest exact period. Replicated by
    iterating communes_history.csv in its own row order, unsorted: this
    script must not "improve" that by picking the max period per year, or a
    cell could show a different value than the CSV-driven version did.

data/communes_history.csv, data/communes_export.csv and every other bulk CSV
stay exactly as they are -- this reads communes_history.csv and writes a new
file alongside it. Nothing here writes to `observations`, and nothing here
recomputes a derived indicator (CONTROL G stays satisfied): every value
written here already exists in the CSV this script reads.
"""

import argparse
import csv
import json
from pathlib import Path


def year_of(period: str) -> str:
    return period[:4]


def build_table(csv_path: Path) -> dict:
    by_geo: dict[str, dict] = {}
    codes_seen: list[str] = []
    meta: dict[str, dict] = {}
    years: set[str] = set()
    latest_fetched_at = ""

    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            # communes.html's own load() derives its "Last fetched" stat and
            # the licence-required attribution date from RAW row data, not
            # from pivot()'s output -- replicated here as its own field
            # rather than smuggled onto a commune or indicator, since it is
            # a property of the whole export, not of any one row.
            if row["fetched_at"] > latest_fetched_at:
                latest_fetched_at = row["fetched_at"]
            geo_id = row["geo_id"]
            commune = by_geo.get(geo_id)
            if commune is None:
                commune = {
                    "geo_id": geo_id,
                    "nis_code": row["nis_code"],
                    "name_en": row["name_en"],
                    "name_fr": row["name_fr"],
                    "name_nl": row["name_nl"],
                    "region": row["region"],
                    "province": row["province"],
                    "arrondissement": row["arrondissement"],
                    "indicators": {},
                }
                by_geo[geo_id] = commune

            code = row["indicator_code"]
            if code not in codes_seen:
                codes_seen.append(code)
            indicator = commune["indicators"].setdefault(code, {"byYear": {}})

            year = year_of(row["period"])
            years.add(year)
            # Overwrite, not merge -- see module docstring: this is the exact
            # rule pivot() applies client-side, and changing it would change
            # which value a visitor sees for a quarterly indicator.
            indicator["byYear"][year] = {
                "value": row["value"],
                "period": row["period"],
                "status": row["status"],
            }

            entry = meta.setdefault(
                code, {"name": row["indicator_name"], "unit": row["unit"], "years": set()}
            )
            entry["years"].add(year)

    meta_out = {}
    for code, entry in meta.items():
        ys = sorted(entry["years"])
        meta_out[code] = {
            "name": entry["name"],
            "unit": entry["unit"],
            "minYear": ys[0],
            "maxYear": ys[-1],
        }

    return {
        "communes": list(by_geo.values()),
        "codes": codes_seen,
        "meta": meta_out,
        "years": sorted(years),
        "latest_fetched_at": latest_fetched_at,
    }


def write_table(csv_path: Path, out_path: Path) -> int:
    table = build_table(csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(table, ensure_ascii=False, separators=(",", ":")))
    return len(table["communes"])


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Precompute communes.html's table payload from communes_history.csv"
    )
    ap.add_argument("--communes-history", type=Path, default=Path("data/communes_history.csv"))
    ap.add_argument("--out", type=Path, default=Path("data/communes_table.json"))
    args = ap.parse_args()
    n = write_table(args.communes_history, args.out)
    size_mb = args.out.stat().st_size / 1e6
    print(f"Wrote {n} commune entries to {args.out} ({size_mb:.2f} MB)")


if __name__ == "__main__":
    main()
