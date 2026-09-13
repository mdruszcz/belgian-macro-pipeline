"""
Export the sharded, per-indicator payloads explorer.html reads (Batch 8a).

WHY SHARDED. all_data.html reads one 149 KB national CSV. The municipal side
has no equivalent small file: data/communes_history.csv is 35 MB / 178,128
rows / 68 indicators. A browser that wants to look at one indicator should
not have to download all 68 to get it -- this script writes one JSON file per
indicator instead (public/data/explorer/{municipal,national}/{CODE}.json,
5.3 MB in total across the municipal side, median ~46 KB, largest ~394 KB),
plus public/data/explorer/index.json, the catalogue explorer.html loads first
so it can populate every filter without fetching a single indicator payload.

WHY TWO SCOPE SUBDIRECTORIES, NOT A FLAT public/data/explorer/{CODE}.json AS
FIRST PROPOSED. UNEMPLOYMENT_RATE_INSURED exists as an indicator_code in BOTH
data/communes_history.csv (a municipal series) and data/belgian_macro_export.csv
(a national one) -- the same ONEM concept, tracked at two geography scopes,
which the (indicator_id, geo_id, period, vintage) primary key fully permits.
A flat "one file per indicator code" scheme would have one of the two
silently overwrite the other depending on write order. Scoping the directory
resolves it with no change to the shape either payload carries.

WHY BUILT FROM THE CSVS, NOT FROM THE DATABASE OR FROM national.json /
communes/{nis}.json. Those payloads are already a derivation once removed
from the committed, downloadable CSVs; reading them again here would let a
bug in that derivation reach explorer.html without ever touching the file a
researcher can actually open and diff against. Building straight from
data/communes_history.csv and data/belgian_macro_export.csv -- the two files
this repository actually publishes as downloads -- is what lets
tests/test_export_explorer_payloads.py assert byte-for-byte that the page and
the download cannot disagree (claude.md rule: never publish a wrong number).

Descriptive metadata (trilingual names, unit, decimals, preferred_direction,
grade, source, retrieval date) is NOT recomputed here -- it is read from the
two metadata files that already publish it: public/data/metadata/indicators.json
(municipal, written by export_site_payloads.py's _indicator_index()) and
public/data/national.json (national, same script's own national-payload pass).
Recomputing grade/source a third way (a third read of config/indicators and
config/sources, alongside export_site_payloads.py's and src/exporters/
provenance.py's own copies) is exactly the kind of second implementation that
eventually disagrees with the first -- CLAUDE.md rule 28 says this metadata
must come from existing published metadata, never be hand-typed or re-derived.

This script therefore needs no --db: every input is a file this repository
already commits or already publishes, so `make exports` can run it in
dependency order right after export_site_payloads.py with no new coupling to
the database.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

#: The six statuses migrations/001_core_schema.sql permits, as the single
#: letters both data/communes_history.csv (via export_communes_csv.py's own
#: STATUS_TO_LETTER) and data/belgian_macro_export.csv (via Batch 8a's
#: extended export_canonical_csv.STATUS_TO_LETTER) already publish them as --
#: plus 'derived', the one word export_communes_history_csv.py writes for a
#: value this pipeline computed itself, which has no source status to letter.
#: Nothing is translated here: passing the letter straight through is what
#: lets communes.html's statusPill() (communes.html:483-493) be reused
#: UNCHANGED on this page, and what makes the CSV-equality test in
#: tests/test_export_explorer_payloads.py a literal string comparison.
KNOWN_STATUSES = frozenset({"A", "P", "R", "E", "S", "N", "derived"})

#: Only these two explain a blank value (migrations/001_core_schema.sql:
#: CHECK (value IS NOT NULL OR status IN ('suppressed','na'))). A blank value
#: under any other status is a schema violation reaching this script, and
#: CLAUDE.md rule 13 says a source schema change must fail loudly rather than
#: silently coerce to zero or drop the row.
STATUSES_EXPLAINING_A_BLANK_VALUE = frozenset({"S", "N"})

NATIONAL_GEO = "be:country"


def _cell(value_raw: str, status: str, *, where: str) -> list:
    """One (value, status) pair, as the payload publishes it -- a 2-element
    list rather than an object, to keep the sharded files small at full
    country x period scale.

    RAISES on anything the schema does not define, rather than passing it
    through unexplained: an unrecognised status here means either CSV reader
    disagrees with the schema, or a source has started emitting a status this
    pipeline has never seen (rule 13)."""
    if status not in KNOWN_STATUSES:
        raise ValueError(
            f"{where}: unrecognised status {status!r}. Known: {sorted(KNOWN_STATUSES)}. "
            "A new status letter reaching this script is a schema change -- map it "
            "deliberately (export_canonical_csv.STATUS_TO_LETTER / "
            "export_communes_csv.STATUS_TO_LETTER), do not let it publish unexplained."
        )
    if value_raw == "":
        if status not in STATUSES_EXPLAINING_A_BLANK_VALUE:
            raise ValueError(
                f"{where}: blank value with status {status!r}, which does not explain a "
                f"blank (only {sorted(STATUSES_EXPLAINING_A_BLANK_VALUE)} do). The schema's "
                "own CHECK constraint should have prevented this row from existing."
            )
        return [None, status]
    return [float(value_raw), status]


def _read_municipal_series(csv_path: Path) -> dict[str, dict]:
    """indicator_code -> {"series": {nis: {period: [value, status]}}, "rows": int}.

    One dict entry per row of data/communes_history.csv -- the trimmed,
    committed, publicly-downloadable file, not communes_history_full.csv
    (which is gitignored and is not the download; see the exports target in
    the Makefile for why two passes exist)."""
    out: dict[str, dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            code = row["indicator_code"]
            entry = out.setdefault(code, {"series": {}, "rows": 0})
            nis = row["nis_code"]
            cell = _cell(
                row["value"],
                row["status"],
                where=f"{csv_path.name}:{code}/{nis}/{row['period']}",
            )
            entry["series"].setdefault(nis, {})[row["period"]] = cell
            entry["rows"] += 1
    return out


def _read_national_series(csv_path: Path) -> dict[str, dict]:
    """indicator_code -> {"series": {"be:country": {period: [value, status]}}, "rows": int}.

    Reads data/belgian_macro_export.csv -- the same file all_data.html fetches
    -- so this scope can never show a value that file does not also show."""
    out: dict[str, dict] = {}
    with csv_path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            code = row["indicator_code"]
            entry = out.setdefault(code, {"series": {NATIONAL_GEO: {}}, "rows": 0})
            cell = _cell(
                row["value"],
                row["obs_status"],
                where=f"{csv_path.name}:{code}/{row['period']}",
            )
            entry["series"][NATIONAL_GEO][row["period"]] = cell
            entry["rows"] += 1
    return out


def _read_municipal_metadata(path: Path) -> dict[str, dict]:
    """indicator_code -> descriptive metadata, from the already-published
    public/data/metadata/indicators.json (69 rows measured today -- one more
    than data/communes_history.csv's 68 indicator codes, because that file
    also lists UNEMPLOYMENT_RATE_INSURED_MONTHLY, which currently has no rows
    in the trimmed history CSV; harmless here, since this script only ever
    looks a code up by iterating the CSV's own codes, never the other way)."""
    if not path.is_file():
        return {}
    rows = json.loads(path.read_text(encoding="utf-8"))["indicators"]
    return {row["indicator_code"]: row for row in rows}


def _read_national_metadata(path: Path) -> dict[str, dict]:
    """indicator_code -> descriptive metadata, from the already-published
    public/data/national.json (written by export_site_payloads.py, which
    joins in names/direction/decimals/grade/source from the indicators table
    and config/sources -- see that script's _display_metadata() and
    src/exporters/provenance.py)."""
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))["indicators"]


def _metadata_fields(meta: dict, *, fallback_name: str | None = None) -> dict:
    """The subset of a metadata row this payload republishes, with the same
    key names and same None-if-absent behaviour for both scopes, so the page
    has one code path regardless of which metadata file supplied them."""
    names = meta.get("names") or ({"en": fallback_name} if fallback_name else {})
    return {
        "names": names,
        "unit": meta.get("unit"),
        "decimals": meta.get("decimals"),
        "direction": meta.get("direction"),
        "grade": meta.get("grade"),
        "source": meta.get("source"),
        "derived_from": meta.get("derived_from"),
        "input_sources": meta.get("input_sources"),
        "updated": meta.get("updated"),
        "inputs_updated": meta.get("inputs_updated"),
    }


def _payload(code: str, scope: str, series_entry: dict, meta: dict) -> dict:
    series = series_entry["series"]
    periods = sorted({period for by_period in series.values() for period in by_period})
    geographies = sorted(series)
    # Every geography's periods sorted too, so the file is deterministic key
    # order top to bottom, not just at the top level (rule 35).
    sorted_series = {
        geo: {period: series[geo][period] for period in sorted(series[geo])} for geo in geographies
    }
    payload = {
        "indicator_code": code,
        "scope": scope,
        **_metadata_fields(meta),
        "periods": periods,
        "geographies": geographies,
        "series": sorted_series,
    }
    return payload


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # sort_keys=True: decision was "sorted-key deterministic" for this new
    # format specifically (docs/features/site_payloads.md), which is a
    # stronger and simpler guarantee than relying on insertion order staying
    # stable -- export_site_payloads.py instead sorts by hand at each level
    # and skips this flag; either is byte-identical run to run (rule 35) over
    # the same input, this one is just harder to get wrong by omission.
    # ensure_ascii=False + explicit UTF-8: export_site_payloads.py's own
    # _write_json note explains why -- write_text with no encoding picks up
    # the platform codepage (cp1252 on a Belgian Windows machine), which
    # mangles the first accented commune or indicator name it meets.
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def export_explorer_payloads(
    communes_history_csv: Path,
    national_csv: Path,
    municipal_metadata: Path,
    national_metadata: Path,
    out_dir: Path,
) -> dict[str, int]:
    municipal_series = _read_municipal_series(communes_history_csv)
    national_series = _read_national_series(national_csv)
    municipal_meta = _read_municipal_metadata(municipal_metadata)
    national_meta = _read_national_metadata(national_metadata)

    index_rows = []

    for code in sorted(municipal_series):
        meta = municipal_meta.get(code)
        if meta is None:
            raise ValueError(
                f"{code} has rows in {communes_history_csv.name} but no entry in "
                f"{municipal_metadata}. Every municipal indicator with published rows "
                "must have published metadata -- re-run export_site_payloads.py first, "
                "or the name/unit/direction shown here would have to be invented."
            )
        entry = municipal_series[code]
        payload = _payload(code, "municipal", entry, meta)
        _write_json(out_dir / "municipal" / f"{code}.json", payload)
        index_rows.append(
            {
                "indicator_code": code,
                "scope": "municipal",
                **_metadata_fields(meta),
                "period_min": payload["periods"][0] if payload["periods"] else None,
                "period_max": payload["periods"][-1] if payload["periods"] else None,
                "geographies": len(payload["geographies"]),
                "rows": entry["rows"],
            }
        )

    for code in sorted(national_series):
        meta = national_meta.get(code)
        if meta is None:
            raise ValueError(
                f"{code} has rows in {national_csv.name} but no entry in "
                f"{national_metadata}. Re-run export_site_payloads.py first."
            )
        entry = national_series[code]
        payload = _payload(code, "national", entry, meta)
        _write_json(out_dir / "national" / f"{code}.json", payload)
        index_rows.append(
            {
                "indicator_code": code,
                "scope": "national",
                **_metadata_fields(meta),
                "period_min": payload["periods"][0] if payload["periods"] else None,
                "period_max": payload["periods"][-1] if payload["periods"] else None,
                "geographies": len(payload["geographies"]),
                "rows": entry["rows"],
            }
        )

    index_rows.sort(key=lambda r: (r["scope"], r["indicator_code"]))
    _write_json(out_dir / "index.json", {"indicators": index_rows})

    return {
        "municipal": len(municipal_series),
        "national": len(national_series),
        "total_rows": sum(e["rows"] for e in municipal_series.values())
        + sum(e["rows"] for e in national_series.values()),
    }


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Export sharded per-indicator payloads for explorer.html (Batch 8a)"
    )
    ap.add_argument("--communes-history", default="data/communes_history.csv")
    ap.add_argument("--national", default="data/belgian_macro_export.csv")
    ap.add_argument(
        "--municipal-metadata",
        default="public/data/metadata/indicators.json",
        help="Written by export_site_payloads.py; must already exist",
    )
    ap.add_argument(
        "--national-metadata",
        default="public/data/national.json",
        help="Written by export_site_payloads.py; must already exist",
    )
    ap.add_argument("--out-dir", default="public/data/explorer")
    args = ap.parse_args()

    counts = export_explorer_payloads(
        Path(args.communes_history),
        Path(args.national),
        Path(args.municipal_metadata),
        Path(args.national_metadata),
        Path(args.out_dir),
    )
    print(
        f"Exported {counts['municipal']} municipal + {counts['national']} national "
        f"indicator payloads ({counts['total_rows']} total observations) to {args.out_dir}"
    )


if __name__ == "__main__":
    main()
