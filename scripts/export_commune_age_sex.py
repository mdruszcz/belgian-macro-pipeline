"""Export compact commune age-by-sex payloads for the visual profile.

The source is Statbel's annual ``TF_SOC_POP_STRUCT_<year>`` bulk file.  It
contains single-year ages (100 is top-coded as 100+) and the source's F/M sex
dimension, further split by nationality and civil status.  This exporter sums
those lower dimensions into five-year age bands without estimating any cell.

The raw files remain manual inputs under ``data/raw``.  Generated payloads are
small, public and reproducible under ``public/data/demography``.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from plot_population_continuity import (  # noqa: E402
    AGE_COLUMNS,
    NIS_COLUMNS,
    POP_COLUMNS,
    _open_text_member,
    _pick_column,
    _year_from_filename,
)

REPO = Path(__file__).resolve().parents[1]
DEFAULT_GEOGRAPHIES = REPO / "public" / "data" / "metadata" / "geographies.json"
DEFAULT_COMMUNES = REPO / "public" / "data" / "communes"
DEFAULT_OUTPUT = REPO / "public" / "data" / "demography"
SEX_COLUMNS = ("cd_sex", "sex", "geslacht", "sexe")
SEX_KEYS = {"M": "male", "F": "female"}


class AgeSexExportError(ValueError):
    """The source cannot be converted without guessing or losing people."""


def _band_start(age: int) -> int:
    if not 0 <= age <= 100:
        raise AgeSexExportError(f"age outside Statbel's documented 0..100 range: {age}")
    return 100 if age == 100 else (age // 5) * 5


def read_age_sex(path: Path) -> tuple[int, dict[str, dict[int, dict[str, int]]]]:
    """Return ``(year, {nis: {band_start: {male, female}}})`` from one file."""
    year = _year_from_filename(path)
    if year is None:
        raise AgeSexExportError(f"no year in source filename: {path.name}")

    display_name, stream = _open_text_member(path)
    try:
        sample = stream.read(8192)
        stream.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters="|;,\t")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(stream, dialect=dialect)
        if not reader.fieldnames:
            raise AgeSexExportError(f"{display_name} is empty or has no header")
        fields = list(reader.fieldnames)
        nis_col = _pick_column(fields, NIS_COLUMNS, path, "nis")
        pop_col = _pick_column(fields, POP_COLUMNS, path, "population")
        age_col = _pick_column(fields, AGE_COLUMNS, path, "age")
        sex_col = _pick_column(fields, SEX_COLUMNS, path, "sex")

        values: dict[str, dict[int, dict[str, int]]] = defaultdict(
            lambda: defaultdict(lambda: {"male": 0, "female": 0})
        )
        rows = 0
        for row in reader:
            nis = (row.get(nis_col) or "").strip()
            raw_value = (row.get(pop_col) or "").strip().replace(" ", "").replace(",", "")
            raw_age = (row.get(age_col) or "").strip()
            raw_sex = (row.get(sex_col) or "").strip().upper()
            if not nis or not raw_value or not raw_age:
                continue
            if raw_sex not in SEX_KEYS:
                raise AgeSexExportError(
                    f"{display_name}: unsupported non-empty sex code {raw_sex!r} for {nis}"
                )
            try:
                value = int(float(raw_value))
                age = int(float(raw_age))
            except ValueError as exc:
                raise AgeSexExportError(
                    f"{display_name}: non-numeric age/population for {nis}: "
                    f"{raw_age!r}/{raw_value!r}"
                ) from exc
            if value < 0:
                raise AgeSexExportError(f"{display_name}: negative population for {nis}")
            values[nis][_band_start(age)][SEX_KEYS[raw_sex]] += value
            rows += 1
    finally:
        stream.close()

    if not rows or not values:
        raise AgeSexExportError(f"{display_name}: parsed no age-by-sex observations")
    return year, values


def _current_nis_codes(path: Path) -> set[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        row["nis_code"]
        for row in payload.get("geographies", [])
        if row.get("level") == "municipality"
    }


def _published_population(communes_dir: Path, nis: str, period: str) -> int:
    payload = json.loads((communes_dir / f"{nis}.json").read_text(encoding="utf-8"))
    entry = payload.get("indicators", {}).get("POPULATION_BY_COMMUNE", {})
    cell = entry.get("periods", {}).get(period, {})
    value = cell.get("value")
    if not isinstance(value, (int, float)):
        raise AgeSexExportError(f"{nis}: no published population for {period}")
    return int(value)


def export(
    source: Path,
    output_dir: Path,
    geographies_path: Path,
    communes_dir: Path,
    source_updated: str,
) -> tuple[int, int]:
    year, values = read_age_sex(source)
    period = str(year)
    expected = _current_nis_codes(geographies_path)
    found = set(values)
    if found != expected:
        raise AgeSexExportError(
            f"source/current geography disagreement: missing={sorted(expected - found)}, "
            f"unexpected={sorted(found - expected)}"
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    observations = 0
    for nis in sorted(expected):
        bands = []
        for start in range(0, 101, 5):
            cell = values[nis].get(start, {"male": 0, "female": 0})
            bands.append(
                {
                    "from": start,
                    "to": None if start == 100 else start + 4,
                    "male": cell["male"],
                    "female": cell["female"],
                }
            )
            observations += 2
        total = sum(band["male"] + band["female"] for band in bands)
        published = _published_population(communes_dir, nis, period)
        if total != published:
            raise AgeSexExportError(
                f"{nis}: age/sex sum {total} != published population {published} for {period}"
            )
        payload = {
            "nis_code": nis,
            "period": period,
            "reference_date": f"{period}-01-01",
            "source_id": "statbel",
            "source_updated": source_updated,
            "age_top_code": 100,
            "bands": bands,
        }
        (output_dir / f"{nis}.json").write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
    return len(expected), observations


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--geographies", type=Path, default=DEFAULT_GEOGRAPHIES)
    parser.add_argument("--communes-dir", type=Path, default=DEFAULT_COMMUNES)
    parser.add_argument(
        "--source-updated",
        required=True,
        help="ISO date from the source file's Last-Modified metadata",
    )
    args = parser.parse_args()
    communes, observations = export(
        args.source,
        args.output_dir,
        args.geographies,
        args.communes_dir,
        args.source_updated,
    )
    print(f"Exported {communes} commune pyramids ({observations} age/sex cells)")


if __name__ == "__main__":
    main()
