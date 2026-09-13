"""Tests for scripts/export_explorer_payloads.py (Batch 8a).

Two tiers. The synthetic-fixture tests below are the everyday loop and cover
every branch (unknown status, a blank value under a status that does not
explain one, missing metadata, the municipal/national filename collision).
The tests at the bottom run against the REAL committed CSVs and are marked
`slow` (pyproject.toml: "reads or rebuilds committed data and takes
seconds") -- they are the ones that actually prove claude.md's "never
disagree with a download" for this batch, so a fixture standing in for the
real files would not prove what this batch exists to prove.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from export_explorer_payloads import (  # noqa: E402
    _cell,
    export_explorer_payloads,
)

REPO = Path(__file__).resolve().parents[1]


# --- fixtures ----------------------------------------------------------------


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


MUNICIPAL_HEADER = [
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

NATIONAL_HEADER = [
    "indicator_code",
    "name",
    "period",
    "value",
    "obs_status",
    "unit",
    "source_agency",
    "fetched_at",
]


@pytest.fixture
def fixture_paths(tmp_path):
    communes_history = tmp_path / "communes_history.csv"
    national = tmp_path / "belgian_macro_export.csv"
    municipal_metadata = tmp_path / "indicators.json"
    national_metadata = tmp_path / "national.json"
    out_dir = tmp_path / "out"

    _write_csv(
        communes_history,
        MUNICIPAL_HEADER,
        [
            [
                "be:mun:11001",
                "11001",
                "Aartselaar",
                "Aartselaar",
                "Aartselaar",
                "Flanders",
                "Antwerp",
                "Arrondissement Antwerpen",
                "POP_TOTAL",
                "Population",
                "count",
                "2020",
                "14000",
                "A",
                "2026-01-01T00:00:00+00:00",
            ],
            [
                "be:mun:11001",
                "11001",
                "Aartselaar",
                "Aartselaar",
                "Aartselaar",
                "Flanders",
                "Antwerp",
                "Arrondissement Antwerpen",
                "POP_TOTAL",
                "Population",
                "count",
                "2021",
                "",
                "S",
                "2026-01-01T00:00:00+00:00",
            ],
            [
                "be:mun:11002",
                "11002",
                "Antwerpen",
                "Anvers",
                "Antwerpen",
                "Flanders",
                "Antwerp",
                "Arrondissement Antwerpen",
                "POP_TOTAL",
                "Population",
                "count",
                "2020",
                "520000",
                "A",
                "2026-01-01T00:00:00+00:00",
            ],
        ],
    )
    _write_csv(
        national,
        NATIONAL_HEADER,
        [
            [
                "GDP_ANNUAL_CY",
                "Annual GDP growth",
                "2020",
                "-5.7",
                "A",
                "pp_contribution",
                "NBB",
                "2026-01-01T00:00:00+00:00",
            ],
            [
                "GDP_ANNUAL_CY",
                "Annual GDP growth",
                "2021",
                "6.9",
                "R",
                "pp_contribution",
                "NBB",
                "2026-01-01T00:00:00+00:00",
            ],
        ],
    )
    _write_json(
        municipal_metadata,
        {
            "indicators": [
                {
                    "indicator_code": "POP_TOTAL",
                    "names": {"en": "Population", "fr": "Population", "nl": "Bevolking"},
                    "unit": "count",
                    "direction": "contextual",
                    "decimals": 0,
                    "grade": "A",
                    "source": "statbel",
                    "updated": "2026-01-01",
                }
            ]
        },
    )
    _write_json(
        national_metadata,
        {
            "indicators": {
                "GDP_ANNUAL_CY": {
                    "names": {"en": "Annual GDP growth", "fr": "x", "nl": "y"},
                    "unit": "pp_contribution",
                    "direction": "higher_is_better",
                    "decimals": 1,
                    "grade": "A",
                    "source": "nbb",
                    "updated": "2026-01-01",
                }
            }
        },
    )
    return {
        "communes_history": communes_history,
        "national": national,
        "municipal_metadata": municipal_metadata,
        "national_metadata": national_metadata,
        "out_dir": out_dir,
    }


# --- _cell: the status/value contract -----------------------------------------


def test_cell_passes_through_a_real_value():
    assert _cell("345000.0", "A", where="x") == [345000.0, "A"]


def test_cell_passes_through_every_known_status_letter():
    for letter in ("A", "P", "R", "E"):
        assert _cell("1.5", letter, where="x") == [1.5, letter]
    assert _cell("41.2", "derived", where="x") == [41.2, "derived"]


def test_cell_turns_a_suppressed_blank_into_a_null_value():
    assert _cell("", "S", where="x") == [None, "S"]


def test_cell_turns_a_na_blank_into_a_null_value():
    assert _cell("", "N", where="x") == [None, "N"]


def test_cell_rejects_an_unrecognised_status():
    with pytest.raises(ValueError, match="unrecognised status"):
        _cell("1.0", "Z", where="x")


def test_cell_rejects_a_blank_value_under_a_status_that_does_not_explain_one():
    """The schema's own CHECK constraint (value IS NOT NULL OR status IN
    ('suppressed','na')) should prevent this row existing at all; if one
    reaches this script anyway, rule 13 says fail loudly rather than
    silently invent a number or drop the row."""
    with pytest.raises(ValueError, match="does not explain a"):
        _cell("", "A", where="x")


# --- export_explorer_payloads: the real thing ---------------------------------


def test_export_writes_one_file_per_scope_subdirectory(fixture_paths):
    counts = export_explorer_payloads(
        fixture_paths["communes_history"],
        fixture_paths["national"],
        fixture_paths["municipal_metadata"],
        fixture_paths["national_metadata"],
        fixture_paths["out_dir"],
    )
    assert counts == {"municipal": 1, "national": 1, "total_rows": 5}
    assert (fixture_paths["out_dir"] / "municipal" / "POP_TOTAL.json").is_file()
    assert (fixture_paths["out_dir"] / "national" / "GDP_ANNUAL_CY.json").is_file()
    assert (fixture_paths["out_dir"] / "index.json").is_file()


def test_a_real_value_and_a_suppressed_one_round_trip(fixture_paths):
    export_explorer_payloads(
        fixture_paths["communes_history"],
        fixture_paths["national"],
        fixture_paths["municipal_metadata"],
        fixture_paths["national_metadata"],
        fixture_paths["out_dir"],
    )
    payload = json.loads(
        (fixture_paths["out_dir"] / "municipal" / "POP_TOTAL.json").read_text("utf-8")
    )
    assert payload["scope"] == "municipal"
    assert payload["series"]["11001"]["2020"] == [14000.0, "A"]
    # THE SUPPRESSED CELL IS PUBLISHED, NOT DROPPED -- absent-vs-suppressed is
    # rule 26, and a payload with no entry at all for 2021 would collapse
    # "withheld" into "never measured".
    assert payload["series"]["11001"]["2021"] == [None, "S"]
    assert payload["series"]["11002"]["2020"] == [520000.0, "A"]
    assert payload["periods"] == ["2020", "2021"]
    assert payload["geographies"] == ["11001", "11002"]


def test_national_payload_keys_its_single_geography_be_country(fixture_paths):
    export_explorer_payloads(
        fixture_paths["communes_history"],
        fixture_paths["national"],
        fixture_paths["municipal_metadata"],
        fixture_paths["national_metadata"],
        fixture_paths["out_dir"],
    )
    payload = json.loads(
        (fixture_paths["out_dir"] / "national" / "GDP_ANNUAL_CY.json").read_text("utf-8")
    )
    assert payload["scope"] == "national"
    assert payload["geographies"] == ["be:country"]
    assert payload["series"]["be:country"]["2021"] == [6.9, "R"]


def test_index_carries_one_row_per_indicator_with_period_range_and_counts(fixture_paths):
    export_explorer_payloads(
        fixture_paths["communes_history"],
        fixture_paths["national"],
        fixture_paths["municipal_metadata"],
        fixture_paths["national_metadata"],
        fixture_paths["out_dir"],
    )
    index = json.loads((fixture_paths["out_dir"] / "index.json").read_text("utf-8"))
    by_code = {(r["scope"], r["indicator_code"]): r for r in index["indicators"]}
    pop = by_code[("municipal", "POP_TOTAL")]
    assert pop["period_min"] == "2020"
    assert pop["period_max"] == "2021"
    assert pop["geographies"] == 2
    assert pop["rows"] == 3
    gdp = by_code[("national", "GDP_ANNUAL_CY")]
    assert gdp["geographies"] == 1
    assert gdp["rows"] == 2


def test_two_scopes_can_share_one_indicator_code_without_colliding(fixture_paths, tmp_path):
    """The real repository has exactly this: UNEMPLOYMENT_RATE_INSURED is an
    indicator_code in BOTH data/communes_history.csv and
    data/belgian_macro_export.csv. A flat public/data/explorer/{CODE}.json
    scheme would have one silently overwrite the other; the scope
    subdirectory is what prevents it."""
    shared = "SHARED_CODE"
    _write_csv(
        fixture_paths["communes_history"],
        MUNICIPAL_HEADER,
        [
            [
                "be:mun:11001",
                "11001",
                "Aartselaar",
                "Aartselaar",
                "Aartselaar",
                "Flanders",
                "Antwerp",
                "Arrondissement Antwerpen",
                shared,
                "Shared",
                "count",
                "2020",
                "1.0",
                "A",
                "2026-01-01T00:00:00+00:00",
            ],
        ],
    )
    _write_csv(
        fixture_paths["national"],
        NATIONAL_HEADER,
        [[shared, "Shared", "2020", "2.0", "A", "count", "NBB", "2026-01-01T00:00:00+00:00"]],
    )
    _write_json(
        fixture_paths["municipal_metadata"],
        {
            "indicators": [
                {
                    "indicator_code": shared,
                    "names": {"en": "Shared", "fr": "x", "nl": "y"},
                    "unit": "count",
                }
            ]
        },
    )
    _write_json(
        fixture_paths["national_metadata"],
        {
            "indicators": {
                shared: {"names": {"en": "Shared", "fr": "x", "nl": "y"}, "unit": "count"}
            }
        },
    )
    export_explorer_payloads(
        fixture_paths["communes_history"],
        fixture_paths["national"],
        fixture_paths["municipal_metadata"],
        fixture_paths["national_metadata"],
        fixture_paths["out_dir"],
    )
    municipal = json.loads(
        (fixture_paths["out_dir"] / "municipal" / f"{shared}.json").read_text("utf-8")
    )
    national = json.loads(
        (fixture_paths["out_dir"] / "national" / f"{shared}.json").read_text("utf-8")
    )
    assert municipal["series"]["11001"]["2020"] == [1.0, "A"]
    assert national["series"]["be:country"]["2020"] == [2.0, "A"]


def test_a_code_with_rows_but_no_published_metadata_fails_loudly(fixture_paths):
    """Rule 13: never silently coerce or drop. A code the CSV carries but the
    metadata file does not is a build-ordering bug (export_site_payloads.py
    must run first) or a genuinely new, undocumented indicator -- either way
    the build should stop, not publish a card with an invented name."""
    _write_json(fixture_paths["municipal_metadata"], {"indicators": []})
    with pytest.raises(ValueError, match="no entry"):
        export_explorer_payloads(
            fixture_paths["communes_history"],
            fixture_paths["national"],
            fixture_paths["municipal_metadata"],
            fixture_paths["national_metadata"],
            fixture_paths["out_dir"],
        )


def test_running_twice_changes_nothing(fixture_paths):
    """Rule 35: identical inputs, byte-identical output."""
    export_explorer_payloads(
        fixture_paths["communes_history"],
        fixture_paths["national"],
        fixture_paths["municipal_metadata"],
        fixture_paths["national_metadata"],
        fixture_paths["out_dir"],
    )
    first = {p: p.read_bytes() for p in sorted(fixture_paths["out_dir"].rglob("*.json"))}
    export_explorer_payloads(
        fixture_paths["communes_history"],
        fixture_paths["national"],
        fixture_paths["municipal_metadata"],
        fixture_paths["national_metadata"],
        fixture_paths["out_dir"],
    )
    second = {p: p.read_bytes() for p in sorted(fixture_paths["out_dir"].rglob("*.json"))}
    assert first == second


def test_payload_keys_are_sorted_for_deterministic_diffs(fixture_paths):
    export_explorer_payloads(
        fixture_paths["communes_history"],
        fixture_paths["national"],
        fixture_paths["municipal_metadata"],
        fixture_paths["national_metadata"],
        fixture_paths["out_dir"],
    )
    raw = (fixture_paths["out_dir"] / "municipal" / "POP_TOTAL.json").read_text("utf-8")
    parsed = json.loads(raw)
    assert json.dumps(parsed, sort_keys=True, separators=(",", ":")) == raw


# --- against the real committed data ------------------------------------------


REAL_COMMUNES_HISTORY = REPO / "data" / "communes_history.csv"
REAL_NATIONAL = REPO / "data" / "belgian_macro_export.csv"
REAL_MUNICIPAL_METADATA = REPO / "public" / "data" / "metadata" / "indicators.json"
REAL_NATIONAL_METADATA = REPO / "public" / "data" / "national.json"
REAL_EXPLORER_DIR = REPO / "public" / "data" / "explorer"

#: At least two of each scope (the handoff's own requirement), chosen for a
#: reason beyond alphabetical convenience: one with real values and one that
#: is known (from a direct read of the committed CSV, see the batch report)
#: to carry at least one suppressed cell, so the equality check below is not
#: exercising only the easy path.
SAMPLE_MUNICIPAL_CODES = ("MEDIAN_HOUSE_PRICE", "PART_TIME_BENEFIT_RECIPIENTS")
SAMPLE_NATIONAL_CODES = ("GDP_ANNUAL_CY", "BUSINESS_CONFIDENCE")


def _municipal_csv_rows(code: str) -> dict[tuple[str, str], list]:
    out = {}
    with REAL_COMMUNES_HISTORY.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["indicator_code"] != code:
                continue
            value = None if row["value"] == "" else float(row["value"])
            out[(row["nis_code"], row["period"])] = [value, row["status"]]
    return out


def _national_csv_rows(code: str) -> dict[str, list]:
    out = {}
    with REAL_NATIONAL.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["indicator_code"] != code:
                continue
            value = None if row["value"] == "" else float(row["value"])
            out[row["period"]] = [value, row["obs_status"]]
    return out


@pytest.mark.slow
@pytest.mark.parametrize("code", SAMPLE_MUNICIPAL_CODES)
def test_real_municipal_payload_matches_the_real_csv_exactly(code):
    payload_path = REAL_EXPLORER_DIR / "municipal" / f"{code}.json"
    if not (REAL_COMMUNES_HISTORY.is_file() and payload_path.is_file()):
        pytest.skip("committed data or built payloads not present")
    expected = _municipal_csv_rows(code)
    payload = json.loads(payload_path.read_text("utf-8"))

    actual = {
        (nis, period): cell
        for nis, by_period in payload["series"].items()
        for period, cell in by_period.items()
    }
    assert actual == expected, (
        f"{code}: payload and data/communes_history.csv disagree on "
        f"{len(set(actual) ^ set(expected))} (geography, period) cells"
    )


@pytest.mark.slow
@pytest.mark.parametrize("code", SAMPLE_NATIONAL_CODES)
def test_real_national_payload_matches_the_real_csv_exactly(code):
    payload_path = REAL_EXPLORER_DIR / "national" / f"{code}.json"
    if not (REAL_NATIONAL.is_file() and payload_path.is_file()):
        pytest.skip("committed data or built payloads not present")
    expected = _national_csv_rows(code)
    payload = json.loads(payload_path.read_text("utf-8"))
    assert (
        payload["series"]["be:country"] == expected
    ), f"{code}: payload and data/belgian_macro_export.csv disagree"


@pytest.mark.slow
def test_every_municipal_indicator_code_in_the_history_csv_has_a_payload():
    """The other direction: no row in the download describes an indicator
    the page cannot show."""
    if not REAL_COMMUNES_HISTORY.is_file():
        pytest.skip("data/communes_history.csv not present")
    codes = set()
    with REAL_COMMUNES_HISTORY.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            codes.add(row["indicator_code"])
    if not (REAL_EXPLORER_DIR / "index.json").is_file():
        pytest.skip("explorer payloads not built")
    index = json.loads((REAL_EXPLORER_DIR / "index.json").read_text("utf-8"))
    published = {r["indicator_code"] for r in index["indicators"] if r["scope"] == "municipal"}
    assert codes <= published, f"missing payloads for {sorted(codes - published)}"
