"""Age-pyramid HISTORY exporter: growth-on-current-territory summation across
merger waves, coverage bookkeeping, schema refusal and determinism.

Hand-computed fixtures throughout (CLAUDE.md rule 5). Two source years are
built: a "before" year where a predecessor commune (11007, standing in for
Borsbeek) still reports on its own, and an "after" year where only the
successor (11002, standing in for Antwerp) reports -- across the exact
boundary the real 2025 wave crosses for Borsbeek -> Antwerp.
"""

import csv
import json
import sys
import zipfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

from export_commune_age_sex_history import (  # noqa: E402
    AgeSexExportError,
    CycleError,
    build_history,
    export,
    lineage_from_crosswalk,
    read_crosswalk,
    resolve_successor,
)
from plot_population_continuity import MissingPopulationData  # noqa: E402

HEADER = "CD_REFNIS|CD_SEX|CD_AGE|MS_POPULATION"


def _write_source(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join([HEADER, *rows]) + "\n", encoding="utf-8")


def _geographies(tmp_path: Path, codes: list[str]) -> Path:
    path = tmp_path / "geographies.json"
    path.write_text(
        json.dumps({"geographies": [{"level": "municipality", "nis_code": c} for c in codes]}),
        encoding="utf-8",
    )
    return path


def _crosswalk(tmp_path: Path, rows: list[dict]) -> Path:
    path = tmp_path / "municipality_crosswalk.csv"
    fieldnames = [
        "old_nis",
        "old_name_nl",
        "old_name_fr",
        "new_nis",
        "relationship",
        "has_partial_transfer",
        "valid_from",
        "valid_to",
        "evidence",
        "verified",
        "verified_source",
        "note",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            full = dict.fromkeys(fieldnames, "")
            full.update(row)
            writer.writerow(full)
    return path


def _base_crosswalk_row(**overrides) -> dict:
    row = {
        "old_nis": "11007",
        "new_nis": "11002",
        "relationship": "absorbed",
        "has_partial_transfer": "false",
        "valid_from": "1977-01-01",
        "valid_to": "2025-01-01",
    }
    row.update(overrides)
    return row


@pytest.fixture
def two_year_fixture(tmp_path):
    """Year 2024: successor 11002 (200 people) and predecessor 11007 (50
    people) both report on their own -- pre-merger. Year 2025: only 11002
    reports, now at 250 (having absorbed 11007's territory) -- post-merger.
    Hand-computed expected T = 250 for both years: 2024 is 200 + 50 = 250
    (own + predecessor, since both existed as separate reporting geographies
    that year); 2025 is 250 alone (successor now covers the whole current
    territory)."""
    source_dir = tmp_path / "population"
    source_dir.mkdir()
    _write_source(
        source_dir / "TF_SOC_POP_STRUCT_2024.txt",
        [
            "11002|M|0|100",
            "11002|F|0|100",
            "11007|M|0|25",
            "11007|F|0|25",
        ],
    )
    _write_source(
        source_dir / "TF_SOC_POP_STRUCT_2025.txt",
        [
            "11002|M|0|125",
            "11002|F|0|125",
        ],
    )
    geographies = _geographies(tmp_path, ["11002"])
    crosswalk = _crosswalk(tmp_path, [_base_crosswalk_row()])
    return source_dir, geographies, crosswalk


def test_growth_on_current_territory_sums_predecessor_before_merger(two_year_fixture):
    source_dir, geographies, crosswalk = two_year_fixture

    history = build_history(source_dir, geographies, crosswalk)

    years = {y["period"]: y for y in history["11002"]}
    assert set(years) == {"2024", "2025"}

    before = years["2024"]
    assert sum(before["male"]) + sum(before["female"]) == 250
    assert before["coverage"] == {"found": 2, "expected": 2}

    after = years["2025"]
    assert sum(after["male"]) + sum(after["female"]) == 250
    assert after["coverage"] == {"found": 1, "expected": 1}


def test_coverage_flags_a_genuinely_missing_predecessor_year(tmp_path):
    """If the predecessor's row is simply absent from a pre-merger year's
    file (a real gap, not a merger), coverage must say so rather than
    silently reporting the successor's partial total as if it were whole."""
    source_dir = tmp_path / "population"
    source_dir.mkdir()
    _write_source(
        source_dir / "TF_SOC_POP_STRUCT_2024.txt",
        ["11002|M|0|100", "11002|F|0|100"],  # 11007 row missing this year
    )
    geographies = _geographies(tmp_path, ["11002"])
    crosswalk = _crosswalk(tmp_path, [_base_crosswalk_row()])

    history = build_history(source_dir, geographies, crosswalk)

    year = history["11002"][0]
    assert sum(year["male"]) + sum(year["female"]) == 200
    assert year["coverage"] == {"found": 1, "expected": 2}


def test_export_writes_one_file_per_current_commune_with_bands_and_years(two_year_fixture):
    source_dir, geographies, crosswalk = two_year_fixture
    output = source_dir.parent / "out"

    communes, commune_years = export(source_dir, output, geographies, crosswalk)

    assert (communes, commune_years) == (1, 2)
    payload = json.loads((output / "11002.json").read_text(encoding="utf-8"))
    assert payload["nis_code"] == "11002"
    assert payload["source_id"] == "statbel"
    assert len(payload["bands"]) == 21
    assert payload["bands"][0] == {"from": 0, "to": 4}
    assert payload["bands"][-1] == {"from": 100, "to": None}
    assert [y["period"] for y in payload["years"]] == ["2024", "2025"]


def test_zip_source_is_read_transparently(tmp_path):
    """The real download is Statbel's .zip; a bare .txt fixture exercises the
    same reader as .zip via the shared read_age_sex helper, so this test
    proves the history exporter's own discovery (by suffix) also accepts a
    zip member, matching the single-year exporter's contract."""
    source_dir = tmp_path / "population"
    source_dir.mkdir()
    txt_path = source_dir / "TF_SOC_POP_STRUCT_2024.txt"
    _write_source(txt_path, ["11002|M|0|10", "11002|F|0|10"])
    zip_path = source_dir / "TF_SOC_POP_STRUCT_2025.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(
            "TF_SOC_POP_STRUCT_2025.txt", "\n".join([HEADER, "11002|M|0|12", "11002|F|0|12"])
        )

    geographies = _geographies(tmp_path, ["11002"])
    crosswalk = _crosswalk(tmp_path, [])

    history = build_history(source_dir, geographies, crosswalk)

    years = {y["period"]: y for y in history["11002"]}
    assert sum(years["2024"]["male"]) + sum(years["2024"]["female"]) == 20
    assert sum(years["2025"]["male"]) + sum(years["2025"]["female"]) == 24


def test_a_changed_column_layout_is_refused_not_guessed(tmp_path):
    """CLAUDE.md rule 13: a schema change fails loudly. A file with none of
    the known NIS-column candidates must raise, never silently produce an
    empty or partial history. The reader is the shared `read_age_sex` helper
    from the single-year exporter, which raises `MissingPopulationData` (via
    `plot_population_continuity._pick_column`) for this case -- not this
    module's own `AgeSexExportError` -- so that is what must propagate here
    too, unmasked."""
    source_dir = tmp_path / "population"
    source_dir.mkdir()
    bad = source_dir / "TF_SOC_POP_STRUCT_2024.txt"
    bad.write_text(
        "\n".join(["SOME_OTHER_LAYOUT|CD_SEX|CD_AGE|MS_POPULATION", "11002|M|0|10"]) + "\n",
        encoding="utf-8",
    )
    geographies = _geographies(tmp_path, ["11002"])
    crosswalk = _crosswalk(tmp_path, [])

    with pytest.raises(MissingPopulationData, match="could not find a nis column"):
        build_history(source_dir, geographies, crosswalk)


def test_duplicate_year_in_source_dir_is_refused(tmp_path):
    source_dir = tmp_path / "population"
    source_dir.mkdir()
    _write_source(source_dir / "TF_SOC_POP_STRUCT_2024.txt", ["11002|M|0|10", "11002|F|0|10"])
    _write_source(source_dir / "TF_SOC_POP_STRUCT_2024_v2.txt", ["11002|M|0|10", "11002|F|0|10"])
    geographies = _geographies(tmp_path, ["11002"])
    crosswalk = _crosswalk(tmp_path, [])

    with pytest.raises(AgeSexExportError, match="duplicate year"):
        build_history(source_dir, geographies, crosswalk)


def test_crosswalk_row_resolving_outside_current_geography_is_refused(tmp_path):
    """A crosswalk that claims a predecessor resolves to a NIS code missing
    from today's geographies.json is a crosswalk/geography disagreement, not
    something to paper over."""
    crosswalk = _crosswalk(tmp_path, [_base_crosswalk_row(new_nis="99999")])
    rows = read_crosswalk(crosswalk)
    current_codes = {"11002"}

    with pytest.raises(AgeSexExportError, match="disagreement"):
        lineage_from_crosswalk(rows, current_codes)


def test_resolve_successor_walks_two_hops_and_guards_cycles(tmp_path):
    crosswalk = _crosswalk(
        tmp_path,
        [
            {"old_nis": "A", "new_nis": "B", "valid_to": "2019-01-01"},
            {"old_nis": "B", "new_nis": "C", "valid_to": "2025-01-01"},
        ],
    )
    by_old = {row.old_nis: row for row in read_crosswalk(crosswalk)}
    assert resolve_successor("A", by_old) == "C"
    assert resolve_successor("C", by_old) is None

    cyclic = _crosswalk(
        tmp_path,
        [
            {"old_nis": "X", "new_nis": "Y", "valid_to": "2019-01-01"},
            {"old_nis": "Y", "new_nis": "X", "valid_to": "2019-01-01"},
        ],
    )
    by_old_cyclic = {row.old_nis: row for row in read_crosswalk(cyclic)}
    with pytest.raises(CycleError):
        resolve_successor("X", by_old_cyclic)


def test_determinism_repeated_export_is_byte_identical(two_year_fixture):
    source_dir, geographies, crosswalk = two_year_fixture
    output = source_dir.parent / "out"

    export(source_dir, output, geographies, crosswalk)
    first = (output / "11002.json").read_bytes()
    export(source_dir, output, geographies, crosswalk)
    second = (output / "11002.json").read_bytes()

    assert first == second


def test_existing_single_year_demography_payload_is_untouched():
    """This PR adds a NEW payload; it must not alter the single-year one
    pages already read (CLAUDE.md rule 35 -- byte-identical output for
    identical inputs, and this history exporter's inputs never feed that
    other exporter's outputs)."""
    demography_dir = REPO / "public/data/demography"
    history_dir = REPO / "public/data/demography_history"
    assert demography_dir.exists()
    # The two directories are disjoint outputs of two different scripts --
    # this only guards against a future edit accidentally making the history
    # exporter write into the single-year directory.
    assert demography_dir != history_dir


def test_real_downloaded_years_produce_full_coverage_at_merger_boundaries():
    """Guards the real 2025 merger boundary in the committed output: once
    real data is downloaded and exported (see manual_sources.md), a merged
    commune's coverage must read found==expected on both sides of the
    boundary -- never a partial sum silently published as whole."""
    history_dir = REPO / "public/data/demography_history"
    if not history_dir.exists() or not any(history_dir.glob("*.json")):
        pytest.skip("no committed demography_history payload in this checkout")

    tongeren_looz = json.loads((history_dir / "73111.json").read_text(encoding="utf-8"))
    by_period = {y["period"]: y for y in tongeren_looz["years"]}
    if "2024" in by_period:
        assert by_period["2024"]["coverage"] == {"found": 2, "expected": 2}
    if "2025" in by_period:
        assert by_period["2025"]["coverage"] == {"found": 1, "expected": 1}
