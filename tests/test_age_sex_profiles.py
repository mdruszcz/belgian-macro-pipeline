"""Population-pyramid exporter and visual-profile contracts."""

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

from export_commune_age_sex import AgeSexExportError, export, read_age_sex  # noqa: E402

HEADER = "CD_REFNIS|CD_SEX|CD_AGE|MS_POPULATION"


def _source(tmp_path: Path, rows: list[str]) -> Path:
    path = tmp_path / "TF_SOC_POP_STRUCT_2026.txt"
    path.write_text("\n".join([HEADER, *rows]) + "\n", encoding="utf-8")
    return path


def test_reader_keeps_sex_and_bands_single_ages_without_estimation(tmp_path):
    source = _source(
        tmp_path,
        [
            "11001|M|0|2",
            "11001|M|4|3",
            "11001|F|2|7",
            # A second lower-dimensional cell is summed, not treated as a total.
            "11001|F|2|5",
            "11001|M|100|1",
            "11001|F|100|4",
        ],
    )

    year, values = read_age_sex(source)

    assert year == 2026
    assert values["11001"][0] == {"male": 5, "female": 12}
    assert values["11001"][100] == {"male": 1, "female": 4}


def test_export_writes_21_bands_and_checks_the_published_total(tmp_path):
    source = _source(tmp_path, ["11001|M|0|2", "11001|F|5|3"])
    geographies = tmp_path / "geographies.json"
    geographies.write_text(
        json.dumps({"geographies": [{"level": "municipality", "nis_code": "11001"}]}),
        encoding="utf-8",
    )
    communes = tmp_path / "communes"
    communes.mkdir()
    (communes / "11001.json").write_text(
        json.dumps({"indicators": {"POPULATION_BY_COMMUNE": {"periods": {"2026": {"value": 5}}}}}),
        encoding="utf-8",
    )
    output = tmp_path / "out"

    count, cells = export(source, output, geographies, communes, "2026-06-10")
    payload = json.loads((output / "11001.json").read_text(encoding="utf-8"))

    assert (count, cells) == (1, 42)
    assert len(payload["bands"]) == 21
    assert payload["bands"][0] == {"from": 0, "to": 4, "male": 2, "female": 0}
    assert payload["bands"][-1]["to"] is None
    assert payload["source_updated"] == "2026-06-10"


def test_export_refuses_a_pyramid_that_disagrees_with_population(tmp_path):
    source = _source(tmp_path, ["11001|M|0|2", "11001|F|5|3"])
    geographies = tmp_path / "geographies.json"
    geographies.write_text(
        json.dumps({"geographies": [{"level": "municipality", "nis_code": "11001"}]}),
        encoding="utf-8",
    )
    communes = tmp_path / "communes"
    communes.mkdir()
    (communes / "11001.json").write_text(
        json.dumps({"indicators": {"POPULATION_BY_COMMUNE": {"periods": {"2026": {"value": 6}}}}}),
        encoding="utf-8",
    )

    with pytest.raises(AgeSexExportError, match="age/sex sum 5 != published population 6"):
        export(source, tmp_path / "out", geographies, communes, "2026-06-10")


def test_current_pyramids_cover_every_current_commune_and_match_samples():
    geography = json.loads(
        (REPO / "public/data/metadata/geographies.json").read_text(encoding="utf-8")
    )
    expected = {
        row["nis_code"] for row in geography["geographies"] if row["level"] == "municipality"
    }
    output = REPO / "public/data/demography"
    assert {path.stem for path in output.glob("*.json")} == expected

    for nis in ("11001", "21004", "92094"):
        pyramid = json.loads((output / f"{nis}.json").read_text(encoding="utf-8"))
        commune = json.loads(
            (REPO / "public/data/communes" / f"{nis}.json").read_text(encoding="utf-8")
        )
        total = sum(row["male"] + row["female"] for row in pyramid["bands"])
        expected_total = commune["indicators"]["POPULATION_BY_COMMUNE"]["periods"]["2026"]["value"]
        assert total == expected_total
        assert len(pyramid["bands"]) == 21


def test_visual_profile_loads_and_renders_the_specialised_payload():
    page = (REPO / "commune.html").read_text(encoding="utf-8")
    assert "'public/data/demography/' + nis + '.json'" in page
    assert "function renderAgeSex()" in page
    assert 'id="pyramidChart"' in page
    assert "payload.bands.slice().reverse()" in page
