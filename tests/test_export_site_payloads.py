"""Tests for the per-entity site payload exporter (Block J,
docs/features/site_payloads.md).

Each payload is a reshape of an existing bulk export, so what matters here is
that the reshape is faithful: no value dropped, none invented, and the
current-vs-historical distinction the source CSVs already enforce survives
the reshape unchanged.
"""

import csv
import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from export_site_payloads import export_site_payloads  # noqa: E402

from src.db import migrate  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
REAL_MIGRATIONS_DIR = REPO / "migrations"

HISTORY_HEADER = (
    "geo_id,nis_code,name_en,name_fr,name_nl,region,province,arrondissement,"
    "indicator_code,indicator_name,unit,period,value,status,fetched_at"
)
LATEST_HEADER = HISTORY_HEADER  # same column shape, latest-only rows


def _history_row(nis, geo_id, name, code, name_ind, unit, period, value, status="A"):
    return (
        f"{geo_id},{nis},{name},{name},{name},Flanders,Antwerp,"
        f"Arrondissement Antwerpen,{code},{name_ind},{unit},{period},{value},{status},"
    )


def _write(path: Path, header: str, rows: list[str]) -> None:
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def _geo_db(db_path: Path) -> None:
    migrate.run(db_path, migrations_dir=REAL_MIGRATIONS_DIR)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, "
        "parent_geo_id, valid_from) VALUES "
        "('be:country','01000','country','Belgie','Belgique','Belgium', NULL, '1830-01-01')"
    )
    conn.execute(
        "INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, "
        "parent_geo_id, valid_from) VALUES "
        "('be:reg:02000','02000','region','Vlaanderen','Flandre','Flanders', "
        "'be:country', '1830-01-01')"
    )
    conn.execute(
        "INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, "
        "parent_geo_id, valid_from) VALUES "
        "('be:mun:11001','11001','municipality','Aartselaar','Aartselaar','Aartselaar', "
        "'be:reg:02000', '1830-01-01')"
    )
    conn.execute(
        "INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, "
        "parent_geo_id, valid_from) VALUES "
        "('be:mun:11002','11002','municipality','Antwerpen','Anvers','Antwerp', "
        "'be:reg:02000', '1830-01-01')"
    )
    # A merged-away commune: not currently valid, must not leak into the payloads.
    conn.execute(
        "INSERT INTO geographies (geo_id, nis_code, level, name_nl, name_fr, name_en, "
        "parent_geo_id, valid_from, valid_to) VALUES "
        "('be:mun:OLD','99999','municipality','Oud','Ancien','Old', "
        "'be:reg:02000', '1830-01-01', '2020-12-31')"
    )
    conn.commit()
    conn.close()


def test_every_value_round_trips_between_history_csv_and_commune_payload(tmp_path):
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history_csv = tmp_path / "history.csv"
    _write(
        history_csv,
        HISTORY_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2020", 100
            ),
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2021", 110
            ),
            _history_row(
                "11002", "be:mun:11002", "Antwerp", "POP", "Population", "count", "2021", 500000
            ),
        ],
    )
    latest_csv = tmp_path / "latest.csv"
    _write(
        latest_csv,
        LATEST_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2021", 110
            ),
            _history_row(
                "11002", "be:mun:11002", "Antwerp", "POP", "Population", "count", "2021", 500000
            ),
        ],
    )
    national_csv = tmp_path / "national.csv"
    national_csv.write_text(
        "indicator_code,name,period,value,obs_status,unit,source_agency,fetched_at\n"
        "GDP,GDP volume,2020-Q1,100.0,A,index,NBB,\n",
        encoding="utf-8",
    )

    out_dir = tmp_path / "public"
    export_site_payloads(
        db_path, history_csv, latest_csv, national_csv, out_dir, "b1", "pass", sections_config=None
    )

    import json

    aartselaar = json.loads((out_dir / "communes" / "11001.json").read_text())
    values_in_payload = {
        (aartselaar["nis_code"], "POP", period, cell["value"])
        for period, cell in aartselaar["indicators"]["POP"]["periods"].items()
    }
    values_in_csv = set()
    with history_csv.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row["nis_code"] == "11001":
                values_in_csv.add(
                    (row["nis_code"], row["indicator_code"], row["period"], float(row["value"]))
                )

    assert values_in_payload == values_in_csv


def test_indicator_payload_contains_only_current_communes(tmp_path):
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history_csv = tmp_path / "history.csv"
    _write(
        history_csv,
        HISTORY_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2021", 100
            )
        ],
    )
    latest_csv = tmp_path / "latest.csv"
    _write(
        latest_csv,
        LATEST_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2021", 100
            ),
            _history_row(
                "11002", "be:mun:11002", "Antwerp", "POP", "Population", "count", "2021", 500000
            ),
        ],
    )
    national_csv = tmp_path / "national.csv"
    national_csv.write_text(
        "indicator_code,name,period,value,obs_status,unit,source_agency,fetched_at\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "public"
    export_site_payloads(
        db_path, history_csv, latest_csv, national_csv, out_dir, "b1", "pass", sections_config=None
    )

    import json

    payload = json.loads((out_dir / "indicators" / "POP.json").read_text())
    assert set(payload["communes"]) == {"11001", "11002"}
    assert "99999" not in payload["communes"]


def test_geographies_metadata_ancestor_walk_resolves_to_a_real_region(tmp_path):
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history_csv = tmp_path / "history.csv"
    _write(history_csv, HISTORY_HEADER, [])
    latest_csv = tmp_path / "latest.csv"
    _write(latest_csv, LATEST_HEADER, [])
    national_csv = tmp_path / "national.csv"
    national_csv.write_text(
        "indicator_code,name,period,value,obs_status,unit,source_agency,fetched_at\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "public"
    export_site_payloads(
        db_path, history_csv, latest_csv, national_csv, out_dir, "b1", "pass", sections_config=None
    )

    import json

    geos = json.loads((out_dir / "metadata" / "geographies.json").read_text())["geographies"]
    by_id = {g["geo_id"]: g for g in geos}

    # The merged-away commune (valid_to set) must not appear at all.
    assert "be:mun:OLD" not in by_id

    commune = by_id["be:mun:11001"]
    parent = by_id[commune["parent_geo_id"]]
    assert parent["level"] == "region"


def test_manifest_row_counts_match_generated_payload_counts(tmp_path):
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history_csv = tmp_path / "history.csv"
    _write(
        history_csv,
        HISTORY_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2021", 100
            )
        ],
    )
    latest_csv = tmp_path / "latest.csv"
    _write(latest_csv, LATEST_HEADER, [])
    national_csv = tmp_path / "national.csv"
    national_csv.write_text(
        "indicator_code,name,period,value,obs_status,unit,source_agency,fetched_at\n"
        "GDP,GDP volume,2020-Q1,100.0,A,index,NBB,\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "public"
    export_site_payloads(
        db_path, history_csv, latest_csv, national_csv, out_dir, "b1", "pass", sections_config=None
    )

    import json

    manifest = json.loads((out_dir / "manifest.json").read_text())
    assert manifest["datasets"]["communes"]["count"] == len(
        list((out_dir / "communes").glob("*.json"))
    )
    assert manifest["datasets"]["national"]["indicators"] == 1
    assert manifest["validation_status"] == "pass"


def test_payload_carries_the_source_retrieval_date_for_licence_attribution(tmp_path):
    """Statbel's 2015 open-data licence requires the published attribution to
    carry the date of last update, and terminates automatically without it
    (docs/data_catalog.md). The pages cannot show a date the payload does not
    carry, so it is exported per indicator as `updated`.

    A DERIVED indicator must NOT get one: it was computed, not fetched, and
    the same licence forbids misleading a reader about the update date as
    squarely as it requires showing one. Its `fetched_at` is empty in the
    source CSV and the key stays absent, matching the payload format's
    absent-not-null rule.
    """
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history_csv = tmp_path / "history.csv"
    _write(
        history_csv,
        HISTORY_HEADER,
        [
            # A fetched indicator, two periods retrieved on different days:
            # the LATER date must win, not the last row read.
            "be:mun:11001,11001,Aartselaar,Aartselaar,Aartselaar,Flanders,Antwerp,"
            "Arrondissement Antwerpen,POP,Population,count,2021,110,A,2026-09-05T10:00:00+00:00",
            "be:mun:11001,11001,Aartselaar,Aartselaar,Aartselaar,Flanders,Antwerp,"
            "Arrondissement Antwerpen,POP,Population,count,2020,100,A,2026-09-01T10:00:00+00:00",
            # A derived indicator: no fetched_at at all.
            "be:mun:11001,11001,Aartselaar,Aartselaar,Aartselaar,Flanders,Antwerp,"
            "Arrondissement Antwerpen,POP_PCT,Population percentile,percent,2021,50,derived,",
        ],
    )
    latest_csv = tmp_path / "latest.csv"
    _write(latest_csv, LATEST_HEADER, [])
    national_csv = tmp_path / "national.csv"
    national_csv.write_text(
        "indicator_code,name,period,value,obs_status,unit,source_agency,fetched_at\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "public"
    export_site_payloads(
        db_path, history_csv, latest_csv, national_csv, out_dir, "b1", "pass", sections_config=None
    )

    import json

    payload = json.loads((out_dir / "communes" / "11001.json").read_text())
    assert payload["indicators"]["POP"]["updated"] == "2026-09-05"
    assert "updated" not in payload["indicators"]["POP_PCT"]


def test_derived_indicators_get_a_cross_section_payload(tmp_path):
    """A derived indicator must be mappable, not just visible one commune at a
    time.

    `indicators/{id}.json` is sliced from communes_export.csv, which holds only
    STORED indicators -- the derived ones are computed on the way into the
    history export. So until the backfill existed, the thirteen derived
    indicators (income per return, unemployment rate, the shares) had no
    cross-commune file at all: perfectly visible on a commune's own page and
    impossible to draw on a map, which is precisely what they are for.
    """
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history = tmp_path / "history.csv"
    _write(
        history,
        HISTORY_HEADER,
        [
            # A stored indicator, present in BOTH exports.
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2024", "16000"
            ),
            _history_row(
                "11002", "be:mun:11002", "Antwerpen", "POP", "Population", "count", "2024", "530000"
            ),
            # A derived one, present only in the history export.
            _history_row(
                "11001",
                "be:mun:11001",
                "Aartselaar",
                "AVG_INC",
                "Average income",
                "eur",
                "2022",
                "40000",
                "derived",
            ),
            _history_row(
                "11001",
                "be:mun:11001",
                "Aartselaar",
                "AVG_INC",
                "Average income",
                "eur",
                "2023",
                "49360",
                "derived",
            ),
            _history_row(
                "11002",
                "be:mun:11002",
                "Antwerpen",
                "AVG_INC",
                "Average income",
                "eur",
                "2023",
                "35362",
                "derived",
            ),
        ],
    )

    latest = tmp_path / "latest.csv"
    _write(
        latest,
        LATEST_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2024", "16000"
            ),
            _history_row(
                "11002", "be:mun:11002", "Antwerpen", "POP", "Population", "count", "2024", "530000"
            ),
        ],
    )

    national = tmp_path / "national.csv"
    _write(national, HISTORY_HEADER, [])

    out_dir = tmp_path / "out"
    export_site_payloads(
        db_path=db_path,
        communes_history_csv=history,
        communes_latest_csv=latest,
        national_csv=national,
        out_dir=out_dir,
        build_id="test",
        validation_status="unknown",
        sections_config=None,
    )

    derived = json.loads((out_dir / "indicators" / "AVG_INC.json").read_text())
    # EVERY commune, not just the first -- the backfill must not stop once the
    # indicator exists.
    assert set(derived["communes"]) == {"11001", "11002"}
    # The latest period per commune, matching what communes_export.csv means by
    # "latest".
    assert derived["communes"]["11001"]["period"] == "2023"
    assert derived["communes"]["11001"]["value"] == 49360.0

    # The stored indicator is untouched by the backfill.
    stored = json.loads((out_dir / "indicators" / "POP.json").read_text())
    assert set(stored["communes"]) == {"11001", "11002"}


def test_indicator_index_lists_every_mappable_indicator(tmp_path):
    """The index is what lets a page offer every indicator without naming one.
    If an indicator has a payload but no index row, it simply cannot be
    reached from the interface."""
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history = tmp_path / "history.csv"
    _write(
        history,
        HISTORY_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2024", "16000"
            ),
            _history_row(
                "11001",
                "be:mun:11001",
                "Aartselaar",
                "AVG_INC",
                "Average income",
                "eur",
                "2023",
                "49360",
                "derived",
            ),
        ],
    )
    latest = tmp_path / "latest.csv"
    _write(
        latest,
        LATEST_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2024", "16000"
            )
        ],
    )
    national = tmp_path / "national.csv"
    _write(national, HISTORY_HEADER, [])

    out_dir = tmp_path / "out"
    export_site_payloads(
        db_path=db_path,
        communes_history_csv=history,
        communes_latest_csv=latest,
        national_csv=national,
        out_dir=out_dir,
        build_id="test",
        validation_status="unknown",
        sections_config=None,
    )

    index = json.loads((out_dir / "metadata" / "indicators.json").read_text())["indicators"]
    by_code = {row["indicator_code"]: row for row in index}
    assert set(by_code) == {"POP", "AVG_INC"}

    payloads = {p.stem for p in (out_dir / "indicators").glob("*.json")}
    assert set(by_code) == payloads, "index and payload files must not drift apart"

    assert by_code["POP"]["coverage"] == 1
    assert by_code["POP"]["unit"] == "count"
    assert by_code["AVG_INC"]["names"]["en"] == "Average income"


def test_a_withheld_cell_is_published_not_dropped(tmp_path):
    """The core of the defect.

    ONEM masks any count below 10 for privacy; the schema stores those with a
    NULL value and status 'suppressed'
    (migrations/001_core_schema.sql: CHECK (value IS NOT NULL OR status IN
    ('suppressed','na'))). Both readers here used to `continue` on the empty
    value before recording anything, so 1,044 such cells never reached
    public/data -- across 188 of the 565 communes. A figure the source
    deliberately withheld then looked exactly like one never collected, and
    local.html's attribution block promised the opposite in three languages.
    """
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history = tmp_path / "history.csv"
    _write(
        history,
        HISTORY_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2023", "12"
            ),
            # Withheld: empty value, status S.
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2024", "", "S"
            ),
            # A blank with no status to explain it is still nothing.
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2025", "", "A"
            ),
        ],
    )
    latest = tmp_path / "latest.csv"
    _write(
        latest,
        LATEST_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2024", "", "S"
            )
        ],
    )
    national = tmp_path / "national.csv"
    _write(national, HISTORY_HEADER, [])

    out_dir = tmp_path / "out"
    export_site_payloads(
        db_path=db_path,
        communes_history_csv=history,
        communes_latest_csv=latest,
        national_csv=national,
        out_dir=out_dir,
        build_id="test",
        validation_status="unknown",
        sections_config=None,
    )

    periods = json.loads((out_dir / "communes" / "11001.json").read_text())["indicators"]["PT"][
        "periods"
    ]
    assert periods["2023"] == {"value": 12.0, "status": "final"}
    assert periods["2024"] == {"value": None, "status": "suppressed"}
    assert "2025" not in periods, "a blank with no explaining status must still be skipped"

    # The cross-section carries it too, so a map can say why a commune is blank.
    cross = json.loads((out_dir / "indicators" / "PT.json").read_text())["communes"]
    assert cross["11001"] == {"value": None, "status": "suppressed", "period": "2024"}


def test_an_indicator_a_commune_has_only_withheld_values_for_still_appears(tmp_path):
    """36 (commune, indicator) pairs are in this position. Every one of them
    used to be absent from the payload entirely, so the indicator did not exist
    on that commune's page at all -- the reader could not learn that the source
    holds the figure and withholds it."""
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history = tmp_path / "history.csv"
    _write(
        history,
        HISTORY_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2024", "16000"
            ),
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2023", "", "S"
            ),
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2024", "", "S"
            ),
        ],
    )
    latest = tmp_path / "latest.csv"
    _write(
        latest,
        LATEST_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "POP", "Population", "count", "2024", "16000"
            )
        ],
    )
    national = tmp_path / "national.csv"
    _write(national, HISTORY_HEADER, [])

    out_dir = tmp_path / "out"
    export_site_payloads(
        db_path=db_path,
        communes_history_csv=history,
        communes_latest_csv=latest,
        national_csv=national,
        out_dir=out_dir,
        build_id="test",
        validation_status="unknown",
        sections_config=None,
    )

    entry = json.loads((out_dir / "communes" / "11001.json").read_text())["indicators"]["PT"]
    assert set(entry["periods"]) == {"2023", "2024"}
    assert all(cell["value"] is None for cell in entry["periods"].values())
    assert all(cell["status"] == "suppressed" for cell in entry["periods"].values())
    # No retrieval date: `updated` means "when the number you are looking at
    # was retrieved", and there is no number.
    assert "updated" not in entry


def test_a_comparison_is_never_drawn_from_a_withheld_period(tmp_path):
    """Was the single most likely way this change could ship a wrong answer.

    158 (commune, indicator) pairs have a SUPPRESSED latest period, and
    _attach_comparisons took sorted(periods)[-1] unconditionally. It would have
    matched the withheld period against a real province aggregate and printed
    a dash beside a figure of 4,120 -- a comparison of nothing with something.
    """
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history = tmp_path / "history.csv"
    _write(
        history,
        HISTORY_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2024", "11"
            ),
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2025", "", "S"
            ),
        ],
    )
    latest = tmp_path / "latest.csv"
    _write(
        latest,
        LATEST_HEADER,
        [
            _history_row(
                "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2024", "11"
            )
        ],
    )
    national = tmp_path / "national.csv"
    _write(national, HISTORY_HEADER, [])

    # Aggregates for BOTH years, so the wrong choice would silently succeed.
    aggregates = tmp_path / "aggregates.csv"
    aggregates.write_text(
        "geo_id,nis_code,level,name_en,name_fr,name_nl,indicator_code,"
        "indicator_name,unit,period,value,coverage_n,coverage_of,coverage_pct\n"
        "be:reg:02000,02000,region,Flanders,Flandre,Vlaanderen,PT,"
        "Part-time,count,2024,4120,1,1,100.0\n"
        "be:reg:02000,02000,region,Flanders,Flandre,Vlaanderen,PT,"
        "Part-time,count,2025,4200,1,1,100.0\n",
        encoding="utf-8",
    )

    out_dir = tmp_path / "out"
    export_site_payloads(
        db_path=db_path,
        communes_history_csv=history,
        communes_latest_csv=latest,
        national_csv=national,
        out_dir=out_dir,
        build_id="test",
        validation_status="unknown",
        aggregates_csv=aggregates,
        sections_config=None,
    )

    entry = json.loads((out_dir / "communes" / "11001.json").read_text())["indicators"]["PT"]
    comparison = entry["comparison"]
    assert comparison["region"]["period"] == "2024", "compared at the withheld period"
    assert comparison["region"]["value"] == 4120


def test_an_unknown_status_letter_stops_the_build(tmp_path):
    """Rule 13. A new status letter arriving from a source is a schema change,
    and it must stop the build rather than render itself onto a published page
    as an unexplained capital letter a reader cannot look up. communes.html
    shipped exactly this bug once -- see the comment above its statusPill()."""
    import export_site_payloads as mod

    assert mod._status_word("A") == "final"
    assert mod._status_word("S") == "suppressed"
    assert mod._status_word("derived") == "derived"
    assert mod._status_word("") is None
    with pytest.raises(ValueError, match="unknown observation status"):
        mod._status_word("X")


def test_coverage_counts_numbers_not_keys(tmp_path):
    """A withheld commune is published now, so counting keys would tell a
    reader the map has data it cannot draw. The two are reported separately
    because "the source masked 152 communes" and "13 were never measured" are
    different facts about an indicator."""
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)

    history = tmp_path / "history.csv"
    rows = [
        _history_row(
            "11001", "be:mun:11001", "Aartselaar", "PT", "Part-time", "count", "2024", "12"
        ),
        _history_row(
            "11002", "be:mun:11002", "Antwerpen", "PT", "Part-time", "count", "2024", "", "S"
        ),
    ]
    _write(history, HISTORY_HEADER, rows)
    latest = tmp_path / "latest.csv"
    _write(latest, LATEST_HEADER, rows)
    national = tmp_path / "national.csv"
    _write(national, HISTORY_HEADER, [])

    out_dir = tmp_path / "out"
    export_site_payloads(
        db_path=db_path,
        communes_history_csv=history,
        communes_latest_csv=latest,
        national_csv=national,
        out_dir=out_dir,
        build_id="test",
        validation_status="unknown",
        sections_config=None,
    )

    row = next(
        r
        for r in json.loads((out_dir / "metadata" / "indicators.json").read_text())["indicators"]
        if r["indicator_code"] == "PT"
    )
    assert row["coverage"] == 1, "a withheld commune must not count as covered"
    assert row["suppressed"] == 1


# --- aggregates.json (Batch 7) ------------------------------------------------

AGGREGATES_HEADER = (
    "geo_id,nis_code,level,name_en,name_fr,name_nl,indicator_code,indicator_name,"
    "unit,period,value,coverage_n,coverage_of,coverage_pct"
)


def _agg_row(geo_id, nis_code, level, name, code, name_ind, unit, period, value, n, of, pct):
    return (
        f"{geo_id},{nis_code},{level},{name},{name},{name},{code},{name_ind},{unit},"
        f"{period},{value},{n},{of},{pct}"
    )


def _base_export_kwargs(tmp_path, db_path, history_csv, latest_csv, national_csv, out_dir):
    return {
        "db_path": db_path,
        "communes_history_csv": history_csv,
        "communes_latest_csv": latest_csv,
        "national_csv": national_csv,
        "out_dir": out_dir,
        "build_id": "test",
        "validation_status": "unknown",
        "sections_config": None,
    }


def test_aggregates_json_has_no_arrondissement(tmp_path):
    """docs/features/comparison.md's own comparison set stops at province --
    arrondissement is computed in the CSV (Block L) but was never meant to
    reach a reader, and micro.html's territorial comparison would otherwise
    have an extra, undocumented level to handle."""
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)
    history = tmp_path / "history.csv"
    _write(history, HISTORY_HEADER, [])
    latest = tmp_path / "latest.csv"
    _write(latest, LATEST_HEADER, [])
    national = tmp_path / "national.csv"
    _write(national, HISTORY_HEADER, [])

    aggregates = tmp_path / "aggregates.csv"
    _write(
        aggregates,
        AGGREGATES_HEADER,
        [
            _agg_row(
                "be:country",
                "01000",
                "country",
                "Belgium",
                "AVG_INC",
                "Average income",
                "EUR",
                "2023",
                "27000",
                565,
                565,
                100.0,
            ),
            _agg_row(
                "be:reg:02000",
                "02000",
                "region",
                "Flanders",
                "AVG_INC",
                "Average income",
                "EUR",
                "2023",
                "28000",
                300,
                300,
                100.0,
            ),
            _agg_row(
                "be:prov:10000",
                "10000",
                "province",
                "Antwerp",
                "AVG_INC",
                "Average income",
                "EUR",
                "2023",
                "29000",
                69,
                69,
                100.0,
            ),
            _agg_row(
                "be:arr:11000",
                "11000",
                "arrondissement",
                "Arr Antwerpen",
                "AVG_INC",
                "Average income",
                "EUR",
                "2023",
                "30000",
                28,
                28,
                100.0,
            ),
        ],
    )

    out_dir = tmp_path / "out"
    export_site_payloads(
        **_base_export_kwargs(tmp_path, db_path, history, latest, national, out_dir),
        aggregates_csv=aggregates,
    )

    payload = json.loads((out_dir / "aggregates.json").read_text(encoding="utf-8"))
    assert payload["levels"] == ["country", "region", "province"]
    geos = payload["indicators"]["AVG_INC"]
    assert set(geos) == {"be:country", "be:reg:02000", "be:prov:10000"}
    assert "be:arr:11000" not in geos


def test_aggregates_json_values_and_coverage_match_the_csv_row(tmp_path):
    """One hand-checked value at each of the three published levels, equal to
    the CSV row it was reshaped from -- the same "reshape, never recompute"
    guarantee every other payload in this module gives."""
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)
    history = tmp_path / "history.csv"
    _write(history, HISTORY_HEADER, [])
    latest = tmp_path / "latest.csv"
    _write(latest, LATEST_HEADER, [])
    national = tmp_path / "national.csv"
    _write(national, HISTORY_HEADER, [])

    aggregates = tmp_path / "aggregates.csv"
    _write(
        aggregates,
        AGGREGATES_HEADER,
        [
            _agg_row(
                "be:country",
                "01000",
                "country",
                "Belgium",
                "AVG_INC",
                "Average income",
                "EUR",
                "2023",
                "27453.1",
                565,
                565,
                100.0,
            ),
            _agg_row(
                "be:reg:02000",
                "02000",
                "region",
                "Flanders",
                "AVG_INC",
                "Average income",
                "EUR",
                "2023",
                "28901.4",
                300,
                300,
                100.0,
            ),
            _agg_row(
                "be:prov:10000",
                "10000",
                "province",
                "Antwerp",
                "AVG_INC",
                "Average income",
                "EUR",
                "2023",
                "29876.2",
                69,
                69,
                100.0,
            ),
        ],
    )

    out_dir = tmp_path / "out"
    export_site_payloads(
        **_base_export_kwargs(tmp_path, db_path, history, latest, national, out_dir),
        aggregates_csv=aggregates,
    )

    geos = json.loads((out_dir / "aggregates.json").read_text(encoding="utf-8"))["indicators"][
        "AVG_INC"
    ]
    assert geos["be:country"]["periods"]["2023"] == {
        "value": 27453.1,
        "coverage": {"n": 565, "of": 565, "pct": 100.0},
    }
    assert geos["be:reg:02000"]["periods"]["2023"]["value"] == 28901.4
    assert geos["be:prov:10000"]["periods"]["2023"]["value"] == 29876.2
    assert geos["be:prov:10000"]["level"] == "province"
    assert geos["be:prov:10000"]["nis_code"] == "10000"


def test_aggregates_json_is_byte_identical_across_two_runs(tmp_path):
    """Rule 35: identical inputs must keep producing byte-identical output."""
    db_path = tmp_path / "db.sqlite"
    _geo_db(db_path)
    history = tmp_path / "history.csv"
    _write(history, HISTORY_HEADER, [])
    latest = tmp_path / "latest.csv"
    _write(latest, LATEST_HEADER, [])
    national = tmp_path / "national.csv"
    _write(national, HISTORY_HEADER, [])

    aggregates = tmp_path / "aggregates.csv"
    _write(
        aggregates,
        AGGREGATES_HEADER,
        [
            _agg_row(
                "be:country",
                "01000",
                "country",
                "Belgium",
                "AVG_INC",
                "Average income",
                "EUR",
                "2023",
                "27453.1",
                565,
                565,
                100.0,
            ),
            _agg_row(
                "be:prov:10000",
                "10000",
                "province",
                "Antwerp",
                "AVG_INC",
                "Average income",
                "EUR",
                "2023",
                "29876.2",
                69,
                69,
                100.0,
            ),
        ],
    )

    out_a = tmp_path / "out_a"
    out_b = tmp_path / "out_b"
    export_site_payloads(
        **_base_export_kwargs(tmp_path, db_path, history, latest, national, out_a),
        aggregates_csv=aggregates,
    )
    export_site_payloads(
        **_base_export_kwargs(tmp_path, db_path, history, latest, national, out_b),
        aggregates_csv=aggregates,
    )

    assert (out_a / "aggregates.json").read_bytes() == (out_b / "aggregates.json").read_bytes()


# --- _check_micro_sections (Batch 7) ------------------------------------------


def test_check_micro_sections_refuses_an_unknown_code():
    import export_site_payloads as mod

    layout = {
        "kpis": ["REAL_ONE"],
        "key_list": [],
        "tiles": {},
        "comparison": {"indicators": ["MADE_UP_CODE"]},
        "map": {},
        "history": {},
    }
    with pytest.raises(ValueError, match="MADE_UP_CODE"):
        mod._check_micro_sections(layout, {"REAL_ONE"})


def test_check_micro_sections_accepts_a_code_from_either_universe():
    """`known` is the UNION of national.json's codes and aggregates.json's
    country-level codes -- a layout may freely mix the two, since that is
    exactly what config/micro_sections.yaml does (e.g. the ONEM-insured
    unemployment rate from national.json beside AVG_NET_TAXABLE_INCOME from
    aggregates.json)."""
    import export_site_payloads as mod

    layout = {
        "kpis": ["FROM_NATIONAL"],
        "key_list": ["FROM_AGGREGATES"],
        "tiles": {"indicators": []},
        "comparison": {"indicators": []},
        "map": {"indicators": []},
        "history": {},
    }
    mod._check_micro_sections(layout, {"FROM_NATIONAL", "FROM_AGGREGATES"})  # must not raise


def test_aggregates_payload_reshape_is_pure():
    """Unit-level check of `_aggregates_payload`, independent of the CSV
    round-trip above: the same (geo, indicator, period) input always produces
    the same nested shape, sorted at both levels."""
    import export_site_payloads as mod

    raw = {
        ("be:prov:20000", "X", "2022"): {
            "geo_id": "be:prov:20000",
            "nis_code": "20000",
            "level": "province",
            "name": {"en": "B", "fr": "B", "nl": "B"},
            "value": 2.0,
            "period": "2022",
            "coverage": {"n": 1, "of": 1, "pct": 100.0},
        },
        ("be:country", "X", "2022"): {
            "geo_id": "be:country",
            "nis_code": None,
            "level": "country",
            "name": {"en": "Belgium", "fr": "Belgique", "nl": "België"},
            "value": 1.0,
            "period": "2022",
            "coverage": {"n": 2, "of": 2, "pct": 100.0},
        },
        ("be:arr:30000", "X", "2022"): {
            "geo_id": "be:arr:30000",
            "nis_code": "30000",
            "level": "arrondissement",
            "name": {"en": "C", "fr": "C", "nl": "C"},
            "value": 3.0,
            "period": "2022",
            "coverage": {"n": 1, "of": 1, "pct": 100.0},
        },
    }
    payload = mod._aggregates_payload(raw)
    assert payload["levels"] == ["country", "region", "province"]
    assert list(payload["indicators"]["X"]) == ["be:country", "be:prov:20000"]
    assert payload["indicators"]["X"]["be:country"]["periods"]["2022"]["value"] == 1.0
