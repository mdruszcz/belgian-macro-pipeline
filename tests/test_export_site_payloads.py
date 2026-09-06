"""Tests for the per-entity site payload exporter (Block J,
docs/features/site_payloads.md).

Each payload is a reshape of an existing bulk export, so what matters here is
that the reshape is faithful: no value dropped, none invented, and the
current-vs-historical distinction the source CSVs already enforce survives
the reshape unchanged.
"""

import csv
import sqlite3
import sys
from pathlib import Path

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
