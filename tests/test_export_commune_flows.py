"""Tests for scripts/export_commune_flows.py -- the buyer-origin flows exporter,
commune-flows PR 2 (docs/features/commune_flows.md, docs/decisions/0013).

Hand-computed expectations (CLAUDE.md rule 5) come straight from ADR 0013's own
"Worked example -- Boechout" and its Herstappe single-origin edge case, not typed
from nowhere (rule 36): both are reproduced here as the small fixture store this
test builds, so nothing is copied from the real committed store.

Covers: the five states (final with buckets, no_purchases_recorded, and the three
schema refusals -- missing store, missing geography name, mismatched bucket set),
byte-identical determinism across two runs of the same input (rule 35), and a
stale-file cleanup case (a commune file left over from a NIS no longer in the store).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.export_commune_flows import (  # noqa: E402
    FlowExportError,
    build_all,
    export,
    latest_store_path,
    load_geography_names,
    load_store,
)

# ADR 0013, "Worked example -- Boechout (NIS 11004), 2025, ParcelNature=TOTAL".
BOECHOUT = {
    "dest_geo_id": "be:mun:11004",
    "dest_nis": "11004",
    "parcels_number": 423,
    "state": "final",
    "denominator": "413.995833337",
    "coverage_pct": "97.9",
    "top_origins": [
        {"nis": "11004", "value": "151.15416667", "share_pct": "36.5"},
        {"nis": "11002", "value": "79.6", "share_pct": "19.2"},
        {"nis": "11029", "value": "46.858333333", "share_pct": "11.3"},
        {"nis": "11021", "value": "22.166666667", "share_pct": "5.4"},
        {"nis": "12021", "value": "17", "share_pct": "4.1"},
        {"nis": "11013", "value": "12.186666667", "share_pct": "2.9"},
        {"nis": "11039", "value": "10", "share_pct": "2.4"},
        {"nis": "41018", "value": "10", "share_pct": "2.4"},
    ],
    "buckets": {
        "same_commune": {"value": "151.15416667", "share_pct": "36.5"},
        "rest_of_arrondissement": {"value": "211.291666667", "share_pct": "51.0"},
        "rest_of_region": {"value": "50.5", "share_pct": "12.2"},
        "other_regions": {"value": "0.05", "share_pct": "0.0"},
        "abroad": {"value": "1", "share_pct": "0.2"},
        "origin_unknown": {"value": "0", "share_pct": "0.0"},
    },
}

# ADR 0013, single-origin edge case: Herstappe (73028), ParcelsNumber = 1, D = 1.
HERSTAPPE = {
    "dest_geo_id": "be:mun:73028",
    "dest_nis": "73028",
    "parcels_number": 1,
    "state": "final",
    "denominator": "1",
    "coverage_pct": "100.0",
    "top_origins": [{"nis": "73028", "value": "1", "share_pct": "100.0"}],
    "buckets": {
        "same_commune": {"value": "1", "share_pct": "100.0"},
        "rest_of_arrondissement": {"value": "0", "share_pct": "0.0"},
        "rest_of_region": {"value": "0", "share_pct": "0.0"},
        "other_regions": {"value": "0", "share_pct": "0.0"},
        "abroad": {"value": "0", "share_pct": "0.0"},
        "origin_unknown": {"value": "0", "share_pct": "0.0"},
    },
}

# A synthetic zero-purchase destination -- ADR 0013 decision 7's contingency case
# (not the real 2025 file, where Herstappe is no longer zero; this is a made-up
# small NIS purely to exercise the state, never asserted as a real commune's figure).
ZERO_PURCHASES = {
    "dest_geo_id": "be:mun:99999",
    "dest_nis": "99999",
    "parcels_number": 3,
    "state": "no_purchases_recorded",
}

NAMES = {
    "11004": {"en": "Boechout", "fr": "Boechout", "nl": "Boechout"},
    "11002": {"en": "Antwerp", "fr": "Anvers", "nl": "Antwerpen"},
    "11029": {"en": "Mortsel", "fr": "Mortsel", "nl": "Mortsel"},
    "11021": {"en": "Hove", "fr": "Hove", "nl": "Hove"},
    "12021": {"en": "Lier", "fr": "Lierre", "nl": "Lier"},
    "11013": {"en": "Edegem", "fr": "Edegem", "nl": "Edegem"},
    "11039": {"en": "Schilde", "fr": "Schilde", "nl": "Schilde"},
    "41018": {"en": "Geraardsbergen", "fr": "Grammont", "nl": "Geraardsbergen"},
    "73028": {"en": "Herstappe", "fr": "Herstappe", "nl": "Herstappe"},
    "99999": {"en": "Testville", "fr": "Testville", "nl": "Testville"},
}


def _geographies_document(names: dict) -> dict:
    return {
        "geographies": [
            {"nis_code": nis, "level": "municipality", "name": name} for nis, name in names.items()
        ]
    }


def _store_document(destinations: list[dict], *, period: str = "2025") -> dict:
    return {
        "schema_version": 1,
        "dataset": "buyer_origin",
        "source_uuid": "b90b50be-9dfc-11f0-99e9-00be432db085",
        "period": period,
        "provenance": {"version_length": 12345, "fetched_at": "2026-09-24T19:31:19+00:00"},
        "destinations": destinations,
    }


@pytest.fixture
def geographies_path(tmp_path) -> Path:
    path = tmp_path / "geographies.json"
    path.write_text(json.dumps(_geographies_document(NAMES)), encoding="utf-8")
    return path


@pytest.fixture
def flows_dir(tmp_path) -> Path:
    d = tmp_path / "flows"
    d.mkdir()
    return d


def _write_store(flows_dir: Path, document: dict, *, year: str = "2025") -> Path:
    path = flows_dir / f"buyer_origin_{year}.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


# --- hand-computed payload shape -------------------------------------------------


def test_boechout_payload_matches_the_adrs_worked_example(geographies_path):
    document = _store_document([BOECHOUT])
    names = load_geography_names(geographies_path)
    payloads = build_all(document, names)

    payload = payloads["11004"]
    assert payload["state"] == "final"
    assert payload["dest_name"] == {"en": "Boechout", "fr": "Boechout", "nl": "Boechout"}
    assert payload["denominator"] == "413.995833337"
    assert payload["coverage_pct"] == "97.9"

    assert [o["nis"] for o in payload["top_origins"]] == [
        "11004",
        "11002",
        "11029",
        "11021",
        "12021",
        "11013",
        "11039",
        "41018",
    ]
    # The tie-break: 11039 (Schilde) precedes 41018 (Geraardsbergen) at equal value 10,
    # NIS ascending (ADR decision 5).
    assert payload["top_origins"][6]["nis"] == "11039"
    assert payload["top_origins"][6]["value"] == "10"
    assert payload["top_origins"][7]["nis"] == "41018"
    assert payload["top_origins"][7]["value"] == "10"
    assert payload["top_origins"][6]["name"]["nl"] == "Schilde"
    assert payload["top_origins"][7]["name"]["fr"] == "Grammont"

    buckets = payload["buckets"]
    assert buckets["same_commune"] == {"value": "151.15416667", "share_pct": "36.5"}
    assert buckets["rest_of_arrondissement"] == {
        "value": "211.291666667",
        "share_pct": "51.0",
    }
    assert buckets["rest_of_region"] == {"value": "50.5", "share_pct": "12.2"}
    # 0.05 rounds to 0.0% but the exact value is still published (ADR: "the block
    # must not render it as nothing").
    assert buckets["other_regions"] == {"value": "0.05", "share_pct": "0.0"}
    assert buckets["abroad"] == {"value": "1", "share_pct": "0.2"}
    assert buckets["origin_unknown"] == {"value": "0", "share_pct": "0.0"}


def test_herstappe_single_origin_edge_case(geographies_path):
    document = _store_document([HERSTAPPE])
    names = load_geography_names(geographies_path)
    payload = build_all(document, names)["73028"]

    assert payload["denominator"] == "1"
    assert payload["coverage_pct"] == "100.0"
    assert len(payload["top_origins"]) == 1
    assert payload["top_origins"][0] == {
        "nis": "73028",
        "name": {"en": "Herstappe", "fr": "Herstappe", "nl": "Herstappe"},
        "value": "1",
        "share_pct": "100.0",
    }
    assert payload["buckets"]["same_commune"]["share_pct"] == "100.0"
    for bucket_id in (
        "rest_of_arrondissement",
        "rest_of_region",
        "other_regions",
        "abroad",
        "origin_unknown",
    ):
        assert payload["buckets"][bucket_id]["value"] == "0"


# --- the five states -------------------------------------------------------------


def test_zero_purchase_state_carries_no_shares(geographies_path):
    """ADR decision 7: no_purchases_recorded publishes no share at all, not 0% for
    six buckets. parcels_number is a measured zero-or-more figure, kept."""
    document = _store_document([ZERO_PURCHASES])
    names = load_geography_names(geographies_path)
    payload = build_all(document, names)["99999"]

    assert payload["state"] == "no_purchases_recorded"
    assert payload["parcels_number"] == 3
    assert "denominator" not in payload
    assert "buckets" not in payload
    assert "top_origins" not in payload


def test_missing_store_refuses(tmp_path, geographies_path):
    with pytest.raises(FlowExportError, match="no data/flows"):
        latest_store_path(tmp_path / "empty")


def test_unresolved_geography_name_refuses(tmp_path):
    """A destination or origin NIS the geography payload does not carry a name for
    is a schema surprise (CLAUDE.md rule 13) -- the store already resolved every
    NIS through resolve_geo() in PR 1, so a miss here means the two payloads
    drifted, not that the name is optional."""
    thin_names = {"11004": NAMES["11004"]}  # missing every origin's name
    geographies_path = tmp_path / "geographies.json"
    geographies_path.write_text(json.dumps(_geographies_document(thin_names)), encoding="utf-8")
    document = _store_document([BOECHOUT])
    names = load_geography_names(geographies_path)
    with pytest.raises(FlowExportError, match="11002"):
        build_all(document, names)


def test_mismatched_bucket_set_refuses(geographies_path):
    bad = dict(BOECHOUT)
    bad["buckets"] = {"same_commune": {"value": "1", "share_pct": "100.0"}}  # only one bucket
    document = _store_document([bad])
    names = load_geography_names(geographies_path)
    with pytest.raises(FlowExportError, match="bucket"):
        build_all(document, names)


def test_geography_name_missing_a_language_refuses(tmp_path):
    bad_names = {
        "geographies": [
            {
                "nis_code": "11004",
                "level": "municipality",
                "name": {"en": "Boechout", "fr": "Boechout"},
            },
        ]
    }
    path = tmp_path / "geographies.json"
    path.write_text(json.dumps(bad_names), encoding="utf-8")
    with pytest.raises(FlowExportError, match="trilingual"):
        load_geography_names(path)


# --- determinism and full export --------------------------------------------------


def test_export_is_byte_identical_across_two_runs(tmp_path, flows_dir, geographies_path):
    _write_store(flows_dir, _store_document([BOECHOUT, HERSTAPPE, ZERO_PURCHASES]))
    out_dir = tmp_path / "out"

    export(flows_dir, geographies_path, out_dir)
    first = {p.name: p.read_bytes() for p in sorted(out_dir.glob("*.json"))}

    export(flows_dir, geographies_path, out_dir)
    second = {p.name: p.read_bytes() for p in sorted(out_dir.glob("*.json"))}

    assert first == second
    assert set(first) == {"11004.json", "73028.json", "99999.json"}
    # LF-only, trailing newline (rule 35 -- Windows/Linux runner parity).
    for content in first.values():
        assert b"\r\n" not in content
        assert content.endswith(b"\n")


def test_export_removes_a_stale_commune_file(tmp_path, flows_dir, geographies_path):
    """A commune file left over from an earlier run (e.g. a NIS the current store
    no longer serves) must not survive a rebuild -- otherwise the published
    directory is not what this run actually produced."""
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    (out_dir / "00000.json").write_text("{}", encoding="utf-8")

    _write_store(flows_dir, _store_document([BOECHOUT]))
    export(flows_dir, geographies_path, out_dir)

    assert not (out_dir / "00000.json").exists()
    assert (out_dir / "11004.json").exists()


def test_latest_store_path_picks_the_highest_year(flows_dir):
    _write_store(flows_dir, _store_document([BOECHOUT], period="2024"), year="2024")
    _write_store(flows_dir, _store_document([BOECHOUT], period="2025"), year="2025")
    assert latest_store_path(flows_dir).name == "buyer_origin_2025.json"


def test_load_store_refuses_wrong_dataset(tmp_path):
    path = tmp_path / "buyer_origin_2025.json"
    path.write_text(
        json.dumps({"dataset": "owner_origin", "period": "2025", "destinations": [{}]}),
        encoding="utf-8",
    )
    with pytest.raises(FlowExportError, match="dataset"):
        load_store(path)
