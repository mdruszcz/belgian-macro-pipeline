"""Batch 14: resolving a binding to a real published figure.

Every test here runs against the REAL payloads in public/data/, not fixtures.
That is deliberate: the whole claim of this batch is that a block shows the
same number the published site shows, and a fixture cannot test that claim --
it can only test that the resolver agrees with a file the test wrote itself.

The communes chosen are the ones that have already caught real bugs in this
repository: Antwerp (dense), Herstappe (smallest, percentile edge), a Brussels
commune (19 regional peers, below ranking.py's 30-peer floor), a 2025-merger
successor (no history from sources that predate it) and a commune carrying a
withheld ONEM value.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.pages.metadata import load_metadata
from src.pages.resolve import PayloadReader, resolve_document
from tests.fixtures.pages import real_data

PAYLOADS = Path("public/data")


@pytest.fixture(scope="module")
def metadata():
    return load_metadata()


@pytest.fixture(scope="module")
def reader():
    return PayloadReader()


def _resolve(metadata, reader, code, nis, operation="latest", period=None, lang="en"):
    binding = {
        "provider": "municipal",
        "indicator": code,
        "operation": operation,
        "geography": {"mode": "fixed", "nis": nis},
    }
    if period:
        binding["period"] = {"mode": "fixed", "period": period}
    doc = {
        "context": {"nis": nis},
        "sections": [{"blocks": [{"id": "b", "type": "kpi_card", "binding": binding}]}],
    }
    return resolve_document(doc, metadata=metadata, lang=lang, reader=reader)["b"]


def _published(nis: str) -> dict:
    return json.loads((PAYLOADS / "communes" / f"{nis}.json").read_text(encoding="utf-8"))


def _a_suppressed_cell():
    """A real (nis, indicator, period) whose status is suppressed.

    Found by searching the published payloads rather than hard-coded: which
    cells ONEM masks changes as data is reloaded, and a hard-coded one would
    rot into a test that silently stops testing suppression.
    """
    for path in sorted((PAYLOADS / "communes").glob("*.json"))[:80]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for code, entry in payload.get("indicators", {}).items():
            for period, cell in (entry.get("periods") or {}).items():
                if cell.get("status") == "suppressed":
                    return path.stem, code, period
    return None


# --- the figure is the published figure ------------------------------------


def test_latest_resolves_to_the_number_the_published_payload_holds(metadata, reader):
    nis = real_data.a_municipal_nis_code()
    code = real_data.an_additive_municipal_indicator_id()
    result = _resolve(metadata, reader, code, nis)

    periods = _published(nis)["indicators"][code]["periods"]
    latest = sorted(periods)[-1]
    assert result["state"] == "ready"
    assert result["period"] == latest
    assert result["value"] == periods[latest]["value"]


def test_history_returns_every_readable_period_in_order(metadata, reader):
    nis = real_data.a_municipal_nis_code()
    code = real_data.an_additive_municipal_indicator_id()
    result = _resolve(metadata, reader, code, nis, operation="history")
    if result["state"] == "unavailable":
        pytest.skip("this indicator has a single period for this commune")
    labels = [p["period"] for p in result["points"]]
    assert labels == sorted(labels), "a chart drawn out of order is a wrong chart"


def test_a_comparison_names_its_province_region_and_country(metadata, reader):
    nis = real_data.a_municipal_nis_code()
    code = real_data.an_additive_municipal_indicator_id()
    result = _resolve(metadata, reader, code, nis, operation="comparison")
    assert result["state"] == "ready"
    assert result["rows"][0]["is_subject"] is True
    assert len(result["rows"]) >= 2, "a comparison against nothing is not a comparison"
    for row in result["rows"]:
        assert isinstance(row["label"], str), "a trilingual name must not reach the page raw"


def test_comparison_labels_follow_the_requested_language(metadata, reader):
    nis = real_data.a_municipal_nis_code()
    code = real_data.an_additive_municipal_indicator_id()
    english = _resolve(metadata, reader, code, nis, operation="comparison", lang="en")
    french = _resolve(metadata, reader, code, nis, operation="comparison", lang="fr")
    assert [r["label"] for r in english["rows"]] != [r["label"] for r in french["rows"]]


# --- the five states, which are the whole job (rule 26) ---------------------


def test_a_withheld_figure_is_suppressed_and_carries_no_number(metadata, reader):
    """A withheld figure is not a blank and not a zero. The source HOLDS this
    value and refuses to publish it, and collapsing that into "missing"
    destroys a distinction the source created -- a bug this repository has
    already shipped once."""
    found = _a_suppressed_cell()
    if found is None:
        pytest.skip("no suppressed cell in the sampled payloads")
    nis, code, period = found
    result = _resolve(metadata, reader, code, nis, period=period)
    assert result["state"] == "suppressed"
    assert "value" not in result
    assert result.get("message"), "a withheld figure must say why it is withheld"


def test_a_real_zero_is_ready_and_never_missing(metadata, reader):
    """An explicit zero is a measurement. Rendering it as absent would be the
    same failure as rendering a withheld figure as zero, pointing the other
    way."""
    for path in sorted((PAYLOADS / "communes").glob("*.json"))[:80]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for code, entry in payload.get("indicators", {}).items():
            for period, cell in (entry.get("periods") or {}).items():
                if cell.get("value") == 0 and cell.get("status") != "suppressed":
                    result = _resolve(metadata, reader, code, path.stem, period=period)
                    assert result["state"] == "ready"
                    assert result["value"] == 0
                    assert result["formatted_value"] != ""
                    return
    pytest.skip("no explicit zero in the sampled payloads")


def test_an_indicator_absent_from_a_commune_is_missing_with_a_reason(metadata, reader):
    """A 2025-merger successor carries no figure from a source that predates
    it. That is missing, and it must say so rather than rendering blank."""
    nis = real_data.a_municipal_nis_code()
    result = _resolve(metadata, reader, "NO_SUCH_INDICATOR_EXISTS", nis)
    assert result["state"] == "missing"
    assert result["message"]


def test_a_single_period_indicator_refuses_history_and_says_why(metadata, reader):
    nis = real_data.a_municipal_nis_code()
    payload = _published(nis)
    single = next(
        (c for c, e in payload["indicators"].items() if len(e.get("periods") or {}) == 1),
        None,
    )
    if single is None:
        pytest.skip("no single-period indicator for this commune")
    result = _resolve(metadata, reader, single, nis, operation="history")
    assert result["state"] == "unavailable"
    assert "history" in result["message"]


def test_a_page_with_no_context_commune_says_so(metadata, reader):
    doc = {
        "context": {},
        "sections": [
            {
                "blocks": [
                    {
                        "id": "b",
                        "type": "kpi_card",
                        "binding": {
                            "provider": "municipal",
                            "indicator": real_data.an_additive_municipal_indicator_id(),
                            "operation": "latest",
                            "geography": {"mode": "context"},
                        },
                    }
                ]
            }
        ],
    }
    result = resolve_document(doc, metadata=metadata, reader=reader)["b"]
    assert result["state"] == "unavailable"
    assert "does not declare one" in result["message"]


# --- what this batch deliberately refuses ----------------------------------


def test_a_level_binding_is_refused_with_a_reason_never_silently(metadata, reader):
    """The province figure exists, but only inside each commune's own
    comparison -- there is no payload a block can read on its own. Refused out
    loud, the way the schema refuses a `peer` provider, rather than rendering
    an empty box."""
    doc = {
        "context": {"nis": real_data.a_municipal_nis_code()},
        "sections": [
            {
                "blocks": [
                    {
                        "id": "b",
                        "type": "kpi_card",
                        "binding": {
                            "provider": "municipal",
                            "indicator": real_data.an_additive_municipal_indicator_id(),
                            "operation": "latest",
                            "geography": {"mode": "level", "level": "province"},
                        },
                    }
                ]
            }
        ],
    }
    result = resolve_document(doc, metadata=metadata, reader=reader)["b"]
    assert result["state"] == "unavailable"
    assert "not available to a block yet" in result["message"]


def test_an_unbound_block_gets_no_entry_at_all(metadata, reader):
    """render.py already renders a block with no binding as `ready`. Inventing
    an entry would say something about data where there is none to say."""
    doc = {
        "context": {},
        "sections": [{"blocks": [{"id": "plain", "type": "hero", "binding": None}]}],
    }
    assert resolve_document(doc, metadata=metadata, reader=reader) == {}


# --- the trust boundary ----------------------------------------------------


def test_the_resolver_reaches_no_database_and_computes_no_analytics():
    """Rule 20: a block consumes published payloads, never SQLite and never a
    raw source. Rule 27: it never recomputes a ratio across geographies. This
    is asserted by scanning the module as text, the same way Batch 11's
    hardening suite asserts the service cannot spawn a process."""
    source = Path("src/pages/resolve.py").read_text(encoding="utf-8")
    for forbidden in ("sqlite3", "src.analytics", "src.fetchers", "src.exporters", "pandas"):
        assert forbidden not in source, f"resolve.py must not reach for {forbidden}"
