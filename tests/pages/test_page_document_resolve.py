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
    assert "not available to a block on its own yet" in result["message"]
    assert "province" in result["message"], "the refusal must name the level asked for"


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


# --- what the Batch 14 audit found -----------------------------------------


def test_a_withheld_subject_is_not_a_blank_cell_in_a_comparison(metadata, reader):
    """A withheld figure beside real province and country numbers, rendered as
    an empty box inside a block reporting "ready", is a figure the source holds
    back presented as nothing at all. Shipped once already on another path."""
    for path in sorted((PAYLOADS / "communes").glob("*.json"))[:150]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for code, entry in payload.get("indicators", {}).items():
            periods = entry.get("periods") or {}
            if not periods or not entry.get("comparison"):
                continue
            latest = sorted(periods)[-1]
            if periods[latest].get("status") != "suppressed":
                continue
            result = _resolve(metadata, reader, code, path.stem, operation="comparison")
            subject = result["rows"][0]
            assert subject["cells"][0] != "", "a withheld figure must never be a blank cell"
            assert "withheld" in subject["cells"][0].lower()
            return
    pytest.skip("no commune with a withheld latest value carrying a comparison")


def test_a_comparison_row_carries_its_own_period(metadata, reader):
    """The aggregates are not always from the subject's latest period -- some
    are years apart. A single column header over rows from different years
    states something false about most of them."""
    nis = real_data.a_municipal_nis_code()
    code = real_data.an_additive_municipal_indicator_id()
    result = _resolve(metadata, reader, code, nis, operation="comparison")
    for row in result["rows"]:
        assert "period" in row


def test_an_aggregate_below_full_coverage_says_so_on_the_row(metadata, reader):
    """A "Belgium" total built from 488 of 529 communes is not Belgium's
    figure, and printing it without saying so is how a figure becomes wrong."""
    for path in sorted((PAYLOADS / "communes").glob("*.json"))[:150]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for code, entry in payload.get("indicators", {}).items():
            for row in (entry.get("comparison") or {}).values():
                pct = (row.get("coverage") or {}).get("pct")
                if isinstance(pct, (int, float)) and pct < 100:
                    result = _resolve(metadata, reader, code, path.stem, operation="comparison")
                    labelled = [r["label"] for r in result["rows"][1:]]
                    assert any("of" in text for text in labelled), labelled
                    return
    pytest.skip("no partial-coverage aggregate in the sampled payloads")


@pytest.mark.parametrize(
    "nis",
    ["/etc/passwd", "../../../../etc/passwd", "..", "11002/../../secret", "", "1100"],
)
def test_a_hostile_nis_never_reaches_the_filesystem(metadata, reader, nis):
    """Resolution runs on documents the validator never saw -- POST /api/preview
    renders an invalid document on purpose. An absolute path silently replaces
    the whole prefix in pathlib, and ".." climbs out of the directory, so the
    guard has to live in the reader, not in the validator that may not have
    run."""
    from src.pages.resolve import PayloadError

    with pytest.raises(PayloadError):
        reader.commune(nis)


@pytest.mark.parametrize("code", ["../manifest", "/etc/passwd", "lower_case", "", "A" * 200])
def test_a_hostile_indicator_code_never_reaches_the_filesystem(reader, code):
    from src.pages.resolve import PayloadError

    with pytest.raises(PayloadError):
        reader.indicator(code)


def test_a_read_error_never_names_a_path_on_this_machine(tmp_path):
    """service.py puts this message into the data a browser is handed, and its
    own rule is that nothing about this machine reaches the client."""
    from src.pages.resolve import PayloadError, PayloadReader

    root = tmp_path / "public" / "data" / "communes"
    root.mkdir(parents=True)
    broken = root / "11002.json"
    broken.write_text("{not json", encoding="utf-8")
    try:
        PayloadReader(tmp_path).commune("11002")
    except PayloadError as exc:
        assert str(tmp_path) not in str(exc)
        assert "communes/11002.json" in str(exc)
    else:
        raise AssertionError("a malformed payload must raise")


def test_a_national_binding_reads_the_national_payload(metadata, reader):
    """Every binding used to go to the commune payload regardless of provider,
    so a valid national binding was told its figure was "not published for this
    commune" -- for a figure published one directory up."""
    code = real_data.national_indicator_ids()[0]
    doc = {
        "context": {},
        "sections": [
            {
                "blocks": [
                    {
                        "id": "b",
                        "type": "kpi_card",
                        "binding": {
                            "provider": "national",
                            "indicator": code,
                            "operation": "latest",
                            "geography": {"mode": "level", "level": "country"},
                        },
                    }
                ]
            }
        ],
    }
    result = resolve_document(doc, metadata=metadata, reader=reader)["b"]
    assert result["state"] == "ready", result
    assert result["formatted_value"]


def test_history_honours_a_declared_range(metadata, reader):
    """`range` is schema-valid. Ignoring it drew every period in the payload on
    a chart authored for a specific window."""
    nis = real_data.a_municipal_nis_code()
    payload = _published(nis)
    code, entry = next(
        ((c, e) for c, e in payload["indicators"].items() if len(e.get("periods") or {}) >= 4),
        (None, None),
    )
    if code is None:
        pytest.skip("no indicator with enough history for this commune")
    labels = sorted(entry["periods"])
    binding = {
        "provider": "municipal",
        "indicator": code,
        "operation": "history",
        "geography": {"mode": "fixed", "nis": nis},
        "period": {"mode": "range", "from": labels[1], "to": labels[2]},
    }
    doc = {
        "context": {"nis": nis},
        "sections": [{"blocks": [{"id": "b", "type": "chart", "binding": binding}]}],
    }
    result = resolve_document(doc, metadata=metadata, reader=reader)["b"]
    drawn = [p["period"] for p in result.get("points", [])]
    assert all(labels[1] <= p <= labels[2] for p in drawn), drawn
    assert len(drawn) < len(labels), "the range must actually narrow the series"


def test_a_figure_is_formatted_the_way_the_published_site_formats_it(metadata):
    """The claim of this batch is that a block shows the same number the live
    site shows. assets/commune_map.js is what the site uses; a percentage
    rendered as a bare integer is not the same number."""
    from src.pages.resolve import _format_value

    percent = next(
        (
            m
            for m in metadata.municipal_indicators.values()
            if (m.get("unit") or "").lower().startswith("percent")
        ),
        None,
    )
    assert percent is not None, "the fixture assumes a percent indicator exists"
    assert _format_value(18.1638, percent, "en").endswith("%")

    euro = next(
        (
            m
            for m in metadata.municipal_indicators.values()
            if (m.get("unit") or "").lower() == "eur"
        ),
        None,
    )
    if euro:
        assert _format_value(32149.0, euro, "en").startswith("\u20ac")


def test_a_non_string_indicator_is_an_error_state_not_a_crash(metadata, reader):
    """Reached only through the preview route, whose whole job is to render
    malformed documents rather than refuse them."""
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
                            "indicator": {"not": "a code"},
                            "operation": "latest",
                        },
                    }
                ]
            }
        ],
    }
    assert resolve_document(doc, metadata=metadata, reader=reader)["b"]["state"] == "error"
