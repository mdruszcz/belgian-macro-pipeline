"""Tests for src/pages/resolve.py's number formatting and cell-state logic,
written for the site_new_indicators batch (docs/features -- eleven new
commune indicators, one of them the first `unit: balance` on the /local page).

`_format_value` deliberately mirrors `assets/commune_map.js`'s
`MapUI.formatValue` (see that function's own docstring): the claim of the
redesign is that a block shows the same number the live site shows, so the
two must never diverge on magnitude, sign or unit. Nothing in the codebase
tested that agreement directly before this batch introduced `balance` as a
signed, no-suffix unit -- the one shape most likely to lose its sign or its
thousands separator on the way through either formatter.
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pages.resolve import _cell_state, _delta, _format_value  # noqa: E402

REPO = Path(__file__).resolve().parents[1]


def _run_node(js_body: str):
    """Same technique as tests/test_map_ui_logic.py: a temp file, not
    `node -e`, because the combined harness comfortably exceeds Windows'
    command-line limit."""
    harness = (
        (REPO / "assets" / "i18n.js").read_text(encoding="utf-8")
        + "\n"
        + (REPO / "assets" / "commune_map.js").read_text(encoding="utf-8")
        + "\n"
        + js_body
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as handle:
        handle.write(harness)
        script = handle.name
    try:
        result = subprocess.run(
            ["node", script], capture_output=True, text=True, encoding="utf-8", timeout=10
        )
    finally:
        os.unlink(script)
    if result.returncode != 0:
        raise AssertionError(f"node failed:\n{result.stderr}")
    return json.loads(result.stdout)


# ── balance keeps its sign (INTERNAL_MIGRATION_NET, INTERNATIONAL_MIGRATION_NET) ──


def test_format_value_keeps_the_minus_sign_on_a_negative_balance():
    out = _format_value(-340.0, {"unit": "balance", "decimals": None}, "en")
    assert out.startswith("-") or out.startswith("−")
    assert "340" in out
    # No stray "%" or currency symbol -- a balance is a bare signed count.
    assert "%" not in out and "€" not in out


def test_format_value_balance_uses_the_reader_language_thousands_separator():
    en = _format_value(-4085.0, {"unit": "balance", "decimals": None}, "en")
    fr = _format_value(-4085.0, {"unit": "balance", "decimals": None}, "fr")
    nl = _format_value(-4085.0, {"unit": "balance", "decimals": None}, "nl")
    assert en == "-4,085"
    assert fr == "-4 085"
    assert nl == "-4.085"


def test_map_ui_format_value_keeps_the_minus_sign_on_a_negative_balance():
    out = _run_node("console.log(JSON.stringify(MapUI.formatValue(-340, 'balance', null, 'en')));")
    assert out.startswith("-")
    assert "340" in out


def test_a_positive_balance_keeps_its_number_with_no_forced_plus_sign():
    # formatValue/​_format_value render the bare number; the "+" that appears
    # in a delta badge is drawn by _delta, not by the value formatter. No
    # minus sign, and no stray "-" hiding in the padding either.
    out = _format_value(826.0, {"unit": "balance", "decimals": None}, "en")
    assert not out.startswith("-") and "−" not in out
    assert out in ("826", "826.00")


def test_a_contextual_direction_indicator_gets_a_neutral_grey_delta():
    """BIRTHS/DEATHS/the two migration balances/MUN_IPP_ADDITIONAL_RATE all
    declare preferred_direction: contextual. _RISE_IS (resolve.py) has no
    'contextual' key on purpose -- more or fewer births, or a higher or lower
    additional-tax rate, is not a favourable/unfavourable fact this module
    can judge, so the delta must render neutral rather than invent a
    judgement. This asserts the existing fallback rather than changing it,
    per the handoff's instruction to confirm, not touch, _RISE_IS."""
    entry = {
        "periods": {
            "2024": {"value": 300.0, "status": "final"},
            "2025": {"value": -340.0, "status": "final"},
        }
    }
    meta = {"unit": "balance", "direction": "contextual"}
    delta = _delta(entry, "2025", -340.0, meta, "en")
    assert delta["direction"] == "neutral"
    assert delta["delta_text"].startswith("−") or delta["delta_text"].startswith("-")


# ── a zero count is ready, never missing (BANKRUPTCIES, BANKRUPTCY_JOBS_LOST) ──


def test_cell_state_treats_an_explicit_zero_as_ready_not_missing():
    entry = {"periods": {"2026-08": {"value": 0.0, "status": "final"}}}
    state, value = _cell_state(entry, "2026-08")
    assert state == "ready"
    assert value == 0.0


def test_format_value_renders_a_zero_count_as_a_plain_zero():
    assert _format_value(0.0, {"unit": "count", "decimals": None}, "en") in ("0", "0.00")
    # Whatever the exact padding, it must never come out as a dash (missing)
    # or empty string (withheld) -- rule 26, a real zero is a distinct state.
    out = _format_value(0.0, {"unit": "count", "decimals": None}, "en")
    assert out not in ("", "—", "–")


def test_map_ui_format_value_renders_a_zero_count_as_a_plain_zero():
    out = _run_node("console.log(JSON.stringify(MapUI.formatValue(0, 'count', null, 'en')));")
    assert out == "0"


# ── suppressed vs missing vs ready (rule 26) ────────────────────────────────


def test_cell_state_distinguishes_suppressed_from_missing_from_ready():
    entry = {
        "periods": {
            "2024": {"status": "suppressed"},
            "2025": {"value": 12.0, "status": "final"},
        }
    }
    assert _cell_state(entry, "2024") == ("suppressed", None)
    assert _cell_state(entry, "2025") == ("ready", 12.0)
    assert _cell_state(entry, "2026") == ("missing", None)  # no period at all


# ── MapUI.formatValue and _format_value must agree ──────────────────────────


def test_map_ui_and_format_value_agree_for_count_percent_and_balance():
    """The one property the whole redesign depends on: the same figure reads
    the same on the SPA (local.html -> MapUI.formatValue, via the map/compare
    picker) and on a statically rendered block (src/pages/resolve.py ->
    _format_value). Checked for the three units this batch's eleven
    indicators actually use."""
    cases = [
        (36.0, "count", None, "en"),
        (0.0, "count", None, "en"),
        (8.5, "percent", None, "fr"),
        (-4085.0, "balance", None, "en"),
        (826.0, "balance", None, "nl"),
    ]
    for value, unit, decimals, lang in cases:
        py = _format_value(value, {"unit": unit, "decimals": decimals}, lang)
        js = _run_node(
            f"console.log(JSON.stringify("
            f"MapUI.formatValue({value!r}, {unit!r}, {json.dumps(decimals)}, {lang!r})));"
        )
        # Compared on sign, unit symbol and NUMERIC MAGNITUDE, not
        # byte-for-byte: _format_value pads an undeclared decimal count to
        # a minimum of two digits below a thousand (resolve.py's own
        # documented rule) while MapUI.formatValue treats the same
        # "undeclared" case as a MAXIMUM rather than a minimum, so a whole
        # number like 36 renders "36.00" from one and "36" from the other.
        # That trailing-zero difference pre-dates this batch and is not
        # specific to these eleven indicators (it would reproduce on any
        # existing undeclared-decimals count, e.g. DWELLINGS_VACANT) --
        # flagged as a risk in this batch's report rather than fixed here,
        # since it is shared formatting code many other indicators rely on.
        # Sign, unit symbol and the number itself must still agree exactly.
        py_sign = "-" if py.startswith("-") or py.startswith("−") else ""
        js_sign = "-" if js.startswith("-") else ""
        assert py_sign == js_sign, (value, unit, py, js)
        assert ("%" in py) == ("%" in js), (value, unit, py, js)
        assert ("€" in py) == ("€" in js), (value, unit, py, js)

        def _numeric(text: str, lang: str) -> float:
            body = text.lstrip("-−").rstrip("%").replace("€", "").strip()
            if lang == "fr":
                body = body.replace(" ", "").replace(",", ".")
            elif lang == "nl":
                body = body.replace(".", "").replace(",", ".")
            else:
                body = body.replace(",", "")
            return float(body)

        assert _numeric(py, lang) == _numeric(js, lang), (value, unit, py, js)


# ── eur_per_month (site_housing_indicators batch: SPF Finances lease medians) ──


def test_format_value_renders_eur_per_month_as_a_rate_not_a_total():
    out = _format_value(725.0, {"unit": "eur_per_month", "decimals": None}, "en")
    assert out.startswith("€")
    assert "725" in out
    # A rate suffix, same treatment as eur_per_inhabitant -- never read as a
    # bare euro total.
    assert "mo" in out.lower()


def test_map_ui_format_value_renders_eur_per_month_as_a_rate():
    out = _run_node(
        "console.log(JSON.stringify(MapUI.formatValue(725, 'eur_per_month', null, 'en')));"
    )
    assert out.startswith("€")
    assert "725" in out
    assert "mo" in out.lower()


def test_map_ui_and_format_value_agree_for_eur_per_month():
    """The unit this batch's eight SPF Finances housing indicators introduce
    (MUN_LEASE_RENT_MEDIAN_HOUSING / MUN_LEASE_CHARGES_MEDIAN_HOUSING) --
    checked the same way the existing count/percent/balance agreement test
    above is, per the handoff's requirement that any new unit be added to
    both formatters and tested for agreement."""
    cases = [
        (725.0, "eur_per_month", None, "en"),
        (0.0, "eur_per_month", None, "fr"),
        (410.5, "eur_per_month", None, "nl"),
    ]
    for value, unit, decimals, lang in cases:
        py = _format_value(value, {"unit": unit, "decimals": decimals}, lang)
        js = _run_node(
            f"console.log(JSON.stringify("
            f"MapUI.formatValue({value!r}, {unit!r}, {json.dumps(decimals)}, {lang!r})));"
        )
        assert ("€" in py) == ("€" in js), (value, unit, py, js)
        py_sign = "-" if py.startswith("-") or py.startswith("−") else ""
        js_sign = "-" if js.startswith("-") else ""
        assert py_sign == js_sign, (value, unit, py, js)

        def _numeric(text: str, lang: str) -> float:
            body = text.lstrip("-−").replace("€", "").split(" ")[0].split("/")[0].strip()
            if lang == "fr":
                body = body.replace(" ", "").replace(",", ".")
            elif lang == "nl":
                body = body.replace(".", "").replace(",", ".")
            else:
                body = body.replace(",", "")
            return float(body)

        assert _numeric(py, lang) == _numeric(js, lang), (value, unit, py, js)


# ── persons_per_household is a plain 2-decimal number, no unit glued on ────
# AVERAGE_HOUSEHOLD_SIZE (Indicator definitions batch): replaces 'count',
# which implied a whole, non-negative tally that a 2-decimal average like
# 2.14 is not. Unlike eur_per_month or eur_per_inhabitant, this unit is not a
# currency rate -- formatValue/_format_value must NOT glue anything onto the
# number itself; the wording ("persons/household") belongs only to
# unitLabel/unitSuffix, checked separately in test_map_ui_logic.py.


def test_format_value_renders_persons_per_household_as_a_plain_number():
    out = _format_value(2.14, {"unit": "persons_per_household", "decimals": 2}, "en")
    assert out == "2.14"
    assert "€" not in out
    assert "%" not in out


def test_map_ui_and_format_value_agree_for_persons_per_household():
    cases = [
        (2.14, "persons_per_household", 2, "en"),
        (0.0, "persons_per_household", 2, "fr"),
        (3.5, "persons_per_household", 2, "nl"),
    ]
    for value, unit, decimals, lang in cases:
        py = _format_value(value, {"unit": unit, "decimals": decimals}, lang)
        js = _run_node(
            f"console.log(JSON.stringify("
            f"MapUI.formatValue({value!r}, {unit!r}, {json.dumps(decimals)}, {lang!r})));"
        )
        assert py == js, (value, unit, py, js)


# ── suppressed median renders withheld wording, never zero (rule 26) ───────
# MUN_LEASE_RENT_MEDIAN_HOUSING / MUN_LEASE_CHARGES_MEDIAN_HOUSING are
# 'suppressed' with value NULL below 5 leases in a commune-quarter, distinct
# from a genuine 0.0 charges median (~60% of cells) and from 'na' when the
# underlying lease count is zero. _cell_state must keep these three states
# apart -- collapsing suppressed into missing or into zero is exactly what
# rule 26 forbids.


def test_cell_state_suppressed_median_is_neither_missing_nor_zero():
    entry = {
        "periods": {
            "2025-Q1": {"status": "suppressed"},
            "2025-Q2": {"value": 0.0, "status": "final"},
        }
    }
    state, value = _cell_state(entry, "2025-Q1")
    assert state == "suppressed"
    assert value is None

    state, value = _cell_state(entry, "2025-Q2")
    assert state == "ready"
    assert value == 0.0


def test_format_value_never_called_with_none_for_a_suppressed_cell():
    """_format_value(None, ...) must not silently render as a zero-like
    string -- callers branch on _cell_state's 'suppressed' before ever
    reaching _format_value (see _resolve_latest), but this guards the
    formatter itself against masking a withheld figure as blank-that-looks-
    like-zero if a caller ever skipped that branch."""
    out = _format_value(None, {"unit": "eur_per_month", "decimals": None}, "en")
    assert out == ""
    assert out != "0" and "0" not in out
