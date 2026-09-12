"""The change beside a figure -- a derived statistic, so hand-computed (rule 5).

The design puts a delta next to every number, and this repository's rule about
derived statistics is that each one has a test with an expected value worked
out by hand rather than by running the code and pasting what it said. Every
expected value below is arithmetic done in the docstring.

Two of these tests are not arithmetic at all. A PERCENTAGE MOVES IN POINTS,
and a PART-YEAR READING IS NOT COMPARABLE to a full one -- both are ways this
kind of badge misleads, and both were live: the first would have called a 0.6
point fall in unemployment "-6.7%", and the second reported Namur's burglary
rate as down 55.8% when the 2025 figure is a provisional part-year total
against a full 2024.
"""

from __future__ import annotations

import pytest

from src.pages.resolve import _delta


def entry(*pairs, status="final"):
    """A payload entry: (period, value) pairs, all with the same status."""
    return {
        "periods": {
            period: ({"value": value} if status == "final" else {"value": value, "status": status})
            for period, value in pairs
        }
    }


def test_a_count_moves_in_percent():
    """100 -> 110 is +10 on a base of 100, which is +10.0%."""
    result = _delta(entry(("2024", 100.0), ("2025", 110.0)), "2025", 110.0, {"unit": "count"}, "en")
    assert result["delta_text"] == "+10.0%"
    assert result["delta_period"] == "2024"


def test_a_percentage_moves_in_points_not_in_percent():
    """8.9% -> 8.3% is a fall of 0.6 POINTS.

    As a relative change it would read "-6.7%" (0.6 / 8.9), which is a
    different and much more alarming claim about the same two numbers.
    """
    result = _delta(entry(("2024", 8.9), ("2025", 8.3)), "2025", 8.3, {"unit": "percent"}, "en")
    assert result["delta_text"] == "−0.6 pt"


def test_a_fall_is_favourable_when_lower_is_better():
    """The direction comes from the indicator's own declaration, never from
    the sign: a falling unemployment rate is good news and a falling
    population is not, and the number cannot tell you which."""
    down = _delta(
        entry(("2024", 12.0), ("2025", 10.0)),
        "2025",
        10.0,
        {"unit": "percent", "direction": "lower_is_better"},
        "en",
    )
    assert down["direction"] == "favourable"
    up = _delta(
        entry(("2024", 10.0), ("2025", 12.0)),
        "2025",
        12.0,
        {"unit": "percent", "direction": "higher_is_better"},
        "en",
    )
    assert up["direction"] == "favourable"


def test_an_indicator_that_declares_no_direction_gets_a_grey_delta():
    """31 of this pipeline's 52 municipal indicators are `contextual` and 13
    declare nothing at all. A rising median house price is good news for a
    seller and bad for a buyer; the page states the change and not a verdict.
    """
    result = _delta(entry(("2024", 100.0), ("2025", 110.0)), "2025", 110.0, {"unit": "eur"}, "en")
    assert result["direction"] == "neutral"


def test_no_delta_across_a_provisional_reading():
    """Namur's burglary rate: 112.27 in 2024, 49.67 so far in 2025.

    That is not a 55.8% fall in burglaries, it is a part-year total against a
    full year -- and the payload says so, with status `provisional`.
    """
    entry_ = entry(("2024", 112.27))
    entry_["periods"]["2025"] = {"value": 49.67, "status": "provisional"}
    assert _delta(entry_, "2025", 49.67, {"unit": "per_10000_dwellings"}, "en") == {}


def test_a_derived_figure_is_comparable():
    """The 13 computed indicators carry status `derived`, average income among
    them. A "final only" rule silently dropped the delta from the page's
    second headline figure. 35,000 -> 37,749 is +2,749 on 35,000 = +7.9%."""
    result = _delta(
        entry(("2022", 35000.0), ("2023", 37749.0), status="derived"),
        "2023",
        37749.0,
        {"unit": "eur"},
        "en",
    )
    assert result["delta_text"] == "+7.9%"


def test_a_withheld_period_is_stepped_over_not_treated_as_a_gap():
    """ONEM masks any count below 10. A suppressed cell is a figure the source
    holds and will not publish -- not a zero and not a break in the series --
    so the comparison reaches back to the last period that has a number."""
    entry_ = entry(("2023", 200.0), ("2025", 220.0))
    entry_["periods"]["2024"] = {"value": None, "status": "suppressed"}
    result = _delta(entry_, "2025", 220.0, {"unit": "count"}, "en")
    assert result["delta_period"] == "2023"
    assert result["delta_text"] == "+10.0%"


@pytest.mark.parametrize(
    "entry_,why",
    [
        (entry(("2025", 100.0)), "a single reading has nothing to compare against"),
        (entry(("2024", 0.0), ("2025", 100.0)), "a previous value of zero has no relative change"),
    ],
)
def test_there_is_no_delta_when_there_is_nothing_honest_to_say(entry_, why):
    assert _delta(entry_, "2025", 100.0, {"unit": "count"}, "en") == {}, why


def test_the_delta_is_written_the_way_each_language_writes_numbers():
    """1000 -> 2234 is +1234 on 1000, which is +123.4% -- and Belgium writes
    the decimal mark two different ways, so a page that gets it wrong reads as
    foreign before a reader has taken in the number.

    A first draft of this test used 2234.5 and expected +123.5%. The code said
    +123.4%, and the code was right: 123.45 is not exactly representable and
    the nearest double is a hair below it, so it rounds down. Kept in the
    docstring because it is the exact reason this rule says to compute the
    expected value by hand -- the test caught the test.
    """
    figures = {
        lang: _delta(
            entry(("2024", 1000.0), ("2025", 2234.0)), "2025", 2234.0, {"unit": "count"}, lang
        )["delta_text"]
        for lang in ("en", "fr", "nl")
    }
    assert figures["en"] == "+123.4%"
    assert figures["fr"] == "+123,4%"
    assert figures["nl"] == "+123,4%"
