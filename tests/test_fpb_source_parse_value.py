from src.fetchers.fpb import FPBSource


def test_parse_value_none():
    assert FPBSource._parse_value(None) is None


def test_parse_value_numeric():
    assert FPBSource._parse_value(1.5) == 1.5
    assert FPBSource._parse_value(3) == 3.0


def test_parse_value_comma_decimal():
    assert FPBSource._parse_value("1,5") == 1.5


def test_parse_value_missing_markers():
    for raw in ("-.-", "—", "-", "...", ""):
        assert FPBSource._parse_value(raw) is None


def test_parse_value_unparseable_string():
    assert FPBSource._parse_value("n/a") is None


def test_parse_value_rounds_to_two_decimals():
    assert FPBSource._parse_value("1.23456") == 1.23
