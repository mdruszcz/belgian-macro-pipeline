from belgian_macro_db import DBnomicsFetcher


def test_rebase_to_2010_scales_to_100_average():
    # 2010 average is 150 -> divide every value by 150 and multiply by 100
    results = [
        {"period": "2010-Q1", "value": 100.0},
        {"period": "2010-Q2", "value": 200.0},
        {"period": "2011-Q1", "value": 300.0},
    ]
    rebased = DBnomicsFetcher._rebase_to_2010(results)
    assert rebased[0]["value"] == 66.67
    assert rebased[1]["value"] == 133.33
    assert rebased[2]["value"] == 200.0


def test_rebase_to_2010_no_2010_data_returns_unchanged():
    results = [{"period": "2011-Q1", "value": 200.0}]
    assert DBnomicsFetcher._rebase_to_2010(results) == results


def test_rebase_to_2010_zero_average_returns_unchanged():
    results = [{"period": "2010-Q1", "value": 0.0}]
    assert DBnomicsFetcher._rebase_to_2010(results) == results


def test_rebase_to_2010_empty_results():
    assert DBnomicsFetcher._rebase_to_2010([]) == []
