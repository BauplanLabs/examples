"""Data quality expectations for the telemetry Silver table.

These checks validate the output of the Bronze -> Silver transformation:
- No null values in key columns (dateTime, signal, value)
"""

import bauplan
from bauplan.standard_expectations import expect_column_no_nulls


@bauplan.expectation()
@bauplan.python("3.11")
def test_no_null_datetime(
    data=bauplan.Model("signal_clean"),
):
    result = expect_column_no_nulls(data, "dateTime")
    assert result, "dateTime column must have no null values"
    return result


@bauplan.expectation()
@bauplan.python("3.11")
def test_no_null_signal(
    data=bauplan.Model("signal_clean"),
):
    result = expect_column_no_nulls(data, "signal")
    assert result, "signal column must have no null values"
    return result


@bauplan.expectation()
@bauplan.python("3.11")
def test_no_null_value(
    data=bauplan.Model("signal_clean"),
):
    result = expect_column_no_nulls(data, "value")
    assert result, "value column must have no null values"
    return result
