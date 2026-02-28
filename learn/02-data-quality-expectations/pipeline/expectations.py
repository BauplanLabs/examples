"""
This script collects bauplan expectations, that is statistical / quality checks that run
against Bauplan models to ensure the data is correct and avoid wasteful computation
or (even worse) non-compliant data artifacts.

This example showcases how you can use the standard expectations provided by bauplan to test your
data in the most efficient way possible.

Note that collecting all expectations in a single file is not required, but we find it useful
to keep the pipeline code clean and separate from the expectations code.
"""

import bauplan

# Import the standard expectations from the
# library to use them in the functions below.
from bauplan.standard_expectations import expect_column_no_nulls


# Expectations are identified by a special decorator.
@bauplan.expectation()

# You can use this to specify the python version used during execution.
@bauplan.python("3.11")
def test_null_values_on_scene_datetime(
    data=bauplan.Model(      
        # As input, we declare the Bauplan model that we want to check.
        "normalized_taxi_trips",
    ),
):
    # Just return the result of the standard
    # expectation (True if passed), passing to it
    # the input data, the column name to check, and the reference value.
    
    # Here is where we declare the columns we want to check.
    column_to_check = "on_scene_datetime"
    
    # Let's make sure there are no null values in the on_scene_datetime column!
    _is_expectation_correct = expect_column_no_nulls(data, column_to_check)

    # Assert the result of the test.
    assert _is_expectation_correct, (
        f"expectation test failed: we expected {column_to_check} to have no null values"
    )

    # Return a boolean.
    return _is_expectation_correct  
