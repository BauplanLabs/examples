"""
This script collects Bauplan models, i.e. transformations that are run in Python mapping an
input table (data=bauplan.Model), to another table (a dataframe-like object we return).

Note that collecting models in a single file called models.py is not required, but we find it useful
to keep the pipeline code together.
"""

# Import bauplan to get the decorators available.
import bauplan


# This decorator tells Bauplan that this function
# has the model semantics - input: table,
# output: table. 

# The input is always an Arrow table,
# output can be an Arrow table, a Polars dataframe,
# a pandas dataframe or a list of dictionaries.
@bauplan.model()

# This decorator allows you to specify the Python
# version and any pip packages you need for this
# function.
# Remember that the environment for each function is entirely separated.
# E.g., different functions can run with different
# packages, different versions of the same packages,
# and/or even different versions of the Python interpreter.
@bauplan.python("3.12")
def trips_and_zones(
    trips=bauplan.Model(
        "taxi_fhvhv",
        # This function performs an S3 scan directly
        # in Python, so we can specify the columns
        # and the filter pushdown. By pushing the
        # filters down to S3, we make the system
        # considerably more performant.
        columns=[
            "pickup_datetime",
            "dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_miles",
            "trip_time",
            "base_passenger_fare",
            "tolls",
            "sales_tax",
            "tips",
        ],
        filter="pickup_datetime >= '2022-12-15T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'",
    ),
    zones=bauplan.Model(
        "taxi_zones",
    ),
):
    # The following code is PyArrow
    # https://arrow.apache.org/docs/python/index.html
    # Because Bauplan speaks Arrow natively, you
    # don't need to import PyArrow explicitly.

    # Join 'trips' with 'zones' on 'PULocationID'.
    pickup_location_table = trips.join(
        zones, "PULocationID", "LocationID"
    ).combine_chunks()
    return pickup_location_table


@bauplan.model(materialization_strategy="REPLACE")
# Polars is recommended for working with Arrow
# tables - it reads Arrow natively with zero-copy.
@bauplan.python("3.12", pip={"polars": "1.38.1"})
def normalized_taxi_trips(
    data=bauplan.Model(
        # This function takes the previous one 'trips_and_zones' as an input.
        # Functions are chained together to form a DAG by naming convention.
        "trips_and_zones",
    ),
):
    import polars as pl
    import math

    # Print some debug info - you will see every
    # print statement directly in your terminal.
    size_in_gb = round(data.nbytes / math.pow(1024, 3), 3)
    print(f"\nThis table is {size_in_gb} GB and has {data.num_rows} rows\n")

    # Convert data from Arrow to Polars (zero-copy).
    df = pl.from_arrow(data)
    # Filter by timestamp and trip_miles, and add a log-transformed column.
    df = (
        df.filter(
            pl.col("pickup_datetime")
            >= pl.lit("2022-01-01").str.to_datetime().dt.replace_time_zone("UTC")
        )
        .filter(pl.col("trip_miles") > 0.0)
        .filter(pl.col("trip_miles") < 200.0)
        .with_columns(pl.col("trip_miles").log(base=10).alias("log_trip_miles"))
    )

    # No need to convert back to Arrow - Bauplan
    # accepts Polars dataframes natively.
    return df.to_arrow()
