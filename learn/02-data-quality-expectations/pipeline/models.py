"""
This pipeline computes a table with the zones of NY ordered by how long it takes to get a taxi cab on average.
"""

import bauplan


@bauplan.model()
@bauplan.python("3.12", pip={"polars": "1.38.1"})
def normalized_taxi_trips(
    trips=bauplan.Model(
        "taxi_fhvhv",
        columns=[
            "PULocationID",
            "request_datetime",
            "on_scene_datetime",
            "pickup_datetime",
            "dropoff_datetime",
        ],
        filter="pickup_datetime >= '2022-12-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'",
    ),
    zones=bauplan.Model("taxi_zones"),
):
    import polars as pl
    import math

    size_in_gb = round(trips.nbytes / math.pow(1024, 3), 3)
    print(f"\nTaxi trips table is {size_in_gb} GB and has {trips.num_rows} rows\n")

    # Join trips with zones on PULocationID to get
    # Zone and Borough for each pickup location.
    trips_df = pl.from_arrow(trips)
    zones_df = pl.from_arrow(zones)
    result = trips_df.join(zones_df, left_on="PULocationID", right_on="LocationID")

    return result.to_arrow()


@bauplan.model()
@bauplan.python("3.12", pip={"polars": "1.38.1"})
def taxi_trip_waiting_times(
    data=bauplan.Model(
        "normalized_taxi_trips",
    ),
):
    import polars as pl

    df = pl.from_arrow(data)

    # Waiting time = minutes between request_datetime and on_scene_datetime.
    df = df.with_columns(
        (
            (
                pl.col("on_scene_datetime") - pl.col("request_datetime")
            ).dt.total_minutes()
        ).alias("waiting_time_minutes")
    )

    return df.to_arrow()


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.12", pip={"polars": "1.38.1"})
def zone_avg_waiting_times(
    taxi_trip_waiting_times=bauplan.Model("taxi_trip_waiting_times"),
):
    import polars as pl

    df = pl.from_arrow(taxi_trip_waiting_times)

    # Average waiting time per Borough/Zone, ordered by longest wait first.
    result = (
        df.group_by("Borough", "Zone")
        .agg(pl.col("waiting_time_minutes").mean().alias("avg_waiting_time"))
        .sort("avg_waiting_time", descending=True)
    )

    return result.to_arrow()
