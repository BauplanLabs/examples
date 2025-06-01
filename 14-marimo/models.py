import bauplan


@bauplan.model()
@bauplan.python('3.11', pip={'polars': '1.30.0', 'marimo': '0.13.14'})
def trips_and_zones(
    trips=bauplan.Model(
        'taxi_fhvhv',
        columns=['pickup_datetime', 'PULocationID', 'trip_miles'],
        filter="pickup_datetime >= '2022-01-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'"
    ),
    zones=bauplan.Model(
        'taxi_zones',
        columns=['LocationID', 'Zone']
    ),
):
    # import the necessary libraries
    import polars as pl
    # make sure to import the marimo function you want to use
    from your_notebook import join_taxi_tables

    # re-use marimo function - it accepts polars DataFrames as input
    # note that this is zero-copy, so the conversion is free
    return join_taxi_tables(pl.from_arrow(trips), pl.from_arrow(zones)).to_arrow() # we return Arrow


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'polars': '1.30.0', 'marimo': '0.13.14'})
def stats_by_taxi_zones(
    data=bauplan.Model('trips_and_zones')
):
    # import the necessary libraries
    import polars as pl
    # make sure to import the marimo function you want to use
    from your_notebook import compute_stats_by_zone
    # re-use marimo function - it accepts a polars DataFrame as input
    # note that this is zero-copy, so the conversion is free
    return compute_stats_by_zone(pl.from_arrow(data)).to_arrow() # we return Arrow
