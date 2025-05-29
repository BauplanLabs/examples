import bauplan


@bauplan.model()
@bauplan.python('3.11', pip={'pandas': '2.2.0', 'marimo': '0.13.14'})
def trips_and_zones(
    trips=bauplan.Model(
        'taxi_fhvhv',
        columns=['pickup_datetime', 'PULocationID', 'trip_miles'],
        filter="pickup_datetime >= '2022-12-01T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'"
    ),
    zones=bauplan.Model(
        'taxi_zones',
        columns=['LocationID', 'Zone']
    ),
):
     from your_notebook import join_taxi_tables
     import pandas as pd
     
    # we re-use the marimo function - it accepts two pandas DataFrames, so we convert the Arrow tables to pandas
     return join_taxi_tables(trips.to_pandas(), zones.to_pandas())


@bauplan.model()
@bauplan.python('3.11', pip={'pandas': '2.2.0', 'marimo': '0.13.14'})
def normalized_taxi_trips(
    data=bauplan.Model('trips_and_zones')
):
    from your_notebook import compute_stats_by_zone
    import pandas as pd
    # re-use marimo function - it accepts a pandas DataFrame as input so we convert the Arrow table to pandas
    return compute_stats_by_zone(data.to_pandas())
