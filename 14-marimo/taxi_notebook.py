import marimo

__generated_with = "0.13.14"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import polars as pl


@app.cell
def _():
    # import bauplan SDK as usual
    import bauplan

    bpln_client = bauplan.Client()
    return (bpln_client,)


@app.cell
def _(bpln_client):
    # get data from the data lake by using Bauplan Python SDK
    branch = 'main' # since we are just reading now, we get the tables directly from main: the "prod" version of the tables
    taxi_trips = 'taxi_fhvhv'
    columns_taxi_trips = ['pickup_datetime', 'PULocationID', 'trip_miles']
    filter_taxi_trips = "pickup_datetime >= '2022-12-21T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'"
    taxi_zones = 'taxi_zones'
    columns_taxi_zones = ['LocationID', 'Zone']

    # get first table with the data from taxi trips
    taxi_trips_df = pl.from_arrow(bpln_client.scan(
        table=taxi_trips,
        ref=branch,
        columns=columns_taxi_trips,
        filters=filter_taxi_trips
    ))

    # get second table with data from nyc zones and neighborhoods
    taxi_zones_df = pl.from_arrow(bpln_client.scan(
        table=taxi_zones,
        ref=branch,
        columns=columns_taxi_zones
    ))
    return taxi_trips_df, taxi_zones_df


@app.cell
def _(taxi_trips_df):
    taxi_trips_df.head()
    return


@app.cell
def _(taxi_zones_df):
    taxi_zones_df.head()
    return


@app.function
def join_taxi_tables(table_1: pl.DataFrame, table_2: pl.DataFrame) -> pl.DataFrame:
    return table_1.join(table_2, left_on="PULocationID", right_on='LocationID', how="full", coalesce=True)


@app.cell
def _(taxi_trips_df, taxi_zones_df):
    parent_df = join_taxi_tables(taxi_trips_df, taxi_zones_df)
    return (parent_df,)


@app.cell
def _(parent_df):
    parent_df.head()
    return


@app.function
def compute_stats_by_zone(df: pl.DataFrame) -> pl.DataFrame:
    from datetime import datetime, timezone
    # clean up the dataset by excluding certain rows
    time_filter = datetime(2022, 1, 1, tzinfo=timezone.utc)
    # filter df by timestamp, exclude rows with trip_miles = 0 and trip_miles > 200
    df = df.filter(
        (pl.col("pickup_datetime") >= pl.lit(time_filter))
        & (pl.col("trip_miles") > 0.0)
        & (pl.col("trip_miles") < 200.0)
    ).with_columns(
        # create a new columns with log-transformed trip_miles to better model skewed distribution
        pl.col("trip_miles").log10().alias("log_trip_miles")
    )    
    result = (
        df
        .select(["Zone", "log_trip_miles"])
        .group_by("Zone")
        .agg(
            pl.col("log_trip_miles").median().alias("log_trip_miles")
        )
    )
    # return a polars
    return result


@app.cell
def _(parent_df):
    child_df = compute_stats_by_zone(parent_df)
    return (child_df,)


@app.cell
def _(child_df):
    child_df.head()
    return


if __name__ == "__main__":
    app.run()
