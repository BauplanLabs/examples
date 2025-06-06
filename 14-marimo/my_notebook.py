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
    # get data from the lakehouse in Python
    branch = 'main' # since we are just reading now, get the "prod" version of my tables
    table_1 ='taxi_fhvhv'
    columns_1 = [ 'pickup_datetime', 'PULocationID', 'trip_miles' ]
    filter_1 = "pickup_datetime >= '2022-12-21T00:00:00-05:00' AND pickup_datetime < '2023-01-01T00:00:00-05:00'"
    table_2 = 'taxi_zones'
    columns_2 = ['LocationID', 'Zone']

    # get first table
    table_1_df = pl.from_arrow(bpln_client.scan(
        table=table_1,
        ref=branch,
        columns=columns_1,
        filters=filter_1
    ))

    # get second table
    table_2_df = pl.from_arrow(bpln_client.scan(
        table=table_2,
        ref=branch,
        columns=columns_2
    ))
    return table_1_df, table_2_df


@app.cell
def _(table_1_df):
    table_1_df.head()
    return


@app.cell
def _(table_2_df):
    table_2_df.head()
    return


@app.function
def join_taxi_tables(table_1: pl.DataFrame, table_2: pl.DataFrame) -> pl.DataFrame:
    return table_1.join(table_2, left_on="PULocationID", right_on='LocationID', how="full", coalesce=True)


@app.cell
def _(table_1_df, table_2_df):
    parent_df = join_taxi_tables(table_1_df, table_2_df)
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


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
