import marimo

__generated_with = "0.13.14"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import pandas as pd
    import numpy as np


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
    table_1_df = bpln_client.scan(
        table=table_1,
        ref=branch,
        columns=columns_1,
        filters=filter_1
    ).to_pandas()

    # get second table
    table_2_df = bpln_client.scan(
        table=table_2,
        ref=branch,
        columns=columns_2
    ).to_pandas()
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
def join_taxi_tables(table_1: pd.DataFrame, table_2: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(table_1, table_2, left_on='PULocationID', right_on='LocationID')


@app.cell
def _(table_1_df, table_2_df):
    parent_df = join_taxi_tables(table_1_df, table_2_df)
    return (parent_df,)


@app.cell
def _(parent_df):
    parent_df.head()
    return


@app.function
def compute_stats_by_zone(df: pd.DataFrame) -> pd.DataFrame:
    # clean up the dataset by excluding certain rows
    time_filter = pd.to_datetime('2022-01-01')
    time_filter_utc = time_filter.tz_localize('UTC')
    # filter df by timestamp
    df = df[df['pickup_datetime'] >= time_filter_utc]
    # exclude rows with trip_miles = 0
    df = df[df['trip_miles'] > 0.0]
    # exclude rows with trip_miles > 200
    df = df[df['trip_miles'] < 200.0]
    # create a new columns with log-transformed trip_miles to better model skewed distribution
    df['log_trip_miles'] = np.log10(df['trip_miles'])

    # return a Pandas dataframe as the average log_miles per zone
    return df[['Zone', 'log_trip_miles']].groupby(['Zone']).median()


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
