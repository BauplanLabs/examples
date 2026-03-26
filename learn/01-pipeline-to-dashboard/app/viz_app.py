"""
Streamlit app to visualize the table 'normalized_taxi_trips' created with the Bauplan pipeline
in this project. This script uses the bauplan Python SDK to query data from a branch.

To run this app:
    uv run streamlit run app/viz_app.py -- --branch <YOUR_BRANCH_NAME>
"""

import streamlit as st
from argparse import ArgumentParser
from datetime import date
import plotly.express as px
import bauplan
import pyarrow as pa

# Polars is the recommended way to work with Arrow
# tables - it reads Arrow natively with zero-copy,
# avoiding the serialization overhead of pandas.
import polars as pl


@st.cache_data
def query_as_dataframe(_client: bauplan.Client, sql: str, branch: str) -> pa.Table | None:
    """
    Runs a query with bauplan and returns the result as an Arrow dataframe.
    """
    try:
        arrow_table = _client.query(query=sql, ref=branch)
        return arrow_table
    except bauplan.exceptions.BauplanError as e:
        print(f"Error: {e}")
        return None


def plot_trips_by_zone(df: pl.DataFrame) -> None:
    """
    Bar chart of trip count per pickup zone.
    """
    zone_counts = (
        df.group_by("Zone")
        .len()
        .rename({"len": "trip_count"})
        .sort("trip_count")
        .tail(30)
    )
    fig = px.bar(
        zone_counts,
        y="Zone",
        x="trip_count",
        orientation="h",
        title="Top 30 Pickup Zones by Trip Count",
        labels={"trip_count": "Number of Trips", "Zone": "Zone"},
        height=700,
    )
    fig.update_layout(
        showlegend=False,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)


def plot_trip_miles_distribution(df: pl.DataFrame) -> None:
    """
    Histogram of log-transformed trip miles.
    """
    fig = px.histogram(
        df,
        x="log_trip_miles",
        nbins=60,
        title="Distribution of Log₁₀(Trip Miles)",
        labels={"log_trip_miles": "log₁₀(trip miles)"},
    )
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig, use_container_width=True)


def plot_fare_vs_miles(df: pl.DataFrame) -> None:
    """
    Scatter plot of fare vs trip miles.
    """
    sample = df.sample(n=min(2000, len(df)), seed=42)
    fig = px.scatter(
        sample,
        x="trip_miles",
        y="base_passenger_fare",
        title="Base Fare vs Trip Miles (sampled)",
        labels={"trip_miles": "Trip Miles", "base_passenger_fare": "Base Fare ($)"},
        opacity=0.4,
    )
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig, use_container_width=True)


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "--branch", type=str, required=True, help="Branch name to query data from"
    )
    args = parser.parse_args()
    current_branch = args.branch
    print(f"Querying data from branch: {current_branch}")
    assert current_branch, (
        "Branch name is required. Use --branch <YOUR_BRANCH_NAME> to specify the branch."
    )

    table_name = "normalized_taxi_trips"

    st.title("NYC Taxi Trips — Quick Start Dashboard")
    st.caption(f"Querying table **{table_name}** on branch **{args.branch}**")

    selected_date = st.sidebar.date_input(
        "Pickup date",
        value=date(2022, 12, 15),
        min_value=date(2022, 12, 15),
        max_value=date(2022, 12, 31),
    )

    client = bauplan.Client()
    
    # Make sure the branch exists before querying.
    assert client.has_branch(current_branch), (
        f"Branch '{current_branch}' does not exist. Please check the branch name and try again."
    )
   
    # Make sure the table exists before querying.
    assert client.has_table(
        table_name, ref=current_branch
    ), f"Table '{table_name}' does not exist on "
       f"branch '{current_branch}'. Please check "
       f"the branch name and make sure to run the "
       f"pipeline first."
   
    table = client.get_table(table_name, ref=current_branch)
    num_records = table.records
    print(f"Table '{table_name}' has {num_records} records.")

    sql = (
        f"SELECT Zone, log_trip_miles, trip_miles, base_passenger_fare "
        f"FROM {table_name} "
        f"WHERE pickup_datetime >= '{selected_date}T00:00:00' "
        f"AND pickup_datetime < '{selected_date}T23:59:59'"
    )
    df = query_as_dataframe(_client=client, sql=sql, branch=current_branch)
    print("Query executed, got result. Converting to Polars DataFrame...")
    
    # Convert to Polars DataFrame for easier
    # manipulation and plotting. This is zero-copy
    # and very fast.
    df = pl.from_arrow(df) if df is not None else None

    if df is not None and not df.is_empty():
        st.metric("Total trips", f"{len(df):,}")

        if st.checkbox("Show raw data"):
            st.dataframe(df.head(100), use_container_width=True)

        tab1, tab2, tab3 = st.tabs(
            [
                "Trips by Zone",
                "Trip Miles Distribution",
                "Fare vs Miles",
            ]
        )

        with tab1:
            plot_trips_by_zone(df)
        with tab2:
            plot_trip_miles_distribution(df)
        with tab3:
            plot_fare_vs_miles(df)
    else:
        st.error("Error retrieving data. Please check your branch name and try again.")


if __name__ == "__main__":
    main()
