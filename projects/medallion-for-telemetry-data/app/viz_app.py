"""
Streamlit app to visualize the Gold telemetry table (signal_summary) produced by the
Bauplan medallion pipeline.

Queries `signal_summary` — hourly per-sensor aggregates — and shows
average readings over time, per-sensor statistics, and reading volume.

To run this app:
    uv run streamlit run app/viz_app.py
"""

import bauplan
import plotly.express as px
import polars as pl
import pyarrow as pa
import streamlit as st


TABLE_NAME = "bauplan.signal_summary"
BRANCH = "main"


def query_as_dataframe(client: bauplan.Client, sql: str) -> pa.Table | None:
    try:
        return client.query(query=sql, ref=BRANCH)
    except bauplan.exceptions.BauplanError as e:
        st.error(f"Query error: {e}")
        return None


def plot_avg_over_time(df: pl.DataFrame) -> None:
    """Line chart of hourly average values per sensor."""
    fig = px.line(
        df.sort("hour"),
        x="hour",
        y="avg_value",
        color="signal",
        title="Hourly Average Reading per Sensor",
        labels={"hour": "Hour", "avg_value": "Avg Value", "signal": "Sensor"},
        height=500,
    )
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig, use_container_width=True)


def plot_sensor_stats(df: pl.DataFrame) -> None:
    """Bar chart showing overall mean, min, max per sensor."""
    stats = (
        df.group_by("signal")
        .agg(
            pl.col("reading_count").sum().alias("total_readings"),
            pl.col("avg_value").mean().round(2).alias("mean"),
            pl.col("min_value").min().alias("min"),
            pl.col("max_value").max().alias("max"),
        )
        .sort("signal")
    )

    st.dataframe(stats, use_container_width=True)

    fig = px.bar(
        stats,
        x="signal",
        y="mean",
        error_y=stats["max"] - stats["mean"],
        error_y_minus=stats["mean"] - stats["min"],
        title="Mean Value per Sensor (with min/max range)",
        labels={"signal": "Sensor", "mean": "Mean Value"},
        height=450,
    )
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig, use_container_width=True)


def plot_reading_volume(df: pl.DataFrame) -> None:
    """Stacked bar chart of hourly reading counts by sensor."""
    fig = px.bar(
        df.sort("hour"),
        x="hour",
        y="reading_count",
        color="signal",
        title="Hourly Reading Volume by Sensor",
        labels={"hour": "Hour", "reading_count": "Readings", "signal": "Sensor"},
        height=450,
    )
    fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig, use_container_width=True)


def main():
    st.title("Telemetry Signal Dashboard")
    st.caption(f"Querying **{TABLE_NAME}** (Gold layer) on **{BRANCH}**")

    client = bauplan.Client()

    if not client.has_table(TABLE_NAME, ref=BRANCH):
        st.error(
            f"Table **{TABLE_NAME}** not found on **{BRANCH}**. "
            "Run the medallion pipeline first and merge to main before launching the app."
        )
        st.stop()

    arrow = query_as_dataframe(client, sql=f"SELECT * FROM {TABLE_NAME}")
    df = pl.from_arrow(arrow) if arrow is not None else None

    if df is not None and not df.is_empty():
        total_readings = df["reading_count"].sum()
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Readings", f"{total_readings:,}")
        col2.metric("Sensors", f"{df['signal'].n_unique()}")
        col3.metric(
            "Time Range",
            f"{df['hour'].min().strftime('%Y-%m-%d')} to {df['hour'].max().strftime('%Y-%m-%d')}",
        )

        if st.checkbox("Show raw data"):
            st.dataframe(df.head(200), use_container_width=True)

        tab1, tab2, tab3 = st.tabs(
            ["Avg Readings Over Time", "Per-Sensor Stats", "Reading Volume"]
        )
        with tab1:
            plot_avg_over_time(df)
        with tab2:
            plot_sensor_stats(df)
        with tab3:
            plot_reading_volume(df)
    else:
        st.error("No data found. Make sure the pipeline has run first.")


if __name__ == "__main__":
    main()
