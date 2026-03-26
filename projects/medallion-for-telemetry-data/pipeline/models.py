"""Bauplan medallion pipeline for telemetry data.

DAG:
    [bauplan.telemetry_bronze] -> [signal_clean] -> [signal_summary]

Bronze -> Silver: parse, clean, deduplicate raw sensor readings.
Silver -> Gold: aggregate per-sensor hourly statistics.
"""

import bauplan


@bauplan.model(
    columns=["dateTime", "signal", "value", "value_original"],
)
@bauplan.python("3.11", pip={"duckdb": "1.1.3"})
def signal_clean(
    bronze_data=bauplan.Model(
        "telemetry_bronze",
        columns=["dateTime", "sensors", "value"],
    ),
):
    """Bronze -> Silver: clean and deduplicate raw telemetry readings.

    - Column mapping: sensors -> signal
    - Type casting: value (string) -> value (float)
    - Null removal
    - Deduplication by (signal, dateTime), keeping highest value

    | dateTime            | signal   | value | value_original |
    |---------------------|----------|-------|----------------|
    | 2026-02-07 05:27:44 | sensor_5 | 65.94 | 65.94          |
    """
    import duckdb

    con = duckdb.connect()
    con.register("bronze_raw", bronze_data)

    result = con.execute(
        """
        WITH parsed AS (
            SELECT
                dateTime AT TIME ZONE 'UTC' AS dateTime,
                sensors AS signal,
                TRY_CAST(value AS DOUBLE) AS value,
                TRY_CAST(value AS DOUBLE) AS value_original
            FROM bronze_raw
        ),
        filtered AS (
            SELECT dateTime, signal, value, value_original
            FROM parsed
            WHERE value IS NOT NULL
              AND dateTime IS NOT NULL
              AND signal IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY signal, dateTime
                    ORDER BY value DESC
                ) AS rn
            FROM filtered
        )
        SELECT dateTime, signal, value, value_original
        FROM ranked
        WHERE rn = 1
        """,
    ).arrow()

    return result


@bauplan.model(
    columns=["hour", "signal", "reading_count", "avg_value", "min_value", "max_value"],
    materialization_strategy="REPLACE",
)
@bauplan.python("3.11", pip={"polars": "1.38.1"})
def signal_summary(
    data=bauplan.Model(
        "signal_clean",
        columns=["dateTime", "signal", "value"],
    ),
):
    """Silver -> Gold: hourly statistics per sensor.

    Aggregates clean readings into per-sensor, per-hour summaries
    with count, mean, min, and max values.

    | hour                | signal   | reading_count | avg_value | min_value | max_value |
    |---------------------|----------|---------------|-----------|-----------|-----------|
    | 2026-02-07 05:00:00 | sensor_5 | 12            | 54.3      | 21.1      | 89.7      |
    """
    import polars as pl

    df = pl.from_arrow(data)

    result = (
        df.with_columns(pl.col("dateTime").dt.truncate("1h").alias("hour"))
        .group_by("hour", "signal")
        .agg(
            pl.col("value").count().alias("reading_count"),
            pl.col("value").mean().round(2).alias("avg_value"),
            pl.col("value").min().alias("min_value"),
            pl.col("value").max().alias("max_value"),
        )
        .sort("hour", "signal")
    )

    return result.to_arrow()
