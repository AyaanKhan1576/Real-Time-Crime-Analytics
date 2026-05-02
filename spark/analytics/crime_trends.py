"""
Module: crime_trends.py
Description: Computes historical crime trend aggregations with Spark.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _trend(df: DataFrame, run_id: str, trend_type: str, trend_key_expr: F.Column) -> DataFrame:
    """
    Compute one trend aggregation.

    Parameters
    ----------
    df : DataFrame
        Cleaned crime records.
    run_id : str
        Batch run identifier.
    trend_type : str
        Type of trend being computed.
    trend_key_expr : Column
        Expression used as the trend key.

    Returns
    -------
    DataFrame
        Aggregated trend rows.
    """
    return (
        df.withColumn("trend_key", trend_key_expr.cast("string"))
        .where(F.col("trend_key").isNotNull() & (F.length(F.trim(F.col("trend_key"))) > 0))
        .groupBy("trend_key")
        .agg(F.count(F.lit(1)).cast("long").alias("crime_count"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("trend_type", F.lit(trend_type))
        .select("run_id", "trend_type", "trend_key", "crime_count")
    )


def compute_crime_trends(cleaned_crimes: DataFrame, run_id: str) -> DataFrame:
    """
    Compute yearly, monthly, day-of-week, and hourly crime trends.

    Parameters
    ----------
    cleaned_crimes : DataFrame
        Cleaned crime records.
    run_id : str
        Batch run identifier.

    Returns
    -------
    DataFrame
        Unioned trend rows ready for crime_trends_temp.
    """
    yearly = _trend(cleaned_crimes, run_id, "year", F.col("year"))
    monthly = _trend(
        cleaned_crimes,
        run_id,
        "month",
        F.date_format(F.col("event_timestamp"), "yyyy-MM"),
    )
    day_of_week = _trend(cleaned_crimes, run_id, "day_of_week", F.col("day_of_week"))
    hourly = _trend(
        cleaned_crimes,
        run_id,
        "hour",
        F.when(F.col("hour").isNotNull(), F.format_string("%02d", F.col("hour"))),
    )

    return yearly.unionByName(monthly).unionByName(day_of_week).unionByName(hourly)
