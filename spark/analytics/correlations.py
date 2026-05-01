"""
Module: correlations.py
Description: Computes cross-dataset district-level correlations.
"""

from __future__ import annotations

import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _safe_corr(df: DataFrame, x_column: str, y_column: str) -> float:
    """
    Compute a finite Pearson correlation value.

    Parameters
    ----------
    df : DataFrame
        Metric DataFrame.
    x_column : str
        First metric column.
    y_column : str
        Second metric column.

    Returns
    -------
    float
        Correlation value, with invalid results mapped to 0.0.
    """
    value = df.stat.corr(x_column, y_column)
    if value is None or math.isnan(value) or math.isinf(value):
        return 0.0
    return float(value)


def compute_correlations(
    cleaned_crimes: DataFrame,
    cleaned_arrests: DataFrame,
    cleaned_violence: DataFrame,
    sex_offender_density: DataFrame,
    run_id: str,
) -> DataFrame:
    """
    Compute violence/arrest and offender/crime cross-dataset correlations.

    Parameters
    ----------
    cleaned_crimes : DataFrame
        Cleaned crime records.
    cleaned_arrests : DataFrame
        Cleaned arrest records.
    cleaned_violence : DataFrame
        Cleaned violence records.
    sex_offender_density : DataFrame
        Output DataFrame from sex offender density analysis.
    run_id : str
        Batch run identifier.

    Returns
    -------
    DataFrame
        Correlation rows ready for correlations_temp.
    """
    crimes = (
        cleaned_crimes.select(
            F.upper(F.trim(F.col("case_number"))).alias("case_number"),
            F.col("district").cast("string").alias("district"),
        )
        .where(F.col("case_number").isNotNull() & (F.length(F.col("case_number")) > 0))
        .dropDuplicates(["case_number"])
    )

    arrests = (
        cleaned_arrests.select(F.upper(F.trim(F.col("case_number"))).alias("case_number"))
        .where(F.col("case_number").isNotNull() & (F.length(F.col("case_number")) > 0))
        .dropDuplicates(["case_number"])
    )

    crime_by_district = crimes.groupBy("district").agg(
        F.countDistinct("case_number").cast("long").alias("total_crimes")
    )
    arrests_by_district = (
        crimes.join(arrests, "case_number", "inner")
        .groupBy("district")
        .agg(F.countDistinct("case_number").cast("long").alias("total_arrests"))
    )
    violence_by_district = cleaned_violence.groupBy(F.col("district").cast("string").alias("district")).agg(
        F.count(F.lit(1)).cast("long").alias("total_violence")
    )

    violence_arrest_metrics = (
        crime_by_district.join(arrests_by_district, "district", "left")
        .join(violence_by_district, "district", "left")
        .fillna({"total_arrests": 0, "total_violence": 0})
        .withColumn(
            "violence_rate",
            F.when(F.col("total_crimes") > 0, F.col("total_violence") / F.col("total_crimes")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "arrest_rate",
            F.when(F.col("total_crimes") > 0, F.col("total_arrests") / F.col("total_crimes")).otherwise(F.lit(0.0)),
        )
        .select("district", "violence_rate", "arrest_rate")
    )
    violence_corr = _safe_corr(violence_arrest_metrics, "violence_rate", "arrest_rate")

    violence_rows = (
        violence_arrest_metrics.withColumn("run_id", F.lit(run_id))
        .withColumn("correlation_name", F.lit("violence_rate_vs_arrest_rate_by_district"))
        .withColumn("grouping_key", F.col("district"))
        .withColumn("x_metric", F.lit("violence_rate"))
        .withColumn("y_metric", F.lit("arrest_rate"))
        .withColumn("x_value", F.col("violence_rate").cast("double"))
        .withColumn("y_value", F.col("arrest_rate").cast("double"))
        .withColumn("correlation_value", F.lit(violence_corr).cast("double"))
        .select("run_id", "correlation_name", "grouping_key", "x_metric", "y_metric", "x_value", "y_value", "correlation_value")
    )

    total_crime_count = crime_by_district.agg(F.sum("total_crimes").alias("total")).collect()[0]["total"] or 0
    offender_crime_metrics = (
        sex_offender_density.select(
            F.col("district").cast("string").alias("district"),
            F.col("offender_count").cast("double").alias("offender_density"),
        )
        .join(crime_by_district, "district", "left")
        .fillna({"total_crimes": 0})
        .withColumn(
            "crime_rate",
            F.when(F.lit(total_crime_count) > 0, F.col("total_crimes") / F.lit(float(total_crime_count))).otherwise(F.lit(0.0)),
        )
        .select("district", "offender_density", "crime_rate")
    )
    offender_corr = _safe_corr(offender_crime_metrics, "offender_density", "crime_rate")

    offender_rows = (
        offender_crime_metrics.withColumn("run_id", F.lit(run_id))
        .withColumn("correlation_name", F.lit("sex_offender_density_vs_crime_rate_by_district"))
        .withColumn("grouping_key", F.col("district"))
        .withColumn("x_metric", F.lit("offender_density"))
        .withColumn("y_metric", F.lit("crime_rate"))
        .withColumn("x_value", F.col("offender_density").cast("double"))
        .withColumn("y_value", F.col("crime_rate").cast("double"))
        .withColumn("correlation_value", F.lit(offender_corr).cast("double"))
        .select("run_id", "correlation_name", "grouping_key", "x_metric", "y_metric", "x_value", "y_value", "correlation_value")
    )

    return violence_rows.unionByName(offender_rows)
