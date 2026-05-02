"""
Module: arrest_rates.py
Description: Computes arrest rates by crime type, police district, and race.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def compute_arrest_rates(cleaned_crimes: DataFrame, cleaned_arrests: DataFrame, run_id: str) -> DataFrame:
    """
    Calculate arrest rates using crimes joined to arrests on case_number.

    Parameters
    ----------
    cleaned_crimes : DataFrame
        Cleaned crime records.
    cleaned_arrests : DataFrame
        Cleaned arrest records.
    run_id : str
        Batch run identifier.

    Returns
    -------
    DataFrame
        Arrest-rate rows ready for arrest_rates_temp.
    """
    crimes = (
        cleaned_crimes.select(
            F.upper(F.trim(F.col("case_number"))).alias("case_number"),
            F.coalesce(F.trim(F.col("primary_type")), F.lit("UNKNOWN")).alias("primary_type"),
            F.coalesce(F.trim(F.col("district").cast("string")), F.lit("UNKNOWN")).alias("district"),
        )
        .where(F.col("case_number").isNotNull() & (F.length(F.col("case_number")) > 0))
        .dropDuplicates(["case_number"])
    )

    arrests = (
        cleaned_arrests.select(
            F.upper(F.trim(F.col("case_number"))).alias("case_number"),
            F.coalesce(F.trim(F.col("race")), F.lit("UNKNOWN")).alias("race"),
        )
        .where(F.col("case_number").isNotNull() & (F.length(F.col("case_number")) > 0))
        .dropDuplicates(["case_number", "race"])
    )

    crime_denominator = crimes.groupBy("primary_type", "district").agg(
        F.countDistinct("case_number").cast("long").alias("total_crimes")
    )

    arrest_numerator = (
        crimes.join(arrests, "case_number", "inner")
        .groupBy("primary_type", "district", "race")
        .agg(F.countDistinct("case_number").cast("long").alias("total_arrests"))
    )

    detailed = (
        arrest_numerator.join(crime_denominator, ["primary_type", "district"], "left")
        .withColumn(
            "arrest_rate",
            F.when(F.col("total_crimes") > 0, F.col("total_arrests") / F.col("total_crimes")).otherwise(F.lit(0.0)),
        )
        .select("primary_type", "district", "race", "total_crimes", "total_arrests", "arrest_rate")
    )

    type_denominator = crimes.groupBy("primary_type").agg(
        F.countDistinct("case_number").cast("long").alias("total_crimes")
    )
    type_numerator = (
        crimes.join(arrests.select("case_number").dropDuplicates(["case_number"]), "case_number", "inner")
        .groupBy("primary_type")
        .agg(F.countDistinct("case_number").cast("long").alias("total_arrests"))
    )
    type_rates = (
        type_denominator.join(type_numerator, "primary_type", "left")
        .fillna({"total_arrests": 0})
        .withColumn(
            "arrest_rate",
            F.when(F.col("total_crimes") > 0, F.col("total_arrests") / F.col("total_crimes")).otherwise(F.lit(0.0)),
        )
    )

    top_window = Window.orderBy(F.desc("arrest_rate"), F.desc("total_crimes"), F.asc("primary_type"))
    top_crime_types = (
        type_rates.withColumn("rank", F.row_number().over(top_window))
        .where(F.col("rank") <= 10)
        .select(
            "primary_type",
            F.lit("ALL").alias("district"),
            F.lit("ALL").alias("race"),
            "total_crimes",
            "total_arrests",
            "arrest_rate",
        )
    )

    return (
        detailed.unionByName(top_crime_types)
        .withColumn("run_id", F.lit(run_id))
        .select(
            "run_id",
            "primary_type",
            "district",
            "race",
            F.col("total_crimes").cast("long").alias("total_crimes"),
            F.col("total_arrests").cast("long").alias("total_arrests"),
            F.col("arrest_rate").cast("double").alias("arrest_rate"),
        )
    )
