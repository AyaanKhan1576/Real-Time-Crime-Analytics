"""
Module: sex_offender_analysis.py
Description: Computes registered sex offender density by derived police district.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def _normalized_block(column_name: str) -> F.Column:
    """
    Normalize block strings for approximate block-level joins.

    Parameters
    ----------
    column_name : str
        Source block column.

    Returns
    -------
    Column
        Normalized block expression.
    """
    return F.regexp_replace(F.upper(F.trim(F.col(column_name))), r"\s+", " ")


def compute_sex_offender_density(
    cleaned_sex_offenders: DataFrame,
    cleaned_police_stations: DataFrame,
    cleaned_crimes: DataFrame,
    run_id: str,
) -> DataFrame:
    """
    Compute offender density by district.

    The sex offender source has no district field. To preserve the district-level
    output contract, this function derives an approximate district from the most
    frequent crime district observed for the same normalized block. Offenders
    whose block cannot be matched are grouped under UNKNOWN.

    Parameters
    ----------
    cleaned_sex_offenders : DataFrame
        Cleaned sex offender records.
    cleaned_police_stations : DataFrame
        Cleaned police station records.
    cleaned_crimes : DataFrame
        Cleaned crime records used only for block-to-district approximation.
    run_id : str
        Batch run identifier.

    Returns
    -------
    DataFrame
        Sex offender density rows ready for sex_offender_density_temp.
    """
    offender_blocks = (
        cleaned_sex_offenders.withColumn("block_key", _normalized_block("block"))
        .where(F.col("block_key").isNotNull() & (F.length(F.col("block_key")) > 0))
        .groupBy("block_key")
        .agg(
            F.count(F.lit(1)).cast("long").alias("offender_count"),
            F.sum(F.when(F.upper(F.trim(F.col("victim_minor"))) == "Y", 1).otherwise(0))
            .cast("long")
            .alias("victim_minor_count"),
        )
    )

    crime_block_counts = (
        cleaned_crimes.withColumn("block_key", _normalized_block("block"))
        .where(F.col("block_key").isNotNull() & (F.length(F.col("block_key")) > 0))
        .groupBy("block_key", F.col("district").cast("string").alias("district"))
        .agg(F.count(F.lit(1)).alias("crime_count"))
    )
    block_window = Window.partitionBy("block_key").orderBy(F.desc("crime_count"), F.asc("district"))
    block_district = (
        crime_block_counts.withColumn("rank", F.row_number().over(block_window))
        .where(F.col("rank") == 1)
        .select("block_key", "district")
    )

    station_lookup = cleaned_police_stations.select(
        F.col("district").cast("string").alias("district"),
        F.col("district_name"),
    ).dropDuplicates(["district"])

    density = (
        offender_blocks.join(block_district, "block_key", "left")
        .withColumn("district", F.coalesce(F.col("district").cast("string"), F.lit("UNKNOWN")))
        .groupBy("district")
        .agg(
            F.sum("offender_count").cast("long").alias("offender_count"),
            F.sum("victim_minor_count").cast("long").alias("victim_minor_count"),
        )
        .join(station_lookup, "district", "left")
        .withColumn("district_name", F.coalesce(F.col("district_name"), F.lit("Unavailable - source lacks district")))
    )

    rank_window = Window.orderBy(F.desc("offender_count"), F.asc("district"))
    return (
        density.withColumn("density_rank", F.dense_rank().over(rank_window).cast("int"))
        .withColumn("run_id", F.lit(run_id))
        .select(
            "run_id",
            "district",
            "district_name",
            "offender_count",
            "victim_minor_count",
            "density_rank",
        )
    )
