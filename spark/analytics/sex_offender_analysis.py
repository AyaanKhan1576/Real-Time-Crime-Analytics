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


def _district_code(column_name: str) -> F.Column:
    """
    Normalize district values to the shared string format.

    Chicago crime rows use zero-padded district strings such as 007, while
    police station rows use values such as 7. This expression keeps non-numeric
    labels unchanged and pads numeric district IDs to three characters.

    Parameters
    ----------
    column_name : str
        Source district column.

    Returns
    -------
    Column
        Normalized district code.
    """
    value = F.trim(F.col(column_name).cast("string"))
    return F.when(value.rlike(r"^[0-9]+$"), F.lpad(value, 3, "0")).otherwise(value)


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
    whose block cannot be matched are retained as an UNMATCHED summary row and
    excluded from density ranking, because they do not all belong to one unknown
    district.

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
    block_key = _normalized_block("block")
    offender_blocks = (
        cleaned_sex_offenders.withColumn(
            "block_key",
            F.when(block_key.isNotNull() & (F.length(block_key) > 0), block_key).otherwise(F.lit("__MISSING_BLOCK__")),
        )
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
        .withColumn("district", _district_code("district"))
        .groupBy("block_key", "district")
        .agg(F.count(F.lit(1)).alias("crime_count"))
    )
    block_window = Window.partitionBy("block_key").orderBy(F.desc("crime_count"), F.asc("district"))
    block_district = (
        crime_block_counts.withColumn("rank", F.row_number().over(block_window))
        .where(F.col("rank") == 1)
        .select("block_key", "district")
    )

    station_lookup = cleaned_police_stations.select(
        _district_code("district").alias("district"),
        F.col("district_name"),
    ).where(F.col("district").rlike(r"^[0-9]{3}$")).dropDuplicates(["district"])

    matched_blocks = offender_blocks.join(block_district, "block_key", "left").where(F.col("district").isNotNull())
    unmatched = (
        offender_blocks.join(block_district, "block_key", "left")
        .where(F.col("district").isNull())
        .agg(
            F.coalesce(F.sum("offender_count"), F.lit(0)).cast("long").alias("offender_count"),
            F.coalesce(F.sum("victim_minor_count"), F.lit(0)).cast("long").alias("victim_minor_count"),
        )
        .where(F.col("offender_count") > 0)
        .select(
            F.lit("UNMATCHED").alias("district"),
            F.lit("No district match from block-level approximation").alias("district_name"),
            "offender_count",
            "victim_minor_count",
            F.lit(None).cast("int").alias("density_rank"),
        )
    )

    density = (
        matched_blocks.withColumn("district", _district_code("district"))
        .groupBy("district")
        .agg(
            F.sum("offender_count").cast("long").alias("offender_count"),
            F.sum("victim_minor_count").cast("long").alias("victim_minor_count"),
        )
        .join(station_lookup, "district", "left")
        .withColumn("district_name", F.coalesce(F.col("district_name"), F.lit("District name unavailable")))
    )

    rank_window = Window.orderBy(F.desc("offender_count"), F.asc("district"))
    ranked_density = (
        density.withColumn("density_rank", F.dense_rank().over(rank_window).cast("int"))
        .select(
            "district",
            "district_name",
            "offender_count",
            "victim_minor_count",
            "density_rank",
        )
    )

    return (
        ranked_density.unionByName(unmatched)
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
