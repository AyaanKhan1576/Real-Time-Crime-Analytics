"""
Module: violence_analysis.py
Description: Computes homicide, shooting, gunshot, and community violence statistics.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def compute_violence_stats(cleaned_violence: DataFrame, run_id: str) -> DataFrame:
    """
    Compute violence and gunshot statistics for PostgreSQL persistence.

    Parameters
    ----------
    cleaned_violence : DataFrame
        Cleaned violence records.
    run_id : str
        Batch run identifier.

    Returns
    -------
    DataFrame
        Violence statistics rows ready for violence_stats_temp.
    """
    base = (
        cleaned_violence.withColumn(
            "month_key",
            F.coalesce(F.date_format(F.col("event_timestamp"), "yyyy-MM"), F.trim(F.col("month")), F.lit("UNKNOWN")),
        )
        .withColumn("district_key", F.coalesce(F.trim(F.col("district").cast("string")), F.lit("UNKNOWN")))
        .withColumn("community_key", F.coalesce(F.trim(F.col("community_area")), F.lit("UNKNOWN")))
        .withColumn("incident_primary_upper", F.upper(F.trim(F.col("incident_primary"))))
        .withColumn("victimization_primary_upper", F.upper(F.trim(F.col("victimization_primary"))))
        .withColumn("gunshot_upper", F.upper(F.trim(F.col("gunshot_injury_i"))))
        .withColumn(
            "is_homicide",
            F.col("incident_primary_upper").contains("HOMICIDE")
            | F.col("victimization_primary_upper").contains("HOMICIDE"),
        )
        .withColumn("is_gunshot", F.col("gunshot_upper").isin("Y", "YES", "TRUE", "1"))
        .withColumn("is_nonfatal_shooting", F.col("is_gunshot") & ~F.col("is_homicide"))
    )

    grouped = (
        base.groupBy("month_key", "district_key", "community_key")
        .agg(
            F.sum(F.when(F.col("is_homicide"), 1).otherwise(0)).cast("long").alias("total_homicides"),
            F.sum(F.when(F.col("is_nonfatal_shooting"), 1).otherwise(0))
            .cast("long")
            .alias("total_nonfatal_shootings"),
            F.sum(F.when(F.col("is_gunshot"), 1).otherwise(0)).cast("long").alias("gunshot_incidents"),
            F.count(F.lit(1)).cast("long").alias("total_incidents"),
        )
        .withColumn(
            "gunshot_proportion",
            F.when(F.col("total_incidents") > 0, F.col("gunshot_incidents") / F.col("total_incidents")).otherwise(F.lit(0.0)),
        )
        .select(
            F.col("month_key").alias("month"),
            F.col("district_key").alias("district"),
            F.col("community_key").alias("community_area"),
            "total_homicides",
            "total_nonfatal_shootings",
            "gunshot_incidents",
            "total_incidents",
            "gunshot_proportion",
        )
    )

    top_community_window = Window.orderBy(F.desc("total_incidents"), F.asc("community_key"))
    top_communities = (
        base.groupBy("community_key")
        .agg(
            F.sum(F.when(F.col("is_homicide"), 1).otherwise(0)).cast("long").alias("total_homicides"),
            F.sum(F.when(F.col("is_nonfatal_shooting"), 1).otherwise(0))
            .cast("long")
            .alias("total_nonfatal_shootings"),
            F.sum(F.when(F.col("is_gunshot"), 1).otherwise(0)).cast("long").alias("gunshot_incidents"),
            F.count(F.lit(1)).cast("long").alias("total_incidents"),
        )
        .withColumn(
            "gunshot_proportion",
            F.when(F.col("total_incidents") > 0, F.col("gunshot_incidents") / F.col("total_incidents")).otherwise(F.lit(0.0)),
        )
        .withColumn("rank", F.row_number().over(top_community_window))
        .where(F.col("rank") <= 20)
        .select(
            F.lit("ALL").alias("month"),
            F.lit("ALL").alias("district"),
            F.col("community_key").alias("community_area"),
            "total_homicides",
            "total_nonfatal_shootings",
            "gunshot_incidents",
            "total_incidents",
            "gunshot_proportion",
        )
    )

    return (
        grouped.unionByName(top_communities)
        .withColumn("run_id", F.lit(run_id))
        .select(
            "run_id",
            "month",
            "district",
            "community_area",
            "total_homicides",
            "total_nonfatal_shootings",
            "gunshot_incidents",
            "total_incidents",
            F.col("gunshot_proportion").cast("double").alias("gunshot_proportion"),
        )
    )
