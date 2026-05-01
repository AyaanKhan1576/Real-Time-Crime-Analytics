"""
Module: hotspot_kmeans.py
Description: Detects geospatial crime hotspots with PySpark MLlib K-Means.
"""

from __future__ import annotations

from typing import Any

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType


def compute_hotspots(
    spark: SparkSession,
    cleaned_crimes: DataFrame,
    config: dict[str, Any],
    run_id: str,
) -> DataFrame:
    """
    Compute K-Means hotspot centroids and cluster counts.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    cleaned_crimes : DataFrame
        Cleaned crime records.
    config : dict[str, Any]
        Project configuration.
    run_id : str
        Batch run identifier.

    Returns
    -------
    DataFrame
        Hotspot rows ready for hotspots_temp.
    """
    schema = StructType(
        [
            StructField("run_id", cleaned_crimes.schema["case_number"].dataType, False),
            StructField("cluster_id", IntegerType(), False),
            StructField("centroid_latitude", DoubleType(), True),
            StructField("centroid_longitude", DoubleType(), True),
            StructField("crime_count", LongType(), True),
        ]
    )

    coordinates = (
        cleaned_crimes.select(
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
        )
        .where(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .where((F.col("latitude") != 0.0) & (F.col("longitude") != 0.0))
    )

    coordinate_count = coordinates.count()
    if coordinate_count == 0:
        return spark.createDataFrame([], schema)

    requested_k = int(config["spark"]["kmeans_k"])
    k = max(1, min(requested_k, coordinate_count))
    assembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
    features = assembler.transform(coordinates).select("features")

    model = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster_id").fit(features)
    predictions = model.transform(features)
    counts = predictions.groupBy("cluster_id").agg(F.count(F.lit(1)).cast("long").alias("crime_count"))

    centers = [
        (run_id, int(index), float(center[0]), float(center[1]))
        for index, center in enumerate(model.clusterCenters())
    ]
    centers_df = spark.createDataFrame(
        centers,
        ["run_id", "cluster_id", "centroid_latitude", "centroid_longitude"],
    )

    return (
        centers_df.join(counts, "cluster_id", "left")
        .fillna({"crime_count": 0})
        .select(
            "run_id",
            F.col("cluster_id").cast("int").alias("cluster_id"),
            F.col("centroid_latitude").cast("double").alias("centroid_latitude"),
            F.col("centroid_longitude").cast("double").alias("centroid_longitude"),
            F.col("crime_count").cast("long").alias("crime_count"),
        )
    )
