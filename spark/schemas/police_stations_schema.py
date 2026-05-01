"""
Module: police_stations_schema.py
Description: Defines the explicit Spark schema for police station records.
"""

from pyspark.sql.types import StringType, StructField, StructType


def get_police_stations_schema() -> StructType:
    """
    Return the raw police stations CSV schema.

    Returns
    -------
    StructType
        Explicit schema matching the police stations CSV header.
    """
    return StructType(
        [
            StructField("DISTRICT", StringType(), True),
            StructField("DISTRICT NAME", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("ZIP", StringType(), True),
            StructField("WEBSITE", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("FAX", StringType(), True),
            StructField("TTY", StringType(), True),
            StructField("X COORDINATE", StringType(), True),
            StructField("Y COORDINATE", StringType(), True),
            StructField("LATITUDE", StringType(), True),
            StructField("LONGITUDE", StringType(), True),
            StructField("LOCATION", StringType(), True),
        ]
    )
