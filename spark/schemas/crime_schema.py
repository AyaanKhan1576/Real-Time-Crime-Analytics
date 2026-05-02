"""
Module: crime_schema.py
Description: Defines the explicit Spark schema for Chicago crime records.
"""

from pyspark.sql.types import StringType, StructField, StructType


def get_crime_schema() -> StructType:
    """
    Return the raw crime CSV schema.

    Returns
    -------
    StructType
        Explicit schema matching the City of Chicago crime CSV header.
    """
    return StructType(
        [
            StructField("ID", StringType(), True),
            StructField("Case Number", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("Block", StringType(), True),
            StructField("IUCR", StringType(), True),
            StructField("Primary Type", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Location Description", StringType(), True),
            StructField("Arrest", StringType(), True),
            StructField("Domestic", StringType(), True),
            StructField("Beat", StringType(), True),
            StructField("District", StringType(), True),
            StructField("Ward", StringType(), True),
            StructField("Community Area", StringType(), True),
            StructField("FBI Code", StringType(), True),
            StructField("X Coordinate", StringType(), True),
            StructField("Y Coordinate", StringType(), True),
            StructField("Year", StringType(), True),
            StructField("Updated On", StringType(), True),
            StructField("Latitude", StringType(), True),
            StructField("Longitude", StringType(), True),
            StructField("Location", StringType(), True),
        ]
    )
