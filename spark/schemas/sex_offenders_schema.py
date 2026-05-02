"""
Module: sex_offenders_schema.py
Description: Defines the explicit Spark schema for sex offender records.
"""

from pyspark.sql.types import StringType, StructField, StructType


def get_sex_offenders_schema() -> StructType:
    """
    Return the raw sex offenders CSV schema.

    Returns
    -------
    StructType
        Explicit schema matching the sex offenders CSV header.
    """
    return StructType(
        [
            StructField("LAST", StringType(), True),
            StructField("FIRST", StringType(), True),
            StructField("BLOCK", StringType(), True),
            StructField("GENDER", StringType(), True),
            StructField("RACE", StringType(), True),
            StructField("BIRTH DATE", StringType(), True),
            StructField("HEIGHT", StringType(), True),
            StructField("WEIGHT", StringType(), True),
            StructField("VICTIM MINOR", StringType(), True),
        ]
    )
