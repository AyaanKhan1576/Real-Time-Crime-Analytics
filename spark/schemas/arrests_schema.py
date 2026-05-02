"""
Module: arrests_schema.py
Description: Defines the explicit Spark schema for Chicago arrest records.
"""

from pyspark.sql.types import StringType, StructField, StructType


def get_arrests_schema() -> StructType:
    """
    Return the raw arrests CSV schema.

    Returns
    -------
    StructType
        Explicit schema matching the arrests CSV header.
    """
    return StructType(
        [
            StructField("CB_NO", StringType(), True),
            StructField("CASE NUMBER", StringType(), True),
            StructField("ARREST DATE", StringType(), True),
            StructField("RACE", StringType(), True),
            StructField("CHARGE 1 STATUTE", StringType(), True),
            StructField("CHARGE 1 DESCRIPTION", StringType(), True),
            StructField("CHARGE 1 TYPE", StringType(), True),
            StructField("CHARGE 1 CLASS", StringType(), True),
            StructField("CHARGE 2 STATUTE", StringType(), True),
            StructField("CHARGE 2 DESCRIPTION", StringType(), True),
            StructField("CHARGE 2 TYPE", StringType(), True),
            StructField("CHARGE 2 CLASS", StringType(), True),
            StructField("CHARGE 3 STATUTE", StringType(), True),
            StructField("CHARGE 3 DESCRIPTION", StringType(), True),
            StructField("CHARGE 3 TYPE", StringType(), True),
            StructField("CHARGE 3 CLASS", StringType(), True),
            StructField("CHARGE 4 STATUTE", StringType(), True),
            StructField("CHARGE 4 DESCRIPTION", StringType(), True),
            StructField("CHARGE 4 TYPE", StringType(), True),
            StructField("CHARGE 4 CLASS", StringType(), True),
            StructField("CHARGES STATUTE", StringType(), True),
            StructField("CHARGES DESCRIPTION", StringType(), True),
            StructField("CHARGES TYPE", StringType(), True),
            StructField("CHARGES CLASS", StringType(), True),
        ]
    )
