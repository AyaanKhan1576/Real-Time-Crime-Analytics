"""
Module: violence_schema.py
Description: Defines the explicit Spark schema for violence reduction records.
"""

from pyspark.sql.types import StringType, StructField, StructType


def get_violence_schema() -> StructType:
    """
    Return the raw violence CSV schema.

    Returns
    -------
    StructType
        Explicit schema matching the violence dataset CSV header.
    """
    return StructType(
        [
            StructField("CASE_NUMBER", StringType(), True),
            StructField("DATE", StringType(), True),
            StructField("BLOCK", StringType(), True),
            StructField("VICTIMIZATION_PRIMARY", StringType(), True),
            StructField("INCIDENT_PRIMARY", StringType(), True),
            StructField("GUNSHOT_INJURY_I", StringType(), True),
            StructField("UNIQUE_ID", StringType(), True),
            StructField("ZIP_CODE", StringType(), True),
            StructField("WARD", StringType(), True),
            StructField("COMMUNITY_AREA", StringType(), True),
            StructField("STREET_OUTREACH_ORGANIZATION", StringType(), True),
            StructField("AREA", StringType(), True),
            StructField("DISTRICT", StringType(), True),
            StructField("BEAT", StringType(), True),
            StructField("AGE", StringType(), True),
            StructField("SEX", StringType(), True),
            StructField("RACE", StringType(), True),
            StructField("VICTIMIZATION_FBI_CD", StringType(), True),
            StructField("INCIDENT_FBI_CD", StringType(), True),
            StructField("VICTIMIZATION_FBI_DESCR", StringType(), True),
            StructField("INCIDENT_FBI_DESCR", StringType(), True),
            StructField("VICTIMIZATION_IUCR_CD", StringType(), True),
            StructField("INCIDENT_IUCR_CD", StringType(), True),
            StructField("VICTIMIZATION_IUCR_SECONDARY", StringType(), True),
            StructField("INCIDENT_IUCR_SECONDARY", StringType(), True),
            StructField("HOMICIDE_VICTIM_FIRST_NAME", StringType(), True),
            StructField("HOMICIDE_VICTIM_MI", StringType(), True),
            StructField("HOMICIDE_VICTIM_LAST_NAME", StringType(), True),
            StructField("MONTH", StringType(), True),
            StructField("DAY_OF_WEEK", StringType(), True),
            StructField("HOUR", StringType(), True),
            StructField("LOCATION_DESCRIPTION", StringType(), True),
            StructField("STATE_HOUSE_DISTRICT", StringType(), True),
            StructField("STATE_SENATE_DISTRICT", StringType(), True),
            StructField("UPDATED", StringType(), True),
            StructField("LATITUDE", StringType(), True),
            StructField("LONGITUDE", StringType(), True),
            StructField("LOCATION", StringType(), True),
        ]
    )
