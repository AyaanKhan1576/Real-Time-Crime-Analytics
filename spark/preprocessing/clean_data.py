"""
Module: clean_data.py
Description: Cleans raw Spark DataFrames into the shared field-name contract.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def standardize_column_name(name: str) -> str:
    """
    Convert a raw dataset column name to snake_case.

    Parameters
    ----------
    name : str
        Raw column name.

    Returns
    -------
    str
        Standardized snake_case name.
    """
    normalized = re.sub(r"[^0-9A-Za-z]+", "_", name.strip().lower())
    return re.sub(r"_+", "_", normalized).strip("_")


def with_standard_column_names(df: DataFrame) -> DataFrame:
    """
    Rename all DataFrame columns to snake_case.

    Parameters
    ----------
    df : DataFrame
        Raw DataFrame.

    Returns
    -------
    DataFrame
        DataFrame with standardized column names.
    """
    renamed_df = df
    for column in df.columns:
        renamed_df = renamed_df.withColumnRenamed(column, standardize_column_name(column))
    return renamed_df


def district_string(column_name: str = "district") -> F.Column:
    """
    Build a normalized district string expression with UNKNOWN fallback.

    Numeric Chicago police districts are stored as zero-padded strings so
    district joins work consistently across crime, violence, and station data.

    Parameters
    ----------
    column_name : str
        Source district column.

    Returns
    -------
    Column
        Cleaned district expression.
    """
    value = F.trim(F.col(column_name).cast("string"))
    numeric_value = F.regexp_replace(value, r"\.0$", "")
    return (
        F.when(value.isNull() | (value == ""), F.lit("UNKNOWN"))
        .when(numeric_value.rlike(r"^[0-9]+$"), F.lpad(numeric_value, 3, "0"))
        .otherwise(value)
    )


def parse_timestamp(column_name: str) -> F.Column:
    """
    Parse common Chicago portal timestamp formats.

    Parameters
    ----------
    column_name : str
        Source timestamp column.

    Returns
    -------
    Column
        Parsed timestamp expression.
    """
    value = F.trim(F.col(column_name))
    return F.coalesce(
        F.to_timestamp(value, "MM/dd/yyyy hh:mm:ss a"),
        F.to_timestamp(value, "MM/dd/yyyy HH:mm:ss"),
        F.to_timestamp(value, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(value, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(value),
    )


def parse_boolean(column_name: str) -> F.Column:
    """
    Parse common boolean text values.

    Parameters
    ----------
    column_name : str
        Source boolean column.

    Returns
    -------
    Column
        Boolean expression.
    """
    value = F.lower(F.trim(F.col(column_name).cast("string")))
    return F.when(value.isin("true", "t", "1", "yes", "y"), F.lit(True)).otherwise(F.lit(False))


def clean_crimes(raw_df: DataFrame) -> DataFrame:
    """
    Clean crime records to the project-wide contract.

    Parameters
    ----------
    raw_df : DataFrame
        Raw crime records.

    Returns
    -------
    DataFrame
        Cleaned crime records.
    """
    df = with_standard_column_names(raw_df)
    event_timestamp = parse_timestamp("date")
    return (
        df.withColumn("event_timestamp", event_timestamp)
        .select(
            F.trim(F.col("id")).alias("crime_id"),
            F.trim(F.col("case_number")).alias("case_number"),
            F.col("event_timestamp"),
            F.trim(F.col("date")).alias("date"),
            F.trim(F.col("block")).alias("block"),
            F.trim(F.col("iucr")).alias("iucr"),
            F.trim(F.col("primary_type")).alias("primary_type"),
            F.trim(F.col("description")).alias("description"),
            F.trim(F.col("location_description")).alias("location_description"),
            parse_boolean("arrest").alias("arrest"),
            parse_boolean("domestic").alias("domestic"),
            F.trim(F.col("beat")).alias("beat"),
            district_string("district").alias("district"),
            F.trim(F.col("ward")).alias("ward"),
            F.trim(F.col("community_area")).alias("community_area"),
            F.trim(F.col("fbi_code")).alias("fbi_code"),
            F.col("x_coordinate").cast("double").alias("x_coordinate"),
            F.col("y_coordinate").cast("double").alias("y_coordinate"),
            F.coalesce(F.year(F.col("event_timestamp")), F.col("year").cast("int")).alias("year"),
            parse_timestamp("updated_on").alias("updated_on"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.trim(F.col("location")).alias("location"),
        )
        .withColumn("month", F.month(F.col("event_timestamp")))
        .withColumn("day_of_week", F.date_format(F.col("event_timestamp"), "EEEE"))
        .withColumn("hour", F.hour(F.col("event_timestamp")))
    )


def clean_arrests(raw_df: DataFrame) -> DataFrame:
    """
    Clean arrest records to the project-wide contract.

    Parameters
    ----------
    raw_df : DataFrame
        Raw arrest records.

    Returns
    -------
    DataFrame
        Cleaned arrest records.
    """
    df = with_standard_column_names(raw_df)
    return df.select(
        F.trim(F.col("cb_no")).alias("cb_no"),
        F.trim(F.col("case_number")).alias("case_number"),
        parse_timestamp("arrest_date").alias("arrest_timestamp"),
        F.trim(F.col("race")).alias("race"),
        F.trim(F.col("charge_1_statute")).alias("charge_1_statute"),
        F.trim(F.col("charge_1_description")).alias("charge_1_description"),
        F.trim(F.col("charge_1_type")).alias("charge_1_type"),
        F.trim(F.col("charge_1_class")).alias("charge_1_class"),
    )


def clean_violence(raw_df: DataFrame) -> DataFrame:
    """
    Clean violence records to the project-wide contract.

    Parameters
    ----------
    raw_df : DataFrame
        Raw violence records.

    Returns
    -------
    DataFrame
        Cleaned violence records.
    """
    df = with_standard_column_names(raw_df)
    return df.select(
        F.trim(F.col("case_number")).alias("case_number"),
        parse_timestamp("date").alias("event_timestamp"),
        F.trim(F.col("block")).alias("block"),
        F.trim(F.col("victimization_primary")).alias("victimization_primary"),
        F.trim(F.col("incident_primary")).alias("incident_primary"),
        F.upper(F.trim(F.col("gunshot_injury_i"))).alias("gunshot_injury_i"),
        F.trim(F.col("zip_code")).alias("zip_code"),
        F.trim(F.col("ward")).alias("ward"),
        F.trim(F.col("community_area")).alias("community_area"),
        district_string("district").alias("district"),
        F.trim(F.col("beat")).alias("beat"),
        F.col("age").cast("int").alias("age"),
        F.trim(F.col("sex")).alias("sex"),
        F.trim(F.col("race")).alias("race"),
        F.trim(F.col("victimization_fbi_cd")).alias("victimization_fbi_cd"),
        F.trim(F.col("incident_fbi_cd")).alias("incident_fbi_cd"),
        F.trim(F.col("month")).alias("month"),
        F.trim(F.col("day_of_week")).alias("day_of_week"),
        F.col("hour").cast("int").alias("hour"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
    )


def clean_sex_offenders(raw_df: DataFrame) -> DataFrame:
    """
    Clean sex offender records to the project-wide contract.

    Parameters
    ----------
    raw_df : DataFrame
        Raw sex offender records.

    Returns
    -------
    DataFrame
        Cleaned sex offender records.
    """
    df = with_standard_column_names(raw_df)
    return df.select(
        F.trim(F.col("last")).alias("last_name"),
        F.trim(F.col("first")).alias("first_name"),
        F.trim(F.col("block")).alias("block"),
        F.trim(F.col("gender")).alias("gender"),
        F.trim(F.col("race")).alias("race"),
        parse_timestamp("birth_date").alias("birth_date"),
        F.trim(F.col("height")).alias("height"),
        F.trim(F.col("weight")).alias("weight"),
        F.upper(F.trim(F.col("victim_minor"))).alias("victim_minor"),
    )


def clean_police_stations(raw_df: DataFrame) -> DataFrame:
    """
    Clean police station records to the project-wide contract.

    Parameters
    ----------
    raw_df : DataFrame
        Raw police station records.

    Returns
    -------
    DataFrame
        Cleaned police station records.
    """
    df = with_standard_column_names(raw_df)
    return df.select(
        district_string("district").alias("district"),
        F.trim(F.col("district_name")).alias("district_name"),
        F.trim(F.col("address")).alias("address"),
        F.trim(F.col("city")).alias("city"),
        F.trim(F.col("state")).alias("state"),
        F.trim(F.col("zip")).alias("zip"),
        F.trim(F.col("phone")).alias("phone"),
        F.col("x_coordinate").cast("double").alias("x_coordinate"),
        F.col("y_coordinate").cast("double").alias("y_coordinate"),
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
    )
