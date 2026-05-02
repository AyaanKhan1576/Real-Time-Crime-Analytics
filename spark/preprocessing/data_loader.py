"""
Module: data_loader.py
Description: Loads Chicago public safety CSV datasets with explicit Spark schemas.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from spark.schemas.arrests_schema import get_arrests_schema
from spark.schemas.crime_schema import get_crime_schema
from spark.schemas.police_stations_schema import get_police_stations_schema
from spark.schemas.sex_offenders_schema import get_sex_offenders_schema
from spark.schemas.violence_schema import get_violence_schema


FALLBACK_FILE_NAMES = {
    "crime_file": "Crimes_-_2001_to_Present_20260501.csv",
    "arrests_file": "Arrests_20260501.csv",
    "violence_file": "Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20260501.csv",
    "sex_offenders_file": "Sex_Offenders_20260501.csv",
    "police_stations_file": "Police_Stations_20260501.csv",
}


def _resolve_data_path(data_config: dict[str, Any], file_key: str) -> str:
    """
    Resolve a configured dataset path, falling back to downloaded portal filenames.

    Parameters
    ----------
    data_config : dict[str, Any]
        Data section from config.yaml.
    file_key : str
        Config key containing a dataset filename.

    Returns
    -------
    str
        Path inside the container or local runtime.
    """
    base_path = Path(data_config["base_path"])
    configured_path = base_path / data_config[file_key]
    if configured_path.exists():
        return str(configured_path)

    fallback_name = FALLBACK_FILE_NAMES.get(file_key)
    if fallback_name:
        fallback_path = base_path / fallback_name
        if fallback_path.exists():
            return str(fallback_path)

    return str(configured_path)


def _read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """
    Read a CSV file with an explicit schema.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    path : str
        CSV file path.
    schema : StructType
        Explicit schema to apply.

    Returns
    -------
    DataFrame
        Raw Spark DataFrame.
    """
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("multiLine", True)
        .option("escape", '"')
        .schema(schema)
        .csv(path)
    )


def _sample_if_enabled(df: DataFrame, data_config: dict[str, Any], row_limit: int) -> DataFrame:
    """
    Apply deterministic local sample mode using the first configured rows.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    data_config : dict[str, Any]
        Data section from config.yaml.
    row_limit : int
        Maximum rows to retain when sample mode is enabled.

    Returns
    -------
    DataFrame
        Original or sampled DataFrame.
    """
    if data_config.get("sample_mode", True):
        return df.limit(int(row_limit))
    return df


def load_crimes(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    """
    Load the crime dataset with its explicit schema.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    DataFrame
        Raw or sampled crime DataFrame.
    """
    data_config = config["data"]
    path = _resolve_data_path(data_config, "crime_file")
    df = _read_csv(spark, path, get_crime_schema())
    return _sample_if_enabled(df, data_config, int(data_config["crime_sample_rows"]))


def load_arrests(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    """
    Load the arrests dataset with its explicit schema.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    DataFrame
        Raw or sampled arrests DataFrame.
    """
    data_config = config["data"]
    path = _resolve_data_path(data_config, "arrests_file")
    df = _read_csv(spark, path, get_arrests_schema())
    row_limit = int(data_config.get("arrests_sample_rows", data_config["other_sample_rows"]))
    return _sample_if_enabled(df, data_config, row_limit)


def load_violence(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    """
    Load the violence dataset with its explicit schema.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    DataFrame
        Raw or sampled violence DataFrame.
    """
    data_config = config["data"]
    path = _resolve_data_path(data_config, "violence_file")
    df = _read_csv(spark, path, get_violence_schema())
    row_limit = int(data_config.get("violence_sample_rows", data_config["other_sample_rows"]))
    return _sample_if_enabled(df, data_config, row_limit)


def load_sex_offenders(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    """
    Load the sex offenders dataset with its explicit schema.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    DataFrame
        Raw or sampled sex offenders DataFrame.
    """
    data_config = config["data"]
    path = _resolve_data_path(data_config, "sex_offenders_file")
    df = _read_csv(spark, path, get_sex_offenders_schema())
    row_limit = int(data_config.get("sex_offenders_sample_rows", data_config["other_sample_rows"]))
    return _sample_if_enabled(df, data_config, row_limit)


def load_police_stations(spark: SparkSession, config: dict[str, Any]) -> DataFrame:
    """
    Load the police stations dataset with its explicit schema.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    DataFrame
        Raw police stations DataFrame.
    """
    data_config = config["data"]
    path = _resolve_data_path(data_config, "police_stations_file")
    df = _read_csv(spark, path, get_police_stations_schema())
    row_limit = int(data_config.get("police_stations_sample_rows", data_config["other_sample_rows"]))
    return _sample_if_enabled(df, data_config, row_limit)
