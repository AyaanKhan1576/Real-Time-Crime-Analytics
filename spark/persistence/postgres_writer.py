"""
Module: postgres_writer.py
Description: Writes Spark batch outputs to PostgreSQL using staging-then-publish.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


def _connection_kwargs(config: dict[str, Any]) -> dict[str, Any]:
    """
    Build psycopg2 connection parameters from config.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    dict[str, Any]
        PostgreSQL connection keyword arguments.
    """
    postgres = config["postgres"]
    return {
        "host": postgres["host"],
        "port": int(postgres["port"]),
        "dbname": postgres["database"],
        "user": postgres["user"],
        "password": postgres["password"],
    }


def mark_batch_started(config: dict[str, Any], run_id: str, job_name: str) -> None:
    """
    Insert a running batch status row.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    run_id : str
        Batch run identifier.
    job_name : str
        Batch job name.
    """
    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO batch_job_status (
                    run_id, job_name, status, started_at, message
                )
                VALUES (%s, %s, 'running', CURRENT_TIMESTAMP, %s);
                """,
                (run_id, job_name, "Batch analytics started"),
            )


def mark_batch_completed(config: dict[str, Any], run_id: str, message: str) -> None:
    """
    Mark a batch run as completed.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    run_id : str
        Batch run identifier.
    message : str
        Completion message.
    """
    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE batch_job_status
                SET status = 'completed',
                    finished_at = CURRENT_TIMESTAMP,
                    last_successful_run = CURRENT_TIMESTAMP,
                    message = %s
                WHERE run_id = %s;
                """,
                (message, run_id),
            )


def mark_batch_failed(config: dict[str, Any], run_id: str, message: str) -> None:
    """
    Mark a batch run as failed.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    run_id : str
        Batch run identifier.
    message : str
        Failure message.
    """
    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE batch_job_status
                SET status = 'failed',
                    finished_at = CURRENT_TIMESTAMP,
                    message = %s
                WHERE run_id = %s;
                """,
                (message[:2000], run_id),
            )


def write_crime_trends_temp(config: dict[str, Any], trends_df: DataFrame, run_id: str) -> int:
    """
    Write crime trends to crime_trends_temp for one run.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    trends_df : DataFrame
        Spark DataFrame containing trend rows.
    run_id : str
        Batch run identifier.

    Returns
    -------
    int
        Number of rows inserted.
    """
    rows = [
        (
            row["run_id"],
            row["trend_type"],
            row["trend_key"],
            int(row["crime_count"]),
        )
        for row in trends_df.collect()
    ]

    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM crime_trends_temp WHERE run_id = %s;", (run_id,))
            if rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO crime_trends_temp (
                        run_id, trend_type, trend_key, crime_count
                    )
                    VALUES %s;
                    """,
                    rows,
                )

    logger.info("Inserted %s crime trend rows into crime_trends_temp", len(rows))
    return len(rows)


def _none_or_int(value: Any) -> int | None:
    """
    Convert nullable values to int.

    Parameters
    ----------
    value : Any
        Source value.

    Returns
    -------
    int | None
        Converted integer or None.
    """
    return None if value is None else int(value)


def _none_or_float(value: Any) -> float | None:
    """
    Convert nullable values to float.

    Parameters
    ----------
    value : Any
        Source value.

    Returns
    -------
    float | None
        Converted float or None.
    """
    return None if value is None else float(value)


def write_arrest_rates_temp(config: dict[str, Any], arrest_rates_df: DataFrame, run_id: str) -> int:
    """
    Write arrest-rate rows to arrest_rates_temp.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    arrest_rates_df : DataFrame
        Arrest-rate DataFrame.
    run_id : str
        Batch run identifier.

    Returns
    -------
    int
        Number of inserted rows.
    """
    rows = [
        (
            row["run_id"],
            row["primary_type"],
            row["district"],
            row["race"],
            _none_or_int(row["total_crimes"]),
            _none_or_int(row["total_arrests"]),
            _none_or_float(row["arrest_rate"]),
        )
        for row in arrest_rates_df.collect()
    ]

    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM arrest_rates_temp WHERE run_id = %s;", (run_id,))
            if rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO arrest_rates_temp (
                        run_id, primary_type, district, race,
                        total_crimes, total_arrests, arrest_rate
                    )
                    VALUES %s;
                    """,
                    rows,
                )

    logger.info("Inserted %s arrest-rate rows into arrest_rates_temp", len(rows))
    return len(rows)


def write_violence_stats_temp(config: dict[str, Any], violence_stats_df: DataFrame, run_id: str) -> int:
    """
    Write violence statistics to violence_stats_temp.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    violence_stats_df : DataFrame
        Violence statistics DataFrame.
    run_id : str
        Batch run identifier.

    Returns
    -------
    int
        Number of inserted rows.
    """
    rows = [
        (
            row["run_id"],
            row["month"],
            row["district"],
            row["community_area"],
            _none_or_int(row["total_homicides"]),
            _none_or_int(row["total_nonfatal_shootings"]),
            _none_or_int(row["gunshot_incidents"]),
            _none_or_int(row["total_incidents"]),
            _none_or_float(row["gunshot_proportion"]),
        )
        for row in violence_stats_df.collect()
    ]

    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM violence_stats_temp WHERE run_id = %s;", (run_id,))
            if rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO violence_stats_temp (
                        run_id, month, district, community_area,
                        total_homicides, total_nonfatal_shootings,
                        gunshot_incidents, total_incidents, gunshot_proportion
                    )
                    VALUES %s;
                    """,
                    rows,
                )

    logger.info("Inserted %s violence-stat rows into violence_stats_temp", len(rows))
    return len(rows)


def write_sex_offender_density_temp(config: dict[str, Any], density_df: DataFrame, run_id: str) -> int:
    """
    Write sex offender density rows to sex_offender_density_temp.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    density_df : DataFrame
        Sex offender density DataFrame.
    run_id : str
        Batch run identifier.

    Returns
    -------
    int
        Number of inserted rows.
    """
    rows = [
        (
            row["run_id"],
            row["district"],
            row["district_name"],
            _none_or_int(row["offender_count"]),
            _none_or_int(row["victim_minor_count"]),
            _none_or_int(row["density_rank"]),
        )
        for row in density_df.collect()
    ]

    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM sex_offender_density_temp WHERE run_id = %s;", (run_id,))
            if rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO sex_offender_density_temp (
                        run_id, district, district_name,
                        offender_count, victim_minor_count, density_rank
                    )
                    VALUES %s;
                    """,
                    rows,
                )

    logger.info("Inserted %s sex offender density rows into sex_offender_density_temp", len(rows))
    return len(rows)


def write_hotspots_temp(config: dict[str, Any], hotspots_df: DataFrame, run_id: str) -> int:
    """
    Write hotspot rows to hotspots_temp.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    hotspots_df : DataFrame
        Hotspot DataFrame.
    run_id : str
        Batch run identifier.

    Returns
    -------
    int
        Number of inserted rows.
    """
    rows = [
        (
            row["run_id"],
            _none_or_int(row["cluster_id"]),
            _none_or_float(row["centroid_latitude"]),
            _none_or_float(row["centroid_longitude"]),
            _none_or_int(row["crime_count"]),
        )
        for row in hotspots_df.collect()
    ]

    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM hotspots_temp WHERE run_id = %s;", (run_id,))
            if rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO hotspots_temp (
                        run_id, cluster_id, centroid_latitude,
                        centroid_longitude, crime_count
                    )
                    VALUES %s;
                    """,
                    rows,
                )

    logger.info("Inserted %s hotspot rows into hotspots_temp", len(rows))
    return len(rows)


def write_correlations_temp(config: dict[str, Any], correlations_df: DataFrame, run_id: str) -> int:
    """
    Write correlation rows to correlations_temp.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    correlations_df : DataFrame
        Correlation DataFrame.
    run_id : str
        Batch run identifier.

    Returns
    -------
    int
        Number of inserted rows.
    """
    rows = [
        (
            row["run_id"],
            row["correlation_name"],
            row["grouping_key"],
            row["x_metric"],
            row["y_metric"],
            _none_or_float(row["x_value"]),
            _none_or_float(row["y_value"]),
            _none_or_float(row["correlation_value"]),
        )
        for row in correlations_df.collect()
    ]

    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM correlations_temp WHERE run_id = %s;", (run_id,))
            if rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO correlations_temp (
                        run_id, correlation_name, grouping_key, x_metric,
                        y_metric, x_value, y_value, correlation_value
                    )
                    VALUES %s;
                    """,
                    rows,
                )

    logger.info("Inserted %s correlation rows into correlations_temp", len(rows))
    return len(rows)


def publish_temp_tables(config: dict[str, Any], run_id: str, tables: Iterable[str]) -> None:
    """
    Publish selected temp tables into final tables for a completed run.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.
    run_id : str
        Batch run identifier.
    tables : Iterable[str]
        Final table names to publish. Each must have a matching *_temp table.
    """
    supported_tables = {
        "crime_trends": ["run_id", "trend_type", "trend_key", "crime_count", "created_at"],
        "arrest_rates": [
            "run_id",
            "primary_type",
            "district",
            "race",
            "total_crimes",
            "total_arrests",
            "arrest_rate",
            "created_at",
        ],
        "violence_stats": [
            "run_id",
            "month",
            "district",
            "community_area",
            "total_homicides",
            "total_nonfatal_shootings",
            "gunshot_incidents",
            "total_incidents",
            "gunshot_proportion",
            "created_at",
        ],
        "sex_offender_density": [
            "run_id",
            "district",
            "district_name",
            "offender_count",
            "victim_minor_count",
            "density_rank",
            "created_at",
        ],
        "hotspots": [
            "run_id",
            "cluster_id",
            "centroid_latitude",
            "centroid_longitude",
            "crime_count",
            "created_at",
        ],
        "correlations": [
            "run_id",
            "correlation_name",
            "grouping_key",
            "x_metric",
            "y_metric",
            "x_value",
            "y_value",
            "correlation_value",
            "created_at",
        ],
    }

    with psycopg2.connect(**_connection_kwargs(config)) as connection:
        with connection.cursor() as cursor:
            for table in tables:
                if table not in supported_tables:
                    raise ValueError(f"Unsupported publish table: {table}")
                columns = supported_tables[table]
                column_list = ", ".join(columns)
                temp_table = f"{table}_temp"
                cursor.execute(f"DELETE FROM {table} WHERE run_id = %s;", (run_id,))
                cursor.execute(
                    f"""
                    INSERT INTO {table} ({column_list})
                    SELECT {column_list}
                    FROM {temp_table}
                    WHERE run_id = %s;
                    """,
                    (run_id,),
                )

    logger.info("Published temp tables for run_id=%s: %s", run_id, ", ".join(tables))
