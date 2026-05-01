"""
Module: run_batch.py
Description: Orchestrates Spark batch analytics with staging-then-publish writes.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_config
from spark.analytics.arrest_rates import compute_arrest_rates
from spark.analytics.correlations import compute_correlations
from spark.analytics.crime_trends import compute_crime_trends
from spark.analytics.sex_offender_analysis import compute_sex_offender_density
from spark.analytics.violence_analysis import compute_violence_stats
from spark.ml.hotspot_kmeans import compute_hotspots
from spark.persistence.postgres_writer import (
    mark_batch_completed,
    mark_batch_failed,
    mark_batch_started,
    publish_temp_tables,
    write_arrest_rates_temp,
    write_correlations_temp,
    write_crime_trends_temp,
    write_hotspots_temp,
    write_sex_offender_density_temp,
    write_violence_stats_temp,
)
from spark.progress_logging import SparkProgressTracker, configure_run_logging
from spark.preprocessing.clean_data import (
    clean_arrests,
    clean_crimes,
    clean_police_stations,
    clean_sex_offenders,
    clean_violence,
)
from spark.preprocessing.data_loader import (
    load_arrests,
    load_crimes,
    load_police_stations,
    load_sex_offenders,
    load_violence,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def generate_run_id(now: datetime | None = None) -> str:
    """
    Generate one deterministic run_id for the full batch run.

    Parameters
    ----------
    now : datetime | None
        Optional timestamp, mainly for deterministic tests.

    Returns
    -------
    str
        Batch run identifier in batch_YYYYMMDD_HHMMSS format.
    """
    timestamp = now or datetime.now()
    return f"batch_{timestamp.strftime('%Y%m%d_%H%M%S')}"


def build_spark_session(config: dict, master_override: str | None = None) -> SparkSession:
    """
    Create the Spark session for batch analytics.

    Parameters
    ----------
    config : dict
        Project configuration.
    master_override : str | None
        Optional Spark master override for local debugging.

    Returns
    -------
    SparkSession
        Configured Spark session.
    """
    spark_config = config["spark"]
    master = master_override or spark_config["master"]
    return (
        SparkSession.builder.appName(spark_config["app_name"])
        .master(master)
        .config("spark.sql.shuffle.partitions", str(spark_config["shuffle_partitions"]))
        .getOrCreate()
    )


def run_batch(config_path: str, master_override: str | None = None, run_id: str | None = None) -> str:
    """
    Run the current Spark batch milestone.

    Parameters
    ----------
    config_path : str
        Path to config.yaml.
    master_override : str | None
        Optional Spark master override.
    run_id : str | None
        Optional externally supplied run_id.

    Returns
    -------
    str
        Completed run_id.
    """
    config = load_config(config_path)
    current_run_id = run_id or generate_run_id()
    job_name = config["spark"]["app_name"]
    spark: SparkSession | None = None
    output_counts: dict[str, int] = {}
    publish_tables = [
        "crime_trends",
        "arrest_rates",
        "violence_stats",
        "sex_offender_density",
        "hotspots",
        "correlations",
    ]

    spark_config = config["spark"]
    progress_interval_seconds = float(spark_config.get("progress_interval_seconds", 30))
    log_path = configure_run_logging(current_run_id, spark_config.get("log_dir"))
    logger.info("Starting batch run_id=%s", current_run_id)
    logger.info("Spark batch log file: %s", log_path)
    logger.info("Progress heartbeat interval: %.1f seconds", progress_interval_seconds)
    progress = SparkProgressTracker(
        current_run_id,
        total_steps=15,
        logger=logger,
        interval_seconds=progress_interval_seconds,
    )

    try:
        with progress.stage("Register batch_job_status row"):
            mark_batch_started(config, current_run_id, job_name)

        with progress.stage("Build Spark session"):
            spark = build_spark_session(config, master_override)

        with progress.stage("Load and standardize crimes"):
            cleaned_crimes = clean_crimes(load_crimes(spark, config)).cache()

        with progress.stage("Load and standardize arrests"):
            cleaned_arrests = clean_arrests(load_arrests(spark, config)).cache()

        with progress.stage("Load and standardize violence incidents"):
            cleaned_violence = clean_violence(load_violence(spark, config)).cache()

        with progress.stage("Load and standardize sex offenders"):
            cleaned_sex_offenders = clean_sex_offenders(load_sex_offenders(spark, config)).cache()

        with progress.stage("Load and standardize police stations"):
            cleaned_police_stations = clean_police_stations(load_police_stations(spark, config)).cache()

        with progress.stage("Compute and stage crime trends"):
            crime_trends = compute_crime_trends(cleaned_crimes, current_run_id)
            output_counts["crime_trends"] = write_crime_trends_temp(config, crime_trends, current_run_id)

        with progress.stage("Compute and stage arrest rates"):
            arrest_rates = compute_arrest_rates(cleaned_crimes, cleaned_arrests, current_run_id)
            output_counts["arrest_rates"] = write_arrest_rates_temp(config, arrest_rates, current_run_id)

        with progress.stage("Compute and stage violence statistics"):
            violence_stats = compute_violence_stats(cleaned_violence, current_run_id)
            output_counts["violence_stats"] = write_violence_stats_temp(config, violence_stats, current_run_id)

        with progress.stage("Compute and stage sex offender density"):
            sex_offender_density = compute_sex_offender_density(
                cleaned_sex_offenders,
                cleaned_police_stations,
                cleaned_crimes,
                current_run_id,
            ).cache()
            output_counts["sex_offender_density"] = write_sex_offender_density_temp(
                config,
                sex_offender_density,
                current_run_id,
            )

        with progress.stage("Compute and stage K-Means hotspots"):
            hotspots = compute_hotspots(spark, cleaned_crimes, config, current_run_id)
            output_counts["hotspots"] = write_hotspots_temp(config, hotspots, current_run_id)

        with progress.stage("Compute and stage cross-dataset correlations"):
            correlations = compute_correlations(
                cleaned_crimes,
                cleaned_arrests,
                cleaned_violence,
                sex_offender_density,
                current_run_id,
            )
            output_counts["correlations"] = write_correlations_temp(config, correlations, current_run_id)

        with progress.stage("Publish staging tables to final tables"):
            publish_temp_tables(config, current_run_id, publish_tables)

        with progress.stage("Mark batch run completed"):
            mark_batch_completed(
                config,
                current_run_id,
                "Batch analytics completed successfully. Published rows: "
                + ", ".join(f"{table}={count}" for table, count in output_counts.items()),
            )
        logger.info("Completed batch run_id=%s", current_run_id)
        return current_run_id
    except Exception as exc:
        logger.exception("Batch run failed for run_id=%s", current_run_id)
        mark_batch_failed(config, current_run_id, str(exc))
        raise
    finally:
        if spark is not None:
            spark.stop()


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Run Spark batch analytics.")
    parser.add_argument(
        "--config",
        default="/app/config/config.yaml",
        help="Path to config.yaml inside the runtime environment.",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master override, such as local[*] for debugging.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run_id override for deterministic test runs.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    run_batch(arguments.config, arguments.master, arguments.run_id)
