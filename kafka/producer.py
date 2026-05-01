"""
Module: producer.py
Description: Replays crime CSV rows to Kafka as normalized JSON events.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path = [
    path_entry
    for path_entry in sys.path
    if path_entry not in {"", str(PROJECT_ROOT)}
]

from kafka import KafkaProducer

sys.path.append(str(PROJECT_ROOT))

from common.config import load_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


FALLBACK_CRIME_FILE = "Crimes_-_2001_to_Present_20260501.csv"


def _resolve_crime_path(config: dict[str, Any]) -> Path:
    """
    Resolve the configured crime CSV path.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    Path
        Crime CSV path.
    """
    data_config = config["data"]
    base_path = Path(data_config["base_path"])
    configured_path = base_path / data_config["crime_file"]
    if configured_path.exists():
        return configured_path

    fallback_path = base_path / FALLBACK_CRIME_FILE
    if fallback_path.exists():
        return fallback_path

    return configured_path


def _parse_bool(value: str | None) -> bool:
    """
    Parse a CSV boolean value.

    Parameters
    ----------
    value : str | None
        Raw boolean text.

    Returns
    -------
    bool
        Parsed boolean.
    """
    return str(value or "").strip().lower() in {"true", "t", "1", "yes", "y"}


def _parse_float(value: str | None) -> float | None:
    """
    Parse a nullable float value.

    Parameters
    ----------
    value : str | None
        Raw numeric text.

    Returns
    -------
    float | None
        Parsed float or None.
    """
    try:
        cleaned = str(value or "").strip()
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def normalize_crime_row(row: dict[str, str]) -> dict[str, Any] | None:
    """
    Convert a raw crime CSV row to the agreed Kafka JSON contract.

    Parameters
    ----------
    row : dict[str, str]
        Raw CSV row.

    Returns
    -------
    dict[str, Any] | None
        Normalized event, or None when required fields are missing.
    """
    event = {
        "case_number": (row.get("Case Number") or "").strip(),
        "date": (row.get("Date") or "").strip(),
        "block": (row.get("Block") or "").strip(),
        "primary_type": (row.get("Primary Type") or "").strip(),
        "district": (row.get("District") or "").strip() or "UNKNOWN",
        "arrest": _parse_bool(row.get("Arrest")),
        "latitude": _parse_float(row.get("Latitude")),
        "longitude": _parse_float(row.get("Longitude")),
    }

    required_fields = ["case_number", "date", "block", "primary_type", "district"]
    if any(not event[field] for field in required_fields):
        return None
    return event


def build_producer(config: dict[str, Any]) -> KafkaProducer:
    """
    Create a Kafka producer for JSON messages.

    Parameters
    ----------
    config : dict[str, Any]
        Project configuration.

    Returns
    -------
    KafkaProducer
        Configured producer.
    """
    kafka_config = config["kafka"]
    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: str(value).encode("utf-8"),
        retries=int(kafka_config["producer_retry_count"]),
        linger_ms=100,
    )


def stream_crimes(config_path: str, max_rows: int | None = None) -> None:
    """
    Stream crime rows from CSV to Kafka.

    Parameters
    ----------
    config_path : str
        Path to config.yaml.
    max_rows : int | None
        Optional limit for smoke tests.
    """
    config = load_config(config_path)
    kafka_config = config["kafka"]
    data_config = config["data"]
    crime_path = _resolve_crime_path(config)
    if not crime_path.exists():
        raise FileNotFoundError(f"Crime CSV not found: {crime_path}")

    rate_per_second = float(kafka_config["producer_rate_per_second"])
    sleep_seconds = 1.0 / rate_per_second if rate_per_second > 0 else 0.0
    row_limit = max_rows
    if row_limit is None and data_config.get("sample_mode", True):
        row_limit = int(data_config["crime_sample_rows"])

    producer = build_producer(config)
    topic = kafka_config["topic_crime_events"]
    sent_count = 0
    skipped_count = 0

    logger.info("Streaming crimes from %s to topic %s", crime_path, topic)
    with crime_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        for raw_row in reader:
            event = normalize_crime_row(raw_row)
            if event is None:
                skipped_count += 1
                logger.warning("Skipping malformed crime row with missing required fields")
                continue

            producer.send(topic, key=event["district"], value=event)
            sent_count += 1

            if sent_count % 100 == 0:
                logger.info("Sent %s events, skipped %s rows", sent_count, skipped_count)

            if row_limit is not None and sent_count >= row_limit:
                break

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    producer.flush()
    producer.close()
    logger.info("Kafka producer finished. Sent=%s skipped=%s", sent_count, skipped_count)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Replay crime CSV rows into Kafka.")
    parser.add_argument("--config", default="/app/config/config.yaml", help="Path to config.yaml.")
    parser.add_argument("--max-rows", type=int, default=None, help="Optional maximum rows to publish.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    stream_crimes(args.config, args.max_rows)
