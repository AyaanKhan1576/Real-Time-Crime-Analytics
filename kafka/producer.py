import csv
import json
import time
import os
import logging
import argparse
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from kafka import KafkaProducer
from config.config_loader import load_config


FALLBACK_FILE_NAMES = {
    "crime_file": "Crimes_-_2001_to_Present_20260501.csv",
}


def json_serializer(obj):
    return json.dumps(obj).encode("utf-8")


def as_bool(value):
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "y", "yes", "t"}


def as_float(value, default=0.0):
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def read_any(row, keys):
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def normalize_header(name):
    if name is None:
        return ""
    normalized = str(name).strip().lstrip("\ufeff").lower()
    normalized = [character if character.isalnum() else "_" for character in normalized]
    normalized = "".join(normalized)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def normalize_district(value):
    district = "" if value is None else str(value).strip()
    if not district:
        return "UNKNOWN"
    if district.endswith(".0"):
        district = district[:-2]
    return district.zfill(3) if district.isdigit() else district


def resolve_data_file(data_cfg, file_key):
    base_path = data_cfg.get("base_path", "data")
    configured_name = data_cfg.get(file_key)
    candidates = []
    if configured_name:
        candidates.append(os.path.join(base_path, configured_name))
        candidates.append(os.path.join(os.path.dirname(__file__), "..", "data", configured_name))
        candidates.append(os.path.join(os.path.dirname(__file__), "..", "data", "raw", configured_name))

    fallback_name = FALLBACK_FILE_NAMES.get(file_key)
    if fallback_name:
        candidates.append(os.path.join(base_path, fallback_name))
        candidates.append(os.path.join(os.path.dirname(__file__), "..", "data", fallback_name))
        candidates.append(os.path.join(os.path.dirname(__file__), "..", "data", "raw", fallback_name))

    for candidate in candidates:
        normalized = os.path.normpath(candidate)
        if os.path.exists(normalized):
            return normalized

    return os.path.normpath(candidates[0]) if candidates else os.path.join(base_path, "")


def run_producer(config_path="config/config.yaml", max_rows=None):
    cfg = load_config(config_path)
    kafka_cfg = cfg.get("kafka", {})
    data_cfg = cfg.get("data", {})

    bootstrap = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    topic = kafka_cfg.get("topic_crime_events", "crime-events")
    rate = float(kafka_cfg.get("producer_rate_per_second", 1))
    interval = kafka_cfg.get("producer_interval_seconds")
    interval = None if interval is None else float(interval)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=json_serializer,
        retries=int(kafka_cfg.get("producer_retry_count", 3)),
    )

    data_file = resolve_data_file(data_cfg, "crime_file")
    if not os.path.exists(data_file):
        raise FileNotFoundError(f"Crime dataset not found: {data_file}")

    if interval is not None and interval > 0:
        sleep_seconds = interval
        rate_label = f"1 event every {interval:g}s"
    elif rate <= 0:
        sleep_seconds = 0
        rate_label = "unlimited"
    else:
        rate = max(rate, 0.1)
        sleep_seconds = 1.0 / rate
        rate_label = f"{rate:g} events/s"
    sent = 0
    logging.info("Producer starting. topic=%s broker=%s rate=%s file=%s", topic, bootstrap, rate_label, data_file)

    with open(data_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            normalized_row = {normalize_header(key): value for key, value in row.items()}
            case_number = read_any(row, ["CASE NUMBER", "CASE_NUMBER", "case_number"]) or normalized_row.get("case_number")
            date_value = read_any(row, ["DATE", "date"]) or normalized_row.get("date")
            block_value = read_any(row, ["BLOCK", "block"]) or normalized_row.get("block")
            primary_type = read_any(row, ["PRIMARY TYPE", "PRIMARY_TYPE", "primary_type"]) or normalized_row.get("primary_type")
            district = read_any(row, ["DISTRICT", "district"]) or normalized_row.get("district")
            arrest_value = read_any(row, ["ARREST", "arrest"]) or normalized_row.get("arrest")
            latitude = read_any(row, ["LATITUDE", "latitude"]) or normalized_row.get("latitude")
            longitude = read_any(row, ["LONGITUDE", "longitude"]) or normalized_row.get("longitude")

            # Skip malformed rows before sending to Kafka.
            if not case_number or not date_value or not block_value or not primary_type:
                continue

            msg = {
                "case_number": str(case_number).strip(),
                "date": str(date_value).strip(),
                "block": str(block_value).strip(),
                "primary_type": str(primary_type).strip(),
                "district": normalize_district(district),
                "arrest": as_bool(arrest_value),
                "latitude": as_float(latitude),
                "longitude": as_float(longitude),
            }

            producer.send(topic, msg)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            sent += 1

            if max_rows and sent >= max_rows:
                break

    producer.flush()
    logging.info("Producer finished. Sent %d records.", sent)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay crime CSV rows to Kafka as JSON events")
    parser.add_argument("--config", default="config/config.yaml", help="Path to config.yaml")
    parser.add_argument("--max-rows", type=int, default=None, help="Optional max rows to publish")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_producer(config_path=args.config, max_rows=args.max_rows)
