import csv
import json
import time
import os
import logging
import argparse
from kafka import KafkaProducer
from config.config_loader import load_config


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


def run_producer(config_path="config/config.yaml", max_rows=None):
    cfg = load_config(config_path)
    kafka_cfg = cfg.get("kafka", {})
    data_cfg = cfg.get("data", {})

    bootstrap = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    topic = kafka_cfg.get("topic_crime_events", "crime-events")
    rate = float(kafka_cfg.get("producer_rate_per_second", 1))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=json_serializer,
        retries=int(kafka_cfg.get("producer_retry_count", 3)),
    )

    data_file = os.path.join(data_cfg.get("base_path", "data"), data_cfg.get("crime_file"))
    # If running locally in repo, fallback to dataset filename under ./data/raw.
    if not os.path.exists(data_file):
        data_file = os.path.join(os.path.dirname(__file__), "..", "data", "raw", data_cfg.get("crime_file"))
        data_file = os.path.normpath(data_file)

    if not os.path.exists(data_file):
        raise FileNotFoundError(f"Crime dataset not found: {data_file}")

    rate = max(rate, 0.1)
    sent = 0
    logging.info("Producer starting. topic=%s broker=%s rate=%s file=%s", topic, bootstrap, rate, data_file)

    with open(data_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            case_number = read_any(row, ["CASE NUMBER", "CASE_NUMBER", "case_number"])
            date_value = read_any(row, ["DATE", "date"])
            block_value = read_any(row, ["BLOCK", "block"])
            primary_type = read_any(row, ["PRIMARY TYPE", "PRIMARY_TYPE", "primary_type"])
            district = read_any(row, ["DISTRICT", "district"])
            arrest_value = read_any(row, ["ARREST", "arrest"])
            latitude = read_any(row, ["LATITUDE", "latitude"])
            longitude = read_any(row, ["LONGITUDE", "longitude"])

            # Skip malformed rows before sending to Kafka.
            if not case_number or not date_value or not block_value or not primary_type:
                continue

            msg = {
                "case_number": str(case_number).strip(),
                "date": str(date_value).strip(),
                "block": str(block_value).strip(),
                "primary_type": str(primary_type).strip(),
                "district": str(district).strip() if district is not None and str(district).strip() else "UNKNOWN",
                "arrest": as_bool(arrest_value),
                "latitude": as_float(latitude),
                "longitude": as_float(longitude),
            }

            producer.send(topic, msg)
            time.sleep(1.0 / rate)
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
