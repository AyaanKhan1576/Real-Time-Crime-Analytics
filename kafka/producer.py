import csv
import json
import time
import os
from kafka import KafkaProducer
from config.config_loader import load_config


def json_serializer(obj):
    return json.dumps(obj).encode("utf-8")


def run_producer(config_path="config/config.yaml"):
    cfg = load_config(config_path)
    kafka_cfg = cfg.get("kafka", {})
    data_cfg = cfg.get("data", {})

    bootstrap = kafka_cfg.get("bootstrap_servers", "localhost:9092")
    topic = kafka_cfg.get("topic_crime_events", "crime-events")
    rate = float(kafka_cfg.get("producer_rate_per_second", 1))

    producer = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=json_serializer)

    data_file = os.path.join(data_cfg.get("base_path", "data"), data_cfg.get("crime_file"))
    # If running locally in repo, fallback to relative path
    if not os.path.exists(data_file):
        data_file = os.path.join(os.path.dirname(__file__), "..", data_cfg.get("crime_file"))
        data_file = os.path.normpath(data_file)

    with open(data_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Build standardized message
            msg = {
                "case_number": (row.get("CASE_NUMBER") or row.get("case_number") or "") ,
                "date": row.get("DATE") or row.get("date"),
                "block": row.get("BLOCK") or row.get("block"),
                "primary_type": row.get("PRIMARY_TYPE") or row.get("primary_type"),
                "district": str(row.get("DISTRICT") or row.get("district") or "UNKNOWN"),
                "arrest": (row.get("ARREST") or row.get("arrest") or "False") in ["True", "true", "1", "Y", "y"],
                "latitude": float(row.get("LATITUDE" or row.get("latitude") or 0) or 0),
                "longitude": float(row.get("LONGITUDE" or row.get("longitude") or 0) or 0)
            }

            producer.send(topic, msg)
            producer.flush()
            time.sleep(1.0 / rate)


if __name__ == "__main__":
    run_producer()
