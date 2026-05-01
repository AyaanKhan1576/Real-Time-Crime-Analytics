from pymongo import ASCENDING, DESCENDING, MongoClient
import os
import sys


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config_loader import load_config


def main() -> None:
    cfg = load_config("config/config.yaml")
    mongo_cfg = cfg.get("mongodb", {})

    client = MongoClient(mongo_cfg.get("host", "mongodb"), int(mongo_cfg.get("port", 27017)))
    db = client[mongo_cfg.get("database", "crime_analytics")]

    raw_name = mongo_cfg.get("raw_events_collection", "raw_events")
    alert_name = mongo_cfg.get("alert_logs_collection", "alert_logs")
    ttl_seconds = int(mongo_cfg.get("raw_event_ttl_seconds", 86400))

    raw_coll = db[raw_name]
    alert_coll = db[alert_name]

    # TTL-controlled retention for optional raw event logging.
    raw_coll.create_index([("ingested_at", ASCENDING)], expireAfterSeconds=ttl_seconds)

    alert_coll.create_index([("district", ASCENDING)])
    alert_coll.create_index([("timestamp", DESCENDING)])
    alert_coll.create_index([("alert_id", ASCENDING)], unique=True)

    print("MongoDB indexes are ready.")


if __name__ == "__main__":
    main()
