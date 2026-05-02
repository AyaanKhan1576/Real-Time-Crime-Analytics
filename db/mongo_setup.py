from pymongo import ASCENDING, DESCENDING, MongoClient
import os
import sys
import time


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.config_loader import load_config


def cfg_value(mongo_cfg, key, env_name, default):
    return os.getenv(env_name) or mongo_cfg.get(key, default)


def main() -> None:
    cfg = load_config("config/config.yaml")
    mongo_cfg = cfg.get("mongodb", {})

    host = cfg_value(mongo_cfg, "host", "MONGODB_HOST", "mongodb")
    port = int(cfg_value(mongo_cfg, "port", "MONGODB_PORT", 27017))
    database = cfg_value(mongo_cfg, "database", "MONGODB_DB", "crime_analytics")
    client = MongoClient(host, port, serverSelectionTimeoutMS=2000)
    for attempt in range(1, 31):
        try:
            client.admin.command("ping")
            break
        except Exception:
            if attempt == 30:
                raise
            time.sleep(1)

    db = client[database]

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
