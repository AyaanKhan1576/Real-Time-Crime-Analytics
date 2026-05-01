"""
Module: mongo_setup.py
Description: Creates MongoDB indexes for raw streamed events and alert logs.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pymongo import ASCENDING, DESCENDING, MongoClient

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.config import load_config


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_mongodb(config_path: str | None = None) -> None:
    """
    Create required MongoDB indexes.

    Parameters
    ----------
    config_path : str | None
        Optional path to config.yaml.
    """
    config = load_config(config_path)
    mongo_config = config["mongodb"]
    client = MongoClient(
        host=mongo_config["host"],
        port=int(mongo_config["port"]),
        serverSelectionTimeoutMS=10000,
    )
    database = client[mongo_config["database"]]

    alert_logs = database[mongo_config["alert_logs_collection"]]
    alert_logs.create_index([("district", ASCENDING)])
    alert_logs.create_index([("timestamp", DESCENDING)])
    alert_logs.create_index([("alert_id", ASCENDING)], unique=True)

    raw_events = database[mongo_config["raw_events_collection"]]
    if mongo_config.get("enable_raw_event_logging", False):
        raw_events.create_index(
            [("ingested_at", ASCENDING)],
            expireAfterSeconds=int(mongo_config["raw_event_ttl_seconds"]),
            name="raw_events_ttl",
        )
    else:
        raw_events.create_index([("ingested_at", ASCENDING)])

    logger.info("MongoDB indexes created successfully")


if __name__ == "__main__":
    setup_mongodb(sys.argv[1] if len(sys.argv) > 1 else None)
